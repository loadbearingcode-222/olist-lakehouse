"""Turn Olist rows into one deterministic, business-time-ordered list of database events.

Per order, the lifecycle becomes the changes an app would have made:

    purchase_ts             INSERT customer, INSERT order (status 'created'), items, payments
    approved_at             UPDATE order  status 'approved',  approved_at
    delivered_carrier_at    UPDATE order  status 'shipped',   delivered_carrier_at
    delivered_customer_at   UPDATE order  status 'delivered', delivered_customer_at
    (final status differs)  UPDATE order  status <final>, one hour after the last change
    review_creation_at      INSERT review

Olist's timestamps are not always in lifecycle order (1,359 orders reach the carrier before
approval). An event's *time* is clamped forward so a lifecycle never runs backwards, but the
*values* written are always the source's own. Every order therefore ends exactly as the CSV
says, and the replay is reproducible: the same data + seed + rates give the same list.

Reference data (categories, geolocation, sellers, products) is inserted one day before the
first purchase. Mutations then add history for CDC and SCD2 to find (seeded):

    seller relocation       UPDATE seller zip/city/state to another seller's location
    product recategorised   UPDATE product category_name
    review deleted          DELETE review, seven days after it was created
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal

from olist_lakehouse.replayer.source import OlistData, Row

Op = Literal["insert", "update", "delete"]

# Bump when the event list for the same data and seed would change: resuming a replay that
# was started under another version would then be refused (fingerprint mismatch).
TIMELINE_VERSION = 1

FINAL_STATUS_DELAY = timedelta(hours=1)
REVIEW_DELETE_DELAY = timedelta(days=7)
LIFECYCLE = (
    ("approved", "approved_at"),
    ("shipped", "delivered_carrier_at"),
    ("delivered", "delivered_customer_at"),
)

KEYS: dict[str, tuple[str, ...]] = {
    "product_category_translation": ("category_name",),
    "zip_geolocation": ("zip_code_prefix",),
    "sellers": ("seller_id",),
    "products": ("product_id",),
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "order_payments": ("order_id", "payment_sequential"),
    "order_reviews": ("review_id", "order_id"),
}


@dataclass(frozen=True)
class Event:
    ts: datetime
    op: Op
    table: str
    key: dict[str, Any]
    values: dict[str, Any] = field(default_factory=dict)
    # Facts a chaos rule may need that are not written (e.g. an order's purchase time).
    context: dict[str, Any] = field(default_factory=dict, compare=False)
    # Tie-breakers for events at the same business time: group, then position in the group.
    group: str = ""
    position: int = 0


@dataclass(frozen=True)
class MutationRates:
    seller_relocation: float = 0.02
    product_recategorisation: float = 0.03
    review_delete: float = 0.005


def _key(table: str, row: Row) -> dict[str, Any]:
    return {column: row[column] for column in KEYS[table]}


def _insert(ts: datetime, table: str, row: Row, group: str, position: int) -> Event:
    return Event(ts, "insert", table, _key(table, row), dict(row), group=group, position=position)


def reference_events(data: OlistData, at: datetime) -> list[Event]:
    events: list[Event] = []
    tables: list[tuple[str, list[Row]]] = [
        ("product_category_translation", data.categories),
        ("zip_geolocation", data.geolocation),
        ("sellers", [{**s, "updated_at": at} for s in data.sellers]),
        ("products", [{**p, "updated_at": at} for p in data.products]),
    ]
    for table, rows in tables:
        for row in rows:
            events.append(_insert(at, table, row, "", len(events)))
    return events


def order_events(order: Row, customer: Row, items: list[Row], payments: list[Row]) -> list[Event]:
    order_id = order["order_id"]
    purchase: datetime = order["purchase_ts"]
    events: list[Event] = []

    def add(event_ts: datetime, op: Op, table: str, key: dict[str, Any], values: Row) -> None:
        events.append(
            Event(
                event_ts,
                op,
                table,
                key,
                values,
                context={"purchase_ts": purchase},
                group=order_id,
                position=len(events),
            )
        )

    add(
        purchase,
        "insert",
        "customers",
        _key("customers", customer),
        {**customer, "updated_at": purchase},
    )
    created = {
        **{
            k: order[k] for k in ("order_id", "customer_id", "purchase_ts", "estimated_delivery_at")
        },
        "order_status": "created",
        "approved_at": None,
        "delivered_carrier_at": None,
        "delivered_customer_at": None,
        "updated_at": purchase,
    }
    add(purchase, "insert", "orders", {"order_id": order_id}, created)
    for item in sorted(items, key=lambda r: r["order_item_id"]):
        add(purchase, "insert", "order_items", _key("order_items", item), dict(item))
    for payment in sorted(payments, key=lambda r: r["payment_sequential"]):
        add(purchase, "insert", "order_payments", _key("order_payments", payment), dict(payment))

    status, last = "created", purchase
    for next_status, column in LIFECYCLE:
        value: datetime | None = order[column]
        if value is None:
            continue
        at = max(value, last)
        add(
            at,
            "update",
            "orders",
            {"order_id": order_id},
            {"order_status": next_status, column: value, "updated_at": at},
        )
        status, last = next_status, at

    if order["order_status"] != status:
        at = last + FINAL_STATUS_DELAY
        add(
            at,
            "update",
            "orders",
            {"order_id": order_id},
            {"order_status": order["order_status"], "updated_at": at},
        )
    return events


def review_events(review: Row, purchase: datetime) -> list[Event]:
    at = max(review["review_creation_at"], purchase)
    return [_insert(at, "order_reviews", review, review["order_id"], 10_000)]


def mutation_events(
    data: OlistData, rates: MutationRates, seed: int, window: tuple[datetime, datetime]
) -> list[Event]:
    rng = random.Random(f"{seed}:mutations")
    start, end = window
    span = (end - start).total_seconds()
    events: list[Event] = []

    def when() -> datetime:
        return start + timedelta(seconds=round(rng.random() * span))

    sellers = sorted(data.sellers, key=lambda r: r["seller_id"])
    for seller in sellers:
        if rng.random() < rates.seller_relocation:
            elsewhere = [s for s in sellers if s["zip_code_prefix"] != seller["zip_code_prefix"]]
            if not elsewhere:
                continue
            target = rng.choice(elsewhere)
            at = when()
            events.append(
                Event(
                    at,
                    "update",
                    "sellers",
                    {"seller_id": seller["seller_id"]},
                    {
                        "zip_code_prefix": target["zip_code_prefix"],
                        "city": target["city"],
                        "state": target["state"],
                        "updated_at": at,
                    },
                    group=f"~seller:{seller['seller_id']}",
                )
            )

    categories = sorted(c["category_name"] for c in data.categories)
    for product in sorted(data.products, key=lambda r: r["product_id"]):
        if rng.random() < rates.product_recategorisation:
            choices = [c for c in categories if c != product["category_name"]]
            if not choices:
                continue
            at = when()
            events.append(
                Event(
                    at,
                    "update",
                    "products",
                    {"product_id": product["product_id"]},
                    {"category_name": rng.choice(choices), "updated_at": at},
                    group=f"~product:{product['product_id']}",
                )
            )
    return events


def build(data: OlistData, *, seed: int, rates: MutationRates | None = None) -> list[Event]:
    """The whole replay, sorted by (business time, group, position). Deterministic."""
    rates = rates or MutationRates()
    orders = sorted(data.orders, key=lambda r: (r["purchase_ts"], r["order_id"]))
    first, last = orders[0]["purchase_ts"], orders[-1]["purchase_ts"]
    purchase_of = {o["order_id"]: o["purchase_ts"] for o in orders}

    events = reference_events(data, first - timedelta(days=1))
    for order in orders:
        events += order_events(
            order,
            data.customers[order["customer_id"]],
            data.items.get(order["order_id"], []),
            data.payments.get(order["order_id"], []),
        )

    deletes = random.Random(f"{seed}:review-deletes")
    for review in sorted(data.reviews, key=lambda r: (r["review_id"], r["order_id"])):
        inserted = review_events(review, purchase_of[review["order_id"]])
        events += inserted
        if deletes.random() < rates.review_delete:
            at = inserted[0].ts + REVIEW_DELETE_DELAY
            events.append(
                Event(
                    at,
                    "delete",
                    "order_reviews",
                    _key("order_reviews", review),
                    group=review["order_id"],
                    position=20_000,
                )
            )

    events += mutation_events(data, rates, seed, (first, last))
    events.sort(key=lambda e: (e.ts, e.group, e.position))
    return events


def expected_rows(events: list[Event]) -> dict[str, int]:
    """Row count per table once `events` have all been applied (inserts minus deletes)."""
    counts = dict.fromkeys(KEYS, 0)
    for event in events:
        if event.op == "insert":
            counts[event.table] += 1
        elif event.op == "delete":
            counts[event.table] -= 1
    return counts
