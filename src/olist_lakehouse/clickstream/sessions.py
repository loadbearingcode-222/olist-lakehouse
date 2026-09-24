"""Turn one replayed order into web sessions, deterministically.

For an order bought at `purchase_ts`, the converting session starts 5-40 minutes earlier:

    page_view     one to three per product in the order (the customer compares)
    add_to_cart   one per order item
    checkout      one, 30 s to 2 min before the purchase
    purchase      one, at purchase_ts, carrying the real order_id

Each order also brings 0-4 anonymous browsing sessions (no customer, 1-6 page views of random
products, no purchase), placed in the hour before the purchase. Conversion funnels then have
a realistic denominator.

Everything is seeded by (seed, order_id): regenerating an order's sessions after a crash
yields byte-identical events with identical ids, so re-sends are exact duplicates that silver
can remove.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

SCHEMA_VERSION = 1
_faker = Faker()  # one instance; re-seeded per session so user agents stay deterministic
DEVICES = ("mobile", "desktop", "tablet")
DEVICE_WEIGHTS = (0.6, 0.32, 0.08)
NAMESPACE = uuid.UUID("5f2b8f0e-3a53-4f6c-9a1f-0b1c1c0de001")

Event = dict[str, Any]


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_unique_id: str | None
    purchase_ts: datetime
    product_ids: tuple[str, ...]


def _event_id(order_id: str, n: int) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{order_id}:{n}"))


@dataclass(frozen=True)
class Visit:
    session_id: str
    customer_unique_id: str | None
    device: str
    user_agent: str


PAGES = {"checkout": "/checkout", "purchase": "/thank-you"}


def _event(
    order: Order,
    n: int,
    visit: Visit,
    kind: str,
    ts: datetime,
    product_id: str | None = None,
    order_id: str | None = None,
) -> Event:
    return {
        "event_id": _event_id(order.order_id, n),
        "schema_version": SCHEMA_VERSION,
        "event_type": kind,
        "event_ts": ts.isoformat(timespec="seconds"),
        "session_id": visit.session_id,
        "customer_unique_id": visit.customer_unique_id,
        "device": visit.device,
        "user_agent": visit.user_agent,
        "page": f"/product/{product_id}" if product_id else PAGES[kind],
        "product_id": product_id,
        "order_id": order_id,
    }


def sessions_for(order: Order, *, seed: int, catalog: list[str]) -> list[Event]:
    """All clickstream events caused by one order, in event-time order."""
    rng = random.Random(f"{seed}:click:{order.order_id}")
    events: list[Event] = []

    def visit(customer: str | None) -> Visit:
        session_id = str(uuid.uuid5(NAMESPACE, f"{order.order_id}:session:{len(events)}"))
        _faker.seed_instance(f"{seed}:{session_id}")
        device = rng.choices(DEVICES, DEVICE_WEIGHTS)[0]
        return Visit(session_id, customer, device, _faker.user_agent())

    # Anonymous browsing that does not convert.
    for _ in range(rng.randint(0, 4)):
        v = visit(None)
        ts = order.purchase_ts - timedelta(minutes=rng.randint(5, 60))
        for _ in range(rng.randint(1, 6)):
            events.append(_event(order, len(events), v, "page_view", ts, rng.choice(catalog)))
            ts += timedelta(seconds=rng.randint(10, 180))

    # The session that ends in this order: plan the steps, then fit them before the purchase.
    v = visit(order.customer_unique_id)
    steps: list[tuple[str, str | None, timedelta]] = []
    offset = timedelta(0)
    for product in dict.fromkeys(order.product_ids):
        for _ in range(rng.randint(1, 3)):
            steps.append(("page_view", product, offset))
            offset += timedelta(seconds=rng.randint(10, 120))
    for product in order.product_ids:
        steps.append(("add_to_cart", product, offset))
        offset += timedelta(seconds=rng.randint(5, 60))
    lead = timedelta(minutes=rng.randint(5, 40))
    checkout_gap = timedelta(seconds=rng.randint(30, 120))
    # Start early enough that every step lands before checkout, however many items there are.
    start = order.purchase_ts - checkout_gap - max(lead, offset + timedelta(seconds=30))
    for kind, step_product, at in steps:
        events.append(_event(order, len(events), v, kind, start + at, step_product))
    events.append(_event(order, len(events), v, "checkout", order.purchase_ts - checkout_gap))
    events.append(
        _event(order, len(events), v, "purchase", order.purchase_ts, order_id=order.order_id)
    )
    # Stable sort on time alone: ties keep generation order, so checkout precedes purchase.
    events.sort(key=lambda e: e["event_ts"])
    return events
