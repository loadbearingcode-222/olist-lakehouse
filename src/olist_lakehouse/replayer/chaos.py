"""The chaos switch for the database path: which events carry a defect, and which defect.

With CHAOS_RATE = p, each ELIGIBLE event carries a defect with probability p. The decision is
a pure function of (seed, sequence number, event), so a resumed replay makes exactly the same
decisions as an uninterrupted one. Every defect is written to `replay.chaos_ledger` in the
same transaction as the defect itself; the quality gate (M2-S2) reconciles against it.

    kind                        eligible event                  defect
    negative_price              INSERT order_items              price → -price
    null_customer_id            INSERT orders                   customer_id → NULL
    unknown_status              UPDATE orders (status change)   status → a misspelling
    approved_before_purchase    UPDATE orders (approval)        approved_at → purchase - 1 day

The Postgres schema lets all four through on purpose (a real app has gaps like these).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

from olist_lakehouse.replayer.timeline import Event

KINDS = ("negative_price", "null_customer_id", "unknown_status", "approved_before_purchase")

MISSPELLINGS = {
    "approved": "aproved",
    "shipped": "shiped",
    "delivered": "delivred",
    "canceled": "cancelled",
    "unavailable": "unavailble",
    "invoiced": "invoced",
    "processing": "procesing",
    "created": "crated",
}


@dataclass(frozen=True)
class Defect:
    kind: str
    column: str
    original: Any
    injected: Any


def eligible(event: Event) -> tuple[str, ...]:
    if event.table == "order_items" and event.op == "insert":
        return ("negative_price",)
    if event.table == "orders" and event.op == "insert":
        return ("null_customer_id",)
    if event.table == "orders" and event.op == "update":
        kinds = ["unknown_status"] if "order_status" in event.values else []
        if "approved_at" in event.values:
            kinds.append("approved_before_purchase")
        return tuple(kinds)
    return ()


def decide(event: Event, *, seq: int, seed: int, rate: float) -> tuple[Event, Defect | None]:
    """Return the event to write (possibly corrupted) and the defect, if any."""
    kinds = eligible(event)
    if not kinds or rate <= 0:
        return event, None
    rng = random.Random(f"{seed}:chaos:{seq}")
    if rng.random() >= rate:
        return event, None
    kind = rng.choice(kinds)

    values = dict(event.values)
    if kind == "negative_price":
        column, new = "price", -values["price"]
    elif kind == "null_customer_id":
        column, new = "customer_id", None
    elif kind == "unknown_status":
        status = values["order_status"]
        column, new = "order_status", MISSPELLINGS.get(status, status.upper())
    else:  # approved_before_purchase
        column, new = "approved_at", event.context["purchase_ts"] - timedelta(days=1)

    defect = Defect(kind=kind, column=column, original=values[column], injected=new)
    values[column] = new
    return replace(event, values=values), defect
