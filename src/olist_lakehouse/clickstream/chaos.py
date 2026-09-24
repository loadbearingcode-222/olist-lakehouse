"""The chaos switch for the clickstream: what can go wrong with events from browsers.

With rate p, each event independently carries one defect with probability p, decided purely
by (seed, event_id). The same event therefore gets the same defect after a restart.

    kind            what the event looks like                      what should catch it (M2)
    duplicate       sent twice, byte-identical, same event_id      dedup on event_id
    late            event_ts moved 3 hours into the past           the stream's watermark
    null_product    product_id is null on a product event          the GX gate
    schema_drift    `event_type` renamed `eventType`, version 2    the GX gate (missing field)

`null_product` only applies to page_view and add_to_cart, the events that need a product.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

KINDS = ("duplicate", "late", "null_product", "schema_drift")
LATE_BY = timedelta(hours=3)


@dataclass(frozen=True)
class Defect:
    kind: str
    detail: dict[str, Any]


def eligible(event: dict[str, Any]) -> tuple[str, ...]:
    kinds = ["duplicate", "late", "schema_drift"]
    if event["event_type"] in ("page_view", "add_to_cart"):
        kinds.append("null_product")
    return tuple(kinds)


def decide(
    event: dict[str, Any], *, seed: int, rate: float
) -> tuple[list[dict[str, Any]], Defect | None]:
    """Return the message(s) to send for `event` and the defect, if any."""
    if rate <= 0:
        return [event], None
    rng = random.Random(f"{seed}:click-chaos:{event['event_id']}")
    if rng.random() >= rate:
        return [event], None
    kind = rng.choice(eligible(event))

    if kind == "duplicate":
        return [event, dict(event)], Defect(kind, {})
    corrupted = dict(event)
    if kind == "late":
        original = corrupted["event_ts"]
        corrupted["event_ts"] = (datetime.fromisoformat(original) - LATE_BY).isoformat(
            timespec="seconds"
        )
        return [corrupted], Defect(kind, {"original": original, "injected": corrupted["event_ts"]})
    if kind == "null_product":
        original = corrupted["product_id"]
        corrupted["product_id"] = None
        return [corrupted], Defect(kind, {"original": original})
    # schema_drift: a client on a newer app version renames a field.
    corrupted["eventType"] = corrupted.pop("event_type")
    corrupted["schema_version"] = 2
    return [corrupted], Defect(kind, {"renamed": "event_type -> eventType"})
