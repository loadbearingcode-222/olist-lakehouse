r"""Red team for the quality gate: plant one defect per rule and watch each be refused by name.

Runs the real `gate.judge` on crafted rows in a throwaway local Spark session. No table is
read or written, so the live lake and the ledger reconciliation stay untouched:

    .\make.ps1 gate-redteam      (or: docker compose run --rm --no-deps bronze-stream
        /opt/spark/bin/spark-submit /opt/app/src/olist_lakehouse/quality/redteam.py)

Prints one JSON object: for every planted row, the rule it was quarantined under (null =
passed), plus whether GX agreed with the gate on every rule. Exit 1 on any surprise.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from olist_lakehouse.quality.expect import Checker
from olist_lakehouse.quality.gate import judge

T0 = datetime(2018, 1, 1, 12)
MICROS = 1_514_808_000_000_000  # 2018-01-01 12:00 as Debezium epoch micros


def cdc(
    name: str, table: str, op: str, after: dict[str, Any], before: Any = None
) -> dict[str, Any]:
    payload = {"before": before, "after": after, "op": op, "source": {"table": table, "lsn": 1}}
    return {
        "plant": name,
        "kafka_topic": f"olist.olist.{table}",
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp": T0,
        "record_key": "{}",
        "payload": json.dumps(payload),
        "source_table": table,
        "op": op,
        "source_lsn": 1,
        "source_ts_ms": 0,
        "ingested_at": T0,
        "batch_id": 0,
    }


def click(name: str, event: dict[str, Any]) -> dict[str, Any]:
    event_type = event.get("event_type", event.get("eventType"))
    return {
        "plant": name,
        "kafka_topic": "clickstream.events",
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp": T0,
        "record_key": "s1",
        "payload": json.dumps(event),
        "event_id": event["event_id"],
        "event_type": event_type,
        "event_ts": T0,
        "schema_version": event.get("schema_version", 1),
        "ingested_at": T0,
        "batch_id": 0,
    }


ORDER = {
    "order_id": "o1",
    "customer_id": "c1",
    "order_status": "created",
    "purchase_ts": MICROS,
    "approved_at": None,
}
VIEW = {
    "event_id": "e",
    "event_type": "page_view",
    "session_id": "s1",
    "product_id": "p1",
    "event_ts": "2018-01-01T12:00:00",
    "schema_version": 1,
}

# (planted row, the rule that must refuse it; None = must pass)
CASES: list[tuple[dict[str, Any], str | None]] = [
    (cdc("clean item", "order_items", "c", {"order_id": "o1", "price": "10.00"}), None),
    (
        cdc("negative price", "order_items", "c", {"order_id": "o1", "price": "-10.00"}),
        "negative_price",
    ),
    (cdc("clean order", "orders", "c", ORDER), None),
    (cdc("null customer", "orders", "c", {**ORDER, "customer_id": None}), "null_customer_id"),
    (
        cdc(
            "null customer, later update",
            "orders",
            "u",
            {**ORDER, "customer_id": None},
            before={**ORDER, "customer_id": None},
        ),
        None,
    ),
    (
        cdc("misspelled status", "orders", "u", {**ORDER, "order_status": "shiped"}, ORDER),
        "unknown_status",
    ),
    (
        cdc(
            "approved before purchase",
            "orders",
            "u",
            {**ORDER, "order_status": "approved", "approved_at": MICROS - 86_400_000_000},
            ORDER,
        ),
        "approved_before_purchase",
    ),
    (
        cdc(
            "bad approval, unchanged later",
            "orders",
            "u",
            {**ORDER, "order_status": "shipped", "approved_at": MICROS - 86_400_000_000},
            {**ORDER, "order_status": "approved", "approved_at": MICROS - 86_400_000_000},
        ),
        None,
    ),
    (cdc("tombstone", "orders", "d", {}), None),
    (click("clean view", {**VIEW, "event_id": "e1"}), None),
    (click("null product", {**VIEW, "event_id": "e2", "product_id": None}), "null_product"),
    (
        click(
            "checkout without product",
            {**VIEW, "event_id": "e3", "event_type": "checkout", "product_id": None},
        ),
        None,
    ),
    (
        click(
            "schema drift",
            {
                **{k: v for k, v in VIEW.items() if k != "event_type"},
                "event_id": "e4",
                "eventType": "page_view",
                "schema_version": 2,
            },
        ),
        "missing_event_type",
    ),
    (
        click(
            "drift AND null product",
            {
                **{k: v for k, v in VIEW.items() if k != "event_type"},
                "event_id": "e5",
                "eventType": "page_view",
                "product_id": None,
            },
        ),
        "missing_event_type",
    ),  # first matching rule wins
]


def main() -> int:
    spark = SparkSession.builder.master("local[2]").appName("gate-redteam").getOrCreate()
    checker = Checker("spark")
    outcome: dict[str, Any] = {"cases": [], "gx_agrees": True}
    surprises = 0
    for source, prefix in (("cdc", "olist."), ("clickstream", "clickstream.")):
        cases = [(row, want) for row, want in CASES if row["kafka_topic"].startswith(prefix)]
        rows = [
            {k: v for k, v in row.items() if k != "plant"} | {"kafka_offset": i}
            for i, (row, _) in enumerate(cases)
        ]
        verdict = judge(spark.createDataFrame(rows), source, checker)
        got = {
            r["kafka_offset"]: r["rule"]
            for r in verdict.bad.select("kafka_offset", "rule").collect()
        }
        for i, (row, want) in enumerate(cases):
            ok = got.get(i) == want
            surprises += not ok
            outcome["cases"].append(
                {"plant": row["plant"], "want": want, "got": got.get(i), "ok": ok}
            )
        outcome["gx_agrees"] &= all(m["agree"] is not False for m in verdict.metrics)
        verdict.release()
    print(json.dumps(outcome, indent=1))
    spark.stop()
    return 1 if surprises or not outcome["gx_agrees"] else 0


if __name__ == "__main__":
    sys.exit(main())
