"""clickstream.events on the running stack: keyed by session, purchases are real orders, and
every chaos defect in the ledger is visible in the topic at the configured rate.

The producer is paused (gracefully, so its last batch and ledger commit) while the topic is
read, and restarted afterwards if it was running.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg
import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
TOPIC = "clickstream.events"


def _env(key: str) -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise AssertionError(f".env has no {key}")


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", "--profile", "replay", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def read_topic() -> list[tuple[bytes, dict[str, Any]]]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:29092",
            "group.id": f"click-check-{time.time_ns()}",
            "enable.auto.commit": False,
        }
    )
    messages: list[tuple[bytes, dict[str, Any]]] = []
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            if high <= low:
                continue
            consumer.assign([TopicPartition(TOPIC, p, low)])
            position = low
            while position < high:
                for m in consumer.consume(num_messages=5000, timeout=5):
                    if m.error():
                        raise AssertionError(m.error())
                    position = m.offset() + 1
                    messages.append((m.key(), json.loads(m.value())))
                    if position >= high:
                        break
    finally:
        consumer.close()
    return messages


@pytest.fixture(scope="module")
def snapshot() -> tuple[
    list[tuple[bytes, dict[str, Any]]], dict[str, dict[str, Any]], set[str], float
]:
    try:
        socket.create_connection(("127.0.0.1", 29092), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Kafka is not reachable on 127.0.0.1:29092")
        pytest.skip("Kafka is not reachable")
    was_running = "clickstream" in compose("ps", "--status", "running", "--services")
    try:
        compose("stop", "clickstream")
        messages = read_topic()
        dsn = (
            f"host=127.0.0.1 port=25432 user=olist dbname=olist "
            f"password={_env('SOURCE_DB_PASSWORD')}"
        )
        with psycopg.connect(dsn) as conn:
            ledger = {
                f"{event_id}:{kind}": {"event_id": event_id, "kind": kind, "detail": detail}
                for event_id, kind, detail in conn.execute(
                    "SELECT event_id, kind, detail FROM replay.clickstream_chaos_ledger"
                )
            }
            orders = {r[0] for r in conn.execute("SELECT order_id FROM olist.orders")}
            row = conn.execute("SELECT chaos_rate FROM replay.clickstream_checkpoint").fetchone()
            rate = float(row[0]) if row else 0.0
    finally:
        if was_running:
            compose("start", "clickstream")
    assert messages, "clickstream.events is empty; is the producer running?"
    return messages, ledger, orders, rate


def test_every_message_is_keyed_by_its_session(snapshot: Any) -> None:
    messages, *_ = snapshot
    assert all(key.decode() == value["session_id"] for key, value in messages)


def test_every_purchase_is_a_real_order(snapshot: Any) -> None:
    messages, _, orders, _ = snapshot
    purchases = {
        v["order_id"] for _, v in messages if v.get("event_type", v.get("eventType")) == "purchase"
    }
    assert purchases and purchases <= orders, (
        f"{len(purchases - orders)} purchases without an order"
    )


def test_each_defect_in_the_ledger_is_visible_in_the_topic(snapshot: Any) -> None:
    messages, ledger, _, _ = snapshot
    by_kind: dict[str, set[str]] = {}
    for entry in ledger.values():
        by_kind.setdefault(entry["kind"], set()).add(entry["event_id"])

    occurrences = Counter(v["event_id"] for _, v in messages)
    seen = {v["event_id"]: v for _, v in messages}
    drifted = {v["event_id"] for _, v in messages if "eventType" in v}
    null_product = {
        v["event_id"]
        for _, v in messages
        if v.get("event_type") in ("page_view", "add_to_cart") and v["product_id"] is None
    }
    late = {
        e["event_id"]
        for e in ledger.values()
        if e["kind"] == "late" and seen[e["event_id"]]["event_ts"] == e["detail"]["injected"]
    }
    resent = {i for i, n in occurrences.items() if n > 1} - by_kind.get("duplicate", set())

    assert all(occurrences[i] >= 2 for i in by_kind.get("duplicate", set()))
    assert drifted == by_kind.get("schema_drift", set())
    assert null_product == by_kind.get("null_product", set())
    assert late == by_kind.get("late", set())
    print(
        f"\nledger {dict(Counter(e['kind'] for e in ledger.values()))}; "
        f"events {len(seen):,}; messages {len(messages):,}; re-sent after a crash {len(resent)}"
    )


def test_defects_appear_at_the_configured_rate(snapshot: Any) -> None:
    messages, ledger, _, rate = snapshot
    events = len({v["event_id"] for _, v in messages})
    observed = len(ledger) / events
    assert events > 5_000, "too few events to judge a rate"
    assert abs(observed - rate) <= 0.25 * rate, f"observed {observed:.4f}, configured {rate}"
    assert set(Counter(e["kind"] for e in ledger.values())) == {
        "duplicate",
        "late",
        "null_product",
        "schema_drift",
    }
