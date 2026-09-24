"""CDC completeness: over a window, Kafka holds exactly the changes Postgres made. No loss,
even when Kafka Connect restarts in the middle.

Method (a fixed window, measured from both ends):
1. pause the replayer and wait for the CDC topics to go quiet;
2. read Postgres's own counters (pg_stat_user_tables: n_tup_ins / n_tup_upd / n_tup_del) and
   every CDC topic's end offsets;
3. replay for a while, restarting Kafka Connect half-way through;
4. pause again, wait for quiet, read both ends again;
5. consume exactly the messages between the two offset snapshots and count them per table by
   Debezium op (c / u / d), de-duplicated by (lsn, op, key). Tombstones are not changes.

Postgres's deltas must equal the de-duplicated counts. Duplicates are allowed and reported:
Connect delivers at least once, so a restart may re-send events; bronze removes them (M2-S1).

This test drives the stack (stops/starts the replayer, restarts Connect) and takes ~2 minutes.
It leaves the replayer running if it found it running.
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
TABLES = (
    "product_category_translation",
    "zip_geolocation",
    "sellers",
    "products",
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
)
TOPICS = [f"olist.olist.{t}" for t in TABLES]
KAFKA = "127.0.0.1:29092"  # IPv4 on purpose (gotcha #27)
PG = ("127.0.0.1", 25432)
# Wall seconds of replay on each side of the Connect restart (longer = more deletes seen).
REPLAY_SECONDS = float(os.environ.get("CDC_WINDOW_SECONDS", 45))


def _env(key: str) -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise AssertionError(f".env has no {key}")


def compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "--profile", "replay", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def pg_counters() -> dict[str, tuple[int, int, int]]:
    dsn = f"host={PG[0]} port={PG[1]} user=olist dbname=olist password={_env('SOURCE_DB_PASSWORD')}"
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables"
            " WHERE schemaname = 'olist'"
        ).fetchall()
    return {name: (int(i), int(u), int(d)) for name, i, u, d in rows}


def end_offsets(consumer: Any) -> dict[tuple[str, int], int]:
    from confluent_kafka import TopicPartition

    offsets = {}
    metadata = consumer.list_topics(timeout=10)
    for topic in TOPICS:
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            offsets[(topic, partition)] = high
    return offsets


def wait_until_quiet(
    consumer: Any, quiet: float = 15, timeout: float = 240
) -> dict[tuple[str, int], int]:
    deadline = time.monotonic() + timeout
    last, since = end_offsets(consumer), time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(3)
        now = end_offsets(consumer)
        if now != last:
            last, since = now, time.monotonic()
        elif time.monotonic() - since >= quiet:
            return now
    raise AssertionError("CDC topics never went quiet; is Connect running?")


def count_window(
    consumer: Any, start: dict[tuple[str, int], int], end: dict[tuple[str, int], int]
) -> tuple[dict[str, Counter[str]], int]:
    from confluent_kafka import TopicPartition

    counts: dict[str, Counter[str]] = {t: Counter() for t in TABLES}
    seen: set[tuple[str, int, str, str]] = set()
    duplicates = 0
    for (topic, partition), stop in end.items():
        begin = start.get((topic, partition), 0)
        if stop <= begin:
            continue
        consumer.assign([TopicPartition(topic, partition, begin)])
        position = begin
        while position < stop:
            for message in consumer.consume(num_messages=1000, timeout=5):
                if message.error():
                    raise AssertionError(message.error())
                if message.offset() >= stop:
                    position = stop
                    break
                position = message.offset() + 1
                if message.value() is None:
                    continue  # tombstone after a delete: not a change
                event = json.loads(message.value())
                table = event["source"]["table"]
                identity = (table, int(event["source"]["lsn"]), event["op"], message.key().decode())
                if identity in seen:
                    duplicates += 1
                    continue
                seen.add(identity)
                counts[table][event["op"]] += 1
    return counts, duplicates


@pytest.fixture
def consumer() -> Any:
    try:
        socket.create_connection(("127.0.0.1", 29092), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Kafka is not reachable on 127.0.0.1:29092; run .\\make.ps1 up")
        pytest.skip("Kafka is not reachable")
    from confluent_kafka import Consumer

    c = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": f"cdc-reconcile-{time.time_ns()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    yield c
    c.close()


def test_kafka_holds_exactly_the_changes_postgres_made(consumer: Any) -> None:
    was_running = "replayer" in compose("ps", "--status", "running", "--services")
    try:
        compose("stop", "replayer")
        before_offsets = wait_until_quiet(consumer)
        before = pg_counters()

        compose("start", "replayer")
        time.sleep(REPLAY_SECONDS)
        compose("restart", "connect")  # mid-window: nothing may be lost
        time.sleep(REPLAY_SECONDS)
        compose("stop", "replayer")

        after_offsets = wait_until_quiet(consumer)
        after = pg_counters()
    finally:
        if was_running:
            compose("start", "replayer")

    counts, duplicates = count_window(consumer, before_offsets, after_offsets)
    report = []
    for table in TABLES:
        ins, upd, dele = (a - b for a, b in zip(after[table], before[table], strict=True))
        got = counts[table]
        report.append(
            f"{table:<30} pg ins/upd/del {ins}/{upd}/{dele}   "
            f"kafka c/u/d {got['c']}/{got['u']}/{got['d']}"
        )
        assert (got["c"], got["u"], got["d"]) == (ins, upd, dele), "\n".join(report)
        assert got["r"] == 0, f"{table}: snapshot reads inside a streaming window"
    total = sum(sum(c.values()) for c in counts.values())
    assert total > 0, "the window captured no changes; did the replayer run?"
    print("\n" + "\n".join(report) + f"\nchanges {total}, duplicates removed {duplicates}")
