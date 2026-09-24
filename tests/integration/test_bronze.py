"""Bronze is exactly-once: after a hard kill of the streaming job mid-flight, every Kafka
message is in bronze once, no more and no fewer.

Sequence: SIGKILL the job while the producers are writing, start it again, pause the producers,
wait until bronze has caught up with Kafka, then compare per (topic, partition):

    bronze rows == Kafka high watermark - low watermark      (nothing lost, nothing extra)
    bronze rows == distinct offsets in bronze                (nothing twice)

Producers are restarted afterwards if they were running. Takes 2-4 minutes.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
TABLES = {
    "lake.bronze.cdc_events": r"^olist\.olist\.",
    "lake.bronze.clickstream_events": r"^clickstream\.events$",
}


def compose(*args: str) -> str:
    return subprocess.run(
        ["docker", "compose", "--profile", "replay", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def kafka_counts() -> dict[tuple[str, int], int]:
    import re

    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": "127.0.0.1:29092", "group.id": "bronze-check"})
    try:
        counts = {}
        topics = consumer.list_topics(timeout=10).topics
        for name, meta in topics.items():
            if not any(re.search(p, name) for p in TABLES.values()):
                continue
            for partition in meta.partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(name, partition), timeout=10
                )
                counts[(name, partition)] = high - low
        return counts
    finally:
        consumer.close()


def bronze_counts(cursor: Any) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], int]]:
    rows, distinct = {}, {}
    for table in TABLES:
        cursor.execute(
            f"SELECT kafka_topic, kafka_partition, count(*), count(DISTINCT kafka_offset)"
            f" FROM {table} GROUP BY 1, 2"
        )
        for topic, partition, n, d in cursor.fetchall():
            rows[(topic, partition)] = n
            distinct[(topic, partition)] = d
    return rows, distinct


@pytest.fixture(scope="module")
def cursor() -> Any:
    try:
        socket.create_connection(("127.0.0.1", 28090), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Trino is not reachable on 127.0.0.1:28090")
        pytest.skip("Trino is not reachable")
    import trino

    return trino.dbapi.connect(host="127.0.0.1", port=28090, user="bronze-check").cursor()


def test_a_hard_kill_neither_loses_nor_duplicates(cursor: Any) -> None:
    running = compose("ps", "--status", "running", "--services").split()
    producers = [s for s in ("replayer", "clickstream") if s in running]
    assert "bronze-stream" in running, "start it first: docker compose up -d bronze-stream"
    try:
        if producers:
            time.sleep(15)  # let a micro-batch get under way
        subprocess.run(
            ["docker", "kill", "--signal", "KILL", "olist-lakehouse-bronze-stream-1"],
            check=True,
            capture_output=True,
        )
        time.sleep(5)
        compose("start", "bronze-stream")
        time.sleep(20)
        if producers:
            compose("stop", *producers)

        deadline = time.monotonic() + 300
        while True:
            expected = kafka_counts()
            rows, distinct = bronze_counts(cursor)
            if rows == expected:
                break
            if time.monotonic() > deadline:
                missing = {
                    k: (expected.get(k), rows.get(k))
                    for k in expected
                    if expected.get(k) != rows.get(k)
                }
                raise AssertionError(f"bronze never caught up: {missing}")
            time.sleep(10)
    finally:
        if producers:
            compose("start", *producers)

    assert rows == distinct, "some Kafka offset is stored twice"
    total = sum(rows.values())
    print(f"\nbronze rows {total:,} across {len(rows)} partitions = Kafka offsets, all distinct")
