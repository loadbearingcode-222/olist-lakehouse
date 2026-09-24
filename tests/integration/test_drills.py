"""Failure drills: break one component while data flows, and prove nothing was lost or doubled.

Traffic is a heartbeat: numbered rows written to olist.drill_heartbeat (an ops table the
publication carries like any other) every 50 ms, before, during and after the fault. Each drill:

    write heartbeats ── fault ── recover ── keep writing ── stop ── wait for bronze to catch up

Then two properties must hold:
- every heartbeat Postgres committed is in bronze, once per event (distinct seq = written);
- across every topic, landed rows (bronze + quarantine) = Kafka offsets, and no offset twice.

Kafka-level redelivery is allowed (Connect is at-least-once) and reported: a redelivered event
has a new offset and lands again, which is exactly what silver's dedup is for.

Every drill writes its evidence to automation/runs/m2-s3/<drill>.json (tracked in git).
Takes about 3 minutes per drill.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
EVIDENCE = REPO / "automation" / "runs" / "m2-s3"
LANDED_TOPICS = re.compile(r"^(olist\.olist\..+|clickstream\.events)$")
BEFORE_FAULT, AFTER_FAULT = 20.0, 30.0


def _env(key: str) -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise AssertionError(f".env has no {key}")


def _dsn() -> str:
    return (
        f"host=127.0.0.1 port=25432 user=olist dbname=olist password={_env('SOURCE_DB_PASSWORD')}"
    )


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=REPO, capture_output=True, check=True)


def docker_kill(service: str) -> None:
    subprocess.run(
        ["docker", "kill", "--signal", "KILL", f"olist-lakehouse-{service}-1"],
        capture_output=True,
        check=True,
    )


class Heartbeat:
    """Writes (drill, seq) rows every 50 ms on its own thread until stopped."""

    def __init__(self, drill: str) -> None:
        self.drill, self.written, self.errors = drill, 0, 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        with psycopg.connect(_dsn(), autocommit=True) as conn:
            while not self._stop.is_set():
                try:
                    conn.execute(
                        "INSERT INTO olist.drill_heartbeat (drill, seq) VALUES (%s, %s)",
                        [self.drill, self.written + 1],
                    )
                    self.written += 1
                except psycopg.Error:
                    self.errors += 1
                time.sleep(0.05)

    def __enter__(self) -> Heartbeat:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()


def kafka_offsets() -> dict[tuple[str, int], int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": "127.0.0.1:29092", "group.id": "drill-check"})
    try:
        counts = {}
        for name, meta in consumer.list_topics(timeout=15).topics.items():
            if LANDED_TOPICS.match(name):
                for partition in meta.partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(name, partition), timeout=15
                    )
                    counts[(name, partition)] = high - low
        return counts
    finally:
        consumer.close()


def landed(cursor: Any) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], int]]:
    union = " UNION ALL ".join(
        f"SELECT kafka_topic, kafka_partition, kafka_offset FROM lake.bronze.{t}"
        for t in ("cdc_events", "clickstream_events", "quarantine")
    )
    cursor.execute(
        "SELECT kafka_topic, kafka_partition, count(*), count(DISTINCT kafka_offset)"
        f" FROM ({union}) GROUP BY 1, 2"
    )
    rows, distinct = {}, {}
    for topic, partition, n, d in cursor.fetchall():
        rows[(topic, partition)], distinct[(topic, partition)] = n, d
    return rows, distinct


def heartbeat_in_bronze(cursor: Any, drill: str) -> tuple[int, int]:
    cursor.execute(
        "SELECT count(*), count(DISTINCT json_extract_scalar(payload, '$.after.seq'))"
        " FROM lake.bronze.cdc_events"
        " WHERE source_table = 'drill_heartbeat' AND op = 'c'"
        f" AND json_extract_scalar(payload, '$.after.drill') = '{drill}'"
    )
    rows, distinct = cursor.fetchall()[0]
    return int(rows), int(distinct)


def connector_running() -> bool:
    try:
        out = subprocess.run(
            ["curl", "-fsS", "http://127.0.0.1:28083/connectors/olist-postgres/status"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
    except OSError:
        return False
    return out.count('"state":"RUNNING"') >= 2


@pytest.fixture(scope="module")
def cursor() -> Iterator[Any]:
    try:
        socket.create_connection(("127.0.0.1", 28090), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Trino is not reachable on 127.0.0.1:28090")
        pytest.skip("the stack is not up")
    compose("run", "--rm", "cdc-setup")  # the drill table exists (idempotent)
    import trino

    yield trino.dbapi.connect(host="127.0.0.1", port=28090, user="drills").cursor()


def run_drill(cursor: Any, name: str, fault: Callable[[], None]) -> dict[str, Any]:
    drill = f"{name}-{int(time.time())}"
    started = datetime.now(UTC)
    with Heartbeat(drill) as beat:
        time.sleep(BEFORE_FAULT)
        faulted_at = time.monotonic()
        fault()
        time.sleep(AFTER_FAULT)
    written = beat.written

    # Recovery: every committed heartbeat is in bronze, and bronze has drained Kafka.
    deadline = time.monotonic() + 420
    while True:
        rows, distinct_events = heartbeat_in_bronze(cursor, drill)
        offsets = kafka_offsets()
        landed_rows, landed_distinct = landed(cursor)
        if distinct_events == written and landed_rows == offsets:
            break
        assert time.monotonic() < deadline, (
            f"{name}: not recovered after 7 min: heartbeat {distinct_events}/{written}, "
            f"landed {sum(landed_rows.values()):,} vs offsets {sum(offsets.values()):,}"
        )
        time.sleep(10)
    recovered_s = round(time.monotonic() - faulted_at, 1)

    evidence = {
        "drill": name,
        "run_id": drill,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "heartbeats_written": written,
        "heartbeat_write_errors": beat.errors,
        "heartbeat_events_in_bronze": distinct_events,
        "heartbeat_rows_in_bronze": rows,
        "redelivered_by_kafka": rows - distinct_events,
        "kafka_offsets_total": sum(offsets.values()),
        "landed_rows_total": sum(landed_rows.values()),
        "landed_distinct_offsets": sum(landed_distinct.values()),
        "seconds_from_fault_to_recovered": recovered_s,
        "lost_events": written - distinct_events,
        "offsets_landed_twice": sum(landed_rows.values()) - sum(landed_distinct.values()),
    }
    evidence["verdict"] = (
        "pass" if evidence["lost_events"] == 0 and evidence["offsets_landed_twice"] == 0 else "fail"
    )
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / f"{name}.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"\n{json.dumps(evidence, indent=1)}")
    return evidence


def test_kill_connect(cursor: Any) -> None:
    def fault() -> None:
        docker_kill("connect")
        time.sleep(10)
        compose("start", "connect")
        deadline = time.monotonic() + 180
        while not connector_running():
            assert time.monotonic() < deadline, "the connector never came back RUNNING"
            time.sleep(3)

    evidence = run_drill(cursor, "kill-connect", fault)
    assert evidence["verdict"] == "pass", evidence


def test_kill_spark(cursor: Any) -> None:
    def fault() -> None:
        docker_kill("bronze-stream")
        time.sleep(5)
        compose("start", "bronze-stream")

    evidence = run_drill(cursor, "kill-spark", fault)
    assert evidence["verdict"] == "pass", evidence


def test_restart_broker(cursor: Any) -> None:
    def fault() -> None:
        compose("restart", "kafka")

    evidence = run_drill(cursor, "restart-broker", fault)
    assert evidence["verdict"] == "pass", evidence
