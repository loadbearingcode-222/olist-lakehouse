"""The quality gate reconciles with the chaos ledgers, rule by rule, and GX agrees with it.

Meaningful on a stack whose replay started AFTER Connect was registered (a fresh `reset`),
so every change was streamed rather than snapshotted: then each injected defect produced
exactly one quarantinable message.

    quarantine, distinct messages per rule == chaos ledger entries of the rule's kind

CDC messages are counted distinct by (table, lsn, key): Connect delivers at least once, so a
restart may repeat a message at a new offset. Clickstream rows are distinct by event_id: the
`duplicate` defect re-sends a whole event, and only one copy may count.

The producers are paused while both sides are read, and bronze is allowed to drain Kafka
first. Producers that were running are restarted afterwards.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg
import pytest

from olist_lakehouse.quality.rules import RULES

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]


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


def landed(cursor: Any) -> int:
    cursor.execute(
        "SELECT (SELECT count(*) FROM lake.bronze.cdc_events)"
        " + (SELECT count(*) FROM lake.bronze.clickstream_events)"
        " + (SELECT count(*) FROM lake.bronze.quarantine)"
    )
    return int(cursor.fetchall()[0][0])


@pytest.fixture(scope="module")
def sides() -> tuple[dict[str, int], dict[str, int], list[tuple[Any, ...]]]:
    try:
        socket.create_connection(("127.0.0.1", 28090), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Trino is not reachable on 127.0.0.1:28090")
        pytest.skip("Trino is not reachable")
    import trino

    cursor = trino.dbapi.connect(host="127.0.0.1", port=28090, user="gate-check").cursor()
    running = compose("ps", "--status", "running", "--services").split()
    producers = [s for s in ("replayer", "clickstream") if s in running]
    try:
        if producers:
            compose("stop", *producers)
        # Let bronze drain Kafka: the landed count must hold still for 60 s.
        last, since, deadline = landed(cursor), time.monotonic(), time.monotonic() + 600
        while time.monotonic() - since < 60:
            assert time.monotonic() < deadline, "bronze never caught up"
            time.sleep(10)
            now = landed(cursor)
            if now != last:
                last, since = now, time.monotonic()

        cursor.execute(
            "SELECT rule, count(DISTINCT CASE WHEN source = 'cdc'"
            "  THEN concat(source_table, ':', CAST(source_lsn AS varchar), ':', record_key)"
            "  ELSE event_id END)"
            " FROM lake.bronze.quarantine GROUP BY rule"
        )
        quarantine = {rule: int(n) for rule, n in cursor.fetchall()}
        cursor.execute(
            "SELECT source, batch_id, rule, gate_violations, gx_unexpected"
            " FROM lake.bronze.quality_metrics WHERE kind = 'gate' AND NOT agree"
        )
        disagreements = list(cursor.fetchall())

        dsn = (
            "host=127.0.0.1 port=25432 user=olist dbname=olist "
            f"password={_env('SOURCE_DB_PASSWORD')}"
        )
        with psycopg.connect(dsn) as conn:
            ledger: dict[str, int] = dict(
                conn.execute("SELECT kind, count(*) FROM replay.chaos_ledger GROUP BY 1").fetchall()
            )
            ledger.update(
                conn.execute(
                    "SELECT kind, count(DISTINCT event_id) FROM replay.clickstream_chaos_ledger"
                    " GROUP BY 1"
                ).fetchall()
            )
    finally:
        if producers:
            compose("start", *producers)
    return quarantine, {k: int(v) for k, v in ledger.items()}, disagreements


def test_quarantine_counts_per_rule_equal_the_chaos_ledger(sides: Any) -> None:
    quarantine, ledger, _ = sides
    report = [
        f"{r.name:<26} quarantined {quarantine.get(r.name, 0):>7,}   "
        f"ledger[{r.chaos_kind}] {ledger.get(r.chaos_kind, 0):>7,}"
        for r in RULES
    ]
    print("\n" + "\n".join(report))
    for rule in RULES:
        assert quarantine.get(rule.name, 0) == ledger.get(rule.chaos_kind, 0), "\n".join(report)
    assert all(quarantine.get(r.name, 0) > 0 for r in RULES), "a rule never fired; is chaos on?"


def test_great_expectations_agrees_with_the_gate_on_every_batch(sides: Any) -> None:
    _, _, disagreements = sides
    assert disagreements == [], f"gate and GX disagree: {disagreements[:10]}"
