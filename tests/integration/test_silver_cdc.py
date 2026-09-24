"""The silver CDC merge on crafted envelopes: out of order, redelivered, deleted, incremental.

Runs the real dbt models against a throwaway bronze (`lake.fixture_bronze`) and writes a
throwaway silver (`lake.fixture_silver`), via the `bronze_schema` / `silver_schema` vars. The
live lake is not touched. Both schemas are dropped afterwards.

Run 1 (full refresh), orders:
    A: c@100 created, u@200 approved, but landed in REVERSE offset order  -> approved
    B: c@110 created, then the same c@110 redelivered at a new offset     -> one row, created
    C: c@120 created, d@220 delete                                        -> _is_deleted
Run 2 (incremental), new bronze rows:
    A: an old change u@150 'created' arrives late (a Connect redelivery)  -> still approved
    B: u@300 shipped                                                      -> shipped
    D: c@310 created (a new order)                                        -> created
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
DBT_DIR = REPO / "dbt" / "olist"
BRONZE, SILVER = "fixture_bronze", "fixture_silver"
T0 = 1_514_808_000_000_000  # 2018-01-01 12:00, Debezium epoch micros
offset = iter(range(1_000_000))


def order(order_id: str, status: str) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "customer_id": f"c-{order_id}",
        "order_status": status,
        "purchase_ts": T0,
        "approved_at": None,
        "delivered_carrier_at": None,
        "delivered_customer_at": None,
        "estimated_delivery_at": None,
        "updated_at": T0,
    }


def envelope(op: str, lsn: int, after: Any, before: Any = None) -> str:
    return json.dumps(
        {
            "before": before,
            "after": after,
            "op": op,
            "source": {"lsn": lsn, "ts_ms": 1_514_808_000_000, "table": "orders"},
        }
    )


def row(op: str, lsn: int, key: str, payload: str | None, ingested: str) -> str:
    p = "NULL" if payload is None else "'" + payload.replace("'", "''") + "'"
    return (
        f"('olist.olist.orders', 0, {next(offset)}, TIMESTAMP '2026-01-01 00:00:00', "
        f"'{{\"order_id\":\"{key}\"}}', {p}, 'orders', '{op}', {lsn}, 0, "
        f"TIMESTAMP '{ingested}', 0)"
    )


def insert(cursor: Any, rows: list[str]) -> None:
    cursor.execute(f"INSERT INTO lake.{BRONZE}.cdc_events VALUES {', '.join(rows)}")
    cursor.fetchall()


def dbt(*args: str) -> None:
    env = {**os.environ, "DBT_PROJECT_DIR": str(DBT_DIR), "DBT_PROFILES_DIR": str(DBT_DIR)}
    vars_ = json.dumps({"bronze_schema": BRONZE, "silver_schema": SILVER})
    result = subprocess.run(
        ["uv", "run", "--group", "dbt", "dbt", *args, "--select", "orders", "--vars", vars_],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    assert result.returncode == 0, result.stdout[-3000:]


def state(cursor: Any) -> dict[str, tuple[str, int, bool]]:
    cursor.execute(
        f"SELECT order_id, order_status, _source_lsn, _is_deleted FROM lake.{SILVER}.orders"
    )
    return {r[0]: (r[1], r[2], r[3]) for r in cursor.fetchall()}


@pytest.fixture(scope="module")
def cursor() -> Iterator[Any]:
    try:
        socket.create_connection(("127.0.0.1", 28090), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Trino is not reachable on 127.0.0.1:28090")
        pytest.skip("Trino is not reachable")
    import trino

    cur = trino.dbapi.connect(host="127.0.0.1", port=28090, user="silver-fixture").cursor()

    def run(sql: str) -> None:
        cur.execute(sql)
        cur.fetchall()

    for schema in (SILVER, BRONZE):
        run(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    run(f"DROP TABLE IF EXISTS lake.{BRONZE}.cdc_events")
    run(f"DROP TABLE IF EXISTS lake.{SILVER}.orders")
    run(
        f"CREATE TABLE lake.{BRONZE}.cdc_events AS SELECT * FROM lake.bronze.cdc_events WITH NO DATA"
    )
    yield cur
    run(f"DROP TABLE IF EXISTS lake.{SILVER}.orders")
    run(f"DROP TABLE IF EXISTS lake.{BRONZE}.cdc_events")
    run(f"DROP SCHEMA IF EXISTS lake.{SILVER}")
    run(f"DROP SCHEMA IF EXISTS lake.{BRONZE}")


def test_lsn_order_redelivery_delete_and_incremental(cursor: Any) -> None:
    first = "2026-01-01 10:00:00"
    insert(
        cursor,
        [
            # A: the newer change lands FIRST (lower offset), the older one after it.
            row(
                "u",
                200,
                "A",
                envelope("u", 200, order("A", "approved"), order("A", "created")),
                first,
            ),
            row("c", 100, "A", envelope("c", 100, order("A", "created")), first),
            # B: the same change twice (Connect redelivery at a new offset).
            row("c", 110, "B", envelope("c", 110, order("B", "created")), first),
            row("c", 110, "B", envelope("c", 110, order("B", "created")), first),
            # C: created, then deleted, then its tombstone.
            row("c", 120, "C", envelope("c", 120, order("C", "created")), first),
            row("d", 220, "C", envelope("d", 220, None, order("C", "created")), first),
            row("d", 220, "C", None, first),
        ],
    )
    dbt("run", "--full-refresh")
    assert state(cursor) == {
        "A": ("approved", 200, False),
        "B": ("created", 110, False),
        "C": ("created", 220, True),  # the delete keeps the row's last values, flagged
    }

    later = "2026-01-01 10:20:00"
    insert(
        cursor,
        [
            # A: an OLD change arrives late; it must not roll A back.
            row(
                "u",
                150,
                "A",
                envelope("u", 150, order("A", "created"), order("A", "created")),
                later,
            ),
            row(
                "u",
                300,
                "B",
                envelope("u", 300, order("B", "shipped"), order("B", "created")),
                later,
            ),
            row("c", 310, "D", envelope("c", 310, order("D", "created")), later),
        ],
    )
    dbt("run")  # incremental
    assert state(cursor) == {
        "A": ("approved", 200, False),
        "B": ("shipped", 300, False),
        "C": ("created", 220, True),
        "D": ("created", 310, False),
    }
