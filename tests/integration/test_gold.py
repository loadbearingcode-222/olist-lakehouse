"""Gold against its sources of truth, never against remembered numbers.

- SCD2: the versions in dim_seller / dim_product are exactly the replayer's mutations, derived
  from its own deterministic timeline (same data, same seed): which entities changed, and
  how many times.
- Money: SUM(fct_payments.payment_value_brl) equals the raw CSV, recomputed here to the cent.
- FX: every order's rate is the ECB rate published on or before its purchase day, and every
  USD amount is the BRL amount times that rate, rounded to the cent.
"""

from __future__ import annotations

import csv
import os
import socket
from collections import Counter
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from olist_lakehouse.replayer import source, timeline

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
RAW = REPO / "data" / "raw" / "olist"


@pytest.fixture(scope="module")
def cursor() -> Iterator[Any]:
    try:
        socket.create_connection(("127.0.0.1", 28090), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Trino is not reachable on 127.0.0.1:28090")
        pytest.skip("Trino is not reachable")
    import trino

    yield trino.dbapi.connect(host="127.0.0.1", port=28090, user="gold-check").cursor()


def query(cursor: Any, sql: str) -> list[Any]:
    cursor.execute(sql)
    return list(cursor.fetchall())


@pytest.fixture(scope="module")
def mutations() -> Counter[tuple[str, str]]:
    """(table, id) → number of UPDATE events the replayer applied to it."""
    seed = int(os.environ.get("REPLAY_SEED", 42))
    events = timeline.build(source.load(RAW), seed=seed)
    return Counter(
        (e.table, next(iter(e.key.values())))
        for e in events
        if e.op == "update" and e.table in ("sellers", "products")
    )


@pytest.mark.parametrize(
    ("dim", "table", "key"),
    [("dim_seller", "sellers", "seller_id"), ("dim_product", "products", "product_id")],
)
def test_scd2_versions_are_exactly_the_replayed_mutations(
    cursor: Any, mutations: Counter[tuple[str, str]], dim: str, table: str, key: str
) -> None:
    rows = query(cursor, f"SELECT {key}, count(*) FROM lake.gold.{dim} GROUP BY 1")
    versions = {k: n for k, n in rows}
    expected_changed = {k: n for (t, k), n in mutations.items() if t == table}
    assert {k for k, n in versions.items() if n > 1} == set(expected_changed)
    assert all(versions[k] == 1 + n for k, n in expected_changed.items())
    assert sum(versions.values()) == len(versions) + sum(expected_changed.values())
    print(
        f"\n{dim}: {len(versions):,} entities, {sum(versions.values()):,} versions, "
        f"{len(expected_changed):,} changed"
    )


def test_gold_payments_equal_the_raw_csv_to_the_cent(cursor: Any) -> None:
    with (RAW / "olist_order_payments_dataset.csv").open(newline="", encoding="utf-8") as f:
        csv_total = sum(Decimal(r["payment_value"]) for r in csv.DictReader(f))
    gold_total = Decimal(
        str(query(cursor, "SELECT sum(payment_value_brl) FROM lake.gold.fct_payments")[0][0])
    )
    print(f"\npayments: csv {csv_total:,} BRL, gold {gold_total:,} BRL")
    assert abs(gold_total - csv_total) <= Decimal("0.01")


def test_usd_uses_the_ecb_rate_of_the_purchase_day(cursor: Any) -> None:
    wrong_rate = query(
        cursor,
        """
        SELECT count(*) FROM lake.gold.fct_orders AS o
        WHERE o.fx_brl_usd <> (
            SELECT max_by(r.rate, r.rate_date) FROM lake.bronze.fx_rates AS r
            WHERE r.base = 'BRL' AND r.quote = 'USD'
              AND r.rate_date <= CAST(o.purchase_ts AS date))
    """,
    )[0][0]
    wrong_amount = query(
        cursor,
        """
        SELECT count(*) FROM lake.gold.fct_orders
        WHERE payment_value_usd <> CAST(round(payment_value_brl * fx_brl_usd, 2) AS decimal(14, 2))
    """,
    )[0][0]
    missing = query(cursor, "SELECT count(*) FROM lake.gold.fct_orders WHERE fx_brl_usd IS NULL")[
        0
    ][0]
    assert (wrong_rate, wrong_amount, missing) == (0, 0, 0)
