"""The FX loader's pure parts: response parsing, request windows, the idempotent MERGE."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

from olist_lakehouse import fx

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "fx" / "brl_usd_2016-09-01_2016-09-12.json"
)


def body() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return data


def test_a_real_response_parses_to_business_days_only() -> None:
    rates = fx.parse(body(), "BRL", "USD")
    days = [r.rate_date for r in rates]
    assert days == sorted(days)
    assert date(2016, 9, 3) not in days and date(2016, 9, 4) not in days  # a weekend
    assert all(r.base == "BRL" and r.quote == "USD" for r in rates)
    assert all(isinstance(r.rate, Decimal) and 0 < r.rate < 1 for r in rates)


@pytest.mark.parametrize(
    "broken",
    [
        {"base": "EUR", "rates": {}},
        {"base": "BRL"},
        {"base": "BRL", "rates": {"2016-09-01": {"EUR": 0.28}}},
    ],
)
def test_an_unexpected_body_is_refused(broken: dict[str, object]) -> None:
    with pytest.raises(fx.FxError) as refused:
        fx.parse(broken, "BRL", "USD")
    assert refused.value.code == "FX-BAD-RESPONSE"


def test_windows_cover_the_range_exactly_once() -> None:
    parts = fx.windows(date(2016, 8, 25), date(2018, 10, 31))
    assert parts[0][0] == date(2016, 8, 25) and parts[-1][1] == date(2018, 10, 31)
    for (_, end), (start, _) in pairwise(parts):
        assert (start - end).days == 1
    assert all((end - start).days < 366 for start, end in parts)


def test_the_merge_only_inserts_new_days_and_updates_changed_rates() -> None:
    sql = fx.merge_sql(fx.parse(body(), "BRL", "USD"), "test")
    assert "MERGE INTO lake.bronze.fx_rates" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN MATCHED AND t.rate <> s.rate THEN UPDATE" in sql
    assert sql.count("DATE '2016-09-") == len(body()["rates"])
