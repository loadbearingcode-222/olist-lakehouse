"""BRL→USD reference rates from the European Central Bank (via Frankfurter) into bronze.

    python -m olist_lakehouse.fx --start 2016-08-25 --end 2018-10-31

Frankfurter (https://api.frankfurter.dev, free, no key) republishes the ECB's daily
reference rates, one per ECB business day (no weekends or TARGET holidays). Rows are merged
into `lake.bronze.fx_rates` on (rate_date, base, quote) through Trino, so a rerun or an
overlapping range adds nothing. Silver fills weekends and holidays with the last known rate.

Only rates dated INSIDE the requested range are written. Asked for a weekend day, the API
answers with Friday's rate. Writing it anyway made Friday's, Saturday's and Sunday's runs
MERGE the same row at the same moment, and Iceberg's optimistic concurrency then rejected all
but one (ICEBERG_COMMIT_ERROR, M4-S1). A commit that still loses a race is retried.

Refusals (exit 1):
    FX-HTTP          the API did not answer 200
    FX-BAD-RESPONSE  the body is not the expected shape (base, rates{date:{quote:rate}})
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

API = os.environ.get("FX_API", "https://api.frankfurter.dev/v1")
TABLE = "lake.bronze.fx_rates"
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    rate_date   date,
    base        varchar,
    quote       varchar,
    rate        decimal(18, 10),
    source      varchar,
    fetched_at  timestamp(6) with time zone
)
"""


class FxError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


@dataclass(frozen=True)
class Rate:
    rate_date: date
    base: str
    quote: str
    rate: Decimal


def parse(body: dict[str, Any], base: str, quote: str) -> list[Rate]:
    """The API's time-series body → one Rate per business day. Strict about shape."""
    if body.get("base") != base or not isinstance(body.get("rates"), dict):
        raise FxError(
            "FX-BAD-RESPONSE", f"expected base {base} and a rates object, got {body!r:.200}"
        )
    rates = []
    for day, quotes in sorted(body["rates"].items()):
        if not isinstance(quotes, dict) or quote not in quotes:
            raise FxError("FX-BAD-RESPONSE", f"{day}: no {quote} in {quotes!r}")
        rates.append(Rate(date.fromisoformat(day), base, quote, Decimal(str(quotes[quote]))))
    return rates


def windows(start: date, end: date, days: int = 366) -> list[tuple[date, date]]:
    """Split a range into requests of at most `days` days, keeping each response small."""
    out, cursor = [], start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=days - 1))
        out.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return out


def in_range(rates: list[Rate], start: date, end: date) -> list[Rate]:
    """Drop the rate the API substitutes for a non-business day (dated before `start`)."""
    return [r for r in rates if start <= r.rate_date <= end]


def fetch(start: date, end: date, base: str, quote: str) -> list[Rate]:
    url = f"{API}/{start.isoformat()}..{end.isoformat()}?base={base}&symbols={quote}"
    request = urllib.request.Request(url, headers={"User-Agent": "olist-lakehouse/fx"})
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise FxError("FX-HTTP", f"{url} answered {response.status}")
        return in_range(parse(json.loads(response.read()), base, quote), start, end)


def execute_with_retry(cursor: Any, sql: str, attempts: int = 5) -> None:
    """Run a write, retrying when Iceberg reports that a concurrent commit won the race."""
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute(sql)
            cursor.fetchall()
            return
        except Exception as exc:  # trino raises several classes; the name is what matters
            if "ICEBERG_COMMIT_ERROR" not in str(exc) or attempt == attempts:
                raise
            time.sleep(0.5 * 2**attempt)


def merge_sql(rates: list[Rate], source: str) -> str:
    values = ", ".join(
        f"(DATE '{r.rate_date.isoformat()}', '{r.base}', '{r.quote}', "
        f"CAST({r.rate} AS decimal(18, 10)))"
        for r in rates
    )
    return f"""
MERGE INTO {TABLE} AS t
USING (VALUES {values}) AS s (rate_date, base, quote, rate)
ON t.rate_date = s.rate_date AND t.base = s.base AND t.quote = s.quote
WHEN MATCHED AND t.rate <> s.rate THEN UPDATE SET rate = s.rate, fetched_at = current_timestamp
WHEN NOT MATCHED THEN INSERT (rate_date, base, quote, rate, source, fetched_at)
    VALUES (s.rate_date, s.base, s.quote, s.rate, '{source}', current_timestamp)
"""


def load(start: date, end: date, *, base: str = "BRL", quote: str = "USD") -> dict[str, int]:
    import trino

    host = os.environ.get("TRINO_HOST", "127.0.0.1")
    port = int(os.environ.get("TRINO_PORT", "28090"))
    cursor = trino.dbapi.connect(host=host, port=port, user="fx").cursor()
    cursor.execute(DDL)
    cursor.fetchall()
    fetched = 0
    for window_start, window_end in windows(start, end):
        rates = fetch(window_start, window_end, base, quote)
        fetched += len(rates)
        if rates:
            execute_with_retry(cursor, merge_sql(rates, "ECB via frankfurter.dev"))
    cursor.execute(
        f"SELECT count(*) FROM {TABLE} WHERE base = '{base}' AND quote = '{quote}'"
        f" AND rate_date BETWEEN DATE '{start}' AND DATE '{end}'"
    )
    stored = int(cursor.fetchall()[0][0])
    return {"fetched": fetched, "stored_in_range": stored}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    try:
        result = load(args.start, args.end)
    except FxError as exc:
        print(f"fx: REFUSED {exc}", file=sys.stderr)
        return 1
    print(
        f"fx: {result['fetched']} business-day rates fetched, {result['stored_in_range']} stored "
        f"for {args.start}..{args.end}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
