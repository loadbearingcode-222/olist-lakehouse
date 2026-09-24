"""The FX DAG, backfilled through Airflow: every ECB business day of the period, and reruns
add nothing.

1. The expected days come from the API itself (one bulk request per window): the source of
   truth for "every ECB business day", never a remembered count.
2. bronze.fx_rates is emptied, then `airflow backfill create` runs the DAG once per day over
   2016-08-25 .. 2018-10-31 (798 runs, --max-active-runs 6).
3. When the backfill finishes, the stored dates must equal the expected dates exactly.
4. One day is re-run (`airflow tasks test`); the row count must not move.

Evidence: automation/runs/m4-s1/backfill.json. Takes 10-20 minutes.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from olist_lakehouse import fx

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
START, END = date(2016, 8, 25), date(2018, 10, 31)
EVIDENCE = REPO / "automation" / "runs" / "m4-s1" / "backfill.json"


def airflow(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return result.stdout


def stored_dates(cursor: Any) -> set[date]:
    cursor.execute(
        "SELECT rate_date FROM lake.bronze.fx_rates WHERE base = 'BRL' AND quote = 'USD'"
        f" AND rate_date BETWEEN DATE '{START}' AND DATE '{END}'"
    )
    return {row[0] for row in cursor.fetchall()}


def backfill_states() -> dict[str, int]:
    out = airflow("dags", "list-runs", "fx_rates_daily", "--output", "json")
    runs = json.loads(out[out.index("[") :]) if "[" in out else []
    states: dict[str, int] = {}
    for run in runs:
        if str(run.get("run_id", "")).startswith("backfill"):
            states[run["state"]] = states.get(run["state"], 0) + 1
    return states


@pytest.fixture(scope="module")
def cursor() -> Any:
    try:
        socket.create_connection(("127.0.0.1", 28082), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Airflow is not reachable on 127.0.0.1:28082")
        pytest.skip("Airflow is not up")
    import trino

    return trino.dbapi.connect(host="127.0.0.1", port=28090, user="fx-backfill").cursor()


def test_backfill_fills_every_business_day_and_reruns_add_nothing(cursor: Any) -> None:
    expected: set[date] = set()
    for window_start, window_end in fx.windows(START, END):
        expected |= {r.rate_date for r in fx.fetch(window_start, window_end, "BRL", "USD")}
    expected = {d for d in expected if START <= d <= END}

    cursor.execute("DELETE FROM lake.bronze.fx_rates")
    cursor.fetchall()
    assert stored_dates(cursor) == set()

    # A backfill starts from clean run history (earlier runs may carry other interval
    # boundaries). `dags delete` removes metadata only; the dag-processor re-parses the file.
    airflow("dags", "delete", "fx_rates_daily", "-y")
    deadline = time.monotonic() + 300
    while "fx_rates_daily" not in airflow("dags", "list"):
        assert time.monotonic() < deadline, "fx_rates_daily was not re-parsed within 5 min"
        time.sleep(10)
    airflow("dags", "unpause", "fx_rates_daily")
    started = time.monotonic()
    airflow(
        "backfill",
        "create",
        "--dag-id",
        "fx_rates_daily",
        "--from-date",
        START.isoformat(),
        "--to-date",
        END.isoformat(),
        "--max-active-runs",
        "6",
    )

    deadline = time.monotonic() + 40 * 60
    while True:
        states = backfill_states()
        runs = sum(states.values())
        if runs and states.get("success", 0) + states.get("failed", 0) == runs:
            break
        assert time.monotonic() < deadline, f"backfill not finished after 40 min: {states}"
        time.sleep(30)
    minutes = round((time.monotonic() - started) / 60, 1)

    stored = stored_dates(cursor)
    cursor.execute("SELECT count(*) FROM lake.bronze.fx_rates")
    rows_before = int(cursor.fetchall()[0][0])
    airflow("tasks", "test", "fx_rates_daily", "load_rate", "2017-03-15")
    cursor.execute("SELECT count(*) FROM lake.bronze.fx_rates")
    rows_after = int(cursor.fetchall()[0][0])

    evidence = {
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "range": [START.isoformat(), END.isoformat()],
        "backfill_runs": states,
        "backfill_minutes": minutes,
        "expected_business_days": len(expected),
        "stored_business_days": len(stored),
        "missing_days": sorted(d.isoformat() for d in expected - stored)[:20],
        "unexpected_days": sorted(d.isoformat() for d in stored - expected)[:20],
        "rerun_2017_03_15_rows_before": rows_before,
        "rerun_2017_03_15_rows_after": rows_after,
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_bytes((json.dumps(evidence, indent=2) + "\n").encode("utf-8"))
    print("\n" + json.dumps(evidence, indent=1))

    assert states.get("failed", 0) == 0, states
    assert stored == expected
    assert rows_after == rows_before
