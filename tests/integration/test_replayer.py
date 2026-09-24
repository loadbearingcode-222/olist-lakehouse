"""The replayer against a real Postgres: end state, resume without duplicates, chaos ledger.

Each test gets a fresh database (`replay_it`) on the running postgres-source, built from the
same init SQL the container uses, and replays the hazard fixtures at full speed.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from olist_lakehouse.replayer import __main__ as replayer
from olist_lakehouse.replayer import chaos, source

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "olist"
SCHEMA_SQL = REPO / "docker" / "postgres-source" / "10-schema.sql"
# 127.0.0.1, not localhost: on Windows libpq tries ::1 first and stalls (gotcha #27).
HOST, PORT, DB = "127.0.0.1", 25432, "replay_it"


def _password() -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("SOURCE_DB_PASSWORD="):
            return line.split("=", 1)[1]
    raise AssertionError(".env has no SOURCE_DB_PASSWORD; run .\\make.ps1 env")


def _dsn(database: str) -> str:
    return f"host={HOST} port={PORT} user=olist password={_password()} dbname={database}"


@pytest.fixture
def dsn() -> Iterator[str]:
    try:
        socket.create_connection((HOST, PORT), timeout=2).close()
    except OSError:
        message = f"postgres-source is not reachable on {HOST}:{PORT}; run .\\make.ps1 up"
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail(message)
        pytest.skip(message)
    with psycopg.connect(_dsn("olist"), autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {DB}")
    with psycopg.connect(_dsn(DB), autocommit=True) as conn:
        conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))  # type: ignore[arg-type,unused-ignore]
    yield _dsn(DB)
    with psycopg.connect(_dsn("olist"), autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")


def replay(dsn: str, *extra: str) -> int:
    args = ["--dsn", dsn, "--data-dir", str(FIXTURES), "--speed", "0", "--report-every", "999"]
    return replayer.main([*args, *extra])


def scalar(dsn: str, query: str) -> object:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(query).fetchone()  # type: ignore[arg-type,unused-ignore]
        return row[0] if row else None


def test_a_full_replay_ends_exactly_as_the_csv(dsn: str) -> None:
    assert replay(dsn) == 0
    assert replay(dsn, "--verify") == 0
    with psycopg.connect(dsn) as conn:
        rows: dict[str, str] = dict(
            conn.execute("SELECT order_id, order_status FROM olist.orders").fetchall()
        )
    expected = {o["order_id"]: o["order_status"] for o in source.load(FIXTURES).orders}
    assert rows == expected


def test_a_stopped_replay_resumes_without_duplicates(dsn: str) -> None:
    assert replay(dsn, "--max-events", "5") == 0
    assert scalar(dsn, "SELECT last_seq FROM replay.checkpoint") == 4
    assert replay(dsn, "--verify") == 0  # a partial replay is internally consistent
    assert replay(dsn) == 0  # resumes at event 5; a duplicate insert would raise
    assert replay(dsn, "--verify") == 0
    assert replay(dsn) == 0  # a finished replay is a no-op


def test_every_injected_defect_is_in_the_chaos_ledger(dsn: str) -> None:
    assert replay(dsn, "--chaos-rate", "1.0", "--max-events", "7") == 0
    assert replay(dsn, "--chaos-rate", "1.0") == 0
    assert replay(dsn, "--chaos-rate", "1.0", "--verify") == 0
    kinds = str(scalar(dsn, "SELECT string_agg(DISTINCT kind, ',') FROM replay.chaos_ledger"))
    # At rate 1.0 every order insert and item insert is corrupted; updates pick one of two kinds.
    assert {"negative_price", "null_customer_id"} <= set(kinds.split(",")) <= set(chaos.KINDS)
    assert scalar(dsn, "SELECT count(*) FROM olist.order_items WHERE price < 0") == scalar(
        dsn, "SELECT count(*) FROM replay.chaos_ledger WHERE kind = 'negative_price'"
    )
    # verify reads the rate from the checkpoint, not from the command line...
    assert replay(dsn, "--chaos-rate", "0", "--verify") == 0


def test_resuming_with_another_chaos_rate_is_refused(
    dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert replay(dsn, "--chaos-rate", "0.5", "--max-events", "3") == 0
    assert replay(dsn, "--chaos-rate", "0") == 1
    assert "REPLAY-CHAOS-RATE-CHANGED" in capsys.readouterr().err


def test_a_database_replayed_with_another_seed_is_refused(
    dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert replay(dsn, "--seed", "1", "--max-events", "3") == 0
    assert replay(dsn, "--seed", "2") == 1
    assert "REPLAY-FINGERPRINT-MISMATCH" in capsys.readouterr().err


def test_verify_notices_a_row_the_replay_did_not_write(
    dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Red team: plant a defect behind the replayer's back and watch verify refuse."""
    assert replay(dsn, "--chaos-rate", "1.0") == 0
    with psycopg.connect(dsn) as conn:
        conn.execute("DELETE FROM olist.order_reviews WHERE order_id LIKE '%2'")
        conn.execute(
            "DELETE FROM replay.chaos_ledger WHERE seq = (SELECT max(seq) FROM replay.chaos_ledger)"
        )
    assert replay(dsn, "--verify") == 1
    err = capsys.readouterr().err
    assert "REPLAY-VERIFY-MISMATCH" in err
    assert "order_reviews" in err and "chaos_ledger" in err
