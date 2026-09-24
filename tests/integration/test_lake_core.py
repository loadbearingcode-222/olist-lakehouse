"""The lake core round-trips: Trino → Lakekeeper (Iceberg REST) → MinIO and back.

Needs the stack up (`.\\make.ps1 up`). Run with `.\\make.ps1 test-integration`, which sets
OLIST_STACK_REQUIRED=1 so an unreachable stack FAILS instead of skipping.
"""

from __future__ import annotations

import os
import socket
import subprocess
from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = pytest.mark.integration

TRINO_HOST, TRINO_PORT = "localhost", 28090
SCHEMA = "lake.smoke"
TABLE = f"{SCHEMA}.round_trip"


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def cursor() -> Iterator[Any]:
    if not _reachable(TRINO_HOST, TRINO_PORT):
        message = f"Trino is not reachable on {TRINO_HOST}:{TRINO_PORT}; run .\\make.ps1 up"
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail(message)
        pytest.skip(message)
    import trino

    connection = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="olist-smoke")
    cur = connection.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.fetchall()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.fetchall()
    yield cur
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.fetchall()
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA}")
    cur.fetchall()
    connection.close()


def query(cur: Any, sql: str) -> list[list[Any]]:
    cur.execute(sql)
    return list(cur.fetchall())


def minio_has(key: str) -> bool:
    """Ask MinIO itself, inside its container, whether the object exists."""
    script = (
        'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" '
        f">/dev/null && mc stat local/{key} >/dev/null"
    )
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "minio", "sh", "-c", script],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def test_create_insert_select_round_trips(cursor: Any) -> None:
    query(cursor, f"CREATE TABLE {TABLE} (id BIGINT, label VARCHAR)")
    query(cursor, f"INSERT INTO {TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    assert query(cursor, f"SELECT count(*), sum(id) FROM {TABLE}") == [[3, 6]]


def test_the_data_files_are_in_the_minio_warehouse(cursor: Any) -> None:
    paths = [row[0] for row in query(cursor, f'SELECT file_path FROM {SCHEMA}."round_trip$files"')]
    assert paths, "the table has no data files"
    for path in paths:
        assert path.startswith("s3://lakehouse/warehouse/"), path
        assert minio_has(path.removeprefix("s3://")), f"MinIO does not hold {path}"


def test_the_catalog_is_lakekeeper_iceberg_rest(cursor: Any) -> None:
    rows = query(cursor, f'SELECT count(*) FROM {SCHEMA}."round_trip$snapshots"')
    assert rows[0][0] >= 1, "an Iceberg table records a snapshot per commit"
