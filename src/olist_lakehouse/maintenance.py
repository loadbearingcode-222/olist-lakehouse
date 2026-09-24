"""Iceberg table maintenance through Trino: compact small files, expire old snapshots.

    python -m olist_lakehouse.maintenance [--tables lake.bronze.cdc_events,...]

Streaming writes one set of small files per micro-batch, and every write adds a snapshot, so a
table left alone gets slower to plan and never frees storage. For each table:

    ALTER TABLE ... EXECUTE optimize(file_size_threshold => '64MB')   rewrite small files
    ALTER TABLE ... EXECUTE expire_snapshots(retention_threshold => '7d')
    ALTER TABLE ... EXECUTE remove_orphan_files(retention_threshold => '7d')

Compaction rewrites files, never rows: rows after must be >= rows before (the streaming job may
append meanwhile, so equality would false-alarm); fewer is a refusal (exit 1). File and
snapshot counts are reported. Commit conflicts with a concurrent writer (the streaming job)
are retried, as in fx.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

DEFAULT_TABLES = (
    "lake.bronze.cdc_events",
    "lake.bronze.clickstream_events",
    "lake.bronze.quarantine",
    "lake.bronze.quality_metrics",
    "lake.bronze.fx_rates",
)


def execute(cursor: Any, sql: str, attempts: int = 5) -> list[Any]:
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute(sql)
            return list(cursor.fetchall())
        except Exception as exc:  # trino's error classes vary; the name is what matters
            if "ICEBERG_COMMIT_ERROR" not in str(exc) or attempt == attempts:
                raise
            time.sleep(0.5 * 2**attempt)
    return []


def stats(cursor: Any, table: str) -> dict[str, int]:
    schema_table = table.split(".", 1)[1]
    schema, name = schema_table.split(".")
    rows = execute(cursor, f"SELECT count(*) FROM {table}")[0][0]
    files = execute(cursor, f'SELECT count(*) FROM lake.{schema}."{name}$files"')[0][0]
    snapshots = execute(cursor, f'SELECT count(*) FROM lake.{schema}."{name}$snapshots"')[0][0]
    return {"rows": int(rows), "files": int(files), "snapshots": int(snapshots)}


def maintain(cursor: Any, table: str) -> dict[str, Any]:
    before = stats(cursor, table)
    execute(cursor, f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '64MB')")
    execute(cursor, f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '7d')")
    execute(cursor, f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '7d')")
    after = stats(cursor, table)
    return {"table": table, "before": before, "after": after}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES))
    args = parser.parse_args(argv)
    import trino

    cursor = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "127.0.0.1"),
        port=int(os.environ.get("TRINO_PORT", "28090")),
        user="maintenance",
    ).cursor()
    report = [maintain(cursor, t) for t in args.tables.split(",") if t]
    print(json.dumps(report, indent=1))
    lost = [r["table"] for r in report if r["after"]["rows"] < r["before"]["rows"]]
    if lost:
        print(f"maintenance: REFUSED rows disappeared from {lost}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
