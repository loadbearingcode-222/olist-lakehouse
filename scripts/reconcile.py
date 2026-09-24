"""Reconcile silver with the source: every live row, every column, table by table.

    uv run --group integration python scripts/reconcile.py [--write]
    .\\make.ps1 reconcile

For each app table, Postgres (the truth) is compared with silver's live rows
(`NOT _is_deleted`): same keys, and identical values in every column. The only keys set
aside are those the chaos ledger says were deliberately corrupted (their bad change was
quarantined by design, so silver is *supposed* to differ). They are counted, not hidden.

The clickstream is checked too: silver.click_events holds each distinct bronze event_id once.

Exit 1 on any missing, extra or different row. `--write` saves the report to
automation/runs/m3-s1/reconcile.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import trino

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from olist_lakehouse.replayer.timeline import KEYS  # noqa: E402

EVIDENCE = REPO / "automation" / "runs" / "m3-s1" / "reconcile.json"


def env(key: str) -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise SystemExit(f".env has no {key}; run .\\make.ps1 env")


def columns_of(pg: psycopg.Connection[Any], table: str) -> list[str]:
    rows = pg.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'olist' AND table_name = %s ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return [r[0] for r in rows]


def ledger_keys(pg: psycopg.Connection[Any]) -> dict[str, set[tuple[Any, ...]]]:
    """Keys whose change carried an injected defect, per table."""
    keys: dict[str, set[tuple[Any, ...]]] = {}
    for table, row_key in pg.execute("SELECT table_name, row_key FROM replay.chaos_ledger"):
        keys.setdefault(table, set()).add(tuple(row_key[k] for k in KEYS[table]))
    return keys


def reconcile_table(
    pg: psycopg.Connection[Any], cur: Any, table: str, excluded: set[tuple[Any, ...]]
) -> dict[str, Any]:
    cols = columns_of(pg, table)
    keys = KEYS[table]
    index = [cols.index(k) for k in keys]
    source = {
        tuple(row[i] for i in index): tuple(row)
        for row in pg.execute(f"SELECT {', '.join(cols)} FROM olist.{table}")
    }
    cur.execute(f"SELECT {', '.join(cols)} FROM lake.silver.{table} WHERE NOT _is_deleted")
    silver = {tuple(row[i] for i in index): tuple(row) for row in cur.fetchall()}

    missing = [k for k in source if k not in silver and k not in excluded]
    extra = [k for k in silver if k not in source and k not in excluded]
    different = [k for k in source if k in silver and k not in excluded and source[k] != silver[k]]
    return {
        "postgres_rows": len(source),
        "silver_live_rows": len(silver),
        "excluded_chaos_keys": len(excluded),
        # Of the set-aside keys: how many silver still matches anyway (a later good change
        # overwrote the defect), and how many differ, as designed, or are absent.
        "excluded_but_identical": sum(
            1 for k in excluded if k in source and k in silver and source[k] == silver[k]
        ),
        "excluded_and_differing": sum(
            1 for k in excluded if k in source and (k not in silver or source[k] != silver[k])
        ),
        "matched": sum(
            1 for k in source if k in silver and k not in excluded and source[k] == silver[k]
        ),
        "missing_in_silver": len(missing),
        "extra_in_silver": len(extra),
        "different_values": len(different),
        "examples": [str(k) for k in (missing + extra + different)[:5]],
    }


def reconcile_clickstream(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT (SELECT count(*) FROM lake.bronze.clickstream_events),"
        " (SELECT count(DISTINCT event_id) FROM lake.bronze.clickstream_events),"
        " (SELECT count(*) FROM lake.silver.click_events),"
        " (SELECT count(DISTINCT event_id) FROM lake.silver.click_events),"
        " (SELECT count(*) FROM lake.silver.click_sessions),"
        " (SELECT count(DISTINCT session_id) FROM lake.silver.click_events)"
    )
    bronze_rows, bronze_ids, silver_rows, silver_ids, sessions, session_ids = cur.fetchall()[0]
    return {
        "bronze_rows": bronze_rows,
        "bronze_distinct_event_ids": bronze_ids,
        "duplicates_removed": bronze_rows - silver_rows,
        "silver_events": silver_rows,
        "silver_distinct_event_ids": silver_ids,
        "sessions": sessions,
        "ok": silver_rows == silver_ids == bronze_ids and sessions == session_ids,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--write", action="store_true", help=f"save the report to {EVIDENCE}")
    args = parser.parse_args(argv)

    dsn = f"host=127.0.0.1 port=25432 user=olist dbname=olist password={env('SOURCE_DB_PASSWORD')}"
    cur = trino.dbapi.connect(host="127.0.0.1", port=28090, user="reconcile").cursor()
    report: dict[str, Any] = {"checked_at": datetime.now(UTC).isoformat(timespec="seconds")}
    with psycopg.connect(dsn) as pg:
        excluded = ledger_keys(pg)
        report["tables"] = {t: reconcile_table(pg, cur, t, excluded.get(t, set())) for t in KEYS}
    report["clickstream"] = reconcile_clickstream(cur)

    bad = [
        t
        for t, r in report["tables"].items()
        if r["missing_in_silver"] or r["extra_in_silver"] or r["different_values"]
    ]
    report["verdict"] = "pass" if not bad and report["clickstream"]["ok"] else "fail"

    for table, r in report["tables"].items():
        print(
            f"  {table:<30} postgres {r['postgres_rows']:>7,}  silver {r['silver_live_rows']:>7,}"
            f"  matched {r['matched']:>7,}  chaos-excluded {r['excluded_chaos_keys']:>5,}"
            f" ({r['excluded_and_differing']:,} differ)"
            f"  missing {r['missing_in_silver']}  extra {r['extra_in_silver']}"
            f"  different {r['different_values']}"
        )
    c = report["clickstream"]
    print(
        f"  {'click_events':<30} bronze {c['bronze_rows']:,} -> {c['silver_events']:,} events"
        f" ({c['duplicates_removed']:,} duplicates removed), {c['sessions']:,} sessions"
    )
    print(f"reconcile: {report['verdict'].upper()}" + (f" ({bad})" if bad else ""))
    if args.write:
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_bytes((json.dumps(report, indent=2) + "\n").encode("utf-8"))
    return 0 if report["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
