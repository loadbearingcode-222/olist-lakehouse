"""The end-to-end gate: drive a fresh stack to "everything caught up", then check every claim.

    uv run --group integration python scripts/demo_acceptance.py            # drive, then check
    uv run --group integration python scripts/demo_acceptance.py --check    # check only
    .\\make.ps1 demo                                                        # fetch, up, replay, this

Drive (after `.\\make.ps1 replay` has started the producers):
    1. wait for the replayer to finish the whole timeline, and the clickstream to cover every order
    2. stop the producers, and wait until bronze has landed every Kafka offset
    3. load the ECB BRL->USD rates for the whole period (bronze.fx_rates)
    4. unpause and trigger `lakehouse_build` in Airflow: GX checkpoint, dbt silver + gold, maintenance

Check, each against its source of truth (never a remembered number):
    services      every long-running service is up and healthy; every one-shot exited 0
    replay        the replayer applied the whole timeline; the clickstream covered every order
    bronze        landed rows per (topic, partition) = Kafka offsets, and all distinct
    quarantine    distinct quarantined messages per rule = chaos ledger entries of its kind
    gate_vs_gx    Great Expectations agrees with the gate on every micro-batch
    silver        equals Postgres, every live row and column (scripts/reconcile.py)
    gold          SUM(payment_value) = the raw CSV to the cent; every order has its FX rate
    airflow       the last lakehouse_build run succeeded, every task on its first try
    superset      4 dashboards, and every chart returns rows

Writes automation/runs/m5-s2/demo.json and exits 1 if any check fails.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import trino

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import reconcile  # noqa: E402
from olist_lakehouse.quality.rules import RULES  # noqa: E402
from olist_lakehouse.replayer.timeline import KEYS  # noqa: E402
from superset_dashboards import CHARTS, Api  # noqa: E402

EVIDENCE = REPO / "automation" / "runs" / "m5-s2" / "demo.json"
RAW = REPO / "data" / "raw" / "olist"
TOPICS = r"^(olist\.olist\.|clickstream\.events$)"
BRONZE = ("lake.bronze.cdc_events", "lake.bronze.clickstream_events", "lake.bronze.quarantine")
TASKS = ["gx_bronze_checkpoint", "dbt_silver", "dbt_gold", "iceberg_maintenance"]


def say(message: str) -> None:
    print(f"demo: {message}", flush=True)


def compose(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", "compose", "--profile", "replay", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if check and result.returncode != 0:
        raise SystemExit(f"docker compose {' '.join(args)} failed:\n{result.stderr[-2000:]}")
    return result.stdout


def airflow_sql(sql: str) -> list[list[str]]:
    out = compose(
        "exec", "-T", "platform-db", "psql", "-U", "platform", "-d", "airflow", "-Atc", sql
    )
    return [line.split("|") for line in out.strip().splitlines() if line]


def wait_for(what: str, probe: Callable[[], tuple[bool, str]], timeout_min: float) -> None:
    deadline, last = time.monotonic() + timeout_min * 60, 0.0
    while True:
        done, progress = probe()
        if done:
            say(f"{what}: done ({progress})")
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"demo: {what} did not finish in {timeout_min} min ({progress})")
        if time.monotonic() - last > 60:
            say(f"{what}: {progress}")
            last = time.monotonic()
        time.sleep(10)


def pg() -> psycopg.Connection[Any]:
    return psycopg.connect(
        "host=127.0.0.1 port=25432 user=olist dbname=olist "
        f"password={reconcile.env('SOURCE_DB_PASSWORD')}",
        autocommit=True,
    )


def producers(conn: psycopg.Connection[Any]) -> dict[str, int]:
    row = conn.execute(
        "SELECT c.last_seq + 1, c.events_total,"
        " (SELECT orders_done FROM replay.clickstream_checkpoint),"
        " (SELECT count(*) FROM olist.orders)"
        " FROM replay.checkpoint AS c"
    ).fetchone()
    names = ("events_applied", "events_total", "click_orders_done", "orders")
    return dict(zip(names, (int(v or 0) for v in row), strict=True)) if row else {}


def kafka_offsets() -> dict[tuple[str, int], int]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": "127.0.0.1:29092", "group.id": "demo-acceptance"})
    try:
        counts = {}
        for name, meta in consumer.list_topics(timeout=10).topics.items():
            if re.search(TOPICS, name):
                for partition in meta.partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(name, partition), timeout=10
                    )
                    counts[(name, partition)] = high - low
        return counts
    finally:
        consumer.close()


def landed(cur: Any) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], int]]:
    union = " UNION ALL ".join(
        f"SELECT kafka_topic, kafka_partition, kafka_offset FROM {t}" for t in BRONZE
    )
    cur.execute(
        "SELECT kafka_topic, kafka_partition, count(*), count(DISTINCT kafka_offset)"
        f" FROM ({union}) GROUP BY 1, 2"
    )
    rows, distinct = {}, {}
    for topic, partition, n, d in cur.fetchall():
        rows[(topic, partition)], distinct[(topic, partition)] = int(n), int(d)
    return rows, distinct


# ------------------------------------------------------------------------------ drive


def drive(cur: Any) -> dict[str, Any]:
    timings: dict[str, float] = {}
    started = time.monotonic()
    with pg() as conn:

        def replay_done() -> tuple[bool, str]:
            p = producers(conn)
            done = bool(p) and p["events_applied"] == p["events_total"]
            done = done and p["click_orders_done"] == p["orders"]
            return done, (
                f"{p.get('events_applied', 0):,}/{p.get('events_total', 0):,} events,"
                f" clickstream {p.get('click_orders_done', 0):,}/{p.get('orders', 0):,} orders"
            )

        wait_for("replay + clickstream", replay_done, timeout_min=240)
    timings["replay_min"] = round((time.monotonic() - started) / 60, 1)
    compose("stop", "replayer", "clickstream")

    mark = time.monotonic()

    def drained() -> tuple[bool, str]:
        expected, (rows, _) = kafka_offsets(), landed(cur)
        behind = sum(expected.values()) - sum(rows.values())
        return rows == expected, f"{behind:,} messages still to land"

    wait_for("bronze drains Kafka", drained, timeout_min=60)
    timings["bronze_drain_min"] = round((time.monotonic() - mark) / 60, 1)

    mark = time.monotonic()
    say("loading ECB BRL->USD rates")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "olist_lakehouse.fx",
            "--start",
            "2016-08-25",
            "--end",
            "2018-10-31",
        ],
        cwd=REPO,
        check=True,
    )
    timings["fx_min"] = round((time.monotonic() - mark) / 60, 1)

    mark = time.monotonic()
    scheduler = ("exec", "-T", "airflow-scheduler", "airflow", "dags")
    compose(*scheduler, "unpause", "lakehouse_build")
    compose(*scheduler, "trigger", "lakehouse_build")
    say("lakehouse_build triggered in Airflow (http://localhost:28082)")

    def built() -> tuple[bool, str]:
        rows = airflow_sql(
            "SELECT state FROM dag_run WHERE dag_id = 'lakehouse_build' AND run_type = 'manual'"
            " ORDER BY id DESC LIMIT 1"
        )
        state = rows[0][0] if rows else "not created yet"
        if state == "failed":
            raise SystemExit("demo: lakehouse_build failed; see Airflow and Data Docs")
        return state == "success", state

    wait_for("lakehouse_build", built, timeout_min=60)
    timings["lakehouse_build_min"] = round((time.monotonic() - mark) / 60, 1)
    return timings


# ------------------------------------------------------------------------------ check


def check_services() -> dict[str, Any]:
    out = compose("ps", "-a", "--format", "json")
    services = [json.loads(line) for line in out.splitlines() if line.strip().startswith("{")]
    bad = []
    for s in services:
        name, state, health = s["Service"], s["State"], s.get("Health", "")
        if name in ("replayer", "clickstream"):
            continue  # stopped on purpose once caught up
        if state == "exited":
            if int(s.get("ExitCode", 1)) != 0:
                bad.append(f"{name} exited {s.get('ExitCode')}")
        elif state != "running" or health not in ("", "healthy"):
            bad.append(f"{name} {state} {health}".strip())
    return {"ok": not bad, "services": len(services), "problems": bad}


def check_replay() -> dict[str, Any]:
    with pg() as conn:
        p = producers(conn)
    ok = p["events_applied"] == p["events_total"] and p["click_orders_done"] == p["orders"]
    return {"ok": ok, **p}


def check_bronze(cur: Any) -> dict[str, Any]:
    expected = kafka_offsets()
    rows, distinct = landed(cur)
    return {
        "ok": rows == expected and rows == distinct and bool(rows),
        "partitions": len(rows),
        "kafka_offsets": sum(expected.values()),
        "landed_rows": sum(rows.values()),
        "distinct_offsets": sum(distinct.values()),
    }


def check_quarantine(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT rule, count(DISTINCT CASE WHEN source = 'cdc'"
        "  THEN concat(source_table, ':', CAST(source_lsn AS varchar), ':', record_key)"
        "  ELSE event_id END)"
        " FROM lake.bronze.quarantine GROUP BY rule"
    )
    quarantined = {rule: int(n) for rule, n in cur.fetchall()}
    with pg() as conn:
        ledger = dict(conn.execute("SELECT kind, count(*) FROM replay.chaos_ledger GROUP BY 1"))
        ledger.update(
            conn.execute(
                "SELECT kind, count(DISTINCT event_id) FROM replay.clickstream_chaos_ledger GROUP BY 1"
            )
        )
    per_rule = {
        r.name: {
            "quarantined": quarantined.get(r.name, 0),
            "ledger": int(ledger.get(r.chaos_kind, 0)),
        }
        for r in RULES
    }
    ok = all(v["quarantined"] == v["ledger"] > 0 for v in per_rule.values())
    return {"ok": ok, "rules": per_rule}


def check_gate_vs_gx(cur: Any) -> dict[str, Any]:
    cur.execute(
        "SELECT count(*), count_if(NOT agree) FROM lake.bronze.quality_metrics WHERE kind = 'gate'"
    )
    checked, disagree = cur.fetchall()[0]
    return {"ok": checked > 0 and disagree == 0, "batch_rules": checked, "disagreements": disagree}


def check_silver(cur: Any) -> dict[str, Any]:
    with pg() as conn:
        excluded = reconcile.ledger_keys(conn)
        tables = {t: reconcile.reconcile_table(conn, cur, t, excluded.get(t, set())) for t in KEYS}
    click = reconcile.reconcile_clickstream(cur)
    bad = [
        t
        for t, r in tables.items()
        if r["missing_in_silver"] or r["extra_in_silver"] or r["different_values"]
    ]
    return {
        "ok": not bad and click["ok"],
        "tables_matching": len(tables) - len(bad),
        "tables": len(tables),
        "rows_matched": sum(r["matched"] for r in tables.values()),
        "chaos_excluded_keys": sum(r["excluded_chaos_keys"] for r in tables.values()),
        "failing_tables": bad,
        "clickstream": click,
    }


def check_gold(cur: Any) -> dict[str, Any]:
    with (RAW / "olist_order_payments_dataset.csv").open(newline="", encoding="utf-8") as f:
        csv_total = sum(Decimal(r["payment_value"]) for r in csv.DictReader(f))
    cur.execute(
        "SELECT (SELECT sum(payment_value_brl) FROM lake.gold.fct_payments),"
        " (SELECT count(*) FROM lake.gold.fct_orders WHERE fx_brl_usd IS NULL),"
        " (SELECT count(*) FROM lake.gold.fct_orders)"
    )
    gold_total, no_fx, orders = cur.fetchall()[0]
    gold_total = Decimal(str(gold_total or 0))
    return {
        "ok": abs(gold_total - csv_total) <= Decimal("0.01") and no_fx == 0 and orders > 0,
        "payments_brl_csv": str(csv_total),
        "payments_brl_gold": str(gold_total),
        "orders": orders,
        "orders_without_fx": no_fx,
    }


def check_airflow() -> dict[str, Any]:
    run = airflow_sql(
        "SELECT run_id, state FROM dag_run WHERE dag_id = 'lakehouse_build'"
        " AND state IN ('success', 'failed') ORDER BY end_date DESC LIMIT 1"
    )
    if not run:
        return {"ok": False, "last_run": None}
    run_id, state = run[0]
    tasks = {
        t: (s, int(n))
        for t, s, n in airflow_sql(
            f"SELECT task_id, state, try_number FROM task_instance"
            f" WHERE dag_id = 'lakehouse_build' AND run_id = '{run_id}'"
        )
    }
    ok = state == "success" and tasks == {t: ("success", 1) for t in TASKS}
    return {"ok": ok, "last_run": run_id, "state": state, "tasks": tasks}


def check_superset() -> dict[str, Any]:
    api = Api()
    found: dict[str, dict[str, int]] = {}
    for dashboard in api.call("GET", "/api/v1/dashboard/?q=(page_size:100)")["result"]:
        rows = {}
        for chart in api.call("GET", f"/api/v1/dashboard/{dashboard['id']}/charts")["result"]:
            data = api.call("GET", f"/api/v1/chart/{chart['id']}/data/")["result"][0]
            rows[chart["slice_name"]] = int(data["rowcount"])
        found[dashboard["dashboard_title"]] = rows
    ok = set(found) == set(CHARTS) and all(
        set(found[t]) == {c["name"] for c in charts} and all(n > 0 for n in found[t].values())
        for t, charts in CHARTS.items()
    )
    return {"ok": ok, "dashboards": found}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--check", action="store_true", help="skip driving; check the stack as is")
    args = parser.parse_args(argv)

    cur = trino.dbapi.connect(host="127.0.0.1", port=28090, user="demo-acceptance").cursor()
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat(timespec="seconds")}
    if not args.check:
        report["timings"] = drive(cur)

    checks: dict[str, Callable[[], dict[str, Any]]] = {
        "services": check_services,
        "replay": check_replay,
        "bronze": lambda: check_bronze(cur),
        "quarantine": lambda: check_quarantine(cur),
        "gate_vs_gx": lambda: check_gate_vs_gx(cur),
        "silver": lambda: check_silver(cur),
        "gold": lambda: check_gold(cur),
        "airflow": check_airflow,
        "superset": check_superset,
    }
    results: dict[str, Any] = {}
    for name, run in checks.items():
        try:
            results[name] = run()
        except Exception as exc:  # a check that cannot run has failed, with the reason recorded
            results[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        say(f"{name:<11} {'PASS' if results[name]['ok'] else 'FAIL'}")

    report["checked_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    report["checks"] = results
    report["verdict"] = "pass" if all(r["ok"] for r in results.values()) else "fail"
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_bytes((json.dumps(report, indent=2, default=str) + "\n").encode("utf-8"))
    say(f"{report['verdict'].upper()}; evidence in {EVIDENCE.relative_to(REPO)}")
    return 0 if report["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
