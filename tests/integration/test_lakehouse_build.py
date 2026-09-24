"""lakehouse_build end to end, twice: once clean, once with a planted failing expectation.

Clean run (`airflow dags test lakehouse_build`):
    gx_bronze_checkpoint -> dbt_silver -> dbt_gold -> iceberg_maintenance all succeed, each
    on its FIRST try (a retry would hide an unstable dependency, as Trino's OOM kills did),
    and compaction leaves bronze with no more files than before.
Planted run (`-c '{"plant_failure": true}'`):
    the checkpoint fails, nothing downstream runs (upstream_failed), and the Data Docs page of
    that validation shows exactly one unsuccessful expectation.

Evidence: automation/runs/m4-s2/lakehouse_build.json. Takes about 8 minutes.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
EVIDENCE = REPO / "automation" / "runs" / "m4-s2" / "lakehouse_build.json"
DOCS = "http://127.0.0.1:28089"
TASKS = ["gx_bronze_checkpoint", "dbt_silver", "dbt_gold", "iceberg_maintenance"]


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def dags_test(conf: str | None = None) -> tuple[int, dict[str, tuple[str, int]]]:
    args = ["exec", "-T", "airflow-scheduler", "airflow", "dags", "test", "lakehouse_build"]
    result = compose(*args, *(["-c", conf] if conf else []))
    states = compose(
        "exec",
        "-T",
        "platform-db",
        "psql",
        "-U",
        "platform",
        "-d",
        "airflow",
        "-Atc",
        "SELECT task_id, state, try_number FROM task_instance WHERE run_id = ("
        "SELECT run_id FROM dag_run WHERE dag_id = 'lakehouse_build' "
        "ORDER BY start_date DESC LIMIT 1)",
    ).stdout
    tasks = {}
    for line in states.strip().splitlines():
        task, state, tries = line.split("|")
        tasks[task] = (state, int(tries))
    return result.returncode, tasks


def bronze_files(cursor: Any) -> int:
    cursor.execute('SELECT count(*) FROM lake.bronze."cdc_events$files"')
    return int(cursor.fetchall()[0][0])


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as response:
        return str(response.read().decode("utf-8"))


@pytest.fixture(scope="module")
def cursor() -> Any:
    try:
        socket.create_connection(("127.0.0.1", 28082), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Airflow is not reachable on 127.0.0.1:28082")
        pytest.skip("Airflow is not up")
    import trino

    return trino.dbapi.connect(host="127.0.0.1", port=28090, user="lakehouse-build").cursor()


def test_clean_run_then_planted_failure(cursor: Any) -> None:
    files_before = bronze_files(cursor)
    clean_exit, clean = dags_test()
    files_after = bronze_files(cursor)

    planted_exit, planted = dags_test('{"plant_failure": true}')
    index = fetch(f"{DOCS}/index.html")
    pages = sorted(
        set(re.findall(r"validations/bronze_profile/__none__/[0-9TZ.]+/bronze-profile.html", index))
    )
    latest = re.sub(r"<[^>]*>", " ", fetch(f"{DOCS}/{pages[-1]}"))
    latest = re.sub(r"\s+", " ", latest)

    evidence = {
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "clean_run": {"exit": clean_exit, "tasks": clean},
        "bronze_cdc_files": {"before": files_before, "after": files_after},
        "planted_run": {"exit": planted_exit, "tasks": planted},
        "data_docs_validations": len(pages),
        "latest_validation_summary": re.findall(
            r"(?:Evaluated|Successful|Unsuccessful) Expectations \d+", latest
        ),
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_bytes((json.dumps(evidence, indent=2) + "\n").encode("utf-8"))
    print("\n" + json.dumps(evidence, indent=1))

    assert clean_exit == 0
    assert clean == {task: ("success", 1) for task in TASKS}, clean
    assert files_after <= files_before
    assert planted_exit != 0
    assert planted["gx_bronze_checkpoint"][0] == "failed"
    assert {planted[t][0] for t in TASKS[1:]} == {"upstream_failed"}
    assert "Unsuccessful Expectations 1" in latest
