"""DAG integrity, checked inside the Airflow image (CI job `dag-integrity`, or:
    docker compose run --rm --no-deps airflow-scheduler bash -c "pip install -q pytest && python -m pytest /opt/olist/tests/dags"
).

Parses the DAG folder the way the dag-processor does, without a metadata database: the DagBag
is read from `bag.dags`, never `get_dag()` (which queries the DB; gotcha #17).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

airflow = pytest.importorskip("airflow", reason="runs inside the Airflow image")

from airflow.models import DagBag  # noqa: E402
from airflow.timetables.interval import CronDataIntervalTimetable  # noqa: E402

DAGS = Path(os.environ.get("OLIST_DAGS", "/opt/airflow/dags"))


@pytest.fixture(scope="module")
def bag() -> DagBag:
    # Airflow 3.3 dropped DagBag(include_examples=...); examples follow the config flag
    # AIRFLOW__CORE__LOAD_EXAMPLES, and ours are selected by file location below.
    return DagBag(dag_folder=str(DAGS))


def ours(bag: DagBag) -> dict[str, Any]:
    return {i: d for i, d in bag.dags.items() if str(d.fileloc).startswith(str(DAGS))}


def test_every_dag_imports(bag: DagBag) -> None:
    assert bag.import_errors == {}
    assert ours(bag), f"no DAGs found in {DAGS}"


def test_fx_rates_daily_is_an_interval_dag_that_never_catches_up_by_accident(bag: DagBag) -> None:
    dag = bag.dags["fx_rates_daily"]
    assert isinstance(dag.timetable, CronDataIntervalTimetable)  # gotcha #16
    assert dag.catchup is False
    assert [t.task_id for t in dag.tasks] == ["load_rate"]
    assert dag.tasks[0].pool == "fx_rates_writer"  # one Iceberg writer at a time


def test_lakehouse_build_checks_bronze_before_building_anything(bag: DagBag) -> None:
    dag = bag.dags["lakehouse_build"]
    order = [t.task_id for t in dag.topological_sort()]
    assert order == ["gx_bronze_checkpoint", "dbt_silver", "dbt_gold", "iceberg_maintenance"]
    assert dag.max_active_runs == 1  # one dbt build at a time
    assert dag.get_task("gx_bronze_checkpoint").retries == 0  # a verdict is not retried
    assert dag.catchup is False


def test_no_template_is_spliced_into_shell_text(bag: DagBag) -> None:
    """Run parameters reach bash through env vars (gotcha #7), never inside the command."""
    for dag in ours(bag).values():
        for task in dag.tasks:
            command = getattr(task, "bash_command", None)
            if command is not None:
                assert "{{" not in command, f"{dag.dag_id}.{task.task_id} splices a template"
