"""Hourly: check bronze, build silver, build gold, look after the tables, publish the evidence.

    gx_bronze_checkpoint >> dbt_silver >> dbt_gold >> iceberg_maintenance

- gx_bronze_checkpoint  Great Expectations on a profile of bronze (olist_lakehouse.quality.
                        checkpoint). A failure stops the run here: nothing downstream builds
                        on bronze that failed its checks. Data Docs update either way.
- dbt_silver / dbt_gold `dbt build` (models + tests) per layer. A failing test stops gold.
- iceberg_maintenance   compaction + snapshot expiry (olist_lakehouse.maintenance).

Trigger with {"plant_failure": true} to watch the checkpoint refuse (the M4-S2 red team).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    dag_id="lakehouse_build",
    description="GX bronze checkpoint -> dbt silver -> dbt gold -> Iceberg maintenance",
    schedule=CronDataIntervalTimetable("15 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one dbt build at a time: runs share the project and the tables
    params={
        "plant_failure": Param(
            False, type="boolean", description="Red team: fail the checkpoint on purpose"
        )
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["olist", "silver", "gold", "quality"],
) as dag:
    checkpoint = BashOperator(
        task_id="gx_bronze_checkpoint",
        bash_command='"$OLIST_PYTHON" -m olist_lakehouse.quality.checkpoint $PLANT',
        env={"PLANT": "{{ '--plant-failure' if params.plant_failure else '' }}"},
        append_env=True,
        retries=0,  # a failed expectation is a verdict, not a flake
        execution_timeout=timedelta(minutes=10),
    )
    silver = BashOperator(
        task_id="dbt_silver",
        # deps first: dbt_packages is not in git, and a fresh clone has none.
        bash_command='"$OLIST_DBT" deps && "$OLIST_DBT" build --select path:models/silver',
        execution_timeout=timedelta(minutes=30),
    )
    gold = BashOperator(
        task_id="dbt_gold",
        bash_command='"$OLIST_DBT" build --select path:models/gold path:tests',
        execution_timeout=timedelta(minutes=30),
    )
    maintenance = BashOperator(
        task_id="iceberg_maintenance",
        bash_command='"$OLIST_PYTHON" -m olist_lakehouse.maintenance',
        execution_timeout=timedelta(minutes=30),
    )

    checkpoint >> silver >> gold >> maintenance
