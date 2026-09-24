"""ECB BRL->USD reference rates, one data interval (day) per run, into lake.bronze.fx_rates.

Backfill the dataset's period once (docs/RUNBOOK.md, "Airflow"):

    airflow backfill create --dag-id fx_rates_daily \
        --from-date 2016-08-25 --to-date 2018-10-31 --max-active-runs 6

A weekend or ECB holiday has no rate of its own, so its run writes nothing. Every run is
idempotent: rerunning a day adds no rows (olist_lakehouse.fx merges on the date). Writes to the
one Iceberg table go through the `fx_rates_writer` pool (1 slot): concurrent MERGEs into one
Iceberg table conflict (optimistic concurrency, ICEBERG_COMMIT_ERROR).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

with DAG(
    dag_id="fx_rates_daily",
    description="ECB BRL->USD rate for one day into lake.bronze.fx_rates",
    # A daily INTERVAL starting at midnight UTC, so an interval IS a calendar day and a
    # backfill's --from-date/--to-date are exactly the first and last day loaded. (At 06:00,
    # --to-date 2018-10-31 stopped at the interval starting 2018-10-30 06:00: gotcha #38.)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2016, 8, 25, tz="UTC"),
    catchup=False,  # history is loaded by an explicit backfill, never by accident
    max_active_runs=2,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["olist", "bronze", "fx"],
) as dag:
    BashOperator(
        task_id="load_rate",
        # The date reaches the command through the environment, never spliced into shell text.
        bash_command='"$OLIST_PYTHON" -m olist_lakehouse.fx --start "$FX_DAY" --end "$FX_DAY"',
        env={"FX_DAY": "{{ data_interval_start | ds }}"},
        append_env=True,
        execution_timeout=timedelta(minutes=5),
        pool="fx_rates_writer",  # created by airflow-init (compose.yaml), 1 slot
    )
