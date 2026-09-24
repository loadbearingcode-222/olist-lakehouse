# 0007: Airflow 3 runs the project's code from an isolated venv; history arrives by backfill
Date: 2026-09-24 · Story: M4-S1 · Status: accepted

## Context
Airflow orchestrates the batch side: FX rates (M4-S1), then quality checks, dbt and Iceberg
maintenance (M4-S2). Airflow pins hundreds of dependencies of its own. The project's code
needs psycopg, trino, dbt and Great Expectations at the versions in `uv.lock`.

## Options
1. `pip install` the project into Airflow's environment: one interpreter, but every
   dependency negotiates with Airflow's constraints, and dbt or GX upgrades can break the
   scheduler.
2. A container per task (DockerOperator or KubernetesPodOperator): perfect isolation, but it
   needs the Docker socket inside Airflow on a Windows host, and pays a start-up per task.
3. **An isolated venv inside the Airflow image**, built from `uv.lock`; tasks call its
   Python and dbt directly.

## Decision
Option 3. `docker/airflow/Dockerfile` starts from `apache/airflow:3.3.2-python3.12` and adds
`/opt/olist-venv` (`uv sync --locked --group integration --group dbt --group quality`).
Tasks are `BashOperator`s calling `"$OLIST_PYTHON" -m ...` or `"$OLIST_DBT" ...`. Run
parameters reach the command through `env`, never spliced into shell text, which the DAG
integrity test enforces.

Other choices, recorded:
- **LocalExecutor** (tasks run in the scheduler container, 2 GB); **FAB auth manager**,
  admin password generated into `.env`.
- **Metadata** lives in its own `airflow` database on `platform-db`, created by the one-shot
  `platform-db-init` (which also creates `superset` for M5).
- **History arrives only by an explicit backfill** (`catchup=False`,
  `CronDataIntervalTimetable`). Nothing loads years of data because a DAG was unpaused.
- **DAG integrity** is tested inside the real image (CI job `dag integrity`), because the host
  venv deliberately has no Airflow.

## Cost (honest)
- The Airflow image carries two Python environments (about 700 MB more).
- LocalExecutor shares the scheduler's resources, so a heavy dbt run slows scheduling. The fix
  at scale is Celery or Kubernetes executors, not this design.
- FAB auth with one admin is a local-demo posture: ports are bound to 127.0.0.1.

## Revisit when
- Tasks need different Python stacks (then containers per task);
- or the scheduler's memory becomes the bottleneck.
