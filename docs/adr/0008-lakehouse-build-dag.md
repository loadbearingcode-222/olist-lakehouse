# 0008: lakehouse_build: a failed bronze checkpoint stops dbt; maintenance runs through Trino
Date: 2026-09-25 · Story: M4-S2 · Status: accepted

## Context
Silver and gold are rebuilt from bronze on a schedule. A build on bad bronze publishes bad
numbers with every test green, since dbt's tests judge the models, not their inputs. The
streaming job also leaves many small files and snapshots behind.

## Options
- **Input gate:** none (trust the streaming gate alone); dbt source tests; a Great
  Expectations checkpoint on a profile of bronze, with Data Docs.
- **Maintenance:** Spark procedures (`rewrite_data_files`), or Trino's `ALTER TABLE ...
  EXECUTE optimize / expire_snapshots / remove_orphan_files`.

## Decision
`lakehouse_build` (hourly, `max_active_runs=1`):
`gx_bronze_checkpoint >> dbt_silver >> dbt_gold >> iceberg_maintenance`.
- **The checkpoint** (`olist_lakehouse.quality.checkpoint`) judges bronze as a whole: every
  expected source has landed rows, quarantine stays at or below 5%, and the gate and GX agree.
  It is not retried: a failed expectation is a verdict. Downstream tasks become
  `upstream_failed`, and Data Docs (http://localhost:28089) show which expectation failed and
  by how much. A DAG param `plant_failure` fails it on purpose.
- **dbt** runs per layer, with `dbt deps` first because a fresh clone has no packages.
- **Maintenance through Trino:** no second engine in Airflow. The invariant is that rows never
  decrease, and file and snapshot counts are reported.
- **Trino gets a fixed heap** (2.5 GB in a 4 GB container) and a 1 GB per-query cap. The
  image's 80%-of-container default let the kernel kill it under dbt's parallel queries
  (gotcha #39).

## Cost (honest)
- The checkpoint judges bronze totals, not the micro-batch window since the last build. A
  source that stops sending is caught only once its rows would be missing entirely.
  Freshness belongs here once data arrives continuously again.
- One run at a time: a slow build delays the next one rather than overlapping it.

## Evidence (2026-09-25, `tests/integration/test_lakehouse_build.py`)
- Clean run: all four tasks succeed on their first try.
- Planted run: the checkpoint fails and the three downstream tasks are `upstream_failed`.
  The Data Docs page reads "Evaluated 5, Successful 4, Unsuccessful 1".
- Compaction (Iceberg snapshot history): `bronze.cdc_events` went from 55 files to 8 and
  `clickstream_events` from 7 to 1, with record totals recorded unchanged in the same
  snapshots (860,712 and 1,208,820).
