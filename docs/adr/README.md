# Architecture decision records

One file per decision that is hard to reverse or worth arguing: `NNNN-<slug>.md`. Records are
immutable: supersede or amend with a marker, never rewrite.

Template:

```
# NNNN: <decision>
Date: YYYY-MM-DD · Story: M#-S# · Status: accepted | superseded by NNNN

## Context
## Options
## Decision
## Cost (honest)
## Revisit when
```

| ADR | Decision | Story |
|---|---|---|
| [0001](0001-lake-core-stack-and-pins.md) | Lake core: Lakekeeper + MinIO + Trino, exact image pins | M0-S3 |
| [0002](0002-cdc-json-without-schema-registry.md) | CDC as plain JSON, no schema registry | M1-S2 |
| [0003](0003-spark-streaming-bronze.md) | Spark 4.1 + Iceberg 1.11 streaming bronze, exactly once by offset anti-join | M2-S1 |
| [0004](0004-quality-gate-quarantine.md) | Quality gate: named rules, quarantine routing, GX as second opinion | M2-S2 |
| [0005](0005-silver-cdc-merge.md) | Silver: LSN-guarded CDC merge, soft deletes, current-state quality flags | M3-S1 |
| [0006](0006-gold-star-schema.md) | Gold: star schema, SCD2 from the CDC log, FX as of the purchase day | M3-S2 |
| [0007](0007-airflow-isolated-project-venv.md) | Airflow 3 runs project code from an isolated venv; history by backfill | M4-S1 |
| [0008](0008-lakehouse-build-dag.md) | lakehouse_build: a failed bronze checkpoint stops dbt; maintenance through Trino | M4-S2 |
| [0009](0009-superset-dashboards-as-code.md) | Superset dashboards as code: an API builder, a committed export, import on `up` | M5-S1 |
