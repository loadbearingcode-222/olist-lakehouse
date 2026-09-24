# Architecture

One Docker Compose stack on a laptop, built from the open-source engines behind the managed
cloud data services. Every arrow below is a real process in `compose.yaml`, and every claim on
this page is checked by `scripts/demo_acceptance.py`.

```mermaid
flowchart LR
    subgraph source["Source: a live app"]
        csv[("Olist CSVs<br/>(Kaggle, 9 files)")]
        rep["replayer<br/>business clock, chaos"]
        pg[("Postgres 17<br/>olist schema")]
        click["clickstream producer<br/>Faker sessions, chaos"]
        csv --> rep --> pg
        pg -. "orders after the watermark" .-> click
    end

    subgraph stream["Streaming"]
        dbz["Debezium<br/>(Kafka Connect)"]
        kafka[["Kafka (KRaft)<br/>olist.olist.* · clickstream.events"]]
        spark["Spark Structured Streaming<br/>exactly-once by offset"]
        gate{"quality gate<br/>named rules + GX"}
        pg -- "WAL, logical decoding" --> dbz --> kafka
        click --> kafka
        kafka --> spark --> gate
    end

    subgraph lake["Lakehouse: Iceberg on MinIO, catalog Lakekeeper"]
        bronze[("bronze<br/>cdc_events · clickstream_events<br/>fx_rates · quality_metrics")]
        quarantine[("bronze.quarantine<br/>row + rule")]
        silver[("silver<br/>current state per key,<br/>_is_deleted · _dq_issues")]
        gold[("gold<br/>star schema, SCD2,<br/>BRL + USD, marts")]
        gate -- pass --> bronze
        gate -- fail --> quarantine
    end

    subgraph batch["Batch: Airflow 3"]
        fx["fx_rates_daily<br/>ECB via Frankfurter"]
        build["lakehouse_build<br/>GX checkpoint → dbt silver → dbt gold → maintenance"]
    end

    fx --> bronze
    bronze --> build
    build --> silver --> gold
    build -. "Data Docs" .-> docs["GX Data Docs<br/>:28089"]

    trino["Trino<br/>(SQL engine)"]
    superset["Superset<br/>4 dashboards as code"]
    gold --> trino --> superset
```

## The layers, and what each one promises

| Layer | Engine | Promise | Checked by |
|---|---|---|---|
| Source | Postgres 17, replayer | Olist replayed in business-time order; every injected defect is recorded in a chaos ledger | `replayer --verify` · [ADR-0001](adr/0001-lake-core-stack-and-pins.md) |
| CDC | Debezium 3.6 → Kafka 4.3 | every insert, update and delete becomes a message; a Connect restart loses nothing | `test_cdc.py` · [ADR-0002](adr/0002-cdc-json-without-schema-registry.md) |
| Bronze | Spark 4.1 → Iceberg 1.11 | every Kafka offset lands once, even across a hard kill | `test_bronze.py`, drills · [ADR-0003](adr/0003-spark-streaming-bronze.md) |
| Quality gate | named rules + Great Expectations | a bad row goes to quarantine under its rule; quarantine per rule = chaos ledger | `test_quality_gate.py` · [ADR-0004](adr/0004-quality-gate-quarantine.md) |
| Silver | dbt-trino, incremental MERGE | current state equals Postgres, every row and column | `reconcile.py` · [ADR-0005](adr/0005-silver-cdc-merge.md) |
| Gold | dbt-trino, enforced contracts | payments equal the raw CSV to the cent; SCD2 = the replayed mutations; USD at the ECB rate of the purchase day | `test_gold.py` · [ADR-0006](adr/0006-gold-star-schema.md) |
| Orchestration | Airflow 3.3 | a failed bronze checkpoint stops the build, and Data Docs show why | `test_lakehouse_build.py` · [ADR-0007](adr/0007-airflow-isolated-project-venv.md), [ADR-0008](adr/0008-lakehouse-build-dag.md) |
| Serving | Trino 483, Superset 6.1 | 4 dashboards come back from git on a fresh start, and every chart has data | `test_superset.py` · [ADR-0009](adr/0009-superset-dashboards-as-code.md) |

## Two clocks

Business time stays real (2016-09 → 2018-10), and the replayer compresses the wall clock
(`REPLAY_SPEED`, 1440 = one day per minute; 0 = as fast as Postgres allows, which the demo
uses). Analytics and the FX rates therefore use real dates, while the platform sees a live
stream. Late and duplicate events are measured against business time.

## Where the trust comes from

- **Nothing is compared with a remembered number.** Every check recomputes its truth from the
  source: Postgres, the raw CSVs, Kafka's offsets, the chaos ledger, the ECB's own calendar.
- **Defects are injected on purpose** (2% by default) and must come out the other side
  accounted for: quarantined under the right rule, or flagged in silver's `_dq_issues`.
- **Two independent validators:** the gate's rules and Great Expectations count every
  micro-batch separately, and `quality_metrics` records whether they agree.

## Screenshots

Captured from the demo stack; see the README.
