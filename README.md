# olist-lakehouse

A local, one-command data platform built on the open-source engines behind the cloud data
services. Real Brazilian e-commerce data (Olist, 2016–2018) is replayed as a live application,
captured with CDC, streamed through Kafka into an Iceberg lakehouse, validated, modeled with dbt,
orchestrated by Airflow, and served through Trino and Superset.

```
Olist CSVs ─► replayer ─► Postgres ─► Debezium ─► Kafka ─► Spark Streaming ─► Iceberg bronze (MinIO)
                  clickstream producer ─────────────┘            │ GX gate → quarantine
                                                                 ▼
             Superset ◄─ Trino ◄─ dbt gold ◄─ dbt silver ◄── Airflow 3 (FX · GX · dbt · maintenance)
```

**Status:** under construction. The plan, in the order it merges, is in
[docs/MILESTONES.md](docs/MILESTONES.md).

## Data

Olist Brazilian E-Commerce Public Dataset, © Olist, licensed
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). The CSVs are downloaded
at runtime from Kaggle and are never committed to this repository.

## License

Code: MIT (see [LICENSE](LICENSE)). The dataset keeps its own license, above.
