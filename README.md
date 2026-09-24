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

**Status:** M0 (foundations) is done: verified Olist download and the lake core (MinIO +
Lakekeeper Iceberg REST catalog + Trino). Next is M1 (Postgres, Debezium CDC and Kafka). The
plan, in the order it merges, is in [docs/MILESTONES.md](docs/MILESTONES.md).

## Quickstart (Windows, Docker Desktop, uv, Kaggle CLI configured)

```powershell
.\make.ps1 check              # lint, types, unit tests
.\make.ps1 fetch              # Olist CSVs from Kaggle, verified against data/olist.sha256
.\make.ps1 up                 # generates .env, runs the preflight, starts the stack
.\make.ps1 test-integration   # Trino → Iceberg REST → MinIO round trip
```

| Service | URL |
|---|---|
| Trino | http://localhost:28090 |
| Lakekeeper UI | http://localhost:28181/ui |
| MinIO console | http://localhost:29001 |

Decisions are in [docs/adr/](docs/adr/) and known traps in [docs/gotchas.md](docs/gotchas.md).

## Data

Olist Brazilian E-Commerce Public Dataset, © Olist, licensed
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). The CSVs are downloaded
at runtime from Kaggle and are never committed to this repository.

## License

Code: MIT (see [LICENSE](LICENSE)). The dataset keeps its own license, above.
