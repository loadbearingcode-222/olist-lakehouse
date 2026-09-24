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

**Status:** M0 to M4 are done (foundations, source and CDC, streaming bronze, dbt modelling,
orchestration):
- Olist replays into Postgres as a live app on a compressed clock.
- Every change streams through Debezium into Kafka, alongside a matching web clickstream.
- Spark lands every message in Iceberg bronze exactly once.
- A quality gate quarantines bad rows by name and reconciles them 1:1 with the injected chaos.
- Failure drills (kill Connect, kill Spark, restart Kafka) show zero loss.
- dbt on Trino builds silver (current state from CDC, equal to Postgres row by row) and a gold
  star schema: SCD2 sellers and products from the change log, BRL and USD at the ECB rate of
  the purchase day, and enforced contracts. Payments reconcile with the source CSV to the cent.

- Airflow 3 backfills ECB FX rates over the whole period. `lakehouse_build` checks bronze
  with Great Expectations (a failed check stops dbt, and Data Docs show why), builds silver
  and gold, and compacts Iceberg.
- Superset serves four dashboards as code (Revenue BRL/USD, Delivery SLA, Funnel, Stream
  health), built on tested gold marts and imported from git on every start.

Next is the one-command demo (M5-S2). The plan, in the order it merges, is in
[docs/MILESTONES.md](docs/MILESTONES.md).

## Quickstart (Windows, Docker Desktop, uv, Kaggle CLI configured)

```powershell
.\make.ps1 check              # lint, types, unit tests
.\make.ps1 fetch              # Olist CSVs from Kaggle, verified against data/olist.sha256
.\make.ps1 up                 # generates .env, runs the preflight, starts the stack
.\make.ps1 replay             # replayer + clickstream producer (REPLAY_SPEED, CHAOS_RATE)
.\make.ps1 test-integration   # lake round trip, replayer, CDC, clickstream, bronze, gate
.\make.ps1 gate-redteam       # plant one defect per quality rule; each must be refused by name
.\make.ps1 drills             # kill Connect / kill Spark / restart Kafka; zero loss
.\make.ps1 fx                 # ECB BRL->USD rates into bronze
.\make.ps1 dbt build          # silver + gold, with tests
.\make.ps1 reconcile          # silver vs Postgres, every row and column
.\make.ps1 superset-build     # rebuild the dashboards via the API, export to superset/dashboards
```

| Service | URL |
|---|---|
| Trino | http://localhost:28090 |
| Lakekeeper UI | http://localhost:28181/ui |
| MinIO console | http://localhost:29001 |
| Kafka (Redpanda Console) | http://localhost:28080 |
| Spark (Structured Streaming) | http://localhost:24040 |
| Airflow | http://localhost:28082 (admin / `AIRFLOW_ADMIN_PASSWORD` in `.env`) |
| Superset | http://localhost:28088 (admin / `SUPERSET_ADMIN_PASSWORD` in `.env`) |
| Great Expectations Data Docs | http://localhost:28089 |
| Postgres (the Olist app) | 127.0.0.1:25432, db `olist` |

Operating the platform: [docs/RUNBOOK.md](docs/RUNBOOK.md) and
[docs/FAILURE_DRILLS.md](docs/FAILURE_DRILLS.md). Decisions are in [docs/adr/](docs/adr/) and
known traps in [docs/gotchas.md](docs/gotchas.md).

## Data

Olist Brazilian E-Commerce Public Dataset, © Olist, licensed
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/). The CSVs are downloaded
at runtime from Kaggle and are never committed to this repository.

## License

Code: MIT (see [LICENSE](LICENSE)). The dataset keeps its own license, above.
