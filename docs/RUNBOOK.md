# Runbook

How to run, check and recover the platform. Commands are PowerShell from the repo root
unless marked otherwise. Traps are in [gotchas.md](gotchas.md); decisions in [adr/](adr/).

## Start, stop, reset

| Goal | Command | Keeps data? |
|---|---|---|
| Start everything (preflight included) | `.\make.ps1 up` | yes |
| Start the producers | `.\make.ps1 replay` (`$env:REPLAY_SPEED` = 1440 default, 0 = flat out) | yes |
| Pause the producers | `.\make.ps1 replay-stop` (graceful; each resumes from its checkpoint) | yes |
| Stop everything | `docker compose --profile replay stop` | yes |
| Factory reset | `.\make.ps1 reset -Force` (deletes every volume: Postgres, Kafka, MinIO) | **no** |

After a reset, start the stack with `up` **before** `replay`, so Connect is registered before
the first change and nothing is captured as a snapshot (the gate's reconciliation relies on it).

## Health at a glance

```powershell
docker compose --profile replay ps -a        # one-shots show Exited (0); the rest (healthy)
.\make.ps1 replay-status                     # replayer + clickstream positions
curl.exe -s http://127.0.0.1:28083/connectors/olist-postgres/status   # connector + task RUNNING
.\make.ps1 bronze-logs                       # "+N rows, M quarantined" per micro-batch
```

UIs: Kafka Console http://localhost:28080 · Spark http://localhost:24040 · Trino
http://localhost:28090 · Lakekeeper http://localhost:28181/ui · MinIO http://localhost:29001.

## CDC (Postgres → Debezium → Kafka)

**Connector or task FAILED.** Read the trace in `.../status`, fix the cause, then restart
the task. Offsets are in Kafka, so nothing is re-snapshotted:
```powershell
curl.exe -s -X POST "http://127.0.0.1:28083/connectors/olist-postgres/restart?includeTasks=true&onlyFailed=true"
```

**Replication slot growing** (Connect down or stuck). Postgres keeps WAL until the slot
confirms it:
```powershell
docker compose exec postgres-source psql -U olist -d olist -c "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained from pg_replication_slots;"
```
Bring Connect back and the retained size drains. Never drop the slot to "fix" disk: that
loses every change not yet delivered.

**Re-register after a config change:** `docker compose run --rm connect-register`
(idempotent PUT).

## Bronze (Kafka → Spark → Iceberg)

**The job died.** `docker compose up -d bronze-stream`. It resumes from
`s3a://lakehouse/checkpoints/bronze/{cdc,clickstream}`, and a replayed batch logs
`already landed (replayed batch)` instead of writing twice.

**Is bronze complete?** Landed rows (bronze + quarantine) per partition must equal Kafka's
offsets: `tests/integration/test_bronze.py` checks exactly that (it also kills the job once).

**Never delete a checkpoint to "unstick" the job** unless you mean to re-read all of Kafka.
It is safe (the anti-join skips what is landed) but slow, and it only works within Kafka's
7-day retention.

## Quality gate

**Where did a row go?** Either its bronze table or `lake.bronze.quarantine` with a `rule`:
```sql
SELECT rule, count(*) FROM lake.bronze.quarantine GROUP BY 1;
```
**`GATE AND GX DISAGREE` in the job log, or `NOT agree` rows in `quality_metrics`.** One of
the two implementations is wrong. Read `GX could not evaluate` lines first (gotcha #30), then
compare the rule's predicate in `quality/rules.py` with its expectation in `expect.py`.
`.\make.ps1 gate-redteam` re-checks every rule on planted rows.

## Airflow

UI at http://localhost:28082 (user `admin`, password `AIRFLOW_ADMIN_PASSWORD` in `.env`).
DAGs are created paused (`DAGS_ARE_PAUSED_AT_CREATION`). After a `reset`, unpause again, or a
manual trigger sits `queued` forever (gotcha #15).

**Load FX history** (the DAG never catches up by itself, `catchup=False`):
```powershell
docker compose exec airflow-scheduler airflow dags unpause fx_rates_daily
docker compose exec airflow-scheduler airflow backfill create --dag-id fx_rates_daily --from-date 2016-08-25 --to-date 2018-10-31 --max-active-runs 6
```
`--from-date`/`--to-date` are `data_interval_start` values and inclusive. Always pass
`--max-active-runs`: a backfill ignores the DAG's own setting (gotcha #18).

**Rerun one day:** `docker compose exec airflow-scheduler airflow tasks test fx_rates_daily load_rate 2017-03-15`
adds nothing if the rate is already stored (MERGE on the date).

**Where did a backfill get to?**
```powershell
docker compose exec platform-db psql -U platform -d airflow -c "select state, count(*) from dag_run where dag_id = 'fx_rates_daily' group by 1;"
```

**Build silver and gold on demand:** `docker compose exec airflow-scheduler airflow dags test lakehouse_build`.
If `gx_bronze_checkpoint` fails, nothing downstream runs. Open http://localhost:28089, pick the
newest `bronze_profile` validation, and read the failed expectation (quarantine share, a
silent source, or gate/GX disagreement).

**Trino restarted during a run** ("connection refused" in a dbt log): check
`docker inspect olist-lakehouse-trino-1 --format '{{.RestartCount}}'`. Its heap is fixed in
`config/trino/jvm.config`; raise `mem_limit` and `-Xmx` together, never one alone (gotcha #39).

## dbt (silver and gold)

- `.\make.ps1 dbt build` runs everything: silver merges incrementally and gold is rebuilt.
  One WARN is expected: `orders_without_dq_issues` counts flagged orders (ADR-0005).
- **"source and target schemas are out of sync"** on an incremental model means its columns
  changed. If the change came from a shared macro, full-refresh **every** model using it:
  `.\make.ps1 dbt run --full-refresh --select <models>` (gotcha #35).
- After any silver change: `.\make.ps1 reconcile` (silver vs Postgres, row by row).

## Replayer and clickstream

- `REPLAY-FINGERPRINT-MISMATCH`: the source DB was replayed from other data, seed or rates.
  Reset the stack to change them.
- `REPLAY-CHAOS-RATE-CHANGED` / `CLICK-CHAOS-RATE-CHANGED`: resume with the rate the run
  started with (it is in `replay.checkpoint` / `replay.clickstream_checkpoint`).
- `.\make.ps1 replay-verify` (producers stopped): row counts and chaos ledger vs checkpoint.

## Drills

[FAILURE_DRILLS.md](FAILURE_DRILLS.md): kill Connect, kill Spark, restart the broker, with
evidence in `automation/runs/m2-s3/`.
