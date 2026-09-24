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

## Replayer and clickstream

- `REPLAY-FINGERPRINT-MISMATCH`: the source DB was replayed from other data, seed or rates.
  Reset the stack to change them.
- `REPLAY-CHAOS-RATE-CHANGED` / `CLICK-CHAOS-RATE-CHANGED`: resume with the rate the run
  started with (it is in `replay.checkpoint` / `replay.clickstream_checkpoint`).
- `.\make.ps1 replay-verify` (producers stopped): row counts and chaos ledger vs checkpoint.

## Drills

[FAILURE_DRILLS.md](FAILURE_DRILLS.md): kill Connect, kill Spark, restart the broker, with
evidence in `automation/runs/m2-s3/`.
