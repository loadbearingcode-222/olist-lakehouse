# Failure drills

Break one component while data flows, recover it, and prove nothing was lost or landed twice.
Run with the stack up:

```powershell
.\make.ps1 drills        # ~5 minutes; writes automation/runs/m2-s3/<drill>.json
```

## How a drill works

Traffic is a **heartbeat**: numbered rows `(drill, seq)` written to `olist.drill_heartbeat`
every 50 ms, before, during and after the fault. The table lives in schema `olist`, so the CDC
publication carries it like app data. The replayer never writes it, `replay-verify` never
counts it, and silver never reads it.

```
write heartbeats ── 20 s ── FAULT ── recover ── 30 s ── stop ── wait until bronze has caught up
```

Two properties are then checked, and the verdict is `pass` only if both hold:

| Property | Meaning |
|---|---|
| `lost_events = 0` | every heartbeat Postgres committed is in bronze (distinct `seq` = written) |
| `offsets_landed_twice = 0` | across every topic, landed rows (bronze + quarantine) = Kafka offsets, each once |

**Redelivery is reported, not failed.** Kafka Connect is at-least-once. If it dies before
committing its source offsets, it re-sends those changes after restart as new Kafka messages
with new offsets. Bronze lands each offset exactly once, so a redelivered event appears twice
in bronze under two offsets. That is the job of silver's dedup (M3-S1: one row per
table + LSN + key), not bronze's. Hiding it here would hide it from silver's tests.

## The drills

| Drill | Fault | What it proves |
|---|---|---|
| `kill-connect` | `docker kill -s KILL` on Connect, then `start` | the replication slot keeps the WAL; Connect resumes from its last committed offset and re-sends the rest |
| `kill-spark` | `docker kill -s KILL` on `bronze-stream`, then `start` | the checkpoint in MinIO + the offset anti-join make the streaming sink exactly once |
| `restart-broker` | `docker compose restart kafka` | producers (Connect) and the consumer (Spark) ride through a broker restart |

## Latest results (2026-09-24)

Evidence: [`automation/runs/m2-s3/`](../automation/runs/m2-s3/).

| Drill | Heartbeats written | Events in bronze | Redelivered | Lost | Landed twice | Recovered in |
|---|---:|---:|---:|---:|---:|---:|
| kill-connect | 1,428 | 1,428 | 361 | 0 | 0 | 84.8 s |
| kill-spark | 1,030 | 1,030 | 0 | 0 | 0 | 61.5 s |
| restart-broker | 927 | 927 | 0 | 0 | 0 | 54.6 s |

"Recovered in" runs from the fault to the moment every heartbeat is in bronze and bronze has
drained Kafka. It includes the 30 s of post-fault writing, so it is an upper bound.

## Reading a failure

- `lost_events > 0`: check the connector status and the replication slot, in that order
  (RUNBOOK → CDC).
- `offsets_landed_twice > 0`: the sink's idempotency broke. Compare the Spark checkpoint's
  `commits/` with the table snapshots (RUNBOOK → Bronze).
- Not recovered after 7 minutes: the evidence is still printed; read `landed` vs `offsets`
  per topic.
