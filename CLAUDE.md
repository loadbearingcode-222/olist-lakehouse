PROTOCOL_MODE: learning

**Scope:** chartered work is a story from `docs/MILESTONES.md` (`M#-S#`) and gets the full
ritual: branch, PR, acceptance demo, evidence, handoff in the draft PR body. Ad-hoc questions in
this repo get the knowledge here and none of the ceremony.

@~/.claude/templates/UNIVERSAL_PROTOCOL.md
@~/.claude/templates/ENGINEERING_DOCTRINE.md

Protocol version in force at kickoff: **2.18** (2026-09-24). Doctrine: 1.0.

# olist-lakehouse

Local portfolio lakehouse. Olist e-commerce CSVs (Kaggle) are replayed into Postgres as a live
app → Debezium CDC → Kafka (plus a Faker clickstream) → Spark Structured Streaming → Iceberg
bronze on MinIO (GX gate → quarantine) → dbt-trino silver/gold (Lakekeeper REST catalog) →
Airflow 3 orchestration → Trino + Superset. The plan, layout and stories: `docs/MILESTONES.md`.
Traps: `docs/gotchas.md`. Decisions: `docs/adr/`.

## User decisions (verbatim, 2026-09-24)

- Repo location: "Windows folder" → `C:\Users\longt\data engineer\olist-lakehouse`.
- GitHub: "option 2, using loadbearingcode-222 identity" → private repo
  `loadbearingcode-222/olist-lakehouse`.
- Mode: "learning (Recommended)".
- PR labels: roles only, no discipline labels. First "this project will omit this part … just
  keep Each commit carries a Role: trailer. data engineer or data analyst in this case", then
  "I change my mind, please keeps role label for pull request". So: commit trailer
  `Role: data engineer` (M0–M4) / `Role: data analyst` (M5), plus the matching PR label
  `role:data engineer` / `role:data analyst`.
- WSL2 memory cap: 32 GB (was 40 GB), machine-wide default.
- Standing grant (learning mode, protocol §7): Claude opens AND merges its own story PR once
  CI is green and the story's Accept-when demo has been run and observed.

## Environment facts (observed 2026-09-24)

- Windows 11 Home, i7-12700H (14 cores), 64 GB RAM, WSL2 capped at 32 GB. Docker Desktop
  (WSL2 backend), Compose v5. `make` is NOT installed: `make.ps1` is the task runner.
- The repo path contains a space; quote every path. State lives in named volumes; only code
  and config are bind-mounted (C: bind mounts are slow through the WSL bridge).
- Host Python is 3.9; the project uses uv-managed Python 3.12 (`.python-version`).
- Kaggle CLI is configured on the host (`%USERPROFILE%\.kaggle\kaggle.json`, never copied).
- GitHub: the active `gh` account is a different one. Use `loadbearingcode-222` per command
  without switching: `$env:GH_TOKEN = gh auth token --user loadbearingcode-222` (PowerShell)
  or `GH_TOKEN=$(gh auth token --user loadbearingcode-222)` (bash). The repo-local git
  credential helper and `user.name`/`user.email` (noreply 329835776+loadbearingcode-222) are
  already set; global git and gh config are untouched.

## Fleet ledger (ports and RAM on this machine)

| Holder | Ports |
|---|---|
| native Postgres (Windows service) | 5432 |
| wikiflow / meridian | 5433, 8080, 9000, 9001, 8501 |
| wrenfield-retail | 8082, 5435, 3002, 5002, 8002 (+ 1xxxx prod) |
| **olist-lakehouse (this repo): 2xxxx family** | Postgres 25432 · Kafka 29092 · Connect 28083 · Console 28080 · MinIO 29000/29001 · Lakekeeper 28181 · Trino 28090 · Spark UI 24040/28070 · Airflow 28082 · Superset 28088 · Data Docs 28089 |

RAM budget: at most 20 GB of `mem_limit` in total, leaving about 12 GB of the 32 GB WSL share.
Bring other stacks down first; `.\make.ps1 doctor` (M0-S3) checks.

## Version pins (OBSERVED values, re-verify live before relying on them)

| Component | Pin | Checked |
|---|---|---|
| MinIO | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z.hotfix.7aa24e772` | 2026-09-24 |
| mc | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z` | 2026-09-24 |
| Postgres | `postgres:17.11-bookworm` | 2026-09-24 |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.13.6` | 2026-09-24 |
| Trino | `trinodb/trino:483` | 2026-09-24 |
| curl | `curlimages/curl:8.16.0` | 2026-09-24 |
| Kafka (KRaft) | `apache/kafka:4.3.1` | 2026-09-24 |
| Debezium Connect | `quay.io/debezium/connect:3.6.3.Final` (Kafka Connect 4.3.0) | 2026-09-24 |
| Redpanda Console | `docker.redpanda.com/redpandadata/console:v3.12.0` | 2026-09-24 |
| app image base | `python:3.12.14-slim-bookworm` + `ghcr.io/astral-sh/uv:0.11.25` | 2026-09-24 |
| psycopg / confluent-kafka | 3.3.6 / 2.15.1 | 2026-09-24 |
| Python / uv | 3.12.13 / 0.11.25 | 2026-09-24 |
| Spark | `apache/spark:4.1.3-scala2.13-java21-python3-ubuntu` (Hadoop 3.4.2) | 2026-09-24 |
| Iceberg (Spark) | `iceberg-spark-runtime-4.1_2.13` + `iceberg-aws-bundle` 1.11.0 | 2026-09-24 |
| Spark Kafka / S3A | `spark-sql-kafka-0-10_2.13:4.1.3`, `hadoop-aws:3.4.2`, AWS SDK `bundle:2.29.52` | 2026-09-24 |

## Commands

```
.\make.ps1 help        # every target
.\make.ps1 check       # lint + types + unit tests (what CI's quality job runs)
.\make.ps1 fetch       # Olist CSVs → data/raw/olist, verified against data/olist.sha256 + contract
.\make.ps1 up          # env + doctor + compose up --wait (lake core: MinIO, Lakekeeper, Trino)
.\make.ps1 test-integration   # round-trip through the running stack
.\make.ps1 reset -Force       # down -v: deletes every volume
.\make.ps1 replay            # replayer + clickstream in the background (REPLAY_SPEED 1440, CHAOS_RATE 0.02)
.\make.ps1 replay-stop       # graceful; resumes from replay.checkpoint on the next `replay`
.\make.ps1 replay-verify     # row counts + chaos ledger vs the checkpoint (stop it first)
```

CDC: topics `olist.olist.<table>` (3 partitions, 7-day retention), connector `olist-postgres`
(status: `curl http://127.0.0.1:28083/connectors/olist-postgres/status`), Console at
http://localhost:28080. Wire format: ADR-0002. Kafka CLI tools from Git Bash need
`MSYS_NO_PATHCONV=1` (gotcha #28).

Clickstream: topic `clickstream.events` (key = session_id), about 12 events per order, with
a watermark and chaos ledger in `replay.clickstream_*`. Chaos kinds are duplicate, late,
null_product and schema_drift (`src/olist_lakehouse/clickstream/chaos.py`).

Replayer facts: 866,582 events for the full dataset (2016-09-03 → 2018-10-17). At 1440× that is
about 13 h of wall time, less because quiet gaps are skipped. The chaos rate is fixed by the
first batch (`REPLAY-CHAOS-RATE-CHANGED`). Replaying different data or another seed needs a
fresh `source-db` volume. Host-side clients connect to `127.0.0.1`, never `localhost`
(gotcha #27).

Trino CLI inside the stack: `docker compose exec trino trino --catalog lake`.

Bronze: `bronze-stream` (Spark local[4]) writes `lake.bronze.cdc_events` and
`lake.bronze.clickstream_events`, with checkpoints in `s3a://lakehouse/checkpoints/bronze/`.
Spark UI is at http://localhost:24040. Exactly-once is by offset anti-join (ADR-0003).
`.\make.ps1 bronze-logs` tails it.

Data facts and hazards (BOM, quoted line breaks, non-unique review_id, customer vs person):
`docs/data/olist.md`. Reconciliation target: total `payment_value` = 16,008,872.12 BRL.

## Conventions

- One story → branch `feat/m#-s#-<slug>` → PR `M#-S#: <title>` → merge commit, in plan order.
  CI refuses anything else (`.github/scripts/story_lint.py`, `layout_lint.py`).
- A new path is declared in the layout in `docs/MILESTONES.md` by the story that adds it.
- Every commit ends with the `Role:` trailer, then the Co-Authored-By line.
- Close each story with a short field note in the PR body: what was built, why this way, the
  concept, what to look at, what to try.
