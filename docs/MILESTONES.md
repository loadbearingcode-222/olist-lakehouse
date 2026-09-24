# MILESTONES — the plan, in the order it merges

Two sections, in this order of priority: **the repo layout**, then **the stories**. Every pull
request is one story below, titled `M#-S#: <title>`, from branch `feat/m#-s#-<slug>`, merged
with a merge commit in the order listed. Nothing else becomes a pull request.

- A problem found after a story merged is fixed in the **next** story's PR, under "Also fixes".
- Only broken production earns a point story (`M1-S2.1`, branch `feat/m1-s2-1-<slug>`), added
  here in the PR that does the work. This is a local demo, so expect none.
- A path not declared in the layout arrives only through a story that edits the layout first.
- A milestone closes inside its **last** story, whose merge commit is tagged `v0.M.0`
  (`v1.0.0` for the final one).
- Every commit ends with a `Role:` trailer and every PR carries the matching label:
  `data engineer` for M0–M4, `data analyst` for M5.

Both structural rules are enforced in CI: `.github/scripts/story_lint.py` (branch, title, order)
and `.github/scripts/layout_lint.py` (every tracked path is declared below).

---

## Repo layout

Each path is tagged with the story that adds it. A directory with nothing listed under it may
hold any files; a directory whose contents are listed may hold only those.

```
olist-lakehouse/
├─ .github/
│  ├─ workflows/ci.yml                                  M0-S1  story-lint · layout-lint · ruff · mypy · pytest · compose render (M0-S3)
│  ├─ scripts/story_lint.py · scripts/layout_lint.py    M0-S1  ported from wikiflow-lakehouse
│  └─ pull_request_template.md                          M0-S1  Accept-when evidence · Also fixes
├─ .githooks/pre-push                                   M0-S1  refuses direct pushes to main
├─ README.md · LICENSE · .gitignore · .gitattributes    init   README grows each milestone
├─ CLAUDE.md                                            M0-S1  mode · scope · imports · env facts · fleet ledger · pins
├─ make.ps1                                             M0-S1  the single task runner; grows every story
├─ pyproject.toml · uv.lock · .python-version           M0-S1  uv, Python 3.12, ruff, mypy, pytest
├─ .pre-commit-config.yaml                              M0-S1
├─ .env.example                                         M0-S3  keys only
├─ .dockerignore                                        M1-S1  keeps data/ and .venv/ out of image builds
├─ compose.yaml                                         M0-S3  grows: lake core (M0) → cdc (M1) → spark (M2) → airflow (M4) → superset (M5)
├─ docker/
│  ├─ app/                                              M1-S1  Python image: replayer (M1-S1), clickstream producer (M1-S3)
│  ├─ postgres-source/                                  M1-S1  OLTP init SQL, wal_level=logical
│  ├─ connect/                                          M1-S2  Debezium Connect image + plugin pins
│  ├─ spark/                                            M2-S1  Iceberg + Kafka jars, GX, job entrypoint
│  ├─ airflow/                                          M4-S1  Airflow 3 image, dbt in an isolated venv
│  └─ superset/                                         M5-S1  Trino driver, superset_config.py
├─ config/
│  ├─ trino/                                            M0-S3  catalog lake.properties (Iceberg REST)
│  ├─ lakekeeper/                                       M0-S3  warehouse bootstrap
│  └─ connectors/                                       M1-S2  olist-postgres.json, Debezium config as code
├─ src/olist_lakehouse/
│  ├─ __init__.py · settings.py                         M0-S2
│  ├─ fetch.py · contracts.py                           M0-S2  Kaggle download · sha256 · raw CSV contract
│  ├─ replayer/                                         M1-S1  timeline · loader · chaos · checkpoint
│  ├─ clickstream/                                      M1-S3  sessions · producer · chaos ledger
│  ├─ streaming/                                        M2-S1  Kafka → Iceberg bronze job
│  ├─ quality/                                          M2-S2  GX suites · quarantine; batch checkpoint added M4-S2
│  ├─ fx.py                                             M3-S2  Frankfurter → bronze.fx_rates
│  └─ maintenance.py                                    M4-S2  Iceberg optimize · expire_snapshots via Trino
├─ dbt/olist/
│  ├─ dbt_project.yml · profiles.yml · packages.yml     M3-S1  dbt-trino
│  ├─ models/staging/ · models/silver/                  M3-S1
│  └─ snapshots/ · models/gold/                         M3-S2
├─ airflow/dags/                                        M4-S1  fx_rates_daily (S1) · lakehouse_build (S2)
├─ superset/dashboards/                                 M5-S1  exported YAML, imported on `up`
├─ data/olist.sha256                                    M0-S2  manifest only
├─ tests/
│  ├─ unit/                                             M0-S1  grows every story
│  ├─ fixtures/                                         M0-S2  declared hazard rows
│  └─ integration/                                      M0-S3  needs the stack; marked, run via make.ps1
├─ scripts/
│  ├─ doctor.ps1                                        M0-S3  port holders · Docker RAM · foreign stacks
│  ├─ reconcile.py                                      M3-S1  Postgres ↔ silver ↔ CSV
│  └─ demo_acceptance.py                                M5-S2  end-to-end gate → automation/runs/
├─ automation/runs/                                     M2-S3  evidence JSON, tracked
└─ docs/
   ├─ MILESTONES.md                                     M0-S1  this file (CI parses it)
   ├─ gotchas.md                                        M0-S1  seeded from GOTCHA_SEED.md
   ├─ adr/                                              M0-S1  one file per decision
   ├─ data/olist.md                                     M0-S2  dictionary · CC BY-NC-SA attribution · event vs processing time
   ├─ RUNBOOK.md · FAILURE_DRILLS.md                    M2-S3
   └─ architecture.md                                   M5-S2  mermaid diagram · screenshots
```

**Never in the repo:**
- `data/raw/` Olist CSVs (CC BY-NC-SA forbids redistribution; 126 MB)
- `.env`, `kaggle.json`, and any token or credential
- A handoff file: the draft PR body is the handoff
- `*.log` and run status files (only `automation/runs/**/*.json` evidence is tracked)
- `.venv/`, `__pycache__/`, `.idea/`, `.vscode/`, `.claude/`
- dbt `target/`, `logs/`, `dbt_packages/`; GX `uncommitted/` (generated Data Docs)
- Spark checkpoints and warehouse data (they live in MinIO volumes)
- Superset's metadata DB, Docker volumes, transcripts

---

## The stories

### M0-S1: Repo skeleton, plan and CI guards
Branch `feat/m0-s1-repo-skeleton` · Role: data engineer

Accept when:
- [ ] CI is green on this PR: story-lint, layout-lint, ruff, mypy, pytest.
- [ ] A planted bad branch name fails story-lint with `BRANCH-NAME`, and a planted undeclared
      file fails layout-lint with `UNDECLARED-PATH` (red-teamed, then reverted).
- [ ] `.\make.ps1 check` runs ruff, mypy and pytest locally and passes.
- [ ] The pre-push hook refuses a push to `main`, and is tracked with mode `100755`.

### M0-S2: Fetch Olist and pin its contract
Branch `feat/m0-s2-olist-fetch` · Role: data engineer

Accept when:
- [ ] `.\make.ps1 fetch` downloads all 9 CSVs through the Kaggle CLI into `data/raw/olist/`,
      and every sha256 matches `data/olist.sha256`.
- [ ] The contract reports each file's row count (orders = 99,441).
- [ ] A planted renamed column is refused and nothing is written.

### M0-S3: Lake core: MinIO, Iceberg REST catalog, Trino
Branch `feat/m0-s3-lake-core` · Role: data engineer · **Release `v0.0.0`**

Accept when:
- [ ] `.\make.ps1 doctor` reports no port or RAM conflicts, naming the holder of any busy port.
- [ ] `.\make.ps1 up` brings every service to healthy.
- [ ] A Trino `CREATE TABLE … INSERT … SELECT` round-trips through Lakekeeper into MinIO.
- [ ] ADR-0001 records the image pins, checked live and dated.

### M1-S1: OLTP schema and time-compressed replayer
Branch `feat/m1-s1-replayer` · Role: data engineer

Accept when:
- [ ] At 1440× speed, orders move created → approved → shipped → delivered in Postgres.
- [ ] Killed and restarted, the replayer resumes with no duplicate rows.
- [ ] With `CHAOS_RATE>0`, each injected defect is written to the chaos ledger.

### M1-S2: Kafka, Debezium and Console
Branch `feat/m1-s2-debezium-cdc` · Role: data engineer

Accept when:
- [ ] The connector registers idempotently from `config/connectors/`.
- [ ] Per-table topic counts equal Postgres insert, update and delete counts over a fixed window.
- [ ] Restarting Connect loses nothing.

### M1-S3: Clickstream producer
Branch `feat/m1-s3-clickstream` · Role: data engineer · **Release `v0.1.0`**

Accept when:
- [ ] `clickstream.events` flows, keyed by session.
- [ ] Every purchase event maps to a real order_id (test).
- [ ] Dupes, late events, nulls and schema drift appear at the configured rate.

### M2-S1: Spark Structured Streaming into Iceberg bronze
Branch `feat/m2-s1-bronze-stream` · Role: data engineer

Accept when:
- [ ] Bronze row count equals the Kafka offsets consumed.
- [ ] After killing and restarting the job, `count(*) = count(distinct topic, partition, offset)`.
- [ ] The streaming checkpoint is stored in MinIO.

### M2-S2: GX micro-batch gate and quarantine
Branch `feat/m2-s2-gx-quarantine` · Role: data engineer

Accept when:
- [ ] With `CHAOS_RATE=0.02`, quarantine counts per rule reconcile with the chaos ledger.
- [ ] A planted defect is refused under its own rule name.
- [ ] A per-batch quality metrics table exists.

### M2-S3: Failure drills and runbook
Branch `feat/m2-s3-failure-drills` · Role: data engineer · **Release `v0.2.0`**

Accept when:
- [ ] Killing Connect, killing Spark and restarting the broker each recover with zero loss and
      zero duplicates.
- [ ] The drill evidence JSON is committed under `automation/runs/`.

### M3-S1: Silver: CDC merge and sessionized clickstream
Branch `feat/m3-s1-silver` · Role: data engineer

Accept when:
- [ ] `reconcile.py` shows silver current state equals Postgres (row counts and statuses 100%),
      including deletes and out-of-order events.
- [ ] Clickstream events are deduplicated by `event_id` and sessionized.
- [ ] dbt tests are green.

### M3-S2: SCD2 snapshots, FX and gold star schema
Branch `feat/m3-s2-gold-star` · Role: data engineer · **Release `v0.3.0`**

Accept when:
- [ ] Seller and product SCD2 history reflects the replayer's mutations.
- [ ] Gold `SUM(payment_value)` equals the raw CSV recomputation (±0.01).
- [ ] Revenue in USD uses `fx_rates`; dbt model contracts are enforced on gold.

### M4-S1: Airflow stack and FX DAG with backfill
Branch `feat/m4-s1-airflow-fx` · Role: data engineer

Accept when:
- [ ] A backfill over 2016-09 → 2018-10 fills every ECB business day.
- [ ] Rerunning a date adds no rows.
- [ ] A DAG-import test runs in CI.

### M4-S2: lakehouse_build DAG
Branch `feat/m4-s2-lakehouse-build` · Role: data engineer · **Release `v0.4.0`**

Accept when:
- [ ] GX bronze checkpoint → dbt silver → dbt gold → Iceberg maintenance → Data Docs succeeds
      end to end.
- [ ] A planted failing expectation stops gold, and Data Docs shows why.

### M5-S1: Superset dashboards as code
Branch `feat/m5-s1-superset` · Role: data analyst

Accept when:
- [ ] `up` on fresh volumes imports 4 dashboards (Revenue BRL/USD, Delivery SLA, Funnel, Stream
      health), each showing data.

### M5-S2: One-command demo and front door
Branch `feat/m5-s2-demo` · Role: data analyst · **Release `v1.0.0`**

Accept when:
- [ ] From a clean clone, `.\make.ps1 demo` passes `demo_acceptance.py` and commits its
      evidence JSON.
- [ ] The README shows the architecture, screenshots and a link to the gotchas.
