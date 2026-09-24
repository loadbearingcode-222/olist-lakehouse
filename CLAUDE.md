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
| (filled in M0-S3, ADR-0001) | | |

## Commands

```
.\make.ps1 help        # every target
.\make.ps1 check       # lint + types + unit tests (what CI's quality job runs)
.\make.ps1 fetch       # Olist CSVs → data/raw/olist, verified against data/olist.sha256 + contract
```

Data facts and hazards (BOM, quoted line breaks, non-unique review_id, customer vs person):
`docs/data/olist.md`. Reconciliation target: total `payment_value` = 16,008,872.12 BRL.

## Conventions

- One story → branch `feat/m#-s#-<slug>` → PR `M#-S#: <title>` → merge commit, in plan order.
  CI refuses anything else (`.github/scripts/story_lint.py`, `layout_lint.py`).
- A new path is declared in the layout in `docs/MILESTONES.md` by the story that adds it.
- Every commit ends with the `Role:` trailer, then the Co-Authored-By line.
- Close each story with a short field note in the PR body: what was built, why this way, the
  concept, what to look at, what to try.
