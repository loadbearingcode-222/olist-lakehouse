# 0009: Superset dashboards as code: an API builder, a committed export, import on `up`
Date: 2026-09-25 · Story: M5-S1 · Status: accepted

## Context
Superset keeps dashboards in its metadata database (Postgres `superset` on platform-db). A
dashboard clicked together in the UI is lost with that volume and cannot be reviewed in a PR.
The four dashboards must come back on fresh volumes with no clicking.

## Options
- **Hand-written export YAML:** reviewable, but chart `params` are undocumented internals and
  easy to get subtly wrong.
- **Clicked in the UI, then exported:** correct, but the source is a person's clicks.
- **Built through the REST API by a script, then exported:** the script is the source, and the
  export is its build output.

## Decision
- `scripts/superset_dashboards.py` (`.\make.ps1 superset-build`) defines the four dashboards
  (Revenue, Delivery SLA, Funnel, Stream health) and their 11 charts in Python. It creates the
  Trino connection and one dataset per gold mart, replaces the dashboards and their charts,
  and exports the bundle to `superset/dashboards/`.
- **Charts read only `lake.gold.mart_*`.** The marts are dbt models with tests
  (`models/gold/marts/`), so every number on a dashboard is covered by a dbt test and a
  contract, not by SQL living in Superset. The metrics are plain aggregates; ratios are
  weighted (`SUM(late_share * delivered_orders) / SUM(delivered_orders)`), never averages of
  averages.
- **`superset-init` imports the committed bundle on every `up`.** The import overwrites by
  UUID, so each start converges on what git holds.
- **No anonymous access.** Viewers log in as `admin` (`SUPERSET_ADMIN_PASSWORD` in `.env`).

## Cost (honest)
- Export file names carry database ids (`Orders_per_month_3.yaml`), so a rebuild renames
  files. Diffs are noisy, but the UUIDs inside stay stable.
- Edits made in the UI are overwritten by the next `up`. Change the script and rebuild.
- The bundle stores the connection URI (`trino://superset@trino:8080/lake`, no password: the
  local Trino has no authentication).

## Evidence (2026-09-25, `tests/integration/test_superset.py`)
The test drops the `superset` database, recreates it, runs `superset-init` and starts
Superset. All four dashboards are imported, and all 11 charts return rows through
`/api/v1/chart/{id}/data/` (`automation/runs/m5-s1/superset.json`).
