# 0006: Gold: star schema, SCD2 from the CDC log, FX as of the purchase day
Date: 2026-09-24 · Story: M3-S2 · Status: accepted

## Context
Gold is what dashboards and analysts read (M5). It needs conformed dimensions with history
(where did a seller sit *when* the sale happened?), money in BRL and USD, and a fixed schema
consumers can rely on.

## Options
- **History:** dbt snapshots on silver (the textbook SCD2), or versions derived from the CDC
  change log.
- **FX:** convert at query time in each dashboard, or once in the facts at the purchase day's
  rate.
- **Flagged rows** (`_dq_issues`, ADR-0005): drop them from gold, or keep them with a trust
  flag.

## Decision
- **SCD2 from the CDC log, not snapshots.** A snapshot records what a table looks like when
  the snapshot runs, so it can only capture history from then on, at its own cadence. The
  replay had already finished, so snapshots would have seen only final states. The change log
  already holds every version with its business time (`updated_at`). `dim_seller` and
  `dim_product` are one row per version, with `valid_from`/`valid_to` in business time, and
  facts join the version valid at `purchase_ts`. The `snapshots/` path left the layout.
- **FX once, in the facts.** `olist_lakehouse.fx` merges ECB rates (via frankfurter.dev) into
  `bronze.fx_rates`, and `silver.fx_brl_usd_daily` carries each rate over weekends and
  holidays. Facts store the rate used and the USD amount, so every dashboard shows the same
  dollars.
- **Flagged rows stay, marked.** `is_trusted = false` on orders silver flagged. Totals
  reconcile with the source; metrics decide whether to include flagged rows.
- **Contracts are enforced** on every gold model (column names and types), and CI parses
  the project.

## Cost (honest)
- Rebuilding the dimensions from the full change log each run is fine at this size (tens of
  thousands of versions). At scale it would become incremental, or snapshot-based going
  forward.
- ECB rates are daily reference rates, not transaction rates. A weekend purchase uses
  Friday's rate.
- An order whose customer_id was lost (flagged) has no `customer_unique_id` in facts. It is
  counted in revenue, not in customer metrics.

## Evidence (2026-09-24, `tests/integration/test_gold.py` and the dbt build)
- `dim_seller`: 3,095 sellers, 3,166 versions, 71 changed. `dim_product`: 32,951 products,
  33,908 versions, 957 changed. Exactly the replayer's mutations, recomputed from its timeline
  (which entities changed, and how often). SCD2 tiling holds: one current version, no gaps,
  no overlaps.
- `SUM(fct_payments.payment_value_brl)` = 16,008,872.12 = the raw CSV, recomputed.
- Every order's rate is the ECB rate published on or before its purchase day, and every
  USD amount equals `round(BRL × rate, 2)`. That check found a double-rounding bug from
  decimal scale overflow (gotcha #34).
- Contract red team: `year` cast to `bigint` makes dbt refuse the model with a
  `data type mismatch` table, before anything is written.
