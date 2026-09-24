# 0005: Silver: LSN-guarded CDC merge, soft deletes, and current-state quality flags
Date: 2026-09-24 · Story: M3-S1 · Status: accepted · Amends: ADR-0004 (see "the gate's reach")

## Context
Bronze holds every Debezium envelope once per Kafka offset, including Connect redeliveries,
tombstones and deletes. Silver must hold the source's current state per key, run
incrementally, and survive out-of-order and repeated changes, using dbt on Trino over Iceberg.

## Options
- **Rebuild silver from all of bronze every run:** simplest, always correct, cost grows with
  history.
- **Incremental merge by key:** cheap per run, but a plain merge lets whatever arrived last
  win, so a late redelivery can roll a row back, and dbt-trino's merge cannot delete.
- **dbt snapshots on bronze:** built for SCD2 history, not current state from a change log.

## Decision
Incremental merge, made safe by ordering on the source's own clock:
- **Staging** (ephemeral) parses each table's envelope once with a typed `CAST(json AS ROW)`,
  picks `before` or `after` **by op** (a delete's `after` is JSON null, gotcha #32), and
  collapses Kafka redeliveries by (LSN, key, op).
- **Silver** keeps, per key, the change with the highest LSN, and on merge only overwrites a
  stored row when the incoming LSN is higher. A late older change cannot roll a row back.
- **Deletes** are soft: `_is_deleted = true`, and the row keeps its last values.
- Each run re-reads `lookback_minutes` (30) behind the newest merged row. Overlap is harmless
  because the merge is deduplicated and LSN-guarded.
- **The clickstream** is deduplicated by `event_id` within each run's window and against what
  silver holds. Sessions are rebuilt in full; an event more than 2 h before its session's
  latest is `late` and does not move the session start.
- **The gate's reach (amends ADR-0004):** the bronze gate refuses the message that introduced
  a bad value. But with REPLICA IDENTITY FULL, every later update re-sends the whole row, so
  the bad value still arrives. Silver therefore judges the **current** row too, into
  `_dq_issues`, and gold decides what to do with flagged rows. Silver is not filtered: it
  mirrors the source.

## Cost (honest)
- LSN ordering holds within one Postgres timeline. A restored or promoted database would
  restart the WAL, and silver would need a rebuild.
- Soft deletes mean every consumer must filter `NOT _is_deleted`.
- `_dq_issues` duplicates two gate rules as current-state checks. They are named the same so
  the lineage is visible.

## Revisit when
- dbt-trino's merge supports `WHEN MATCHED ... DELETE` (then hard deletes become an option);
- or history is needed in silver (then SCD2 snapshots, as gold does for sellers and products
  in M3-S2).

## Evidence (2026-09-24)
- `.\make.ps1 reconcile` → PASS. Every live row and column equals Postgres. Only keys the
  chaos ledger marks as corrupted are set aside: 7,581 orders, of which 1,948 differ, all with
  a quarantined status misspelling on the order's last update; and 2,320 items. Reviews show
  98,716 live after 508 deletes. The clickstream went from 1,208,820 bronze rows to 1,202,337
  events: 6,483 duplicates removed, equal to the ledger.
- `tests/integration/test_silver_cdc.py`: reversed arrival, redelivery, delete + tombstone,
  and a late *older* change in an incremental run, all resolved by LSN.
- An incremental `dbt build` with no new bronze gives `MERGE (0 rows)` on every table.
- `_dq_issues`: 1,955 `null_customer_id` and 979 `approved_before_purchase`, equal to the
  ledger.
