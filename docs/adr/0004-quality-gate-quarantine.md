# 0004: The quality gate: named rules, quarantine routing, GX as the second opinion
Date: 2026-09-24 · Story: M2-S2 · Status: accepted

## Context
Chaos injects known defects at 2% (the ledgers in `replay.*`). Bad rows must not reach the
tables silver reads, and every refusal must be explainable by name and reconcilable with the
ledgers. The plan named Great Expectations as the tool.

## Options
1. **GX as the gate itself:** row-level unexpected-index lists from GX decide routing.
   Heavy per batch on Spark, and it ties routing to GX's result format.
2. **Spark predicates only:** fast and simple, but one implementation checks itself.
3. **A rule catalog with two implementations:** Spark SQL predicates route rows (first
   matching rule), GX stock expectations recount every rule, and the two must agree per batch.

Also: where do bad rows go? (a) bronze keeps everything and quarantine holds flagged copies;
(b) bad rows are routed to quarantine only, and bronze tables hold only rows that passed.

## Decision
Option 3 with routing (b).
- `src/olist_lakehouse/quality/rules.py` states each rule once: name, scope, violation, and
  the chaos kind it answers to. Scoping makes one defect one quarantined message, because
  REPLICA IDENTITY FULL repeats a bad value on every later update. So `null_customer_id`
  judges inserts only, and `approved_before_purchase` judges only the message that changed
  `approved_at`.
- A row lands in exactly one place: its bronze table or `lake.bronze.quarantine` (with the
  rule). The idempotent anti-join checks both.
- `lake.bronze.quality_metrics` records, per batch and rule: rows in scope, gate violations,
  GX unexpected, quarantined, and agree.
- Duplicates and late events are **measured, not quarantined**. They are valid data, and
  silver handles them (dedup on event_id; the watermark).

## Cost (honest)
- GX runs once per rule per micro-batch. Its state is process-global (`ProjectManager`), so
  all validations are serialised behind one lock. That is invisible at this scale, and a
  ceiling at a much larger one.
- Quarantine is a table, not a workflow: there is no replay-after-fix path yet. A fixed row
  would be re-emitted by the source (CDC), not un-quarantined.
- The late-event measure is a lower bound: a one-event session or a session split across
  batches hides lateness from a batch-local check (6,183 of 6,352 seen).

## Revisit when
- A rule needs state across batches (then it belongs in silver, not the gate);
- or GX gains thread-safe contexts (then drop the lock);
- or a consumer needs to repair and re-release quarantined rows.

## Evidence (full clean replay, 2026-09-24)
- Quarantine per rule equals the ledgers exactly:
  - negative_price 2,320
  - null_customer_id 1,955
  - unknown_status 4,870
  - approved_before_purchase 979
  - missing_event_type 6,513 (ledger `schema_drift`)
  - null_product 5,023
- The gate and GX agree on every batch of the run.
- The red team (`.\make.ps1 gate-redteam`) routes 14 planted rows, one per rule plus clean
  and scoping controls, exactly as intended.
- Found and fixed along the way: gotcha #30. GX read as 0 when it had failed, because of a
  cross-thread race through GX's global state.
