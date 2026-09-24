-- The streaming quality gate, per rule: what was judged, refused, and whether Great
-- Expectations agreed (gate rules), plus the measures silver deals with (duplicates, late).
select
    source,
    rule,
    kind,
    cast(count(*) as integer) as batches,
    cast(sum(rows_in_scope) as bigint) as rows_in_scope,
    cast(sum(gate_violations) as bigint) as gate_violations,
    cast(sum(gx_unexpected) as bigint) as gx_unexpected,
    cast(sum(quarantined) as bigint) as quarantined,
    coalesce(bool_and(agree), true) as gate_and_gx_agree
from {{ source('bronze', 'quality_metrics') }}
group by 1, 2, 3
