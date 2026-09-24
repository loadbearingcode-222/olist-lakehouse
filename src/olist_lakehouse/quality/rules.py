"""The rule catalog: every quality rule, stated once.

A rule has a *scope* (which rows it judges) and a *violation* (what makes a judged row bad),
both as Spark SQL over the parsed columns below. The gate quarantines a row under the FIRST
rule it violates, in catalog order; metrics count every rule independently. Each rule names
the chaos kind it answers to, so quarantine counts can be reconciled with the ledgers.

Scoping is what makes one defect one quarantined message:
- a row keeps a bad value after the defect (REPLICA IDENTITY FULL sends the whole row on every
  later update), so `null_customer_id` only judges inserts, and `approved_before_purchase` only
  judges the message that *changed* approved_at (before vs after differ);
- `unknown_status` judges every status the app writes, because each misspelling is its own
  update.

Duplicates and late events are not rules: they are valid data, measured here and handled in
silver (dedup on event_id, and the watermark).
"""

from __future__ import annotations

from dataclasses import dataclass

ORDER_STATUSES = (
    "approved",
    "canceled",
    "created",
    "delivered",
    "invoiced",
    "processing",
    "shipped",
    "unavailable",
)

# Parsed columns the rules read. Values are Spark SQL over the bronze row (payload is the raw
# JSON string). Debezium sends timestamps as epoch microseconds and decimals as strings.
CDC_PARSED = {
    "a_price": "CAST(get_json_object(payload, '$.after.price') AS DECIMAL(12, 2))",
    "a_customer_id": "get_json_object(payload, '$.after.customer_id')",
    "a_status": "get_json_object(payload, '$.after.order_status')",
    "a_approved_at": "CAST(get_json_object(payload, '$.after.approved_at') AS BIGINT)",
    "b_approved_at": "CAST(get_json_object(payload, '$.before.approved_at') AS BIGINT)",
    "a_purchase_ts": "CAST(get_json_object(payload, '$.after.purchase_ts') AS BIGINT)",
}
CLICK_PARSED = {
    "event_type_raw": "get_json_object(payload, '$.event_type')",
    "product_id": "get_json_object(payload, '$.product_id')",
}


@dataclass(frozen=True)
class Rule:
    name: str
    source: str  # "cdc" | "clickstream"
    scope: str  # Spark SQL predicate: rows this rule judges
    violation: str  # Spark SQL predicate: a judged row is bad when this is TRUE
    chaos_kind: str  # the ledger kind this rule answers to


_STATUSES = ", ".join(f"'{s}'" for s in ORDER_STATUSES)

RULES: tuple[Rule, ...] = (
    Rule(
        "negative_price",
        "cdc",
        "source_table = 'order_items' AND op IN ('c', 'r')",
        "a_price < 0",
        "negative_price",
    ),
    Rule(
        "null_customer_id",
        "cdc",
        "source_table = 'orders' AND op IN ('c', 'r')",
        "a_customer_id IS NULL",
        "null_customer_id",
    ),
    Rule(
        "unknown_status",
        "cdc",
        "source_table = 'orders' AND op IN ('c', 'u', 'r')",
        f"a_status NOT IN ({_STATUSES})",
        "unknown_status",
    ),
    Rule(
        "approved_before_purchase",
        "cdc",
        "source_table = 'orders' AND op IN ('c', 'u', 'r')"
        " AND a_approved_at IS NOT NULL AND NOT (a_approved_at <=> b_approved_at)",
        "a_approved_at < a_purchase_ts",
        "approved_before_purchase",
    ),
    Rule(
        "missing_event_type",
        "clickstream",
        "TRUE",
        "event_type_raw IS NULL",
        "schema_drift",
    ),
    Rule(
        "null_product",
        "clickstream",
        "event_type IN ('page_view', 'add_to_cart')",
        "product_id IS NULL",
        "null_product",
    ),
)


def rules_for(source: str) -> tuple[Rule, ...]:
    return tuple(r for r in RULES if r.source == source)


def quarantine_expression(source: str) -> str:
    """CASE expression naming the first rule a row violates, or NULL when it passes them all.

    A NULL violation (e.g. a price that is missing, not negative) does not quarantine: the rule
    only fires on TRUE. That matches Great Expectations, which ignores missing values.
    """
    branches = " ".join(
        f"WHEN ({r.scope}) AND coalesce({r.violation}, FALSE) THEN '{r.name}'"
        for r in rules_for(source)
    )
    return f"CASE {branches} ELSE NULL END"
