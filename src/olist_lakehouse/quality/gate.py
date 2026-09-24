"""The quality gate, in Spark: judge a micro-batch, route each row, and measure.

For each batch of fresh rows:
1. parse the columns the rules read (rules.CDC_PARSED / CLICK_PARSED);
2. name the first rule each row violates (rules.quarantine_expression). Rows with a rule go to
   quarantine and the rest go to bronze;
3. for every rule, count its scoped rows and violations twice, once with the gate's own
   predicate and once with Great Expectations (expect.Checker). The two must agree. If GX
   could not evaluate, its count is recorded as NULL and the row counts as a disagreement;
4. for the clickstream, also measure what is valid but notable: duplicate event_ids and late
   events. Silver handles those; nothing is quarantined for them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_lakehouse.quality.expect import Checker
from olist_lakehouse.quality.rules import (
    CDC_PARSED,
    CLICK_PARSED,
    quarantine_expression,
    rules_for,
)

PARSED = {"cdc": CDC_PARSED, "clickstream": CLICK_PARSED}
LATE_AFTER_SECONDS = 2 * 3600  # a session spans under an hour; late events arrive 3 h behind


@dataclass(frozen=True)
class Verdict:
    good: DataFrame  # bronze columns only
    bad: DataFrame  # bronze columns + rule
    metrics: list[dict[str, Any]]
    cached: DataFrame  # unpersist once good and bad are written

    def release(self) -> None:
        self.cached.unpersist()


def judge(batch: DataFrame, source: str, checker: Checker) -> Verdict:
    columns = batch.columns
    parsed = batch.select(
        "*", *[F.expr(expression).alias(name) for name, expression in PARSED[source].items()]
    ).withColumn("quarantine_rule", F.expr(quarantine_expression(source)))
    parsed.persist()

    rows = parsed.count()
    metrics: list[dict[str, Any]] = []
    for rule in rules_for(source):
        scoped = parsed.where(rule.scope)
        in_scope = scoped.count()
        gate = scoped.where(f"coalesce({rule.violation}, FALSE)").count() if in_scope else 0
        gx = checker.unexpected(rule.name, scoped) if in_scope else 0
        quarantined = parsed.where(F.col("quarantine_rule") == rule.name).count()
        metrics.append(
            {
                "rule": rule.name,
                "kind": "gate",
                "rows_in_batch": rows,
                "rows_in_scope": in_scope,
                "gate_violations": gate,
                "gx_unexpected": gx,
                "quarantined": quarantined,
                "agree": gx is not None and gate == gx,
            }
        )

    if source == "clickstream":
        metrics += _measures(parsed, rows)

    good = parsed.where("quarantine_rule IS NULL").select(*columns)
    bad = parsed.where("quarantine_rule IS NOT NULL").select(
        *columns, F.col("quarantine_rule").alias("rule")
    )
    return Verdict(good=good, bad=bad, metrics=metrics, cached=parsed)


def _measures(parsed: DataFrame, rows: int) -> list[dict[str, Any]]:
    """Valid-but-notable events in this batch: repeated event_ids and late arrivals."""
    distinct_ids = parsed.where("event_id IS NOT NULL").select("event_id").distinct().count()
    with_id = parsed.where("event_id IS NOT NULL").count()
    session_latest = parsed.groupBy("record_key").agg(F.max("event_ts").alias("latest"))
    late = (
        parsed.join(session_latest, "record_key")
        .where(
            F.col("event_ts").isNotNull()
            & (F.unix_timestamp("latest") - F.unix_timestamp("event_ts") > LATE_AFTER_SECONDS)
        )
        .count()
    )
    base = {"kind": "measure", "rows_in_batch": rows, "gx_unexpected": None, "quarantined": 0}
    return [
        {
            **base,
            "rule": "duplicate_event_id",
            "rows_in_scope": with_id,
            "gate_violations": with_id - distinct_ids,
            "agree": None,
        },
        {
            **base,
            "rule": "late_event",
            "rows_in_scope": rows,
            "gate_violations": late,
            "agree": None,
        },
    ]
