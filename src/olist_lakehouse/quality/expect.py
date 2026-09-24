"""The rule catalog as Great Expectations expectations: an independent second opinion.

The gate decides with its own Spark SQL predicates (rules.py). GX re-checks each rule's scoped
rows with a stock expectation, so the violation logic is implemented twice by different code.
Per batch, the two counts must agree. A disagreement is recorded in the metrics table, and
it means one of the two implementations is wrong.

Works on any GX datasource. The gate uses Spark; unit tests use pandas.
"""

from __future__ import annotations

import threading
from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe

from olist_lakehouse.quality.rules import ORDER_STATUSES

# GX resolves datasources and validators through a process-wide singleton (ProjectManager:
# "the most recently created context"), so separate contexts do not isolate threads. Every
# validation in the process goes through this one lock (gotcha #30).
_GX_LOCK = threading.Lock()


def expectation_for(rule_name: str) -> Any:
    """The stock GX expectation that fails exactly on the rule's violating (scoped) rows."""
    table: dict[str, Any] = {
        "negative_price": gxe.ExpectColumnValuesToBeBetween(column="a_price", min_value=0),
        "null_customer_id": gxe.ExpectColumnValuesToNotBeNull(column="a_customer_id"),
        "unknown_status": gxe.ExpectColumnValuesToBeInSet(
            column="a_status", value_set=list(ORDER_STATUSES)
        ),
        "approved_before_purchase": gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="a_approved_at",
            column_B="a_purchase_ts",
            or_equal=True,
            ignore_row_if="either_value_is_missing",
        ),
        "missing_event_type": gxe.ExpectColumnValuesToNotBeNull(column="event_type_raw"),
        "null_product": gxe.ExpectColumnValuesToNotBeNull(column="product_id"),
    }
    return table[rule_name]


class Checker:
    """One ephemeral GX context for the whole process, shared by every streaming query.

    Thread-safe only through _GX_LOCK: `get_batch` swaps the dataframe behind the batch
    definition, and GX looks its datasource up through global state. Unlocked, two queries
    raced and GX validated one query's rows against the other query's rule (gotcha #30).
    """

    def __init__(self, engine: str) -> None:
        self.context = gx.get_context(mode="ephemeral")
        if engine == "spark":
            source = self.context.data_sources.add_spark(name="gate")
        else:
            source = self.context.data_sources.add_pandas(name="gate")
        self.batch_definition = source.add_dataframe_asset(
            name="scoped_rows"
        ).add_batch_definition_whole_dataframe("whole")

    def unexpected(self, rule_name: str, scoped_rows: Any) -> int | None:
        """How many of `scoped_rows` violate the rule, according to GX, or None if GX failed.

        A failed validation must never read as "no violations": GX reports an exception inside
        the result rather than raising, and its counts are then simply absent.
        """
        with _GX_LOCK:
            batch = self.batch_definition.get_batch(batch_parameters={"dataframe": scoped_rows})
            result = batch.validate(expectation_for(rule_name), result_format="SUMMARY")
        info = result.exception_info or {}
        if info.get("raised_exception") or "unexpected_count" not in result.result:
            message = str(info.get("exception_message", "no unexpected_count in the result"))
            print(f"quality: GX could not evaluate {rule_name}: {message[:300]}", flush=True)
            return None
        return int(result.result["unexpected_count"])
