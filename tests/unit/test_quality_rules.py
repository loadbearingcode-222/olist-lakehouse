"""The rule catalog and its GX twin: naming, routing order, and expectation semantics."""

from __future__ import annotations

import pandas as pd
import pytest

from olist_lakehouse.clickstream import chaos as click_chaos
from olist_lakehouse.quality.expect import Checker
from olist_lakehouse.quality.rules import RULES, quarantine_expression, rules_for
from olist_lakehouse.replayer import chaos as db_chaos


@pytest.fixture(scope="module")
def checker() -> Checker:
    return Checker("pandas")


def test_rule_names_are_unique_and_every_source_has_rules() -> None:
    names = [r.name for r in RULES]
    assert len(names) == len(set(names))
    assert rules_for("cdc") and rules_for("clickstream")


def test_every_quarantinable_chaos_kind_has_exactly_one_rule() -> None:
    """Duplicates and late events are valid data (handled in silver), so no rule quarantines them."""
    quarantinable = set(db_chaos.KINDS) | (set(click_chaos.KINDS) - {"duplicate", "late"})
    kinds = [r.chaos_kind for r in RULES]
    assert sorted(kinds) == sorted(quarantinable)


def test_the_quarantine_case_tries_rules_in_catalog_order() -> None:
    expression = quarantine_expression("clickstream")
    positions = [expression.index(f"'{r.name}'") for r in rules_for("clickstream")]
    assert positions == sorted(positions)
    assert expression.startswith("CASE WHEN") and expression.endswith("ELSE NULL END")
    assert "negative_price" not in expression  # cdc rules stay out of the clickstream gate


@pytest.mark.parametrize(
    ("rule", "rows", "expected"),
    [
        ("negative_price", {"a_price": [10.0, 0.0, -3.5, None]}, 1),
        ("null_customer_id", {"a_customer_id": ["c1", None, "c3"]}, 1),
        ("unknown_status", {"a_status": ["created", "shiped", "delivered", "crated"]}, 2),
        (
            "approved_before_purchase",
            {"a_approved_at": [100, 50, None, 200], "a_purchase_ts": [100, 80, 10, None]},
            1,
        ),
        ("missing_event_type", {"event_type_raw": ["page_view", None, "purchase"]}, 1),
        ("null_product", {"product_id": ["p1", None, None]}, 2),
    ],
)
def test_each_expectation_counts_exactly_the_violations(
    checker: Checker, rule: str, rows: dict[str, list[object]], expected: int
) -> None:
    assert checker.unexpected(rule, pd.DataFrame(rows)) == expected


def test_a_failed_validation_is_none_never_zero(checker: Checker) -> None:
    """GX reports an evaluation error inside the result; it must not read as "no violations"."""
    wrong_rows = pd.DataFrame({"a_status": ["created"]})  # null_product needs product_id
    assert checker.unexpected("null_product", wrong_rows) is None


def test_an_empty_scope_has_no_violations(checker: Checker) -> None:
    assert checker.unexpected("null_product", pd.DataFrame({"product_id": []})) == 0
