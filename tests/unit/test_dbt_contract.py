"""dbt's view of the app tables (macros/olist_schema.sql) matches the Postgres DDL, column for
column and in order. A column added to the app but not to dbt would silently never reach
silver; this is where that drift is caught."""

from __future__ import annotations

import re
from pathlib import Path

from olist_lakehouse.replayer.timeline import KEYS

REPO = Path(__file__).resolve().parents[2]
DDL = (REPO / "docker" / "postgres-source" / "10-schema.sql").read_text(encoding="utf-8")
MACRO = (REPO / "dbt" / "olist" / "macros" / "olist_schema.sql").read_text(encoding="utf-8")


def ddl_columns(table: str) -> list[str]:
    body = re.search(rf"CREATE TABLE olist\.{table} \((.*?)\n\);", DDL, re.S)
    assert body, f"no CREATE TABLE olist.{table} in the DDL"
    names = []
    for line in body.group(1).splitlines():
        line = line.strip()
        if line and not line.startswith(("--", "PRIMARY KEY")):
            names.append(line.split()[0])
    return names


def macro_columns(table: str) -> list[str]:
    body = re.search(rf"'{table}': \[(.*?)\]", MACRO, re.S)
    assert body, f"no '{table}' entry in olist_columns"
    return re.findall(r"\('(\w+)', '\w+'\)", body.group(1))


def test_every_app_table_is_declared_to_dbt_exactly() -> None:
    for table in KEYS:
        assert macro_columns(table) == ddl_columns(table), table


def test_the_contract_test_would_notice_a_missing_column() -> None:
    """Red team: drop one column from the macro text and the comparison must fail."""
    tampered = MACRO.replace("('freight_value', 'decimal')", "")
    body = re.search(r"'order_items': \[(.*?)\]", tampered, re.S)
    assert body is not None
    assert re.findall(r"\('(\w+)', '\w+'\)", body.group(1)) != ddl_columns("order_items")
