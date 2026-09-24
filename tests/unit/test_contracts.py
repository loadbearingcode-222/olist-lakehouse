"""The raw contract accepts the real shape and refuses every structural change by name."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from olist_lakehouse.contracts import COLUMNS, ContractError, check_directory, check_file

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "olist"


@pytest.fixture
def olist(tmp_path: Path) -> Path:
    target = tmp_path / "olist"
    shutil.copytree(FIXTURES, target)
    return target


def rewrite_header(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8-sig")
    header, rest = text.split("\n", 1)
    path.write_text(header.replace(old, new) + "\n" + rest, encoding="utf-8", newline="")


def test_every_declared_file_has_a_fixture() -> None:
    assert sorted(p.name for p in FIXTURES.iterdir()) == sorted(COLUMNS)


def test_the_fixture_set_passes_and_is_counted_by_record(olist: Path) -> None:
    reports = {r.file: r for r in check_directory(olist)}
    assert set(reports) == set(COLUMNS)
    assert all(r.columns == len(COLUMNS[r.file]) for r in reports.values())
    # The first review's comment holds a quoted CRLF: 2 records, more than 2 physical lines.
    reviews = olist / "olist_order_reviews_dataset.csv"
    assert reports[reviews.name].rows == 2
    assert reviews.read_bytes().count(b"\n") > 3


def test_a_byte_order_mark_is_not_part_of_the_first_column(olist: Path) -> None:
    path = olist / "product_category_name_translation.csv"
    assert path.read_bytes().startswith(b"\xef\xbb\xbf")
    assert check_file(path).rows == 1


def test_a_renamed_column_is_refused_as_missing_and_unknown(olist: Path) -> None:
    path = olist / "olist_orders_dataset.csv"
    rewrite_header(path, "order_status", "status")
    with pytest.raises(ContractError) as refused:
        check_file(path)
    assert refused.value.code == "CONTRACT-MISSING-COLUMN"
    assert "renamed?" in str(refused.value)


def test_an_extra_column_is_refused(olist: Path) -> None:
    path = olist / "olist_sellers_dataset.csv"
    path.write_text('"seller_id","seller_zip_code_prefix","seller_city","seller_state","x"\n')
    with pytest.raises(ContractError) as refused:
        check_file(path)
    assert refused.value.code == "CONTRACT-UNKNOWN-COLUMN"


def test_reordered_columns_are_refused(olist: Path) -> None:
    path = olist / "olist_sellers_dataset.csv"
    path.write_text('"seller_city","seller_id","seller_zip_code_prefix","seller_state"\n')
    with pytest.raises(ContractError) as refused:
        check_file(path)
    assert refused.value.code == "CONTRACT-COLUMN-ORDER"


def test_a_ragged_record_is_refused(olist: Path) -> None:
    path = olist / "olist_sellers_dataset.csv"
    with path.open("a", encoding="utf-8") as handle:
        handle.write("s2,13023,campinas\n")
    with pytest.raises(ContractError) as refused:
        check_file(path)
    assert refused.value.code == "CONTRACT-RAGGED-ROW"


def test_empty_unknown_and_missing_files_are_refused(olist: Path) -> None:
    empty = olist / "olist_sellers_dataset.csv"
    empty.write_text("")
    with pytest.raises(ContractError) as refused:
        check_file(empty)
    assert refused.value.code == "CONTRACT-EMPTY-FILE"

    stray = olist / "stray.csv"
    stray.write_text("a,b\n")
    with pytest.raises(ContractError) as refused:
        check_file(stray)
    assert refused.value.code == "CONTRACT-UNKNOWN-FILE"

    (olist / "olist_orders_dataset.csv").unlink()
    with pytest.raises(ContractError) as refused:
        check_directory(olist)
    assert refused.value.code == "CONTRACT-MISSING-FILE"
