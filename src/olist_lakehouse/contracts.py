"""The raw CSV contract: which files exist and exactly which columns each one has.

The contract refuses structure. A missing, unknown or reordered column refuses the whole file,
and the caller writes nothing. Each refusal has its own code so a failure says which rule broke:

    CONTRACT-UNKNOWN-FILE     a file name the contract does not declare
    CONTRACT-MISSING-FILE     a declared file is absent from the directory
    CONTRACT-EMPTY-FILE       no header row at all
    CONTRACT-MISSING-COLUMN   a declared column is absent (a rename shows as missing + unknown)
    CONTRACT-UNKNOWN-COLUMN   a column the contract does not declare
    CONTRACT-COLUMN-ORDER     the right columns in the wrong order
    CONTRACT-RAGGED-ROW       a record whose field count differs from the header

Records are counted with a CSV parser, never by lines: review comments contain quoted line
breaks (99,224 records span 104,719 physical lines). Files are read as `utf-8-sig` because the
category translation file starts with a byte-order mark.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

COLUMNS: dict[str, tuple[str, ...]] = {
    "olist_customers_dataset.csv": (
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ),
    "olist_geolocation_dataset.csv": (
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ),
    "olist_order_items_dataset.csv": (
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ),
    "olist_order_payments_dataset.csv": (
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ),
    "olist_order_reviews_dataset.csv": (
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
    ),
    "olist_orders_dataset.csv": (
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ),
    "olist_products_dataset.csv": (
        "product_id",
        "product_category_name",
        # Misspelled in the source ("lenght"). Kept verbatim here; staging renames it.
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ),
    "olist_sellers_dataset.csv": (
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ),
    "product_category_name_translation.csv": (
        "product_category_name",
        "product_category_name_english",
    ),
}


class ContractError(Exception):
    def __init__(self, code: str, file: str, message: str) -> None:
        super().__init__(f"{code}: {file}: {message}")
        self.code = code
        self.file = file


@dataclass(frozen=True)
class FileReport:
    file: str
    columns: int
    rows: int


def check_header(file: str, header: list[str]) -> None:
    expected = COLUMNS.get(file)
    if expected is None:
        raise ContractError("CONTRACT-UNKNOWN-FILE", file, "not declared in the contract")
    missing = [c for c in expected if c not in header]
    unknown = [c for c in header if c not in expected]
    if missing:
        detail = f"missing {missing}" + (f", unknown {unknown} (renamed?)" if unknown else "")
        raise ContractError("CONTRACT-MISSING-COLUMN", file, detail)
    if unknown:
        raise ContractError("CONTRACT-UNKNOWN-COLUMN", file, f"unknown {unknown}")
    if tuple(header) != expected:
        raise ContractError("CONTRACT-COLUMN-ORDER", file, f"got {header}, expected {expected}")


def check_file(path: Path) -> FileReport:
    """Validate one CSV against the contract and count its records."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise ContractError("CONTRACT-EMPTY-FILE", path.name, "no header row")
        check_header(path.name, header)
        rows = 0
        for rows, record in enumerate(reader, start=1):
            if len(record) != len(header):
                raise ContractError(
                    "CONTRACT-RAGGED-ROW",
                    path.name,
                    f"record {rows} has {len(record)} fields, header has {len(header)}",
                )
    return FileReport(file=path.name, columns=len(header), rows=rows)


def check_directory(directory: Path) -> list[FileReport]:
    """Validate every declared file in `directory`; the first refusal stops the whole check."""
    reports = []
    for file in sorted(COLUMNS):
        path = directory / file
        if not path.is_file():
            raise ContractError("CONTRACT-MISSING-FILE", file, f"not found in {directory}")
        reports.append(check_file(path))
    return reports
