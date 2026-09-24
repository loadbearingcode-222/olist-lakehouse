"""Read the raw Olist CSVs into the app's row shapes.

This is the one place a raw column is renamed or cast (DAY_ONE "one cast, in one place"):
- `product_name_lenght` → `product_name_length` (and the description twin);
- `order_purchase_timestamp` → `purchase_ts`, `order_approved_at` → `approved_at`, …;
- empty strings → None; timestamps → naive datetime; money → Decimal.

Geolocation is reduced to one point per zip prefix (19,015 rows from 1,000,163): the mean
lat/lng, and the most common city/state spelling (ties broken alphabetically).
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from olist_lakehouse.contracts import check_directory

Row = dict[str, Any]


@dataclass(frozen=True)
class OlistData:
    categories: list[Row]
    geolocation: list[Row]
    sellers: list[Row]
    products: list[Row]
    customers: dict[str, Row]
    orders: list[Row]
    items: dict[str, list[Row]]
    payments: dict[str, list[Row]]
    reviews: list[Row]


def _rows(directory: Path, name: str) -> Iterator[dict[str, str]]:
    with (directory / name).open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def _ts(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _int(value: str) -> int | None:
    return int(value) if value else None


def _text(value: str) -> str | None:
    return value if value else None


def _mode(counter: Counter[str]) -> str:
    return min(counter.items(), key=lambda kv: (-kv[1], kv[0]))[0]


def load(directory: Path) -> OlistData:
    """Validate the directory against the contract, then load it."""
    check_directory(directory)

    categories = [
        {
            "category_name": r["product_category_name"],
            "category_name_english": r["product_category_name_english"],
        }
        for r in _rows(directory, "product_category_name_translation.csv")
    ]

    sums: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
    cities: dict[str, Counter[str]] = defaultdict(Counter)
    states: dict[str, Counter[str]] = defaultdict(Counter)
    for r in _rows(directory, "olist_geolocation_dataset.csv"):
        zip_prefix = r["geolocation_zip_code_prefix"]
        acc = sums[zip_prefix]
        acc[0] += float(r["geolocation_lat"])
        acc[1] += float(r["geolocation_lng"])
        acc[2] += 1
        cities[zip_prefix][r["geolocation_city"]] += 1
        states[zip_prefix][r["geolocation_state"]] += 1
    geolocation = [
        {
            "zip_code_prefix": zip_prefix,
            "lat": round(lat / n, 6),
            "lng": round(lng / n, 6),
            "city": _mode(cities[zip_prefix]),
            "state": _mode(states[zip_prefix]),
            "samples": int(n),
        }
        for zip_prefix, (lat, lng, n) in sorted(sums.items())
    ]

    sellers = [
        {
            "seller_id": r["seller_id"],
            "zip_code_prefix": r["seller_zip_code_prefix"],
            "city": r["seller_city"],
            "state": r["seller_state"],
        }
        for r in _rows(directory, "olist_sellers_dataset.csv")
    ]

    products = [
        {
            "product_id": r["product_id"],
            "category_name": _text(r["product_category_name"]),
            "product_name_length": _int(r["product_name_lenght"]),
            "product_description_length": _int(r["product_description_lenght"]),
            "product_photos_qty": _int(r["product_photos_qty"]),
            "product_weight_g": _int(r["product_weight_g"]),
            "product_length_cm": _int(r["product_length_cm"]),
            "product_height_cm": _int(r["product_height_cm"]),
            "product_width_cm": _int(r["product_width_cm"]),
        }
        for r in _rows(directory, "olist_products_dataset.csv")
    ]

    customers = {
        r["customer_id"]: {
            "customer_id": r["customer_id"],
            "customer_unique_id": r["customer_unique_id"],
            "zip_code_prefix": r["customer_zip_code_prefix"],
            "city": r["customer_city"],
            "state": r["customer_state"],
        }
        for r in _rows(directory, "olist_customers_dataset.csv")
    }

    orders = [
        {
            "order_id": r["order_id"],
            "customer_id": r["customer_id"],
            "order_status": r["order_status"],
            "purchase_ts": _ts(r["order_purchase_timestamp"]),
            "approved_at": _ts(r["order_approved_at"]),
            "delivered_carrier_at": _ts(r["order_delivered_carrier_date"]),
            "delivered_customer_at": _ts(r["order_delivered_customer_date"]),
            "estimated_delivery_at": _ts(r["order_estimated_delivery_date"]),
        }
        for r in _rows(directory, "olist_orders_dataset.csv")
    ]

    items: dict[str, list[Row]] = defaultdict(list)
    for r in _rows(directory, "olist_order_items_dataset.csv"):
        items[r["order_id"]].append(
            {
                "order_id": r["order_id"],
                "order_item_id": int(r["order_item_id"]),
                "product_id": r["product_id"],
                "seller_id": r["seller_id"],
                "shipping_limit_at": _ts(r["shipping_limit_date"]),
                "price": Decimal(r["price"]),
                "freight_value": Decimal(r["freight_value"]),
            }
        )

    payments: dict[str, list[Row]] = defaultdict(list)
    for r in _rows(directory, "olist_order_payments_dataset.csv"):
        payments[r["order_id"]].append(
            {
                "order_id": r["order_id"],
                "payment_sequential": int(r["payment_sequential"]),
                "payment_type": r["payment_type"],
                "payment_installments": int(r["payment_installments"]),
                "payment_value": Decimal(r["payment_value"]),
            }
        )

    reviews = [
        {
            "review_id": r["review_id"],
            "order_id": r["order_id"],
            "review_score": int(r["review_score"]),
            "review_comment_title": _text(r["review_comment_title"]),
            "review_comment_message": _text(r["review_comment_message"]),
            "review_creation_at": _ts(r["review_creation_date"]),
            "review_answer_at": _ts(r["review_answer_timestamp"]),
        }
        for r in _rows(directory, "olist_order_reviews_dataset.csv")
    ]

    return OlistData(
        categories=categories,
        geolocation=geolocation,
        sellers=sellers,
        products=products,
        customers=customers,
        orders=orders,
        items=dict(items),
        payments=dict(payments),
        reviews=reviews,
    )
