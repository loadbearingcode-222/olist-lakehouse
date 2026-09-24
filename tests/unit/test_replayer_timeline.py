"""The replay timeline: lifecycle order, clamping, final states, determinism, chaos."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from olist_lakehouse.replayer import chaos, source, timeline
from olist_lakehouse.replayer.timeline import Event, MutationRates

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "olist"
NO_MUTATIONS = MutationRates(0.0, 0.0, 0.0)


@pytest.fixture(scope="module")
def data() -> source.OlistData:
    return source.load(FIXTURES)


def final_state(events: list[Event]) -> dict[tuple[str, tuple[object, ...]], dict[str, object]]:
    """Fold the events the way the database would, keyed by (table, primary key)."""
    rows: dict[tuple[str, tuple[object, ...]], dict[str, object]] = {}
    for e in events:
        key = (e.table, tuple(e.key.values()))
        if e.op == "insert":
            assert key not in rows, f"duplicate insert {key}"
            rows[key] = dict(e.values)
        elif e.op == "update":
            rows[key].update(e.values)
        else:
            del rows[key]
    return rows


def test_source_renames_and_casts_in_one_place(data: source.OlistData) -> None:
    product = next(p for p in data.products if p["product_id"].endswith("1"))
    assert product["product_name_length"] == 40
    assert data.items["o0000000000000000000000000000001"][0]["price"] == Decimal("29.99")
    assert data.geolocation == [
        {
            "zip_code_prefix": "01310",
            "lat": -23.5615,
            "lng": -46.65595,
            "city": "sao paulo",  # a 1-1 tie between spellings breaks alphabetically
            "state": "SP",
            "samples": 2,
        }
    ]
    assert data.categories[0]["category_name"] == "perfumaria"  # the BOM is gone


def test_events_are_sorted_and_reference_data_comes_first(data: source.OlistData) -> None:
    events = timeline.build(data, seed=1, rates=NO_MUTATIONS)
    assert [e.ts for e in events] == sorted(e.ts for e in events)
    first_order = next(i for i, e in enumerate(events) if e.table == "orders")
    assert {e.table for e in events[:first_order]} >= {"sellers", "products"}
    assert events[first_order - 1].table == "customers"  # FK parent right before the order


def test_a_delivered_order_walks_the_whole_lifecycle(data: source.OlistData) -> None:
    events = timeline.build(data, seed=1, rates=NO_MUTATIONS)
    statuses = [
        e.values["order_status"]
        for e in events
        if e.table == "orders" and e.key["order_id"].endswith("1")
    ]
    assert statuses == ["created", "approved", "shipped", "delivered"]


def test_every_order_ends_exactly_as_the_csv_says(data: source.OlistData) -> None:
    state = final_state(timeline.build(data, seed=1, rates=NO_MUTATIONS))
    for order in data.orders:
        row = state[("orders", (order["order_id"],))]
        for column in (
            "order_status",
            "approved_at",
            "delivered_carrier_at",
            "delivered_customer_at",
            "customer_id",
        ):
            assert row[column] == order[column], (order["order_id"], column)


def test_a_canceled_order_gets_its_final_status_an_hour_later(data: source.OlistData) -> None:
    events = [
        e
        for e in timeline.build(data, seed=1, rates=NO_MUTATIONS)
        if e.table == "orders" and e.key["order_id"].endswith("2")
    ]
    assert [e.values["order_status"] for e in events] == ["created", "canceled"]
    assert events[1].ts - events[0].ts == timedelta(hours=1)


def test_out_of_order_source_timestamps_are_clamped_not_rewritten() -> None:
    purchase = datetime(2018, 1, 1, 10)
    order = {
        "order_id": "o",
        "customer_id": "c",
        "order_status": "delivered",
        "purchase_ts": purchase,
        "estimated_delivery_at": None,
        "approved_at": purchase + timedelta(days=2),
        "delivered_carrier_at": purchase + timedelta(days=1),  # before approval, as in Olist
        "delivered_customer_at": purchase + timedelta(days=5),
    }
    customer = {
        "customer_id": "c",
        "customer_unique_id": "u",
        "zip_code_prefix": "1",
        "city": "x",
        "state": "SP",
    }
    events = [e for e in timeline.order_events(order, customer, [], []) if e.op == "update"]
    shipped = next(e for e in events if e.values["order_status"] == "shipped")
    assert shipped.ts == purchase + timedelta(days=2)  # clamped time
    assert shipped.values["delivered_carrier_at"] == purchase + timedelta(days=1)  # true value


def test_the_timeline_is_deterministic(data: source.OlistData) -> None:
    rates = MutationRates(1.0, 1.0, 1.0)
    assert timeline.build(data, seed=7, rates=rates) == timeline.build(data, seed=7, rates=rates)


def test_mutations_add_history_and_deletes(data: source.OlistData) -> None:
    events = timeline.build(data, seed=7, rates=MutationRates(0.0, 1.0, 1.0))
    ops = Counter((e.table, e.op) for e in events)
    categories = {c["category_name"] for c in data.categories}
    movable = [p for p in data.products if categories - {p["category_name"]}]
    assert 0 < len(movable) < len(data.products)  # the fixture exercises the no-choice guard
    assert ops[("products", "update")] == len(movable)
    assert ops[("order_reviews", "delete")] == len(data.reviews)
    assert timeline.expected_rows(events)["order_reviews"] == 0


def test_expected_rows_counts_inserts_minus_deletes(data: source.OlistData) -> None:
    counts = timeline.expected_rows(timeline.build(data, seed=1, rates=NO_MUTATIONS))
    assert counts["orders"] == len(data.orders)
    assert counts["order_items"] == sum(len(v) for v in data.items.values())
    assert counts["zip_geolocation"] == 1


# ---------------------------------------------------------------- chaos


def test_chaos_off_changes_nothing(data: source.OlistData) -> None:
    for seq, event in enumerate(timeline.build(data, seed=1, rates=NO_MUTATIONS)):
        assert chaos.decide(event, seq=seq, seed=1, rate=0.0) == (event, None)


def test_chaos_at_rate_one_corrupts_every_eligible_event(data: source.OlistData) -> None:
    events = timeline.build(data, seed=1, rates=NO_MUTATIONS)
    for seq, event in enumerate(events):
        corrupted, defect = chaos.decide(event, seq=seq, seed=1, rate=1.0)
        if not chaos.eligible(event):
            assert defect is None
            continue
        assert defect is not None and defect.kind in chaos.eligible(event)
        assert corrupted.values[defect.column] == defect.injected != defect.original


def test_chaos_decisions_are_reproducible_per_sequence(data: source.OlistData) -> None:
    events = timeline.build(data, seed=1, rates=NO_MUTATIONS)
    first = [chaos.decide(e, seq=s, seed=3, rate=0.5) for s, e in enumerate(events)]
    again = [chaos.decide(e, seq=s, seed=3, rate=0.5) for s, e in enumerate(events)]
    assert first == again


def test_each_chaos_kind_produces_its_defect() -> None:
    purchase = datetime(2018, 1, 1)
    item = Event(
        purchase,
        "insert",
        "order_items",
        {"order_id": "o", "order_item_id": 1},
        {"price": Decimal("10.00")},
    )
    order = Event(purchase, "insert", "orders", {"order_id": "o"}, {"customer_id": "c"})
    approve = Event(
        purchase,
        "update",
        "orders",
        {"order_id": "o"},
        {"order_status": "approved", "approved_at": purchase},
        context={"purchase_ts": purchase},
    )
    assert chaos.decide(item, seq=0, seed=0, rate=1)[0].values["price"] == Decimal("-10.00")
    assert chaos.decide(order, seq=0, seed=0, rate=1)[0].values["customer_id"] is None
    seen = {chaos.decide(approve, seq=s, seed=0, rate=1)[1].kind for s in range(40)}  # type: ignore[union-attr]
    assert seen == {"unknown_status", "approved_before_purchase"}
    assert set(chaos.KINDS) == {"negative_price", "null_customer_id", *seen}
