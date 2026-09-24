"""Clickstream sessions and chaos: shape, ordering, determinism, one defect per chaotic event."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta

from olist_lakehouse.clickstream import chaos
from olist_lakehouse.clickstream.sessions import Order, sessions_for

CATALOG = [f"p{i:03d}" for i in range(50)]
BIG = Order("o-big", "u1", datetime(2017, 5, 1, 12), tuple(f"p{i:03d}" for i in range(21)))
SMALL = Order("o-small", None, datetime(2017, 5, 1, 12), ("p001", "p001"))


def converting(events: list[dict[str, object]]) -> list[dict[str, object]]:
    purchase = next(e for e in events if e["event_type"] == "purchase")
    return [e for e in events if e["session_id"] == purchase["session_id"]]


def test_the_converting_session_ends_in_the_real_order() -> None:
    session = converting(sessions_for(BIG, seed=1, catalog=CATALOG))
    kinds = [e["event_type"] for e in session]
    assert kinds[-2:] == ["checkout", "purchase"]
    assert session[-1]["order_id"] == "o-big"
    assert session[-1]["event_ts"] == BIG.purchase_ts.isoformat(timespec="seconds")
    assert Counter(kinds)["add_to_cart"] == len(BIG.product_ids)
    assert {e["customer_unique_id"] for e in session} == {"u1"}


def test_even_a_21_item_order_fits_before_its_purchase() -> None:
    for order in (BIG, SMALL):
        events = sessions_for(order, seed=7, catalog=CATALOG)
        assert max(e["event_ts"] for e in events) == order.purchase_ts.isoformat(timespec="seconds")
        assert [e["event_ts"] for e in events] == sorted(e["event_ts"] for e in events)


def test_browsing_sessions_are_anonymous_and_never_purchase() -> None:
    events = sessions_for(BIG, seed=3, catalog=CATALOG)
    purchase_session = converting(events)[0]["session_id"]
    browsing = [e for e in events if e["session_id"] != purchase_session]
    assert all(e["customer_unique_id"] is None for e in browsing)
    assert all(e["event_type"] == "page_view" for e in browsing)
    assert sum(e["event_type"] == "purchase" for e in events) == 1


def test_sessions_are_deterministic_and_ids_unique() -> None:
    first = sessions_for(BIG, seed=5, catalog=CATALOG)
    assert first == sessions_for(BIG, seed=5, catalog=CATALOG)
    assert len({e["event_id"] for e in first}) == len(first)
    assert first != sessions_for(BIG, seed=6, catalog=CATALOG)
    assert all(isinstance(e["user_agent"], str) and e["user_agent"] for e in first)


def test_an_order_without_items_still_checks_out() -> None:
    events = sessions_for(Order("o-empty", "u", datetime(2017, 1, 1), ()), seed=1, catalog=CATALOG)
    assert [e["event_type"] for e in converting(events)] == ["checkout", "purchase"]


# ---------------------------------------------------------------- chaos


def all_events() -> list[dict[str, object]]:
    orders = [
        Order(f"o{i}", "u", datetime(2017, 1, 1) + timedelta(hours=i), ("p001", "p002"))
        for i in range(200)
    ]
    return [e for o in orders for e in sessions_for(o, seed=1, catalog=CATALOG)]


def test_chaos_off_sends_each_event_once_untouched() -> None:
    for event in all_events()[:100]:
        assert chaos.decide(event, seed=1, rate=0.0) == ([event], None)


def test_each_kind_produces_its_defect() -> None:
    kinds: Counter[str] = Counter()
    for event in all_events():
        messages, defect = chaos.decide(event, seed=1, rate=1.0)
        assert defect is not None
        kinds[defect.kind] += 1
        if defect.kind == "duplicate":
            assert messages == [event, event]
        elif defect.kind == "late":
            (m,) = messages
            delta = datetime.fromisoformat(str(event["event_ts"])) - datetime.fromisoformat(
                str(m["event_ts"])
            )
            assert delta == chaos.LATE_BY
        elif defect.kind == "null_product":
            (m,) = messages
            assert event["event_type"] in ("page_view", "add_to_cart") and m["product_id"] is None
        else:
            (m,) = messages
            assert "event_type" not in m and m["eventType"] == event["event_type"]
            assert m["schema_version"] == 2
    assert set(kinds) == set(chaos.KINDS)


def test_the_chaos_rate_is_honoured_and_reproducible() -> None:
    events = all_events()
    decisions = [chaos.decide(e, seed=9, rate=0.05)[1] for e in events]
    share = sum(d is not None for d in decisions) / len(events)
    assert 0.03 < share < 0.07, share
    assert decisions == [chaos.decide(e, seed=9, rate=0.05)[1] for e in events]
