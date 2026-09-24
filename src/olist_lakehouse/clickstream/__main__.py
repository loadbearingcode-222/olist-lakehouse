"""Produce the clickstream for the orders the replayer writes.

    python -m olist_lakehouse.clickstream              # run (or resume)
    python -m olist_lakehouse.clickstream --status     # where is it?

Loop: read the next orders after the watermark `(purchase_ts, order_id)` from postgres-source,
turn each into sessions (sessions.py), apply chaos (chaos.py), produce to Kafka keyed by
session_id and flush, then commit the chaos-ledger rows and the new watermark in one Postgres
transaction.

Delivery is at-least-once. A crash between the Kafka flush and the Postgres commit re-sends
that batch after restart, and the re-sent events are byte-identical with the same event_id
(everything is seeded), so dedup on event_id removes them. The replayer inserts orders in
(purchase_ts, order_id) order, and an order and its items commit together, so a watermark
never skips an order.

Refusals (exit 1):
    CLICK-CHAOS-RATE-CHANGED   resuming with another CHAOS_RATE than the stream started with
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime
from types import FrameType
from typing import Any

import psycopg
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from olist_lakehouse.clickstream import chaos
from olist_lakehouse.clickstream.sessions import Order, sessions_for

_stop = False

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS replay.clickstream_checkpoint (
    id            integer PRIMARY KEY CHECK (id = 1),
    purchase_ts   timestamp NOT NULL,
    order_id      text NOT NULL,
    orders_done   bigint NOT NULL,
    events_sent   bigint NOT NULL,
    chaos_rate    double precision NOT NULL,
    updated_wall  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS replay.clickstream_chaos_ledger (
    event_id      text NOT NULL,
    kind          text NOT NULL,
    order_id      text NOT NULL,
    event_ts      timestamp NOT NULL,
    detail        jsonb NOT NULL,
    injected_wall timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, kind)
);
"""

ORDERS_SQL = """
SELECT o.order_id, c.customer_unique_id, o.purchase_ts,
       coalesce(array_agg(i.product_id ORDER BY i.order_item_id)
                FILTER (WHERE i.product_id IS NOT NULL), '{}') AS product_ids
FROM olist.orders o
LEFT JOIN olist.customers c ON c.customer_id = o.customer_id
LEFT JOIN olist.order_items i ON i.order_id = o.order_id
WHERE (o.purchase_ts, o.order_id) > (%s, %s)
GROUP BY o.order_id, c.customer_unique_id, o.purchase_ts
ORDER BY o.purchase_ts, o.order_id
LIMIT %s
"""


class ClickstreamError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _request_stop(signum: int, frame: FrameType | None) -> None:
    global _stop
    _stop = True
    print("clickstream: stop requested; finishing the current batch", flush=True)


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=15).topics:
        return
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=partitions,
                replication_factor=1,
                config={"retention.ms": str(7 * 24 * 3600 * 1000)},
            )
        ]
    )
    try:
        futures[topic].result()
        print(f"clickstream: created topic {topic} ({partitions} partitions)")
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def read_checkpoint(conn: psycopg.Connection[Any]) -> tuple[datetime, str, int, int, float] | None:
    row = conn.execute(
        "SELECT purchase_ts, order_id, orders_done, events_sent, chaos_rate"
        " FROM replay.clickstream_checkpoint"
    ).fetchone()
    conn.rollback()
    return (row[0], row[1], int(row[2]), int(row[3]), float(row[4])) if row else None


def run(args: argparse.Namespace) -> int:
    with psycopg.connect(args.dsn) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()
        checkpoint = read_checkpoint(conn)
        if args.status:
            print(f"status: {checkpoint or 'not started'}")
            return 0
        if checkpoint and checkpoint[4] != args.chaos_rate:
            raise ClickstreamError(
                "CLICK-CHAOS-RATE-CHANGED",
                f"stream started with CHAOS_RATE={checkpoint[4]}, resuming with {args.chaos_rate}",
            )
        watermark = (checkpoint[0], checkpoint[1]) if checkpoint else (datetime.min, "")
        orders_done = checkpoint[2] if checkpoint else 0
        events_sent = checkpoint[3] if checkpoint else 0

        catalog = [r[0] for r in conn.execute("SELECT product_id FROM olist.products ORDER BY 1")]
        conn.rollback()
        if not catalog:
            print("clickstream: no products yet; waiting for the replayer", flush=True)

        ensure_topic(args.bootstrap, args.topic, args.partitions)
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap,
                "enable.idempotence": True,  # no duplicates from producer retries
                "acks": "all",
                "linger.ms": 50,
                "compression.type": "zstd",
            }
        )
        print(
            f"clickstream: resuming after {watermark[0]} {watermark[1] or '(start)'}; "
            f"chaos rate {args.chaos_rate}",
            flush=True,
        )

        last_report = time.monotonic()
        while not _stop:
            if not catalog:
                catalog = [
                    r[0] for r in conn.execute("SELECT product_id FROM olist.products ORDER BY 1")
                ]
                conn.rollback()
                if not catalog:
                    time.sleep(args.poll)
                    continue
            rows = conn.execute(ORDERS_SQL, [watermark[0], watermark[1], args.batch]).fetchall()
            conn.rollback()
            if not rows:
                if args.once:
                    break
                time.sleep(args.poll)
                continue

            ledger: list[tuple[str, str, str, str, str]] = []
            sent = 0
            for order_id, customer, purchase_ts, product_ids in rows:
                order = Order(order_id, customer, purchase_ts, tuple(product_ids))
                for event in sessions_for(order, seed=args.seed, catalog=catalog):
                    messages, defect = chaos.decide(event, seed=args.seed, rate=args.chaos_rate)
                    for message in messages:
                        producer.produce(
                            args.topic,
                            key=message["session_id"].encode(),
                            value=json.dumps(message, separators=(",", ":")).encode(),
                        )
                        sent += 1
                        producer.poll(0)
                    if defect is not None:
                        ledger.append(
                            (
                                event["event_id"],
                                defect.kind,
                                order_id,
                                event["event_ts"],
                                json.dumps(defect.detail),
                            )
                        )
            remaining = producer.flush(30)
            if remaining:
                raise ClickstreamError("CLICK-FLUSH-TIMEOUT", f"{remaining} messages unsent")

            watermark = (rows[-1][2], rows[-1][0])
            orders_done += len(rows)
            events_sent += sent
            with conn.transaction(), conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO replay.clickstream_chaos_ledger"
                    " (event_id, kind, order_id, event_ts, detail) VALUES (%s, %s, %s, %s, %s)"
                    " ON CONFLICT DO NOTHING",
                    ledger,
                )
                cur.execute(
                    "INSERT INTO replay.clickstream_checkpoint"
                    " (id, purchase_ts, order_id, orders_done, events_sent, chaos_rate)"
                    " VALUES (1, %s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET"
                    " purchase_ts = EXCLUDED.purchase_ts, order_id = EXCLUDED.order_id,"
                    " orders_done = EXCLUDED.orders_done, events_sent = EXCLUDED.events_sent,"
                    " updated_wall = now()",
                    [watermark[0], watermark[1], orders_done, events_sent, args.chaos_rate],
                )
            if time.monotonic() - last_report >= args.report_every:
                print(
                    f"clickstream: {orders_done:,} orders, {events_sent:,} events, "
                    f"business time {watermark[0]}",
                    flush=True,
                )
                last_report = time.monotonic()
        print(f"clickstream: stopped after {orders_done:,} orders, {events_sent:,} events")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dsn", default=os.environ.get("SOURCE_DB_DSN", ""))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:29092"))
    parser.add_argument("--topic", default=os.environ.get("CLICK_TOPIC", "clickstream.events"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=int(os.environ.get("REPLAY_SEED", 42)))
    parser.add_argument(
        "--chaos-rate", type=float, default=float(os.environ.get("CHAOS_RATE", 0.0))
    )
    parser.add_argument("--batch", type=int, default=500, help="orders per batch")
    parser.add_argument("--poll", type=float, default=1.0)
    parser.add_argument("--report-every", type=float, default=10.0)
    parser.add_argument("--once", action="store_true", help="stop when caught up (tests)")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    try:
        return run(args)
    except ClickstreamError as exc:
        print(f"clickstream: REFUSED {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
