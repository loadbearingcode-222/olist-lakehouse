"""Write events to Postgres. A batch, its chaos-ledger rows and the checkpoint commit together.

That single transaction is what makes a kill safe: after a crash the database holds either the
whole batch and the checkpoint that names it, or neither. A resumed replayer starts after the
checkpoint, so no event is applied twice and none is skipped.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql

from olist_lakehouse.replayer.chaos import Defect
from olist_lakehouse.replayer.timeline import KEYS, Event

SCHEMA = "olist"


@dataclass(frozen=True)
class Checkpoint:
    fingerprint: str
    last_seq: int
    business_ts: datetime
    events_total: int
    chaos_rate: float


class ReplayError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _statement(event: Event) -> tuple[sql.Composed, list[Any]]:
    table = sql.Identifier(SCHEMA, event.table)
    if event.op == "insert":
        columns = list(event.values)
        return (
            sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                table,
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            ),
            [event.values[c] for c in columns],
        )
    where = sql.SQL(" AND ").join(
        sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder()) for c in event.key
    )
    if event.op == "update":
        columns = list(event.values)
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(c), sql.Placeholder()) for c in columns
        )
        return (
            sql.SQL("UPDATE {} SET {} WHERE {}").format(table, assignments, where),
            [event.values[c] for c in columns] + list(event.key.values()),
        )
    return sql.SQL("DELETE FROM {} WHERE {}").format(table, where), list(event.key.values())


def read_checkpoint(conn: psycopg.Connection[Any]) -> Checkpoint | None:
    try:
        row = conn.execute(
            "SELECT fingerprint, last_seq, business_ts, events_total, chaos_rate FROM replay.checkpoint"
        ).fetchone()
    except psycopg.errors.UndefinedTable as exc:
        raise ReplayError(
            "REPLAY-SCHEMA-MISSING", "replay.checkpoint does not exist; is this the source DB?"
        ) from exc
    finally:
        conn.rollback()
    return Checkpoint(*row) if row else None


def apply_batch(
    conn: psycopg.Connection[Any],
    batch: list[tuple[int, Event, Defect | None]],
    *,
    fingerprint: str,
    events_total: int,
    chaos_rate: float,
) -> None:
    last_seq, last_event, _ = batch[-1]
    with conn.transaction(), conn.pipeline(), conn.cursor() as cur:
        for seq, event, defect in batch:
            statement, params = _statement(event)
            cur.execute(statement, params)
            if defect is not None:
                cur.execute(
                    "INSERT INTO replay.chaos_ledger"
                    " (seq, business_ts, kind, table_name, row_key, detail)"
                    " VALUES (%s, %s, %s, %s, %s, %s)",
                    [
                        seq,
                        event.ts,
                        defect.kind,
                        event.table,
                        json.dumps(event.key, default=str),
                        json.dumps(
                            {
                                "column": defect.column,
                                "original": defect.original,
                                "injected": defect.injected,
                            },
                            default=str,
                        ),
                    ],
                )
        cur.execute(
            "INSERT INTO replay.checkpoint"
            " (id, fingerprint, last_seq, business_ts, events_total, chaos_rate)"
            " VALUES (1, %s, %s, %s, %s, %s)"
            " ON CONFLICT (id) DO UPDATE SET last_seq = EXCLUDED.last_seq,"
            " business_ts = EXCLUDED.business_ts, updated_wall = now()",
            [fingerprint, last_seq, last_event.ts, events_total, chaos_rate],
        )


def chaos_ledger_seqs(conn: psycopg.Connection[Any]) -> set[int]:
    seqs = {int(row[0]) for row in conn.execute("SELECT seq FROM replay.chaos_ledger")}
    conn.rollback()
    return seqs


def row_counts(conn: psycopg.Connection[Any]) -> dict[str, int]:
    counts = {}
    for table in KEYS:
        query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(SCHEMA, table))
        row = conn.execute(query).fetchone()
        counts[table] = int(row[0]) if row else 0
    conn.rollback()
    return counts
