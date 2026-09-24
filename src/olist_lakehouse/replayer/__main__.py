"""Replay Olist into the source Postgres on a compressed business clock.

    python -m olist_lakehouse.replayer                 # run (or resume) the replay
    python -m olist_lakehouse.replayer --verify        # row counts match the checkpoint?
    python -m olist_lakehouse.replayer --status        # where is the replay?

The business clock runs `--speed` times faster than the wall clock (1440 = one day per minute;
0 = as fast as the database allows). A gap in business time longer than `--max-idle` wall
seconds is skipped rather than waited out. Connection settings come from the standard libpq
variables (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE) or `--dsn`.

Refusals, each with its own code (exit 1):
    REPLAY-FINGERPRINT-MISMATCH  the database was replayed from other data, seed or rates
    REPLAY-CHAOS-RATE-CHANGED    resuming with another CHAOS_RATE than the replay started with
    REPLAY-SCHEMA-MISSING        the replay schema is absent (wrong database?)
    REPLAY-VERIFY-MISMATCH       row counts differ from what the checkpoint implies
    CONTRACT-*                   the raw CSVs break the contract (olist_lakehouse.contracts)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType

import psycopg

from olist_lakehouse import settings
from olist_lakehouse.contracts import COLUMNS, ContractError
from olist_lakehouse.fetch import sha256
from olist_lakehouse.replayer import chaos, db, source, timeline

_stop = False


def _request_stop(signum: int, frame: FrameType | None) -> None:
    global _stop
    _stop = True
    print("replayer: stop requested; finishing the current batch", flush=True)


def fingerprint(directory: Path, seed: int, rates: timeline.MutationRates) -> str:
    digest = hashlib.sha256()
    for name in sorted(COLUMNS):
        digest.update(f"{name}={sha256(directory / name)}\n".encode())
    digest.update(f"seed={seed}\nrates={rates}\nversion={timeline.TIMELINE_VERSION}\n".encode())
    return digest.hexdigest()[:16]


def verify(conn: psycopg.Connection[object], events: list[timeline.Event], *, seed: int) -> int:
    """Row counts and the chaos ledger both match what the checkpoint implies.

    The ledger check recomputes every chaos decision up to the checkpoint, with the rate the
    checkpoint recorded (a replay keeps one rate from its first batch to its last).
    """
    checkpoint = db.read_checkpoint(conn)
    chaos_rate = checkpoint.chaos_rate if checkpoint else 0.0
    applied = events[: checkpoint.last_seq + 1] if checkpoint else []
    expected = timeline.expected_rows(applied)
    actual = db.row_counts(conn)
    wrong = sorted(t for t in expected if expected[t] != actual[t])
    for table in expected:
        mark = "MISMATCH" if table in wrong else "ok"
        print(f"  {table:<30} expected {expected[table]:>9,}  actual {actual[table]:>9,}  {mark}")

    expected_chaos = {
        seq
        for seq, event in enumerate(applied)
        if chaos.decide(event, seq=seq, seed=seed, rate=chaos_rate)[1] is not None
    }
    ledger = db.chaos_ledger_seqs(conn)
    chaos_ok = ledger == expected_chaos
    print(
        f"  {'chaos_ledger':<30} expected {len(expected_chaos):>9,}  actual {len(ledger):>9,}  "
        f"{'ok' if chaos_ok else 'MISMATCH'}"
    )
    if wrong or not chaos_ok:
        problems = wrong + ([] if chaos_ok else ["chaos_ledger"])
        print(f"REPLAY-VERIFY-MISMATCH: {problems}", file=sys.stderr)
        return 1
    seq = checkpoint.last_seq if checkpoint else -1
    print(
        f"verify: rows and chaos ledger match the checkpoint (last_seq {seq:,} of {len(events):,})"
    )
    return 0


def run(args: argparse.Namespace) -> int:
    directory: Path = args.data_dir
    rates = timeline.MutationRates()
    print(f"replayer: loading {directory}", flush=True)
    data = source.load(directory)
    events = timeline.build(data, seed=args.seed, rates=rates)
    total = len(events)
    print_ = fingerprint(directory, args.seed, rates)

    with psycopg.connect(args.dsn) as conn:
        if args.verify:
            return verify(conn, events, seed=args.seed)

        checkpoint = db.read_checkpoint(conn)
        if checkpoint and checkpoint.fingerprint != print_:
            raise db.ReplayError(
                "REPLAY-FINGERPRINT-MISMATCH",
                f"database was replayed with fingerprint {checkpoint.fingerprint}, this run is "
                f"{print_}; reset the source volume to replay different data, seed or rates",
            )
        if checkpoint and checkpoint.chaos_rate != args.chaos_rate:
            raise db.ReplayError(
                "REPLAY-CHAOS-RATE-CHANGED",
                f"this replay started with CHAOS_RATE={checkpoint.chaos_rate}; resuming with "
                f"{args.chaos_rate} would break the ledger's reconciliation (M2-S2)",
            )
        start = checkpoint.last_seq + 1 if checkpoint else 0
        if args.status:
            where = checkpoint.business_ts if checkpoint else "not started"
            print(f"status: {start:,} of {total:,} events applied; business time {where}")
            return 0
        if start >= total:
            print(f"replayer: replay already complete ({total:,} events)")
            return 0

        until: datetime | None = args.until
        print(
            f"replayer: {total:,} events, resuming at {start:,} "
            f"(business {events[start].ts}); speed {args.speed}x, chaos rate {args.chaos_rate}",
            flush=True,
        )

        anchor_business, anchor_wall = events[start].ts, time.monotonic()
        applied_this_run, last_report = 0, time.monotonic()
        i = start
        while i < total and not _stop:
            if until is not None and events[i].ts > until:
                print(f"replayer: reached --until {until}")
                break
            if args.max_events and applied_this_run >= args.max_events:
                print(f"replayer: reached --max-events {args.max_events}")
                break

            if args.speed > 0:
                elapsed = time.monotonic() - anchor_wall
                clock = anchor_business + timedelta(seconds=elapsed * args.speed)
                if events[i].ts > clock:
                    wait = (events[i].ts - clock).total_seconds() / args.speed
                    if wait > args.max_idle:
                        print(f"replayer: skipping a quiet gap in business time to {events[i].ts}")
                        anchor_business, anchor_wall = events[i].ts, time.monotonic()
                    else:
                        time.sleep(min(wait, 0.5))
                    continue
            else:
                clock = datetime.max

            j = i
            limit = args.batch
            if args.max_events:
                limit = min(limit, args.max_events - applied_this_run)
            while j < total and j - i < limit and events[j].ts <= clock:
                if until is not None and events[j].ts > until:
                    break
                j += 1

            batch = []
            for seq in range(i, j):
                event, defect = chaos.decide(
                    events[seq], seq=seq, seed=args.seed, rate=args.chaos_rate
                )
                batch.append((seq, event, defect))
            db.apply_batch(
                conn, batch, fingerprint=print_, events_total=total, chaos_rate=args.chaos_rate
            )
            applied_this_run += j - i
            i = j

            if time.monotonic() - last_report >= args.report_every:
                print(
                    f"replayer: {i:,}/{total:,} events, business time {events[i - 1].ts}",
                    flush=True,
                )
                last_report = time.monotonic()

        state = "complete" if i >= total else "paused"
        print(f"replayer: {state} at {i:,}/{total:,} events ({applied_this_run:,} this run)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dsn", default=os.environ.get("SOURCE_DB_DSN", ""))
    parser.add_argument("--data-dir", type=Path, default=settings.RAW_DIR)
    parser.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", 1440)))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("REPLAY_SEED", 42)))
    parser.add_argument(
        "--chaos-rate", type=float, default=float(os.environ.get("CHAOS_RATE", 0.0))
    )
    parser.add_argument(
        "--until",
        type=datetime.fromisoformat,
        default=os.environ.get("REPLAY_UNTIL") or None,
        help="stop before the first event after this business time",
    )
    parser.add_argument("--max-events", type=int, default=0, help="stop after N events this run")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--max-idle", type=float, default=5.0)
    parser.add_argument("--report-every", type=float, default=10.0)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    if isinstance(args.until, str):
        args.until = datetime.fromisoformat(args.until)

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    try:
        return run(args)
    except (db.ReplayError, ContractError) as exc:
        print(f"replayer: REFUSED {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
