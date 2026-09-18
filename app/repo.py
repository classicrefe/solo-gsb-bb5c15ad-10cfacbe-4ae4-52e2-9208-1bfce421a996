"""Persistence and the ordered catch-up merge algorithm."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from . import db


class ConflictError(Exception):
    """Raised when a replay/clash contradicts what is already stored (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class StreamState:
    stream_id: str
    balance: int
    next_sequence: int
    pending_sequences: list[int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "balance": self.balance,
            "next_sequence": self.next_sequence,
            "pending_sequences": self.pending_sequences,
        }


@dataclass(frozen=True)
class BatchEvent:
    """One event of a batch submission (framework-free input record)."""

    event_id: str
    sequence: int
    delta: int


@dataclass(frozen=True)
class BatchItemResult:
    """Per-event outcome of a batch submission, in input order."""

    event_id: str
    sequence: int
    status: str  # "created" or "replayed"

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "status": self.status,
        }


def _get_stream(conn: sqlite3.Connection, stream_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
        (stream_id,),
    ).fetchone()


def is_sealed(conn: sqlite3.Connection, stream_id: str) -> bool:
    """Return True when ``stream_id`` has a (permanent) seal row.

    Must run inside an open transaction so the caller's write lock guarantees
    the result stays valid until commit.
    """
    return conn.execute(
        "SELECT 1 FROM seals WHERE stream_id = ?", (stream_id,)
    ).fetchone() is not None


def _get_event(
    conn: sqlite3.Connection, stream_id: str, event_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT event_id, sequence, delta, applied "
        "FROM events WHERE stream_id = ? AND event_id = ?",
        (stream_id, event_id),
    ).fetchone()


def _get_event_by_sequence(
    conn: sqlite3.Connection, stream_id: str, sequence: int
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT event_id, sequence, delta, applied "
        "FROM events WHERE stream_id = ? AND sequence = ?",
        (stream_id, sequence),
    ).fetchone()


def _pending_sequences(conn: sqlite3.Connection, stream_id: str) -> list[int]:
    rows = conn.execute(
        "SELECT sequence FROM events WHERE stream_id = ? AND applied = 0 "
        "ORDER BY sequence ASC",
        (stream_id,),
    ).fetchall()
    return [row["sequence"] for row in rows]


def build_state(
    conn: sqlite3.Connection,
    stream_id: str,
    balance: int,
    next_sequence: int,
) -> StreamState:
    return StreamState(
        stream_id=stream_id,
        balance=balance,
        next_sequence=next_sequence,
        pending_sequences=_pending_sequences(conn, stream_id),
    )


def apply_contiguous(
    conn: sqlite3.Connection, stream_id: str, balance: int, next_sequence: int
) -> tuple[int, int]:
    """Fold every unapplied event contiguous with ``next_sequence``.

    Starts at the stream's current ``next_sequence`` and walks the buffered
    events in order, adding each ``delta`` to ``balance`` and marking it
    ``applied = 1``, stopping at the first gap. Must run inside an open write
    transaction. Returns the resulting ``(balance, next_sequence)``.
    """
    expected = next_sequence
    while True:
        row = conn.execute(
            "SELECT event_id, delta FROM events "
            "WHERE stream_id = ? AND sequence = ? AND applied = 0",
            (stream_id, expected),
        ).fetchone()
        if row is None:
            break
        balance += row["delta"]
        conn.execute(
            "UPDATE events SET applied = 1 "
            "WHERE stream_id = ? AND sequence = ?",
            (stream_id, expected),
        )
        expected += 1
    return balance, expected


def get_stream_state(stream_id: str) -> StreamState | None:
    with db.read_connection() as conn:
        stream = _get_stream(conn, stream_id)
        if stream is None:
            return None
        return StreamState(
            stream_id=stream["stream_id"],
            balance=stream["balance"],
            next_sequence=stream["next_sequence"],
            pending_sequences=_pending_sequences(conn, stream_id),
        )


def submit_event(
    stream_id: str, event_id: str, sequence: int, delta: int
) -> tuple[StreamState, bool]:
    """Persist one event and run contiguous catch-up.

    Returns ``(state, replayed)`` where ``replayed`` is True when the event was
    already stored (an idempotent replay). Raises ``ConflictError`` on 409 cases.
    """
    with db.write_transaction() as conn:
        stream = _get_stream(conn, stream_id)
        if stream is None:
            conn.execute(
                "INSERT INTO streams (stream_id, balance, next_sequence) VALUES (?, 0, 0)",
                (stream_id,),
            )
            balance, next_sequence = 0, 0
        else:
            balance, next_sequence = stream["balance"], stream["next_sequence"]

        existing = _get_event(conn, stream_id, event_id)
        if existing is not None:
            # Idempotent replay: same event_id must carry identical content.
            if existing["sequence"] != sequence or existing["delta"] != delta:
                raise ConflictError(
                    "replay_conflict",
                    "event_id already exists with different sequence/delta",
                )
            replayed = True
        else:
            # Sealing is irreversible: a sealed stream rejects any *new* event.
            # Completed operations stay replayable because they take the branch
            # above without writing.
            if is_sealed(conn, stream_id):
                raise ConflictError(
                    "stream_sealed",
                    f"stream {stream_id!r} is sealed and cannot accept new events",
                )
            clash = _get_event_by_sequence(conn, stream_id, sequence)
            if clash is not None:
                # The UNIQUE(stream_id, sequence) index also guards this, but
                # producing a clear error here keeps the response informative.
                raise ConflictError(
                    "sequence_conflict",
                    f"sequence {sequence} already occupied by event_id "
                    f"{clash['event_id']!r}",
                )
            conn.execute(
                "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
                "VALUES (?, ?, ?, ?, 0)",
                (stream_id, event_id, sequence, delta),
            )
            replayed = False

        # Contiguous catch-up, executed inside the same transaction.
        balance, expected = apply_contiguous(conn, stream_id, balance, next_sequence)

        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (balance, expected, stream_id),
        )

        state = build_state(conn, stream_id, balance, expected)
        return state, replayed


def submit_event_batch(
    stream_id: str, events: list[BatchEvent]
) -> tuple[StreamState, list[BatchItemResult]]:
    """Validate and persist a whole batch of events, then catch up once.

    The batch is checked and written in input order inside a single
    ``BEGIN IMMEDIATE`` transaction: an already-stored event with identical
    content is a replay, while mismatched content, an occupied sequence or a
    duplicated ``event_id``/``sequence`` within the batch raises
    ``ConflictError`` — the transaction then rolls back, so no new event and
    no balance change of this batch survives. A sealed stream accepts a batch
    only when every event replays; any new event rejects the whole batch.
    Exactly one contiguous catch-up runs after every item passed.

    Returns ``(state, results)`` with one :class:`BatchItemResult` per input
    event, in input order. Raises ``ConflictError`` on 409 cases.
    """
    if not events:
        raise ValueError("event batch must contain at least one event")

    with db.write_transaction() as conn:
        # Within one batch, event_id and sequence must each be unique.
        seen_event_ids: set[str] = set()
        seen_sequences: set[int] = set()
        for event in events:
            if event.event_id in seen_event_ids:
                raise ConflictError(
                    "duplicate_event_id",
                    f"event_id {event.event_id!r} appears more than once in the batch",
                )
            if event.sequence in seen_sequences:
                raise ConflictError(
                    "duplicate_sequence",
                    f"sequence {event.sequence} appears more than once in the batch",
                )
            seen_event_ids.add(event.event_id)
            seen_sequences.add(event.sequence)

        stream = _get_stream(conn, stream_id)
        if stream is None:
            conn.execute(
                "INSERT INTO streams (stream_id, balance, next_sequence) VALUES (?, 0, 0)",
                (stream_id,),
            )
            balance, next_sequence = 0, 0
        else:
            balance, next_sequence = stream["balance"], stream["next_sequence"]

        results: list[BatchItemResult] = []
        for event in events:
            existing = _get_event(conn, stream_id, event.event_id)
            if existing is not None:
                # Idempotent replay: same event_id must carry identical content.
                if existing["sequence"] != event.sequence or existing["delta"] != event.delta:
                    raise ConflictError(
                        "replay_conflict",
                        "event_id already exists with different sequence/delta",
                    )
                results.append(BatchItemResult(event.event_id, event.sequence, "replayed"))
                continue
            # Sealing is irreversible: a sealed stream replays a whole batch
            # but rejects it as soon as it contains any new event.
            if is_sealed(conn, stream_id):
                raise ConflictError(
                    "stream_sealed",
                    f"stream {stream_id!r} is sealed and cannot accept new events",
                )
            clash = _get_event_by_sequence(conn, stream_id, event.sequence)
            if clash is not None:
                raise ConflictError(
                    "sequence_conflict",
                    f"sequence {event.sequence} already occupied by event_id "
                    f"{clash['event_id']!r}",
                )
            conn.execute(
                "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
                "VALUES (?, ?, ?, ?, 0)",
                (stream_id, event.event_id, event.sequence, event.delta),
            )
            results.append(BatchItemResult(event.event_id, event.sequence, "created"))

        # Every item passed: run the contiguous catch-up exactly once.
        balance, expected = apply_contiguous(conn, stream_id, balance, next_sequence)

        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (balance, expected, stream_id),
        )

        state = build_state(conn, stream_id, balance, expected)
        return state, results
