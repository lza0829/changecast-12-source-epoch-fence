from __future__ import annotations

import json
from typing import Any

import sqlite3


def _decode_projection(r: sqlite3.Row) -> dict[str, Any]:
    d = dict(r)
    d["payload"] = json.loads(d.pop("payload_json")) if d["payload_json"] else None
    d.pop("payload_json", None)
    d["deleted"] = bool(d["deleted"])
    return d


def replace_stream_projection(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    rows: list[dict[str, Any]],
    updated_at: str,
) -> None:
    conn.execute("DELETE FROM projection_rows WHERE stream_id=?", (stream_id,))
    for row in rows:
        conn.execute(
            """
            INSERT INTO projection_rows(
                stream_id, entity_key, payload_json, applied_seq, applied_event_id,
                applied_epoch, deleted, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                stream_id,
                row["entity_key"],
                json.dumps(row.get("payload"), ensure_ascii=False) if row.get("payload") is not None else None,
                row["applied_seq"],
                row["applied_event_id"],
                int(row.get("applied_epoch", 1)),
                1 if row.get("deleted") else 0,
                updated_at,
            ),
        )


def load_stream_projection(conn: sqlite3.Connection, stream_id: int) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM projection_rows WHERE stream_id=?", (stream_id,)
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = _decode_projection(r)
        out[d["entity_key"]] = d
    return out


def upsert_projection_rows(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    rows: dict[str, dict[str, Any]],
    updated_at: str,
) -> None:
    """Write only the supplied entities; untouched rows stay as they are."""
    for entity_key, row in rows.items():
        conn.execute("DELETE FROM projection_rows WHERE stream_id=? AND entity_key=?", (stream_id, entity_key))
        conn.execute(
            """
            INSERT INTO projection_rows(
                stream_id, entity_key, payload_json, applied_seq, applied_event_id,
                applied_epoch, deleted, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                stream_id,
                entity_key,
                json.dumps(row.get("payload"), ensure_ascii=False) if row.get("payload") is not None else None,
                row["applied_seq"],
                row["applied_event_id"],
                int(row.get("applied_epoch", 1)),
                1 if row.get("deleted") else 0,
                updated_at,
            ),
        )


def get_projection_row(
    conn: sqlite3.Connection, *, stream_id: int, entity_key: str
) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM projection_rows WHERE stream_id=? AND entity_key=?",
        (stream_id, entity_key),
    ).fetchone()
    return _decode_projection(r) if r else None


def list_projections(
    conn: sqlite3.Connection,
    *,
    stream_id: int | None = None,
    include_deleted: bool = True,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM projection_rows WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if not include_deleted:
        sql += " AND deleted=0"
    sql += " ORDER BY stream_id, entity_key"
    out: list[dict[str, Any]] = []
    for r in conn.execute(sql, params).fetchall():
        out.append(_decode_projection(r))
    return out


def list_known_event_ids(conn: sqlite3.Connection, stream_id: int) -> set[str]:
    """Idempotency keys: deliveries with a final verdict.

    Rejected-late / bypassed events were consumed for this generation, so a
    re-run must not inflate the counters. Events merely fenced off because
    their epoch is *ahead* of the current one are NOT consumed: they stay
    re-deliverable and land once the raise is declared.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT event_id FROM applied_events
        WHERE stream_id=? AND result != 'rejected_ahead_epoch'
        """,
        (stream_id,),
    ).fetchall()
    return {r["event_id"] for r in rows}


def list_ooo_buffer(conn: sqlite3.Connection, stream_id: int | None = None) -> list[dict[str, Any]]:
    if stream_id is None:
        rows = conn.execute("SELECT * FROM ooo_buffer ORDER BY id DESC").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM ooo_buffer WHERE stream_id=? ORDER BY id DESC",
            (stream_id,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d["payload_json"]) if d["payload_json"] else None
        d.pop("payload_json", None)
        out.append(d)
    return out


# --- epoch bypass bucket -------------------------------------------------


def insert_bypass_row(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    row: dict[str, Any],
    bypassed_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO epoch_bypass(
            stream_id, event_id, entity_key, op, seq, event_time, epoch,
            payload_json, origin, reason, status, bypassed_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            row["event_id"],
            row["entity_key"],
            row["op"],
            int(row["seq"]),
            row["event_time"],
            int(row["epoch"]),
            json.dumps(row.get("payload"), ensure_ascii=False) if row.get("payload") is not None else None,
            row.get("origin", "late_arrival"),
            row.get("reason", "event_epoch_lt_stream_epoch"),
            row.get("status", "open"),
            bypassed_at,
        ),
    )
    if cur.lastrowid and cur.rowcount:
        return int(cur.lastrowid)
    existing = conn.execute(
        "SELECT id FROM epoch_bypass WHERE stream_id=? AND event_id=?",
        (stream_id, row["event_id"]),
    ).fetchone()
    return int(existing["id"]) if existing else -1


def list_bypass(
    conn: sqlite3.Connection,
    *,
    stream_id: int | None = None,
    origin: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM epoch_bypass WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if origin is not None:
        sql += " AND origin=?"
        params.append(origin)
    if status is not None:
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY id DESC"
    out: list[dict[str, Any]] = []
    for r in conn.execute(sql, params).fetchall():
        d = dict(r)
        d["payload"] = json.loads(d["payload_json"]) if d["payload_json"] else None
        d.pop("payload_json", None)
        out.append(d)
    return out


def get_bypass_row(conn: sqlite3.Connection, bypass_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM epoch_bypass WHERE id=?", (bypass_id,)).fetchone()
    if not r:
        return None
    d = dict(r)
    d["payload"] = json.loads(d["payload_json"]) if d["payload_json"] else None
    d.pop("payload_json", None)
    return d


def mark_bypass_replayed(
    conn: sqlite3.Connection,
    *,
    bypass_id: int,
    status: str,
    result: str,
    at: str,
) -> None:
    conn.execute(
        """
        UPDATE epoch_bypass
        SET status=?, replayed_at=?, last_replay_result=?, last_replay_at=?
        WHERE id=?
        """,
        (status, at, result, at, bypass_id),
    )


def insert_replay_log(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    bypass_id: int | None,
    event_id: str,
    entity_key: str,
    result: str,
    reason: str,
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO epoch_replay_log(
            stream_id, bypass_id, event_id, entity_key, result, reason, created_at
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (stream_id, bypass_id, event_id, entity_key, result, reason, created_at),
    )


def list_replay_log(conn: sqlite3.Connection, stream_id: int | None = None, limit: int = 100):
    sql = "SELECT * FROM epoch_replay_log WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def insert_epoch_change(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    from_epoch: int,
    to_epoch: int,
    source: str,
    buffer_policy: str,
    buffered_seen: int,
    buffered_rejected: int,
    buffered_migrated: int,
    projected_alive: int,
    epoch_rejected_total: int,
    epoch_bypassed_total: int,
    note: str | None,
    created_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO epoch_changes(
            stream_id, from_epoch, to_epoch, source, buffer_policy,
            buffered_seen, buffered_rejected, buffered_migrated,
            projected_alive, epoch_rejected_total, epoch_bypassed_total,
            note, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            from_epoch,
            to_epoch,
            source,
            buffer_policy,
            buffered_seen,
            buffered_rejected,
            buffered_migrated,
            projected_alive,
            epoch_rejected_total,
            epoch_bypassed_total,
            note,
            created_at,
        ),
    )
    return int(cur.lastrowid)


def list_epoch_changes(
    conn: sqlite3.Connection, stream_id: int | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM epoch_changes WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
