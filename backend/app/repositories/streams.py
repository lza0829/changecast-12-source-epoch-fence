from __future__ import annotations

import json
from typing import Any

import sqlite3


def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    if r is None:
        return None
    d = dict(r)
    d["payload_fields"] = json.loads(d.pop("payload_fields_json"))
    d["enabled"] = bool(d["enabled"])
    return d


def list_streams(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM project_streams ORDER BY id").fetchall()
    return [_row(r) for r in rows]  # type: ignore[misc]


def get_stream(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any] | None:
    return _row(conn.execute("SELECT * FROM project_streams WHERE id=?", (stream_id,)).fetchone())


def set_watermark(conn: sqlite3.Connection, stream_id: int, watermark_seq: int) -> None:
    conn.execute(
        "UPDATE project_streams SET watermark_seq=? WHERE id=?",
        (watermark_seq, stream_id),
    )


def set_epoch(conn: sqlite3.Connection, stream_id: int, epoch: int, bumped_at: str) -> None:
    conn.execute(
        """
        UPDATE project_streams
        SET current_epoch=?, epoch_bump_count=epoch_bump_count+1, last_epoch_bump_at=?
        WHERE id=?
        """,
        (epoch, bumped_at, stream_id),
    )


def add_epoch_counters(
    conn: sqlite3.Connection,
    stream_id: int,
    *,
    rejected: int = 0,
    bypassed: int = 0,
    replayed: int = 0,
    replay_blocked: int = 0,
) -> None:
    conn.execute(
        """
        UPDATE project_streams
        SET epoch_rejected_count=epoch_rejected_count+?,
            epoch_bypassed_count=epoch_bypassed_count+?,
            epoch_replayed_count=epoch_replayed_count+?,
            epoch_replay_blocked_count=epoch_replay_blocked_count+?
        WHERE id=?
        """,
        (rejected, bypassed, replayed, replay_blocked, stream_id),
    )


def set_epoch_policies(
    conn: sqlite3.Connection,
    stream_id: int,
    *,
    late_epoch_policy: str | None = None,
    epoch_bump_buffer_policy: str | None = None,
) -> None:
    if late_epoch_policy is not None:
        conn.execute(
            "UPDATE project_streams SET late_epoch_policy=? WHERE id=?",
            (late_epoch_policy, stream_id),
        )
    if epoch_bump_buffer_policy is not None:
        conn.execute(
            "UPDATE project_streams SET epoch_bump_buffer_policy=? WHERE id=?",
            (epoch_bump_buffer_policy, stream_id),
        )


def set_fixture_stage(conn: sqlite3.Connection, stream_id: int, stage: int) -> None:
    conn.execute(
        "UPDATE project_streams SET fixture_stage=? WHERE id=?",
        (stage, stream_id),
    )
