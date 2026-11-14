from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.project import LATE_EPOCH_POLICIES, apply_events
from app.repositories import batches as batch_repo
from app.repositories import projections as proj_repo
from app.repositories import streams as stream_repo

BUMP_POLICY_REJECT = "reject_buffer"
BUMP_POLICY_MIGRATE = "migrate_bypass"
BUMP_POLICIES = (BUMP_POLICY_REJECT, BUMP_POLICY_MIGRATE)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _collection(name: str, *, next_stage: int) -> tuple[list[dict[str, Any]], int | None, dict[str, Any]]:
    """Read a fixture.

    Two shapes are supported:
    * a bare event array (single generation, no declared raise);
    * ``{"stages": [{"stage", "declared_epoch", "events"}]}`` where each run
      consumes the next stage, letting a demo walk across an epoch switch.

    Returns ``(events, declared_epoch, meta)``.
    """
    path = FIXTURES_DIR / name
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data, None, {"shape": "flat"}
    if isinstance(data, dict) and isinstance(data.get("stages"), list):
        stages = sorted(data["stages"], key=lambda s: int(s["stage"]))
        if not stages:
            raise ValueError(f"fixture {name} has no stages")
        picked = next((s for s in stages if int(s["stage"]) == next_stage), stages[-1])
        exhausted = int(picked["stage"]) < next_stage
        events = picked.get("events") or []
        # Re-running past the last stage replays it idempotently: known event
        # ids are skipped, so no generation raise is re-declared.
        declared = None if exhausted else picked.get("declared_epoch")
        return (
            events,
            int(declared) if declared is not None else None,
            {
                "shape": "staged",
                "stage": int(picked["stage"]),
                "stage_count": len(stages),
                "stage_title": picked.get("title"),
                "exhausted": exhausted,
                "note": "重跑最后阶段：已知事件按 event_id 幂等跳过，不再宣告提升"
                if exhausted
                else None,
            },
        )
    if isinstance(data, dict) and isinstance(data.get("events"), list):
        declared = data.get("epoch")
        return data["events"], (int(declared) if declared is not None else None), {"shape": "epoch_declared"}
    raise ValueError(f"fixture {name} must be an event array or object with events/stages")


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        return {
            "stream_count": conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"],
            "enabled_streams": conn.execute(
                "SELECT COUNT(*) c FROM project_streams WHERE enabled=1"
            ).fetchone()["c"],
            "batch_count": conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"],
            "projection_alive": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=0"
            ).fetchone()["c"],
            "projection_deleted": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=1"
            ).fetchone()["c"],
            "ooo_buffered": conn.execute("SELECT COUNT(*) c FROM ooo_buffer").fetchone()["c"],
            "apply_order": conn.execute(
                "SELECT value FROM settings WHERE key='apply_order'"
            ).fetchone()["value"],
            "epoch_rejected_total": conn.execute(
                "SELECT COALESCE(SUM(epoch_rejected_count),0) c FROM project_streams"
            ).fetchone()["c"],
            "epoch_bypassed_open": conn.execute(
                "SELECT COUNT(*) c FROM epoch_bypass WHERE status='open'"
            ).fetchone()["c"],
            "epoch_bump_total": conn.execute(
                "SELECT COALESCE(SUM(epoch_bump_count),0) c FROM project_streams"
            ).fetchone()["c"],
        }
    finally:
        conn.close()


def list_streams() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return stream_repo.list_streams(conn)
    finally:
        conn.close()


def get_stream(stream_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        s = stream_repo.get_stream(conn, stream_id)
        if s:
            s["epoch_state"] = _epoch_state(conn, stream_id)
        return s
    finally:
        conn.close()


def _epoch_state(conn, stream_id: int) -> dict[str, Any]:
    s = stream_repo.get_stream(conn, stream_id)
    open_bypass = conn.execute(
        "SELECT COUNT(*) c FROM epoch_bypass WHERE stream_id=? AND status='open'",
        (stream_id,),
    ).fetchone()["c"]
    return {
        "current_epoch": s["current_epoch"],
        "late_epoch_policy": s["late_epoch_policy"],
        "epoch_bump_buffer_policy": s["epoch_bump_buffer_policy"],
        "epoch_bump_count": s["epoch_bump_count"],
        "epoch_rejected_count": s["epoch_rejected_count"],
        "epoch_bypassed_count": s["epoch_bypassed_count"],
        "epoch_replayed_count": s["epoch_replayed_count"],
        "epoch_replay_blocked_count": s["epoch_replay_blocked_count"],
        "bypass_open": open_bypass,
        "last_epoch_bump_at": s["last_epoch_bump_at"],
    }


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = batch_repo.list_batches(conn, limit=limit)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_projections(stream_id: int | None = None, include_deleted: bool = True) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = proj_repo.list_projections(
            conn, stream_id=stream_id, include_deleted=include_deleted
        )
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_events(batch_id: int | None = None, stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_applied_events(conn, batch_id=batch_id, stream_id=stream_id)
    finally:
        conn.close()


def list_ooo_buffer(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return proj_repo.list_ooo_buffer(conn, stream_id=stream_id)
    finally:
        conn.close()


def list_settings() -> dict[str, str]:
    conn = connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def set_stream_policies(
    stream_id: int,
    *,
    late_epoch_policy: str | None = None,
    epoch_bump_buffer_policy: str | None = None,
) -> dict[str, Any]:
    if late_epoch_policy is not None and late_epoch_policy not in LATE_EPOCH_POLICIES:
        raise ValueError(f"late_epoch_policy must be one of {LATE_EPOCH_POLICIES}")
    if epoch_bump_buffer_policy is not None and epoch_bump_buffer_policy not in BUMP_POLICIES:
        raise ValueError(f"epoch_bump_buffer_policy must be one of {BUMP_POLICIES}")
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        stream_repo.set_epoch_policies(
            conn,
            stream_id,
            late_epoch_policy=late_epoch_policy,
            epoch_bump_buffer_policy=epoch_bump_buffer_policy,
        )
        conn.commit()
        return _epoch_state(conn, stream_id)
    finally:
        conn.close()


def list_bypass(stream_id: int | None = None, origin: str | None = None, status: str | None = None):
    conn = connect()
    try:
        rows = proj_repo.list_bypass(conn, stream_id=stream_id, origin=origin, status=status)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_epoch_changes(stream_id: int | None = None, limit: int = 50):
    conn = connect()
    try:
        return proj_repo.list_epoch_changes(conn, stream_id=stream_id, limit=limit)
    finally:
        conn.close()


def list_replay_log(stream_id: int | None = None, limit: int = 100):
    conn = connect()
    try:
        return proj_repo.list_replay_log(conn, stream_id=stream_id, limit=limit)
    finally:
        conn.close()


def epoch_panel() -> dict[str, Any]:
    """Everything the epoch page cross-checks against each other."""
    conn = connect()
    try:
        streams = stream_repo.list_streams(conn)
        out_streams = []
        for s in streams:
            state = _epoch_state(conn, s["id"])
            main_alive = conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=0",
                (s["id"],),
            ).fetchone()["c"]
            main_deleted = conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=1",
                (s["id"],),
            ).fetchone()["c"]
            main_rows = conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=?", (s["id"],)
            ).fetchone()["c"]
            bypass_rows = conn.execute(
                "SELECT COUNT(*) c FROM epoch_bypass WHERE stream_id=?", (s["id"],)
            ).fetchone()["c"]
            applied_events = conn.execute(
                "SELECT COUNT(*) c FROM applied_events WHERE stream_id=?", (s["id"],)
            ).fetchone()["c"]
            out_streams.append(
                {
                    "id": s["id"],
                    "code": s["code"],
                    "title": s["title"],
                    "watermark_seq": s["watermark_seq"],
                    "fixture_stage": s.get("fixture_stage", 0),
                    **state,
                    "projected_alive": main_alive,
                    "projected_deleted": main_deleted,
                    "projection_rows": main_rows,
                    "bypass_rows": bypass_rows,
                    "applied_events": applied_events,
                }
            )
        return {
            "streams": out_streams,
            "epoch_changes": proj_repo.list_epoch_changes(conn, limit=30),
            "bypass_open_total": conn.execute(
                "SELECT COUNT(*) c FROM epoch_bypass WHERE status='open'"
            ).fetchone()["c"],
            "replay_log": proj_repo.list_replay_log(conn, limit=20),
        }
    finally:
        conn.close()


# --- projection run ------------------------------------------------------


def _apply_bump_if_declared(
    conn,
    *,
    stream: dict[str, Any],
    declared_epoch: int | None,
    buffer_policy: str,
    source: str,
    note: str | None,
    now: str,
) -> dict[str, Any] | None:
    """Atomically switch the stream epoch when the fixture declares a raise."""
    current = int(stream["current_epoch"])
    if declared_epoch is None or declared_epoch <= current:
        return None

    buffered = conn.execute(
        "SELECT * FROM ooo_buffer WHERE stream_id=? AND status='open'", (stream["id"],)
    ).fetchall()
    rejected = migrated = 0
    if buffer_policy == BUMP_POLICY_REJECT:
        # Refuse every unfinished old-epoch buffer item, then clear them.
        for r in buffered:
            rejected += 1
            conn.execute(
                "UPDATE ooo_buffer SET status='rejected_on_bump' WHERE id=?", (r["id"],)
            )
    elif buffer_policy == BUMP_POLICY_MIGRATE:
        for r in buffered:
            new_id = proj_repo.insert_bypass_row(
                conn,
                stream_id=stream["id"],
                row={
                    "event_id": r["event_id"],
                    "entity_key": r["entity_key"],
                    "op": r["op"],
                    "seq": r["seq"],
                    "event_time": r["event_time"],
                    "epoch": r["epoch"],
                    "payload": json.loads(r["payload_json"]) if r["payload_json"] else None,
                    "origin": "bump_migrated",
                    "reason": "epoch_raised_while_buffered",
                },
                bypassed_at=now,
            )
            if new_id > 0:
                migrated += 1
            conn.execute(
                "UPDATE ooo_buffer SET status='migrated_on_bump' WHERE id=?", (r["id"],)
            )

    stream_repo.set_epoch(conn, stream["id"], declared_epoch, now)
    if migrated:
        stream_repo.add_epoch_counters(conn, stream["id"], bypassed=migrated)
    proj_alive = conn.execute(
        "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=0",
        (stream["id"],),
    ).fetchone()["c"]
    refreshed = stream_repo.get_stream(conn, stream["id"])
    change_id = proj_repo.insert_epoch_change(
        conn,
        stream_id=stream["id"],
        from_epoch=current,
        to_epoch=declared_epoch,
        source=source,
        buffer_policy=buffer_policy,
        buffered_seen=len(buffered),
        buffered_rejected=rejected,
        buffered_migrated=migrated,
        projected_alive=proj_alive,
        epoch_rejected_total=refreshed["epoch_rejected_count"],
        epoch_bypassed_total=refreshed["epoch_bypassed_count"],
        note=note,
        created_at=now,
    )
    return {
        "change_id": change_id,
        "from_epoch": current,
        "to_epoch": declared_epoch,
        "buffer_policy": buffer_policy,
        "buffered_seen": len(buffered),
        "buffered_rejected": rejected,
        "buffered_migrated": migrated,
    }


def run_project(stream_id: int, note: str | None = None) -> dict[str, Any]:
    """Load the CDC fixture, apply it under the stream's epoch fence."""
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")

        events, declared_epoch, fixture_meta = _collection(
            stream["fixture"], next_stage=int(stream.get("fixture_stage", 0)) + 1
        )
        if fixture_meta.get("exhausted") and not events:
            raise ValueError("fixture has no further stage to apply")
        now = _now()
        buffer_policy = stream["epoch_bump_buffer_policy"]
        bump = _apply_bump_if_declared(
            conn,
            stream=stream,
            declared_epoch=declared_epoch,
            buffer_policy=buffer_policy,
            source="fixture_declared",
            note=note,
            now=now,
        )
        stream = stream_repo.get_stream(conn, stream_id)
        assert stream is not None
        current_epoch = int(stream["current_epoch"])

        known = proj_repo.list_known_event_ids(conn, stream_id)
        existing = proj_repo.load_stream_projection(conn, stream_id)
        result = apply_events(
            events,
            current_epoch=current_epoch,
            late_epoch_policy=stream["late_epoch_policy"],
            known_event_ids=known,
            projection=existing,
        )

        counts = result["counts"]
        seq_no = conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"] + 1
        batch_no = f"PB-{now[:10].replace('-', '')}-{seq_no:04d}"
        batch_id = batch_repo.insert_batch(
            conn,
            batch_no=batch_no,
            stream_id=stream_id,
            status="completed",
            counts=counts,
            note=note,
            created_at=now,
            epoch_after=current_epoch,
            epoch_bumped_from=bump["from_epoch"] if bump else None,
            epoch_bumped_to=bump["to_epoch"] if bump else None,
        )
        changed = {
            k: result["projection"][k]
            for k in result["changed_entity_keys"]
            if k in result["projection"]
        }
        proj_repo.upsert_projection_rows(
            conn, stream_id=stream_id, rows=changed, updated_at=now
        )

        fresh_bypass = 0
        for row in result["bypass_rows"]:
            new_id = proj_repo.insert_bypass_row(
                conn, stream_id=stream_id, row=row, bypassed_at=now
            )
            if new_id > 0:
                fresh_bypass += 1

        for ev in result["applied_log"]:
            batch_repo.insert_applied_event(
                conn, batch_id=batch_id, stream_id=stream_id, event=ev, created_at=now
            )

        if counts["max_seq"] > 0:
            new_watermark = max(int(stream["watermark_seq"]), counts["max_seq"])
            stream_repo.set_watermark(conn, stream_id, new_watermark)
        stream_repo.add_epoch_counters(
            conn,
            stream_id,
            rejected=counts["epoch_rejected"],
            bypassed=fresh_bypass,
        )
        if fixture_meta.get("stage") is not None:
            stream_repo.set_fixture_stage(conn, stream_id, int(fixture_meta["stage"]))
        conn.commit()

        batch = batch_repo.get_batch(conn, batch_id)
        assert batch is not None
        batch["stream_code"] = stream["code"]
        batch["stream_title"] = stream["title"]
        batch["apply_order"] = "by_seq"
        batch["counts"] = counts
        batch["epoch_bump"] = bump
        batch["fixture_stage"] = fixture_meta.get("stage")
        batch["fixture_stage_count"] = fixture_meta.get("stage_count")
        batch["fixture_stage_title"] = fixture_meta.get("stage_title")
        batch["epoch_state"] = _epoch_state(conn, stream_id)
        batch["projection_preview"] = proj_repo.list_projections(conn, stream_id=stream_id)[:20]
        batch["event_preview"] = batch_repo.list_applied_events(conn, batch_id=batch_id, limit=20)
        batch["bypass_preview"] = proj_repo.list_bypass(conn, stream_id=stream_id)[:10]
        return batch
    finally:
        conn.close()


# --- manual epoch bump ---------------------------------------------------


def bump_epoch(
    stream_id: int,
    *,
    to_epoch: int | None = None,
    buffer_policy: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Manually declare a generation raise (fixture-free trigger)."""
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")
        target = int(to_epoch) if to_epoch is not None else int(stream["current_epoch"]) + 1
        if target <= int(stream["current_epoch"]):
            raise ValueError("to_epoch must be greater than current_epoch")
        policy = buffer_policy or stream["epoch_bump_buffer_policy"]
        if policy not in BUMP_POLICIES:
            raise ValueError(f"buffer_policy must be one of {BUMP_POLICIES}")
        now = _now()
        bump = _apply_bump_if_declared(
            conn,
            stream=stream,
            declared_epoch=target,
            buffer_policy=policy,
            source="manual",
            note=note,
            now=now,
        )
        conn.commit()
        assert bump is not None
        state = _epoch_state(conn, stream_id)
        return {**bump, "epoch_state": state}
    finally:
        conn.close()


# --- bypass replay -------------------------------------------------------


def replay_bypass(
    stream_id: int,
    *,
    bypass_id: int | None = None,
    event_id: str | None = None,
    payload_override: dict[str, Any] | None = None,
    allow_overwrite_newer: bool = False,
) -> dict[str, Any]:
    """Manually replay one bypass row into the main projection.

    Replay never automatically overrides an entity whose main projection row
    was written by the current generation. That requires an explicit
    ``allow_overwrite_newer`` flag and is logged as an explicit override.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if bypass_id is not None:
            row = proj_repo.get_bypass_row(conn, bypass_id)
        elif event_id is not None:
            r = conn.execute(
                "SELECT id FROM epoch_bypass WHERE stream_id=? AND event_id=?",
                (stream_id, event_id),
            ).fetchone()
            row = proj_repo.get_bypass_row(conn, int(r["id"])) if r else None
        else:
            raise ValueError("bypass_id or event_id is required")
        if not row or int(row["stream_id"]) != stream_id:
            raise ValueError("bypass row not found for this stream")
        if row["status"] == "replayed":
            raise ValueError("bypass row already replayed")

        now = _now()
        current_epoch = int(stream["current_epoch"])
        entity = row["entity_key"]
        main = proj_repo.get_projection_row(conn, stream_id=stream_id, entity_key=entity)
        blocked_reason: str | None = None
        if main is not None and not allow_overwrite_newer:
            if int(main.get("applied_epoch", 1)) >= current_epoch:
                blocked_reason = "newer_generation_already_projected"
            elif int(main.get("applied_seq", 0)) >= int(row["seq"]):
                # Replaying an older seq over a newer applied seq would roll
                # the entity back, which is exactly what the fence prevents.
                blocked_reason = "older_seq_would_rollback"

        if blocked_reason:
            proj_repo.insert_replay_log(
                conn,
                stream_id=stream_id,
                bypass_id=int(row["id"]),
                event_id=row["event_id"],
                entity_key=entity,
                result="blocked",
                reason=blocked_reason,
                created_at=now,
            )
            proj_repo.mark_bypass_replayed(
                conn,
                bypass_id=int(row["id"]),
                status=row["status"],
                result="blocked_newer_generation",
                at=now,
            )
            stream_repo.add_epoch_counters(conn, stream_id, replay_blocked=1)
            conn.commit()
            return {
                "result": "blocked",
                "reason": blocked_reason,
                "event_id": row["event_id"],
                "entity_key": entity,
                "epoch_state": _epoch_state(conn, stream_id),
            }

        payload = payload_override if payload_override is not None else row["payload"]
        op = row["op"]
        new_row = {
            "entity_key": entity,
            "payload": None if op == "delete" else (payload or {}),
            "applied_seq": int(row["seq"]),
            "applied_event_id": row["event_id"],
            "applied_epoch": int(row["epoch"]),
            "deleted": op == "delete",
        }
        proj_repo.upsert_projection_rows(
            conn, stream_id=stream_id, rows={entity: new_row}, updated_at=now
        )
        proj_repo.mark_bypass_replayed(
            conn,
            bypass_id=int(row["id"]),
            status="replayed",
            result="replayed_overwrite" if main is not None else "replayed_insert",
            at=now,
        )
        proj_repo.insert_replay_log(
            conn,
            stream_id=stream_id,
            bypass_id=int(row["id"]),
            event_id=row["event_id"],
            entity_key=entity,
            result="replayed_overwrite" if main is not None else "replayed_insert",
            reason="manual_replay_override" if allow_overwrite_newer else "manual_replay",
            created_at=now,
        )
        stream_repo.add_epoch_counters(conn, stream_id, replayed=1)
        conn.commit()
        return {
            "result": "replayed",
            "event_id": row["event_id"],
            "entity_key": entity,
            "overwrote_existing": main is not None,
            "explicit_override": bool(allow_overwrite_newer),
            "projection_row": proj_repo.get_projection_row(
                conn, stream_id=stream_id, entity_key=entity
            ),
            "epoch_state": _epoch_state(conn, stream_id),
        }
    finally:
        conn.close()


def seed_old_buffer(stream_id: int, *, count: int = 2, note: str | None = None) -> dict[str, Any]:
    """Demo hook: park unfinished superseded-generation rows in the buffer.

    The base out-of-order buffer is otherwise empty; these rows make the
    bump-time buffer policy (refuse vs. migrate-to-bypass) observable.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        current = int(stream["current_epoch"])
        old = max(1, current - 1)
        now = _now()
        base_seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0) m FROM ooo_buffer WHERE stream_id=?", (stream_id,)
        ).fetchone()["m"]
        inserted = 0
        for i in range(1, int(count) + 1):
            event_id = f"buf-old-{current}-{base_seq + i}"
            conn.execute(
                """
                INSERT INTO ooo_buffer(
                    stream_id, event_id, entity_key, op, seq, event_time, epoch,
                    payload_json, buffered_at, status
                ) VALUES (?,?,?,?,?,?,?,?,?, 'open')
                """,
                (
                    stream_id,
                    event_id,
                    f"BUF{base_seq + i}",
                    "upsert",
                    int(base_seq) + i,
                    now,
                    old,
                    json.dumps({"note": note or "旧世代未完成缓冲"}, ensure_ascii=False),
                    now,
                ),
            )
            inserted += 1
        conn.commit()
        return {"inserted": inserted, "epoch": old, "current_epoch": current}
    finally:
        conn.close()
