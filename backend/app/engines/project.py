"""Apply CDC events onto a projection under a source-epoch fence.

The projector keeps one *current epoch* per stream. Events carry their own
generation number. An event whose epoch is older than the stream's current
epoch must never touch the main projection or the watermark: it is either
rejected outright, or parked in an old-generation bypass bucket for manual
replay. Events from a newer epoch are held back too until a bump is
explicitly declared by the fixture or by configuration.

Base seq ordering is preserved for *accepted* events; duplicates are skipped
by event_id. Out-of-order windowing is a different topic and is not done here.
"""

from __future__ import annotations

from typing import Any

POLICY_REJECT = "reject"
POLICY_BYPASS = "bypass"

RESULT_APPLIED_UPSERT = "applied_upsert"
RESULT_APPLIED_DELETE = "applied_delete"
RESULT_SKIPPED_DUPLICATE = "skipped_duplicate"
RESULT_REJECTED_LATE_EPOCH = "rejected_late_epoch"
RESULT_REJECTED_AHEAD_EPOCH = "rejected_ahead_epoch"
RESULT_BYPASSED_LATE_EPOCH = "bypassed_late_epoch"

LATE_EPOCH_POLICIES = (POLICY_REJECT, POLICY_BYPASS)


def _new_counts() -> dict[str, int]:
    return {
        "source_events": 0,
        "applied_upsert": 0,
        "applied_delete": 0,
        "skipped_duplicate": 0,
        "epoch_rejected": 0,
        "epoch_ahead_rejected": 0,
        "epoch_late_rejected": 0,
        "epoch_bypassed": 0,
        "max_seq": 0,
    }


def apply_events(
    events: list[dict[str, Any]],
    *,
    current_epoch: int,
    late_epoch_policy: str = POLICY_REJECT,
    known_event_ids: set[str] | None = None,
    projection: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return the *accepted* projection plus logs, bypass rows and counters.

    ``projection`` is the existing per-entity projection map for the stream.
    Only entities touched by accepted events are returned in
    ``changed_entity_keys`` so the caller can update state in place without
    rewriting untouched rows (a rejected event must leave the projection
    exactly as it was).
    """

    policy = late_epoch_policy if late_epoch_policy in LATE_EPOCH_POLICIES else POLICY_REJECT
    known = set(known_event_ids or ())
    proj: dict[str, dict[str, Any]] = {k: dict(v) for k, v in (projection or {}).items()}
    counts = _new_counts()
    counts["source_events"] = len(events)
    applied_log: list[dict[str, Any]] = []
    bypass_rows: list[dict[str, Any]] = []
    changed_entity_keys: list[str] = []

    ordered = sorted(events, key=lambda e: (int(e.get("seq", 0)), str(e.get("event_id", ""))))

    for ev in ordered:
        event_id = str(ev["event_id"])
        entity_key = str(ev["entity_key"])
        op = str(ev.get("op", "upsert"))
        seq = int(ev.get("seq", 0))
        event_time = str(ev.get("event_time", ""))
        event_epoch = int(ev.get("epoch", current_epoch))
        entry_base = {
            "event_id": event_id,
            "entity_key": entity_key,
            "op": op,
            "seq": seq,
            "event_time": event_time,
            "epoch": event_epoch,
            "payload": ev.get("payload"),
        }

        if event_id in known:
            counts["skipped_duplicate"] += 1
            applied_log.append({**entry_base, "result": RESULT_SKIPPED_DUPLICATE})
            continue

        if event_epoch < current_epoch:
            if policy == POLICY_REJECT:
                counts["epoch_rejected"] += 1
                counts["epoch_late_rejected"] += 1
                applied_log.append({**entry_base, "result": RESULT_REJECTED_LATE_EPOCH})
            else:
                counts["epoch_bypassed"] += 1
                applied_log.append({**entry_base, "result": RESULT_BYPASSED_LATE_EPOCH})
                bypass_rows.append(
                    {
                        "event_id": event_id,
                        "entity_key": entity_key,
                        "op": op,
                        "seq": seq,
                        "event_time": event_time,
                        "epoch": event_epoch,
                        "payload": ev.get("payload"),
                        "origin": "late_arrival",
                        "reason": "event_epoch_lt_stream_epoch",
                    }
                )
            known.add(event_id)
            continue

        if event_epoch > current_epoch:
            # A newer generation may only enter after an explicitly declared
            # epoch bump. Until then the event is fenced off, but NOT consumed:
            # it stays re-deliverable so it can land once the bump is declared.
            counts["epoch_rejected"] += 1
            counts["epoch_ahead_rejected"] += 1
            applied_log.append({**entry_base, "result": RESULT_REJECTED_AHEAD_EPOCH})
            continue

        known.add(event_id)
        if op == "delete":
            proj[entity_key] = {
                "entity_key": entity_key,
                "payload": None,
                "applied_seq": seq,
                "applied_event_id": event_id,
                "applied_epoch": event_epoch,
                "deleted": True,
            }
            counts["applied_delete"] += 1
            result = RESULT_APPLIED_DELETE
        else:
            proj[entity_key] = {
                "entity_key": entity_key,
                "payload": ev.get("payload") or {},
                "applied_seq": seq,
                "applied_event_id": event_id,
                "applied_epoch": event_epoch,
                "deleted": False,
            }
            counts["applied_upsert"] += 1
            result = RESULT_APPLIED_UPSERT

        counts["max_seq"] = max(counts["max_seq"], seq)
        changed_entity_keys.append(entity_key)
        applied_log.append({**entry_base, "result": result})

    alive = sum(1 for r in proj.values() if not r["deleted"])
    counts["projected_alive"] = alive
    counts["projected_deleted"] = len(proj) - alive
    return {
        "counts": counts,
        "projection": proj,
        "changed_entity_keys": changed_entity_keys,
        "applied_log": applied_log,
        "bypass_rows": bypass_rows,
    }
