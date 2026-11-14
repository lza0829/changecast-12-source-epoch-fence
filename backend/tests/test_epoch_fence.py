from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr("app.config.DATA_DIR", tmp_path)
    monkeypatch.setattr("app.db.DATA_DIR", tmp_path)
    monkeypatch.setattr("app.db.DB_PATH", tmp_path / "app.db")
    with TestClient(app) as c:
        yield c


def _stream(client, code="member_profile"):
    for s in client.get("/api/streams").json():
        if s["code"] == code:
            return s
    raise AssertionError("stream missing")


def _run(client, stream_id):
    r = client.post("/api/project", json={"stream_id": stream_id})
    assert r.status_code == 200, r.text
    return r.json()


def test_stage_raise_is_atomic_and_rejects_late_events(client):
    s = _stream(client, "member_profile")
    assert s["current_epoch"] == 1

    first = _run(client, s["id"])
    assert first["epoch_after"] == 1
    assert first["epoch_rejected"] == 0
    assert first["applied_upsert"] == 4

    second = _run(client, s["id"])
    assert second["epoch_after"] == 2
    assert second["epoch_bump"]["from_epoch"] == 1
    assert second["epoch_bump"]["to_epoch"] == 2
    # Two epoch-2 events applied; the two epoch-1 late events never enter.
    assert second["applied_upsert"] == 2
    assert second["epoch_rejected"] == 2

    rows = client.get(f"/api/projections?stream_id={s['id']}").json()
    by_key = {r["entity_key"]: r for r in rows}
    # Late epoch-1 events must not have rolled M1001 back to gold.
    assert by_key["M1001"]["applied_epoch"] == 2
    assert by_key["M1001"]["payload"]["tier"] == "platinum"
    # Deleted entity is not resurrected by the old-generation late event.
    assert by_key["M1002"]["deleted"] is True

    st = _stream(client, "member_profile")
    assert st["current_epoch"] == 2
    assert st["epoch_rejected_count"] == 2


def test_bypass_policy_keeps_late_rows_out_of_main_projection(client):
    s = _stream(client, "member_profile")
    p = client.post(
        f"/api/epoch/policies/{s['id']}",
        json={"late_epoch_policy": "bypass"},
    )
    assert p.status_code == 200, p.text
    _run(client, s["id"])
    second = _run(client, s["id"])
    assert second["epoch_bypassed"] == 2
    assert second["epoch_rejected"] == 0
    after_rows = client.get(f"/api/projections?stream_id={s['id']}").json()
    after = {r["entity_key"]: r for r in after_rows}
    # Main projection holds the new generation; bypassed late rows never wrote it.
    assert after["M1001"]["applied_epoch"] == 2
    assert after["M1001"]["payload"]["tier"] == "platinum"
    assert after["M1002"]["deleted"] is True

    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}").json()
    assert len(bypass) == 2
    assert {b["event_id"] for b in bypass} == {"mp-005-late", "mp-003-late"}
    assert all(b["epoch"] == 1 and b["status"] == "open" for b in bypass)


def test_manual_bump_migrates_buffer_into_bypass(client):
    s = _stream(client, "store_archive")
    _run(client, s["id"])
    seed = client.post("/api/epoch/seed-buffer", json={"stream_id": s["id"], "count": 2}).json()
    assert seed["inserted"] == 2
    bump = client.post(
        "/api/epoch/bump",
        json={"stream_id": s["id"], "buffer_policy": "migrate_bypass"},
    )
    assert bump.status_code == 200, bump.text
    body = bump.json()
    assert body["from_epoch"] == 1 and body["to_epoch"] == 2
    assert body["buffered_seen"] == 2 and body["buffered_migrated"] == 2
    assert body["epoch_state"]["epoch_bypassed_count"] == 2

    buffers = client.get(f"/api/ooo-buffer?stream_id={s['id']}").json()
    assert all(b["status"] == "migrated_on_bump" for b in buffers)
    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}&origin=bump_migrated").json()
    assert len(bypass) == 2


def test_manual_bump_rejects_buffer(client):
    s = _stream(client, "channel_attr")
    client.post("/api/epoch/seed-buffer", json={"stream_id": s["id"], "count": 3})
    bump = client.post(
        "/api/epoch/bump",
        json={"stream_id": s["id"], "buffer_policy": "reject_buffer", "to_epoch": 5},
    ).json()
    assert bump["buffered_seen"] == 3 and bump["buffered_rejected"] == 3
    assert bump["to_epoch"] == 5
    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}").json()
    assert bypass == []
    buffers = client.get(f"/api/ooo-buffer?stream_id={s['id']}").json()
    assert all(b["status"] == "rejected_on_bump" for b in buffers)


def test_replay_never_auto_overrides_newer_generation(client):
    s = _stream(client, "member_profile")
    client.post(f"/api/epoch/policies/{s['id']}", json={"late_epoch_policy": "bypass"})
    _run(client, s["id"])
    _run(client, s["id"])
    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}").json()
    m1001 = next(b for b in bypass if b["entity_key"] == "M1001")

    blocked = client.post(
        "/api/epoch/replay",
        json={"stream_id": s["id"], "bypass_id": m1001["id"]},
    )
    assert blocked.status_code == 200
    assert blocked.json()["result"] == "blocked"
    assert blocked.json()["reason"] == "newer_generation_already_projected"
    rows = {r["entity_key"]: r for r in client.get(
        f"/api/projections?stream_id={s['id']}").json()}
    assert rows["M1001"]["payload"]["tier"] == "platinum"

    forced = client.post(
        "/api/epoch/replay",
        json={
            "stream_id": s["id"],
            "bypass_id": m1001["id"],
            "allow_overwrite_newer": True,
        },
    ).json()
    assert forced["result"] == "replayed"
    assert forced["explicit_override"] is True
    rows = {r["entity_key"]: r for r in client.get(
        f"/api/projections?stream_id={s['id']}").json()}
    assert rows["M1001"]["payload"]["tier"] == "gold"
    assert rows["M1001"]["applied_epoch"] == 1


def test_epoch_panel_cross_checks(client):
    s = _stream(client, "member_profile")
    _run(client, s["id"])
    _run(client, s["id"])
    panel = client.get("/api/epoch").json()
    row = next(r for r in panel["streams"] if r["id"] == s["id"])
    assert row["current_epoch"] == 2
    assert row["epoch_rejected_count"] == 2
    assert row["projection_rows"] == row["projected_alive"] + row["projected_deleted"]
    assert len(panel["epoch_changes"]) == 1
    assert panel["epoch_changes"][0]["from_epoch"] == 1
    assert panel["epoch_changes"][0]["to_epoch"] == 2


def test_unannounced_ahead_epoch_is_fenced(client, tmp_path, monkeypatch):
    """A newer-generation event without a declared raise must not enter."""
    import json as _json

    from app.config import FIXTURES_DIR

    fixture = {
        "stages": [
            {"stage": 1, "declared_epoch": 1, "events": [
                {"event_id": "x-1", "entity_key": "E1", "op": "upsert", "seq": 1,
                 "epoch": 1, "event_time": "2026-01-01T00:00:00Z", "payload": {"v": 1}},
            ]},
            {"stage": 2, "declared_epoch": 1, "events": [
                {"event_id": "x-2", "entity_key": "E1", "op": "upsert", "seq": 2,
                 "epoch": 4, "event_time": "2026-01-02T00:00:00Z", "payload": {"v": 9}},
            ]},
        ]
    }
    path = FIXTURES_DIR / "_tmp_ahead.json"
    path.write_text(_json.dumps(fixture), encoding="utf-8")
    try:
        s = _stream(client, "channel_attr")
        # Point the stream at the temp fixture for this test.
        import sqlite3
        from app.db import DB_PATH
        c = sqlite3.connect(DB_PATH)
        c.execute("UPDATE project_streams SET fixture='_tmp_ahead.json', fixture_stage=0 WHERE id=?", (s["id"],))
        c.commit(); c.close()

        _run(client, s["id"])
        second = _run(client, s["id"])
        assert second["epoch_after"] == 1
        assert second["epoch_rejected"] == 1
        assert second["applied_upsert"] == 0
        rows = client.get(f"/api/projections?stream_id={s['id']}").json()
        assert rows[0]["payload"] == {"v": 1}
        evs = client.get(f"/api/events?stream_id={s['id']}").json()
        assert any(e["result"] == "rejected_ahead_epoch" for e in evs)
    finally:
        path.unlink(missing_ok=True)


def test_rerun_of_last_stage_is_idempotent(client):
    s = _stream(client, "member_profile")
    _run(client, s["id"])
    _run(client, s["id"])
    state = _stream(client, "member_profile")
    third = _run(client, s["id"])
    assert third["epoch_after"] == 2
    assert third["epoch_bump"] is None
    assert third["applied_upsert"] == 0
    assert third["epoch_rejected"] == 0
    assert _stream(client, "member_profile")["current_epoch"] == 2
    assert _stream(client, "member_profile")["epoch_rejected_count"] == state["epoch_rejected_count"]


def test_ahead_epoch_event_lands_after_declared_bump(client):
    import json as _json
    import sqlite3

    from app.config import FIXTURES_DIR
    from app.db import DB_PATH

    fixture = {
        "stages": [
            {"stage": 1, "declared_epoch": 1, "events": [
                {"event_id": "z-1", "entity_key": "E1", "op": "upsert", "seq": 1,
                 "epoch": 1, "event_time": "2026-01-01T00:00:00Z", "payload": {"v": 1}},
            ]},
            {"stage": 2, "declared_epoch": 1, "events": [
                {"event_id": "z-2", "entity_key": "E1", "op": "upsert", "seq": 2,
                 "epoch": 4, "event_time": "2026-01-02T00:00:00Z", "payload": {"v": 9}},
            ]},
        ]
    }
    path = FIXTURES_DIR / "_tmp_ahead2.json"
    path.write_text(_json.dumps(fixture), encoding="utf-8")
    try:
        s = _stream(client, "channel_attr")
        c = sqlite3.connect(DB_PATH)
        c.execute("UPDATE project_streams SET fixture='_tmp_ahead2.json', fixture_stage=0 WHERE id=?", (s["id"],))
        c.commit(); c.close()

        _run(client, s["id"])            # stage 1
        _run(client, s["id"])            # stage 2, epoch-4 event fenced
        rows = client.get(f"/api/projections?stream_id={s['id']}").json()
        assert rows[0]["payload"] == {"v": 1}

        # Declare the raise the event was waiting for, then re-deliver stage 2.
        client.post("/api/epoch/bump", json={"stream_id": s["id"], "to_epoch": 4})
        third = _run(client, s["id"])
        assert third["applied_upsert"] == 1
        rows = client.get(f"/api/projections?stream_id={s['id']}").json()
        assert rows[0]["payload"] == {"v": 9}
        assert rows[0]["applied_epoch"] == 4
    finally:
        path.unlink(missing_ok=True)


def test_replay_blocks_older_seq_even_within_current_generation(client):
    """An old-gen replay must not roll back a newer applied seq."""
    s = _stream(client, "store_archive")
    client.post(f"/api/epoch/policies/{s['id']}", json={"late_epoch_policy": "bypass"})
    _run(client, s["id"])
    _run(client, s["id"])
    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}").json()
    row = next(b for b in bypass if b["event_id"] == "sa-003-late")
    rows = {r["entity_key"]: r for r in client.get(
        f"/api/projections?stream_id={s['id']}").json()}
    assert rows["S01"]["applied_seq"] >= row["seq"]

    r = client.post("/api/epoch/replay", json={"stream_id": s["id"], "bypass_id": row["id"]})
    assert r.status_code == 200
    body = r.json()
    assert body["result"] == "blocked"
    assert body["reason"] in ("newer_generation_already_projected", "older_seq_would_rollback")


def test_replay_inserts_when_entity_absent(client):
    s = _stream(client, "channel_attr")
    client.post(f"/api/epoch/policies/{s['id']}", json={"late_epoch_policy": "bypass"})
    _run(client, s["id"])
    _run(client, s["id"])
    bypass = client.get(f"/api/epoch/bypass?stream_id={s['id']}").json()
    assert bypass, "expected a bypassed late event"
    target = bypass[0]
    import sqlite3
    from app.db import DB_PATH
    c = sqlite3.connect(DB_PATH)
    c.execute("DELETE FROM projection_rows WHERE stream_id=? AND entity_key=?", (s["id"], target["entity_key"]))
    c.commit(); c.close()

    r = client.post("/api/epoch/replay", json={"stream_id": s["id"], "bypass_id": target["id"]}).json()
    assert r["result"] == "replayed"
    assert r["overwrote_existing"] is False
    rows = {x["entity_key"]: x for x in client.get(
        f"/api/projections?stream_id={s['id']}").json()}
    assert target["entity_key"] in rows
