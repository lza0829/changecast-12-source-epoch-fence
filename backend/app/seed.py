from __future__ import annotations

from datetime import datetime, timezone

from app.db import connect

STREAM_SEEDS = [
    {
        "code": "member_profile",
        "title": "会员档案变更",
        "domain": "member",
        "entity_key_name": "member_id",
        "payload_fields": ["display_name", "tier", "region"],
        "fixture": "member_profile.json",
        "enabled": 1,
    },
    {
        "code": "store_archive",
        "title": "门店档案变更",
        "domain": "store",
        "entity_key_name": "store_id",
        "payload_fields": ["name", "status", "city"],
        "fixture": "store_archive.json",
        "enabled": 1,
    },
    {
        "code": "channel_attr",
        "title": "渠道属性变更",
        "domain": "channel",
        "entity_key_name": "channel_code",
        "payload_fields": ["label", "active"],
        "fixture": "channel_attr.json",
        "enabled": 1,
    },
]

SETTING_SEEDS = [
    ("apply_order", "by_seq"),
    ("duplicate_policy", "skip"),
    ("tombstone_mode", "soft_delete"),
]

# Columns added for the epoch fence topic. Kept as ALTER statements so an
# existing SQLite volume from the base snapshot keeps working.
_ADDED_COLUMNS = [
    ("project_streams", "current_epoch", "INTEGER NOT NULL DEFAULT 1"),
    ("project_streams", "late_epoch_policy", "TEXT NOT NULL DEFAULT 'reject'"),
    ("project_streams", "epoch_bump_buffer_policy", "TEXT NOT NULL DEFAULT 'migrate_bypass'"),
    ("project_streams", "epoch_bump_count", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "fixture_stage", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "epoch_rejected_count", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "epoch_bypassed_count", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "epoch_replayed_count", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "epoch_replay_blocked_count", "INTEGER NOT NULL DEFAULT 0"),
    ("project_streams", "last_epoch_bump_at", "TEXT"),
    ("project_batches", "epoch_after", "INTEGER NOT NULL DEFAULT 0"),
    ("project_batches", "epoch_bumped_from", "INTEGER"),
    ("project_batches", "epoch_bumped_to", "INTEGER"),
    ("project_batches", "epoch_rejected", "INTEGER NOT NULL DEFAULT 0"),
    ("project_batches", "epoch_bypassed", "INTEGER NOT NULL DEFAULT 0"),
    ("projection_rows", "applied_epoch", "INTEGER NOT NULL DEFAULT 1"),
    ("applied_events", "epoch", "INTEGER NOT NULL DEFAULT 1"),
    ("ooo_buffer", "epoch", "INTEGER NOT NULL DEFAULT 1"),
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_column(conn, table: str, column: str, ddl: str) -> None:
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db() -> None:
    import json

    conn = connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS project_streams(
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                domain TEXT NOT NULL,
                entity_key_name TEXT NOT NULL,
                payload_fields_json TEXT NOT NULL,
                fixture TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                watermark_seq INTEGER NOT NULL DEFAULT 0,
                current_epoch INTEGER NOT NULL DEFAULT 1,
                late_epoch_policy TEXT NOT NULL DEFAULT 'reject',
                epoch_bump_buffer_policy TEXT NOT NULL DEFAULT 'migrate_bypass',
                fixture_stage INTEGER NOT NULL DEFAULT 0,
                epoch_bump_count INTEGER NOT NULL DEFAULT 0,
                epoch_rejected_count INTEGER NOT NULL DEFAULT 0,
                epoch_bypassed_count INTEGER NOT NULL DEFAULT 0,
                epoch_replayed_count INTEGER NOT NULL DEFAULT 0,
                epoch_replay_blocked_count INTEGER NOT NULL DEFAULT 0,
                last_epoch_bump_at TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS project_batches(
                id INTEGER PRIMARY KEY,
                batch_no TEXT NOT NULL UNIQUE,
                stream_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                source_events INTEGER NOT NULL,
                applied_upsert INTEGER NOT NULL,
                applied_delete INTEGER NOT NULL,
                skipped_duplicate INTEGER NOT NULL,
                projected_alive INTEGER NOT NULL,
                projected_deleted INTEGER NOT NULL,
                max_seq INTEGER NOT NULL,
                epoch_after INTEGER NOT NULL DEFAULT 0,
                epoch_bumped_from INTEGER,
                epoch_bumped_to INTEGER,
                epoch_rejected INTEGER NOT NULL DEFAULT 0,
                epoch_bypassed INTEGER NOT NULL DEFAULT 0,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS projection_rows(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                entity_key TEXT NOT NULL,
                payload_json TEXT,
                applied_seq INTEGER NOT NULL,
                applied_event_id TEXT NOT NULL,
                applied_epoch INTEGER NOT NULL DEFAULT 1,
                deleted INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(stream_id, entity_key),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS applied_events(
                id INTEGER PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 1,
                result TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(batch_id) REFERENCES project_batches(id),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS ooo_buffer(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 1,
                payload_json TEXT,
                buffered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS epoch_changes(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                from_epoch INTEGER NOT NULL,
                to_epoch INTEGER NOT NULL,
                source TEXT NOT NULL,
                buffer_policy TEXT NOT NULL,
                buffered_seen INTEGER NOT NULL DEFAULT 0,
                buffered_rejected INTEGER NOT NULL DEFAULT 0,
                buffered_migrated INTEGER NOT NULL DEFAULT 0,
                projected_alive INTEGER NOT NULL DEFAULT 0,
                epoch_rejected_total INTEGER NOT NULL DEFAULT 0,
                epoch_bypassed_total INTEGER NOT NULL DEFAULT 0,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS epoch_bypass(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                payload_json TEXT,
                origin TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                bypassed_at TEXT NOT NULL,
                replayed_at TEXT,
                last_replay_result TEXT,
                last_replay_at TEXT,
                UNIQUE(stream_id, event_id),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS epoch_replay_log(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                bypass_id INTEGER,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                result TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id),
                FOREIGN KEY(bypass_id) REFERENCES epoch_bypass(id)
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        for table, column, ddl in _ADDED_COLUMNS:
            _ensure_column(conn, table, column, ddl)

        if conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"] == 0:
            now = _now()
            for s in STREAM_SEEDS:
                conn.execute(
                    """
                    INSERT INTO project_streams(
                        code, title, domain, entity_key_name, payload_fields_json,
                        fixture, enabled, watermark_seq, current_epoch, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        s["code"],
                        s["title"],
                        s["domain"],
                        s["entity_key_name"],
                        json.dumps(s["payload_fields"], ensure_ascii=False),
                        s["fixture"],
                        s["enabled"],
                        0,
                        1,
                        now,
                    ),
                )
        for key, value in SETTING_SEEDS:
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                (key, value),
            )
        conn.commit()
    finally:
        conn.close()
