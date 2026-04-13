from __future__ import annotations

import json
import os
import re
import sqlite3
from typing import Any

from App.services import cleaning_rules, parsing
from App.services.runtime import APP_BUILD_ID, DB_PATH, WORKER_LEASE_TTL_SECONDS, utc_after_seconds_iso, utc_now_iso


def _row_factory(cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = _row_factory
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS builders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                notes TEXT NOT NULL DEFAULT '',
                parser_strategy TEXT NOT NULL DEFAULT '',
                rule_config_json TEXT NOT NULL DEFAULT '{}',
                rule_config_updated_at TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS builder_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                builder_id INTEGER NOT NULL REFERENCES builders(id) ON DELETE CASCADE,
                stored_name TEXT NOT NULL,
                original_name TEXT NOT NULL,
                mime_type TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                uploaded_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_no TEXT NOT NULL UNIQUE,
                builder_id INTEGER NOT NULL REFERENCES builders(id) ON DELETE RESTRICT,
                title TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'idle',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS job_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                file_role TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                original_name TEXT NOT NULL,
                mime_type TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                uploaded_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                run_kind TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                requested_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT '',
                error_text TEXT NOT NULL DEFAULT '',
                parser_version TEXT NOT NULL DEFAULT '',
                worker_pid INTEGER NOT NULL DEFAULT 0,
                app_build_id TEXT NOT NULL DEFAULT '',
                parser_strategy TEXT NOT NULL DEFAULT '',
                worker_token TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS worker_leases (
                lease_key TEXT PRIMARY KEY,
                owner_token TEXT NOT NULL DEFAULT '',
                worker_pid INTEGER NOT NULL DEFAULT 0,
                app_build_id TEXT NOT NULL DEFAULT '',
                acquired_at TEXT NOT NULL DEFAULT '',
                heartbeat_at TEXT NOT NULL DEFAULT '',
                expires_at TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                snapshot_kind TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(job_id, snapshot_kind)
            );

            CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
                data_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS snapshot_verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(id) ON DELETE CASCADE,
                snapshot_kind TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                checked_by TEXT NOT NULL DEFAULT '',
                checked_at TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                checklist_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                action TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )
        _ensure_column(conn, "builders", "rule_config_json", "TEXT NOT NULL DEFAULT '{}'")
        _ensure_column(conn, "builders", "rule_config_updated_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "builders", "parser_strategy", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "runs", "worker_pid", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "runs", "app_build_id", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "runs", "parser_strategy", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "runs", "worker_token", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "snapshot_kind", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "status", "TEXT NOT NULL DEFAULT 'pending'")
        _ensure_column(conn, "snapshot_verifications", "checked_by", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "checked_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "notes", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "checklist_json", "TEXT NOT NULL DEFAULT '[]'")
        _ensure_column(conn, "snapshot_verifications", "created_at", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "snapshot_verifications", "updated_at", "TEXT NOT NULL DEFAULT ''")
        conn.execute(
            """
            UPDATE builders
            SET rule_config_json = ?,
                rule_config_updated_at = COALESCE(NULLIF(rule_config_updated_at, ''), updated_at)
            """,
            (cleaning_rules.serialize_rule_flags(cleaning_rules.global_rule_flags()),),
        )
        conn.execute(
            """
            UPDATE builders
            SET parser_strategy = ?
            """,
            (cleaning_rules.global_parser_strategy(),),
        )


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connect() as conn:
        return [_decorate_row(dict(row)) for row in conn.execute(query, params).fetchall()]


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connect() as conn:
        row = conn.execute(query, params).fetchone()
        return _decorate_row(dict(row)) if row else None


def _decorate_row(row: dict[str, Any]) -> dict[str, Any]:
    if "rule_config_json" in row:
        row["rule_flags"] = cleaning_rules.normalize_rule_flags(row.get("rule_config_json", ""))
    if "parser_strategy" in row:
        row["parser_strategy"] = cleaning_rules.normalize_parser_strategy(
            row.get("parser_strategy", ""),
            builder_name=str(row.get("name", "")),
            builder_slug=str(row.get("slug", "")),
        )
    return row


def create_builder(name: str, slug: str, notes: str) -> int:
    now = utc_now_iso()
    parser_strategy = cleaning_rules.default_parser_strategy(builder_name=name, builder_slug=slug)
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO builders (name, slug, notes, parser_strategy, rule_config_json, rule_config_updated_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                slug,
                notes,
                parser_strategy,
                cleaning_rules.serialize_rule_flags(cleaning_rules.global_rule_flags()),
                now,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)


def list_builders() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT b.*,
               (SELECT COUNT(*) FROM builder_templates t WHERE t.builder_id = b.id) AS template_count
        FROM builders b
        ORDER BY b.name COLLATE NOCASE
        """
    )


def get_builder(builder_id: int) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM builders WHERE id = ?", (builder_id,))


def get_builder_by_slug(slug: str) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM builders WHERE slug = ?", (slug,))


def update_builder_rules(builder_id: int, rule_flags: dict[str, bool], parser_strategy: str | None = None) -> None:
    now = utc_now_iso()
    with connect() as conn:
        conn.execute(
            """
            UPDATE builders
            SET parser_strategy = ?, rule_config_json = ?, rule_config_updated_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                cleaning_rules.global_parser_strategy(),
                cleaning_rules.serialize_rule_flags(cleaning_rules.global_rule_flags()),
                now,
                now,
                builder_id,
            ),
        )


def create_builder_template(builder_id: int, stored_name: str, original_name: str, mime_type: str, size_bytes: int) -> int:
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO builder_templates (builder_id, stored_name, original_name, mime_type, size_bytes, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (builder_id, stored_name, original_name, mime_type, size_bytes, utc_now_iso()),
        )
        return int(cur.lastrowid)


def list_builder_templates(builder_id: int) -> list[dict[str, Any]]:
    return fetch_all(
        "SELECT * FROM builder_templates WHERE builder_id = ? ORDER BY uploaded_at DESC, id DESC",
        (builder_id,),
    )


def get_builder_template(template_id: int) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM builder_templates WHERE id = ?", (template_id,))


def delete_builder_template(template_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM builder_templates WHERE id = ?", (template_id,))


def create_job(job_no: str, builder_id: int, title: str, notes: str) -> int:
    now = utc_now_iso()
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO jobs (job_no, builder_id, title, notes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'idle', ?, ?)
            """,
            (job_no, builder_id, title, notes, now, now),
        )
        return int(cur.lastrowid)


def list_jobs(job_query: str = "", sort_by: str = "created_desc") -> list[dict[str, Any]]:
    order_by = {
        "created_desc": "j.created_at DESC, j.id DESC",
        "updated_desc": "j.updated_at DESC, j.id DESC",
    }.get(sort_by, "j.created_at DESC, j.id DESC")
    query = (
        """
        SELECT j.*, b.name AS builder_name, b.slug AS builder_slug,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'spec') AS spec_file_count,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'drawing') AS drawing_file_count,
               (SELECT data_json FROM snapshots s WHERE s.job_id = j.id AND s.snapshot_kind = 'raw_spec') AS raw_snapshot_json
        FROM jobs j
        JOIN builders b ON b.id = j.builder_id
        """
    )
    params: tuple[Any, ...] = ()
    if job_query:
        query += " WHERE j.job_no LIKE ?"
        params = (f"%{job_query}%",)
    query += f" ORDER BY {order_by}"
    return fetch_all(query, params)


def get_job(job_id: int) -> dict[str, Any] | None:
    return fetch_one(
        """
        SELECT j.*, b.name AS builder_name, b.slug AS builder_slug
        FROM jobs j
        JOIN builders b ON b.id = j.builder_id
        WHERE j.id = ?
        """,
        (job_id,),
    )


def get_job_by_no(job_no: str) -> dict[str, Any] | None:
    return fetch_one(
        """
        SELECT j.*, b.name AS builder_name, b.slug AS builder_slug
        FROM jobs j
        JOIN builders b ON b.id = j.builder_id
        WHERE j.job_no = ?
        """,
        (job_no,),
    )


def update_job_status(job_id: int, status: str) -> None:
    with connect() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            (status, utc_now_iso(), job_id),
        )


def delete_job(job_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))


def create_job_file(job_id: int, file_role: str, stored_name: str, original_name: str, mime_type: str, size_bytes: int) -> int:
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO job_files (job_id, file_role, stored_name, original_name, mime_type, size_bytes, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, file_role, stored_name, original_name, mime_type, size_bytes, utc_now_iso()),
        )
        return int(cur.lastrowid)


def list_job_files(job_id: int, file_role: str | None = None) -> list[dict[str, Any]]:
    if file_role:
        return fetch_all(
            "SELECT * FROM job_files WHERE job_id = ? AND file_role = ? ORDER BY uploaded_at DESC, id DESC",
            (job_id, file_role),
        )
    return fetch_all(
        "SELECT * FROM job_files WHERE job_id = ? ORDER BY uploaded_at DESC, id DESC",
        (job_id,),
    )


def get_job_file(file_id: int) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM job_files WHERE id = ?", (file_id,))


def delete_job_file(file_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM job_files WHERE id = ?", (file_id,))


def create_run(job_id: int, run_kind: str) -> int:
    now = utc_now_iso()
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (job_id, run_kind, status, stage, message, requested_at, parser_version, app_build_id, result_json)
            VALUES (?, ?, 'queued', 'queued', 'Waiting for worker', ?, ?, ?, '')
            """,
            (job_id, run_kind, now, APP_BUILD_ID, ""),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("queued", now, job_id))
    return int(cur.lastrowid)


def _pid_is_running(pid: int) -> bool:
    try:
        normalized_pid = int(pid or 0)
    except (TypeError, ValueError):
        return False
    if normalized_pid <= 0:
        return False
    try:
        os.kill(normalized_pid, 0)
    except OSError:
        return False
    return True


def list_runs(job_id: int) -> list[dict[str, Any]]:
    return fetch_all("SELECT * FROM runs WHERE job_id = ? ORDER BY id DESC", (job_id,))


def get_run(run_id: int) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM runs WHERE id = ?", (run_id,))


def get_job_run(job_id: int, run_id: int) -> dict[str, Any] | None:
    return fetch_one("SELECT * FROM runs WHERE id = ? AND job_id = ?", (run_id, job_id))


def acquire_worker_lease(owner_token: str, worker_pid: int, app_build_id: str, ttl_seconds: int = WORKER_LEASE_TTL_SECONDS) -> bool:
    now = utc_now_iso()
    expires_at = utc_after_seconds_iso(ttl_seconds)
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM worker_leases WHERE lease_key = 'primary'").fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO worker_leases (lease_key, owner_token, worker_pid, app_build_id, acquired_at, heartbeat_at, expires_at)
                VALUES ('primary', ?, ?, ?, ?, ?, ?)
                """,
                (owner_token, worker_pid, app_build_id, now, now, expires_at),
            )
            conn.commit()
            return True
        lease_pid = int(row.get("worker_pid", 0) or 0)
        lease_stale = str(row.get("expires_at", "")) <= now or not _pid_is_running(lease_pid)
        if row.get("owner_token") == owner_token or lease_stale:
            conn.execute(
                """
                UPDATE worker_leases
                SET owner_token = ?, worker_pid = ?, app_build_id = ?, acquired_at = COALESCE(NULLIF(acquired_at, ''), ?), heartbeat_at = ?, expires_at = ?
                WHERE lease_key = 'primary'
                """,
                (owner_token, worker_pid, app_build_id, now, now, expires_at),
            )
            conn.commit()
            return True
        conn.commit()
        return False


def heartbeat_worker_lease(owner_token: str, ttl_seconds: int = WORKER_LEASE_TTL_SECONDS) -> None:
    now = utc_now_iso()
    expires_at = utc_after_seconds_iso(ttl_seconds)
    with connect() as conn:
        conn.execute(
            """
            UPDATE worker_leases
            SET heartbeat_at = ?, expires_at = ?
            WHERE lease_key = 'primary' AND owner_token = ?
            """,
            (now, expires_at, owner_token),
        )


def release_worker_lease(owner_token: str) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM worker_leases WHERE lease_key = 'primary' AND owner_token = ?", (owner_token,))


def claim_next_run(worker_pid: int = 0, app_build_id: str = "", worker_token: str = "") -> dict[str, Any] | None:
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM runs WHERE status = 'queued' ORDER BY id ASC LIMIT 1").fetchone()
        if not row:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE runs
            SET status = 'running',
                stage = 'starting',
                message = 'Worker claimed run',
                started_at = ?,
                parser_version = ?,
                worker_pid = ?,
                app_build_id = ?,
                worker_token = ?
            WHERE id = ?
            """,
            (utc_now_iso(), app_build_id or APP_BUILD_ID, worker_pid, app_build_id or APP_BUILD_ID, worker_token, row["id"]),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("running", utc_now_iso(), row["job_id"]))
        conn.commit()
        row["status"] = "running"
        row["stage"] = "starting"
        row["worker_pid"] = worker_pid
        row["app_build_id"] = app_build_id or APP_BUILD_ID
        row["worker_token"] = worker_token
        return row


def update_run_runtime_metadata(run_id: int, parser_strategy: str, worker_pid: int, app_build_id: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE runs
            SET parser_strategy = ?, worker_pid = ?, app_build_id = ?, parser_version = ?
            WHERE id = ?
            """,
            (parser_strategy, worker_pid, app_build_id, app_build_id, run_id),
        )


def update_run_progress(run_id: int, stage: str, message: str, worker_token: str = "") -> None:
    with connect() as conn:
        conn.execute("UPDATE runs SET stage = ?, message = ? WHERE id = ?", (stage, message, run_id))
    if worker_token:
        heartbeat_worker_lease(worker_token)


def mark_run_succeeded(run_id: int, job_id: int, result_payload: dict[str, Any], worker_token: str = "") -> None:
    now = utc_now_iso()
    with connect() as conn:
        conn.execute(
            """
            UPDATE runs
            SET status = 'succeeded', stage = 'done', message = 'Completed', finished_at = ?, result_json = ?, error_text = ''
            WHERE id = ?
            """,
            (now, json.dumps(result_payload, ensure_ascii=False), run_id),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("ready", now, job_id))
    if worker_token:
        heartbeat_worker_lease(worker_token)


def mark_run_failed(run_id: int, job_id: int, error_text: str, worker_token: str = "") -> None:
    now = utc_now_iso()
    with connect() as conn:
        conn.execute(
            """
            UPDATE runs
            SET status = 'failed', stage = 'failed', message = 'Failed', finished_at = ?, error_text = ?
            WHERE id = ?
            """,
            (now, error_text[:4000], run_id),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("failed", now, job_id))
    if worker_token:
        heartbeat_worker_lease(worker_token)


def upsert_snapshot(job_id: int, snapshot_kind: str, data: dict[str, Any]) -> None:
    now = utc_now_iso()
    payload = json.dumps(data, ensure_ascii=False)
    with connect() as conn:
        existing = conn.execute(
            "SELECT id FROM snapshots WHERE job_id = ? AND snapshot_kind = ?",
            (job_id, snapshot_kind),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE snapshots SET data_json = ?, updated_at = ? WHERE id = ?",
                (payload, now, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO snapshots (job_id, snapshot_kind, data_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, snapshot_kind, payload, now, now),
            )
            snapshot_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        if existing:
            snapshot_id = int(existing["id"])
        if snapshot_kind == "raw_spec" and str(data.get("source_kind", "") or "spec").lower() == "spec":
            _upsert_snapshot_verification_locked(conn, snapshot_id, snapshot_kind, data)


def get_snapshot(job_id: int, snapshot_kind: str) -> dict[str, Any] | None:
    row = fetch_one("SELECT * FROM snapshots WHERE job_id = ? AND snapshot_kind = ?", (job_id, snapshot_kind))
    if not row:
        return None
    row["data"] = json.loads(row["data_json"])
    return row


def upsert_review(job_id: int, data: dict[str, Any]) -> None:
    payload = json.dumps(data, ensure_ascii=False)
    now = utc_now_iso()
    with connect() as conn:
        existing = conn.execute("SELECT id FROM reviews WHERE job_id = ?", (job_id,)).fetchone()
        if existing:
            conn.execute("UPDATE reviews SET data_json = ?, updated_at = ? WHERE id = ?", (payload, now, existing["id"]))
        else:
            conn.execute("INSERT INTO reviews (job_id, data_json, updated_at) VALUES (?, ?, ?)", (job_id, payload, now))


def get_review(job_id: int) -> dict[str, Any] | None:
    row = fetch_one("SELECT * FROM reviews WHERE job_id = ?", (job_id,))
    if not row:
        return None
    row["data"] = json.loads(row["data_json"])
    return row


def get_snapshot_verification(snapshot_id: int) -> dict[str, Any] | None:
    row = fetch_one("SELECT * FROM snapshot_verifications WHERE snapshot_id = ?", (snapshot_id,))
    if not row:
        return None
    row["checklist"] = _load_checklist_json(row.get("checklist_json", "[]"))
    row["status"] = _normalize_verification_status(row.get("status", "pending"))
    return row


def get_job_snapshot_verification(job_id: int, snapshot_kind: str = "raw_spec") -> dict[str, Any] | None:
    row = fetch_one(
        """
        SELECT sv.*, s.job_id, s.snapshot_kind, s.id AS joined_snapshot_id
        FROM snapshot_verifications sv
        JOIN snapshots s ON s.id = sv.snapshot_id
        WHERE s.job_id = ? AND s.snapshot_kind = ?
        """,
        (job_id, snapshot_kind),
    )
    if not row:
        return None
    row["snapshot_id"] = int(row.get("joined_snapshot_id", row.get("snapshot_id", 0)) or 0)
    row["checklist"] = _load_checklist_json(row.get("checklist_json", "[]"))
    row["status"] = _normalize_verification_status(row.get("status", "pending"))
    return row


def save_snapshot_verification(
    snapshot_id: int,
    checklist: list[dict[str, Any]],
    checked_by: str,
    notes: str = "",
    force_status: str | None = None,
) -> dict[str, Any] | None:
    now = utc_now_iso()
    normalized_checklist = _normalize_verification_checklist(checklist)
    status = _normalize_verification_status(force_status or _verification_status_from_checklist(normalized_checklist))
    with connect() as conn:
        existing = conn.execute(
            "SELECT id, created_at FROM snapshot_verifications WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        payload = json.dumps(normalized_checklist, ensure_ascii=False)
        checked_at = now if checked_by else ""
        if existing:
            conn.execute(
                """
                UPDATE snapshot_verifications
                SET status = ?, checked_by = ?, checked_at = ?, notes = ?, checklist_json = ?, updated_at = ?
                WHERE snapshot_id = ?
                """,
                (status, checked_by, checked_at, notes, payload, now, snapshot_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO snapshot_verifications (
                    snapshot_id, snapshot_kind, status, checked_by, checked_at, notes, checklist_json, created_at, updated_at
                )
                VALUES (?, '', ?, ?, ?, ?, ?, ?, ?)
                """,
                (snapshot_id, status, checked_by, checked_at, notes, payload, now, now),
            )
    return get_snapshot_verification(snapshot_id)


def is_job_snapshot_verification_passed(job_id: int, snapshot_kind: str = "raw_spec") -> bool:
    verification = get_job_snapshot_verification(job_id, snapshot_kind)
    return bool(verification and verification.get("status") == "passed")


def insert_auth_event(username: str, action: str, detail: str = "") -> None:
    with connect() as conn:
        conn.execute(
            "INSERT INTO auth_events (username, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (username, action, detail, utc_now_iso()),
        )


def _upsert_snapshot_verification_locked(
    conn: sqlite3.Connection,
    snapshot_id: int,
    snapshot_kind: str,
    snapshot_data: dict[str, Any],
) -> None:
    now = utc_now_iso()
    checklist = _build_snapshot_verification_checklist(snapshot_data)
    payload = json.dumps(checklist, ensure_ascii=False)
    existing = conn.execute(
        "SELECT id, created_at FROM snapshot_verifications WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE snapshot_verifications
            SET snapshot_kind = ?, status = 'pending', checked_by = '', checked_at = '', notes = '', checklist_json = ?, updated_at = ?
            WHERE snapshot_id = ?
            """,
            (snapshot_kind, payload, now, snapshot_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO snapshot_verifications (
                snapshot_id, snapshot_kind, status, checked_by, checked_at, notes, checklist_json, created_at, updated_at
            )
            VALUES (?, ?, 'pending', '', '', '', ?, ?, ?)
            """,
            (snapshot_id, snapshot_kind, payload, now, now),
        )


def _build_snapshot_verification_checklist(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    if parsing._is_imperial_builder(str(snapshot.get("builder_name", "") or "")):
        return _build_imperial_snapshot_verification_checklist(snapshot)
    checklist: list[dict[str, Any]] = []
    rooms = snapshot.get("rooms", [])
    for room in rooms if isinstance(rooms, list) else []:
        if not isinstance(room, dict):
            continue
        room_label = _verification_room_label(room)
        page_refs = _verification_text(room.get("page_refs", ""))
        _append_verification_item(
            checklist,
            section_type="room",
            entity_label=room_label,
            field_name="room_title",
            extracted_value=room_label,
            source_page_refs=page_refs,
        )
        for field_name, source_key in (
            ("bench_tops_wall_run", "bench_tops_wall_run"),
            ("bench_tops_island", "bench_tops_island"),
            ("bench_tops_other", "bench_tops_other"),
            ("door_colours_overheads", "door_colours_overheads"),
            ("door_colours_base", "door_colours_base"),
            ("door_colours_tall", "door_colours_tall"),
            ("door_colours_island", "door_colours_island"),
            ("door_colours_bar_back", "door_colours_bar_back"),
            ("feature_colour", "feature_colour"),
            ("toe_kick", "toe_kick"),
            ("bulkheads", "bulkheads"),
            ("handles", "handles"),
            ("floating_shelf", "floating_shelf"),
            ("shelf", "shelf"),
            ("led", "led"),
            ("led_note", "led_note"),
            ("accessories", "accessories"),
            ("others", "other_items"),
            ("sink", "sink_info"),
            ("basin", "basin_info"),
            ("tap", "tap_info"),
            ("drawers", "drawers_soft_close"),
            ("hinges", "hinges_soft_close"),
            ("splashback", "splashback"),
            ("flooring", "flooring"),
        ):
            _append_verification_item(
                checklist,
                section_type="room",
                entity_label=room_label,
                field_name=field_name,
                extracted_value=_verification_text(room.get(source_key, "")),
                source_page_refs=page_refs,
            )
    appliances = snapshot.get("appliances", [])
    for appliance in appliances if isinstance(appliances, list) else []:
        if not isinstance(appliance, dict):
            continue
        entity_label = _verification_text(appliance.get("appliance_type", "")) or "Appliance"
        extracted_value = " | ".join(
            value
            for value in (
                _verification_text(appliance.get("make", "")),
                _verification_text(appliance.get("model_no", "")),
                _verification_text(appliance.get("overall_size", "")),
            )
            if value
        )
        _append_verification_item(
            checklist,
            section_type="appliance",
            entity_label=entity_label,
            field_name="appliance",
            extracted_value=extracted_value,
            source_page_refs=_verification_text(appliance.get("page_refs", "")),
        )
    special_sections = snapshot.get("special_sections", [])
    for section in special_sections if isinstance(special_sections, list) else []:
        if not isinstance(section, dict):
            continue
        entity_label = _verification_text(section.get("original_section_label", "")) or _verification_text(section.get("section_key", "")) or "Special Section"
        _append_verification_item(
            checklist,
            section_type="special",
            entity_label=entity_label,
            field_name="fields",
            extracted_value=_verification_text(section.get("fields", "")),
            source_page_refs=_verification_text(section.get("page_refs", "")),
        )
    return checklist


def _build_imperial_snapshot_verification_checklist(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    checklist: list[dict[str, Any]] = []
    rooms = snapshot.get("rooms", [])
    ordered_rooms = sorted(
        [room for room in rooms if isinstance(room, dict)],
        key=lambda room: (int(room.get("room_order", 0) or 0), _verification_room_label(room)),
    )
    summary_entries: dict[str, dict[str, dict[str, Any]]] = {
        "door_colours": {},
        "handles": {},
        "bench_tops": {},
    }
    for room in ordered_rooms:
        room_label = _verification_room_label(room)
        room_page_refs = _verification_text(room.get("page_refs", ""))
        _append_verification_item(
            checklist,
            section_type="room",
            entity_label=room_label,
            field_name="room_title",
            extracted_value=room_label,
            source_page_refs=room_page_refs,
        )
        material_rows = room.get("material_rows", [])
        if isinstance(material_rows, list):
            def _sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
                provenance = item.get("provenance", {})
                source_row_index = 0
                if isinstance(provenance, dict):
                    source_row_index = int(provenance.get("source_row_index", 0) or 0)
                return (
                    int(item.get("page_no", 0) or 0),
                    source_row_index or int(item.get("row_order", 0) or 0),
                    _verification_text(item.get("area_or_item", "")),
                )
            ordered_rows = sorted(
                [item for item in material_rows if isinstance(item, dict)],
                key=_sort_key,
            )
            for item in ordered_rows:
                area_or_item = _verification_text(item.get("area_or_item", ""))
                if not area_or_item:
                    continue
                provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
                absorbed_handle_texts = provenance.get("absorbed_inline_handle_texts", [])
                if isinstance(absorbed_handle_texts, list):
                    for raw_handle_text in absorbed_handle_texts:
                        for summary_text in _verification_summary_values_for_bucket(
                            "handles",
                            _verification_text(raw_handle_text),
                        ):
                            entry = _verification_find_imperial_summary_entry(
                                "handles",
                                list(summary_entries["handles"].values()),
                                summary_text,
                            )
                            if entry is None:
                                entry = {"text": summary_text, "rooms": []}
                                summary_entries["handles"][summary_text.lower()] = entry
                            if room_label and room_label not in entry["rooms"]:
                                entry["rooms"].append(room_label)
                supplier = _verification_text(item.get("supplier", ""))
                description = _verification_text(item.get("specs_or_description", ""))
                notes = _verification_text(item.get("notes", ""))
                raw_summary_value = " - ".join(part for part in (supplier, description, notes) if part)
                display_lines = [
                    _verification_text(line)
                    for line in parsing._imperial_material_row_display_lines_for_view(item)
                    if _verification_text(line)
                ]
                extracted_value = _verification_text(" | ".join(display_lines))
                if not extracted_value:
                    extracted_value = _verification_text(parsing._imperial_material_row_display_value_for_view(item))
                if not extracted_value and _imperial_verification_row_is_handle_summary_review_fallback(item):
                    fallback_sources = _verification_handle_summary_fallback_sources(item)
                    if fallback_sources:
                        extracted_value = fallback_sources[0]
                if not extracted_value:
                    extracted_value = raw_summary_value
                if not extracted_value:
                    continue
                tags = [str(tag).strip() for tag in (item.get("tags", []) or []) if str(tag).strip()]
                primary_tag = _imperial_verification_summary_bucket_key_for_row(
                    area_or_item=area_or_item,
                    tags=tags,
                )
                if primary_tag not in summary_entries:
                    continue
                if not _imperial_verification_row_is_summary_worthy(
                    area_or_item=area_or_item,
                    extracted_value=extracted_value,
                    tags=tags,
                    needs_review=_imperial_verification_effective_needs_review(item),
                    item=item,
                ):
                    continue
                field_name = f"{primary_tag}: {area_or_item}"
                source_page_refs = _verification_text(item.get("page_no", "")) or room_page_refs
                _append_verification_item(
                    checklist,
                    section_type="room",
                    entity_label=room_label,
                    field_name=field_name,
                    extracted_value=extracted_value,
                    source_page_refs=source_page_refs,
                )
                summary_source_values: list[str] = []
                for source_text in display_lines:
                    if source_text and source_text not in summary_source_values:
                        summary_source_values.append(source_text)
                for source_text in (extracted_value, raw_summary_value):
                    if source_text and source_text not in summary_source_values:
                        summary_source_values.append(source_text)
                if primary_tag == "handles" and extracted_value:
                    used_handle_fallback = not bool(display_lines) and not bool(
                        _verification_text(parsing._imperial_material_row_display_value_for_view(item))
                    )
                    if used_handle_fallback and _imperial_verification_row_is_handle_summary_review_fallback(item):
                        for fallback_source in _verification_handle_summary_fallback_sources(item):
                            if fallback_source and fallback_source not in summary_source_values:
                                summary_source_values.append(fallback_source)
                for summary_source in summary_source_values:
                    for summary_text in _verification_summary_values_for_bucket(
                        primary_tag,
                        summary_source,
                        supplier=supplier,
                    ):
                        entry = _verification_find_imperial_summary_entry(
                            primary_tag,
                            list(summary_entries[primary_tag].values()),
                            summary_text,
                        )
                        if entry is None:
                            entry = {"text": summary_text, "rooms": []}
                            summary_entries[primary_tag][summary_text.lower()] = entry
                        elif _verification_imperial_summary_value_quality(
                            primary_tag,
                            summary_text,
                        ) > _verification_imperial_summary_value_quality(
                            primary_tag,
                            entry.get("text", ""),
                        ):
                            old_key = next(
                                (
                                    key
                                    for key, value in summary_entries[primary_tag].items()
                                    if value is entry
                                ),
                                "",
                            )
                            if old_key:
                                summary_entries[primary_tag].pop(old_key, None)
                            entry["text"] = summary_text
                            summary_entries[primary_tag][summary_text.lower()] = entry
                        if room_label and room_label not in entry["rooms"]:
                            entry["rooms"].append(room_label)
        for field_name, source_key in (
            ("drawers", "drawers_soft_close"),
            ("hinges", "hinges_soft_close"),
            ("flooring", "flooring"),
            ("sink", "sink_info"),
        ):
            extracted_value = _verification_text(room.get(source_key, ""))
            if field_name == "sink" and not extracted_value:
                extracted_value = _verification_text(room.get("basin_info", ""))
            _append_verification_item(
                checklist,
                section_type="room",
                entity_label=room_label,
                field_name=field_name,
                extracted_value=extracted_value,
                source_page_refs=room_page_refs,
            )
    live_summary = _verification_live_imperial_material_summary(snapshot)
    for bucket_key, label in (
        ("door_colours", "Door Colours"),
        ("handles", "Handles"),
        ("bench_tops", "Bench Tops"),
    ):
        live_bucket = live_summary.get(bucket_key, {}) if isinstance(live_summary, dict) else {}
        live_entries = live_bucket.get("entries", []) if isinstance(live_bucket, dict) else []
        extracted_value = " | ".join(
            f"{_verification_text(entry.get('display_text') or entry.get('text'))} (Room: {_verification_text(entry.get('rooms_display')) or '-'})"
            for entry in live_entries
            if _verification_text(entry.get("display_text") or entry.get("text"))
        )
        if not extracted_value:
            extracted_value = " | ".join(
                f"{entry['text']} (Room: {' | '.join(entry['rooms']) or '-'})"
                for entry in summary_entries[bucket_key].values()
            )
        _append_verification_item(
            checklist,
            section_type="summary",
            entity_label="Material Summary",
            field_name=label,
            extracted_value=extracted_value,
            source_page_refs="",
        )
    return checklist


def _verification_live_imperial_material_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    try:
        from App import main as app_main

        summary = app_main._build_imperial_material_summary(snapshot)
        if isinstance(summary, dict):
            return summary
    except Exception:
        return {}
    return {}


def _imperial_verification_summary_bucket_key(tags: list[str]) -> str:
    normalized = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    if "door_colours" in normalized:
        return "door_colours"
    if "handles" in normalized:
        return "handles"
    if "bench_tops" in normalized:
        return "bench_tops"
    return ""


def _imperial_verification_summary_bucket_key_for_row(
    *,
    area_or_item: str,
    tags: list[str],
) -> str:
    bucket_key = _imperial_verification_summary_bucket_key(tags)
    if bucket_key:
        return bucket_key
    title = _verification_text(area_or_item)
    title_upper = title.upper()
    if "BENCHTOP" in title_upper:
        return "bench_tops"
    if re.search(r"(?i)\b(?:handles?|knob)\b", title):
        return "handles"
    if re.search(r"(?i)\b(?:colour|frame|bar back|panel)\b", title):
        return "door_colours"
    return ""


def _imperial_verification_row_is_summary_worthy(
    *,
    area_or_item: str,
    extracted_value: str,
    tags: list[str],
    needs_review: bool,
    item: dict[str, Any] | None = None,
) -> bool:
    if needs_review:
        if _imperial_verification_row_is_only_row_order_review(item or {}):
            pass
        elif _imperial_verification_row_is_handle_summary_review_fallback(item or {}):
            pass
        else:
            return False
    bucket_key = _imperial_verification_summary_bucket_key_for_row(area_or_item=area_or_item, tags=tags)
    if not bucket_key:
        return False
    title = _verification_text(area_or_item)
    value = _verification_text(extracted_value)
    if bucket_key == "handles" and not value and _imperial_verification_row_is_handle_summary_review_fallback(item or {}):
        fallback_sources = _verification_handle_summary_fallback_sources(item or {})
        if fallback_sources:
            value = " | ".join(fallback_sources)
    normalized_value = _verification_normalize_summary_value(bucket_key, value)
    if not title or not value:
        return False
    if not normalized_value or re.match(r"(?i)^(?:incl|include|open|split|allow|note)\b", normalized_value):
        return False
    if bucket_key == "bench_tops":
        if re.search(r"(?i)\bunder\s+dryer\b", title):
            return False
        return "BENCHTOP" in title.upper()
    if bucket_key == "handles":
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b|casters?)\b", value):
            return False
        if re.search(r"(?i)\b(?:handles?|knob)\b", title):
            return True
        return bool(
            re.search(r"(?i)\b(?:desk|drawers?|drawer|pedestal|benchseat)\b", title)
            and re.search(
                r"(?i)\b(?:handle|knob|profile handle|voda|bevel edge|push to open|finger pull|no handles?|momo|tekform|hinoki|so-[a-z0-9-]+)\b",
                value,
            )
        )
    if bucket_key == "door_colours":
        if not re.search(r"(?i)\b(?:colour|frame|bar back|panel)\b", title):
            return False
        if re.search(r"(?i)\b(?:internals?|kickboards?|open shelving|drawers?\b)\b", title):
            return False
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b|casters?|handle|fingerpull|knob)\b", value):
            return False
        return True
    return False


def _imperial_verification_effective_needs_review(item: dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    if bool(item.get("needs_review", False)):
        return True
    revalidation_status = _verification_text(item.get("revalidation_status", "")).lower()
    return revalidation_status in {"needs_review", "failed", "pending"}


def _imperial_verification_accepted_issue_types(item: dict[str, Any]) -> set[str]:
    if not isinstance(item, dict):
        return set()
    accepted_issue_types: set[str] = set()
    for verdict in item.get("repair_verdicts", []) or []:
        if not isinstance(verdict, dict):
            continue
        if _verification_text(verdict.get("status", "")).lower() != "accepted":
            continue
        revalidation_status = _verification_text(verdict.get("revalidation_status", "")).lower()
        if revalidation_status and revalidation_status != "passed":
            continue
        issue_type = _verification_text(verdict.get("issue_type", "")).lower()
        if issue_type:
            accepted_issue_types.add(issue_type)
    return accepted_issue_types


def _imperial_verification_review_issue_types(item: dict[str, Any]) -> set[str]:
    if not isinstance(item, dict):
        return set()
    issue_types: set[str] = set()
    for issue in item.get("issues", []) or []:
        if isinstance(issue, dict):
            issue_type = _verification_text(issue.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    for verdict in item.get("repair_verdicts", []) or []:
        if isinstance(verdict, dict) and _verification_text(verdict.get("status", "")).lower() in {"needs_review", "pending"}:
            issue_type = _verification_text(verdict.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    for issue in item.get("revalidation_issues", []) or []:
        if isinstance(issue, dict):
            issue_type = _verification_text(issue.get("related_issue_type", "") or issue.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    issue_types -= _imperial_verification_accepted_issue_types(item)
    return issue_types


def _imperial_verification_row_is_only_row_order_review(item: dict[str, Any]) -> bool:
    issue_types = _imperial_verification_review_issue_types(item)
    return bool(issue_types) and issue_types <= {"row_order_drift"}


def _imperial_verification_row_is_handle_summary_review_fallback(item: dict[str, Any]) -> bool:
    issue_types = _imperial_verification_review_issue_types(item)
    return bool(issue_types) and issue_types <= {
        "row_order_drift",
        "handle_block_over_split",
        "cross_row_spillover",
        "supplier_notes_misassignment",
    }


def _verification_handle_summary_fallback_sources(item: dict[str, Any]) -> list[str]:
    if not isinstance(item, dict):
        return []
    provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
    sources: list[str] = []
    for key in ("layout_value_text", "page_text_handle_block"):
        text = _verification_text(provenance.get(key, ""))
        if text and text not in sources:
            sources.append(text)
    visual_fragments = provenance.get("visual_fragments", [])
    if isinstance(visual_fragments, list):
        fragment_lines: list[str] = []
        for fragment in visual_fragments:
            if not isinstance(fragment, dict):
                continue
            fragment_line = parsing._compose_supplier_description_note(
                _verification_text(fragment.get("supplier", "")),
                _verification_text(fragment.get("specs_or_description", "")),
                _verification_text(fragment.get("notes", "")),
            )
            fragment_line = _verification_text(fragment_line)
            if fragment_line:
                fragment_lines.append(fragment_line)
        if fragment_lines:
            combined = " | ".join(fragment_lines)
            if combined not in sources:
                sources.append(combined)
    return sources


def _verification_imperial_summary_token_set(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[A-Za-z0-9]+", _verification_text(text).upper())
        if len(token) > 1
        and token not in {
            "THE",
            "AND",
            "FOR",
            "WITH",
            "ONLY",
            "NOTE",
            "TO",
            "OF",
            "AS",
            "PER",
            "BY",
            "AT",
            "ON",
            "IN",
            "MM",
            "NO",
            "ROOM",
        }
    }


def _verification_imperial_summary_anchor_token_set(bucket_key: str, text: str) -> set[str]:
    generic_tokens = {
        "POLYTEC",
        "LAMINEX",
        "CAESARSTONE",
        "CDK",
        "STONE",
        "WOODMATT",
        "MATT",
        "MATTE",
        "NATURAL",
        "THERMOLAMINATED",
        "LAMINATED",
        "VERTICAL",
        "HORIZONTAL",
        "GRAIN",
        "COLOUR",
        "COLOR",
        "IMAGE",
    }
    if bucket_key == "handles":
        generic_tokens |= {
            "HANDLE",
            "HANDLES",
            "DOOR",
            "DOORS",
            "DRAWER",
            "DRAWERS",
            "KNOB",
            "PULL",
        }
    return {
        token
        for token in _verification_imperial_summary_token_set(text)
        if token not in generic_tokens
    }


def _verification_imperial_summary_overlap_ratio(left: str, right: str) -> float:
    left_tokens = _verification_imperial_summary_token_set(left)
    right_tokens = _verification_imperial_summary_token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    shared = left_tokens & right_tokens
    if not shared:
        return 0.0
    return float(len(shared)) / float(min(len(left_tokens), len(right_tokens)))


def _verification_imperial_handle_summary_identity_tokens(text: str) -> set[str]:
    normalized = _verification_text(text)
    if not normalized:
        return set()
    tokens = {
        parsing.normalize_space(match.group(0)).upper()
        for match in re.finditer(
            r"(?i)\b(?:S\d+\.\d+\.[A-Z]+|HT\d+\s*-\s*\d+\s*-\s*[A-Z]+|SO-\d+-[A-Z0-9-]+|\d{3,5}-[A-Z0-9]+)\b",
            normalized,
        )
    }
    if re.search(r"(?i)\bknob\b", normalized):
        tokens.add("KNOB")
    if re.search(r"(?i)\bvoda\s+profile\s+handle\b", normalized):
        tokens.add("VODA")
    if re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", normalized):
        tokens.add("BEVEL_EDGE_FINGER_PULL")
    if re.search(r"(?i)\bpush\s+to\s+open\b|\bPTO\b", normalized):
        tokens.add("PTO")
    return tokens


def _verification_imperial_summary_value_quality(bucket_key: str, text: str) -> float:
    cleaned = _verification_text(text)
    quality = float(len(_verification_imperial_summary_token_set(cleaned)))
    quality += 2.0 if " - " in cleaned else 0.0
    if bucket_key == "handles":
        if re.search(r"(?i)\b(?:investigating|pricing from|for original selection)\b", cleaned):
            quality -= 4.0
        if cleaned.count("|") >= 1:
            quality -= float(cleaned.count("|")) * 3.0
        if re.search(r"(?i)\bimage\b", cleaned):
            quality -= 3.0
        if re.search(r"(?i)\bno\s+handles?\s+on\s+uppers?\b", cleaned):
            quality -= 6.0
        if re.search(r"(?i)\btalls?\b", cleaned):
            quality -= 4.0
        if re.search(r"(?i)\blowers?\b", cleaned):
            quality -= 3.0
    if bucket_key == "door_colours":
        if re.search(r"(?i)\b(?:handle|knob|push to open|finger pull|lighting|led)\b", cleaned):
            quality -= 5.0
        if re.search(r"(?i)\bframed?\s+sliding\s+doors?\b", cleaned):
            quality -= 4.0
    if bucket_key == "bench_tops":
        if re.search(r"(?i)\b(?:floating shelf|internal steel support)\b", cleaned):
            quality -= 4.0
    return quality


def _verification_imperial_summary_values_equivalent(bucket_key: str, left: str, right: str) -> bool:
    left_text = _verification_text(left)
    right_text = _verification_text(right)
    if not left_text or not right_text:
        return False
    if left_text.lower() == right_text.lower():
        return True
    if bucket_key == "handles":
        left_identity = _verification_imperial_handle_summary_identity_tokens(left_text)
        right_identity = _verification_imperial_handle_summary_identity_tokens(right_text)
        if left_identity and right_identity and left_identity != right_identity:
            return False
    left_tokens = _verification_imperial_summary_anchor_token_set(bucket_key, left_text)
    right_tokens = _verification_imperial_summary_anchor_token_set(bucket_key, right_text)
    if min(len(left_tokens), len(right_tokens)) < 2:
        return False
    overlap = float(len(left_tokens & right_tokens)) / float(min(len(left_tokens), len(right_tokens)))
    if bucket_key == "handles":
        return overlap >= 0.9
    if bucket_key in {"door_colours", "bench_tops"}:
        return overlap >= 0.72
    return False


def _verification_find_imperial_summary_entry(
    bucket_key: str,
    entries: list[dict[str, Any]],
    candidate_text: str,
) -> dict[str, Any] | None:
    candidate = _verification_text(candidate_text)
    if not candidate:
        return None
    candidate_key = candidate.lower()
    for entry in entries:
        existing_text = _verification_text(entry.get("text", ""))
        if not existing_text:
            continue
        if existing_text.lower() == candidate_key:
            return entry
        if _verification_imperial_summary_values_equivalent(bucket_key, existing_text, candidate):
            return entry
    return None


def _append_verification_item(
    checklist: list[dict[str, Any]],
    section_type: str,
    entity_label: str,
    field_name: str,
    extracted_value: str,
    source_page_refs: str,
) -> None:
    checklist.append(
        {
            "section_type": section_type,
            "entity_label": entity_label,
            "field_name": field_name,
            "extracted_value": extracted_value,
            "source_page_refs": source_page_refs,
            "pdf_page_ref": "",
            "status": "pending",
            "qa_note": "",
        }
    )


def _verification_room_label(room: dict[str, Any]) -> str:
    return _verification_text(room.get("original_room_label", "")) or _verification_text(room.get("room_key", "")) or "Room"


def _verification_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple, set)):
        parts = [_verification_text(item) for item in value]
        return " | ".join(part for part in parts if part)
    if isinstance(value, dict):
        if "label" in value and "value" in value:
            label = _verification_text(value.get("label", ""))
            item_value = _verification_text(value.get("value", ""))
            return f"{label}: {item_value}".strip(": ")
        parts = []
        for key, item in value.items():
            key_text = _verification_text(key)
            item_text = _verification_text(item)
            if key_text and item_text:
                parts.append(f"{key_text}: {item_text}")
            elif item_text:
                parts.append(item_text)
        return " | ".join(parts)
    return str(value).strip()


def _verification_normalize_summary_value(bucket_key: str, value: str) -> str:
    text = parsing.normalize_space(value)
    if bucket_key == "door_colours":
        text = re.sub(r"(?i)\bCOLOURED?\b", "", text)
        text = re.sub(r"(?i)\bREFER TO DRAWINGS(?: FOR ALLOCATIONS)?\b.*$", "", text)
        text = re.sub(r"(?i)\bBLUM\s+AVENTOS\b.*$", "", text)
        text = re.sub(r"(?i)\bframed?\s+sliding\s+doors?\b\s*-?\s*", "", text)
        text = re.sub(r"(?i)\bNOTE:\s*.*$", "", text)
        text = re.sub(r"(?i)\b(?:COLOURED\s+)?BOTTOMS TO OVERHEADS\b.*$", "", text)
        text = re.sub(r"(?i)\bVERTICAL\s*-\s*GRAIN\b", "Vertical Grain", text)
        text = re.sub(r"(?i)\bHORIZONTAL\s*-\s*GRAIN\b", "Horizontal Grain", text)
        text = re.sub(r"\([^)]*(upper|overhead|base|island|bar back|cabinet|panel|run|shelf)[^)]*\)", "", text, flags=re.IGNORECASE)
        return _verification_strip_summary_tail(
            text,
            (
                r"(?i)\b(?:plain glass\s+)?display cabinet\b.*$",
                r"(?i)\bto tall open shelves\b.*$",
                r"(?i)\b(?:to|for)\b[^|;]*\b(upper|overhead|base|island|bar back|cabinetry|run|shelf|shelves)\b.*$",
            ),
        )
    if bucket_key == "handles":
        return _verification_normalize_imperial_handle_summary_value(text)
    if bucket_key == "bench_tops":
        text = re.sub(r"(?i)^back benchtops?\s*", "", text)
        text = re.sub(r"(?i)^wall run bench top\s*", "", text)
        text = re.sub(r"(?i)^island bench top\s*", "", text)
        text = re.sub(r"(?i)^island benchtop\s*", "", text)
        text = re.sub(r"(?i)\b(?:um\s*sink|undermount\s+sink)\b.*$", "", text)
        text = re.sub(r"(?i)\b(?:stone\s+)?splashback\b.*$", "", text)
        text = re.sub(r"(?i)\bNOTE:\s*.*$", "", text)
        text = re.sub(r"(?i)\bIncl\.\s*Spring\s+Free\s+Upgrade\s+Promotion\b.*$", "", text)
        text = re.sub(r"(?i)\bFree\s+Upgrade\s+Promotion\b.*$", "", text)
        text = re.sub(
            r"(?i)\s*-\s*to\s+(?:the\s+)?(?:cooktop run|wall run|wall bench|wall side|island bench|island|powder room\s*\d*|powder\s+room|ensuite\s*\d*|ensuite|main bathroom|bathroom|laundry|vanities?|butler'?s pantry|pantry)\b.*$",
            "",
            text,
        )
        text = re.sub(
            r"(?i)\s+\bto\s+(?:the\s+)?(?:cooktop run|wall run|wall bench|wall side|island bench|island|powder room\s*\d*|powder\s+room|ensuite\s*\d*|ensuite|main bathroom|bathroom|laundry|vanities?|butler'?s pantry|pantry)\b.*$",
            "",
            text,
        )
    return text.strip(" -;,/")


def _verification_imperial_summary_material_supplier(
    bucket_key: str,
    source_text: str,
    explicit_supplier: str,
    *,
    material_kind: str = "",
) -> str:
    explicit = parsing.normalize_brand_casing_text(parsing.normalize_space(explicit_supplier))
    upper = parsing.normalize_space(source_text).upper()
    if material_kind == "colour_code" and "LAMINEX" in upper:
        return "Laminex"
    if material_kind == "woodmatt" and "POLYTEC" in upper:
        return "Polytec"
    if "LAMINEX" in upper and "POLYTEC" not in upper:
        return "Laminex"
    if "POLYTEC" in upper and "LAMINEX" not in upper:
        return "Polytec"
    if explicit:
        return explicit
    if bucket_key == "door_colours" and "THERMOLAMINATED" in upper:
        return "Polytec"
    return ""


def _verification_imperial_summary_material_candidates(
    bucket_key: str,
    raw_value: str,
    supplier: str,
) -> list[str]:
    text = parsing.normalize_space(raw_value)
    if not text or bucket_key not in {"door_colours", "bench_tops"}:
        return []
    candidates: list[str] = []
    colour_code_patterns = (
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*Natural\s*-\s*Colour Code:\s*(\d{2,4})\b",
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s+Natural Colour Code:\s*(\d{2,4})\b",
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*Colour Code:\s*(\d{2,4})\b",
    )
    for pattern in colour_code_patterns:
        for match in re.finditer(pattern, text):
            material = parsing.normalize_space(match.group(1))
            code = parsing.normalize_space(match.group(2))
            if not material or not code:
                continue
            if material.upper() in {"COLOUR", "CODE", "NATURAL"}:
                continue
            supplier_hint = _verification_imperial_summary_material_supplier(
                bucket_key,
                text,
                supplier,
                material_kind="colour_code",
            )
            composed = f"{supplier_hint + ' - ' if supplier_hint else ''}{material} Natural Colour Code: {code}"
            normalized = _verification_normalize_summary_value(bucket_key, composed)
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    woodmatt_pattern = re.compile(
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*(Woodmatt|Matt)(?:\s*-\s*(Vertical Grain|Horizontal Grain))?\b"
    )
    for match in woodmatt_pattern.finditer(text):
        material = parsing.normalize_space(match.group(1))
        finish = parsing.normalize_space(match.group(2))
        grain = parsing.normalize_space(match.group(3) or "")
        if not material or material.upper() in {"STYLE", "PROFILE", "NATURAL"}:
            continue
        material = re.sub(
            r"(?i)\b(?:\d+\s*mm\s+)?(?:laminate(?:d)?(?:\s+apron)?\s+benchtop|laminate(?:d)?\s+with\s+bullnose\s+edge(?:\s+profile)?)\b\s*-\s*",
            "",
            material,
        ).strip(" -;,/")
        material = parsing.normalize_space(material)
        if not material:
            continue
        supplier_hint = _verification_imperial_summary_material_supplier(
            bucket_key,
            text,
            supplier,
            material_kind="woodmatt",
        )
        composed_parts = [part for part in (supplier_hint, material, finish, grain) if part]
        composed = " - ".join(composed_parts)
        normalized = _verification_normalize_summary_value(bucket_key, composed)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _verification_summary_values_for_bucket(bucket_key: str, raw_value: str, *, supplier: str = "") -> list[str]:
    values: list[str] = []
    candidates = [raw_value]
    handle_text = parsing.normalize_space(raw_value) if bucket_key == "handles" else ""
    if bucket_key == "handles":
        semantic_candidates = _verification_semantic_imperial_handle_summary_candidates(handle_text)
        if semantic_candidates:
            candidates = semantic_candidates
    elif bucket_key in {"door_colours", "bench_tops"}:
        material_candidates = _verification_imperial_summary_material_candidates(bucket_key, raw_value, supplier)
        if material_candidates:
            candidates = material_candidates
    for candidate in candidates:
        normalized_candidate = _verification_normalize_summary_value(bucket_key, candidate)
        normalized = _verification_text(
            normalized_candidate if normalized_candidate else ("" if bucket_key == "handles" else candidate)
        )
        if not normalized or normalized in values:
            continue
        if bucket_key == "handles" and re.match(r"(?i)^(?:pto|drawers?|benchseat|casters?)$", normalized):
            continue
        values.append(normalized)
    if bucket_key == "handles" and len(values) > 1:
        if re.search(
            r"(?i)\b(?:desk\b|voda\s+profile\s+handle|high\s+split\s+handle|benchseat\s+drawers?\s*-\s*pto|so-[a-z0-9-]+)\b",
            handle_text,
        ):
            values = [value for value in values if value != "Bevel Edge finger pull"] or values
        values = [value for value in values if value not in {"No handles", "Push to open"}] or values
    return values


def _verification_normalize_handle_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    text = re.sub(r"(?i)\b([A-Z][A-Z0-9&]+)\s+\1\b", r"\1", text)
    text = re.sub(r"(?i)\bimage of\b.*$", "", text)
    text = re.sub(r"(?i)\s*-\s*[^-|()]+\s*\(image\)\s*$", "", text)
    text = re.sub(r"(?i)\s*\(image\)\s*$", "", text)
    text = re.sub(r"(?i)\bas per drawings\b.*$", "", text)
    text = re.sub(r"(?i)\bPTO\s+(?:where\s+required|where\s+req|req(?:uired)?)\b.*$", "", text)
    text = re.sub(r"(?i)\bsize shown varies\b.*$", "", text)
    text = re.sub(r"(?i)\binvestigating\b.*$", "", text)
    text = re.sub(r"(?i)\bpricing from\b.*$", "", text)
    text = re.sub(r"\([^)]*(location|up/down|left/right|door|drawer|centre|center)[^)]*\)", "", text, flags=re.IGNORECASE)
    return _verification_strip_summary_tail(
        text,
        (
            r"(?i)\bdoor location\b.*$",
            r"(?i)\bdown\s*drawer location\b.*$",
            r"(?i)\bdrawer location\b.*$",
            r"(?i)\b(?:centre|center)\s+to\s+profile\b.*$",
            r"(?i)\bto\s+(?:base|upper|overhead|base cabinets?|upper cabinets?|cabinet locations?)\b.*$",
        ),
    )


def _verification_normalize_imperial_handle_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    if re.fullmatch(r"(?i)none", text):
        return ""
    text = re.sub(r"^\[[^\]]+\]\s*-\s*", "", text)
    text = re.sub(r"(?i)\s*-\s*\((Vertical|Horizontal)\)\s*$", r" - \1", text)
    text = re.sub(
        r"(?i)\s*-\s*\((?:Investigating[^)]*|pricing[^)]*|(?:Horizontal|Vertical)\s+Install|location[^)]*)\)\s*$",
        "",
        text,
    )
    text = re.sub(
        r"(?i)\b(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Momo|Barchie|Lincoln Sentry|ABI Interiors)\b\s*-\s*",
        "",
        text,
    )
    text = re.sub(
        r"(?i)\b(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Momo|Barchie|Lincoln Sentry|ABI Interiors)\b$",
        "",
        text,
    ).strip(" -|;,")
    text = re.sub(r"(?i)\b(?:Horizontal|Vertical)\s+Install\b.*$", "", text).strip(" -|;,")
    if re.search(r"(?i)\bUPPER\s*-\s*FINGERPULL\b", text):
        return "UPPER - FINGERPULL"
    if re.search(r"(?i)\bBASE\s*-\s*BEVEL\s+EDGE\s+FINGERPULL\b", text):
        return "BASE - BEVEL EDGE FINGERPULL"
    tall_code_match = re.search(r"(?i)\b(?:TALL\s*-\s*)?(S225\.280\.MBK)\b", text)
    if tall_code_match:
        return f"TALL - {parsing.normalize_space(tall_code_match.group(1)).rstrip('.') }."
    chute_match = re.search(r"(?i)\b(?:CHUTE\s+DOOR\s*-\s*)?(S225\.160\.MBK)\b", text)
    if chute_match:
        return f"CHUTE DOOR - {parsing.normalize_space(chute_match.group(1)).rstrip('.') }."
    allegra_knob_match = re.search(
        r"(?i)\bKnob\b(?:\s*-\s*|\s+)(?P<code>[A-Z0-9.-]*K)\b(?:\s+in\s+(?P<finish>[A-Za-z ]+?))?(?=\s*(?:-|$|\(|\|))",
        text,
    )
    if allegra_knob_match:
        knob_text = f"Knob - {parsing.normalize_space(allegra_knob_match.group('code'))}"
        finish = parsing.normalize_space(allegra_knob_match.group("finish") or "")
        if finish:
            knob_text = f"{knob_text} in {finish}"
        return knob_text
    knob_match = re.search(
        r"(?i)\b(?P<body>(?:[A-Z][A-Za-z0-9& ]+\s*-\s*)?(?:Woodgate\s+Round\s+Cabinet\s+Knob|[^|]*?\b(?:cabinet\s+)?knob\b[^|]*?)(?:SKU:Part No:\s*[A-Z0-9.]+)?)",
        text,
    )
    if knob_match:
        knob_text = parsing.normalize_space(knob_match.group("body"))
        knob_text = re.sub(r"(?i)^Handles?\s*-\s*", "", knob_text).strip(" -|;,")
        knob_text = re.sub(r"(?i)^Barchie\s+", "", knob_text).strip(" -|;,")
        knob_text = re.sub(r"(?i)\s*\|?\s*SKU:Part No:\s*[A-Z0-9.]+\b", "", knob_text).strip(" -|;,")
        knob_text = re.sub(r"(?i)\s*\|\s*Casters\b.*$", "", knob_text).strip(" -|;,")
        return knob_text
    desk_handle_match = re.search(
        r"(?i)\b(?:(?P<prefix>DESK)\s*-\s*)?(?P<body>\d+\s+Voda\s+Profile\s+Handle\s+(?:Brushed\s+Nickel|Matt\s+Black)\s+\d+\s*mm\s*-\s*SO-2163-[A-Z0-9-]+)\b",
        text,
    )
    if desk_handle_match:
        desk_prefix = "DESK - " if desk_handle_match.group("prefix") else ""
        desk_text = f"{desk_prefix}{parsing.normalize_space(desk_handle_match.group('body'))}"
        desk_text = re.sub(r"(?i)\b(?:Furnware|Titus Tekform)\b\s*$", "", desk_text).strip(" -|;,")
        return desk_text
    ht576_match = re.search(r"(?i)\b(HT576\s*-\s*(?:128|192)\s*-\s*BKO)\b", text)
    if ht576_match:
        base = parsing.normalize_space(ht576_match.group(1))
        if re.search(r"(?i)\bDarwen\s+Cabinet\s+Pull\s+Handle\b", text):
            base = f"{base} Darwen Cabinet Pull Handle"
        if re.search(r"(?i)\bBlack\s+Olive\s+Colour\b", text):
            base = f"{base} - Black Olive Colour"
        return parsing.normalize_space(base)
    complex_markers = bool(
        re.search(
            r"(?i)\b(?:tall door handles?|high split handle|voda profile handle|hinoki|tekform|so-[a-z0-9-]+|hin[0-9a-z.-]+|benchseat drawers\s*-\s*pto)\b",
            text,
        )
    )
    if re.search(r"(?i)\bno\s+handles?\b", text) and not complex_markers and not re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", text):
        return "No handles"
    if re.search(r"(?i)\btouch\s+catch\b", text):
        touch_match = re.search(r"(?i)\bTouch catch(?:\s*-\s*Overheads above)?", text)
        if touch_match:
            return parsing.normalize_space(touch_match.group(0))
    if re.search(r"(?i)\bpush\s+to\s+open\b", text) and not complex_markers:
        return "Push to open"
    if re.search(r"(?i)\bDrawers?\s*-\s*Bevel Edge finger pull\b", text):
        return "Drawers - Bevel Edge finger pull"
    if re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", text) and not re.search(
        r"(?i)\b(?:desk|benchseat|drawers?\s*-|voda|so-[a-z0-9-]+)\b",
        text,
    ):
        return "Bevel Edge finger pull"
    normalized = _verification_normalize_handle_summary_value(text)
    normalized = re.sub(r"(?i)\b(?:Furnware|Titus Tekform)\b\s*$", "", normalized).strip(" -|;,")
    return normalized


def _verification_semantic_imperial_handle_summary_candidates(value: str) -> list[str]:
    text = parsing.normalize_space(value)
    if not text:
        return []
    if re.search(r"(?i)\bHT576\s*-\s*(?:128|192)\s*-\s*BKO\b", text):
        normalized = _verification_normalize_imperial_handle_summary_value(text)
        return [normalized] if normalized else []
    text = re.sub(
        r"(?i)\b((?:DESK\s*-\s*)?\d*\s*Voda\s+Profile\s+Handle\s+(?:Brushed\s+Nickel|Matt\s+Black)\s+\d+\s*mm)\s*\|\s*(SO-2163-[A-Z0-9-]+)\b",
        lambda match: f"{parsing.normalize_space(match.group(1))} - {parsing.normalize_space(match.group(2))}",
        text,
    )
    text = re.sub(r"(?i)\bUPPER\s*-\s*FINGERPULL(?:\s*\([^)]*\))?", lambda match: f" | {match.group(0)} | ", text)
    text = re.sub(r"(?i)\bBASE\s*-\s*BEVEL\s+EDGE\s+FINGERPULL\b", lambda match: f" | {match.group(0)} | ", text)
    text = re.sub(r"(?i)\bS225\.280\.MBK\b", lambda match: f" | {match.group(0)} | ", text)
    text = re.sub(r"(?i)\bS225\.160\.MBK\b", lambda match: f" | {match.group(0)} | ", text)
    candidates: list[str] = []
    patterns = (
        r"(?i)\bUPPER\s*-\s*FINGERPULL(?:\s*\([^)]*\))?",
        r"(?i)\bBASE\s*-\s*BEVEL\s+EDGE\s+FINGERPULL\b",
        r"(?i)\bTALL\s*-\s*[A-Z]\d+\.\d+\.?[A-Z]*\b",
        r"(?i)\bCHUTE\s+DOOR\s*-\s*[A-Z]\d+\.\d+\.?[A-Z]*\b",
        r"(?i)\bS225\.280\.MBK\b",
        r"(?i)\bS225\.160\.MBK\b",
        r"(?i)\bTall Door Handles?\s*-\s*[^|]+",
        r"(?i)\bHigh Split Handle\s*-\s*[^|]+",
        r"(?i)\bDrawers?\s*-\s*Bevel Edge finger pull\b",
        r"(?i)\bBevel edge finger pull(?:\s+on\s+lowers)?\b",
        r"(?i)\bKnob\s*-\s*[A-Z0-9.-]*K\b[^|]*",
        r"(?i)\b(?:Handles?\s*-\s*)?(?:Barchie\s+)?Woodgate\s+Round\s+Cabinet\s+Knob(?:\s*\|\s*SKU:Part No:\s*[A-Z0-9.]+)?\b",
        r"(?i)\bDESK\s*-\s*\d+\s+Voda\s+Profile\s+Handle\s+(?:Brushed\s+Nickel|Matt\s+Black)\s+\d+\s*mm\s*-\s*SO-2163-[A-Z0-9-]+\b",
        r"(?i)\bBENCHSEAT DRAWERS?\s*-\s*PTO\b",
        r"(?i)\bPush to open\b",
        r"(?i)\bNo handles?(?:\s+on\s+[A-Za-z ]+)?\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            normalized = _verification_normalize_imperial_handle_summary_value(match.group(0))
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    if candidates:
        return candidates
    whole_normalized = _verification_normalize_imperial_handle_summary_value(text)
    if whole_normalized and re.search(
        r"(?i)\b(?:pm\d+[a-z0-9 /.-]*|hole centres|oa size|matt silver|touch catch|no handles?|push to open|bevel edge finger pull)\b",
        text,
    ):
        return [parsing.normalize_space(re.sub(r"\s*\|\s*", " ", whole_normalized))]
    fallback_values = [
        normalized
        for normalized in (
            _verification_normalize_imperial_handle_summary_value(part)
            for part in re.split(r"\s*\|\s*", text)
        )
        if normalized and not re.match(r"(?i)^casters?$", normalized)
    ]
    if any(re.match(r"(?i)^DESK\s*-\s*\d+\s+Voda\s+Profile\s+Handle\b", value) for value in fallback_values):
        fallback_values = [
            value
            for value in fallback_values
            if not re.fullmatch(r"(?i)SO-2163-[A-Z0-9-]+", value)
        ]
    return fallback_values


def _verification_strip_summary_tail(text: str, patterns: tuple[str, ...]) -> str:
    normalized = parsing.normalize_space(text)
    if not normalized:
        return ""
    end_index = len(normalized)
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match:
            end_index = min(end_index, match.start())
    return normalized[:end_index].strip(" -;,/")


def _load_checklist_json(value: Any) -> list[dict[str, Any]]:
    try:
        raw_items = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(raw_items, list):
        return []
    return _normalize_verification_checklist(raw_items)


def _normalize_verification_checklist(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "section_type": str(item.get("section_type", "") or ""),
                "entity_label": str(item.get("entity_label", "") or ""),
                "field_name": str(item.get("field_name", "") or ""),
                "extracted_value": str(item.get("extracted_value", "") or ""),
                "source_page_refs": str(item.get("source_page_refs", "") or ""),
                "pdf_page_ref": str(item.get("pdf_page_ref", "") or ""),
                "status": _normalize_item_status(item.get("status", "pending")),
                "qa_note": str(item.get("qa_note", "") or ""),
            }
        )
    return normalized


def _normalize_verification_status(value: Any) -> str:
    text = str(value or "pending").strip().lower()
    if text in {"passed", "failed", "pending"}:
        return text
    return "pending"


def _normalize_item_status(value: Any) -> str:
    text = str(value or "pending").strip().lower()
    if text in {"pass", "fail", "na", "pending"}:
        return text
    return "pending"


def _verification_status_from_checklist(checklist: list[dict[str, Any]]) -> str:
    if not checklist:
        return "pending"
    seen_pending = False
    for item in checklist:
        status = _normalize_item_status(item.get("status", "pending"))
        if status == "fail":
            return "failed"
        if status not in {"pass", "na"}:
            seen_pending = True
    return "pending" if seen_pending else "passed"


init_db()
