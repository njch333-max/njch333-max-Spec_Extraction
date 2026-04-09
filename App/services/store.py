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
    summary_entries: dict[str, list[str]] = {
        "door_colours": [],
        "handles": [],
        "bench_tops": [],
    }
    seen_summary_entries: dict[str, set[str]] = {key: set() for key in summary_entries}
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
            ordered_rows = sorted(
                [item for item in material_rows if isinstance(item, dict)],
                key=lambda item: (
                    int(item.get("page_no", 0) or 0),
                    int(item.get("row_order", 0) or 0),
                    _verification_text(item.get("area_or_item", "")),
                ),
            )
            for item in ordered_rows:
                area_or_item = _verification_text(item.get("area_or_item", ""))
                if not area_or_item:
                    continue
                supplier = _verification_text(item.get("supplier", ""))
                description = _verification_text(item.get("specs_or_description", ""))
                notes = _verification_text(item.get("notes", ""))
                extracted_value = " - ".join(part for part in (supplier, description, notes) if part)
                if not extracted_value:
                    continue
                tags = [str(tag).strip() for tag in (item.get("tags", []) or []) if str(tag).strip()]
                primary_tag = tags[0] if tags else "other_material"
                if primary_tag not in summary_entries:
                    continue
                if not _imperial_verification_row_is_summary_worthy(
                    area_or_item=area_or_item,
                    extracted_value=extracted_value,
                    tags=tags,
                    needs_review=bool(item.get("needs_review", False)),
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
                normalized_value = _verification_normalize_summary_value(primary_tag, extracted_value)
                summary_text = normalized_value or extracted_value
                display_text = f"{room_label}: {area_or_item} -> {summary_text}"
                dedupe_basis = summary_text
                dedupe_key = f"{room_label}|{primary_tag}|{dedupe_basis}".lower()
                if dedupe_key not in seen_summary_entries[primary_tag]:
                    seen_summary_entries[primary_tag].add(dedupe_key)
                    summary_entries[primary_tag].append(display_text)
        for field_name, source_key in (
            ("drawers", "drawers_soft_close"),
            ("hinges", "hinges_soft_close"),
            ("flooring", "flooring"),
            ("sink", "sink_info"),
        ):
            _append_verification_item(
                checklist,
                section_type="room",
                entity_label=room_label,
                field_name=field_name,
                extracted_value=_verification_text(room.get(source_key, "")),
                source_page_refs=room_page_refs,
            )
    for bucket_key, label in (
        ("door_colours", "Door Colours"),
        ("handles", "Handles"),
        ("bench_tops", "Bench Tops"),
    ):
        _append_verification_item(
            checklist,
            section_type="summary",
            entity_label="Material Summary",
            field_name=label,
            extracted_value=" | ".join(summary_entries[bucket_key]),
            source_page_refs="",
        )
    return checklist


def _imperial_verification_summary_bucket_key(tags: list[str]) -> str:
    normalized = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    if "door_colours" in normalized:
        return "door_colours"
    if "handles" in normalized:
        return "handles"
    if "bench_tops" in normalized:
        return "bench_tops"
    return ""


def _imperial_verification_row_is_summary_worthy(
    *,
    area_or_item: str,
    extracted_value: str,
    tags: list[str],
    needs_review: bool,
) -> bool:
    if needs_review:
        return False
    bucket_key = _imperial_verification_summary_bucket_key(tags)
    if not bucket_key:
        return False
    title = _verification_text(area_or_item)
    value = _verification_text(extracted_value)
    normalized_value = _verification_normalize_summary_value(bucket_key, value)
    if not title or not value:
        return False
    if not normalized_value or re.match(r"(?i)^(?:incl|include|open|split|allow|note)\b", normalized_value):
        return False
    if bucket_key == "bench_tops":
        return "BENCHTOP" in title.upper()
    if bucket_key == "handles":
        if not re.search(r"(?i)\b(?:handles?|knob)\b", title):
            return False
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b|casters?)\b", value):
            return False
        return True
    if bucket_key == "door_colours":
        if not re.search(r"(?i)\b(?:colour|frame|bar back|panel)\b", title):
            return False
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b|casters?|handle|fingerpull|knob)\b", value):
            return False
        return True
    return False


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
        text = re.sub(r"(?i)\b([A-Z][A-Z0-9&]+)\s+\1\b", r"\1", text)
        text = re.sub(r"(?i)\bimage of\b.*$", "", text)
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
