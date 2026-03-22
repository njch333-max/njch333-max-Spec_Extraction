from __future__ import annotations

import json
import sqlite3
from typing import Any

from App.services import cleaning_rules
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


def list_jobs(job_query: str = "") -> list[dict[str, Any]]:
    query = (
        """
        SELECT j.*, b.name AS builder_name, b.slug AS builder_slug,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'spec') AS spec_file_count,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'drawing') AS drawing_file_count
        FROM jobs j
        JOIN builders b ON b.id = j.builder_id
        """
    )
    params: tuple[Any, ...] = ()
    if job_query:
        query += " WHERE j.job_no LIKE ?"
        params = (f"%{job_query}%",)
    query += " ORDER BY j.created_at DESC, j.id DESC"
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


def list_runs(job_id: int) -> list[dict[str, Any]]:
    return fetch_all("SELECT * FROM runs WHERE job_id = ? ORDER BY id DESC", (job_id,))


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
        if row.get("owner_token") == owner_token or str(row.get("expires_at", "")) <= now:
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


def insert_auth_event(username: str, action: str, detail: str = "") -> None:
    with connect() as conn:
        conn.execute(
            "INSERT INTO auth_events (username, action, detail, created_at) VALUES (?, ?, ?, ?)",
            (username, action, detail, utc_now_iso()),
        )


init_db()
