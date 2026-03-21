from __future__ import annotations

import json
import sqlite3
from typing import Any

from App.services.runtime import DB_PATH, utc_now_iso


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
                result_json TEXT NOT NULL DEFAULT ''
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


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connect() as conn:
        return list(conn.execute(query, params).fetchall())


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with connect() as conn:
        return conn.execute(query, params).fetchone()


def create_builder(name: str, slug: str, notes: str) -> int:
    now = utc_now_iso()
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO builders (name, slug, notes, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (name, slug, notes, now, now),
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


def list_jobs() -> list[dict[str, Any]]:
    return fetch_all(
        """
        SELECT j.*, b.name AS builder_name, b.slug AS builder_slug,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'spec') AS spec_file_count,
               (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.id AND jf.file_role = 'drawing') AS drawing_file_count
        FROM jobs j
        JOIN builders b ON b.id = j.builder_id
        ORDER BY j.created_at DESC, j.id DESC
        """
    )


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
            INSERT INTO runs (job_id, run_kind, status, stage, message, requested_at, parser_version, result_json)
            VALUES (?, ?, 'queued', 'queued', 'Waiting for worker', ?, 'v1', '')
            """,
            (job_id, run_kind, now),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("queued", now, job_id))
        return int(cur.lastrowid)


def list_runs(job_id: int) -> list[dict[str, Any]]:
    return fetch_all("SELECT * FROM runs WHERE job_id = ? ORDER BY id DESC", (job_id,))


def claim_next_run() -> dict[str, Any] | None:
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM runs WHERE status = 'queued' ORDER BY id ASC LIMIT 1").fetchone()
        if not row:
            conn.commit()
            return None
        conn.execute(
            """
            UPDATE runs
            SET status = 'running', stage = 'starting', message = 'Worker claimed run', started_at = ?
            WHERE id = ?
            """,
            (utc_now_iso(), row["id"]),
        )
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", ("running", utc_now_iso(), row["job_id"]))
        conn.commit()
        row["status"] = "running"
        row["stage"] = "starting"
        return row


def update_run_progress(run_id: int, stage: str, message: str) -> None:
    with connect() as conn:
        conn.execute("UPDATE runs SET stage = ?, message = ? WHERE id = ?", (stage, message, run_id))


def mark_run_succeeded(run_id: int, job_id: int, result_payload: dict[str, Any]) -> None:
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


def mark_run_failed(run_id: int, job_id: int, error_text: str) -> None:
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
