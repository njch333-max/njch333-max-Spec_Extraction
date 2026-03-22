from __future__ import annotations

import atexit
import os
import secrets
import time
from pathlib import Path
from typing import Any

from App.services import store
from App.services.extraction_service import build_drawing_snapshot, build_spec_snapshot
from App.services import cleaning_rules
from App.services.runtime import APP_BUILD_ID, ensure_builder_dir, ensure_job_dirs


WORKER_PID = os.getpid()
WORKER_TOKEN = f"{WORKER_PID}-{secrets.token_hex(8)}"


def run_worker_loop(poll_seconds: int = 3) -> None:
    atexit.register(store.release_worker_lease, WORKER_TOKEN)
    while True:
        if not store.acquire_worker_lease(WORKER_TOKEN, WORKER_PID, APP_BUILD_ID):
            time.sleep(poll_seconds)
            continue
        run = store.claim_next_run(worker_pid=WORKER_PID, app_build_id=APP_BUILD_ID, worker_token=WORKER_TOKEN)
        if not run:
            time.sleep(poll_seconds)
            continue
        process_run(run)


def process_run(run: dict[str, Any]) -> None:
    job = store.get_job(int(run["job_id"]))
    if not job:
        store.mark_run_failed(int(run["id"]), int(run["job_id"]), "Job no longer exists.", worker_token=WORKER_TOKEN)
        return
    builder = store.get_builder(int(job["builder_id"]))
    if not builder:
        store.mark_run_failed(int(run["id"]), int(run["job_id"]), "Builder no longer exists.", worker_token=WORKER_TOKEN)
        return

    try:
        run_id = int(run["id"])
        parser_strategy = cleaning_rules.global_parser_strategy()
        store.update_run_runtime_metadata(run_id, parser_strategy, WORKER_PID, APP_BUILD_ID)
        store.update_run_progress(run_id, "loading", "Loading files from job folders", worker_token=WORKER_TOKEN)
        job_dirs = ensure_job_dirs(job["job_no"])
        if run["run_kind"] == "spec":
            spec_files = _attach_paths(job_dirs["spec_dir"], store.list_job_files(int(job["id"]), "spec"))
            template_dir = ensure_builder_dir(builder["slug"])
            template_files = _attach_paths(template_dir, store.list_builder_templates(int(builder["id"])))
            snapshot = build_spec_snapshot(
                job=job,
                builder=builder,
                files=spec_files,
                template_files=template_files,
                progress_callback=lambda stage, message: store.update_run_progress(run_id, stage, message, worker_token=WORKER_TOKEN),
            )
            store.upsert_snapshot(int(job["id"]), "raw_spec", snapshot)
        else:
            drawing_files = _attach_paths(job_dirs["drawing_dir"], store.list_job_files(int(job["id"]), "drawing"))
            snapshot = build_drawing_snapshot(
                job=job,
                builder=builder,
                files=drawing_files,
                progress_callback=lambda stage, message: store.update_run_progress(run_id, stage, message, worker_token=WORKER_TOKEN),
            )
            store.upsert_snapshot(int(job["id"]), "drawing", snapshot)
        store.update_run_progress(run_id, "saving", "Saving snapshot to SQLite", worker_token=WORKER_TOKEN)
        store.mark_run_succeeded(int(run["id"]), int(job["id"]), snapshot, worker_token=WORKER_TOKEN)
    except Exception as exc:
        store.mark_run_failed(int(run["id"]), int(job["id"]), str(exc), worker_token=WORKER_TOKEN)


def _attach_paths(base_dir: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row["path"] = str(base_dir / row["stored_name"])
    return rows
