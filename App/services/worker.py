from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from App.services import store
from App.services.extraction_service import build_drawing_snapshot, build_spec_snapshot
from App.services.runtime import ensure_builder_dir, ensure_job_dirs


def run_worker_loop(poll_seconds: int = 3) -> None:
    while True:
        run = store.claim_next_run()
        if not run:
            time.sleep(poll_seconds)
            continue
        process_run(run)


def process_run(run: dict[str, Any]) -> None:
    job = store.get_job(int(run["job_id"]))
    if not job:
        store.mark_run_failed(int(run["id"]), int(run["job_id"]), "Job no longer exists.")
        return
    builder = store.get_builder(int(job["builder_id"]))
    if not builder:
        store.mark_run_failed(int(run["id"]), int(run["job_id"]), "Builder no longer exists.")
        return

    try:
        store.update_run_progress(int(run["id"]), "loading", "Loading files")
        job_dirs = ensure_job_dirs(job["job_no"])
        if run["run_kind"] == "spec":
            spec_files = _attach_paths(job_dirs["spec_dir"], store.list_job_files(int(job["id"]), "spec"))
            template_dir = ensure_builder_dir(builder["slug"])
            template_files = _attach_paths(template_dir, store.list_builder_templates(int(builder["id"])))
            store.update_run_progress(int(run["id"]), "extracting", "Extracting spec data")
            snapshot = build_spec_snapshot(job=job, builder=builder, files=spec_files, template_files=template_files)
            store.upsert_snapshot(int(job["id"]), "raw_spec", snapshot)
        else:
            drawing_files = _attach_paths(job_dirs["drawing_dir"], store.list_job_files(int(job["id"]), "drawing"))
            store.update_run_progress(int(run["id"]), "extracting", "Extracting drawing data")
            snapshot = build_drawing_snapshot(job=job, builder=builder, files=drawing_files)
            store.upsert_snapshot(int(job["id"]), "drawing", snapshot)
        store.mark_run_succeeded(int(run["id"]), int(job["id"]), snapshot)
    except Exception as exc:
        store.mark_run_failed(int(run["id"]), int(job["id"]), str(exc))


def _attach_paths(base_dir: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row["path"] = str(base_dir / row["stored_name"])
    return rows
