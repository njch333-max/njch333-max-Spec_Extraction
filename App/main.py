from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from App.services import cleaning_rules, parsing, store
from App.services.auth import authenticate, current_user, ensure_csrf_token, login_user, logout_user, verify_csrf
from App.services.export_service import build_exports, build_spec_list_excel
from App.services.runtime import (
    HOST_DOMAIN,
    HTTPS_ONLY,
    JOBS_ROOT,
    MAX_UPLOAD_MB,
    SECRET_KEY,
    SESSION_DOMAIN,
    STATIC_DIR,
    TEMPLATES_DIR,
    ensure_builder_dir,
    ensure_job_dirs,
    safe_filename,
    slugify,
    utc_now_iso,
    write_bytes_atomic,
)


app = FastAPI(title="Spec Extraction", version="0.1.0")
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="spec_extraction_session",
    same_site="lax",
    https_only=HTTPS_ONLY,
    domain=SESSION_DOMAIN or None,
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
BRISBANE_TZ = timezone(timedelta(hours=10), name="AEST")


@app.get("/")
def root(request: Request):
    if current_user(request):
        return RedirectResponse("/jobs", status_code=303)
    return RedirectResponse("/login", status_code=303)


@app.get("/login")
def login_page(request: Request):
    if current_user(request):
        return RedirectResponse("/jobs", status_code=303)
    return templates.TemplateResponse(request, "login.html", _context(request, "Login"))


@app.post("/login")
async def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
):
    verify_csrf(request, csrf_token)
    if not authenticate(username.strip(), password):
        _set_flash(request, "error", "Invalid username or password.")
        return RedirectResponse("/login", status_code=303)
    login_user(request, username.strip())
    _set_flash(request, "success", "Signed in successfully.")
    return RedirectResponse("/jobs", status_code=303)


@app.post("/logout")
async def logout_action(request: Request, csrf_token: str = Form("")):
    verify_csrf(request, csrf_token)
    logout_user(request)
    _set_flash(request, "success", "Signed out.")
    return RedirectResponse("/login", status_code=303)


@app.get("/builders")
def builders_page(request: Request):
    user = _require_page_user(request)
    if user:
        builders = store.list_builders()
        for builder in builders:
            builder["templates"] = _present_files(store.list_builder_templates(int(builder["id"])))
        return templates.TemplateResponse(request, "builders.html", _context(request, "Builders", builders=builders))
    return RedirectResponse("/login", status_code=303)


@app.post("/builders")
async def create_builder_action(
    request: Request,
    name: str = Form(...),
    slug_value: str = Form(""),
    notes: str = Form(""),
    csrf_token: str = Form(""),
):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    clean_name = name.strip()
    if not clean_name:
        _set_flash(request, "error", "Builder name is required.")
        return RedirectResponse("/builders", status_code=303)
    clean_slug = slugify(slug_value or clean_name)
    if store.get_builder_by_slug(clean_slug):
        _set_flash(request, "error", "Builder slug already exists.")
        return RedirectResponse("/builders", status_code=303)
    builder_id = store.create_builder(clean_name, clean_slug, notes.strip())
    builder = store.get_builder(builder_id)
    ensure_builder_dir(builder["slug"])
    _set_flash(request, "success", f"Builder '{clean_name}' created.")
    return RedirectResponse("/builders", status_code=303)


@app.get("/builders/{builder_id}/rules")
def builder_rules_page(request: Request, builder_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    _set_flash(request, "info", "Builder-specific cleaning rules have been retired. All builders now use the global conservative profile.")
    return RedirectResponse("/builders", status_code=303)


@app.post("/builders/{builder_id}/rules")
async def save_builder_rules_action(request: Request, builder_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    verify_csrf(request, str(form.get("csrf_token", "")))
    _set_flash(request, "info", "Builder-specific cleaning rules have been retired. All builders now use the global conservative profile.")
    return RedirectResponse("/builders", status_code=303)


@app.post("/builders/{builder_id}/templates")
async def upload_builder_templates(
    request: Request,
    builder_id: int,
    files: list[UploadFile] = File(...),
    csrf_token: str = Form(""),
):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    builder = store.get_builder(builder_id)
    if not builder:
        _set_flash(request, "error", "Builder not found.")
        return RedirectResponse("/builders", status_code=303)
    builder_dir = ensure_builder_dir(builder["slug"])
    try:
        for upload in files:
            payload = await upload.read()
            _guard_upload_size(len(payload))
            stored_name = f"{utc_now_iso().replace(':', '').replace('-', '')}_{safe_filename(upload.filename)}"
            write_bytes_atomic(builder_dir / stored_name, payload)
            store.create_builder_template(builder_id, stored_name, upload.filename or stored_name, upload.content_type or "", len(payload))
    except ValueError as exc:
        _set_flash(request, "error", str(exc))
        return RedirectResponse("/builders", status_code=303)
    _set_flash(request, "success", "Template files uploaded.")
    return RedirectResponse("/builders", status_code=303)


@app.post("/templates/{template_id}/delete")
async def delete_template_action(request: Request, template_id: int, csrf_token: str = Form("")):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    row = store.get_builder_template(template_id)
    if row:
        builder = store.get_builder(int(row["builder_id"]))
        if builder:
            path = ensure_builder_dir(builder["slug"]) / row["stored_name"]
            if path.exists():
                path.unlink()
        store.delete_builder_template(template_id)
    _set_flash(request, "success", "Template deleted.")
    return RedirectResponse("/builders", status_code=303)


@app.get("/builders/templates/{template_id}/download")
def download_template(request: Request, template_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    row = store.get_builder_template(template_id)
    if not row:
        return RedirectResponse("/builders", status_code=303)
    builder = store.get_builder(int(row["builder_id"]))
    if not builder:
        return RedirectResponse("/builders", status_code=303)
    path = ensure_builder_dir(builder["slug"]) / row["stored_name"]
    return FileResponse(path, filename=row["original_name"])


@app.get("/jobs")
def jobs_page(request: Request):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    query = request.query_params.get("q", "").strip()
    jobs = _present_jobs(store.list_jobs(query))
    builders = store.list_builders()
    return templates.TemplateResponse(request, "jobs.html", _context(request, "Jobs", jobs=jobs, builders=builders, job_query=query))


@app.post("/jobs")
async def create_job_action(
    request: Request,
    job_no: str = Form(...),
    builder_id: int = Form(...),
    title: str = Form(""),
    notes: str = Form(""),
    csrf_token: str = Form(""),
):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    clean_job_no = safe_filename(job_no).replace("_", "")
    if not clean_job_no:
        _set_flash(request, "error", "Job number is required.")
        return RedirectResponse("/jobs", status_code=303)
    if store.get_job_by_no(clean_job_no):
        _set_flash(request, "error", "Job number already exists.")
        return RedirectResponse("/jobs", status_code=303)
    builder = store.get_builder(builder_id)
    if not builder:
        _set_flash(request, "error", "Builder not found.")
        return RedirectResponse("/jobs", status_code=303)
    job_id = store.create_job(clean_job_no, builder_id, title.strip(), notes.strip())
    ensure_job_dirs(clean_job_no)
    _set_flash(request, "success", f"Job '{clean_job_no}' created.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/{job_id}/delete")
async def delete_job_action(request: Request, job_id: int, csrf_token: str = Form("")):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    active_run = next(
        (
            run
            for run in store.list_runs(job_id)
            if str(run.get("status", "")).lower() in {"queued", "running"}
        ),
        None,
    )
    if active_run:
        _set_flash(request, "error", "Cannot delete a job while a parse run is queued or running.")
        return RedirectResponse("/jobs", status_code=303)
    job_root = JOBS_ROOT / str(job["job_no"])
    store.delete_job(job_id)
    shutil.rmtree(job_root, ignore_errors=True)
    _set_flash(request, "success", f"Job '{job['job_no']}' deleted.")
    return RedirectResponse("/jobs", status_code=303)


@app.get("/jobs/{job_id}")
def job_detail_page(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    builder = store.get_builder(int(job["builder_id"]))
    dirs = ensure_job_dirs(job["job_no"])
    spec_files = _present_files(store.list_job_files(job_id, "spec"))
    drawing_files = _present_files(store.list_job_files(job_id, "drawing"))
    runs = _present_runs(store.list_runs(job_id))
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    drawing_snapshot_row = store.get_snapshot(job_id, "drawing")
    raw_verification = store.get_job_snapshot_verification(job_id, "raw_spec")
    review_row = store.get_review(job_id)
    review_snapshot = review_row["data"] if review_row else (raw_snapshot_row["data"] if raw_snapshot_row else _blank_snapshot(job))
    exports = _list_export_files(dirs["export_dir"])
    return templates.TemplateResponse(
        request,
        "job_detail.html",
        _context(
            request,
            f"Job {job['job_no']}",
            sidebar_collapsible=True,
            sidebar_default_hidden=True,
            job=job,
            job_site_address=_job_site_address(raw_snapshot_row["data"] if raw_snapshot_row else None, drawing_snapshot_row["data"] if drawing_snapshot_row else None),
            builder=builder,
            spec_files=spec_files,
            drawing_files=drawing_files,
            runs=runs,
            raw_snapshot=raw_snapshot_row["data"] if raw_snapshot_row else None,
            raw_analysis=_analysis_from_snapshot(raw_snapshot_row["data"] if raw_snapshot_row else None),
            raw_verification=raw_verification,
            raw_verification_summary=_verification_summary(raw_verification),
            drawing_snapshot=drawing_snapshot_row["data"] if drawing_snapshot_row else None,
            drawing_analysis=_analysis_from_snapshot(drawing_snapshot_row["data"] if drawing_snapshot_row else None),
            review_snapshot=review_snapshot,
            room_rows=_flatten_rooms(review_snapshot),
            appliance_rows=_flatten_appliances(review_snapshot),
            exports=exports,
        ),
    )


@app.get("/jobs/{job_id}/run-history")
def run_history_partial(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    return templates.TemplateResponse(
        request,
        "partials/run_history_section.html",
        _context(
            request,
            f"Run History {job['job_no']}",
            job=job,
            runs=_present_runs(store.list_runs(job_id)),
            raw_verification_summary=_verification_summary(store.get_job_snapshot_verification(job_id, "raw_spec")),
        ),
    )


@app.get("/jobs/{job_id}/spec-list")
def spec_list_page(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    raw_snapshot = raw_snapshot_row["data"] if raw_snapshot_row else None
    raw_verification = store.get_job_snapshot_verification(job_id, "raw_spec")
    latest_spec_run = _latest_completed_run(store.list_runs(job_id), "spec")
    return _spec_list_template_response(
        request,
        job=job,
        raw_snapshot=raw_snapshot,
        raw_verification=raw_verification,
        raw_extraction_duration=_format_run_duration(latest_spec_run),
    )


@app.get("/jobs/{job_id}/runs/{run_id}/spec-list")
def historical_spec_list_page(request: Request, job_id: int, run_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    run = store.get_job_run(job_id, run_id)
    if not run or str(run.get("run_kind", "")) != "spec" or str(run.get("status", "")) != "succeeded":
        _set_flash(request, "error", "Historical spec result not found for that run.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    payload_text = str(run.get("result_json", "") or "").strip()
    if not payload_text:
        _set_flash(request, "error", "This run does not have a stored spec result.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    try:
        payload = json.loads(payload_text)
    except (TypeError, ValueError, json.JSONDecodeError):
        _set_flash(request, "error", "Stored run result is invalid JSON.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    historical_run = {
        "id": int(run["id"]),
        "requested_at": _format_brisbane_time(run.get("requested_at", "")),
        "finished_at": _format_brisbane_time(run.get("finished_at", "")),
        "duration": _run_duration_display(run),
        "app_build_id": str(run.get("app_build_id", "") or ""),
    }
    return _spec_list_template_response(
        request,
        job=job,
        raw_snapshot=payload if isinstance(payload, dict) else {},
        raw_verification=None,
        raw_extraction_duration=_run_duration_display(run),
        historical_run=historical_run,
    )


@app.get("/jobs/{job_id}/spec-list.xlsx")
def spec_list_excel_download(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    if not raw_snapshot_row:
        _set_flash(request, "error", "No raw spec snapshot is available for this job.")
        return RedirectResponse(f"/jobs/{job_id}/spec-list", status_code=303)
    if not store.is_job_snapshot_verification_passed(job_id, "raw_spec"):
        _set_flash(request, "error", "PDF QA must pass before exporting the raw spec list.")
        return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)
    path = Path(build_spec_list_excel(job["job_no"], raw_snapshot_row["data"]))
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=path.name,
    )


@app.post("/jobs/{job_id}/files/upload")
async def upload_job_files(
    request: Request,
    job_id: int,
    file_role: str = Form(...),
    files: list[UploadFile] = File(...),
    csrf_token: str = Form(""),
):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    dirs = ensure_job_dirs(job["job_no"])
    target_dir = dirs["spec_dir"] if file_role == "spec" else dirs["drawing_dir"]
    try:
        for upload in files:
            payload = await upload.read()
            _guard_upload_size(len(payload))
            stored_name = f"{utc_now_iso().replace(':', '').replace('-', '')}_{safe_filename(upload.filename)}"
            write_bytes_atomic(target_dir / stored_name, payload)
            store.create_job_file(job_id, file_role, stored_name, upload.filename or stored_name, upload.content_type or "", len(payload))
    except ValueError as exc:
        _set_flash(request, "error", str(exc))
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    _set_flash(request, "success", "Files uploaded.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/files/{file_id}/delete")
async def delete_job_file_action(request: Request, file_id: int, csrf_token: str = Form("")):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    row = store.get_job_file(file_id)
    if not row:
        return RedirectResponse("/jobs", status_code=303)
    job = store.get_job(int(row["job_id"]))
    if job:
        dirs = ensure_job_dirs(job["job_no"])
        base_dir = dirs["spec_dir"] if row["file_role"] == "spec" else dirs["drawing_dir"]
        path = base_dir / row["stored_name"]
        if path.exists():
            path.unlink()
    store.delete_job_file(file_id)
    _set_flash(request, "success", "File deleted.")
    return RedirectResponse(f"/jobs/{row['job_id']}", status_code=303)


@app.get("/jobs/files/{file_id}/download")
def download_job_file(request: Request, file_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    row = store.get_job_file(file_id)
    if not row:
        return RedirectResponse("/jobs", status_code=303)
    job = store.get_job(int(row["job_id"]))
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    dirs = ensure_job_dirs(job["job_no"])
    base_dir = dirs["spec_dir"] if row["file_role"] == "spec" else dirs["drawing_dir"]
    return FileResponse(base_dir / row["stored_name"], filename=row["original_name"])


@app.post("/jobs/{job_id}/runs/start")
async def start_run_action(request: Request, job_id: int, run_kind: str = Form(...), csrf_token: str = Form("")):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    file_role = _run_file_role(run_kind)
    if not file_role:
        _set_flash(request, "error", "Unsupported parse request.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    if not store.list_job_files(job_id, file_role):
        _set_flash(request, "error", f"Upload at least one {file_role} file before parsing.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    run_id = store.create_run(job_id, run_kind)
    _set_flash(request, "success", f"Parse run #{run_id} created. It will start when the worker picks it up.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/{job_id}/review/save")
async def save_review_action(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    verify_csrf(request, str(form.get("csrf_token", "")))
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    base = raw_snapshot_row["data"] if raw_snapshot_row else _blank_snapshot(job)
    review_payload = _review_payload_from_form(base, form)
    store.upsert_review(job_id, review_payload)
    _set_flash(request, "success", "Reviewed data saved.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/{job_id}/export")
async def export_job_action(request: Request, job_id: int, csrf_token: str = Form("")):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    verify_csrf(request, csrf_token)
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    review_row = store.get_review(job_id)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    snapshot = review_row["data"] if review_row else (raw_snapshot_row["data"] if raw_snapshot_row else None)
    if not snapshot:
        _set_flash(request, "error", "No spec snapshot is available for export.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    if not store.is_job_snapshot_verification_passed(job_id, "raw_spec"):
        _set_flash(request, "error", "PDF QA must pass before generating formal exports.")
        return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)
    build_exports(job["job_no"], snapshot)
    _set_flash(request, "success", "Export files generated.")
    return RedirectResponse(f"/jobs/{job_id}", status_code=303)


@app.get("/jobs/{job_id}/exports/{file_name}")
def download_export(request: Request, job_id: int, file_name: str):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    if not store.is_job_snapshot_verification_passed(job_id, "raw_spec"):
        _set_flash(request, "error", "PDF QA must pass before downloading formal exports.")
        return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)
    dirs = ensure_job_dirs(job["job_no"])
    path = dirs["export_dir"] / Path(file_name).name
    return FileResponse(path, filename=path.name)


@app.get("/jobs/{job_id}/pdf-qa")
def pdf_qa_page(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    if not raw_snapshot_row:
        _set_flash(request, "error", "No raw spec snapshot is available for PDF QA yet.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    verification = store.get_job_snapshot_verification(job_id, "raw_spec")
    return templates.TemplateResponse(
        request,
        "pdf_qa.html",
        _context(
            request,
            f"PDF QA {job['job_no']}",
            sidebar_collapsible=True,
            sidebar_default_hidden=True,
            job=job,
            job_site_address=_job_site_address(raw_snapshot_row["data"], None),
            raw_snapshot=raw_snapshot_row["data"],
            raw_verification=verification,
            raw_verification_summary=_verification_summary(verification),
            verification_groups=_group_verification_items((verification or {}).get("checklist", [])),
        ),
    )


@app.post("/jobs/{job_id}/pdf-qa/save")
async def save_pdf_qa_action(request: Request, job_id: int):
    return await _persist_pdf_qa_action(request, job_id, mode="save")


@app.post("/jobs/{job_id}/pdf-qa/mark-pass")
async def mark_pdf_qa_pass_action(request: Request, job_id: int):
    return await _persist_pdf_qa_action(request, job_id, mode="mark_pass")


@app.post("/jobs/{job_id}/pdf-qa/mark-fail")
async def mark_pdf_qa_fail_action(request: Request, job_id: int):
    return await _persist_pdf_qa_action(request, job_id, mode="mark_fail")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _context(request: Request, title: str, **extra: Any) -> dict[str, Any]:
    ctx = {
        "request": request,
        "title": title,
        "current_user": current_user(request),
        "csrf_token": ensure_csrf_token(request.session),
        "flash": request.session.pop("flash", None),
        "host_domain": HOST_DOMAIN,
        "max_upload_mb": MAX_UPLOAD_MB,
        "style_version": _asset_version("style.css"),
    }
    ctx.update(extra)
    return ctx


def _set_flash(request: Request, level: str, message: str) -> None:
    request.session["flash"] = {"level": level, "message": message}


def _asset_version(file_name: str) -> str:
    path = STATIC_DIR / file_name
    try:
        return str(path.stat().st_mtime_ns)
    except OSError:
        return "1"


def _require_page_user(request: Request) -> str | None:
    return current_user(request)


def _guard_upload_size(size_bytes: int) -> None:
    if size_bytes > MAX_UPLOAD_MB * 1024 * 1024:
        raise ValueError(f"Upload exceeds {MAX_UPLOAD_MB} MB.")


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=BRISBANE_TZ)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BRISBANE_TZ)
    return parsed.astimezone(BRISBANE_TZ)


def _format_brisbane_time(value: Any) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return _display_value(value)
    return parsed.strftime("%Y-%m-%d %H:%M AEST")


def _format_duration_seconds(total_seconds: float | int) -> str:
    seconds = max(int(round(float(total_seconds))), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _format_run_duration(run: dict[str, Any] | None) -> str:
    if not run:
        return ""
    started_at = _parse_datetime(run.get("started_at", ""))
    finished_at = _parse_datetime(run.get("finished_at", ""))
    if not started_at or not finished_at:
        return ""
    return _format_duration_seconds((finished_at - started_at).total_seconds())


def _run_duration_display(run: dict[str, Any] | None, now: datetime | None = None) -> str:
    if not run:
        return "-"
    started_at = _parse_datetime(run.get("started_at", ""))
    finished_at = _parse_datetime(run.get("finished_at", ""))
    if started_at and finished_at:
        return _format_duration_seconds((finished_at - started_at).total_seconds())
    if started_at and str(run.get("status", "")).lower() == "running":
        current = now.astimezone(BRISBANE_TZ) if now else datetime.now(BRISBANE_TZ)
        return _format_duration_seconds((current - started_at).total_seconds())
    return "-"


def _latest_completed_run(runs: list[dict[str, Any]], run_kind: str) -> dict[str, Any] | None:
    for run in runs:
        if str(run.get("run_kind", "")) == run_kind and str(run.get("status", "")) == "succeeded":
            return run
    return None


def _present_jobs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    presented: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["created_at"] = _format_brisbane_time(item.get("created_at", ""))
        room_count = ""
        raw_snapshot_json = item.get("raw_snapshot_json", "")
        if raw_snapshot_json:
            try:
                raw_snapshot = json.loads(str(raw_snapshot_json))
                room_count = len([entry for entry in raw_snapshot.get("rooms", []) if isinstance(entry, dict)])
            except (TypeError, ValueError, json.JSONDecodeError):
                room_count = ""
        item["room_count"] = room_count
        presented.append(item)
    return presented


def _present_files(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    presented: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["uploaded_at"] = _format_brisbane_time(item.get("uploaded_at", ""))
        presented.append(item)
    return presented


def _flatten_rooms(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("rooms", []):
        if not isinstance(row, dict):
            continue
        door_groups = _split_room_door_groups(row)
        benchtop_groups = _split_room_benchtops(row)
        room_key = _display_value(row.get("room_key", ""))
        room_key_normalized = parsing.normalize_room_key(room_key)
        has_explicit_overheads = bool(row.get("has_explicit_overheads", False))
        show_split_benchtops = room_key_normalized == "kitchen" and bool(benchtop_groups["bench_tops_wall_run"] or benchtop_groups["bench_tops_island"])
        rows.append(
            {
                "room_key": room_key,
                "original_room_label": _display_value(row.get("original_room_label", "")),
                "bench_tops": _display_value(row.get("bench_tops", [])),
                "bench_tops_wall_run": benchtop_groups["bench_tops_wall_run"],
                "bench_tops_island": benchtop_groups["bench_tops_island"],
                "bench_tops_other": benchtop_groups["bench_tops_other"],
                "show_split_benchtops": show_split_benchtops,
                "door_panel_colours": _display_value(row.get("door_panel_colours", [])),
                "door_colours_overheads": door_groups["door_colours_overheads"],
                "door_colours_base": door_groups["door_colours_base"],
                "door_colours_tall": door_groups["door_colours_tall"],
                "door_colours_island": door_groups["door_colours_island"],
                "door_colours_bar_back": door_groups["door_colours_bar_back"],
                "show_door_colours_overheads": bool(door_groups["door_colours_overheads"]) and (room_key_normalized == "kitchen" or has_explicit_overheads),
                "show_door_colours_base": bool(door_groups["door_colours_base"]),
                "show_door_colours_tall": True,
                "show_door_colours_island": room_key_normalized == "kitchen" and bool(door_groups["door_colours_island"]),
                "show_door_colours_bar_back": room_key_normalized == "kitchen" and bool(door_groups["door_colours_bar_back"]),
                "toe_kick": _display_value(row.get("toe_kick", [])),
                "bulkheads": _display_value(row.get("bulkheads", [])),
                "handles": _display_value(row.get("handles", [])),
                "floating_shelf": _display_value(row.get("floating_shelf", "")),
                "led": "Yes" if _display_value(row.get("led", "")) else "",
                "accessories": _string_list(row.get("accessories", [])),
                "other_items": [
                    {
                        "label": _display_value(item.get("label", "")),
                        "value": _display_value(item.get("value", "")),
                    }
                    for item in row.get("other_items", [])
                    if isinstance(item, dict) and _display_value(item.get("label", "")) and _display_value(item.get("value", ""))
                ],
                "sink_info": _display_value(row.get("sink_info", "")),
                "basin_info": _display_value(row.get("basin_info", "")),
                "tap_info": _display_value(row.get("tap_info", "")),
                "drawers_soft_close": _normalize_soft_close_display(row.get("drawers_soft_close", ""), "drawer"),
                "hinges_soft_close": _normalize_soft_close_display(row.get("hinges_soft_close", ""), "hinge"),
                "splashback": _display_value(row.get("splashback", "")),
                "flooring": _display_value(row.get("flooring", "")),
                "source_file": _display_value(row.get("source_file", "")),
                "page_refs": _display_value(row.get("page_refs", "")),
                "evidence_snippet": _display_value(row.get("evidence_snippet", "")),
                "confidence": _display_value(row.get("confidence", "")),
            }
        )
    return rows


def _flatten_special_sections(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("special_sections", []):
        if not isinstance(row, dict):
            continue
        fields = row.get("fields") or {}
        field_rows = []
        if isinstance(fields, dict):
            for key, value in fields.items():
                text = _display_value(value)
                if text:
                    field_rows.append({"key": _display_value(key), "value": text})
        rows.append(
            {
                "section_key": _display_value(row.get("section_key", "")),
                "original_section_label": _display_value(row.get("original_section_label", "")),
                "fields": field_rows,
                "source_file": _display_value(row.get("source_file", "")),
                "page_refs": _display_value(row.get("page_refs", "")),
                "evidence_snippet": _display_value(row.get("evidence_snippet", "")),
                "confidence": _display_value(row.get("confidence", "")),
            }
        )
    return rows


def _flatten_appliances(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("appliances", []):
        if not isinstance(row, dict):
            continue
        if _is_plumbing_appliance_row(row):
            continue
        rows.append(
            {
                "appliance_type": _display_value(row.get("appliance_type", "")),
                "make": _display_value(row.get("make", "")),
                "model_no": _display_value(row.get("model_no", "")),
                "product_url": _display_value(row.get("product_url", "") or row.get("website_url", "")),
                "website_url": _display_value(row.get("product_url", "") or row.get("website_url", "")),
                "overall_size": _display_value(row.get("overall_size", "")),
                "source_file": _display_value(row.get("source_file", "")),
                "page_refs": _display_value(row.get("page_refs", "")),
                "evidence_snippet": _display_value(row.get("evidence_snippet", "")),
                "confidence": _display_value(row.get("confidence", "")),
            }
        )
    return rows


def _flatten_others(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    others = snapshot.get("others") or {}
    if isinstance(others, dict):
        for key, value in others.items():
            rows.append({"key": _display_value(key), "value": _display_value(value)})
        return rows
    if others:
        rows.append({"key": "notes", "value": _display_value(others)})
    return rows


def _display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        parts = [_display_value(item) for item in value]
        return " | ".join(part for part in parts if part)
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def _split_room_door_groups(row: dict[str, Any]) -> dict[str, str]:
    room_key_normalized = parsing.normalize_room_key(_display_value(row.get("room_key", "")))
    has_explicit_overheads = bool(row.get("has_explicit_overheads", False))
    derived = (
        parsing._blank_door_group_values()
        if parsing._has_explicit_door_group_markers(row)
        else parsing._split_door_colour_groups(parsing._coerce_string_list(row.get("door_panel_colours", [])))
    )
    if room_key_normalized != "kitchen":
        derived["door_colours_island"] = ""
        derived["door_colours_bar_back"] = ""
        if not has_explicit_overheads:
            derived["door_colours_overheads"] = ""
    overheads = parsing._merge_clean_group_text(row.get("door_colours_overheads", ""), derived["door_colours_overheads"], cleaner=parsing._clean_door_colour_value)
    base = parsing._merge_clean_group_text(row.get("door_colours_base", ""), derived["door_colours_base"], cleaner=parsing._clean_door_colour_value)
    tall = parsing._merge_clean_group_text(row.get("door_colours_tall", ""), derived["door_colours_tall"], cleaner=parsing._clean_door_colour_value)
    if room_key_normalized != "kitchen" and not has_explicit_overheads and overheads:
        base = parsing._merge_clean_group_text(base, overheads, cleaner=parsing._clean_door_colour_value)
        overheads = ""
    island = parsing._merge_clean_group_text(row.get("door_colours_island", ""), derived["door_colours_island"], cleaner=parsing._clean_door_colour_value) if room_key_normalized == "kitchen" else ""
    bar_back = parsing._merge_clean_group_text(row.get("door_colours_bar_back", ""), derived["door_colours_bar_back"], cleaner=parsing._clean_door_colour_value) if room_key_normalized == "kitchen" else ""
    return {
        "door_colours_overheads": overheads,
        "door_colours_base": base,
        "door_colours_tall": tall,
        "door_colours_island": island,
        "door_colours_bar_back": bar_back,
    }


def _split_room_benchtops(row: dict[str, Any]) -> dict[str, str]:
    entries = parsing._coerce_string_list(row.get("bench_tops", []))
    grouped = parsing._split_benchtop_groups(entries)
    room_key_normalized = parsing.normalize_room_key(_display_value(row.get("room_key", "")))
    wall_run = _merge_display_text(_display_value(row.get("bench_tops_wall_run", "")), grouped["bench_tops_wall_run"])
    island = _merge_display_text(_display_value(row.get("bench_tops_island", "")), grouped["bench_tops_island"])
    other_candidates = _split_material_values(_merge_display_text(_display_value(row.get("bench_tops_other", "")), grouped["bench_tops_other"]))
    suppressed = {value.lower() for value in (wall_run, island) if value} if room_key_normalized == "kitchen" else set()
    other = " | ".join(value for value in other_candidates if value.lower() not in suppressed)
    return {
        "bench_tops_wall_run": wall_run,
        "bench_tops_island": island,
        "bench_tops_other": other,
    }


def _merge_display_text(left: str, right: str) -> str:
    values: list[str] = []
    for candidate in (left, right):
        text = _display_value(candidate)
        if text and text not in values:
            values.append(text)
    return " | ".join(values)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _display_value(item))]
    text = _display_value(value)
    return [text] if text else []


def _source_document_rows(value: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not isinstance(value, (list, tuple)):
        value = [value] if value else []
    for item in value:
        if isinstance(item, dict):
            rows.append(
                {
                    "role": _display_value(item.get("role", "")),
                    "file_name": _display_value(item.get("file_name", "")),
                    "page_count": _display_value(item.get("page_count", "")),
                    "room_role": _display_value(item.get("room_role", "")),
                }
            )
            continue
        text = _display_value(item)
        if text:
            rows.append({"role": "", "file_name": text, "page_count": "", "room_role": ""})
    return rows


def _review_payload_from_form(base: dict[str, Any], form: Any) -> dict[str, Any]:
    rooms: list[dict[str, Any]] = []
    room_count = int(form.get("room_count", 0) or 0)
    base_rooms = [row for row in base.get("rooms", []) if isinstance(row, dict)]
    for index in range(room_count):
        room_payload = dict(base_rooms[index]) if index < len(base_rooms) else {}
        room_payload.update(
            {
                "room_key": str(form.get(f"room_key_{index}", "")),
                "original_room_label": str(form.get(f"original_room_label_{index}", "")),
                "bench_tops": _split_pipe(str(form.get(f"bench_tops_{index}", ""))),
                "door_panel_colours": _split_pipe(str(form.get(f"door_panel_colours_{index}", ""))),
                "door_colours_tall": str(form.get(f"door_colours_tall_{index}", "")),
                "toe_kick": _split_pipe(str(form.get(f"toe_kick_{index}", ""))),
                "bulkheads": _split_pipe(str(form.get(f"bulkheads_{index}", ""))),
                "handles": _split_pipe(str(form.get(f"handles_{index}", ""))),
                "drawers_soft_close": _normalize_soft_close_display(form.get(f"drawers_soft_close_{index}", ""), "drawer"),
                "hinges_soft_close": _normalize_soft_close_display(form.get(f"hinges_soft_close_{index}", ""), "hinge"),
                "splashback": str(form.get(f"splashback_{index}", "")),
                "flooring": str(form.get(f"flooring_{index}", "")),
                "source_file": str(form.get(f"source_file_{index}", "")),
                "page_refs": str(form.get(f"page_refs_{index}", "")),
                "evidence_snippet": str(form.get(f"evidence_snippet_{index}", "")),
                "confidence": _safe_float(form.get(f"confidence_{index}", "")),
            }
        )
        rooms.append(room_payload)

    appliances: list[dict[str, Any]] = []
    appliance_count = int(form.get("appliance_count", 0) or 0)
    base_appliances = [row for row in base.get("appliances", []) if isinstance(row, dict) and not _is_plumbing_appliance_row(row)]
    for index in range(appliance_count):
        appliance_payload = dict(base_appliances[index]) if index < len(base_appliances) else {}
        appliance_payload.update(
            {
                "appliance_type": str(form.get(f"appliance_type_{index}", "")),
                "make": str(form.get(f"make_{index}", "")),
                "model_no": str(form.get(f"model_no_{index}", "")),
                "product_url": str(form.get(f"product_url_{index}", "") or form.get(f"website_url_{index}", "")),
                "spec_url": str(form.get(f"spec_url_{index}", "")),
                "manual_url": str(form.get(f"manual_url_{index}", "")),
                "website_url": str(form.get(f"product_url_{index}", "") or form.get(f"website_url_{index}", "")),
                "overall_size": str(form.get(f"overall_size_{index}", "")),
                "source_file": str(form.get(f"appliance_source_file_{index}", "")),
                "page_refs": str(form.get(f"appliance_page_refs_{index}", "")),
                "evidence_snippet": str(form.get(f"appliance_evidence_snippet_{index}", "")),
                "confidence": _safe_float(form.get(f"appliance_confidence_{index}", "")),
            }
        )
        appliances.append(appliance_payload)

    return {
        "job_no": base.get("job_no", ""),
        "builder_name": base.get("builder_name", ""),
        "source_kind": base.get("source_kind", "spec"),
        "generated_at": utc_now_iso(),
        "rooms": rooms,
        "special_sections": list(base.get("special_sections", [])),
        "appliances": appliances,
        "others": {
            "flooring_notes": str(form.get("others_flooring_notes", "")),
            "splashback_notes": str(form.get("others_splashback_notes", "")),
            "manual_notes": str(form.get("others_manual_notes", "")),
        },
        "analysis": dict(base.get("analysis") or _analysis_from_snapshot(base)),
        "warnings": list(base.get("warnings", [])),
        "source_documents": list(base.get("source_documents", [])),
    }


def _split_pipe(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _blank_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_no": job["job_no"],
        "builder_name": job["builder_name"],
        "source_kind": "spec",
        "generated_at": utc_now_iso(),
        "site_address": "",
        "analysis": _analysis_from_snapshot(None),
        "rooms": [],
        "special_sections": [],
        "appliances": [],
        "others": {"flooring_notes": "", "splashback_notes": "", "manual_notes": ""},
        "warnings": [],
        "source_documents": [],
    }


def _job_site_address(raw_snapshot: dict[str, Any] | None, drawing_snapshot: dict[str, Any] | None) -> str:
    for snapshot in (raw_snapshot, drawing_snapshot):
        if isinstance(snapshot, dict):
            value = parsing.normalize_space(str(snapshot.get("site_address", "") or ""))
            if value:
                return value
    return ""


async def _persist_pdf_qa_action(request: Request, job_id: int, mode: str) -> RedirectResponse:
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    form = await request.form()
    verify_csrf(request, str(form.get("csrf_token", "")))
    job = store.get_job(job_id)
    if not job:
        _set_flash(request, "error", "Job not found.")
        return RedirectResponse("/jobs", status_code=303)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    if not raw_snapshot_row:
        _set_flash(request, "error", "No raw spec snapshot is available for PDF QA.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    verification = store.get_job_snapshot_verification(job_id, "raw_spec")
    if not verification:
        _set_flash(request, "error", "No PDF QA checklist is available for the current raw spec snapshot.")
        return RedirectResponse(f"/jobs/{job_id}", status_code=303)
    checklist, notes = _verification_payload_from_form(form)
    checked_by = current_user(request) or ""
    if mode == "mark_fail":
        saved = store.save_snapshot_verification(int(verification["snapshot_id"]), checklist, checked_by, notes, force_status="failed")
        _set_flash(request, "error", "PDF QA marked as failed.")
        return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)
    saved = store.save_snapshot_verification(int(verification["snapshot_id"]), checklist, checked_by, notes)
    status = str((saved or {}).get("status", "pending") or "pending")
    if mode == "mark_pass":
        if status != "passed":
            _set_flash(request, "error", "PDF QA cannot be marked as passed until every checklist item is Pass or N/A.")
        else:
            _set_flash(request, "success", "PDF QA passed. Formal exports are now unlocked.")
        return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)
    if status == "passed":
        _set_flash(request, "success", "PDF QA checklist saved and marked as passed.")
    elif status == "failed":
        _set_flash(request, "error", "PDF QA checklist saved with failed items.")
    else:
        _set_flash(request, "success", "PDF QA checklist saved.")
    return RedirectResponse(f"/jobs/{job_id}/pdf-qa", status_code=303)


def _verification_payload_from_form(form: Any) -> tuple[list[dict[str, Any]], str]:
    checklist: list[dict[str, Any]] = []
    item_count = int(form.get("item_count", 0) or 0)
    for index in range(item_count):
        checklist.append(
            {
                "section_type": str(form.get(f"section_type_{index}", "") or ""),
                "entity_label": str(form.get(f"entity_label_{index}", "") or ""),
                "field_name": str(form.get(f"field_name_{index}", "") or ""),
                "extracted_value": str(form.get(f"extracted_value_{index}", "") or ""),
                "source_page_refs": str(form.get(f"source_page_refs_{index}", "") or ""),
                "pdf_page_ref": str(form.get(f"pdf_page_ref_{index}", "") or ""),
                "status": str(form.get(f"status_{index}", "pending") or "pending"),
                "qa_note": str(form.get(f"qa_note_{index}", "") or ""),
            }
        )
    notes = str(form.get("notes", "") or "")
    return checklist, notes


def _verification_summary(verification: dict[str, Any] | None) -> dict[str, Any]:
    checklist = list((verification or {}).get("checklist", []) or [])
    counts = {"pass": 0, "fail": 0, "na": 0, "pending": 0}
    for item in checklist:
        status = str(item.get("status", "pending") or "pending").lower()
        if status not in counts:
            status = "pending"
        counts[status] += 1
    total = len(checklist)
    done = counts["pass"] + counts["na"]
    status = str((verification or {}).get("status", "pending") or "pending").lower()
    if status not in {"pending", "passed", "failed"}:
        status = "pending"
    return {
        "status": status,
        "status_label": {
            "pending": "Pending PDF QA",
            "passed": "PDF QA Passed",
            "failed": "PDF QA Failed",
        }.get(status, "Pending PDF QA"),
        "status_class": {
            "pending": "warning",
            "passed": "ready",
            "failed": "failed",
        }.get(status, "warning"),
        "checked_by": str((verification or {}).get("checked_by", "") or ""),
        "checked_at": _format_brisbane_time((verification or {}).get("checked_at", "")),
        "notes": str((verification or {}).get("notes", "") or ""),
        "counts": counts,
        "total": total,
        "done": done,
        "can_export": status == "passed",
    }


def _group_verification_items(checklist: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for item in checklist:
        if not isinstance(item, dict):
            continue
        key = (str(item.get("section_type", "") or ""), str(item.get("entity_label", "") or ""))
        group = lookup.get(key)
        if not group:
            group = {"section_type": key[0], "entity_label": key[1], "checklist_items": []}
            lookup[key] = group
            groups.append(group)
        group["checklist_items"].append(item)
    return groups


def _list_export_files(export_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not export_dir.exists():
        return rows
    for path in sorted(export_dir.glob("*"), key=lambda item: item.stat().st_mtime, reverse=True):
        if path.is_file():
            stat = path.stat()
            rows.append(
                {
                    "name": path.name,
                    "size_bytes": stat.st_size,
                    "modified": _format_brisbane_time(stat.st_mtime),
                }
            )
    return rows


def _run_file_role(run_kind: str) -> str | None:
    if run_kind == "spec":
        return "spec"
    if run_kind == "drawing":
        return "drawing"
    return None


def _present_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in runs:
        row = dict(run)
        row["kind_label"] = {"spec": "Spec Parse", "drawing": "Drawing Parse"}.get(str(run.get("run_kind", "")), str(run.get("run_kind", "")))
        row["status_label"] = {
            "queued": "Queued",
            "running": "Parsing",
            "succeeded": "Completed",
            "failed": "Failed",
        }.get(str(run.get("status", "")), str(run.get("status", "")))
        stage = str(run.get("stage", ""))
        row["stage_label"] = {
            "queued": "Waiting for worker",
            "starting": "Starting",
            "loading": "Loading files",
            "extracting": "Parsing",
            "heuristic": "Heuristic extraction",
            "vision_prepare": "Preparing vision",
            "vision_request": "Calling OpenAI Vision",
            "vision_apply": "Applying visual layout",
            "vision_fallback": "Vision fallback",
            "vision_skipped": "Vision skipped",
            "openai_prepare": "Preparing OpenAI",
            "openai_request": "Calling OpenAI",
            "openai_merge": "Merging AI result",
            "openai_fallback": "OpenAI fallback",
            "openai_skipped": "OpenAI skipped",
            "room_enrichment": "Assigning room fixtures",
            "clarendon_polish": "Clarendon polish",
            "official_model_lookup": "Official model lookup",
            "spec_manual_discovery": "Spec/manual discovery",
            "official_size_extraction": "Official size extraction",
            "saving": "Saving snapshot",
            "done": "Completed",
        }.get(stage, stage.replace("_", " ").title())
        row["message_display"] = str(run.get("message") or run.get("error_text") or "")
        if str(run.get("status", "")) == "queued" and not row["message_display"]:
            row["message_display"] = "Waiting for worker to start parsing."
        row["requested_at"] = _format_brisbane_time(run.get("requested_at", ""))
        row["finished_at"] = _format_brisbane_time(run.get("finished_at", ""))
        row["parser_strategy_label"] = (
            cleaning_rules.parser_strategy_label(row.get("parser_strategy", ""))
            if str(run.get("parser_strategy", "")).strip()
            else "Pending"
        )
        worker_pid = int(run.get("worker_pid", 0) or 0)
        app_build_id = str(run.get("app_build_id", "") or "")
        if worker_pid and app_build_id:
            row["worker_build_display"] = f"PID {worker_pid} | {app_build_id}"
        elif worker_pid:
            row["worker_build_display"] = f"PID {worker_pid}"
        elif app_build_id:
            row["worker_build_display"] = app_build_id
        else:
            row["worker_build_display"] = "Not claimed yet"
        row["duration_display"] = _run_duration_display(run)
        row["can_open_result"] = (
            str(run.get("run_kind", "")) == "spec"
            and str(run.get("status", "")) == "succeeded"
            and bool(str(run.get("result_json", "") or "").strip())
            and int(run.get("id", 0) or 0) > 0
            and int(run.get("job_id", 0) or 0) > 0
        )
        rows.append(row)
    return rows


def _historical_verification_summary() -> dict[str, Any]:
    return {
        "status": "pending",
        "status_label": "Historical Run (Read-only)",
        "status_class": "warning",
        "checked_by": "",
        "checked_at": "",
        "notes": "",
        "counts": {"pass": 0, "fail": 0, "na": 0, "pending": 0},
        "total": 0,
        "done": 0,
        "can_export": False,
    }


def _spec_list_template_response(
    request: Request,
    job: dict[str, Any],
    raw_snapshot: dict[str, Any] | None,
    raw_verification: dict[str, Any] | None,
    raw_extraction_duration: str,
    historical_run: dict[str, Any] | None = None,
):
    historical_view = historical_run is not None
    raw_verification_summary = (
        _historical_verification_summary() if historical_view else _verification_summary(raw_verification)
    )
    return templates.TemplateResponse(
        request,
        "spec_list.html",
        _context(
            request,
            f"Spec List {job['job_no']}",
            sidebar_collapsible=True,
            sidebar_default_hidden=True,
            job=job,
            job_site_address=_job_site_address(raw_snapshot, None),
            raw_snapshot=raw_snapshot,
            raw_generated_at=_format_brisbane_time((raw_snapshot or {}).get("generated_at", "")),
            raw_analysis=_analysis_from_snapshot(raw_snapshot),
            raw_verification=raw_verification,
            raw_verification_summary=raw_verification_summary,
            raw_extraction_duration=raw_extraction_duration,
            raw_spec_rooms=_flatten_rooms(raw_snapshot or {}),
            raw_special_sections=_flatten_special_sections(raw_snapshot or {}),
            raw_spec_appliances=_flatten_appliances(raw_snapshot or {}),
            raw_spec_others=_flatten_others(raw_snapshot or {}),
            raw_spec_warnings=_string_list((raw_snapshot or {}).get("warnings", [])),
            raw_source_documents=_source_document_rows((raw_snapshot or {}).get("source_documents", [])),
            material_summary=_build_material_summary(raw_snapshot or {}),
            historical_view=historical_view,
            historical_run=historical_run,
        ),
    )


def _is_plumbing_appliance_row(row: dict[str, Any]) -> bool:
    appliance_type = _display_value(row.get("appliance_type", "")).lower()
    return any(token in appliance_type for token in ("sink", "basin", "tap", "tub"))


def _analysis_from_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    analysis = dict((snapshot or {}).get("analysis") or {})
    mode = analysis.get("mode", "heuristic_only")
    parser_strategy = str(analysis.get("parser_strategy", "") or "")
    layout_mode = str(analysis.get("layout_mode", "") or "")
    return {
        "mode": mode,
        "label": {
            "heuristic_only": "Heuristic only",
            "openai_merged": "OpenAI merged",
            "openai_fallback": "OpenAI fallback",
        }.get(mode, mode.replace("_", " ").title()),
        "parser_strategy": parser_strategy,
        "parser_strategy_label": cleaning_rules.parser_strategy_label(parser_strategy) if parser_strategy else "Not recorded",
        "layout_attempted": bool(analysis.get("layout_attempted", False)),
        "layout_succeeded": bool(analysis.get("layout_succeeded", False)),
        "layout_mode": layout_mode,
        "layout_provider": str(analysis.get("layout_provider", "") or ""),
        "layout_mode_label": {
            "lightweight": "Lightweight structure",
            "docling": "Docling structure",
            "mixed": "Structure-first mixed",
            "heavy_vision": "High-precision vision",
        }.get(layout_mode, layout_mode.replace("_", " ").title() if layout_mode else "Not recorded"),
        "layout_pages": [int(item) for item in analysis.get("layout_pages", []) if str(item).strip().isdigit()],
        "heavy_vision_pages": [int(item) for item in analysis.get("heavy_vision_pages", []) if str(item).strip().isdigit()],
        "layout_note": analysis.get("layout_note", ""),
        "docling_attempted": bool(analysis.get("docling_attempted", False)),
        "docling_succeeded": bool(analysis.get("docling_succeeded", False)),
        "docling_pages": [int(item) for item in analysis.get("docling_pages", []) if str(item).strip().isdigit()],
        "docling_note": analysis.get("docling_note", ""),
        "openai_attempted": bool(analysis.get("openai_attempted", False)),
        "openai_succeeded": bool(analysis.get("openai_succeeded", False)),
        "openai_model": analysis.get("openai_model", ""),
        "vision_attempted": bool(analysis.get("vision_attempted", False)),
        "vision_succeeded": bool(analysis.get("vision_succeeded", False)),
        "vision_pages": [int(item) for item in analysis.get("vision_pages", []) if str(item).strip().isdigit()],
        "vision_page_count": int(analysis.get("vision_page_count", 0) or 0),
        "vision_note": analysis.get("vision_note", ""),
        "note": analysis.get("note", ""),
        "worker_pid": int(analysis.get("worker_pid", 0) or 0),
        "app_build_id": analysis.get("app_build_id", ""),
        "room_master_file": str(analysis.get("room_master_file", "") or ""),
        "room_master_reason": str(analysis.get("room_master_reason", "") or ""),
        "supplement_files": [str(item) for item in analysis.get("supplement_files", []) if item],
        "ignored_room_like_lines_count": int(analysis.get("ignored_room_like_lines_count", 0) or 0),
    }


def _normalize_soft_close_display(value: Any, keyword: str) -> str:
    return parsing.normalize_soft_close_value(value, keyword=keyword) or parsing.normalize_soft_close_value(value)


def _build_material_summary(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rooms = _flatten_rooms(snapshot)
    return {
        "door_colours": _material_bucket_with_rooms(
            "Door Colours",
            rooms,
            lambda row: [
                row.get("door_colours_overheads", ""),
                row.get("door_colours_base", ""),
                row.get("door_colours_tall", ""),
                row.get("door_colours_island", ""),
                row.get("door_colours_bar_back", ""),
            ] if any(
                _display_value(value)
                for value in (
                    row.get("door_colours_overheads", ""),
                    row.get("door_colours_base", ""),
                    row.get("door_colours_tall", ""),
                    row.get("door_colours_island", ""),
                    row.get("door_colours_bar_back", ""),
                )
            ) else [row.get("door_panel_colours", "")],
            _normalize_door_colour_summary_value,
        ),
        "handles": _material_bucket_with_rooms(
            "Handles",
            rooms,
            lambda row: [row.get("handles", "")],
            _normalize_handle_summary_value,
        ),
        "bench_tops": _material_bucket_with_rooms(
            "Bench Tops",
            rooms,
            lambda row: [
                row.get("bench_tops_wall_run", ""),
                row.get("bench_tops_island", ""),
                row.get("bench_tops_other", ""),
                row.get("floating_shelf", ""),
            ],
            _normalize_benchtop_summary_value,
        ),
    }


def _material_bucket_with_rooms(
    label: str,
    rooms: list[dict[str, Any]],
    value_getter: Any,
    normalizer: Any,
) -> dict[str, Any]:
    ordered_entries: list[dict[str, Any]] = []
    lookup: dict[str, dict[str, Any]] = {}
    for room in rooms:
        room_label = _display_value(room.get("original_room_label", "")) or _display_value(room.get("room_key", "")) or "Room"
        for value in value_getter(room):
            for item in _split_material_values(value):
                normalized = normalizer(item)
                key = normalized.lower()
                if not normalized:
                    continue
                entry = lookup.get(key)
                if not entry:
                    entry = {"text": normalized, "rooms": []}
                    lookup[key] = entry
                    ordered_entries.append(entry)
                if room_label not in entry["rooms"]:
                    entry["rooms"].append(room_label)
    for entry in ordered_entries:
        rooms_display = " / ".join(entry["rooms"])
        entry["display_text"] = f"{entry['text']} ({rooms_display})" if rooms_display else entry["text"]
    return {"label": label, "count": len(ordered_entries), "entries": ordered_entries}


def _split_material_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            result.extend(_split_material_values(item))
        return result
    text = _display_value(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"\s*\|\s*", text) if part.strip()]


def _normalize_door_colour_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    text = re.sub(r"\([^)]*(upper|overhead|base|island|bar back|cabinet|panel|run|shelf)[^)]*\)", "", text, flags=re.IGNORECASE)
    text = _strip_summary_location_tail(
        text,
        (
            r"(?i)\b(?:plain glass\s+)?display cabinet\b.*$",
            r"(?i)\bto tall open shelves\b.*$",
            r"(?i)\b(?:to|for)\b[^|;]*\b(upper|overhead|base|island|bar back|cabinetry|run|shelf|shelves)\b.*$",
        ),
    )
    return text


def _normalize_handle_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    text = re.sub(r"\([^)]*(location|up/down|left/right|door|drawer|centre|center)[^)]*\)", "", text, flags=re.IGNORECASE)
    return _strip_summary_location_tail(
        text,
        (
            r"(?i)\bdoor location\b.*$",
            r"(?i)\bdown\s*drawer location\b.*$",
            r"(?i)\bdrawer location\b.*$",
            r"(?i)\b(?:centre|center)\s+to\s+profile\b.*$",
            r"(?i)\bto\s+(?:base|upper|overhead|base cabinets?|upper cabinets?|cabinet locations?)\b.*$",
        ),
    )


def _normalize_benchtop_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    text = re.sub(r"(?i)^back benchtops?\s*", "", text)
    text = re.sub(r"(?i)^wall run bench top\s*", "", text)
    text = re.sub(r"(?i)^island bench top\s*", "", text)
    text = re.sub(r"(?i)^island benchtop\s*", "", text)
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
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" -;,/")


def _clean_summary_segments(text: str, location_tokens: set[str]) -> str:
    cleaned_segments: list[str] = []
    for segment in [part.strip(" -;,") for part in re.split(r"\s+\|\s+|\s+-\s+", text) if part.strip(" -;,")]:
        lowered = segment.lower()
        if any(token in lowered for token in location_tokens) and len(cleaned_segments) >= 1:
            continue
        cleaned_segments.append(segment)
    result = " - ".join(cleaned_segments) if cleaned_segments else parsing.normalize_space(text)
    result = re.sub(r"\s{2,}", " ", result)
    return result.strip(" -;,")


def _strip_summary_location_tail(text: str, patterns: tuple[str, ...]) -> str:
    normalized = parsing.normalize_space(text)
    if not normalized:
        return ""
    end_index = len(normalized)
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if match and match.start() < end_index:
            end_index = match.start()
    cleaned = parsing.normalize_space(normalized[:end_index])
    return cleaned.strip(" -;,")
