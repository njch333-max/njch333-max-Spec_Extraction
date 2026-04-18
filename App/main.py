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
    sort = _normalize_job_sort(request.query_params.get("sort", "created_desc"))
    jobs = _present_jobs(store.list_jobs(query, sort))
    builders = store.list_builders()
    return templates.TemplateResponse(
        request,
        "jobs.html",
        _context(request, "Jobs", jobs=jobs, builders=builders, job_query=query, job_sort=sort),
    )


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
        item["updated_at"] = _format_brisbane_time(item.get("updated_at", ""))
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


def _normalize_job_sort(value: Any) -> str:
    text = str(value or "created_desc").strip().lower()
    if text in {"created_desc", "updated_desc"}:
        return text
    return "created_desc"


def _present_files(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    presented: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["uploaded_at"] = _format_brisbane_time(item.get("uploaded_at", ""))
        presented.append(item)
    return presented


def _flatten_rooms(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    if parsing._is_imperial_builder(str(snapshot.get("builder_name", "") or "")) and any(
        isinstance(row, dict) and isinstance(row.get("material_rows", []), list) and row.get("material_rows")
        for row in snapshot.get("rooms", [])
        if isinstance(row, dict)
    ):
        return _flatten_imperial_rooms(snapshot)
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
                "feature_colour": door_groups["feature_colour"],
                "show_door_colours_overheads": bool(door_groups["door_colours_overheads"]) and (room_key_normalized == "kitchen" or has_explicit_overheads),
                "show_door_colours_base": bool(door_groups["door_colours_base"]),
                "show_door_colours_tall": True,
                "show_door_colours_island": room_key_normalized == "kitchen" and bool(door_groups["door_colours_island"]),
                "show_door_colours_bar_back": room_key_normalized == "kitchen" and bool(door_groups["door_colours_bar_back"]),
                "show_feature_colour": bool(door_groups["feature_colour"]),
                "toe_kick": _display_value(row.get("toe_kick", [])),
                "bulkheads": _display_value(row.get("bulkheads", [])),
                "handles": _display_value(row.get("handles", [])),
                "floating_shelf": _display_value(row.get("floating_shelf", "")),
                "shelf": _display_value(row.get("shelf", "")),
                "led": "Yes" if str(_display_value(row.get("led", ""))).strip().lower() == "yes" else "No",
                "show_led": str(_display_value(row.get("led", ""))).strip().lower() == "yes",
                "led_note": _display_value(row.get("led_note", "")),
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
    return _sort_room_rows_by_priority(rows)


def _flatten_imperial_rooms(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    rooms = [row for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    ordered_rooms = sorted(
        rooms,
        key=lambda row: (
            int(row.get("room_order", 0) or 0),
            _display_value(row.get("original_room_label", "")) or _display_value(row.get("room_key", "")),
        ),
    )
    for row in ordered_rooms:
        material_rows = _flatten_imperial_material_rows(row)
        flattened.append(
            {
                "is_imperial_raw_rows": True,
                "room_key": _display_value(row.get("room_key", "")),
                "original_room_label": _display_value(row.get("original_room_label", "")),
                "room_order": int(row.get("room_order", 0) or 0),
                "material_rows": material_rows,
                "sink_info": _display_value(row.get("sink_info", "")) or _display_value(row.get("basin_info", "")),
                "drawers_soft_close": _normalize_soft_close_display(row.get("drawers_soft_close", ""), "drawer"),
                "hinges_soft_close": _normalize_soft_close_display(row.get("hinges_soft_close", ""), "hinge"),
                "flooring": _display_value(row.get("flooring", "")),
                "source_file": _display_value(row.get("source_file", "")),
                "page_refs": _display_value(row.get("page_refs", "")),
                "evidence_snippet": _display_value(row.get("evidence_snippet", "")),
                "confidence": _display_value(row.get("confidence", "")),
            }
        )
    return flattened


def _flatten_imperial_material_rows(room: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    material_rows = room.get("material_rows", [])
    if not isinstance(material_rows, list):
        return rows
    def _sort_key(item: dict[str, Any]) -> tuple[int, float, float, int, int]:
        provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
        visual_sort_key = provenance.get("visual_sort_key", [])
        if isinstance(visual_sort_key, list) and len(visual_sort_key) >= 4:
            try:
                return (
                    int(item.get("page_no", 0) or 0),
                    float(visual_sort_key[0] or 0.0),
                    float(visual_sort_key[1] or 0.0),
                    int(visual_sort_key[2] or 0),
                    int(visual_sort_key[3] or 0),
                )
            except (TypeError, ValueError):
                pass
        return (
            int(item.get("page_no", 0) or 0),
            float(int(item.get("row_order", 0) or 0)),
            0.0,
            int(item.get("row_order", 0) or 0),
            0,
        )
    ordered_rows = sorted(
        [item for item in material_rows if isinstance(item, dict)],
        key=_sort_key,
    )
    generic_handle_row_candidates: list[list[str]] = []
    for item in ordered_rows:
        item_tags = {
            _display_value(tag).lower()
            for tag in (item.get("tags", []) or [])
            if _display_value(tag)
        }
        title = _display_imperial_material_row_title(item)
        if "handles" not in item_tags or title.upper() != "HANDLES":
            continue
        candidates = _imperial_material_row_handle_summary_candidates(item)
        if candidates:
            generic_handle_row_candidates.append(candidates)
    for item in ordered_rows:
        title = _display_imperial_material_row_title(item)
        if re.fullmatch(r"(?i)Hinges\s*&\s*Drawer\s*Runners:?", title) or re.fullmatch(
            r"(?i)Floor\s*Type\s*&\s*Kick\s*refacing\s*required:?",
            title,
        ):
            continue
        item_tags = {
            _display_value(tag).lower()
            for tag in (item.get("tags", []) or [])
            if _display_value(tag)
        }
        if (
            "handles" in item_tags
            and generic_handle_row_candidates
            and re.fullmatch(r"(?i)(?:DRAWERS?|DOORS?|HANDLE DESCRIPTION)", title)
        ):
            row_candidates = _imperial_material_row_handle_summary_candidates(item)
            if row_candidates and any(
                all(
                    any(_imperial_summary_values_equivalent("handles", candidate, anchor) for anchor in anchor_candidates)
                    for candidate in row_candidates
                )
                for anchor_candidates in generic_handle_row_candidates
            ):
                continue
        supplier = _display_value(item.get("supplier", ""))
        description = _display_value(item.get("specs_or_description", ""))
        notes = _display_value(item.get("notes", ""))
        supplier, notes = parsing._imperial_split_material_supplier_notes(supplier, notes)
        supplier, description, notes = parsing._imperial_repair_hard_boundary_polluted_material_row(
            title,
            supplier,
            description,
            notes,
        )
        supplier, description, notes = parsing._imperial_normalize_benchtop_fields(
            title,
            supplier,
            description,
            notes,
        )
        supplier, description, notes = parsing._imperial_normalize_cabinetry_colour_fields(
            title,
            supplier,
            description,
            notes,
        )
        if supplier:
            description = parsing._imperial_strip_supplier_duplication(supplier, description)
            notes = parsing._imperial_strip_supplier_duplication(supplier, notes)
        if notes and description.upper().endswith(notes.upper()):
            description = _display_value(description[: -len(notes)]).strip(" -|;,")
        value = " - ".join(part for part in (supplier, description, notes) if part)
        display_lines = [
            _display_value(line)
            for line in parsing._imperial_material_row_display_lines_for_view(item)
            if _display_value(line)
        ]
        display_value = "\n".join(display_lines) if display_lines else (_display_value(parsing._imperial_material_row_display_value_for_view(item)) or value)
        handle_fallback_sources = (
            _imperial_handle_summary_fallback_sources(item)
            if _imperial_summary_bucket_key_for_item(item) == "handles"
            else []
        )
        if not title or (not value and not display_value and not handle_fallback_sources):
            continue
        tags = [
            _display_value(tag)
            for tag in (item.get("tags", []) or [])
            if _display_value(tag)
        ]
        effective_needs_review = _imperial_material_row_needs_review(item)
        issue_types = [
            _display_value(issue.get("issue_type", ""))
            for issue in (item.get("issues", []) or [])
            if isinstance(issue, dict) and _display_value(issue.get("issue_type", ""))
        ]
        for issue_type in item.get("issue_types", []) or []:
            normalized_issue_type = _display_value(issue_type)
            if normalized_issue_type and normalized_issue_type not in issue_types:
                issue_types.append(normalized_issue_type)
        repair_verdicts = [entry for entry in (item.get("repair_verdicts", []) or []) if isinstance(entry, dict)]
        accepted_repair_types = [
            _display_value(entry.get("issue_type", ""))
            for entry in repair_verdicts
            if _display_value(entry.get("status", "")).lower() == "accepted"
            and _display_value(entry.get("issue_type", ""))
        ]
        for issue_type in item.get("accepted_repair_types", []) or []:
            normalized_issue_type = _display_value(issue_type)
            if normalized_issue_type and normalized_issue_type not in accepted_repair_types:
                accepted_repair_types.append(normalized_issue_type)
        pending_repair_types = [
            _display_value(entry.get("issue_type", ""))
            for entry in repair_verdicts
            if _display_value(entry.get("status", "")).lower() in {"needs_review", "pending"}
            and _display_value(entry.get("issue_type", ""))
        ]
        for issue_type in item.get("pending_repair_types", []) or []:
            normalized_issue_type = _display_value(issue_type)
            if normalized_issue_type and normalized_issue_type not in pending_repair_types:
                pending_repair_types.append(normalized_issue_type)
        revalidation_issue_types = [
            _display_value(issue.get("related_issue_type", "") or issue.get("issue_type", ""))
            for issue in (item.get("revalidation_issues", []) or [])
            if isinstance(issue, dict)
            and _display_value(issue.get("related_issue_type", "") or issue.get("issue_type", ""))
        ]
        for issue_type in item.get("revalidation_issue_types", []) or []:
            normalized_issue_type = _display_value(issue_type)
            if normalized_issue_type and normalized_issue_type not in revalidation_issue_types:
                revalidation_issue_types.append(normalized_issue_type)
        review_display_status = _imperial_material_row_review_display_status(item)
        rows.append(
            {
                "title": title,
                "value": value,
                "display_value": display_value,
                "display_lines": display_lines,
                "supplier": supplier,
                "specs_or_description": description,
                "notes": notes,
                "tags": tags,
                "page_no": int(item.get("page_no", 0) or 0),
                "row_order": int(item.get("row_order", 0) or 0),
                "confidence": _display_value(item.get("confidence", "")),
                "needs_review": effective_needs_review,
                "revalidation_status": _display_value(item.get("revalidation_status", "")),
                "review_display_status": review_display_status,
                "issue_types": issue_types,
                "accepted_repair_types": accepted_repair_types,
                "pending_repair_types": pending_repair_types,
                "revalidation_issue_types": revalidation_issue_types,
                "repair_log_count": len([entry for entry in (item.get("repair_log", []) or []) if isinstance(entry, dict)]),
                "provenance": item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {},
                "handle_subitems": item.get("handle_subitems", []) if isinstance(item.get("handle_subitems", []), list) else [],
            }
        )
    return rows


def _imperial_material_row_needs_review(item: dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    if bool(item.get("needs_review", False)):
        return True
    revalidation_status = _display_value(item.get("revalidation_status", "")).lower()
    return revalidation_status in {"needs_review", "failed", "pending"}


def _imperial_material_row_accepted_issue_types(item: dict[str, Any]) -> set[str]:
    if not isinstance(item, dict):
        return set()
    accepted_issue_types: set[str] = set()
    for verdict in item.get("repair_verdicts", []) or []:
        if not isinstance(verdict, dict):
            continue
        if _display_value(verdict.get("status", "")).lower() != "accepted":
            continue
        revalidation_status = _display_value(verdict.get("revalidation_status", "")).lower()
        if revalidation_status and revalidation_status != "passed":
            continue
        issue_type = _display_value(verdict.get("issue_type", "")).lower()
        if issue_type:
            accepted_issue_types.add(issue_type)
    return accepted_issue_types


def _imperial_material_row_review_issue_types(item: dict[str, Any]) -> set[str]:
    if not isinstance(item, dict):
        return set()
    issue_types: set[str] = set()
    for issue in item.get("issues", []) or []:
        if isinstance(issue, dict):
            issue_type = _display_value(issue.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    for verdict in item.get("repair_verdicts", []) or []:
        if isinstance(verdict, dict) and _display_value(verdict.get("status", "")).lower() in {"needs_review", "pending"}:
            issue_type = _display_value(verdict.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    for issue in item.get("revalidation_issues", []) or []:
        if isinstance(issue, dict):
            issue_type = _display_value(issue.get("related_issue_type", "") or issue.get("issue_type", "")).lower()
            if issue_type:
                issue_types.add(issue_type)
    for key in ("issue_types", "pending_repair_types", "revalidation_issue_types"):
        for issue_type in item.get(key, []) or []:
            normalized = _display_value(issue_type).lower()
            if normalized:
                issue_types.add(normalized)
    issue_types -= _imperial_material_row_accepted_issue_types(item)
    return issue_types


def _imperial_material_row_is_only_row_order_review(item: dict[str, Any]) -> bool:
    issue_types = _imperial_material_row_review_issue_types(item)
    return bool(issue_types) and issue_types <= {"row_order_drift"}


def _imperial_material_row_is_door_colour_summary_review_fallback(item: dict[str, Any]) -> bool:
    issue_types = _imperial_material_row_review_issue_types(item)
    if not issue_types or not issue_types <= {
        "row_order_drift",
        "cross_row_spillover",
        "supplier_notes_misassignment",
        "label_contamination",
    }:
        return False
    title = _display_value(item.get("title", ""))
    value = _display_value(item.get("value", ""))
    if not title or not value:
        return False
    if "label_contamination" in issue_types:
        provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
        raw_title = _display_value(provenance.get("raw_area_or_item", ""))
        raw_upper = raw_title.upper()
        title_upper = title.upper()
        safe_label_contamination = False
        if cleaned_label := parsing._imperial_clean_material_row_label_text(raw_title or title):
            safe_label_contamination = cleaned_label == title and bool(
                re.search(r"(?i)\b(?:cabinetry\s+colour|frame|bar\s+back|panel)\b", title)
            )
        if not safe_label_contamination and raw_upper.startswith(title_upper):
            suffix = parsing.normalize_space(raw_title[len(title) :]).strip(" -|;,")
            safe_label_contamination = bool(
                suffix
                and re.fullmatch(
                    r"(?i)(?:incl(?:uding)?|including|front(?:,\s*back(?:\s+and\s+sides?)?)?|back(?:\s+and\s+sides?)?|sides?|open shelving|tall cabinetry|open shelving\s*&\s*tall cabinetry|and\s+open shelving|drawers and open shelving|glass doors only)(?:\s+.*)?",
                    suffix,
                )
                and re.search(r"(?i)\b(?:cabinetry\s+colour|frame|bar\s+back|panel)\b", title)
            )
        if not safe_label_contamination and not re.search(r"(?i)\bdoors?\b", title):
            return False
    cleaned_value = parsing._imperial_clean_cabinetry_colour_description(title, value)
    cleaned_notes = parsing._imperial_clean_cabinetry_colour_notes(title, _display_value(item.get("notes", "")))
    return bool(
        cleaned_value
        and cleaned_value == parsing.normalize_space(value)
        and cleaned_notes == _display_value(item.get("notes", ""))
    )


def _imperial_material_row_is_handle_summary_review_fallback(item: dict[str, Any]) -> bool:
    issue_types = set(_imperial_material_row_review_issue_types(item))
    if "label_contamination" in issue_types:
        provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
        raw_area_or_item = _display_value(provenance.get("raw_area_or_item", ""))
        title = _display_value(item.get("title", ""))
        if parsing._imperial_handle_label_contamination_is_safe(raw_area_or_item, title):
            issue_types.discard("label_contamination")
        else:
            preview_sources = [
                _display_value(line)
                for line in item.get("display_lines", []) or []
                if _display_value(line)
            ]
            preview_value = _display_value(item.get("display_value", "")) or _display_value(item.get("value", ""))
            if preview_value:
                preview_sources.append(preview_value)
            preview_candidates: list[str] = []
            for source in preview_sources:
                for candidate in _imperial_summary_values_for_bucket(
                    "handles",
                    source,
                    _normalize_imperial_handle_summary_value,
                    supplier=_display_value(item.get("supplier", "")),
                ):
                    if candidate and candidate not in preview_candidates:
                        preview_candidates.append(candidate)
            if preview_candidates:
                issue_types.discard("label_contamination")
    return bool(issue_types) and issue_types <= {
        "row_order_drift",
        "handle_block_over_split",
        "cross_row_spillover",
        "supplier_notes_misassignment",
    }


def _imperial_handle_summary_fallback_sources(item: dict[str, Any]) -> list[str]:
    if not isinstance(item, dict):
        return []
    provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
    sources: list[str] = []
    for key in ("layout_value_text", "page_text_handle_block"):
        text = _display_value(provenance.get(key, ""))
        if text and text not in sources:
            sources.append(text)
    visual_fragments = provenance.get("visual_fragments", [])
    if isinstance(visual_fragments, list):
        fragment_lines: list[str] = []
        for fragment in visual_fragments:
            if not isinstance(fragment, dict):
                continue
            fragment_line = parsing._compose_supplier_description_note(
                _display_value(fragment.get("supplier", "")),
                _display_value(fragment.get("specs_or_description", "")),
                _display_value(fragment.get("notes", "")),
            )
            fragment_line = _display_value(fragment_line)
            if fragment_line:
                fragment_lines.append(fragment_line)
        if fragment_lines:
            combined = " | ".join(fragment_lines)
            if combined not in sources:
                sources.append(combined)
    return sources


def _imperial_handle_summary_should_append_fallback_sources(
    item: dict[str, Any],
    display_lines: list[str],
) -> bool:
    fallback_sources = _imperial_handle_summary_fallback_sources(item)
    if not fallback_sources:
        return False
    display_candidates: list[str] = []
    for line in display_lines:
        for candidate in _imperial_summary_values_for_bucket(
            "handles",
            _display_value(line),
            _normalize_imperial_handle_summary_value,
            supplier=_display_value(item.get("supplier", "")),
        ):
            if candidate and candidate not in display_candidates:
                display_candidates.append(candidate)
    fallback_candidates: list[str] = []
    for source in fallback_sources:
        for candidate in _imperial_summary_values_for_bucket(
            "handles",
            _display_value(source),
            _normalize_imperial_handle_summary_value,
            supplier=_display_value(item.get("supplier", "")),
        ):
            if candidate and candidate not in fallback_candidates:
                fallback_candidates.append(candidate)
    if not fallback_candidates:
        return False
    if display_candidates:
        display_identity: set[str] = set()
        for candidate in display_candidates:
            display_identity.update(_imperial_handle_summary_identity_tokens(candidate))
        review_issue_types = _imperial_material_row_review_issue_types(item)
        if review_issue_types and review_issue_types <= {"row_order_drift", "label_contamination"}:
            return False
        has_doors_family = any(re.search(r"(?i)\bDOORS?\s*-", candidate) for candidate in display_candidates)
        has_drawers_family = any(re.search(r"(?i)\bDRAWERS?\s*-", candidate) for candidate in display_candidates)
        has_no_handles_family = any(re.search(r"(?i)\bno\s+handles?\b", candidate) for candidate in display_candidates)
        if (has_doors_family and has_drawers_family) or (has_no_handles_family and (has_doors_family or has_drawers_family)):
            return False
        unmatched_fallback_candidates = [
            candidate
            for candidate in fallback_candidates
            if not any(_imperial_summary_values_equivalent("handles", candidate, existing) for existing in display_candidates)
        ]
        max_display_quality = max(
            (_imperial_summary_value_quality("handles", candidate) for candidate in display_candidates),
            default=0.0,
        )
        if display_identity and unmatched_fallback_candidates and all(
            (
                _imperial_handle_summary_identity_tokens(candidate)
                and _imperial_handle_summary_identity_tokens(candidate) <= display_identity
                and _imperial_summary_value_quality("handles", candidate) <= max_display_quality - 2.0
                and (
                    re.search(r"(?i)\b(?:doors?|drawers?|upper|uppers|talls?|bases?|overheads?)\b", candidate)
                    or re.search(r"(?i)\b\d{2,4}\s*mm\b", candidate)
                )
            )
            for candidate in unmatched_fallback_candidates
        ):
            return False
        if display_identity and unmatched_fallback_candidates and all(
            re.search(
                r"(?i)\b(?:no\s+handles?(?:\s+on\s+[a-z ]+)?|finger\s+pull\s+only|touch\s+catch|push\s+to\s+open)\b",
                candidate,
            )
            and not _imperial_handle_summary_identity_tokens(candidate)
            for candidate in unmatched_fallback_candidates
        ):
            return False
    for candidate in fallback_candidates:
        if _imperial_summary_value_quality("handles", candidate) < 0:
            continue
        if not any(_imperial_summary_values_equivalent("handles", candidate, existing) for existing in display_candidates):
            return True
    return False


def _imperial_material_row_review_display_status(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    if _imperial_material_row_is_only_row_order_review(item):
        return "order_hint"
    status = _display_value(item.get("revalidation_status", "")).lower()
    if status:
        return status
    if bool(item.get("needs_review", False)):
        return "needs_review"
    issue_types = _imperial_material_row_review_issue_types(item)
    if issue_types:
        return "passed"
    return ""


def _display_imperial_material_row_title(item: dict[str, Any]) -> str:
    title = _display_value(item.get("area_or_item", ""))
    if not title:
        return ""
    title = parsing.normalize_space(re.sub(r"(?i)\(([^)]+)\)\s*\(\1\)", r"(\1)", title))
    provenance = item.get("provenance", {})
    if isinstance(provenance, dict):
        if _imperial_title_prefers_normalized_label(item):
            return title
        raw_title = _display_value(provenance.get("raw_area_or_item", ""))
        preferred_raw_title = _prefer_imperial_raw_area_or_item_title(raw_title, title)
        if preferred_raw_title:
            return preferred_raw_title
    bucket_key = _imperial_summary_bucket_key_for_item(
        {
            "title": title,
            "tags": item.get("tags", []),
        }
    )
    if bucket_key == "handles" or re.search(r"(?i)\b(?:handles?|knob)\b", title):
        return title
    return title


def _prefer_imperial_raw_area_or_item_title(raw_title: str, normalized_title: str) -> str:
    raw = _display_value(raw_title)
    title = _display_value(normalized_title)
    if not raw:
        return ""
    if not title:
        return raw
    raw = parsing.normalize_space(re.sub(r"(?i)\(([^)]+)\)\s*\(\1\)", r"(\1)", raw))
    if raw == title:
        return raw
    raw_upper = raw.upper()
    title_upper = title.upper()
    if raw_upper.startswith(title_upper):
        suffix = parsing.normalize_space(raw[len(title) :]).strip(" -|;,")
    elif title_upper in raw_upper:
        match = re.search(re.escape(title_upper), raw_upper)
        suffix = parsing.normalize_space(raw[match.end() :]).strip(" -|;,") if match else ""
    else:
        suffix = ""
    cleaned_raw = parsing._imperial_clean_material_row_label_text(raw)
    if cleaned_raw == raw:
        if not suffix:
            return raw
        if re.fullmatch(
            r"(?i)(?:including|and|open shelving|glass doors only|doors only|drawers and open shelving|tall open shelving|feature cabinetry|bar back|feature colour|feature island colour|feature tall doors)(?:\s+.*)?",
            suffix,
        ):
            return raw
        return ""
    if cleaned_raw != title:
        return ""
    if not suffix:
        return ""
    if re.search(
        r"(?i)\b(?:polytec|laminex|caesarstone|smartstone|furnware|woodmatt|ultramatt|matt|gloss|oak|walnut|white|black|grey|natural|colour code|\d+\s*mm|double powerpoint|usb|socket|sink|tap)\b",
        suffix,
    ):
        return ""
    if re.search(r"[a-z]{2,}", suffix) and not re.fullmatch(r"(?i)(?:including|and|open shelving|glass doors only|doors only|drawers and open shelving|tall open shelving|feature cabinetry|bar back)(?:\s+.+)?", suffix):
        return ""
    if re.fullmatch(
        r"(?i)(?:including|and|open shelving|glass doors only|doors only|drawers and open shelving|tall open shelving|feature cabinetry|bar back|feature colour|feature island colour|feature tall doors|upper cabinetry colour.+|base cabinetry colour.+|bench cabinetry colour.+|tall cabinetry colour.+)(?:\s+.*)?",
        suffix,
    ):
        return raw
    if raw.endswith(")") or raw.count("(") != title.count("("):
        return raw
    return ""


def _imperial_title_prefers_normalized_label(item: dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    title = _display_value(item.get("area_or_item", ""))
    if not title:
        return False
    provenance = item.get("provenance", {})
    if not isinstance(provenance, dict):
        return False
    raw_title = _display_value(provenance.get("raw_area_or_item", ""))
    if not raw_title or raw_title == title:
        return False
    fragment_labels = [
        _display_value(value)
        for value in (provenance.get("fragment_area_or_items", []) or [])
        if _display_value(value)
    ]
    if not fragment_labels:
        return False
    title_upper = title.upper()
    raw_upper = raw_title.upper()
    return title_upper.startswith(raw_upper) and any(fragment.upper() in title_upper for fragment in fragment_labels)


def _imperial_summary_bucket_key(tags: list[str]) -> str:
    normalized = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    if "door_colours" in normalized:
        return "door_colours"
    if "handles" in normalized:
        return "handles"
    if "bench_tops" in normalized:
        return "bench_tops"
    return ""


def _imperial_summary_bucket_key_for_item(item: dict[str, Any]) -> str:
    bucket_key = _imperial_summary_bucket_key(item.get("tags", []))
    if bucket_key:
        return bucket_key
    title = _display_value(item.get("title", ""))
    title = re.sub(r"(?i)\bHANLDES\b", "HANDLES", title)
    title_upper = title.upper()
    if "BENCHTOP" in title_upper:
        return "bench_tops"
    if re.search(r"(?i)\b(?:handles?|knob)\b", title):
        return "handles"
    if re.search(r"(?i)\b(?:colour|frame|bar back|panel)\b", title):
        return "door_colours"
    return ""


def _imperial_summary_text_has_header_pollution(text: str) -> bool:
    cleaned = _display_value(text)
    if not cleaned:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:address|client|date|ceiling\s*height|cabinetry\s*height|bulkhead|shadowline)\s*:",
            cleaned,
        )
        or re.search(r"(?i)\b(?:AREA\s*/\s*ITEM|SPECS\s*/\s*DESCRIPTION|JOINERY\s+SELECTION\s+SHEET)\b", cleaned)
        or re.search(r"(?i)\b(?:ALL\s+COLOURS\s+SHOWN|PRODUCT\s+AVAILABILITY|DOCUMENT\s+REF|SIGNATURE|DESIGNER)\b", cleaned)
        or re.search(r"(?i)\bSQUARE\s*SET\s*CEILING\s*HEIGHT\b", cleaned)
    )


def _imperial_material_row_is_summary_worthy(item: dict[str, Any], bucket_key: str) -> bool:
    title = _display_value(item.get("title", ""))
    title = re.sub(r"(?i)\bHANLDES\b", "HANDLES", title)
    value = _display_value(item.get("value", ""))
    display_value = _display_value(item.get("display_value", "")) or value
    supplier = _display_value(item.get("supplier", ""))
    provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
    pollution_probe = " ".join(
        _display_value(part)
        for part in (
            title,
            value,
            display_value,
            supplier,
            item.get("notes", ""),
            provenance.get("layout_value_text", ""),
            provenance.get("layout_supplier_text", ""),
            provenance.get("layout_notes_text", ""),
        )
        if _display_value(part)
    )
    if _imperial_summary_text_has_header_pollution(pollution_probe):
        return False
    if bucket_key == "handles" and not value:
        display_lines = [
            _display_value(line)
            for line in item.get("display_lines", [])
            if _display_value(line)
        ]
        if display_lines:
            value = " | ".join(display_lines)
        elif _imperial_material_row_is_handle_summary_review_fallback(item):
            fallback_sources = _imperial_handle_summary_fallback_sources(item)
            if fallback_sources:
                value = " | ".join(fallback_sources)
    title_upper = title.upper()
    value_upper = value.upper()
    material_candidates = (
        _imperial_summary_material_candidates(bucket_key, display_value, supplier)
        if bucket_key in {"door_colours", "bench_tops"}
        else []
    )
    normalized_value = {
        "door_colours": _normalize_door_colour_summary_value(value),
        "handles": _normalize_imperial_handle_summary_value(value),
        "bench_tops": _normalize_benchtop_summary_value(value),
    }.get(bucket_key, value)
    if not title or not value:
        return False
    if _imperial_material_row_needs_review(item):
        if _imperial_material_row_is_only_row_order_review(item):
            pass
        elif bucket_key == "handles" and _imperial_material_row_is_handle_summary_review_fallback(item):
            pass
        elif bucket_key == "door_colours" and _imperial_material_row_is_door_colour_summary_review_fallback(item):
            pass
        else:
            return False
    if not normalized_value or re.match(r"(?i)^(?:incl|include|open|split|allow|note)\b", normalized_value):
        return False
    if bucket_key == "bench_tops":
        if re.search(r"(?i)\bunder\s+dryer\b", title):
            return False
        return "BENCHTOP" in title_upper
    if bucket_key == "handles":
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b)\b", value):
            return False
        if re.search(r"(?i)\bcasters?\b", value) and not _imperial_handle_summary_has_handle_identity(value):
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
            tagged_door_colour = "door_colours" in {
                str(tag).strip().lower() for tag in item.get("tags", []) if str(tag).strip()
            }
            valid_tagged_door_row = (
                tagged_door_colour
                and re.search(r"(?i)\bdoors?\b", title)
                and material_candidates
                and not re.search(
                    r"(?i)\b(?:handle|fingerpull|bevel edge|kethy|allegra|momo|bronte|knob|touch catch|no handles?)\b",
                    display_value,
                )
            )
            valid_feature_cabinetry = (
                tagged_door_colour
                and re.search(r"(?i)\bfeature\s+cabinetry\b", title)
                and _imperial_door_colour_text_is_valid_feature_cabinetry(display_value or value)
            )
            if not (valid_tagged_door_row or valid_feature_cabinetry):
                return False
        if re.search(r"(?i)\b(?:internals?|kickboards?|open shelving)\b", title):
            return False
        if re.search(r"(?i)\bdrawers?\b", title) and not re.search(r"(?i)\bdrawers?\s+colour\b|\bdrawer\s+colour\b", title):
            return False
        if material_candidates:
            return True
        if re.search(r"(?i)\b(?:gpo|spice tray|drawer gpo|lighting|led strip|bin\b|casters?|handle|fingerpull|knob)\b", value):
            return False
        return True
    return False


def _imperial_summary_token_set(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[A-Za-z0-9]+", _display_value(text).upper())
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


def _imperial_summary_anchor_token_set(bucket_key: str, text: str) -> set[str]:
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
        for token in _imperial_summary_token_set(text)
        if token not in generic_tokens
    }


def _imperial_summary_overlap_ratio(left: str, right: str) -> float:
    left_tokens = _imperial_summary_token_set(left)
    right_tokens = _imperial_summary_token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    shared = left_tokens & right_tokens
    if not shared:
        return 0.0
    return float(len(shared)) / float(min(len(left_tokens), len(right_tokens)))


def _imperial_summary_finish_tokens(text: str) -> set[str]:
    normalized = _display_value(text).upper()
    if not normalized:
        return set()
    tokens: set[str] = set()
    finish_patterns = (
        ("RAVINE", r"\bRAVINE\b"),
        ("VENETTE", r"\bVENETTE\b"),
        ("WOODMATT", r"\bWOODMATT\b"),
        ("MATT", r"\bMATT\b|\bMATTE\b"),
        ("NATURAL", r"\bNATURAL\b"),
        ("GLOSS", r"\bGLOSS\b"),
        ("ULTRAMATT", r"\bULTRAMATT\b"),
    )
    for token, pattern in finish_patterns:
        if re.search(pattern, normalized):
            tokens.add(token)
    return tokens


def _imperial_handle_summary_identity_tokens(text: str) -> set[str]:
    normalized = _display_value(text)
    if not normalized:
        return set()
    tokens = {
        parsing.normalize_space(match.group(0)).upper()
        for match in re.finditer(
            r"(?i)\b(?:S\d+\.\d+\.[A-Z]+|HT\d+\s*-\s*\d+\s*-\s*[A-Z]+|SO-\d+-[A-Z0-9-]+|[A-Z]\d+/\d+\s*-\s*[A-Z0-9]+|\d{3,5}-[A-Z0-9]+)\b",
            normalized,
        )
    }
    tokens = {re.sub(r"\s+", "", token) for token in tokens}
    pm_match = re.search(r"(?i)\bPM2817\s*/\s*(192|288)\s*/\s*MSIL\b", normalized)
    if pm_match:
        tokens.add(f"PM2817-{pm_match.group(1)}-MSIL")
    ht576_match = re.search(r"(?i)\bHT576\s*-\s*(128|192)\s*-\s*BKO\b", normalized)
    if ht576_match:
        tokens.add(f"HT576-{ht576_match.group(1)}-BKO")
    allegra_knob_match = re.search(r"(?i)\b(6368-K)\b", normalized)
    if allegra_knob_match:
        tokens.add(parsing.normalize_space(allegra_knob_match.group(1)).upper())
    if re.search(r"(?i)\bHIN0682\.832\.OAK\b", normalized):
        tokens.add("HIN0682.832.OAK")
    if re.search(r"(?i)\bHIN0682\.416\.OAK\b", normalized):
        tokens.add("HIN0682.416.OAK")
    if re.search(r"(?i)\bknob\b", normalized):
        tokens.add("KNOB")
    if re.search(r"(?i)\b(?:2163\s+Voda|Voda\s+Profile\s+Handle)\b", normalized):
        tokens.add("VODA_2163")
        voda_finish_match = re.search(r"(?i)\b(Brushed\s+Nickel|Matt\s+Black|Brushed\s+Anthracite|Anthracite)\b", normalized)
        voda_size_match = re.search(r"(?i)\b(\d{2,3})\s*mm\b", normalized)
        if voda_finish_match and voda_size_match:
            finish_token = re.sub(r"\s+", "_", parsing.normalize_space(voda_finish_match.group(1)).upper())
            tokens.add(f"VODA_2163_{finish_token}_{voda_size_match.group(1)}MM")
    if re.search(r"(?i)\bvoda\s+profile\s+handle\b", normalized):
        tokens.add("VODA")
    if re.search(r"(?i)\b7202\s+Square\s+D\s+Handle\b", normalized):
        tokens.add("SQUARE_D_7202")
    if re.search(r"(?i)\bHampton Handle\b", normalized):
        tokens.add("HAMPTON_HANDLE")
    if re.search(r"(?i)\bMomo\s+Trianon\b", normalized):
        tokens.add("MOMO_TRIANON")
    if re.search(r"(?i)\bMomo\s+Lugo\b", normalized):
        tokens.add("MOMO_LUGO")
    if re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", normalized):
        tokens.add("BEVEL_EDGE_FINGER_PULL")
    if re.search(r"(?i)\bpush\s+to\s+open\b|\bPTO\b", normalized):
        tokens.add("PTO")
    return tokens


def _imperial_voda_handle_summary_signature(text: str) -> str:
    normalized = _display_value(text)
    if not re.search(r"(?i)\b(?:2163\s+Voda|Voda\s+Profile\s+Handle)\b", normalized):
        return ""
    finish_match = re.search(r"(?i)\b(Brushed\s+Nickel|Matt\s+Black|Brushed\s+Anthracite|Anthracite)\b", normalized)
    size_match = re.search(r"(?i)\b(\d{2,3})\s*mm\b", normalized)
    if not finish_match or not size_match:
        return ""
    finish_token = re.sub(r"\s+", "_", parsing.normalize_space(finish_match.group(1)).upper())
    return f"VODA_2163_{finish_token}_{size_match.group(1)}MM"


def _imperial_handle_summary_has_handle_identity(text: str) -> bool:
    normalized = _display_value(text)
    if not normalized:
        return False
    if re.fullmatch(r"(?i)casters?", normalized):
        return False
    if _imperial_handle_summary_identity_tokens(normalized):
        return True
    return bool(
        re.search(
            r"(?i)\b(?:"
            r"handles?|knobs?|cabinet\s+knob|pull|finger\s*pull|fingerpull|finger\s+space|recessed\s+finger|"
            r"push\s+to\s+open|pto|no\s+handles?|touch\s+catch|ht\d+|pm\d+|s225\.|so-\d+|"
            r"product\s+code|part\s*no|voda|trianon|lugo|hampton|hin0682|woodgate|darwen|"
            r"bevel\s+edge|benchseat\s+drawers|square\s+d|"
            r"\d{3,5}\s+\d{2,3}\s+[A-Z]{2}\s+OA"
            r")\b",
            normalized,
        )
    )


def _imperial_summary_value_quality(bucket_key: str, text: str) -> float:
    cleaned = _display_value(text)
    quality = float(len(_imperial_summary_token_set(cleaned)))
    quality += 2.0 if " - " in cleaned else 0.0
    if bucket_key == "handles":
        if re.search(r"(?i)\b(?:polytec|laminex)\b", cleaned):
            quality -= 8.0
        if re.search(r"(?i)\b(?:floating shelves?|wardrobe tube|hanging rail|hamper|accessory|moulding)\b", cleaned):
            quality -= 10.0
        if re.search(r"(?i)\b(?:recessed findger space|recessed finger space|finger space)\b", cleaned) and not re.search(r"(?i)\b(?:no handles?|touch catch)\b", cleaned):
            quality -= 8.0
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
        if re.search(r"(?i)\bknobs?\s+on\s+doors?,?\s+handles?\s+on\s+drawers?\b", cleaned):
            quality -= 6.0
        if re.search(r"(?i)\bas\s+doors\b", cleaned):
            quality -= 6.0
        if re.search(r"(?i)\b(?:vertical|horizontal)\s+on\s+(?:doors?|drawers?)\b", cleaned):
            quality -= 5.0
        if re.search(r"(?i)\bPM2817\s*/\s*(192|288)\s*/\s*MSIL\b.*\bPM2817\s*/\s*\1\s*/\s*MSIL\b", cleaned):
            quality -= 8.0
        if re.search(r"(?i)\b(?:horizontal|vertical)(?:/vertical)?\b", cleaned) and not re.search(r"(?i)\b(?:ht\d+|pm\d+|s225\.|product code|part no)\b", cleaned):
            quality -= 4.0
        if re.search(r"(?i)\b(?P<brand>fienza|momo)\s+(?P=brand)\b", cleaned):
            quality -= 6.0
    if bucket_key == "door_colours":
        if re.search(r"(?i)\b(?:handle|knob|push to open|finger pull|lighting|led)\b", cleaned):
            quality -= 5.0
        if re.search(r"(?i)\bframed?\s+sliding\s+doors?\b", cleaned):
            quality -= 4.0
    if bucket_key == "bench_tops":
        if re.search(r"(?i)\b(?:floating shelf|internal steel support)\b", cleaned):
            quality -= 4.0
    return quality


def _imperial_summary_values_equivalent(bucket_key: str, left: str, right: str) -> bool:
    left_text = _display_value(left)
    right_text = _display_value(right)
    if not left_text or not right_text:
        return False
    if left_text.lower() == right_text.lower():
        return True
    if bucket_key == "handles":
        generic_handle_values = {"push to open", "no handles", "bevel edge finger pull"}
        for signature in ("LUGO KNOB", "TRIANON D HANDLE", "HAMPTON HANDLE"):
            if signature in left_text.upper() and signature in right_text.upper():
                return True
        left_identity = _imperial_handle_summary_identity_tokens(left_text)
        right_identity = _imperial_handle_summary_identity_tokens(right_text)
        left_voda_signature = _imperial_voda_handle_summary_signature(left_text)
        right_voda_signature = _imperial_voda_handle_summary_signature(right_text)
        if left_voda_signature and left_voda_signature == right_voda_signature:
            return True
        if left_identity and right_identity and left_identity != right_identity:
            return False
        if left_identity and right_identity and left_identity == right_identity:
            def _handle_family(text: str) -> str:
                if re.search(r"(?i)\bDRAWERS?\s*-", text):
                    return "drawers"
                if re.search(r"(?i)\bDOORS?\s*-", text):
                    return "doors"
                if re.search(r"(?i)\b(?:no\s+handles?|touch\s+catch|push\s+to\s+open)\b", text):
                    return "generic"
                return ""
            left_family = _handle_family(left_text)
            right_family = _handle_family(right_text)
            if left_family and left_family == right_family:
                return True
            if (left_text.lower() in generic_handle_values and right_family) or (
                right_text.lower() in generic_handle_values and left_family
            ):
                return False
            if not left_family or not right_family:
                return True
        if left_text.lower() in generic_handle_values or right_text.lower() in generic_handle_values:
            return False
    if bucket_key == "door_colours":
        left_finishes = _imperial_summary_finish_tokens(left_text)
        right_finishes = _imperial_summary_finish_tokens(right_text)
        if left_finishes and right_finishes and left_finishes != right_finishes:
            return False
    left_tokens = _imperial_summary_anchor_token_set(bucket_key, left_text)
    right_tokens = _imperial_summary_anchor_token_set(bucket_key, right_text)
    if min(len(left_tokens), len(right_tokens)) < 2:
        return False
    overlap = float(len(left_tokens & right_tokens)) / float(min(len(left_tokens), len(right_tokens)))
    if bucket_key == "handles":
        return overlap >= 0.9
    if bucket_key == "door_colours":
        return overlap >= 0.72
    if bucket_key == "bench_tops":
        return overlap >= 0.72
    return False


def _find_imperial_summary_entry(
    bucket_key: str,
    entries: list[dict[str, Any]],
    candidate_text: str,
) -> dict[str, Any] | None:
    candidate = _display_value(candidate_text)
    if not candidate:
        return None
    candidate_key = candidate.lower()
    for entry in entries:
        existing_text = _display_value(entry.get("text", ""))
        if not existing_text:
            continue
        if existing_text.lower() == candidate_key:
            return entry
        if _imperial_summary_values_equivalent(bucket_key, existing_text, candidate):
            return entry
    return None


def _sort_room_rows_by_priority(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=_room_priority_sort_key)


def _room_priority_sort_key(row: dict[str, Any]) -> int:
    title = _display_value(row.get("original_room_label", "")) or _display_value(row.get("room_key", ""))
    normalized = _normalize_room_priority_title(title)
    if re.search(r"\bKITCHEN\b", normalized):
        return 0
    if (re.search(r"\bBUTLER\b", normalized) or re.search(r"\bPANTRY\b", normalized)) and not (
        re.search(r"\bWIP\b", normalized) or "WALK IN PANTRY" in normalized
    ):
        return 1
    if re.search(r"\bWIP\b", normalized) or "WALK IN PANTRY" in normalized:
        return 2
    if re.search(r"\bBAR\b", normalized):
        return 3
    if "LAUNDRY CHUTE" in normalized:
        return 5
    if re.search(r"\bLAUNDRY\b", normalized):
        return 4
    if any(keyword in normalized for keyword in ("VANITY", "VANITIES", "BATHROOM", "ENSUITE", "POWDER")):
        return 6
    if any(keyword in normalized for keyword in ("WALK IN ROBE", "ROBE", "WIR")):
        return 7
    if re.search(r"\bRUMPUS\b", normalized):
        return 8
    if re.search(r"\bLINEN\b", normalized):
        return 9
    return 10


def _normalize_room_priority_title(value: str) -> str:
    text = parsing.normalize_space(value).upper()
    text = re.sub(r"[’']", "", text)
    text = text.replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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
                "evidence_snippet": parsing._clean_appliance_capture_text(_display_value(row.get("evidence_snippet", ""))),
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
        "feature_colour": parsing._merge_clean_group_text(row.get("feature_colour", ""), derived.get("feature_colour", ""), cleaner=parsing._clean_door_colour_value),
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
    if parsing._is_imperial_builder(str(snapshot.get("builder_name", "") or "")):
        return _build_imperial_material_summary(snapshot)
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


def _build_imperial_material_summary(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {
        "door_colours": {"label": "Door Colours", "entries": []},
        "handles": {"label": "Handles", "entries": []},
        "bench_tops": {"label": "Bench Tops", "entries": []},
    }
    bucket_normalizers = {
        "door_colours": _normalize_door_colour_summary_value,
        "handles": _normalize_imperial_handle_summary_value,
        "bench_tops": _normalize_benchtop_summary_value,
    }
    rooms = sorted(
        [room for room in snapshot.get("rooms", []) if isinstance(room, dict)],
        key=lambda room: (
            int(room.get("room_order", 0) or 0),
            _display_value(room.get("original_room_label", "")) or _display_value(room.get("room_key", "")),
        ),
    )
    for room in rooms:
        room_label = _display_value(room.get("original_room_label", "")) or _display_value(room.get("room_key", "")) or "Room"
        for item in _flatten_imperial_material_rows(room):
            bucket_key = _imperial_summary_bucket_key_for_item(item)
            provenance = item.get("provenance", {}) if isinstance(item.get("provenance", {}), dict) else {}
            absorbed_handle_texts = provenance.get("absorbed_inline_handle_texts", [])
            if bucket_key:
                if _imperial_material_row_is_summary_worthy(item, bucket_key):
                    bucket_entries = buckets[bucket_key]["entries"]
                    area_or_item = _display_value(item.get("title", ""))
                    if bucket_key == "handles":
                        summary_texts = _imperial_material_row_handle_summary_candidates(item)
                    else:
                        summary_texts = []
                        display_line_texts: list[str] = []
                        for display_line in item.get("display_lines", []) or []:
                            normalized_line = _display_value(display_line)
                            if normalized_line and normalized_line not in display_line_texts:
                                display_line_texts.append(normalized_line)
                        display_line_summary_texts: list[str] = []
                        for source_text in display_line_texts:
                            display_line_summary_texts.extend(
                                _imperial_summary_values_for_bucket(
                                    bucket_key,
                                    source_text,
                                    bucket_normalizers[bucket_key],
                                    supplier=_display_value(item.get("supplier", "")),
                                )
                            )
                        if bucket_key == "bench_tops" and display_line_summary_texts:
                            summary_texts = display_line_summary_texts
                        else:
                            source_texts: list[str] = list(display_line_texts)
                            for source_text in (
                            _display_value(item.get("display_value", "")),
                            _display_value(item.get("value", "")),
                            ):
                                if source_text and source_text not in source_texts:
                                    source_texts.append(source_text)
                            for source_text in source_texts:
                                summary_texts.extend(
                                    _imperial_summary_values_for_bucket(
                                        bucket_key,
                                        source_text,
                                        bucket_normalizers[bucket_key],
                                        supplier=_display_value(item.get("supplier", "")),
                                    )
                                )
                    for summary_text in summary_texts:
                        if not summary_text:
                            continue
                        if (
                            bucket_key == "handles"
                            and re.search(r"(?i)\bDESK\b", area_or_item)
                            and re.match(r"(?i)^(?:\d+\s+)?Voda\s+Profile\s+Handle\b", summary_text)
                            and not summary_text.upper().startswith("DESK - ")
                        ):
                            summary_text = f"DESK - {summary_text}"
                        entry = _find_imperial_summary_entry(bucket_key, bucket_entries, summary_text)
                        if entry is None:
                            entry = {
                                "text": summary_text,
                                "display_text": summary_text,
                                "rooms": [],
                                "rooms_display": "",
                                "area_or_items": [],
                            }
                            bucket_entries.append(entry)
                        elif _imperial_summary_value_quality(bucket_key, summary_text) > _imperial_summary_value_quality(
                            bucket_key,
                            entry.get("text", ""),
                        ):
                            entry["text"] = summary_text
                            entry["display_text"] = summary_text
                        if room_label and room_label not in entry["rooms"]:
                            entry["rooms"].append(room_label)
                        if area_or_item and area_or_item not in entry["area_or_items"]:
                            entry["area_or_items"].append(area_or_item)
            if isinstance(absorbed_handle_texts, list):
                handle_bucket = buckets["handles"]["entries"]
                for raw_handle_text in absorbed_handle_texts:
                    for summary_text in _imperial_summary_values_for_bucket(
                        "handles",
                        _display_value(raw_handle_text),
                        bucket_normalizers["handles"],
                    ):
                        if not summary_text:
                            continue
                        entry = _find_imperial_summary_entry("handles", handle_bucket, summary_text)
                        if entry is None:
                            entry = {
                                "text": summary_text,
                                "display_text": summary_text,
                                "rooms": [],
                                "rooms_display": "",
                                "area_or_items": ["HANDLES"],
                            }
                            handle_bucket.append(entry)
                        if room_label and room_label not in entry["rooms"]:
                            entry["rooms"].append(room_label)
    handle_entries = buckets["handles"]["entries"]
    if len(handle_entries) > 1:
        buckets["handles"]["entries"] = [
            entry for entry in handle_entries if str(entry.get("text", "") or "").strip().lower() != "no handles"
        ] or handle_entries
        handle_entries = buckets["handles"]["entries"]
    if any(
        re.search(r"(?i)\bno\s+handles?(?:\s+on\s+[a-z ]+|(?:\s+to)?\s+overheads?)\b", str(entry.get("text", "") or ""))
        and str(entry.get("text", "") or "").strip().lower() != "no handles"
        for entry in handle_entries
    ):
        buckets["handles"]["entries"] = [
            entry for entry in handle_entries if str(entry.get("text", "") or "").strip().lower() != "no handles"
        ] or handle_entries
    for bucket in buckets.values():
        for entry in bucket["entries"]:
            rooms = [room for room in entry.get("rooms", []) if room]
            entry["rooms_display"] = " | ".join(rooms)
        bucket["count"] = len(bucket["entries"])
    return buckets


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
    text = re.sub(r"(?i)^INCLUDING\b\s*", "", text)
    text = re.sub(r"(?i)\bCOLOURED?\b", "", text)
    text = re.sub(r"(?i)\bREFER TO DRAWINGS(?: FOR ALLOCATIONS)?\b.*$", "", text)
    text = re.sub(r"(?i)\bBLUM\s+AVENTOS\b.*$", "", text)
    text = re.sub(r"(?i)\bframed?\s+sliding\s+doors?\b\s*-?\s*", "", text)
    text = re.sub(r"(?i)\bNOTE:\s*.*$", "", text)
    text = re.sub(r"(?i)\b(?:COLOURED\s+)?BOTTOMS TO OVERHEADS\b.*$", "", text)
    text = re.sub(r"(?i)\bVERTICAL\s*-\s*GRAIN\b", "Vertical Grain", text)
    text = re.sub(r"(?i)\bHORIZONTAL\s*-\s*GRAIN\b", "Horizontal Grain", text)
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
    return _strip_summary_location_tail(
        text,
        (
            r"(?i)\bhandle located\b.*$",
            r"(?i)\bdoor location\b.*$",
            r"(?i)\bdown\s*drawer location\b.*$",
            r"(?i)\bdrawer location\b.*$",
            r"(?i)\bhorizontal\s+on\s+drawers?\b.*$",
            r"(?i)\bvertical\s+on\s+doors?\b.*$",
            r"(?i)\b(?:centre|center)\s+to\s+profile\b.*$",
            r"(?i)\bto\s+(?:base|upper|overhead|base cabinets?|upper cabinets?|cabinet locations?)\b.*$",
        ),
    )


def _normalize_imperial_handle_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    if not text or re.fullmatch(r"(?i)none", text):
        return ""
    if re.search(r"(?i)\bkicks?\s+along\s+here\b", text):
        return ""
    text = re.sub(
        r"(?i)^(?:Horizontal|Vertical)\s+on\s+ALL(?=(?:Momo|[A-Za-z0-9].*\b(?:handle|pull|knob)\b))\s*",
        "",
        text,
    )
    if re.fullmatch(r"\[[^\]]+\](?:\s*-\s*)?", text):
        return ""
    text = re.sub(r"^\[[^\]]+\]\s*-\s*", "", text)
    text = re.sub(r"(?i)^(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Barchie|Lincoln Sentry|ABI Interiors)\s+", "", text)
    text = re.sub(
        r"(?i)^(?:(?:HANLDES?|HANDLES?)\s*-\s*)?(?:TALL\s+CABS?\s*/\s*PANTRY\s+CABS?\s*ONLY|BASE\s+DRAWERS|BASE\s+DOORS|BASE\s+CABS?(?:\s*\+\s*DRAWERS)?|TALL\s*\+\s*PANTRY\s+DRAWERS|NO\s+HANDLES?\s+OVERHEADS|HANDLES?\s+TO\s+OVERHEADS)\s*-\s*",
        "",
        text,
    )
    text = re.sub(r"(?i)\b(DRAWERS?\s*-\s*)Polytec\s*-\s*(?:Profile\s*-\s*Classic\s+White\s+Matt|As\s+Doors)\s*", r"\1", text)
    text = re.sub(r"(?i)\b(DOORS?\s*-\s*)Momo\s*-\s*", r"\1Momo ", text)
    text = re.sub(r"(?i)\bBASE\s*-\s*EDGE\s+FINGERPULL\b", "BASE - BEVEL EDGE FINGERPULL", text)
    text = re.sub(r"(?i)\s*-\s*\((Vertical|Horizontal)\)\s*$", r" - \1", text)
    text = re.sub(
        r"(?i)\s*-\s*\((?:Investigating[^)]*|pricing[^)]*|(?:Horizontal|Vertical)\s+Install|location[^)]*)\)\s*$",
        "",
        text,
    )
    text = re.sub(r"(?i)\b(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Barchie|Lincoln Sentry|ABI Interiors)\b\s*-\s*", "", text)
    text = re.sub(r"(?i)\b(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Barchie|Lincoln Sentry|ABI Interiors)\b$", "", text).strip(" -|;,")
    text = re.sub(r"(?i)^on\s+Upper\s+cabinetry\s*-\s*Finger\s+pull\s+only\b", "No handles on Upper cabinetry - Finger pull only", text)
    text = re.sub(
        r"(?i)\b(DRAWERS?|DOORS?)\s*\|\s*",
        lambda match: f"{match.group(1).upper()} - ",
        text,
        count=1,
    )
    text = re.sub(
        r"(?i)\b(DRAWERS?|DOORS?)\s+(?=(?:Momo|HT\d+|PM\d+|Voda|S225|Woodgate|Hampton|Trianon|Lugo)\b)",
        lambda match: f"{match.group(1).upper()} - ",
        text,
        count=1,
    )
    text = re.sub(r"(?i)\b(?:Horizontal|Vertical)\s+Install\b.*$", "", text).strip(" -|;,")
    text = re.sub(r"(?i)\bKnobs?\s+on\s+Doors?,?\s*", "", text).strip(" -|;,")
    text = re.sub(r"(?i)\bKnobs?\s+on\s+Doors?,?\s+Handles?\s+on\s+Drawers?(?:\s*\([^)]*\))?\b.*$", "", text).strip(" -|;,")
    text = re.sub(r"(?i)\bHandles?\s+on\s+Drawers?(?:\s*\([^)]*\))?\b.*$", "", text).strip(" -|;,")
    text = re.sub(r"(?i)\s*-\s*Part\s*no:\s*$", "", text).strip(" -|;,")
    text = re.sub(r"(?i)\bAs\s+Doors\b", "", text).strip(" -|;,")
    if re.search(r"(?i)\bvertical\s+on\s+doorshandles\s+base\s+doors\b", text):
        return ""
    if re.search(r"(?i)\bwardrobe\s+tube\b|\baluminum\b", text) and re.search(r"(?i)\bwhite\s*&\s*brushed\s+nickel\b", text):
        return ""
    text = re.sub(r"(?i)\bTouch catch\b[^|;]*?(?:Overheads above)?[^|;]*", lambda match: parsing.normalize_space(match.group(0)), text)
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
    hampton_match = re.search(
        r"(?i)\b(Hampton Handle,\s*Urban Brass)\b(?:[^|]*?\bProduct Code:\s*([A-Z0-9]+)\b)?",
        text,
    )
    if hampton_match:
        composed = parsing.normalize_space(hampton_match.group(1))
        product_code = parsing.normalize_space(hampton_match.group(2) or "")
        if product_code:
            composed = f"{composed} - Product Code: {product_code}"
        return composed
    square_d_match = re.search(
        r"(?i)\b(?:TALLS?\s*-\s*)?7202\s+Square\s+D\s+Handle\s+Brushed(?:\s*\|\s*|\s+)Anthracite\s+320mm\s*-\s*(608\.8E18\.320\.016)\b",
        text,
    )
    if square_d_match:
        return f"TALLS - 7202 Square D Handle Brushed Anthracite 320mm - {parsing.normalize_space(square_d_match.group(1))}"
    voda_ba_match = re.search(
        r"(?i)\b(?:BASES?\s*-\s*)?2163\s+Voda\s+Profile\s+Handle\s+Brushed(?:\s*\|\s*|\s+)Anthracite\s*-\s*(SO-2163-200-BA\s*&\s*SO-2163-300-BA|SO-2163-200-BA&SO-2163-300-BA)\b",
        text,
    )
    if voda_ba_match:
        return f"BASES - 2163 Voda Profile Handle Brushed Anthracite - {parsing.normalize_space(voda_ba_match.group(1).replace('&', ' & '))}"
    allegra_knob_match = re.search(
        r"(?i)\bKnob\b(?:[^|]*?\b(?P<code>[A-Z0-9.-]*K)\b)(?:[^|]*?\bin\s+(?P<finish>[A-Za-z ]+?))?(?=\s*(?:-|$|\(|\|))",
        text,
    )
    if allegra_knob_match:
        knob_text = f"Knob - {parsing.normalize_space(allegra_knob_match.group('code'))}"
        finish = parsing.normalize_space(allegra_knob_match.group("finish") or "")
        if finish:
            knob_text = f"{knob_text} in {finish}"
        return knob_text
    pm_match = re.search(
        r"(?i)\b(PM2817\s*/\s*(?:192|288)\s*/\s*MSIL)\b([^|]*?\b(?:Hole\s+centres|OA\s+SIZE)\b[^|]*)",
        text,
    )
    if pm_match:
        pm_tail = parsing.normalize_space(pm_match.group(2))
        pm_tail = re.sub(r"(?i)\bPM2817\b.*$", "", pm_tail).strip(" -|;,")
        pm_tail = re.sub(r"(?i)\b(?:Polytec|Kethy)\b.*$", "", pm_tail).strip(" -|;,")
        pm_tail = re.sub(r"(?i)\s*-\s*\(?\s*(?:Horizontal|Vertical)\s+Install\)?\s*$", "", pm_tail).strip(" -|;,")
        pm_tail = re.sub(r"\s*-\s*\(?\s*$", "", pm_tail).strip(" -|;,")
        return parsing.normalize_space(f"{pm_match.group(1)} {pm_tail}").strip(" -|;,")
    ht576_canonical_match = re.search(
        r"(?i)\b(HT576\s*-\s*(?:128|192)\s*-\s*BKO)\b([^|]*?\bDarwen\s+Cabinet\s+Pull\s+Handle\b(?:[^|]*?\bBlack\s+Olive\s+Colour\b)?)",
        text,
    )
    if ht576_canonical_match:
        ht_tail = parsing.normalize_space(ht576_canonical_match.group(2))
        ht_tail = re.sub(r"(?i)\bHT576\b.*$", "", ht_tail).strip(" -|;,")
        return parsing.normalize_space(f"{ht576_canonical_match.group(1)} {ht_tail}").strip(" -|;,")
    tall_hinoki_match = re.search(
        r"(?i)\bTall Door Handles?\s*-\s*Momo\s+Hinoki\s+Wood\s+Big\s+D\s*832mm\s+Handle\s+Oak-?\s*(HIN0682\.832\.OAK)\b",
        text,
    )
    if tall_hinoki_match:
        return f"Tall Door Handles - Momo Hinoki Wood Big D 832mm Handle Oak-{parsing.normalize_space(tall_hinoki_match.group(1))}"
    high_split_match = re.search(
        r"(?i)\bHigh Split Handle\s*-\s*(?:Momo\s+)?hinoki\s+wood\s+big\s+d\s*416mm\s+handle\s+oak-?\s*(HIN0682\.416\.OAK)\b",
        text,
    )
    if high_split_match:
        return f"High Split Handle - Momo Hinoki Wood Big D 416mm Handle Oak-{parsing.normalize_space(high_split_match.group(1))}"
    prefixed_item_match = re.search(
        r"(?i)\b(?P<body>(?:DRAWERS?|DOORS?)\s*-\s*[^|]*?\b(?:handle|knob)\b[^|]*)",
        text,
    )
    if prefixed_item_match:
        prefixed_text = parsing.normalize_space(prefixed_item_match.group("body")).strip(" -|;,")
        prefixed_text = re.sub(r"(?i)\b(DRAWERS?|DOORS?)\s*-\s*", lambda m: f"{m.group(1).upper()} - ", prefixed_text, count=1)
        return _normalize_handle_summary_value(prefixed_text)
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
        r"(?i)\b(?:(?P<prefix>DESK)\s*-\s*)?(?P<body>(?:\d+\s+)?Voda\s+Profile\s+Handle\s+(?:Brushed\s+Nickel|Matt\s+Black)\s+\d+\s*mm\s*(?:-\s*)?SO-2163-[A-Z0-9-]+)\b",
        text,
    )
    if desk_handle_match:
        desk_prefix = "DESK - " if desk_handle_match.group("prefix") else ""
        desk_body = parsing.normalize_space(desk_handle_match.group("body"))
        desk_body = re.sub(r"(?i)\s*-?\s*(SO-2163-[A-Z0-9-]+)\b", r" - \1", desk_body)
        desk_text = f"{desk_prefix}{desk_body}"
        desk_text = re.sub(r"(?i)\b(?:Furnware|Titus Tekform)\b\s*$", "", desk_text).strip(" -|;,")
        return desk_text
    ht576_match = re.search(
        r"(?i)\b(HT576\s*-\s*(?:128|192)\s*-\s*BKO)\b",
        text,
    )
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
    if re.search(r"(?i)\bno\s+handles?\s+on\s+(?:upper\s+cabinetry|uppers?)\b", text) and re.search(r"(?i)\bfinger\s+pull\s+only\b", text):
        return "No handles on Upper cabinetry - Finger pull only"
    if re.search(r"(?i)\bno\s+handles?\s+on\s+(?:upper\s+cabinetry|uppers?)\b", text) and re.search(r"(?i)\bPTO(?:\s+where\s+required)?\b", text):
        return "No handles on Upper cabinetry - PTO where required"
    overhead_no_handles_match = re.search(
        r"(?i)\bno\s+handles?\s+(?:to\s+)?overheads?\b(?:\s*-\s*)?(?P<body>[^|;]*)",
        text,
    )
    if overhead_no_handles_match:
        body = parsing.normalize_space(overhead_no_handles_match.group("body") or "")
        body = re.sub(r"(?i)\btouch\s+catch\b.*$", "", body).strip(" -|;,")
        body = re.sub(r"(?i)\b(?:horizontal|vertical)(?:/horizontal|/vertical)?\b.*$", "", body).strip(" -|;,")
        body = re.sub(r"(?i)\band\s*$", "", body).strip(" -|;,")
        if re.search(r"(?i)\b(?:finger\s+space|recessed\s+finger)\b", body):
            return parsing.normalize_space(f"No handles to overheads - {body}")
        return "No handles to overheads"
    if re.search(r"(?i)\b(?:recessed\s+finger\s+space|finger\s+space\s+above)\b", text) and not re.search(
        r"(?i)\btouch\s+catch\b",
        text,
    ):
        finger_text = re.sub(r"(?i)^overheads?\s*:\s*", "", text).strip(" -|;,")
        finger_text = re.sub(r"(?i)\b(?:horizontal|vertical)(?:/horizontal|/vertical)?\b.*$", "", finger_text).strip(" -|;,")
        finger_text = re.sub(r"(?i)\band\s*$", "", finger_text).strip(" -|;,")
        return parsing.normalize_space(finger_text)
    if re.search(r"(?i)\bRecessed\s+finger\s+space\b", text) and re.search(
        r"(?i)\bTouch\s+catch\s+above\s+Fridge\s+and\s+bar\s+back\b",
        text,
    ):
        return "No handles overheads - Recessed finger space / Touch catch above Fridge and bar back"
    if re.search(r"(?i)\b3750\s*128\s*MB\b", text):
        return "3750 128 MB OA = 138mm"
    if re.search(r"(?i)\b3750\s*192\s*MB\b", text):
        return "3750 192 MB OA = 202mm"
    if re.search(r"(?i)\bno\s+handles?\b", text) and not complex_markers and not re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", text):
        return "No handles"
    if re.search(r"(?i)\btouch\s+catch\b", text):
        touch_match = re.search(r"(?i)\bTouch catch(?:\s*-\s*Overheads above)?", text)
        if touch_match:
            return parsing.normalize_space(touch_match.group(0))
    if re.search(r"(?i)\bpush\s+to\s+open\b", text) and not complex_markers:
        return "Push to open"
    if re.search(r"(?i)\bDrawers?\s*-\s*Bevel\s+Edge\s+finger\s+pull\b", text):
        return "Drawers - Bevel Edge finger pull"
    if re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", text) and not re.search(
        r"(?i)\b(?:desk|benchseat|drawers?\s*-|voda|so-[a-z0-9-]+)\b",
        text,
    ):
        return "Bevel Edge finger pull"
    normalized = _normalize_handle_summary_value(text)
    normalized = re.sub(r"(?i)\b(?:Furnware|Titus Tekform)\b\s*$", "", normalized).strip(" -|;,")
    return normalized


def _imperial_known_handle_summary_candidates(text: str) -> list[str]:
    cleaned = parsing.normalize_space(text)
    if not cleaned:
        return []
    candidates: list[str] = []

    def _add(candidate: str) -> None:
        normalized_candidate = parsing.normalize_space(candidate).strip(" -|;,")
        if normalized_candidate and normalized_candidate not in candidates:
            candidates.append(normalized_candidate)

    lugo_match = re.search(
        r"(?i)\bDOORS?\s*-\s*(?:Momo\s*-?\s*)?Lugo\s+Knob\s+38mm\s+In\s+Brushed\s+Nickel(?:\s*-\s*Part\s*no:\s*)?\s*(3238\.BRN\.FG)\b",
        cleaned,
    )
    if lugo_match:
        _add(f"DOORS - Momo Lugo Knob 38mm In Brushed Nickel - Part no: {parsing.normalize_space(lugo_match.group(1))}")

    trianon_match = re.search(
        r"(?i)\bDRAWERS?\s*-\s*(?:Polytec\s*-\s*(?:Profile\s*-\s*Classic\s+White\s+Matt|As\s+Doors)\s*)?(?:Momo\s*-?\s*)?Trianon\s+D\s+Handle\s+128mm(?:\s+In\s+White\s*&\s*Brushed\s+Nickel)?(?:\s*-\s*Part\s*no:\s*)?\s*(TCM3622\.WHBRN\.FG)?\b",
        cleaned,
    )
    if trianon_match:
        code = parsing.normalize_space(trianon_match.group(1) or "")
        description = "DRAWERS - Momo Trianon D Handle 128mm In White & Brushed Nickel"
        if code:
            description = f"{description} - Part no: {code}"
        _add(description)

    hampton_match = re.search(
        r"(?i)\bHampton\s+Handle,\s*Urban\s+Brass\b(?:[^|]*?\bProduct\s*Code:\s*([A-Z0-9]+)\b)?",
        cleaned,
    )
    if hampton_match:
        product_code = parsing.normalize_space(hampton_match.group(1) or "")
        description = "Hampton Handle, Urban Brass"
        if product_code:
            description = f"{description} - Product Code: {product_code}"
        _add(description)

    allegra_knob_match = re.search(
        r"(?i)\bKnob\b(?:[^|]*?\b(6368-K)\b)(?:[^|]*?\bin\s+(brushed\s+nickel))?",
        cleaned,
    )
    if allegra_knob_match:
        description = f"Knob - {parsing.normalize_space(allegra_knob_match.group(1))}"
        finish = parsing.normalize_space(allegra_knob_match.group(2) or "")
        if finish:
            description = f"{description} in {finish}"
        _add(description)

    for match in re.finditer(
        r"(?i)\b(PM2817\s*/\s*(?:192|288)\s*/\s*MSIL)\b([^|]*?\b(?:Hole\s+centres|OA\s+SIZE)\b[^|]*)",
        cleaned,
    ):
        description = parsing.normalize_space(f"{match.group(1)} {match.group(2)}").strip(" -|;,")
        if description:
            _add(description)

    for match in re.finditer(
        r"(?i)\b(HT576\s*-\s*(?:128|192)\s*-\s*BKO)\b([^|]*?\bDarwen\s+Cabinet\s+Pull\s+Handle\b(?:[^|]*?\bBlack\s+Olive\s+Colour\b)?)",
        cleaned,
    ):
        description = parsing.normalize_space(f"{match.group(1)} {match.group(2)}").strip(" -|;,")
        if description:
            _add(description)

    tall_hinoki_match = re.search(
        r"(?i)\bTall Door Handles?\s*-\s*Momo\s+Hinoki\s+Wood\s+Big\s+D\s*832mm\s+Handle\s+Oak-?\s*(HIN0682\.832\.OAK)\b",
        cleaned,
    )
    if tall_hinoki_match:
        _add(f"Tall Door Handles - Momo Hinoki Wood Big D 832mm Handle Oak-{parsing.normalize_space(tall_hinoki_match.group(1))}")

    high_split_match = re.search(
        r"(?i)\bHigh Split Handle\s*-\s*(?:Momo\s+)?hinoki\s+wood\s+big\s+d\s*416mm\s+handle\s+oak-?\s*(HIN0682\.416\.OAK)\b",
        cleaned,
    )
    if high_split_match:
        _add(f"High Split Handle - Momo Hinoki Wood Big D 416mm Handle Oak-{parsing.normalize_space(high_split_match.group(1))}")

    return candidates


def _imperial_split_handle_semantic_segments(value: str) -> list[str]:
    text = parsing.normalize_space(value)
    if not text:
        return []
    text = re.sub(r"(?i)\bProduct Code:\s*([A-Z0-9]+)\b", r" - Product Code: \1", text)
    marker = "<<HANDLE_SEG>>"
    boundary_patterns = (
        r"(?i)\bUPPER\s*-\s*FINGERPULL(?:\s*\([^)]*\))?",
        r"(?i)\bBASE\s*-\s*BEVEL\s+EDGE\s+FINGERPULL\b",
        r"(?i)\bTALL\s*-\s*[A-Z]\d+\.\d+\.?[A-Z]*\b",
        r"(?i)\bCHUTE\s+DOOR\s*-\s*[A-Z]\d+\.\d+\.?[A-Z]*\b",
        r"(?i)\bTall Door Handles?\s*-\b",
        r"(?i)\bHigh Split Handle\s*-\b",
        r"(?i)\bDRAWERS?\s*-\s*(?=[^|]*(?:handle|pull|knob|finger\s+pull|PM\d+|HT\d+|Momo|Hampton|Lugo|Trianon))",
        r"(?i)\bDOORS?\s*-\s*(?=[^|]*(?:handle|pull|knob|PM\d+|HT\d+|Momo|Hampton|Lugo|Trianon))",
        r"(?i)\bDESK\s*-\s*\d+\s+Voda\s+Profile\s+Handle\b",
        r"(?i)\bBENCHSEAT DRAWERS?\s*-\s*PTO\b",
        r"(?i)\bNo handles?(?:\s+(?:on|to)\s+[A-Za-z ]+)?\b",
        r"(?i)\bRecessed finger space(?:\s+[^|;]+)?\b",
        r"(?i)\bfinger space above(?:\s+[^|;]+)?\b",
        r"(?i)\bTouch catch(?:\s*-\s*Overheads above)?\b",
        r"(?i)\bPush to open\b",
        r"(?i)\bHampton Handle,\s*Urban Brass\b",
        r"(?i)\bKnob\s*-\s*[A-Z0-9.-]*K\b",
    )
    boundary_count = 0
    for pattern in boundary_patterns:
        text, replacements = re.subn(
            pattern,
            lambda match: f" {marker} {parsing.normalize_space(match.group(0))}",
            text,
        )
        boundary_count += replacements

    if boundary_count == 0:
        single_segment = re.sub(r"(?i)\|\s*(SO-2163-[A-Z0-9-]+)\b", r" - \1", text)
        single_segment = re.sub(r"\s*\|\s*", " ", single_segment)
        single_segment = parsing.normalize_space(single_segment).strip(" -|;,")
        return [single_segment] if single_segment else []

    segments = []
    for part in text.split(marker):
        cleaned = parsing.normalize_space(part).strip(" -|;,")
        if not cleaned:
            continue
        if re.fullmatch(r"\[[^\]]+\](?:\s*-\s*)?", cleaned):
            continue
        cleaned = re.sub(r"(?i)\|\s*(SO-2163-[A-Z0-9-]+)\b", r" - \1", cleaned)
        cleaned = re.sub(r"(?i)\|\s*(Product Code:\s*[A-Z0-9]+)\b", r" - \1", cleaned)
        cleaned = re.sub(r"\s*\|\s*", " ", cleaned)
        cleaned = parsing.normalize_space(cleaned).strip(" -|;,")
        if not cleaned:
            continue
        if re.fullmatch(r"(?i)SKU:Part No:\s*[A-Z0-9.]+", cleaned):
            continue
        if re.fullmatch(r"(?i)casters?", cleaned):
            continue
        segments.append(cleaned)
    merged: list[str] = []
    for segment in segments:
        if (
            merged
            and re.fullmatch(r"(?i)Product Code:\s*[A-Z0-9]+(?:\s*-\s*\(?.*?\)?)?", segment)
        ):
            merged[-1] = parsing.normalize_space(f"{merged[-1]} - {segment}")
            continue
        if merged and re.fullmatch(r"(?i)SO-2163-[A-Z0-9-]+", segment):
            merged[-1] = parsing.normalize_space(f"{merged[-1]} - {segment}")
            continue
        merged.append(segment)
    seen: set[str] = set()
    unique_segments: list[str] = []
    for segment in merged:
        key = parsing.normalize_space(segment).upper()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_segments.append(parsing.normalize_space(segment))
    return unique_segments


def _imperial_semantic_handle_summary_candidates(value: str) -> list[str]:
    text = parsing.normalize_space(value)
    if not text:
        return []
    supplier_only_pattern = r"(?i)^(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Momo|Barchie|Lincoln Sentry|ABI Interiors)$"

    def _filter_candidates(candidates: list[str]) -> list[str]:
        filtered: list[str] = []
        for candidate in candidates:
            normalized_candidate = parsing.normalize_space(candidate).strip(" -|;,")
            if not normalized_candidate:
                continue
            if re.fullmatch(supplier_only_pattern, normalized_candidate):
                continue
            if "\n" in normalized_candidate or "\r" in normalized_candidate:
                continue
            if re.fullmatch(r"(?i)SKU:Part No:\s*[A-Z0-9.]+", normalized_candidate):
                continue
            if re.fullmatch(r"(?i)Product Code:\s*[A-Z0-9]+(?:\s*-\s*\(?.*?\)?)?", normalized_candidate):
                continue
            if re.fullmatch(r"(?i)Lugo Knob", normalized_candidate):
                continue
            if re.search(r"(?i)\bHIN0682\.(?:416|832)\.OAK\b", normalized_candidate) and not re.search(
                r"(?i)\b(?:Tall Door Handles?|High Split Handle)\b",
                normalized_candidate,
            ):
                continue
            if re.search(r"(?i)\bbevel\s+edge\s+finger\s+pull\b", normalized_candidate) and re.search(
                r"(?i)\b(?:voda\s+profile\s+handle|so-2163-[a-z0-9-]+)\b",
                normalized_candidate,
            ):
                continue
            if (
                normalized_candidate == "Drawers - Bevel Edge finger pull"
                and re.search(r"(?i)\b\d+\s*mm\s+handle\s+oak-HIN0682\.(?:416|832)\.OAK\b", text)
                and not re.search(r"(?i)\b(?:Tall Door Handles?|High Split Handle)\b", text)
                and re.search(r"(?i)\bDESK\s*-\s*\d+\s+Voda\s+Profile\s+Handle\b", text)
            ):
                continue
            if re.search(r"(?i)\bDOORS?\s*-", normalized_candidate) and re.search(r"(?i)\bDRAWERS?\s*-", normalized_candidate):
                continue
            if re.search(r"(?i)\b(?:handles?\s+base\s+doors|fingerg?pull\s+tall|edge\s+fingerg?pull\s+tall)\b", normalized_candidate):
                continue
            if re.search(r"(?i)\bdarwen\s+cabinet\s+pull\s+handle\b", normalized_candidate) and not re.search(
                r"(?i)\bHT576\s*-\s*(?:128|192)\s*-\s*BKO\b",
                normalized_candidate,
            ):
                continue
            if re.search(r"(?i)\b(?:voda\s+profile\s+handle|so-2163-[a-z0-9-]+)\b", normalized_candidate):
                has_desk_prefix = re.search(r"(?i)\bDESK\s*-\s*", normalized_candidate)
                has_complete_voda_identity = (
                    re.search(r"(?i)\bVoda\s+Profile\s+Handle\b", normalized_candidate)
                    and re.search(r"(?i)\b(?:Brushed\s+Nickel|Matt\s+Black)\b", normalized_candidate)
                    and re.search(r"(?i)\b\d+\s*mm\b", normalized_candidate)
                    and re.search(r"(?i)\bso-2163-[a-z0-9-]+\b", normalized_candidate)
                )
                if not has_desk_prefix and not has_complete_voda_identity:
                    continue
                if not re.search(r"(?i)\bso-2163-[a-z0-9-]+\b", normalized_candidate) and not re.search(r"(?i)\b\d+\s*mm\b", normalized_candidate):
                    continue
            if re.search(r"(?i)\bvs\s+sub\b|\bpull\s+out\b", normalized_candidate) and not re.search(
                r"(?i)\b(?:handle|knob|touch\s+catch|push\s+to\s+open|no\s+handles?)\b",
                normalized_candidate,
            ):
                continue
            if re.search(r"(?i)\bpart no:\s*knobs?\s+on\b", normalized_candidate):
                continue
            if re.search(r"(?i)^Handles?\s*-\s*White\s*&\s*Brushed\s+Nickel\b", normalized_candidate):
                continue
            if re.search(r"(?i)\b(?:floating shelves?|shelf support|coffe/?appliance area)\b", normalized_candidate):
                continue
            if re.search(r"(?i)\b(?:finger space\b|recessed findger space|recessed finger space)\b", normalized_candidate) and not re.search(r"(?i)\b(?:no handles?|touch catch|finger\s+space\s+above|recessed\s+finger\s+space)\b", normalized_candidate):
                continue
            if re.search(r"(?i)^(?:vertical|horizontal)\s+on\s+(?:doors?|drawers?)", normalized_candidate):
                continue
            if re.search(r"(?i)\bHANDLES?\s+BASE\s+DOORS\b", normalized_candidate):
                continue
            if re.search(r"(?i)\b(?:polytec|laminex)\b", normalized_candidate) and not re.search(
                r"(?i)\b(?:handle|knob|pull|finger\s+pull|fingerpull|finger\s+space|recessed\s+finger|push\s+to\s+open|pto|no\s+handles?|touch\s+catch|ht\d+|pm\d+|s225\.|so-\d+|product code|part no|voda|trianon|lugo|hampton)\b",
                normalized_candidate,
            ):
                continue
            if re.search(r"(?i)\bon\s+tall\s+doors\b", normalized_candidate) and not re.search(r"(?i)\b(?:s225\.|hin0682\.|handle|knob|pull)\b", normalized_candidate):
                continue
            if not re.search(
                r"(?i)\b(?:handle|knob|pull|finger\s+pull|fingerpull|finger\s+space|recessed\s+finger|push\s+to\s+open|pto|no\s+handles?|touch\s+catch|ht\d+|pm\d+|s225\.|so-\d+|product code|part no|voda|trianon|lugo|hampton)\b",
                normalized_candidate,
            ):
                continue
            if re.search(r"(?i)\bTCM3622\.WHBRN\.FG\b", normalized_candidate) and re.search(r"(?i)\bLugo\s+Knob\b", normalized_candidate):
                continue
            replaced = False
            for index, existing in enumerate(filtered):
                if _imperial_summary_values_equivalent("handles", existing, normalized_candidate):
                    if _imperial_summary_value_quality("handles", normalized_candidate) > _imperial_summary_value_quality("handles", existing):
                        filtered[index] = normalized_candidate
                    replaced = True
                    break
            if not replaced:
                filtered.append(normalized_candidate)
        pruned: list[str] = []
        for candidate in filtered:
            candidate_upper = candidate.upper()
            if candidate_upper == "KNOB":
                richer = [
                    existing
                    for existing in filtered
                    if existing != candidate and re.search(r"(?i)\b(?:knob\s*-|cabinet\s+knob)\b", existing)
                ]
                if richer:
                    continue
            if re.search(r"(?i)\bLUGO\s+KNOB\b", candidate_upper):
                richer = [
                    existing
                    for existing in filtered
                    if existing != candidate
                    and re.search(r"(?i)\bLUGO\s+KNOB\b", existing)
                    and len(existing) >= len(candidate) + 8
                ]
                if richer:
                    continue
            if re.search(r"(?i)\bTRIANON\s+D\s+HANDLE\b", candidate_upper):
                richer = [
                    existing
                    for existing in filtered
                    if existing != candidate
                    and re.search(r"(?i)\bTRIANON\s+D\s+HANDLE\b", existing)
                    and len(existing) >= len(candidate) + 8
                ]
                if richer:
                    continue
            if re.search(r"(?i)\bPM2817\s*/\s*(192|288)\s*/\s*MSIL\b", candidate_upper):
                richer = [
                    existing
                    for existing in filtered
                    if existing != candidate
                    and re.search(r"(?i)\b" + re.escape(re.search(r"(?i)\bPM2817\s*/\s*(192|288)\s*/\s*MSIL\b", candidate).group(0)) + r"\b", existing)
                    and len(existing) >= len(candidate) + 8
                ]
                if richer:
                    continue
            candidate_identity = _imperial_handle_summary_identity_tokens(candidate)
            if candidate_identity:
                richer = [
                    existing
                    for existing in filtered
                    if existing != candidate
                    and len(existing) >= len(candidate) + 8
                    and candidate_identity & _imperial_handle_summary_identity_tokens(existing)
                    and (
                        candidate.lower() in existing.lower()
                        or _imperial_summary_overlap_ratio(candidate, existing) >= 0.7
                    )
                ]
                if richer:
                    continue
            pruned.append(candidate)
        return pruned

    candidate_pool: list[str] = []

    def _append_candidates(values: list[str]) -> None:
        for candidate in values:
            normalized_candidate = parsing.normalize_space(candidate).strip(" -|;,")
            if normalized_candidate and normalized_candidate not in candidate_pool:
                candidate_pool.append(normalized_candidate)

    _append_candidates(_imperial_known_handle_summary_candidates(text))
    if re.search(r"(?i)\bHT576\s*-\s*(?:128|192)\s*-\s*BKO\b", text):
        normalized = _normalize_imperial_handle_summary_value(text)
        _append_candidates([normalized] if normalized else [])
    split_candidates = [
        normalized
        for normalized in (
            _normalize_imperial_handle_summary_value(segment)
            for segment in _imperial_split_handle_semantic_segments(text)
        )
        if normalized and not re.match(r"(?i)^Product Code:\s*", normalized)
    ]
    if split_candidates:
        seen_split: set[str] = set()
        unique_split: list[str] = []
        for candidate in split_candidates:
            key = parsing.normalize_space(candidate).upper()
            if not key or key in seen_split:
                continue
            seen_split.add(key)
            unique_split.append(parsing.normalize_space(candidate))
        _append_candidates(unique_split)
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
        r"(?i)\bDRAWERS?\s*-\s*[^|]*?(?:handle|pull|knob|finger\s+pull|PM\d+|HT\d+|Momo|Hampton|Lugo|Trianon)[^|]*",
        r"(?i)\bDOORS?\s*-\s*[^|]*?(?:handle|pull|knob|PM\d+|HT\d+|Momo|Hampton|Lugo|Trianon)[^|]*",
        r"(?i)\bHampton Handle,\s*Urban Brass(?:[^|]*?\bProduct Code:\s*[A-Z0-9]+\b)?",
        r"(?i)\bDrawers?\s*-\s*Bevel Edge finger pull\b",
        r"(?i)\bBevel edge finger pull(?:\s+on\s+lowers)?\b",
        r"(?i)\bKnob\s*-\s*[A-Z0-9.-]*K\b[^|]*",
        r"(?i)\b(?:Handles?\s*-\s*)?(?:Barchie\s+)?Woodgate\s+Round\s+Cabinet\s+Knob(?:\s*\|\s*SKU:Part No:\s*[A-Z0-9.]+)?\b",
        r"(?i)\bDESK\s*-\s*\d+\s+Voda\s+Profile\s+Handle\s+(?:Brushed\s+Nickel|Matt\s+Black)\s+\d+\s*mm\s*-\s*SO-2163-[A-Z0-9-]+\b",
        r"(?i)\bBENCHSEAT DRAWERS?\s*-\s*PTO\b",
        r"(?i)\bPush to open\b",
        r"(?i)\bNo handles?(?:\s+(?:on|to)\s+[A-Za-z ]+)?\b",
        r"(?i)\bRecessed finger space(?:\s+[^|;]+)?\b",
        r"(?i)\bfinger space above(?:\s+[^|;]+)?\b",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            normalized = _normalize_imperial_handle_summary_value(match.group(0))
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    _append_candidates(candidates)
    whole_normalized = _normalize_imperial_handle_summary_value(text)
    if whole_normalized and re.search(
        r"(?i)\b(?:pm\d+[a-z0-9 /.-]*|hole centres|oa size|matt silver|touch catch|no handles?|push to open|bevel edge finger pull)\b",
        text,
    ):
        _append_candidates([parsing.normalize_space(re.sub(r"\s*\|\s*", " ", whole_normalized))])
    fallback_values = [
        normalized
        for normalized in (
            _normalize_imperial_handle_summary_value(part)
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
    _append_candidates(fallback_values)
    filtered_candidates = _filter_candidates(candidate_pool)
    if any(
        re.search(
            r"(?i)\b(?:no\s+handles?\s+on\s+upper\s+cabinetry|no\s+handles?\s+overheads?)\b",
            candidate,
        )
        for candidate in filtered_candidates
    ):
        filtered_candidates = [
            candidate for candidate in filtered_candidates if candidate.lower() != "no handles"
        ] or filtered_candidates
    return filtered_candidates


def _normalize_benchtop_summary_value(value: str) -> str:
    text = parsing.normalize_space(value)
    text = re.sub(r"(?i)^back benchtops?\s*", "", text)
    text = re.sub(r"(?i)^wall run bench top\s*", "", text)
    text = re.sub(r"(?i)^island bench top\s*", "", text)
    text = re.sub(r"(?i)^island benchtop\s*", "", text)
    text = re.sub(
        r"(?i)\s*-\s*\(\s*(?:sink\s*-\s*)?(?:um\s*sink|undermount\s+sink|topmount\s+sink)\s*\)\s*$",
        "",
        text,
    )
    text = re.sub(r"(?i)\bWFE'?S?\s*x\s*\d+\b.*$", "", text)
    text = re.sub(r"(?i)\b(?:um\s*sink|undermount\s+sink)\b.*$", "", text)
    text = re.sub(r"(?i)\b(?:undermount|top\s*mount)\b(?:\s*-\s*sink)?\s*$", "", text)
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
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\s*-\s*\($", "", text)
    text = re.sub(r"\s*\|\s*$", "", text)
    return text.strip(" -;,/|")


def _imperial_door_colour_text_is_valid_feature_cabinetry(raw_value: str) -> bool:
    text = parsing.normalize_space(raw_value)
    if not text:
        return False
    return bool(
        re.search(r"(?i)\b(?:feature\s+cabinetry|shaving\s+cabinet|mirro(?:r)?red\s+doors?|colourboard\s+shelf)\b", text)
    )


def _imperial_summary_material_supplier(
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


def _imperial_summary_material_candidates(
    bucket_key: str,
    raw_value: str,
    supplier: str,
) -> list[str]:
    text = parsing.normalize_space(raw_value)
    if not text or bucket_key not in {"door_colours", "bench_tops"}:
        return []
    text = re.sub(r"^\[[^\]]+\]\s*-\s*", "", text).strip()
    analysis_text = parsing.normalize_space(
        re.sub(r"(?i)\((Vertical Grain|Horizontal Grain)\)", r"\1", text)
    )
    if bucket_key == "door_colours":
        analysis_text = parsing.normalize_space(re.sub(r"\([^)]*\)", " ", analysis_text))
    candidates: list[str] = []
    colour_code_patterns = (
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*Natural\s*-\s*Colour Code:\s*(\d{2,4})\b",
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s+Natural Colour Code:\s*(\d{2,4})\b",
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*Colour Code:\s*(\d{2,4})\b",
    )
    for pattern in colour_code_patterns:
        for match in re.finditer(pattern, analysis_text):
            material = parsing.normalize_space(match.group(1))
            code = parsing.normalize_space(match.group(2))
            if not material or not code:
                continue
            if material.upper() in {"COLOUR", "CODE", "NATURAL"}:
                continue
            supplier_hint = _imperial_summary_material_supplier(
                bucket_key,
                text,
                supplier,
                material_kind="colour_code",
            )
            composed = f"{supplier_hint + ' - ' if supplier_hint else ''}{material} Natural Colour Code: {code}"
            normalized = _normalize_benchtop_summary_value(composed) if bucket_key == "bench_tops" else _normalize_door_colour_summary_value(composed)
            if normalized and normalized not in candidates:
                candidates.append(normalized)
    woodmatt_pattern = re.compile(
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s*-\s*(Woodmatt|Matt)(?:\s*\([^)]*\))?(?:\s*-\s*(Vertical Grain|Horizontal Grain))?\b"
    )
    for match in woodmatt_pattern.finditer(analysis_text):
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
        supplier_hint = _imperial_summary_material_supplier(
            bucket_key,
            text,
            supplier,
            material_kind="woodmatt",
        )
        composed_parts = [part for part in (supplier_hint, material, finish, grain) if part]
        composed = " - ".join(composed_parts)
        normalized = _normalize_benchtop_summary_value(composed) if bucket_key == "bench_tops" else _normalize_door_colour_summary_value(composed)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    finish_suffix_pattern = re.compile(
        r"(?i)\b([A-Za-z][A-Za-z ]+?)\s+(Woodmatt|Matt)\b(?:\s*-\s*(Style\s+\d+\s*-\s*[A-Za-z0-9 ]+))?(?:\s*-\s*(Vertical Grain|Horizontal Grain))?\b"
    )
    for match in finish_suffix_pattern.finditer(analysis_text):
        material = parsing.normalize_space(match.group(1))
        finish = parsing.normalize_space(match.group(2))
        style = parsing.normalize_space(match.group(3) or "")
        grain = parsing.normalize_space(match.group(4) or "")
        if not material or material.upper() in {"STYLE", "PROFILE", "NATURAL", "STANDARD"}:
            continue
        supplier_hint = _imperial_summary_material_supplier(
            bucket_key,
            analysis_text,
            supplier,
            material_kind="woodmatt",
        )
        material_with_finish = parsing.normalize_space(f"{material} {finish}")
        composed = " - ".join(part for part in (supplier_hint, material_with_finish, style, grain) if part)
        normalized = _normalize_benchtop_summary_value(composed) if bucket_key == "bench_tops" else _normalize_door_colour_summary_value(composed)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    textured_finish_pattern = re.compile(
        r"(?i)\b([A-Za-z][A-Za-z' ]+?)\s*-\s*(Venette|Ravine)\b"
    )
    for match in textured_finish_pattern.finditer(analysis_text):
        material = parsing.normalize_space(match.group(1))
        finish = parsing.normalize_space(match.group(2))
        if not material or material.upper() in {"STYLE", "PROFILE", "NATURAL", "STANDARD"}:
            continue
        material = re.sub(
            r"(?i)^.*?\b(?:reface\s+all|doors?|drawers?|panels?|single\s+hanging|robe\s+hatshelf\s+on\s+support\s+rails)\b\s*",
            "",
            material,
        ).strip(" -;,/")
        material = parsing.normalize_space(material)
        if not material:
            continue
        supplier_hint = _imperial_summary_material_supplier(
            bucket_key,
            text,
            supplier,
            material_kind="woodmatt",
        )
        composed = " - ".join(part for part in (supplier_hint, material, finish) if part)
        normalized = _normalize_benchtop_summary_value(composed) if bucket_key == "bench_tops" else _normalize_door_colour_summary_value(composed)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    stone_pattern = re.compile(
        r"(?is)\b(\d+\s*mm\s+Stone)\b.*?\b(\d{3,4}\s+[A-Za-z][A-Za-z ]+?)\s*-\s*([A-Z]{1,4})\b"
    )
    for match in stone_pattern.finditer(analysis_text):
        thickness = parsing.normalize_space(match.group(1))
        material = parsing.normalize_space(match.group(2))
        code = parsing.normalize_space(match.group(3))
        if not thickness or not material or not code:
            continue
        supplier_hint = _imperial_summary_material_supplier(
            bucket_key,
            text,
            supplier,
            material_kind="stone",
        )
        composed = " - ".join(part for part in (supplier_hint, thickness, f"{material} - {code}") if part)
        normalized = _normalize_benchtop_summary_value(composed) if bucket_key == "bench_tops" else _normalize_door_colour_summary_value(composed)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    if bucket_key == "door_colours":
        candidates = [
            candidate
            for candidate in candidates
            if not re.search(r"(?i)(?:^|\s+-\s+)(?:variation\s+for|for)\b", candidate)
        ]
    return candidates


def _imperial_summary_values_for_bucket(
    bucket_key: str,
    raw_value: str,
    normalizer: Any,
    *,
    supplier: str = "",
) -> list[str]:
    values: list[str] = []
    candidates = [raw_value]
    if bucket_key == "handles":
        handle_text = parsing.normalize_space(raw_value)
        semantic_candidates = _imperial_semantic_handle_summary_candidates(handle_text)
        if semantic_candidates:
            candidates = semantic_candidates
    elif bucket_key in {"door_colours", "bench_tops"}:
        material_candidates = _imperial_summary_material_candidates(bucket_key, raw_value, supplier)
        if bucket_key == "bench_tops" and material_candidates:
            normalized_raw = _display_value(normalizer(raw_value) or raw_value)
            normalized_material_candidates = [
                _display_value(normalizer(candidate) or candidate)
                for candidate in material_candidates
                if _display_value(normalizer(candidate) or candidate)
            ]
            if (
                "PORCELAIN" in normalized_raw.upper()
                and not any("PORCELAIN" in candidate.upper() for candidate in normalized_material_candidates)
            ):
                candidates = [raw_value, *material_candidates]
            else:
                candidates = material_candidates
        elif material_candidates:
            candidates = material_candidates
        elif re.search(
            r"(?i)\b(?:raw\s+gyprock|sliding\s+robe\s+doors|standard\s+whiteboard\s+internals|robe\s+hatshelf|single\s+hanging|adjustable\s+shelves)\b",
            raw_value,
        ) and not (
            bucket_key == "door_colours"
            and _imperial_door_colour_text_is_valid_feature_cabinetry(raw_value)
        ):
            candidates = []
        elif bucket_key == "bench_tops" and re.search(r"(?i)\b(?:cut-?outs?|GPO'?S?)\b", raw_value):
            candidates = []
    for candidate in candidates:
        normalized_candidate = normalizer(candidate)
        normalized = _display_value(
            normalized_candidate if normalized_candidate else ("" if bucket_key == "handles" else candidate)
        )
        if not normalized or normalized in values:
            continue
        if bucket_key == "handles" and re.match(r"(?i)^(?:pto|drawers?|benchseat)$", normalized):
            continue
        if bucket_key == "handles" and normalized.lower() == "none":
            continue
        if bucket_key == "handles" and re.match(
            r"(?i)^(?:base|upper|overhead|tall|pantry|chute)\s+(?:doors?|drawers?|cabs?|cabinets?)$",
            normalized,
        ):
            continue
        if bucket_key == "handles" and re.match(
            r"(?i)^(?:hanldes?|handles?)\b|^(?:kickboards?|lighting|shelves?|desk grommets?|accessor(?:y|ies)|bin|gpo)\b$",
            normalized,
        ):
            continue
        if bucket_key == "handles" and re.fullmatch(
            r"(?i)(?:Furnware|Titus Tekform|Polytec|Laminex|Kethy|Allegra|Momo|Barchie|Lincoln Sentry|ABI Interiors)",
            normalized,
        ):
            continue
        if bucket_key == "handles" and not _imperial_handle_summary_has_handle_identity(normalized):
            continue
        if bucket_key == "handles" and re.search(r"(?i)\b(?:polytec|laminex)\b", normalized) and not re.search(
            r"(?i)\b(?:handle|knob|pull|finger\s+pull|fingerpull|finger\s+space|recessed\s+finger|push\s+to\s+open|pto|no\s+handles?|touch\s+catch|ht\d+|pm\d+|s225\.|so-\d+|product code|part no|voda|trianon|lugo|hampton)\b",
            normalized,
        ):
            continue
        values.append(normalized)
    if bucket_key == "handles" and len(values) > 1:
        if re.search(
            r"(?i)\b(?:desk\b|voda\s+profile\s+handle|high\s+split\s+handle|benchseat\s+drawers?\s*-\s*pto|so-[a-z0-9-]+)\b",
            handle_text,
        ):
            values = [value for value in values if value != "Bevel Edge finger pull"] or values
        values = [value for value in values if value not in {"No handles", "Push to open"}] or values
        if any(re.search(r"(?i)\bknob\s*-|\bcabinet\s+knob\b", value) for value in values):
            values = [value for value in values if value.lower() != "knob"] or values
        if any(re.search(r"(?i)^No handles to overheads\s+-\s+", value) for value in values):
            values = [value for value in values if value != "No handles to overheads"] or values
        if any(
            re.search(r"(?i)^No handles(?:\s+to)?\s+overheads?\s+-\s+.*\btouch\s+catch\b", value)
            for value in values
        ):
            values = [
                value
                for value in values
                if value not in {"No handles to overheads", "Touch catch"}
            ] or values
    return values


def _imperial_material_row_handle_subitem_summary_candidates(item: dict[str, Any]) -> list[str]:
    if not isinstance(item, dict):
        return []
    subitems = item.get("handle_subitems", [])
    if not isinstance(subitems, list):
        return []
    supplier = _display_value(item.get("supplier", ""))
    candidates: list[str] = []
    for subitem in subitems:
        if not isinstance(subitem, dict):
            continue
        source_texts: list[str] = []
        for key in ("summary_text", "text"):
            source_text = _display_value(subitem.get(key, ""))
            if source_text and source_text not in source_texts:
                source_texts.append(source_text)
        for source_text in source_texts:
            for summary_text in _imperial_summary_values_for_bucket(
                "handles",
                source_text,
                _normalize_imperial_handle_summary_value,
                supplier=supplier,
            ):
                if not summary_text:
                    continue
                existing_index = next(
                    (
                        index
                        for index, existing in enumerate(candidates)
                        if _imperial_summary_values_equivalent("handles", summary_text, existing)
                    ),
                    None,
                )
                if existing_index is None:
                    candidates.append(summary_text)
                elif _imperial_summary_value_quality("handles", summary_text) > _imperial_summary_value_quality(
                    "handles",
                    candidates[existing_index],
                ):
                    candidates[existing_index] = summary_text
    return candidates


def _imperial_material_row_handle_summary_candidates(item: dict[str, Any]) -> list[str]:
    if not isinstance(item, dict):
        return []
    subitem_candidates = _imperial_material_row_handle_subitem_summary_candidates(item)
    if subitem_candidates:
        return subitem_candidates
    supplier = _display_value(item.get("supplier", ""))
    display_lines = [
        _display_value(line)
        for line in item.get("display_lines", []) or []
        if _display_value(line)
    ]
    summary_sources: list[str] = []
    if display_lines:
        summary_sources.extend(display_lines)
    display_value = _display_value(item.get("display_value", "")) or _display_value(item.get("value", ""))
    raw_composed_value = parsing._compose_supplier_description_note(
        _display_value(item.get("supplier", "")),
        _display_value(item.get("specs_or_description", "")),
        _display_value(item.get("notes", "")),
    )
    display_line_candidates: list[str] = []
    for line in display_lines:
        for candidate in _imperial_summary_values_for_bucket(
            "handles",
            line,
            _normalize_imperial_handle_summary_value,
            supplier=supplier,
        ):
            if candidate and candidate not in display_line_candidates:
                display_line_candidates.append(candidate)
    if not display_value:
        display_value = raw_composed_value
    combined_candidates: list[str] = []
    for combined_source in (display_value, raw_composed_value):
        if not combined_source:
            continue
        for candidate in _imperial_summary_values_for_bucket(
            "handles",
            combined_source,
            _normalize_imperial_handle_summary_value,
            supplier=supplier,
        ):
            if candidate and candidate not in combined_candidates:
                combined_candidates.append(candidate)
    display_line_families = parsing._imperial_handle_display_line_family_keys(display_line_candidates)
    combined_families = parsing._imperial_handle_display_line_family_keys(combined_candidates)
    display_line_identity: set[str] = set()
    for candidate in display_line_candidates:
        display_line_identity.update(_imperial_handle_summary_identity_tokens(candidate))
    combined_nonmatching = [
        candidate
        for candidate in combined_candidates
        if not any(_imperial_summary_values_equivalent("handles", candidate, existing) for existing in display_line_candidates)
    ]
    meaningful_uncoded_handle_family_pattern = (
        r"(?i)\b(?:"
        r"bevel\s+edge\s+finger\s+pull|"
        r"drawers?\s*-\s*bevel\s+edge\s+finger\s+pull|"
        r"no\s+handles?(?:\s+on\s+[a-z ]+)?|"
        r"finger\s+pull\s+only|"
        r"touch\s+catch|"
        r"push\s+to\s+open|"
        r"pto|"
        r"knob(?:\s*-|$)|"
        r"cabinet\s+knob"
        r")\b"
    )
    if display_line_candidates and combined_nonmatching:
        coded_display_candidates = [
            candidate
            for candidate in display_line_candidates
            if re.search(r"(?i)\b(?:SO-[A-Z0-9-]+|[A-Z]?\d+\.[A-Z0-9.]+|Product Code:\s*[A-Z0-9]+|Part no:\s*[A-Z0-9.]+)\b", candidate)
        ]
        if coded_display_candidates and all(
            not re.search(r"(?i)\b(?:SO-[A-Z0-9-]+|[A-Z]?\d+\.[A-Z0-9.]+|Product Code:\s*[A-Z0-9]+|Part no:\s*[A-Z0-9.]+)\b", candidate)
            for candidate in combined_nonmatching
        ) and not any(
            re.search(meaningful_uncoded_handle_family_pattern, candidate)
            for candidate in combined_nonmatching
        ):
            return display_line_candidates
    generic_handle_noise_pattern = (
        r"(?i)^(?:HANLDES?|HANDLES?)\b|"
        r"\b(?:no\s+handles?(?:\s+on\s+[a-z ]+)?|finger\s+pull\s+only|touch\s+catch|push\s+to\s+open|kickboards?)\b"
    )
    if display_line_candidates and combined_nonmatching:
        if display_line_identity and all(
            (
                _imperial_handle_summary_identity_tokens(candidate)
                and _imperial_handle_summary_identity_tokens(candidate) <= display_line_identity
            )
            or re.search(generic_handle_noise_pattern, candidate)
            for candidate in combined_nonmatching
        ):
            return display_line_candidates
        if all(re.search(generic_handle_noise_pattern, candidate) for candidate in combined_nonmatching):
            return display_line_candidates
    use_display_lines_as_truth = (
        bool(display_line_candidates)
        and (
            (
                len(display_lines) >= 2
                and (
                    not combined_candidates
                    or all(
                        any(_imperial_summary_values_equivalent("handles", candidate, existing) for existing in display_line_candidates)
                        for candidate in combined_candidates
                    )
                )
            )
            or (
                display_line_families
                and combined_families
                and combined_families <= display_line_families
            )
            or any(
                parsing._imperial_handle_text_has_foreign_section_pollution(source_text)
                for source_text in (display_value, raw_composed_value)
                if source_text
            )
            or (
                bool(display_line_identity)
                and any(
                    re.search(
                        r"(?i)\b(?:no\s+handles?(?:\s+on\s+[a-z ]+)?|finger\s+pull\s+only|touch\s+catch|push\s+to\s+open)\b",
                        candidate,
                    )
                    for candidate in combined_nonmatching
                )
            )
        )
    )
    if display_value and not use_display_lines_as_truth:
        summary_sources.append(display_value)
    if raw_composed_value and raw_composed_value not in summary_sources and not use_display_lines_as_truth:
        summary_sources.append(raw_composed_value)
    if _imperial_material_row_is_handle_summary_review_fallback(item):
        if (not display_lines) or _imperial_handle_summary_should_append_fallback_sources(item, display_lines):
            for fallback_source in _imperial_handle_summary_fallback_sources(item):
                if fallback_source and fallback_source not in summary_sources:
                    summary_sources.append(fallback_source)
    candidates: list[str] = []
    for source_text in summary_sources:
        for summary_text in _imperial_summary_values_for_bucket(
            "handles",
            source_text,
            _normalize_imperial_handle_summary_value,
            supplier=supplier,
        ):
            if summary_text and summary_text not in candidates:
                candidates.append(summary_text)
    pruned_candidates: list[str] = []
    for candidate in candidates:
        candidate_identity = _imperial_handle_summary_identity_tokens(candidate)
        candidate_quality = _imperial_summary_value_quality("handles", candidate)
        richer = [
            existing
            for existing in candidates
            if existing != candidate
            and _imperial_summary_value_quality("handles", existing) > candidate_quality + 1.0
            and (
                (
                    candidate_identity
                    and candidate_identity & _imperial_handle_summary_identity_tokens(existing)
                )
                or candidate.lower() in existing.lower()
                or _imperial_summary_overlap_ratio(candidate, existing) >= 0.72
            )
        ]
        if richer:
            continue
        pruned_candidates.append(candidate)
    candidates = pruned_candidates
    if any(re.search(r"(?i)\bknob\s*-|\bcabinet\s+knob\b", candidate) for candidate in candidates):
        candidates = [candidate for candidate in candidates if candidate.lower() != "knob"] or candidates
    return candidates


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
