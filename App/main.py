from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from App.services import store
from App.services.auth import authenticate, current_user, ensure_csrf_token, login_user, logout_user, verify_csrf
from App.services.export_service import build_exports
from App.services.runtime import (
    HOST_DOMAIN,
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
    https_only=False,
    domain=SESSION_DOMAIN or None,
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.get("/")
def root(request: Request):
    if current_user(request):
        return RedirectResponse("/jobs", status_code=303)
    return RedirectResponse("/login", status_code=303)


@app.get("/login")
def login_page(request: Request):
    if current_user(request):
        return RedirectResponse("/jobs", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        _context(request, "Login"),
    )


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
            builder["templates"] = store.list_builder_templates(int(builder["id"]))
        return templates.TemplateResponse("builders.html", _context(request, "Builders", builders=builders))
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
    jobs = store.list_jobs()
    builders = store.list_builders()
    return templates.TemplateResponse("jobs.html", _context(request, "Jobs", jobs=jobs, builders=builders))


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


@app.get("/jobs/{job_id}")
def job_detail_page(request: Request, job_id: int):
    if not _require_page_user(request):
        return RedirectResponse("/login", status_code=303)
    job = store.get_job(job_id)
    if not job:
        return RedirectResponse("/jobs", status_code=303)
    dirs = ensure_job_dirs(job["job_no"])
    spec_files = store.list_job_files(job_id, "spec")
    drawing_files = store.list_job_files(job_id, "drawing")
    runs = store.list_runs(job_id)
    raw_snapshot_row = store.get_snapshot(job_id, "raw_spec")
    drawing_snapshot_row = store.get_snapshot(job_id, "drawing")
    review_row = store.get_review(job_id)
    review_snapshot = review_row["data"] if review_row else (raw_snapshot_row["data"] if raw_snapshot_row else _blank_snapshot(job))
    exports = _list_export_files(dirs["export_dir"])
    return templates.TemplateResponse(
        "job_detail.html",
        _context(
            request,
            f"Job {job['job_no']}",
            job=job,
            spec_files=spec_files,
            drawing_files=drawing_files,
            runs=runs,
            raw_snapshot=raw_snapshot_row["data"] if raw_snapshot_row else None,
            drawing_snapshot=drawing_snapshot_row["data"] if drawing_snapshot_row else None,
            review_snapshot=review_snapshot,
            room_rows=_flatten_rooms(review_snapshot),
            appliance_rows=_flatten_appliances(review_snapshot),
            exports=exports,
        ),
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
    run_id = store.create_run(job_id, run_kind)
    _set_flash(request, "success", f"Run #{run_id} queued for {run_kind}.")
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
    dirs = ensure_job_dirs(job["job_no"])
    path = dirs["export_dir"] / Path(file_name).name
    return FileResponse(path, filename=path.name)


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
    }
    ctx.update(extra)
    return ctx


def _set_flash(request: Request, level: str, message: str) -> None:
    request.session["flash"] = {"level": level, "message": message}


def _require_page_user(request: Request) -> str | None:
    return current_user(request)


def _guard_upload_size(size_bytes: int) -> None:
    if size_bytes > MAX_UPLOAD_MB * 1024 * 1024:
        raise ValueError(f"Upload exceeds {MAX_UPLOAD_MB} MB.")


def _flatten_rooms(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("rooms", []):
        rows.append(
            {
                "room_key": row.get("room_key", ""),
                "original_room_label": row.get("original_room_label", ""),
                "bench_tops": " | ".join(row.get("bench_tops", [])),
                "door_panel_colours": " | ".join(row.get("door_panel_colours", [])),
                "toe_kick": " | ".join(row.get("toe_kick", [])),
                "bulkheads": " | ".join(row.get("bulkheads", [])),
                "handles": " | ".join(row.get("handles", [])),
                "drawers_soft_close": row.get("drawers_soft_close", ""),
                "hinges_soft_close": row.get("hinges_soft_close", ""),
                "splashback": row.get("splashback", ""),
                "flooring": row.get("flooring", ""),
                "source_file": row.get("source_file", ""),
                "page_refs": row.get("page_refs", ""),
                "evidence_snippet": row.get("evidence_snippet", ""),
                "confidence": row.get("confidence", ""),
            }
        )
    return rows


def _flatten_appliances(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in snapshot.get("appliances", []):
        rows.append(
            {
                "appliance_type": row.get("appliance_type", ""),
                "make": row.get("make", ""),
                "model_no": row.get("model_no", ""),
                "website_url": row.get("website_url", ""),
                "overall_size": row.get("overall_size", ""),
                "source_file": row.get("source_file", ""),
                "page_refs": row.get("page_refs", ""),
                "evidence_snippet": row.get("evidence_snippet", ""),
                "confidence": row.get("confidence", ""),
            }
        )
    return rows


def _review_payload_from_form(base: dict[str, Any], form: Any) -> dict[str, Any]:
    rooms: list[dict[str, Any]] = []
    room_count = int(form.get("room_count", 0) or 0)
    for index in range(room_count):
        rooms.append(
            {
                "room_key": str(form.get(f"room_key_{index}", "")),
                "original_room_label": str(form.get(f"original_room_label_{index}", "")),
                "bench_tops": _split_pipe(str(form.get(f"bench_tops_{index}", ""))),
                "door_panel_colours": _split_pipe(str(form.get(f"door_panel_colours_{index}", ""))),
                "toe_kick": _split_pipe(str(form.get(f"toe_kick_{index}", ""))),
                "bulkheads": _split_pipe(str(form.get(f"bulkheads_{index}", ""))),
                "handles": _split_pipe(str(form.get(f"handles_{index}", ""))),
                "drawers_soft_close": str(form.get(f"drawers_soft_close_{index}", "")),
                "hinges_soft_close": str(form.get(f"hinges_soft_close_{index}", "")),
                "splashback": str(form.get(f"splashback_{index}", "")),
                "flooring": str(form.get(f"flooring_{index}", "")),
                "source_file": str(form.get(f"source_file_{index}", "")),
                "page_refs": str(form.get(f"page_refs_{index}", "")),
                "evidence_snippet": str(form.get(f"evidence_snippet_{index}", "")),
                "confidence": _safe_float(form.get(f"confidence_{index}", "")),
            }
        )

    appliances: list[dict[str, Any]] = []
    appliance_count = int(form.get("appliance_count", 0) or 0)
    for index in range(appliance_count):
        appliances.append(
            {
                "appliance_type": str(form.get(f"appliance_type_{index}", "")),
                "make": str(form.get(f"make_{index}", "")),
                "model_no": str(form.get(f"model_no_{index}", "")),
                "website_url": str(form.get(f"website_url_{index}", "")),
                "overall_size": str(form.get(f"overall_size_{index}", "")),
                "source_file": str(form.get(f"appliance_source_file_{index}", "")),
                "page_refs": str(form.get(f"appliance_page_refs_{index}", "")),
                "evidence_snippet": str(form.get(f"appliance_evidence_snippet_{index}", "")),
                "confidence": _safe_float(form.get(f"appliance_confidence_{index}", "")),
            }
        )

    return {
        "job_no": base.get("job_no", ""),
        "builder_name": base.get("builder_name", ""),
        "source_kind": base.get("source_kind", "spec"),
        "generated_at": utc_now_iso(),
        "rooms": rooms,
        "appliances": appliances,
        "others": {
            "flooring_notes": str(form.get("others_flooring_notes", "")),
            "splashback_notes": str(form.get("others_splashback_notes", "")),
            "manual_notes": str(form.get("others_manual_notes", "")),
        },
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
        "rooms": [],
        "appliances": [],
        "others": {"flooring_notes": "", "splashback_notes": "", "manual_notes": ""},
        "warnings": [],
        "source_documents": [],
    }


def _list_export_files(export_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not export_dir.exists():
        return rows
    for path in sorted(export_dir.glob("*"), key=lambda item: item.stat().st_mtime, reverse=True):
        if path.is_file():
            stat = path.stat()
            rows.append({"name": path.name, "size_bytes": stat.st_size, "modified": stat.st_mtime})
    return rows
