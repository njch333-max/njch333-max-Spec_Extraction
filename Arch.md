# Spec_Extraction Architecture

## 1. Top-Level Structure
- `App/main.py`: FastAPI entrypoint, routes, middleware, page rendering
- `App/services/runtime.py`: environment loading, path setup, atomic file helpers
- `App/services/store.py`: SQLite schema and persistence helpers
- `App/services/auth.py`: password hashing, session auth, CSRF helpers
- `App/services/parsing.py`: PDF and DOCX text extraction, room normalization, heuristic parsing
- `App/services/extraction_service.py`: raw extraction orchestration, optional OpenAI call, enrichment helpers
- `App/services/export_service.py`: Excel and CSV export generation
- `App/services/worker.py`: queue polling and job execution loop
- `App/templates/`: Jinja templates
- `App/static/`: CSS
- `App/scripts/`: local run scripts and Linux systemd templates
- `tools/`: Git helper scripts

## 2. Runtime Layout
- Default project root: the `Spec_Extraction` folder
- Default data root for local development: `App/data/`
- Production path override via env: `/var/lib/builder-spec-extractor/`
- Main database: SQLite file inside the configured data root
- File storage:
  - `templates/{builder_slug}/`
  - `jobs/{job_no}/spec/`
  - `jobs/{job_no}/drawings/`
  - `jobs/{job_no}/exports/`

## 3. Main Components

### 3.1 Web App
- FastAPI with session middleware
- Jinja templates for English-only UI
- Static CSS served by FastAPI
- Form-based actions with CSRF token checks

### 3.2 Persistence
- SQLite tables:
  - `builders`
  - `builder_templates`
  - `jobs`
  - `job_files`
  - `runs`
  - `snapshots`
  - `reviews`
  - `auth_events`
- All data access goes through `store.py`

### 3.3 Worker
- Separate Python process
- Polls queued runs from SQLite
- Processes:
  - spec extraction jobs
  - drawing parsing jobs
- Writes raw results and run state back into SQLite

### 3.4 Extraction Pipeline
1. Read uploaded files from job folders.
2. Extract text from PDF or DOCX.
3. Flag low-text PDF pages for OCR/vision fallback.
4. Run heuristic extraction into canonical schema.
5. If OpenAI is enabled, send consolidated text and template context for higher-quality structured output.
6. Enrich links with brand-domain defaults and search hints.
7. Save the raw snapshot.

### 3.5 Review Pipeline
1. Load latest raw snapshot.
2. Load reviewed snapshot if present.
3. Render flattened rows into editable HTML tables.
4. Save edited values as a reviewed snapshot.
5. Export from the reviewed snapshot if present, otherwise from raw snapshot.

## 4. Canonical Schema

### Rooms
- One row per normalized room
- Array-like fields are stored as lists in JSON and flattened with ` | ` in the review UI
- `room_key` is normalized, `original_room_label` preserves the original label

### Appliances
- One row per appliance
- Each row carries source metadata and confidence

### Others
- Free-form notes and extraction diagnostics

### Drawing Snapshot
- Stored separately from the spec snapshot
- Uses the same field naming so future comparison logic can diff field-to-field

## 5. Security Model
- Single admin account from environment variables
- Session cookie signed by `SPEC_EXTRACTION_SECRET_KEY`
- CSRF token stored in session and checked on POST forms
- All business routes require login
- Uploads are stored on disk and referenced from SQLite

## 6. Deployment Model
- Web process bound to `127.0.0.1:8010`
- Reverse proxy terminates TLS and forwards the app subdomain
- Web and worker each get a `systemd` unit
- Recommended domain shape: `specs.<existing-lxtransport-domain>`

## 7. Implemented Route Map
- `GET /`: redirect to login or jobs
- `GET/POST /login`
- `POST /logout`
- `GET/POST /builders`
- `POST /builders/{builder_id}/templates`
- `POST /templates/{template_id}/delete`
- `GET /builders/templates/{template_id}/download`
- `GET/POST /jobs`
- `GET /jobs/{job_id}`
- `POST /jobs/{job_id}/files/upload`
- `POST /jobs/files/{file_id}/delete`
- `GET /jobs/files/{file_id}/download`
- `POST /jobs/{job_id}/runs/start`
- `POST /jobs/{job_id}/review/save`
- `POST /jobs/{job_id}/export`
- `GET /jobs/{job_id}/exports/{file_name}`
- `GET /api/health`

## 8. Git Tooling
- `git-setup.ps1`: install Git if needed and initialize repo
- `checkpoint.ps1`: stage and commit changes, with major-change doc guard
- `history.ps1`: compact history viewer
- `restore.ps1`: create a restore branch from a previous ref

## 9. Change Management
Major changes must update:
- `PRD.md`
- `Arch.md`
- `Project_state.md`

This rule is enforced by the major-change checkpoint script.
