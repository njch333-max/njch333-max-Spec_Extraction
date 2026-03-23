# Spec_Extraction Architecture

## 1. Top-Level Structure
- `App/main.py`: FastAPI entrypoint, routes, middleware, page rendering
- `App/services/runtime.py`: environment loading, path setup, atomic file helpers
- `App/services/store.py`: SQLite schema and persistence helpers
- `App/services/auth.py`: password hashing, session auth, CSRF helpers
- `App/services/parsing.py`: PDF and DOCX text extraction, room normalization, heuristic parsing
- `App/services/extraction_service.py`: raw extraction orchestration, optional OpenAI call, enrichment helpers
- `App/services/appliance_official.py`: official appliance product/spec/manual lookup and official-size extraction
- `App/services/export_service.py`: Excel and CSV export generation
- `App/services/worker.py`: queue polling and job execution loop
- `App/templates/`: Jinja templates
- `App/templates/spec_list.html`: dedicated single-job raw Spec List page
- `App/static/`: CSS
- `App/scripts/`: local run scripts and Linux systemd templates
- `tools/`: Git helper scripts

## 2. Runtime Layout
- Default project root: the `Spec_Extraction` folder
- Default data root for local development: `App/data/`
- Production path override via env: `/var/lib/spec-extraction/`
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
- Static asset links include a file-version query string so browser caches do not hold stale layouts after CSS updates
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
- Legacy Builder rule fields remain in SQLite only for backward compatibility with older snapshots and rows.

### 3.3 Worker
- Separate Python process
- Polls queued runs from SQLite
- Acquires a single-worker lease before claiming queued runs, so only one active worker processes the queue at a time
- Processes:
  - spec extraction jobs
  - drawing parsing jobs
- Writes raw results and run state back into SQLite
- Job-page parse actions create queued runs first; the actual parsing starts only after the worker claims them.
- Emits granular run-progress messages so the Job page can poll and display the current parsing step in near real time.
- Records `worker_pid`, `app_build_id`, and `parser_strategy` on claimed runs for runtime traceability.

### 3.4 Extraction Pipeline
1. Read uploaded files from job folders.
2. Extract text from PDF or DOCX.
3. Flag low-text PDF pages for OCR/vision fallback.
4. Run heuristic extraction into canonical schema, including explicit appliance model parsing from labeled rows.
5. Enrich room rows with fixture fields (`sink_info`, `basin_info`, `tap_info`), split door-colour fields (`door_colours_overheads`, `door_colours_base`, `door_colours_island`, `door_colours_bar_back`), and derived bench-top fields (`bench_tops_wall_run`, `bench_tops_island`, `bench_tops_other`).
6. For Yellowwood-style schedule PDFs, parse room sections page-by-page from joinery schedule pages instead of scanning the whole document blindly, so `Back Benchtops`, island bench fields, vanity colours, and cabinet-only materials are mapped from the correct pages.
7. Remove plumbing fixtures from appliance rows so they only appear on room rows.
8. If OpenAI is enabled, send consolidated text and template context for higher-quality structured output.
   Default model target: `gpt-4.1-mini` unless an environment override is explicitly applied.
9. Normalize OpenAI text output before JSON parsing so fenced JSON or small prefatory text does not trigger an unnecessary fallback.
10. Apply the fixed `Global Conservative` profile for every Builder:
  - heuristic room structure and cleaning stay primary
  - OpenAI may fill missing fields and improve sparse evidence
  - OpenAI must not introduce extra room splits or overwrite already-clean fields with noisier text
  - room identity is source-driven for every builder, so only the same detected room merges across pages/files
  - for multi-file spec jobs, automatically pick one room-master file by schedule density and only let that file define the room set
  - room material fields remain room-local, so supplement files can enrich fixtures/appliances but must not inject another room's material text into the current room
  - room-master detection first normalizes glued schedule headings such as `KITCHEN COLOUR SCHEDULEBENCHTOP...` or `VANITIES COLOUR SCHEDULENOTE...` so the clean heading is extracted before room matching
  - the room-master room set is precomputed before supplement files are parsed, so supplement-file ordering cannot accidentally create extra rooms
11. Merge OpenAI output conservatively: keep the heuristic room set as the primary layout, merge room fields into that layout, and preserve heuristic appliance `model_no` values instead of replacing them with weaker guesses.
12. For Clarendon-only spec runs, apply a deterministic post-polish stage after source-driven room detection:
  - rebuild stable room text from colour-schedule and fixture pages for each detected room
  - prefer clean schedule-page values over OCR-noisy field fragments
  - keep source-driven room ownership while replacing noisy field text with cleaner deterministic values
13. Apply the fixed global cleaning rules after heuristic, merge, and Clarendon post-polish so brand casing, door-colour cleanup, kitchen-only bench-top splitting, and soft-close normalization stay consistent across all builders.
14. Record analysis metadata in the snapshot: mode, parser strategy, attempted, succeeded, model, note, and runtime identifiers (`worker_pid`, `app_build_id`).
15. Normalize drawer and hinge states to `Soft Close`, `Not Soft Close`, or blank.
16. Look up official appliance resources by `make + model_no`, first probing deterministic brand-site model URLs where supported and then falling back to search-based discovery; AEG, Westinghouse, and Fisher & Paykel now extract official dimensions from product pages when available, including JSON-like structured metadata.
17. Save the raw snapshot.

### 3.5 Review Pipeline
1. Load latest raw snapshot.
2. Load reviewed snapshot if present.
3. Render flattened rows into editable HTML tables.
4. Save edited values as a reviewed snapshot, preserving the expanded appliance link fields.
5. Export from the reviewed snapshot if present, otherwise from raw snapshot.

### 3.6 Raw Spec List Pipeline
1. Load `snapshots.snapshot_kind = raw_spec` for the requested job.
2. Flatten room, appliance, and other fields into read-only page rows.
3. Render the `Rooms` section as a vertical stack of wide horizontal room cards on desktop, with one display row per field and a separate metadata column.
4. Show room fixtures (`Sink`, `Basin`, `Tap`) directly on the room card and split door colours into `Overheads`, `Base`, `Island`, and `Bar Back`, while trimming location-only suffixes and filtering obvious OCR noise.
5. Only the kitchen card expands bench tops into `Wall Run` and `Island`; all other rooms collapse to a single `Benchtop` display row.
6. Filter plumbing fixtures out of the `Appliances` table and export.
7. Render a `Material Summary` section that smart-deduplicates room-level door colours, handle models, and bench tops, using the split wall-run/island bench-top values when available.
8. Render appliance official links as a clickable wrapped `Product` column.
9. Export that raw snapshot through a dedicated Excel route.
10. Never fall back to `reviews` when rendering the raw Spec List page.

### 3.7 Upload Interaction
1. Job detail uses the existing upload POST route.
2. File input controls submit their form immediately on `change`, so no dedicated upload button is required.
3. The page reloads after upload and the file list reflects the latest state.

### 3.8 Run History Refresh
1. The Job page renders the run history as an htmx partial.
2. The browser polls `/jobs/{job_id}/run-history` every few seconds.
3. The worker updates `runs.stage` and `runs.message` at key checkpoints such as loading, heuristic extraction, Clarendon polish, OpenAI request, merge/fallback, official model lookup, spec/manual discovery, official size extraction, and saving.
4. The partial replaces only the run-history card instead of reloading the full page.

### 3.9 Job Search
1. `GET /jobs` accepts an optional `q` query string.
2. `store.list_jobs()` applies a SQL `LIKE` filter against `job_no` when `q` is present.
3. The Jobs page renders a search form with submit and clear actions.

### 3.10 Global Conservative Profile
1. Builder-level rule editing has been retired from the UI.
2. Every parse run uses the same fixed `Global Conservative` profile, which reflects the accepted `37016` output style.
3. Legacy `/builders/{builder_id}/rules` requests redirect back to `/builders` so old bookmarks do not break the app.
4. Snapshots and runs still store parser strategy and runtime metadata so output remains traceable.
5. Source-driven room detection is now the default for all builders:
  - room rows are created from the actual source heading/label
  - only the same normalized room identity merges across pages/files
  - fixed Clarendon room compaction buckets such as `vanities` and preallocated rooms such as `theatre`/`rumpus` are no longer injected by layout stabilization
  - in multi-file spec jobs, supplement files cannot create new rooms; unmatched room-like sections are ignored and surfaced through warnings/diagnostics
  - when the room-master file already groups a room family such as `Vanities`, supplement bathroom/ensuite/powder vanity pages can enrich that grouped room without splitting it apart

## 4. Canonical Schema

### Rooms
- One row per source-driven room
- Array-like fields are stored as lists in JSON and flattened with ` | ` in the review UI
- `room_key` is a source-driven normalized identity, `original_room_label` preserves the detected source label
- Room rows also carry fixture fields for sinks, basins, and taps plus split door-colour and bench-top display fields.
- Clarendon rows pass through a deterministic post-polish layer after layout stabilization so handle strings, fixture text, splashback notes, and soft-close fallbacks stay readable without changing source-driven room ownership.
- That Clarendon post-polish now detects at least two schedule families: the `37016` reference family and the denser single-line `LUXE / handleless / mirror splashback` family, then applies family-specific field splitting before the shared compact-summary cleanup.

### Appliances
- One row per appliance
- Each row carries source metadata and confidence
- Sink, basin, tap, and tub fixture rows are excluded from the appliance presentation/export layer because they are surfaced on the corresponding room row.
- `product_url` is the primary visible link field; `website_url` remains a compatibility alias.
- `overall_size` is only populated from official model resources, not from raw spec text heuristics, and supports both compact `W x D x H` text and labeled `51 mm (H) / 900 mm (W) / 520 mm (D)` product-page patterns.
- Official size extraction also supports structured product metadata such as embedded `height / width / depth` JSON blocks on brand product pages.

### Others
- Free-form notes and extraction diagnostics

### Drawing Snapshot
- Stored separately from the spec snapshot
- Uses the same field naming so future comparison logic can diff field-to-field
- Carries the same `analysis` metadata shape as the spec snapshot

## 5. Security Model
- Single admin account from environment variables
- Session cookie signed by `SPEC_EXTRACTION_SECRET_KEY`
- CSRF token stored in session and checked on POST forms
- All business routes require login
- Uploads are stored on disk and referenced from SQLite

## 6. Deployment Model
- Web process bound to `127.0.0.1:8010`
- Nginx terminates TLS and forwards `https://spec.lxtransport.online/` to the local web process
- Web and worker each get a `systemd` unit
- Production `systemd` units run as the server's `ubuntu` user, matching the existing LXtransport services on that host
- Recommended production code path: `/opt/spec-extraction`
- Recommended production data path: `/var/lib/spec-extraction`
- Recommended environment file: `/etc/spec-extraction.env`
- Production Nginx and FastAPI upload limits should stay aligned at `100 MB`
- Routine updates are online-first: after local verification, deploy the current repo state to `/opt/spec-extraction`, restart both production services, verify `/api/health`, and then re-run any job whose parsing output should change.
- The repo now includes `tools/deploy_online.py` and `tools/deploy_online.ps1` to stage selected repo files to the LXtransport host, install them into `/opt/spec-extraction`, restart `spec-extraction-web.service` and `spec-extraction-worker.service`, and validate the live health endpoint.

## 7. Implemented Route Map
- `GET /`: redirect to login or jobs
- `GET/POST /login`
- `POST /logout`
- `GET/POST /builders`
- `POST /builders/{builder_id}/templates`
- `POST /templates/{template_id}/delete`
- `GET /builders/templates/{template_id}/download`
- `GET/POST /jobs`
  - supports optional `q` query string on the GET route for `job_no` search
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/spec-list`
- `GET /jobs/{job_id}/spec-list.xlsx`
- `POST /jobs/{job_id}/files/upload`
- `POST /jobs/files/{file_id}/delete`
- `GET /jobs/files/{file_id}/download`
- `POST /jobs/{job_id}/runs/start`
  - validates that matching uploaded files exist before creating the parse run
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

## 10. Test Safety
- Smoke tests must point `SPEC_EXTRACTION_DATA_DIR` at a temporary directory before importing the app.
- Test setup and teardown must never clear the real `App/data/` SQLite database.
