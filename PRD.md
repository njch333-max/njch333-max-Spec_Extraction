# Spec_Extraction PRD

## 1. Project Goal
Deliver an English-only web application called `Spec_Extraction` for cabinet production checking workflows. The app must let the user:
- manage Builder template files,
- create unique jobs by `job_no`,
- upload multiple spec files and drawing files,
- extract structured room and appliance information,
- review and manually correct the result,
- export reviewed data to Excel and CSV,
- store drawing-side parsed data for future automated comparison.

## 2. Target User
- Primary user: cabinet drafter or production checker
- Secondary user: small internal team using the same server
- Access model in v1: single admin login

## 3. Core User Flows
1. Log into the web app.
2. Create a Builder entry or upload Builder template files.
3. Create a new job with a unique `job_no` and selected Builder.
4. Upload one or more spec files for the job.
5. Start spec extraction and wait for the worker to finish.
6. Review the extracted `Rooms`, `Hardware`, `Appliances`, and `Others`.
7. Edit incorrect values and save the reviewed result.
8. Export the reviewed result to Excel or CSV.
9. Upload production drawing PDFs so compare-ready data is stored for a future release.

## 4. Functional Requirements

### 4.1 Builder Library
- Create Builder records with name, slug, and notes.
- Upload, list, and delete template files for each Builder.
- Store template files under a Builder-specific folder.

### 4.2 Job Management
- Create jobs with unique `job_no`.
- Each job must belong to exactly one Builder.
- List jobs with status summary.
- View a job detail page with files, runs, results, and exports.

### 4.3 File Support
- Spec files: `PDF`, `DOCX`
- Drawing files: `PDF`
- Allow multiple uploaded spec files per job.
- Allow multiple uploaded drawing files per job.

### 4.4 Extraction
- Extract text directly from digital PDFs and DOCX files.
- Mark low-text PDF pages for OCR or vision fallback.
- Produce a canonical JSON result containing:
  - room rows,
  - appliance rows,
  - other notes,
  - evidence and confidence,
  - source references.
- Merge information across multiple spec files in the same job.
- If OpenAI is configured, use it to improve structured extraction.

### 4.5 Review
- Show reviewable data in English-only sections:
  - `Rooms`
  - `Hardware`
  - `Appliances`
  - `Others`
- Allow direct editing in the browser.
- Preserve reviewed data separately from raw machine extraction.

### 4.6 Export
- Export reviewed data to:
  - one Excel workbook with multiple sheets
  - one CSV file
- Include source file and page references in the exported data.

### 4.7 Drawing Foundation
- Upload drawing PDFs from the job page.
- Parse drawing-side summary blocks into the same canonical schema.
- Save compare-ready data, but do not expose a formal comparison UI in v1.

### 4.8 Security
- Require login for all working pages.
- Use CSRF protection for forms.
- Keep session cookies scoped to the app domain.
- Keep secrets in environment variables or config files outside source control.

### 4.9 Git Rollback Tooling
- Provide local Git helper scripts to initialize, checkpoint, inspect history, and restore from previous commits.
- Require synchronized doc updates for major changes.

## 5. Canonical Data Requirements

### 5.1 Room Fields
- `room_key`
- `original_room_label`
- `bench_tops`
- `door_panel_colours[]`
- `toe_kick`
- `bulkheads`
- `handles[]`
- `drawers_soft_close`
- `hinges_soft_close`
- `splashback`
- `flooring`
- `source_file`
- `page_refs`
- `evidence_snippet`
- `confidence`

### 5.2 Appliance Fields
- `appliance_type`
- `make`
- `model_no`
- `website_url`
- `overall_size`
- `source_file`
- `page_refs`
- `evidence_snippet`
- `confidence`

## 6. Non-Goals For V1
- Public self-service user registration
- Full compare result UI
- Automated image scraping and caching for product references
- Production-grade OCR infrastructure beyond the fallback hooks
- Multi-tenant permissions

## 7. Acceptance Criteria
- User can log in, create Builders, upload template files, create Jobs, upload files, trigger extraction, review results, save edits, and export files.
- SQLite persists Builders, Jobs, files, run history, raw results, and reviewed results.
- Worker can process queued spec and drawing runs separately from the web process.
- The app boots locally with documented commands.
- Git helper scripts work inside the project folder.

## 8. Implemented V1 Surface
- Pages:
  - `/login`
  - `/builders`
  - `/jobs`
  - `/jobs/{job_id}`
- Actions:
  - create builder
  - upload and delete template files
  - create job
  - upload and delete spec files
  - upload and delete drawing files
  - queue spec extraction
  - queue drawing parsing
  - save reviewed result
  - generate Excel and CSV exports
- Diagnostics:
  - `/api/health`

## 9. Document Sync Rule
Any major change must update:
- `PRD.md`
- `Arch.md`
- `Project_state.md`
