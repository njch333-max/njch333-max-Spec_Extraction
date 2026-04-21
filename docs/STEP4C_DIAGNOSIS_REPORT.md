# Step 4c Diagnosis Report

Date: 2026-04-21

Scope: P1 diagnose only. No production source files were modified. All production checks were run as standalone Python or shell commands over SSH.

## Executive Finding

The original hypothesis is false: `document.get("path")` is not falsy in the current production runtime.

Current production dispatch evidence for `job_id=70`:

- `USE_V6_IMPERIAL_EXTRACTOR`: `True`
- `imperial_builder`: `True`
- `is_room_master`: `True`
- `document.get("path")`: truthy and points to an existing PDF
- `_use_v6_for_this_document`: `True`
- A monkey-patched `_process_v6_imperial_document` is called once by `parsing.parse_documents`

The actual blocker is downstream: after v6 successfully creates `material_rows`, the legacy Imperial post-processing path `_apply_imperial_row_polish` rebuilds Imperial rooms from the old source-PDF text boundary parser and overwrites the v6 output.

There is a second design issue: `build_spec_snapshot` runs `_apply_layout_pipeline` before `parsing.parse_documents`, so Docling / heavy-vision analysis can still appear even when v6 dispatch later fires. `runs.parser_strategy` is also written from `cleaning_rules.global_parser_strategy()` before parser dispatch, so `global_conservative` is not proof that v6 did not run.

## Root Cause Variable

Not `document.get("path")`.

The destructive variable is the snapshot state after `_apply_imperial_row_polish`:

- Before polish: `11` material rows, all from provider `v6`
- After `_apply_imperial_row_polish`: `0` material rows in the fast diagnostic probe

In the original production run `2271`, the stored snapshot had no v6 provenance:

- `SNAPSHOT_MATERIAL_ROWS=6`
- `SNAPSHOT_V6_ROWS=0`
- `SNAPSHOT_PROVIDERS={'cell_grid_repair': 2, '': 4}`

This means the persisted output is legacy-polished output, not v6 adapter output.

## Root Cause Source

Primary source:

- `App/services/extraction_service.py`
- Function: `_apply_imperial_row_polish`
- Local line range inspected: around `11702-11721`

Relevant behavior:

```python
if parser_strategy not in {"stable_hybrid", cleaning_rules.global_parser_strategy()} or "imperial" not in builder_name.strip().lower():
    return snapshot
_report_progress(progress_callback, "imperial_polish", "Rebuilding Imperial room rows from source PDF text boundaries")
rebuilt_rooms: dict[str, dict[str, Any]] = {}
...
for section in parsing._collect_imperial_sections_for_document(document):
    ...
    row = parsing._imperial_room_from_section(section).model_dump()
    rebuilt_rooms[str(row.get("room_key", ""))] = row
...
if rebuilt_rooms:
    polished["rooms"] = list(rebuilt_rooms.values())
```

Why this breaks v6:

- v6 output is already present in `snapshot["rooms"][*]["material_rows"]`.
- `_apply_imperial_row_polish` ignores that provenance and rebuilds rooms through `_collect_imperial_sections_for_document` + `_imperial_room_from_section`, i.e. the old path.
- `build_spec_snapshot` calls this polish path after parse and again near the end of the non-AI branch.
- `_build_raw_spec_crosscheck_snapshot` also calls `parsing.parse_documents`, then `_apply_builder_specific_polish`, so the raw crosscheck path is also legacy-polish contaminated.

Secondary source:

- `App/services/extraction_service.py`
- Function: `build_spec_snapshot`
- Local lines inspected: around `303-319`, `394-426`

Relevant behavior:

- `_apply_layout_pipeline(...)` runs before `parsing.parse_documents(...)`.
- Therefore Docling / heavy vision can run before v6 dispatch.
- Analysis fields are later populated from `vision_meta`, not from the v6 dispatch decision.

## Evidence

Full diagnostic outputs are stored locally under:

- `tmp/step4c/p1_q1_q2_snapshot_dispatch.txt`
- `tmp/step4c/p1_fast_dispatch_probe.txt`
- `tmp/step4c/p1_v6_extractor_probe.txt`
- `tmp/step4c/p1_build_spec_snapshot_probe.txt`
- `tmp/step4c/p1_pipeline_stage_probe.txt`
- `tmp/step4c/p1_q3_q4_logging_entrypoints.txt`

### Q1: Path Real Value

Production diagnostic on `job_id=70`:

```text
JOB= {'id': 70, 'job_no': '378131', 'builder_id': 4, 'title': '', 'status': 'ready'}
BUILDER= {'id': 4, 'name': 'Imperial', 'slug': 'imperial', ...}
ATTACHED_FILES= [{'original_name': 'Colour Selections - 29.10.25 - 92 Haldham Crescent.25 - 92 Haldham Crescent.PDF', 'path': '/var/lib/spec-extraction/jobs/378131/spec/20260420T115211+0000_Colour_Selections_-_29.10.25_-_92_Haldham_Crescent.25_-_92_Haldham_Crescent.PDF', 'exists': True}]
RAW_DOCUMENT 0 keys= ['file_name', 'pages', 'path', 'role'] path= '/var/lib/spec-extraction/jobs/378131/spec/20260420T115211+0000_Colour_Selections_-_29.10.25_-_92_Haldham_Crescent.25_-_92_Haldham_Crescent.PDF' exists= True pages= 3
COPIED_DOCUMENT 0 keys= ['file_name', 'pages', 'path', 'role'] path= '/var/lib/spec-extraction/jobs/378131/spec/20260420T115211+0000_Colour_Selections_-_29.10.25_-_92_Haldham_Crescent.25_-_92_Haldham_Crescent.PDF' exists= True
```

Fast no-external-provider probe after `_apply_layout_pipeline`:

```text
AFTER_LAYOUT_DOCUMENT 0 keys= ['builder_name', 'file_name', 'pages', 'path', 'role'] path= '/var/lib/spec-extraction/jobs/378131/spec/20260420T115211+0000_Colour_Selections_-_29.10.25_-_92_Haldham_Crescent.25_-_92_Haldham_Crescent.PDF' exists= True
```

Conclusion: `path` is present and valid through `_attach_paths`, `_load_documents`, the document copy, and `_apply_layout_pipeline`.

### Q2: is_room_master Real Value

Production diagnostic:

```text
ROOM_MASTER_REASON= Colour Selections - 29.10.25 - 92 Haldham Crescent.25 - 92 Haldham Crescent.PDF selected as room master for Imperial single-file parse.
ROOM_MASTER_IDENTITY_MATCH= True
DISPATCH_CONDITIONS 0 flag= True imperial= True is_room_master= True path_truthy= True use_v6= True
FAKE_V6_CALLED file= Colour Selections - 29.10.25 - 92 Haldham Crescent.25 - 92 Haldham Crescent.PDF path_truthy= True is_room_master= True
FAKE_V6_CALL_COUNT= 1
```

Conclusion: `is_room_master` is also not the blocker.

### v6 Extractor Standalone Probe

Production diagnostic:

```text
V6_EXTRACTOR_PATH= /opt/spec-extraction/App/services/pdf_to_structured_json.py exists= True
SOURCE_PATH_EXISTS= True
V6_RUN_OK sections= 3
V6_SECTION_TITLES= ['KITCHEN JOINERY SELECTION SHEET', 'APPLIANCES', 'SINKWARE & TAPWARE']
V6_FIRST_ITEMS= 5
```

Conclusion: the v6 extractor can run against the uploaded Haldham PDF on production.

### Pipeline Stage Probe

Production diagnostic:

```text
AFTER_PARSE_DOCUMENTS rooms= 3 material_rows= 11 providers= {'v6': 11}
AFTER_ENRICH rooms= 3 material_rows= 11 providers= {'v6': 11}
AFTER_STABILIZE rooms= 3 material_rows= 11 providers= {'v6': 11}
POLISH_PROGRESS imperial_polish Rebuilding Imperial room rows from source PDF text boundaries
AFTER_POLISH rooms= 1 material_rows= 0 providers= {}
AFTER_APPLIANCE_ENRICH rooms= 1 material_rows= 0 providers= {}
```

Conclusion: v6 dispatch works and v6 rows survive through `parse_documents`, `enrich_snapshot_rooms`, and `_stabilize_snapshot_layout`. They are destroyed by `_apply_imperial_row_polish`.

### Stored Snapshot Probe for Existing Run 2271

Production DB snapshot for `job_id=70`:

```text
LATEST_RUN= {'id': 2271, 'status': 'succeeded', 'parser_strategy': 'global_conservative', 'worker_pid': 564547, 'app_build_id': 'local-2993dc20', 'started_at': '2026-04-20T11:52:13+00:00', 'finished_at': '2026-04-20T11:53:10+00:00'}
SNAPSHOT_ANALYSIS= {'parser_strategy': 'global_conservative', 'layout_provider': 'heavy_vision', 'layout_mode': 'heavy_vision', 'docling_attempted': True, 'docling_succeeded': True, 'vision_attempted': True, 'vision_succeeded': True, 'room_master_file': 'Colour Selections - 29.10.25 - 92 Haldham Crescent.25 - 92 Haldham Crescent.PDF'}
SNAPSHOT_MATERIAL_ROWS= 6
SNAPSHOT_V6_ROWS= 0
SNAPSHOT_PROVIDERS= {'cell_grid_repair': 2, '': 4}
```

Conclusion: persisted run `2271` output is not v6 output. The current runtime proves dispatch can trigger, so the no-v6 stored output is explained by downstream legacy overwrite, not by missing `path`.

### Q3: Worker stdout/log location

Files inspected:

- `App/worker_main.py`
- `App/scripts/run_worker.sh`

Findings:

- `run_worker.sh` executes `python -m App.worker_main` directly.
- There is no explicit file logging or stdout redirection.
- `find /opt/spec-extraction -name "*.log" -mmin -180` returned no app log files.
- Worker stdout/stderr goes to `systemd` journal.
- `journalctl -u spec-extraction-worker.service -o cat` shows third-party progress bars and exceptions, but the application itself does not print parse-stage progress; progress is written to the DB by `store.update_run_progress`.

Relevant journal excerpt:

```text
Loading weights: 100%|██████████| 770/770 [...]
Traceback (most recent call last):
  File "/opt/spec-extraction/App/worker_main.py", line 7, in <module>
    run_worker_loop()
  File "/opt/spec-extraction/App/services/worker.py", line 23, in run_worker_loop
    if not store.acquire_worker_lease(WORKER_TOKEN, WORKER_PID, APP_BUILD_ID):
  File "/opt/spec-extraction/App/services/store.py", line 463, in acquire_worker_lease
    conn.commit()
sqlite3.OperationalError: database is locked
```

Operational note: this journal is useful for dependency stderr and crashes, but not for normal parser branch decisions unless code is changed to emit logs. P1 did not add logs, per constraint.

### Q4: Alternate Entry Paths

Production grep:

```text
App/services/extraction_service.py:319:    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="spec", documents=documents, rule_flags=rule_flags)
App/services/extraction_service.py:450:    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="drawing", documents=documents, rule_flags=rule_flags)
App/services/extraction_service.py:7247:        heuristic = parsing.parse_documents(
App/services/extraction_service.py:8363:    snapshot = parsing.parse_documents(
App/services/parsing.py:16984:def _process_v6_imperial_document(
App/services/parsing.py:17086:        _use_v6_for_this_document = (
App/services/parsing.py:17092:        if _use_v6_for_this_document:
App/services/parsing.py:17093:            section_order_counter = _process_v6_imperial_document(
App/services/parsing.py:17232:def parse_documents(
```

Interpretation:

- Main spec snapshot entry is `extraction_service.py:319`.
- Drawing parsing is unrelated.
- Vision fallback can re-run `parse_documents` on vision-normalized documents.
- Raw crosscheck `_build_raw_spec_crosscheck_snapshot` also calls `parse_documents`, then applies builder polish.
- There is no evidence of the worker bypassing `parse_documents` for spec runs.

## Recommended Fix Strategies

### Strategy 1: Minimal post-processing guard

Add a small helper such as `_snapshot_uses_imperial_v6(snapshot)` and use it to:

- early-return from `_apply_imperial_row_polish` when material rows have v6 provenance
- skip Imperial raw crosscheck when the current snapshot is v6-backed
- keep flag-off behavior exactly unchanged

Pros:

- Smallest patch.
- Directly fixes the observed destructive overwrite.
- Low regression risk outside Imperial.

Cons:

- `build_spec_snapshot` would still run `_apply_layout_pipeline` before v6 parsing.
- Snapshot Summary may still show Docling / heavy vision metadata.
- Duration may remain long.
- Fails the intended Step 5 signal that v6 should avoid Docling/Vision.

### Strategy 2: v6 fast path at `build_spec_snapshot` level

Detect `USE_V6_IMPERIAL` + Imperial + spec file path before `_apply_layout_pipeline`, then route to a v6-specific snapshot branch:

- load raw documents enough to get page text and source metadata
- call `parsing.parse_documents(...)` so existing `_process_v6_imperial_document` remains the v6 construction point
- skip `_apply_layout_pipeline`, `_apply_imperial_row_polish`, and old Imperial raw crosscheck
- still run safe generic room enrichment / cleaning only if it does not rebuild material rows
- set analysis fields explicitly, e.g. `parser_strategy = "imperial_v6"`, `layout_attempted = False`, `docling_attempted = False`, `vision_attempted = False`
- keep flag-off behavior exactly unchanged

Pros:

- Matches the intended v6 architecture: v6 extractor is the table engine, so no Docling/Heavy Vision pre-pass is needed.
- Meets Step 5 acceptance signals: no Docling/Heavy Vision in Snapshot Summary, faster runtime, parser strategy can identify v6.
- Avoids both destructive legacy polish and misleading analysis metadata.

Cons:

- Larger than Strategy 1.
- Needs careful tests for flag-off fallback and v6 hit case.
- Must decide which enrichment/cleaning functions are safe for v6 snapshots.

### Strategy 3: Parser-level v6 mode marker plus selective no-op downstream

Have `parsing.parse_documents` set an explicit analysis marker when v6 dispatch fires, then allow downstream functions to no-op or adjust behavior based on that marker.

Pros:

- Keeps v6 decision close to the current Step 4a dispatch.
- Avoids relying only on material-row provenance scans.
- Can be combined with Strategy 1.

Cons:

- Does not by itself avoid pre-parser layout/docling.
- Requires touching multiple downstream checks to respect the marker.
- Easier to miss one destructive postprocessor.

## Codex Recommendation

Use Strategy 2.

Reasoning:

The production bug is not just "v6 rows are overwritten"; the current integration point is too low in the pipeline. `build_spec_snapshot` runs layout/docling/heavy-vision before v6 can make a decision, and later Imperial polish/crosscheck assumes the old parser remains authoritative. Guarding `_apply_imperial_row_polish` is necessary but not sufficient for the intended Step 5 acceptance criteria.

The correct architecture is a high-level Imperial v6 fast path in `build_spec_snapshot`:

- flag off: unchanged old production pipeline
- flag on + Imperial + path available: v6 pipeline owns the room snapshot
- old Imperial row polish/crosscheck does not run on v6 snapshots
- analysis explicitly identifies v6

Minimum P2 tests should cover:

- flag on + Imperial document with path dispatches to v6 and bypasses layout/polish
- flag off still calls the existing layout/legacy path
- v6 snapshot material rows keep `source_provider = "v6"` after full `build_spec_snapshot`
- missing path or v6 failure falls back safely without breaking the run, per agreed behavior

## P1 Status

P1 is complete.

Per task instructions, no P2 code changes or deploy were performed.
