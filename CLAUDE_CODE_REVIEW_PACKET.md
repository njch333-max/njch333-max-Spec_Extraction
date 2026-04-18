# Claude Code Review Packet

## Executive Summary
`Spec_Extraction` is an internal English-only web application for cabinet production checking workflows. It manages builders, jobs, uploaded spec files, drawing files, parser runs, raw extraction results, PDF QA signoff, and Excel/CSV exports.

The live production environment is `https://spec.lxtransport.online/`. The app runs as a FastAPI web service plus a separate worker, behind Nginx and systemd. Production verification is part of the normal delivery workflow for parser changes.

The current priority is parser correctness, especially Imperial table/grid extraction. The project is moving toward a Sogou-like PDF table reconstruction model: recover visible table geometry first, then assemble rows and cells, then run constrained repair and semantic summaries. It is not yet at commercial PDF-converter-level table reconstruction. Known weak spots remain around merged cells, missing grid lines, weak separators, long `AREA / ITEM` labels, and same-cell continuation.

This packet is intended for Claude Code review. It compresses the current PRD, architecture, project state, and Imperial tracker into a focused review brief. The canonical long-form sources remain:
- `PRD.md`
- `Arch.md`
- `Project_state.md`
- `AGENTS.md`
- `IMPERIAL_GRID_TRACKER.md`

Latest known repo state when this packet was created:
- Branch: `master`
- Remote tracking: `origin/master`
- Latest commit: `7d15424 Sign off Imperial job 61 Phase 3B QA`
- Working tree before packet creation: clean

## Product Requirements
The core user workflow is:
1. Log in as a single admin user.
2. Create or manage builder records and template files.
3. Create jobs with unique `job_no` values and an assigned builder.
4. Upload one or more spec PDFs/DOCX files and drawing PDFs.
5. Start a spec or drawing parse run.
6. Let the worker process the queued run.
7. Review the raw Spec List output.
8. Complete field-level PDF QA against the source PDF.
9. Export reviewed or raw data only after PDF QA passes.

Main product surfaces:
- Builder library.
- Jobs list with search and sorting.
- Job workspace with files, run history, parsing actions, and latest raw snapshot status.
- Raw Spec List page for read-only extracted data.
- Historical run Spec List pages backed by stored run JSON.
- PDF QA page with checklist save, pass, and fail actions.
- Excel/CSV export flow.
- Drawing upload and compare-ready drawing parsing foundation.

Hard product rules:
- UI is English-only.
- Builder routing is owned by the website job's assigned Builder. PDF header text, logos, client names, or sheet styling must not override routing.
- Parser output is not accepted merely because fields are populated. Parser-accuracy work is complete only after a fresh live rerun passes source-PDF field-level QA.
- Formal exports remain locked until the latest raw spec PDF QA status is `passed`.
- Parser diagnostics may remain in backend JSON, but user-facing raw Spec List pages should not show noisy review/debug banners by default.

## Architecture Overview
Top-level components:
- `App/main.py`: FastAPI app, routes, templates, PDF QA workflow, page rendering.
- `App/services/store.py`: SQLite schema and persistence.
- `App/services/worker.py`: queued run processor with single-worker lease behavior.
- `App/services/extraction_service.py`: extraction orchestration, layout/grid recovery, Imperial grid and row assembly.
- `App/services/parsing.py`: PDF/DOCX extraction helpers, parser cleanup, builder-specific finalizers, Imperial material-row postprocess.
- `App/services/export_service.py`: Excel and CSV export generation.
- `App/templates/`: Jinja UI templates.
- `App/static/`: CSS and frontend assets.
- `tools/`: deployment, debug, and Git helper tools.

Runtime model:
- SQLite stores builders, jobs, files, runs, snapshots, reviews, and PDF QA verification rows.
- Uploaded files are stored under the configured data root.
- Web and worker processes are separate.
- New parse actions create queued runs; the worker claims and processes them.
- Run history records parser strategy, worker PID, app build ID, stage, message, and duration.

Deployment model:
- Production web service binds locally and is proxied by Nginx.
- Web and worker run as systemd services.
- Routine confirmed implementation workflow is local verification, deploy, service restart, health check, fresh live rerun if parser logic changed, then source-PDF QA.

## Current Project State
Implemented and active:
- FastAPI web app with login, builder management, job management, uploads, parsing, raw Spec List, PDF QA, and exports.
- SQLite-backed persistence and run history.
- Separate production worker process.
- Field-level PDF QA records automatically created for new spec snapshots.
- Export gating behind PDF QA.
- Structure-first parser pipeline across builders.
- Builder-specific finalizer layer.
- Online deployment helper.
- Large smoke-test suite and compile verification.

Current high-value production signoffs include:
- Imperial `job 67 / run 2250`: strict PDF QA passed, `51 pass / 0 fail / 0 pending`.
- Imperial `job 64 / run 2251`: strict PDF QA passed, `61 pass / 1 na / 0 fail / 0 pending`.
- Imperial `job 62 / run 2260`: strict PDF QA passed, `65 pass / 20 na / 0 fail / 0 pending`.
- Imperial `job 61 / run 2269`: strict PDF QA passed, `66 pass / 0 fail / 0 pending`.

Recent work has focused on Imperial Phase 3B sinkware/appliance overlay correctness, appliance row-first capture, fixture-base constrained note/taphole carry-forward, and strict PDF QA signoff on the current production build path.

## Parser Strategy
The parser follows a structure-first, row-local policy:
- Detect page type, room blocks, table/grid rows, row fragments, and source context before filling fields.
- Keep field ownership same-room, same-section, and same-row-or-row-fragment.
- Do not borrow supplier, note, profile, model, or material text across unrelated rows or rooms.
- Keep source wording with light cleanup rather than semantic rewriting.

Global runtime strategy:
- All builders use the fixed `Global Conservative` profile.
- Default automatic AI merge is disabled.
- Default automatic heavy vision is disabled except for Imperial joinery/material pages, where vision/grid recovery is used as a table-boundary layer.
- Selective Docling/table-grid parsing is used for difficult table-style pages on selected builders.

Builder routing rule:
- The website job's assigned Builder is the source of truth.
- If the site classifies a job as `Imperial`, parse and QA it as Imperial even if the uploaded PDF visually resembles another builder's delegated colour-consult sheet.

Builder-family summary:
- `Imperial`: joinery/material is grid-first and `material_rows`-first; sinkware/appliances are row-first overlays.
- `Clarendon`: heuristic-grid-first, with `Drawings and Colours` as deterministic room master when present.
- `Yellowwood`: table/grid-first for cabinetry, vanity, flooring, and tiling; finalizer preserves concrete room titles and drops non-material rooms.
- `Simonds`: table/grid-first for grouped property schedules.
- `Evoca`: table/grid-first for finishes, flooring, plumbing, and appliance schedules.

## Imperial Focus
Imperial is currently the most complex parser path.

Locked Imperial decisions:
- `material_rows` are the primary truth layer for joinery/material pages.
- Room cards show raw source-like rows in source order.
- `AREA / ITEM` display should prefer the original table label cell.
- Row values use lightly cleaned `SUPPLIER / SPECS / DESCRIPTION / NOTES`.
- `Tap` is excluded from Imperial primary room cards, primary summary, and primary QA.
- `Material Summary` only aggregates `Door Colours`, `Handles`, and `Bench Tops`.
- Summary entries group by material and show `Room: A | B | C`.
- Backend diagnostics can retain review/repair details, but normal frontend output hides them.

Imperial grid direction:
- Grid boundary recovery is the first truth layer.
- Page structure should distinguish table header, content grid, footer/noise, image bboxes, separator segments, row bands, and cell ownership.
- `IMAGE` cells are geometry only; no OCR/image text should enter material rows, summary, sinkware, or appliances.
- `visible` and `inferred_high` separators are hard boundaries.
- `none` and `inferred_low` separators may allow conservative same-cell continuation or label continuation.

Imperial self-repair direction:
- Repairs must be constrained and provenance-backed.
- Allowed repair targets include missing labels, row boundary issues, column spillover, room ownership, handle over-splitting, supplier/notes misassignment, and summary tags.
- Low-confidence repairs should remain unresolved or backend-only, not silently become formal output.
- Summary must not mask bad raw rows.

Imperial current phases:
- Phase 1 Grid Truth: page-structure bboxes, segment provenance, debug JSON/SVG overlays, and separator-aware row-band coalescing exist.
- Phase 2 Row Assembly: `AREA / ITEM` anchored row assembly exists, including constrained `GPO -> ACCESSORIES` leading-fragment repair and several continuation/label cleanup paths.
- Phase 3 Semantic/Summary: handle semantic subitems, identity-gated handle summary, sinkware/appliance row-first overlays, and fixture-base constrained note carry-forward exist.

Current Imperial open concerns:
- Short-value termination still needs stronger generalization for rows such as `KICKBOARDS`, `LIGHTING`, `SHELVES`, `HANGING RAIL`, `ACCESSORIES`, and `BIN`.
- Row assembly can still depend too much on postprocess cleanup when grid evidence is weak.
- Commercial-grade PDF table reconstruction is not yet achieved; the system needs more robust segment-level geometry solving and better merged-cell inference.

## Regression And QA Matrix
Important Imperial jobs and current meaning:
- `job 52`: historical splashback/base boundary, knob spillover, appliance placeholder handling.
- `job 55`: handle row separation and handle summary holes.
- `job 56`: handle completeness, `HANDLES to OVERHEADS`, soft-close stability.
- `job 59`: handle summary canonicalization and grouping.
- `job 60 / 37867`: continuation-heavy desk/shelf/robe baseline and gold fixture family.
- `job 61`: base/upper contamination, handle grouping, appliance and sinkware ownership.
- `job 62`: short-value termination, pantry/kitchen handle subitems, sinkware/appliance overlays.
- `job 64`: `ACCESSORIES / GPO`, source-case flooring, `450mm BIN`, supplier-cell ownership, seven appliance rows.
- `job 67`: hard content-grid boundary, header/meta bleed, supplier/notes split, row-first appliances, sinkware ownership.

Highest-priority fixture:
- `tests/fixtures/imperial_37867_gold.json`

Strict recent Imperial signoffs:
- `job 67 / run 2250 / build local-c28adee4`: passed.
- `job 64 / run 2251 / build local-c28adee4`: passed.
- `job 62 / run 2260 / build local-a1afcf24`: passed.
- `job 61 / run 2269 / build local-363a0642`: passed.

Reviewers should distinguish:
- Targeted regression readback.
- Strict source-PDF field-by-field QA.
- Historical snapshots that are useful references but not acceptance.

## Known Risks
Parser risks:
- Imperial grid boundaries can still be weak when PDF lines are missing, hidden by images, or represented inconsistently.
- Merged cells can still produce row/column ownership mistakes before postprocess repair.
- Long `AREA / ITEM` labels can be split or can absorb description text.
- Same-cell continuation can be lost if row termination is too aggressive.
- Short-value rows can absorb following labels if separator evidence is soft.
- Summary logic can hide upstream raw-row errors if gating is too permissive.

Cross-builder risks:
- Shared cleanup may unintentionally change builder-specific finalizer behavior.
- A fix for Imperial may break Clarendon, Yellowwood, Simonds, or Evoca assumptions if shared functions are touched.
- Builder routing must not be inferred from PDF header text.

QA risks:
- Non-empty extracted values are not proof of correctness.
- Bulk `pass/na` based on populated values is invalid as final signoff.
- Parser fixes require fresh live reruns and source-PDF field-level QA before they are considered complete.

Operational risks:
- Official appliance lookup depends on external brand sites and search result structures.
- OCR fallback remains warning-driven unless stronger OCR or vision infrastructure is configured.
- Old snapshots must continue rendering safely despite schema evolution.
- Local smoke tests must not touch real app data.

## Review Instructions For Claude Code
Please review this repository as a production parser system, not as a style-only web app.

Primary review focus:
- Bugs that can cause cross-room, cross-row, or cross-section data leakage.
- Builder routing mistakes.
- Parser regressions caused by shared cleanup or postprocess changes.
- Imperial grid/row assembly paths that depend on string cleanup instead of cell ownership.
- Summary aggregation that uses polluted or unresolved rows.
- PDF QA state handling and export gating.
- Schema/UI/export drift where a field is parsed but not displayed, exported, or QA-checked consistently.
- Tests that assert display strings but do not protect source ownership.

Secondary review focus:
- Unreachable code or duplicated repair paths.
- Over-broad regexes that may fix one sample and break another.
- Places where backend diagnostics leak into normal frontend UI.
- Risky fallback paths that read from page-wide text after a cell-grid row exists.
- Any mutation of row order after canonical `material_rows.row_order` has been established.

Do not prioritize:
- Cosmetic code style alone.
- Reformatting large parser files without reducing bug risk.
- Suggestions that require a full parallel parser rewrite before incremental hardening.

## Suggested Review Questions
Use these as concrete prompts when reviewing the code:

1. Can any parser path still read values from page header, footer, `IMAGE`, or whole-page text after a clean content-grid cell exists?
2. Can supplier, notes, model, profile, or material text cross from one row into another without provenance-backed repair?
3. Can a room receive material text from another room, section, or page cluster?
4. Can the website Builder route be overridden by PDF header text or visual branding?
5. Can Imperial `material_rows` be reordered after source row order has been assigned?
6. Can `AREA / ITEM` display titles be synthesized when a source label cell exists?
7. Can `Tap` re-enter Imperial room cards, summaries, or primary PDF QA?
8. Can handle summary use provenance raw text or fallback blocks instead of clean `handle_subitems` when subitems exist?
9. Can `Material Summary` include rows with unresolved non-handle-specific repair issues?
10. Can `Door Colours`, `Handles`, or `Bench Tops` summary entries be created from notes-only, header/meta, image-column, or footer fragments?
11. Do PDF QA checklist rows follow canonical source order, especially for Imperial `material_rows`?
12. Are exports blocked when latest raw spec PDF QA is not `passed`?
13. Do old snapshots render safely when new fields such as `material_rows`, `handle_subitems`, or repair metadata are missing?
14. Are smoke tests isolated from real local/production app data?
15. Are there broad regex repairs that should be made table-driven, provenance-gated, or builder-scoped?

## Suggested Files To Inspect First
Start with these files:
- `App/services/extraction_service.py`
- `App/services/parsing.py`
- `App/main.py`
- `App/services/store.py`
- `App/templates/spec_list.html`
- `tests/smoke_test.py`
- `IMPERIAL_GRID_TRACKER.md`
- `PRD.md`
- `Arch.md`
- `Project_state.md`
- `AGENTS.md`

For Imperial-specific review, prioritize:
- separator and row-band logic in `extraction_service.py`
- five-column row assembly and repair logic in `extraction_service.py`
- material-row postprocess and self-repair in `parsing.py`
- Imperial summary generation in `main.py`
- PDF QA checklist generation and ordering in `main.py`
- tests around `job 60`, `job 62`, `job 64`, and `job 67`

## Acceptance Discipline
For any code change suggested by review:
1. Run local smoke tests and compile checks.
2. Deploy only after local verification passes.
3. Fresh rerun the affected live job if parser behavior changes.
4. Compare against the source PDF.
5. Complete strict field-level PDF QA.
6. Update `PRD.md`, `Arch.md`, `Project_state.md`, `AGENTS.md`, and when relevant `IMPERIAL_GRID_TRACKER.md` for major changes.

The review should call out whether a proposed change requires live rerun and PDF QA. For parser changes, assume yes unless proven otherwise.
