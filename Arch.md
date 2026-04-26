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
- The Jobs list keeps the left navigation visible, but the Job Workspace and Raw Spec List pages render the same shell with a client-side collapsible navigation rail that starts hidden on each visit.
- The Jobs list `Open` action is a button-styled control that opens each job in a new browser tab.
- The Jobs list supports `Created` and `Last Updated` sorting while preserving the current search filter.
- The Job Workspace run history shows actual `Duration`, separate `Worker / Build` metadata, and per-run actions such as `Open Result` for succeeded spec runs with stored result JSON.
- The app exposes a read-only historical spec result route at `/jobs/{job_id}/runs/{run_id}/spec-list`; it renders stored run JSON, does not mutate the latest snapshot, and does not allow export from the historical view.
- Dense tables switch to a stacked card-style presentation below roughly `1280px` so 1080p half-screen layouts stay readable without horizontal scrolling.

### 3.2 Persistence
- SQLite tables:
  - `builders`
  - `builder_templates`
  - `jobs`
  - `job_files`
  - `runs`
  - `snapshots`
  - `snapshot_verifications`
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
2. Extract `raw_text` from PDF or DOCX and keep it alongside a normalized `text` copy.
3. Build a lightweight page-layout object for every spec page, including `page_type`, `section_label`, `room_label`, `room_blocks`, and `rows`.
4. Run the speed-first builder policy:
   - `Clarendon`: heuristic-only
   - `Imperial / Simonds / Evoca / Yellowwood`: `layout + row-local parser + selective Docling`
   - default automatic `Heavy Vision`: disabled except for Imperial joinery/material selection sheets
   - default automatic `AI merge`: disabled
5. Apply `Docling` only to builder/page combinations that need structure recovery, such as grouped joinery schedules, cabinetry tables, vanity schedules, tiling schedules, and `Area / Item / Colour / Supplier` style pages. Docling runs per-page subset only and keeps OCR off by default.
5a. Builder × page-family provider order is fixed:
  - `Imperial joinery/material`: `Vision -> Docling -> pdfplumber -> heuristic text-grid`
  - `Imperial sinkware/appliances`: `Docling/pdfplumber -> deterministic row parser`, with Vision only as a later single-page fallback
  - `Yellowwood cabinetry/vanity/flooring/tiling`: `Docling -> pdfplumber -> heuristic text-grid`
  - `Simonds grouped property schedules`: `Docling -> pdfplumber -> heuristic text-grid`
  - `Evoca finishes/flooring/plumbing/appliances`: `Docling -> pdfplumber -> heuristic text-grid`
  - `Clarendon colour schedule`: `heuristic-grid-first -> pdfplumber -> text-grid`
  - `Clarendon AFC sinkware/appliances/flooring`: `pdfplumber -> Docling -> heuristic text-grid`
  - `Clarendon drawing pages`: heuristic-only
5b. The shared extraction order for non-drawing pages is now:
  - classify `page_family`
  - recover `GridPage / GridRow / GridCell` style structure
  - apply merged-cell carry-forward
  - map grid rows to room/appliance fields
  - run room-local overlay merge
  - run the builder finalizer
  - render and sort fields only after extraction is complete
5c. Builder routing identity is owned by the website job record, not by uploaded PDF header text. `Client`, `Builder`, logos, or external sheet branding may describe document origin, but they must not override the parser, finalizer, or regression route implied by the job's assigned Builder.
6. Run heuristic extraction into canonical schema, then rebuild shared fields through `layout_rows -> row-fragment -> row-local mapping` so supplier, model, profile, note, and value text stay attached to the owning row.
7. Run a builder-finalizer dispatch stage:
   - the shared layer owns page classification, room/row block detection, room-local overlays, and generic noise cleanup
   - each builder finalizer owns final room-title preservation, overlay merge priority, fixture blacklist enforcement, and grouped-row/property-row cleanup
   - this stage is where Clarendon room whitelisting, Yellowwood vanity/robe title preservation, Imperial fixture cleanup, and future Simonds/Evoca grouped-row cleanup are applied
8. Enrich room rows with fixture fields (`sink_info`, `basin_info`, `tap_info`), split door-colour fields (`door_colours_overheads`, `door_colours_base`, `door_colours_tall`, `door_colours_island`, `door_colours_bar_back`), and derived bench-top fields (`bench_tops_wall_run`, `bench_tops_island`, `bench_tops_other`).
8a. Imperial is now a deliberate exception for joinery/material pages:
  - after the five-column cell-aware recovery stage, persist `material_rows` as the primary truth layer instead of treating the split door-colour / benchtop fields as the main output
  - each row carries `area_or_item`, `supplier`, `specs_or_description`, `notes`, `tags`, `page_no`, `row_order`, `confidence`, `needs_review`, and row/cell provenance
  - room order follows spec appearance order, and row order follows source table order; no later display or finalize stage may re-sort Imperial material rows by tag or inferred semantics
  - this Imperial route still applies when the uploaded PDF is another builder's delegated colour-consult sheet, as long as the website job itself is classified as `Imperial`
8b. Imperial joinery/material rows now pass through an explicit second-pass control loop before persistence:
  - detect row-local `FieldIssue` objects such as label contamination, cross-row spillover, handle over-splitting, short-value/orphan fragments, and true canonical-order drift
  - generate constrained `RepairCandidate` / `RepairVerdict` records for accepted or pending fixes
  - run row-level `revalidation_status` after accepted repair so failed or unresolved rows can be gated out of Imperial summary output
  - keep diagnostics in the snapshot/backend, while the default raw Spec List frontend hides those review/order hints
9. Remove plumbing fixtures from appliance rows so they only appear on room rows.
10. Apply the fixed `Global Conservative` profile for every builder:
  - room identity is source-driven
  - field ownership is same-room-only, same-section-only, and same-row-or-row-fragment-only
  - supplement files may enrich existing rooms only and must not create new rooms outside the room-master set
  - `colour/material` and appliance placeholders use `original wording + light cleanup`, not semantic rewriting
11. Clarendon-specific behavior:
  - stays heuristic-only
  - if a `Drawings and Colours` file exists, it is the deterministic room-name master
  - final room names may only come from that master file
  - AFC/supplement pages may enrich existing rooms only
  - glued headers such as `VanitiesDate`, `LaundryDate`, and `TheatreDate` are normalized back to clean room titles
  - deterministic post-polish prefers `raw_text` over vision-normalized `text`
  - AFC `CARPET & MAIN FLOOR TILE` pages now act as room-local flooring overlays for existing master rooms such as `KITCHEN`, `BUTLERS PANTRY`, `THEATRE ROOM`, and `RUMPUS ROOM`
  - Clarendon flooring overlay is strict-PDF-only: broad AFC labels such as `WIL/Linen/s Ground Floor` must not be inferred back into `LAUNDRY`
  - Clarendon fixture cleanup must preserve full tap wording when the source contains valid names such as `Twin Handle Sink Mixer`; generic wet-area cleanup markers must not truncate legitimate tap model text
12. Yellowwood-specific behavior:
  - uses selective Docling for grouped schedule/table pages
  - preserves the more specific spec-title room names such as `PANTRY`, `BED 1 MASTER ENSUITE VANITY`, `GROUND FLOOR POWDER ROOM`, `UPPER-LEVEL POWDER ROOM`, `BED 1 MASTER WALK IN ROBE FIT OUT`, and `BED 2/3/4/5 ROBE FIT OUT`
  - retains rooms only when there is joinery/material evidence
  - fake room fragments such as `WIP`, row-note cells, shelving-only cell text, and collapsed generic labels like a single `ROBE FIT OUT` room are removed during the Yellowwood finalizer
  - `robe` and `media` rooms remain only when they contain real material evidence such as `Polytec` or `Laminex`
  - fixture-only wet-area parent rooms are merged into the corresponding vanity room instead of surviving as standalone rooms
  - vanity-room plumbing cleanup now removes non-joinery wet-area items entirely, including shower, bath, toilet, towel-rail, towel-hook, floor-waste, feature-waste, shower-base/frame, basin-waste, bottle-trap, and in-wall-mixer-only rows
  - only `Basin`, `Basin Mixer`, room-local flooring, and joinery/material fields are allowed to survive on final Yellowwood vanity room cards
  - the Yellowwood finalizer keeps `Kitchen` wall-run / island / other benchtops separate, preserves `Overhead Cupboards`, treats `*To Bulkhead*` text as a note instead of a bulkhead material value, and rehydrates kitchen sink/tap from plumbing overlays when needed
  - non-wet-area `FLOORING` pages and wet-area `TILING SCHEDULE` pages enrich retained room cards as room-local flooring overlays, while contents-page flooring text is excluded from `others.flooring_notes`
13. For Imperial-only spec runs, apply a title-driven section parser before the generic cleanup stages:
  - use the page-top `... JOINERY SELECTION SHEET` title as the authoritative section start
  - use the currently extractable title body as the authoritative room label, preserving values such as `WALK-BEHIND PANTRY`, `BENCH SEAT`, or `OFFICE` without shorthand aliases
  - treat joinery/material pages as table-first Excel-to-PDF layouts: Vision supplies the grid boundary layer for header rows, data rows, merged cells, and footer/signature isolation before deterministic mapping runs
  - enforce a hard `content_grid` boundary before `material_rows` persistence: clean cell-grid rows outrank broader layout/vision candidates, and candidates containing page header/meta/table-heading tokens are rejected or heavily down-ranked
  - keep page-structure and cell-ownership provenance with Imperial material rows: `table_header_bbox`, `content_grid_bbox`, `footer_bbox`, column ownership, and separator segment source/confidence must be inspectable before downstream cleanup
  - provide dev-only grid debug artifacts under `tmp/imperial_grid_debug/`, using JSON and SVG overlays to show visible/inferred separators, image obstruction boxes, row bands, cell ownership, and content-grid boundaries. In those artifacts, `grid_rows` represents the repaired parser view and `unrepaired_grid_rows` preserves pre-repair five-column rows for diagnosis.
  - keep `IMAGE` cells out of final Imperial content. Image geometry may help infer covered grid edges, but image text/OCR must not contribute to material rows, summaries, sinkware, or appliance values
  - split `SUPPLIER` and `NOTES` by recovered cell ownership. Recognized supplier prefixes such as `Polytec` or `By Others` may be separated from adjacent note tails, but row assemblers must not infer supplier/notes by whole-line free text
  - use the title to identify the section, but do not discard same-page body text that appears before the title in extracted reading order
  - keep untitled continuation pages attached to the current section until the next top title appears
  - break the current joinery section when the next page switches into non-joinery full-page headings such as `APPLIANCES` or `SINKWARE & TAPWARE`
  - stop section text collection at footer markers such as `CLIENT NAME`, `SIGNATURE`, and `SIGNED DATE`, including glued variants like `CLIENT NAME: SIGNATURE: SIGNED DATE:`, `CLIENTNAMESIGNATURESIGNEDDATE`, and footer noise such as `NOTESSUPPLIER`
  - avoid turning `... TO TOP OF BENCHTOP` layout text plus a later `OFFICE JOINERY SELECTION SHEET` title into a fake benchtop field
  - keep sinkware/tapware and appliance pages on deterministic text/overlay parsing by default; Vision on Imperial is for table-boundary recovery, not free-form final field generation
  - `CLIENT NAME`, `SIGNATURE`, `SIGNED DATE`, `NOTES SUPPLIER`, and `DOCUMENT REF` are always treated as footer-noise markers on Imperial pages and must never enter room/appliance fields
13a. After Imperial five-column recovery and section parsing, run a constrained self-repair pass before persistence:
  - validator marks row-order drift, missing `AREA / ITEM` labels, column spillover, room-ownership conflicts, handle over-splitting, and summary tag conflicts
  - self-repair may only fix those row-local issues using recorded provenance; it may not freely rewrite whole-room JSON
  - low-confidence repairs become `needs_review` / unresolved evidence instead of formal output

### 3.5 Source Control And Review
- The repository is prepared for a GitHub-hosted workflow centered on pull-request review.
- Local helper scripts support:
  - connecting an empty GitHub repository as `origin`
  - creating short-lived feature branches
  - preserving the existing checkpoint/history/restore flow
- `.github/PULL_REQUEST_TEMPLATE.md` and `.github/CODEOWNERS` define the default review shape once the remote repository is connected.
- Expected review focus for parser work is regression safety rather than code style: room-local ownership, builder-specific finalizers, and UI/export/schema consistency.
- `IMPERIAL_GRID_TRACKER.md` is the durable execution tracker for Imperial structural work. It maps the current codebase to three staged phases (`Grid Truth`, `Row Assembly`, `Semantic / Summary`) and records the live regression matrix, open blockers, and next target so Imperial work does not depend on chat-session memory.
- Architectural rule for Imperial structure work: `grid boundary recovery` is the upstream truth layer. When `AREA / ITEM` absorbs `SPECS / DESCRIPTION`, or merged-cell content spills across rows, the fix belongs in separator recovery / row assembly first, not in summary cleanup or UI-only patching.
- Display rule for Imperial room cards: preserve the source-table `AREA / ITEM` label when it is available. Parser-side normalization remains valid for tags, matching, and constrained repair, but the rendered title should not replace the original label text with a synthesized variant.
- Operational rule: use `fix this bug` as the default path for PDF-grounded live defects with a clear target field/room/result. Use `review this PR` when the code change affects shared parser flow, grouped-row cleanup, builder finalizers, user workflow, or exports.
 - `tests/fixtures/imperial_37867_gold.json` is the highest-priority Imperial regression fixture. Any change that affects Imperial raw rows, row order, handle preservation, summary grouping, or retained bottom fields must pass that fixture before broader Imperial reruns.
  - parse table-style rows so `BENCHTOPS`, `SPLASHBACK`, `UPPER CABINETRY COLOUR + TALL CABINETS`, `BASE CABINETRY COLOUR`, `KICKBOARDS`, and `HANDLES` stay on their own row boundaries
  - treat auxiliary all-caps row starts such as `ISLAND CABINETRY COLOUR`, `GPO'S`, `BIN`, `HAMPER`, `HANGING RAIL`, `MIRRORED SHAVING CABINET`, and `EXTRA TOP IN ...` as stop markers for the previous row, even when those rows do not yet map to a top-level room field
  - only split glued inline markers at real row starts or lowercase-to-uppercase row transitions, so words such as `CABINETRY` are never broken into fake `BIN` rows
  - recover `Soft Close` from `Hinges & Drawer Runners` even when OCR glues that row to `Floor Type & Kick refacing required`
  - keep material ownership same-room-only, same-section-only, and same-row-or-adjacent-only so kitchen rows cannot absorb pantry, office, appliance, or tapware values
  - default a plain `BENCHTOP` or `COOKTOP RUN` row to `Wall Run Bench Top` when no explicit wall-run row exists
  - deduplicate repeated `Accessories` values inside the same room card
  - reject orientation-only notes such as `Vertical on Tall doors only` or `Horizontal on all` as door-colour material values
  - prefer builder-specific Imperial sink/tap overlay text over noisier AI fixture guesses when both are present
  - emit non-room sections such as `FEATURE TALL DOORS` into `special_sections[]` instead of merging them into nearby room cards
  - recover delayed Imperial handle lines that appear later in the same section while rejecting adjacent cabinet-colour rows as handle noise
  - semantic sinkware parsing now ignores unrelated pre-heading basin/tub noise, keeps `UNDERMOUNT` / similar mounting suffixes attached to the correct sink item, and shares generic taphole notes only across the relevant sink cluster
14. Apply the fixed global cleaning rules after heuristic parsing, builder-finalizer cleanup, Imperial section parsing, and row-local field reconstruction so brand casing, door-colour cleanup, kitchen-only bench-top splitting, tall-cabinet capture, and soft-close normalization stay consistent across all builders.
15. Record analysis metadata in the snapshot: parser strategy, layout metadata, runtime identifiers, Docling metadata, and whether any manual OpenAI stage was attempted.
16. Normalize drawer and hinge states to `Soft Close`, `Not Soft Close`, or blank.
17. Look up official appliance resources by `make + model_no`, first probing deterministic brand-site model URLs where supported and then falling back to search-based discovery; AEG, Westinghouse, and Fisher & Paykel now extract official dimensions from product pages when available, including JSON-like structured metadata.
18. Before final appliance storage, placeholder rows such as `As Above`, `By Client`, `N/A - By others`, and `N/A CLIENT TO CHECK` keep their original wording, but same-source placeholder rows are deduplicated away when a concrete model of the same appliance type already exists.
19. Extract an optional `site_address` from the authoritative source text and carry it in the snapshot for header display on the Job Workspace and Raw Spec List pages.
20. Save the raw snapshot.
21. Save the latest `raw_spec` snapshot without automatically creating or resetting a user-facing verification record.

### 3.5 Review Pipeline
1. Load latest raw snapshot.
2. Load reviewed snapshot if present.
3. Render flattened rows into editable HTML tables.
4. Save edited values as a reviewed snapshot, preserving the expanded appliance link fields.
5. Export from the reviewed snapshot if present, otherwise from raw snapshot.

### 3.6 Retired PDF QA Compatibility Layer
1. The user-facing PDF QA workflow and routes have been removed.
2. New `raw_spec` snapshots do not automatically create or reset `snapshot_verifications` records.
3. The `snapshot_verifications` table remains in the schema as a compatibility and historical-data table. Its record shape stores:
  - `snapshot_id`
  - `snapshot_kind`
  - `status`
  - `checked_by`
  - `checked_at`
  - `notes`
  - `checklist_json`
4. Internal checklist builder helpers can remain for regression analysis and old data interpretation, but they are not invoked by `upsert_snapshot()` and do not gate exports.
5. Parser-accuracy acceptance remains source-PDF review outside the retired in-app PDF QA flow.

### 3.7 Raw Spec List Pipeline
1. Load `snapshots.snapshot_kind = raw_spec` for the requested job.
2. Do not load PDF QA state for the rendered page or export gate.
3. Flatten room, appliance, and other fields into read-only page rows.
4. Render the `Rooms` section as a vertical stack of wide horizontal room cards on desktop, with one display row per field and a separate metadata column.
5. Non-Imperial room cards show room fixtures (`Sink`, `Basin`, `Tap`) directly on the room card and split door colours into `Overheads`, `Base`, `Tall`, `Island`, and `Bar Back`, while trimming location-only suffixes and filtering obvious OCR noise.
6. Non-Imperial kitchen cards expand bench tops into `Wall Run` and `Island`; all other non-Imperial rooms collapse to a single `Benchtop` display row.
7. Non-Imperial cards only render door-colour groups that are both allowed for that room and actually present; `Island` and `Bar Back` are kitchen-only UI rows.
7a. Imperial room cards render `material_rows` in source order instead:
  - left column = `AREA / ITEM`
  - right column = lightly cleaned `SUPPLIER - SPECS / DESCRIPTION - NOTES`
  - preserve original handle-block wording order; do not aggressively split `HANDLES` text into artificial description/note fragments
  - when an Imperial v6 `HANDLES` row carries structured `display_groups`, render supplier-group headers with indented grouped lines on the raw Spec List page; otherwise fall back to the existing flat `display_lines` block
  - render row-local `notes` in the room-card row only: inline `text - (notes)` when the visible payload is one line and the notes string contains no `\n`, otherwise a trailing muted `(notes)` line with `.row-note-multiline { white-space: pre-line; }`; this rule does not alter `Material Summary`
  - prefer the most complete accepted raw-row/layout continuation text over truncated visual-subrow fragments when building the displayed value for desk / shelf / robe / study style rows
  - only retain `Drawers`, `Hinges`, `Flooring`, and `Sink` beneath the raw rows
  - omit `Tap` from Imperial room cards
7b. Imperial sink/basin fixture cleanup runs after row-local overlay parsing. It preserves source-equivalent supplier/mounting order (`Sink Mounting - Undermount - By Others`) and may normalize deterministic PDF text-layer encoding artifacts such as `MounƟng` to the visual-source word `Mounting`.
8. Filter plumbing fixtures out of the `Appliances` table and export.
9. Render a `Material Summary` section that smart-deduplicates room-level door colours, handle models, and bench tops, using the split wall-run/island bench-top values when available, preserving distinct thickness/edge variants, and including floating-shelf materials in the bench-top summary bucket.
9a. Imperial summary entries are built directly from tagged `material_rows` and rendered as:
  - first line: normalized material text
  - second line: `Room: A | B | C`
  - room lists are de-duplicated and kept in source spec order
  - rows with failing or unresolved non-handle-specific `revalidation_status` are excluded from summary aggregation; handle-specific provenance fallback is allowed only for tightly scoped summary recovery
9b. Imperial summary aggregation includes a hard-boundary pollution gate. Rows containing page header/meta/table-heading contamination are excluded before `Door Colours / Handles / Bench Tops` grouping so notes-only fragments such as `Bulkhead:Colourboard` cannot become summary materials.
9c. Imperial summary gating distinguishes true internals/robe noise from tagged feature-cabinetry rows. A `FEATURE CABINETRY` row with shaving-cabinet, mirrored-door, or colourboard-shelf evidence can still contribute to `Door Colours` even when the same source row mentions `Standard Whiteboard Internals`; bench-top summary normalization also strips dangling separators after WFE/cutout tails are removed.
9d. Imperial handle summary aggregation is subitem-first and identity-gated. `handle_subitems` provide the preferred source; summary canonicalization dedupes short/full PM2817, HT576, and Voda variants while keeping `No handles`, `Touch catch`, `finger space`, `PTO`, knobs, and pull handles as independent families. Absorbed inline provenance can recover true handle text, but non-handle material or accessory fragments such as timber finish text and `Casters` are rejected before grouping.
10. Render appliance official links as a clickable wrapped `Product` column.
11. Render non-room joinery sections such as `FEATURE TALL DOORS` in a dedicated `Special Sections` block instead of folding them into nearby rooms.
12. Show `Generated at` and `Extraction duration` in Brisbane time / human-readable duration format on the raw Spec List page.
13. Export the latest raw snapshot through `/jobs/{job_id}/spec-list.xlsx` whenever a latest `raw_spec` snapshot exists.
13a. Raw Spec List Excel uses the Claude-style workbook shape: `Summary`, `By Section`, optional `Flagged`, and `Material Summary` when Bench Tops / Door Colours / Handles rows exist.
13b. `By Section` contains `Section / Area`, `Specs / Description`, `Supplier`, `Notes`, `Page`, and `Flag`, with a blue Arial 11 header row, green Arial 11 section header rows, Arial 10 wrapped item rows, yellow flagged-row fill, and frozen header row. Section names stay in section header rows; item rows keep only the raw area/item label.
13c. `Material Summary` is a filtered review sheet for Bench Tops, Door Colours, and Handles only, with columns `Category`, `Section`, `Area`, `Supplier`, `Specs / Description`, and `Notes`.
13d. Imperial Raw Spec List Excel prefers `rooms[].v6_review_rows` when present, preserving the Claude/v6 source item boundaries, row wording, and notes before parser finalization. It falls back to `rooms[].material_rows` for older snapshots. Non-Imperial snapshots flatten room fields, appliances, special sections, others, and warnings into `By Section`.
13d-1. Imperial raw Spec List room-card rendering also consumes `rooms[].v6_review_rows[*].notes` as a fallback when flattened `material_rows[*].notes` is empty post-finalization, sharing the same notes-preservation channel Excel already uses while leaving all other room-card fields on `material_rows`.
13e. Historical run result pages stay read-only and do not expose Excel export.
13f. Imperial v6 adapter-owned display payload is now split by purpose: `display_lines` remains the flat downstream fallback surface, while optional HANDLES-only `display_groups` carries grouped `supplier + lines[]` data for grouped UI rendering.
13g. `main.py` consumes `display_groups` in two user-visible places only:
  - Imperial raw Spec List room-card `HANDLES` rows: supplier header plus grouped lines
  - Imperial raw Spec List `Material Summary -> Handles`: one grouped entry per supplier/lines block, deduped across rooms by exact `(supplier, tuple(lines))`, with grouped-entry count used as the distinct-item count
13h. When `display_groups` is absent, Imperial `Material Summary -> Handles` falls back to the existing flat `_imperial_material_row_handle_summary_candidates(...)` path. `Door Colours` and `Bench Tops` continue to use the existing flat summary-entry shape and rendering.
13i. Before Imperial raw Spec List room rows are flattened for rendering, `main.py` now runs a v6 HANDLES-only subset dedupe pass. If one same-room `HANDLES` row's normalized line set is a strict subset of another row's longer line set, the smaller row is dropped from both room-card rendering and `Material Summary -> Handles`. This display-layer pass accepts both canonical v6 rows and `synthesized_from_room_handles` derivative rows as eligible inputs, but it does not mutate stored snapshot rows.
13i-1. Inside `parsing.py`, `_imperial_reconcile_material_rows_with_room_fields(...)` remains the only emitter of `synthesized_from_room_handles` rows. Because the shared Imperial builder finalizer runs twice during snapshot construction, the reconcile emit gate must be idempotent: if a room already contains a synth row with the same normalized `provenance.layout_value_text`, the second pass skips re-emitting it. This guard sits at the emit gate only and does not alter the two-pass finalizer topology or handle-identity helpers.
14. Never fall back to `reviews` when rendering or exporting the raw Spec List page.
15. Start the page shell with the left navigation rail collapsed by default and let the user toggle it open client-side when needed.
16. When a parsed `site_address` exists, append it to the page heading as `job no - site address`; otherwise omit the separator.
17. Below roughly `1280px`, remove fixed wide-table minimum widths, force card containers to `min-width: 0`, and suppress page-level horizontal overflow so the raw snapshot remains readable in 1080p half-screen windows without horizontal dragging.
18. Shared UI density is intentionally tighter than the original baseline; the common stylesheet should shrink fonts and spacing to roughly 75% visual scale across jobs, builders, and spec-list pages without using browser-level zoom.
19. Room-card sorting should treat grouped vanity titles such as `VANITIES` as part of the vanity/bathroom priority bucket instead of leaving them in generic `Other`.
20. Imperial debug metadata such as issue types, repair verdicts, order hints, and revalidation hints remain available in backend snapshot payloads, but the default frontend rendering suppresses them unless a debug-oriented UI is introduced later.
21. Ongoing Imperial structural work is tracked outside the rendered UI in `IMPERIAL_GRID_TRACKER.md`. The intended implementation order is:
  - strengthen `ImperialSeparatorModel` and separator provenance in `extraction_service.py`
  - coalesce adjacent row bands before cell extraction only when separator evidence is soft (`none` / `inferred_low`) and row evidence supports same-cell continuation
  - repair weak-boundary leading fragments at the five-column row assembly layer, for example assigning `GPO` accessory text to the following `ACCESSORIES` row when no hard separator proves a separate row
  - correct boundary-straddling size prefixes during Imperial postprocess and display/checklist rendering, for example moving `450mm` from `AREA / ITEM` back into the `BIN` value when visible grid evidence places it on the description side
- backfill empty Imperial supplier fields from clean cell-aware provenance, including `By Imperial`, so raw rows and raw export values preserve the supplier cell even when summary later performs supplier-free grouping; exact duplicate notes equal to the supplier are removed during final row assignment
  - keep valid tagged `FEATURE CABINETRY` rows in `Door Colours` summary even when they include `Standard Whiteboard Internals`, and clean trailing bench-top separators after WFE/cutout tail stripping
  - keep single-word sinkware mounting continuations such as `Undermount` attached to the current room cluster and prefer fuller source candidates when they restore missing `By Others` supplier or mounting evidence without demoting product names such as `Undermount Sink`
  - repair deterministic sinkware cleanup tails after overlay selection, including `Sink Mounting Undermount sink` -> same-room undermount mounting evidence and split taphole endings such as `behind` / `behind basin sink` -> `behind sink` / `behind basin`
  - Phase 3B constrains sinkware taphole sharing by fixture-base signature: same-base generic sink rows may share a source sink-oriented taphole note, but a different fixture base cannot inherit that note, and utility rooms clear sink-derived pseudo-`basin_info`. Imperial appliance layout rows are cleaned before extraction so image/placeholder text such as `N / A - By others` cannot become model content, while `Specs - TBC` rows remain explicit row-first evidence.
  - keep debug overlay row semantics aligned with the parser path: repaired rows are shown as `grid_rows`; unrepaired intermediate rows are retained only as `unrepaired_grid_rows`
  - protect accepted leading-fragment repairs during material-row postprocess, so later accessory cleanup cannot remove the recovered `GPO` prefix before persistence
  - repair handle label/value spillover during material-row postprocess and display/checklist rendering, so contaminated labels such as `Momo HANDLES oval` become `HANDLES` before visual fragments and summary aggregation run, recover valid same-cell brand prefixes from provenance when the final label was already normalized, and prevent visual-subrow cleanup from trimming those accepted prefixes back out
  - stabilize `AREA / ITEM` anchored row assembly before later parsing stages
  - then tighten semantic subitems and summary inputs in `parsing.py` / `main.py`
- Phase 3A now attaches internal `handle_subitems` during Imperial material-row postprocess. `main.py` flattens those subitems into the summary layer, and raw export rendering uses the same subitem `summary_text` / `text` source values where subitem identity matters. Provenance fields such as subitem `raw_text` remain evidence only and are not summary input. The summary layer also applies a handle-identity gate and identity dedupe so PM2817 / HT576 / Voda short and coded variants merge correctly without admitting non-handle absorbed material.

### 3.7 Upload Interaction
1. Job detail uses the existing upload POST route.
2. File input controls submit their form immediately on `change`, so no dedicated upload button is required.
3. The page reloads after upload and the file list reflects the latest state.

### 3.8 Run History Refresh
1. The Job page renders the run history as an htmx partial.
2. The browser polls `/jobs/{job_id}/run-history` every few seconds.
3. The worker updates `runs.stage` and `runs.message` at key checkpoints such as loading, heuristic extraction, Clarendon polish, Docling structure, official model lookup, spec/manual discovery, official size extraction, and saving.
4. The partial replaces only the run-history card instead of reloading the full page.
5. Completed runs display actual `Duration`, `Worker / Build`, and a read-only `Open Result` action for succeeded spec runs with stored result payloads.

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
  - Clarendon now treats `Drawings and Colours` as the deterministic room-name master when present, and final room names are whitelisted to titles from that file
  - Yellowwood keeps only rooms with joinery/material evidence and prefers specific joinery/spec titles over generalized parent labels

### 3.11 Imperial V6 Fast Path
1. Imperial builders now have an alternative cell-aware extraction path that bypasses the legacy Docling/Heavy Vision pipeline. The new path is gated by the `USE_V6_IMPERIAL` feature flag and is the primary Imperial extractor when enabled.
2. The v6 path is a temporary transition tool. When Step 6 removes the Imperial legacy route, the flag and the legacy branch are removed together. Until then, turning the flag off reverts the Imperial runtime to the Phase 3B legacy pipeline with no other behavior change.
3. The v6 path applies to Imperial jobs only. Yellowwood, Simonds, Evoca, and Clarendon remain on their existing extraction pipelines and are not affected by this flag.
4. New source files:
   - `App/services/pdf_to_structured_json.py`: cell-aware raw PDF extractor that emits per-section item dicts with `area`, `specs`, `supplier`, `notes`, `_source`, and section metadata.
   - `App/services/imperial_v6_adapter.py`: maps v6 JSON sections into `RoomRow` and `material_rows` objects while preserving v6 provenance (`source_provider = "v6"`, `source_extractor = "pdf_to_structured_json_v6"`).
   - `App/services/imperial_v6_room_fields.py`: populates Imperial room-level fields from v6 section metadata and items, including cross-section `(ROOM)` marker lookup for sinkware/tapware.
5. New entrypoint: `extraction_service._build_imperial_v6_fast_snapshot` runs before `_apply_layout_pipeline` inside `build_spec_snapshot`. When the flag is on, the builder is Imperial, and at least one document has a non-empty `path`, the fast path is attempted.
6. Preserved inside the v6 fast path: `_load_documents`, `parsing.parse_documents`, `_process_v6_imperial_document`, `imperial_v6_adapter.run_v6_extraction`, `imperial_v6_adapter.build_room_from_v6_section`, `parsing._imperial_finalize_material_rows`, `parsing._imperial_attach_handle_subitems`, `parsing.enrich_snapshot_rooms`, `_enrich_snapshot_appliances`, and `parsing.apply_snapshot_cleaning_rules` (through `parse_documents`).
7. Bypassed inside the v6 fast path: `_apply_layout_pipeline`, `_try_openai`, `_merge_ai_result`, `_stabilize_snapshot_layout`, `_apply_builder_specific_polish`, `_apply_imperial_row_polish`, `_build_raw_spec_crosscheck_snapshot`, `_crosscheck_imperial_snapshot_with_raw`, the Docling layout branch, and the Heavy Vision branch.
8. Fallback behavior: if `parse_documents` raises during the v6 fast path, or if the resulting snapshot contains no material rows with v6 provenance, `_build_imperial_v6_fast_snapshot` emits an `imperial_v6_fallback` progress warning, returns `None`, and lets the legacy Imperial pipeline run normally. The run does not fail just because v6 failed.
9. New parser strategy `imperial_v6` is added to `cleaning_rules.PARSER_STRATEGIES`. Snapshot analysis fields `parser_strategy`, `mode`, `layout_provider`, `layout_mode`, `layout_attempted`, `docling_attempted`, and `vision_attempted` are set explicitly in the v6 path to reflect that no layout/docling/vision work was run. The worker reconciles `runs.parser_strategy` against the snapshot's `analysis.parser_strategy` after the run so the DB reflects actual dispatch.
10. Observed production performance for the v6 fast path on Imperial spec PDFs is typically 2-6 seconds per job, compared with 40-60 seconds on the legacy Docling/Heavy Vision path.

### 3.12 Actual v6 Architecture (2026-04-22 Update)
The v6 fast path is a two-layer process, not a single-layer extractor:

**(a) Extractor layer**: `pdf_to_structured_json.py` (758 lines)
- Input: PDF
- Output: section-based JSON
- Schema: `{source_pdf, pages, sections[].{section_title, metadata, items, pages}}`
- Item fields: `area`, `specs`, `image`, `supplier`, `notes`, `_source.{page,row_index,method}`, with optional `_review_hint`
- Known bugs 1-5 all live in this layer.

**(b) Post-processing layer**: spans two files
- `extraction_service.py` (~11700 lines): `_build_imperial_v6_fast_snapshot`, area canonical label mapping (lines 1011-1326), label merge/conversion relationships (lines 4124-4173, etc.), and many area-processing regexes
- `imperial_v6_room_fields.py` (268 lines): room-level field population
- Output: room-based model, which feeds the website display and factory reference data
- Transformations: sections -> rooms + appliances + sinkware; add confidence/evidence; add room-specific attributes (`drawers` / `hinges` / `flooring` / `sink`); deduplicate Material Summary
- Layer (b) shares `extraction_service.py` area-processing logic with the legacy Docling + Heavy Vision path.
- Bug 6 has been observed in this layer.

Bug reports must first identify which layer owns the defect. Compare the raw JSON emitted by `pdf_to_structured_json.py` with the website display:
- Raw JSON has the item but the website does not -> layer (b) bug, such as Bug 6
- Raw JSON is wrong -> layer (a) bug, such as Bugs 1-5
- Both are wrong in the same shape -> layer (a) bug passed through layer (b)

Auxiliary tool: `render_v6_review.py` (local tool, not in mainline) renders layer (a) raw JSON to Excel, providing a baseline for comparison against the layer (b) website display.

## 4. Canonical Schema

### Rooms
- One row per source-driven room
- Array-like fields are stored as lists in JSON and flattened with ` | ` in the review UI
- `room_key` is a source-driven normalized identity, `original_room_label` preserves the detected source label
- Room rows also carry fixture fields for sinks, basins, and taps plus split door-colour and bench-top display fields, including the global `door_colours_tall` split for tall-cabinet material.
- Imperial room rows additionally carry `material_rows` as the primary joinery/material truth layer; the older split fields remain compatibility outputs only and are no longer the authoritative source for Imperial room-card rendering or Imperial material summary generation.
- Room rows now also support `floating_shelf`, conditional `shelf`, `led`, ordered `accessories`, and curated `other_items` accessory labels such as `RAIL` and `JEWELLERY INSERT`. `Shelf` is source-driven and same-room-local: it must come from explicit shelf wording in that room's own source text, not from generic fit-out notes or nearby room content.
- The finalizer also gates `Shelf` by room family. It should only survive for simple fit-out/storage spaces such as `WIP`, `WIR`, `WIL`, `Linen Cupboard/Fit Out`, and robe-fit-out rooms. A plain `PANTRY` may keep `Shelf` only when its local evidence clearly shows walk-in/open-shelving fit-out wording such as `WIP`, `Open Shelving`, or `Shelving Only`. Main rooms such as `Kitchen`, `Butlers Pantry`, `Laundry`, and vanity/bathroom spaces must not keep `Shelf`, even if their text contains construction phrases like `CARCASS & SHELF EDGES` or `OPEN FACED SHELVES`.
- Builder finalization now applies a global material-evidence gate after overlays merge: rooms survive only when they hold true joinery/material fields, not merely handles, plumbing fixtures, flooring, LED, or accessory text.
- Snapshot payloads now also carry an optional `site_address` string extracted from source documents.
- Clarendon rows pass through a deterministic post-polish layer after layout stabilization so handle strings, fixture text, splashback notes, and soft-close fallbacks stay readable without changing source-driven room ownership.
- That Clarendon post-polish now detects at least two schedule families: the `37016` reference family and the denser single-line `LUXE / handleless / mirror splashback` family, then applies family-specific field splitting before the shared compact-summary cleanup.
- LED evidence is handled as a dedicated room pair: parsing normalizes `led` to explicit `Yes/No`, keeps the matched source wording in `led_note`, and only the presentation layer suppresses `No` rows for readability.

### Special Sections
- Non-room sections such as `FEATURE TALL DOORS` are stored in `special_sections[]`.
- Each special section carries:
  - `section_key`
  - `original_section_label`
  - `fields`
  - `source_file`
  - `page_refs`
  - `evidence_snippet`
  - `confidence`
- The raw Spec List page renders them separately from rooms, and exports keep them in a dedicated worksheet.

### Appliances
- One row per appliance
- Each row carries source metadata and confidence
- Sink, basin, tap, and tub fixture rows are excluded from the appliance presentation/export layer because they are surfaced on the corresponding room row.
- Imperial sinkware/basin overlays are resolved before room display using row-first fixture text, normalized fixture-base signatures, and source-equivalent supplier/note ordering. Cross-room taphole or note backfill is allowed only when the fixture base matches; utility-room pseudo-`basin_info` is cleared.
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
- The repo now includes `tools/deploy_online.py` and `tools/deploy_online.ps1` to stage selected repo files to the LXtransport host, install them into `/opt/spec-extraction`, restart `spec-extraction-web.service` and `spec-extraction-worker.service`, and validate the live health endpoint. The deploy include set also ships `IMPERIAL_GRID_TRACKER.md` so production-side parser work has the same durable tracker as the local repo.

## 6.1 Presentation Timezone
- SQLite and snapshot timestamps remain stored in UTC.
- The FastAPI presentation layer converts user-facing timestamps to a fixed Brisbane timezone object (`AEST`, UTC+10) before rendering:
  - jobs list
  - builder template upload tables
  - job file tables
  - export file tables
  - run history
  - raw spec snapshot summary

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

Parser-accuracy changes must be validated against the source PDF itself after a fresh rerun. Older webpages or snapshots are useful references, but they are not the acceptance source of truth.

This rule is enforced by the major-change checkpoint script.

## 10. Test Safety
- Smoke tests must point `SPEC_EXTRACTION_DATA_DIR` at a temporary directory before importing the app.
- Test setup and teardown must never clear the real `App/data/` SQLite database.
