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
- The app exposes a read-only historical spec result route at `/jobs/{job_id}/runs/{run_id}/spec-list`; it renders stored run JSON, does not mutate the latest snapshot, and does not allow export or PDF QA from the historical view.
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
5c. Builder routing identity is owned by the website job record, not by uploaded PDF header text. `Client`, `Builder`, logos, or external sheet branding may describe document origin, but they must not override the parser/finalizer/QA route implied by the job's assigned Builder.
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
  - provide dev-only grid debug artifacts under `tmp/imperial_grid_debug/`, using JSON and SVG overlays to show visible/inferred separators, image obstruction boxes, row bands, cell ownership, and content-grid boundaries
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
- Expected review focus for parser work is regression safety rather than code style: room-local ownership, builder-specific finalizers, PDF QA gating, and UI/export/schema consistency.
- `IMPERIAL_GRID_TRACKER.md` is the durable execution tracker for Imperial structural work. It maps the current codebase to three staged phases (`Grid Truth`, `Row Assembly`, `Semantic / Summary`) and records the live regression matrix, open blockers, and next target so Imperial work does not depend on chat-session memory.
- Architectural rule for Imperial structure work: `grid boundary recovery` is the upstream truth layer. When `AREA / ITEM` absorbs `SPECS / DESCRIPTION`, or merged-cell content spills across rows, the fix belongs in separator recovery / row assembly first, not in summary cleanup or UI-only patching.
- Display rule for Imperial room cards: preserve the source-table `AREA / ITEM` label when it is available. Parser-side normalization remains valid for tags, matching, and constrained repair, but the rendered title should not replace the original label text with a synthesized variant.
- Operational rule: use `fix this bug` as the default path for PDF-grounded live defects with a clear target field/room/result. Use `review this PR` when the code change affects shared parser flow, grouped-row cleanup, builder finalizers, or PDF QA state transitions.
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
21. Immediately generate or reset a `snapshot_verifications` row for the latest `raw_spec` snapshot with status `pending` and a field-level checklist derived from the extracted room/appliance fields.

### 3.5 Review Pipeline
1. Load latest raw snapshot.
2. Load reviewed snapshot if present.
3. Render flattened rows into editable HTML tables.
4. Save edited values as a reviewed snapshot, preserving the expanded appliance link fields.
5. Export from the reviewed snapshot if present, otherwise from raw snapshot.

### 3.6 PDF QA Pipeline
1. Every new `raw_spec` snapshot creates or resets a one-to-one `snapshot_verifications` row.
2. The verification record stores:
  - `snapshot_id`
  - `snapshot_kind`
  - `status`
  - `checked_by`
  - `checked_at`
  - `notes`
  - `checklist_json`
3. `checklist_json` stores field-level items such as room title, benchtops, cabinetry colour splits, toe kick, bulkheads, floating shelf, shelf, handles, accessories/others, sink/basin/tap, drawers/hinges/flooring, and appliance rows.
3a. Imperial joinery/material QA is now an explicit exception: the primary checklist focus is `material_rows` correctness, tag correctness, summary correctness, and the retained bottom fields (`Drawers`, `Hinges`, `Flooring`, `Sink`). `Tap` is intentionally excluded from Imperial primary QA.
4. The PDF QA page edits those checklist items directly and can save, mark pass, or mark fail.
4a. Final PDF QA signoff is source-PDF, field-by-field signoff. A checklist item is not `pass` merely because `extracted_value` is non-empty.
4b. Automated bulk `pass/na` writes based only on non-empty extracted values are invalid as final signoff and must not be recorded as accepted QA.
5. `passed` is only valid when every checklist item is `pass` or `na` and no item is `fail`.
6. Raw snapshots remain visible while QA is pending or failed, but formal exports are blocked until the latest raw-spec verification is `passed`.

### 3.7 Raw Spec List Pipeline
1. Load `snapshots.snapshot_kind = raw_spec` for the requested job.
2. Load the latest matching `snapshot_verifications` row for PDF QA state.
3. Flatten room, appliance, and other fields into read-only page rows.
4. Render the `Rooms` section as a vertical stack of wide horizontal room cards on desktop, with one display row per field and a separate metadata column.
5. Non-Imperial room cards show room fixtures (`Sink`, `Basin`, `Tap`) directly on the room card and split door colours into `Overheads`, `Base`, `Tall`, `Island`, and `Bar Back`, while trimming location-only suffixes and filtering obvious OCR noise.
6. Non-Imperial kitchen cards expand bench tops into `Wall Run` and `Island`; all other non-Imperial rooms collapse to a single `Benchtop` display row.
7. Non-Imperial cards only render door-colour groups that are both allowed for that room and actually present; `Island` and `Bar Back` are kitchen-only UI rows.
7a. Imperial room cards render `material_rows` in source order instead:
  - left column = `AREA / ITEM`
  - right column = lightly cleaned `SUPPLIER - SPECS / DESCRIPTION - NOTES`
  - preserve original handle-block wording order; do not aggressively split `HANDLES` text into artificial description/note fragments
  - prefer the most complete accepted raw-row/layout continuation text over truncated visual-subrow fragments when building the displayed value for desk / shelf / robe / study style rows
  - only retain `Drawers`, `Hinges`, `Flooring`, and `Sink` beneath the raw rows
  - omit `Tap` from Imperial room cards
8. Filter plumbing fixtures out of the `Appliances` table and export.
9. Render a `Material Summary` section that smart-deduplicates room-level door colours, handle models, and bench tops, using the split wall-run/island bench-top values when available, preserving distinct thickness/edge variants, and including floating-shelf materials in the bench-top summary bucket.
9a. Imperial summary entries are built directly from tagged `material_rows` and rendered as:
  - first line: normalized material text
  - second line: `Room: A | B | C`
  - room lists are de-duplicated and kept in source spec order
  - rows with failing or unresolved non-handle-specific `revalidation_status` are excluded from summary aggregation; handle-specific provenance fallback is allowed only for tightly scoped summary recovery
9b. Imperial summary aggregation includes a hard-boundary pollution gate. Rows containing page header/meta/table-heading contamination are excluded before `Door Colours / Handles / Bench Tops` grouping so notes-only fragments such as `Bulkhead:Colourboard` cannot become summary materials.
10. Render appliance official links as a clickable wrapped `Product` column.
11. Render non-room joinery sections such as `FEATURE TALL DOORS` in a dedicated `Special Sections` block instead of folding them into nearby rooms.
12. Show `Generated at`, `Extraction duration`, and the current PDF QA status in Brisbane time / human-readable duration format on the raw Spec List page.
13. Export that raw snapshot through a dedicated Excel route, including a `Special Sections` worksheet and the expanded room fields for `Floating Shelf`, `Shelf`, `LED`, `LED Note`, `Accessories`, and curated accessory `Others`, but only when PDF QA has passed.
14. Never fall back to `reviews` when rendering the raw Spec List page.
15. Start the page shell with the left navigation rail collapsed by default and let the user toggle it open client-side when needed.
16. When a parsed `site_address` exists, append it to the page heading as `job no - site address`; otherwise omit the separator.
17. Below roughly `1280px`, remove fixed wide-table minimum widths, force card containers to `min-width: 0`, and suppress page-level horizontal overflow so the raw snapshot remains readable in 1080p half-screen windows without horizontal dragging.
18. Shared UI density is intentionally tighter than the original baseline; the common stylesheet should shrink fonts and spacing to roughly 75% visual scale across jobs, builders, QA, and spec-list pages without using browser-level zoom.
19. Room-card sorting should treat grouped vanity titles such as `VANITIES` as part of the vanity/bathroom priority bucket instead of leaving them in generic `Other`.
20. Imperial debug metadata such as issue types, repair verdicts, order hints, and revalidation hints remain available in backend snapshot payloads, but the default frontend rendering suppresses them unless a debug-oriented UI is introduced later.
21. Ongoing Imperial structural work is tracked outside the rendered UI in `IMPERIAL_GRID_TRACKER.md`. The intended implementation order is:
  - strengthen `ImperialSeparatorModel` and separator provenance in `extraction_service.py`
  - stabilize `AREA / ITEM` anchored row assembly before later parsing stages
  - then tighten semantic subitems and summary inputs in `parsing.py` / `main.py`

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
- `GET /jobs/{job_id}/pdf-qa`
- `POST /jobs/{job_id}/pdf-qa/save`
- `POST /jobs/{job_id}/pdf-qa/mark-pass`
- `POST /jobs/{job_id}/pdf-qa/mark-fail`
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
