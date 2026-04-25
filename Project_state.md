# Spec_Extraction Project State

## Current Status
- Runnable project created under `Spec_Extraction`
- Root docs are present and synchronized:
  - `PRD.md`
  - `Arch.md`
  - `Project_state.md`
  - `AGENTS.md`
- GitHub collaboration scaffolding is now present locally:
- GitHub collaboration scaffolding is now present and the local repository is connected to a live remote:
  - `.github/PULL_REQUEST_TEMPLATE.md`
  - `.github/CODEOWNERS`
  - `tools/connect-github-remote.ps1`
  - `tools/new-feature-branch.ps1`
  - `GITHUB_SETUP.md`
- The current remote is `git@github.com:njch333-max/njch333-max-Spec_Extraction.git`
- The repo is ready for Codex-centered PR review on short-lived feature branches.
- The Builder × Page-Family table/grid-first matrix is now implemented:
  - `Imperial joinery/material`: Vision-grid-first
  - `Imperial sinkware/appliances`: deterministic row parser first
  - `Yellowwood cabinetry/vanity/flooring/tiling`: table/grid-first
  - `Simonds grouped property schedules`: table/grid-first
  - `Evoca finishes/flooring/plumbing/appliances`: table/grid-first
  - `Clarendon colour schedule`: heuristic-grid-first
  - `Clarendon AFC sinkware/appliances/flooring`: table/grid-first
  - `Clarendon drawing pages`: heuristic-only
- Workflow rule is now explicit:
  - default to `fix this bug` for specific live defects that are already PDF-grounded
- use `review this PR` for shared parser, grouped-row, builder-finalizer, user-workflow, or export changes
- Builder classification is website-owned, not PDF-owned:
  - parsing, QA scope, and regression routing follow the job's assigned Builder in the app
  - PDF header text such as `Client`, `Builder`, logos, or sheet styling may describe the source document but must not override the website Builder route
- Application code is implemented:
  - FastAPI web app
  - SQLite persistence
  - Jinja templates
  - separate worker loop
  - Excel and CSV export
  - raw Spec List page and dedicated Excel export
  - Material Summary block on the raw Spec List page
  - split wall-run and island bench-top display on room cards and exports
  - official appliance product-page link display plus official-size enrichment
  - deterministic Westinghouse-style official product-page probing before search fallback
  - deterministic AEG Australia product-page probing on `aegaustralia.com.au`
  - Fisher & Paykel official product-page matching with official-size extraction from structured product metadata
  - structure-first spec parsing for all builders, with lightweight page-layout analysis on every spec page and a speed-first runtime that uses selective Docling only on hard schedule/table pages for selected builders
  - shared structure-first `spec` parsing entrypoint for all builders, so new builders no longer default back to the legacy room-section text scan as their main room-creation path
  - layout diagnostics recorded per snapshot, including `layout_attempted`, `layout_succeeded`, `layout_mode`, `layout_pages`, `layout_provider`, `docling_pages`, and `layout_note`
  - Raw Snapshot pages now export the latest `raw_spec` directly through `Export Excel` without a PDF QA gate
  - Raw Snapshot Excel now uses a Claude-style workbook with `Summary`, `By Section`, optional `Flagged`, and filtered `Material Summary` sheets; the export keeps Claude's Arial font sizing, blue table headers, green section headers, yellow flagged rows, and section-header row layout, and Imperial v6 snapshots preserve `v6_review_rows` so the workbook can use the original v6 item boundaries, row wording, and notes
- Imperial v6 raw Spec List room cards now support grouped `HANDLES` rendering: the adapter emits HANDLES-only `display_groups` when supplier ownership is safe enough (including the accepted equal-share multi-supplier case), and the template renders one supplier header per group with indented lines beneath it
- Imperial raw Spec List `Material Summary -> Handles` now also consumes grouped `display_groups` when present: each supplier block counts as one distinct item, grouped entries dedupe across rooms only on exact `(supplier, lines[])` matches, and flat `display_lines` remain the fallback path when `display_groups` is absent. `Door Colours` and `Bench Tops` remain flat
- Imperial raw Spec List now also removes same-source v6 `HANDLES` subset duplicates before rendering and summary aggregation: if a same-room `HANDLES` row is only a strict subset of a longer canonical row, the shorter row is suppressed from Rooms and `Material Summary -> Handles`. This specifically covers `synthesized_from_room_handles` derivative rows such as the duplicated `BASE- ...` / `UPPER - ...` rows observed on live job 73 while keeping grouped-supplier rows and single-line rooms unchanged
- Imperial v6 room-field adapter now drops pure PDF-header `floor_type` pollution before assigning `room.flooring`; fix landed on 2026-04-25 in `82cc920` and is scoped to `App/services/imperial_v6_room_fields.py` only, leaving the extractor and other `_populate_*` helpers unchanged
- Imperial raw Spec List room-card template now renders v6 `material_rows[].notes` as of 2026-04-25 commit `396b184`: single-line rows append ` - (notes)` inline, multi-line rows render a trailing muted `(notes)` line, and `Material Summary` remains unchanged because notes rendering is scoped to the room-card v6 row block only
- Imperial raw Spec List room-card flattening now falls back to `rooms[].v6_review_rows[*].notes` when v6 `material_rows[].notes` is empty after parser finalization; fix landed on 2026-04-25 in commit `4e669be` and is scoped to `App/main.py` `_flatten_imperial_material_rows` plus a conservative `_match_v6_review_row_for` helper, leaving templates, Excel export, and `Material Summary` logic unchanged
  - user-visible PDF QA cards, routes, pending warnings, and export locks have been removed from Job Workspace and Raw Snapshot pages
  - new `raw_spec` snapshot writes no longer auto-create `snapshot_verifications`; the table and internal helpers remain as compatibility residue
  - Clarendon-only deterministic post-polish that rebuilds cleaner room text from schedule and fixture pages while preserving source-driven room ownership
  - default runtime is now speed-first: all builders use `layout + row-local parser`, `Imperial / Simonds / Evoca / Yellowwood` may additionally use selective Docling, and automatic `Heavy Vision` / `AI merge` are disabled by default
  - Imperial joinery/material selection sheets now override that default and use Vision-assisted table/grid boundary recovery by default, because these Excel-to-PDF schedules are more reliable when parsed as visible tables rather than free text
  - job-page Parse buttons with clearer run-status wording
  - live-polling run history with granular worker stage messages
  - extraction diagnostics showing the active runtime path, including heuristic-only vs selective Docling
  - global `37016`-style conservative parsing profile for all builders
  - source-driven room detection for all builders, with same-room-only merge behavior across pages/files
  - automatic `room master` selection for multi-file spec jobs, with supplement files limited to enriching rooms defined by the room-master document
  - Clarendon now treats `Drawings and Colours` as the deterministic room-name master when that file exists, and AFC/supplement files may enrich those rooms only
  - glued room-schedule headings such as `KITCHEN COLOUR SCHEDULEBENCHTOP...` are now normalized before room-master extraction so noisy heading text no longer becomes a room name
  - Clarendon glued headers such as `VanitiesDate`, `LaundryDate`, and `TheatreDate` now normalize back to clean room titles before room-master whitelisting
  - Clarendon appliance placeholder rows such as `N/A CLIENT TO CHECK` now preserve source wording but are deduplicated away when the same source file already contains a concrete model for that appliance type
  - grouped room-master headings such as `Vanities` remain grouped while supplement bathroom/ensuite/powder fixture pages enrich that grouped room instead of creating extra room rows
  - grouped-room material ownership is now same-room-only, so `Vanities` benchtops and door colours can only come from the authoritative `VANITIES COLOUR SCHEDULE` section while grouped fixture fallback remains limited to basin/tap/sink details
  - supplement-file upload order no longer matters because the room-master room set is precomputed before supplement files are parsed
  - authoritative schedule labels such as `WALK-IN-PANTRY` and `MEALS ROOM` are now preserved as display labels instead of being shortened to generic pantry names
  - multi-file Clarendon parsing now keeps `BUTLERS PANTRY` and `WALK-IN-PANTRY` separate when the room-master schedule defines both
  - composite supplement headings such as `Kitchen/Pantry/Family/Meals` no longer create synthetic rooms; only explicit room-master schedule pages can add rooms like `MEALS ROOM`
  - Yellowwood now uses selective Docling on grouped schedule/table pages while final field ownership remains row-local
  - Yellowwood final room names now prefer concrete joinery/spec titles such as `BED 1 ENSUITE VANITY`, `BATHROOM VANITY`, `BED 1 WALK IN ROBE`, and `BED 2/3/4 ROBE`
  - Yellowwood rooms without joinery/material evidence are dropped, and `robe` or `media` rooms remain only when they contain real material evidence such as `Polytec` or `Laminex`
  - Yellowwood vanity wet-area cleanup now removes non-joinery wet-area items such as shower, bath, toilet, towel-rail, towel-hook, floor-waste, feature-waste, shower-base/frame, basin-waste, bottle-trap, and in-wall-mixer-only rows from final room output, while keeping vanity `Basin / Basin Mixer` and room-local flooring
  - Clarendon AFC `CARPET & MAIN FLOOR TILE` pages now enrich existing room-master rooms with strict-PDF room-local flooring values instead of leaving those room-specific values blank or stranded in global notes
  - Broad AFC area labels such as `WIL/Linen/s Ground Floor` no longer backfill inferred `LAUNDRY` flooring on Clarendon jobs unless the PDF explicitly uses a laundry room label
  - Yellowwood `FLOORING` and `TILING SCHEDULE` pages now act as room-local flooring overlays for retained rooms such as `Kitchen`, robe rooms, and vanity rooms, while contents-page flooring lines are excluded from `others.flooring_notes`
  - Imperial builder parsing now uses page-top `... JOINERY SELECTION SHEET` titles as authoritative section boundaries, keeps continuation pages with the current section, and ignores signature/footer blocks during field extraction, including glued footer markers such as `CLIENT NAME: SIGNATURE: SIGNED DATE:` / `CLIENTNAMESIGNATURESIGNEDDATE` and related footer noise such as `NOTESSUPPLIER`
  - Imperial footer-noise handling now also treats `DOCUMENT REF` as a section-break/noise marker, and Vision-grid parsing isolates those footer/signature blocks before deterministic row mapping runs
  - Imperial room sections now also stop cleanly when later pages switch into non-joinery headings such as `APPLIANCES` or `SINKWARE & TAPWARE`, so office/joinery cards do not swallow appliance and tapware pages
  - Imperial room labels now preserve the currently extractable title body exactly, so names such as `WALK-BEHIND PANTRY`, `BENCH SEAT`, and `OFFICE` survive instead of collapsing to shortened room names
  - Imperial non-room sections such as `FEATURE TALL DOORS` are preserved as `special_sections` instead of being merged into nearby room cards
  - Imperial joinery parsing now enforces same-room-only, same-section-only, and same-row-or-adjacent-only material boundaries so kitchen, pantry, office, appliance, and tapware rows do not bleed into one another
  - Imperial bench-top parsing now defaults a plain `Bench Top` or `Cooktop Run` row to `Wall Run Bench Top` when no explicit wall-run row exists, while keeping island-only notes inside `Kitchen`
  - Imperial office pages now ignore `... TO TOP OF BENCHTOP` layout text and later address/title noise when resolving the actual office benchtop value
  - Imperial room accessories are now deduplicated within the same room before display and export
  - Imperial handle parsing now includes a delayed same-section recovery pass so footer-adjacent handle model lines can still populate `handles` without letting nearby cabinet-colour rows pollute the value
  - orientation-only notes such as `Vertical on Tall doors only` and `Horizontal on all` are now rejected as door-colour material values, so they do not populate `Tall` or `Island`
  - Imperial sink and tap room fields now prefer the builder-specific non-joinery overlay parser over noisier AI fixture guesses when both are present
  - Imperial joinery/material now uses `cell-aware raw rows + constrained self-repair` as the primary output path: `AREA / ITEM` is the row title, the visible `SUPPLIER / SPECS / DESCRIPTION / NOTES` wording is preserved with minimal cleanup, and each row carries `row_order`, `confidence`, `needs_review`, and row/cell provenance
  - Imperial room cards now render `material_rows` in source room order and source row order instead of the older split field stack; the retained footer fields for Imperial are now only `Drawers`, `Hinges`, `Flooring`, and `Sink`
- Imperial `Tap` is intentionally omitted from room cards, primary summary output, and primary Imperial raw export/display so sinkware/tap overlay noise does not dominate the joinery/material workflow
  - Imperial handle cleanup is now deliberately conservative: handle blocks keep original `Specs / Description` wording order and only remove footer noise, duplicated fragments, or duplicated supplier prefixes instead of splitting into aggressive `description/notes` fragments
  - Imperial `Material Summary` now aggregates directly from tagged `material_rows`; `Door Colours`, `Handles`, and `Bench Tops` are grouped by normalized material text and each item renders a de-duplicated `Room: ...` list in source spec order
  - Imperial handle rows now carry internal `handle_subitems` for summary generation. Room-card raw rows still preserve source-like handle wording, while `Material Summary / Handles` reads semantic subitem `summary_text` / `text` before raw row text, never provenance `raw_text`, so parser evidence cannot become a displayed handle material.
  - `tests/fixtures/imperial_37867_gold.json` is now the highest-priority Imperial gold fixture, covering room order, row order, handle-block preservation, summary output, and retained bottom fields against the source PDF
  - Imperial `job 60 / 37867` now passes source-PDF QA on run `1838`, which is the current live acceptance sample for the raw-row + self-repair presentation model
  - Imperial `material_rows` now carry explicit second-pass parser diagnostics and repair metadata (`issues`, `repair_candidates`, `repair_verdicts`, `repair_log`, `revalidation_issues`, `revalidation_status`) so parser-side review/recovery is structured instead of implicit
  - Imperial summary aggregation now respects `revalidation_status`, excluding rows that fail or remain unresolved outside tightly scoped handle-specific fallback cases
  - Imperial raw-row frontend diagnostics such as `Order hint`, `Review`, `Issues`, `Repairs`, `Pending`, and `Revalidation` are now hidden by default in `spec-list`; the backend still retains them for parser analysis and future debug tooling
  - Imperial continuation/display assembly now prefers fuller accepted layout/raw-row continuation over truncated visual-subrow snippets, which fixed the continuation-heavy desk/shelf family on `job 60`
  - `job 60 / run 2037 / build local-c061c5e6` is the current live continuation acceptance sample: desk/robe/study/shelf rows restored to source-PDF fidelity and PDF QA passed (`65 pass / 26 na / 0 fail / 0 pending`)
  - `IMPERIAL_GRID_TRACKER.md` now exists as the durable execution tracker for Imperial structure work; it records locked decisions, staged grid/row/semantic phases, the Imperial regression matrix (`52 / 55 / 56 / 59 / 60 / 61 / 62`), current blockers, and the next live acceptance target
  - Imperial structure work now explicitly treats `grid boundary recovery` as the first truth layer: if `AREA / ITEM` and `SPECS / DESCRIPTION` bleed together, the defect is tracked as a separator / row-assembly failure rather than a summary-only issue
  - Imperial Phase 1B row-band coalescing is now implemented locally: adjacent bands with no hard separator, or only `inferred_low`, can merge before cell extraction when they are same-cell continuation or label continuation, while `visible` and `inferred_high` remain hard row boundaries
  - Imperial Phase 2A row assembly now has a constrained leading-fragment repair for `GPO -> ACCESSORIES`, so weak-boundary accessory preludes can be owned by the following original `AREA / ITEM` label instead of becoming standalone rows
  - Imperial postprocess and display/checklist rendering now correct boundary-straddling size prefixes such as `450mm BIN`, keeping the original table label as `BIN` and moving the size token back into the value text when visible grid evidence puts it on the description side
- Imperial supplier-cell ownership now backfills empty supplier fields from clean cell-aware provenance, including `By Imperial`, so room-card raw rows and raw export values preserve supplier cells even when handle summary later removes suppliers; exact duplicate `notes == supplier` values are removed at final row assignment
  - Imperial summary gating now preserves valid tagged `FEATURE CABINETRY` rows with shaving-cabinet / mirrored-door / colourboard-shelf evidence even when the row also mentions `Standard Whiteboard Internals`, while still excluding true internals/robe noise; bench-top summary cleanup removes dangling separators after WFE/cutout tails are stripped
  - Imperial sinkware overlay now keeps single-word mounting continuations such as `Undermount`, completes split taphole tails such as `behind sink`, and prefers fuller source-backed sink/basin candidates when they restore missing supplier or mounting evidence
  - Imperial Phase 3B local rules now constrain sinkware taphole sharing by normalized fixture base and clean appliance layout cells before row-first extraction, preventing `N / A - By others` / image-column placeholder text from becoming appliance model content while preserving explicit `Specs - TBC` rows
  - Imperial Phase 3B targeted live regression passed on `job 67 / run 2244`, `job 64 / run 2245`, `job 62 / run 2246`, and `job 61 / run 2247` after deploying build `local-3551afd8`; this was targeted sinkware/appliance readback, not full strict PDF QA signoff
  - Imperial `job 67 / run 2250 / build local-c28adee4` now has formal strict source-PDF QA signoff with `51 pass / 0 na / 0 fail / 0 pending`; the cycle also fixed sinkware supplier/mounting display order, deterministic `MounƟng -> Mounting` fixture text cleanup, and Imperial PDF QA checklist ordering so canonical `row_order` wins over stale provenance row-index hints
  - Imperial `job 64 / run 2251 / build local-c28adee4` now has formal strict Phase 3B source-PDF QA signoff with `61 pass / 1 na / 0 fail / 0 pending`; the fresh rerun supersedes targeted `run 2245` and verifies `ACCESSORIES / GPO`, source-case flooring, `450mm BIN`, `By Imperial` feature cabinetry/handles, summary grouping, seven appliance rows including `Specs - TBC`, and sinkware supplier/mounting/taphole tails. The only `N/A` item is `KITCHEN / sink`, because the source sinkware page has no kitchen sink row.
  - Imperial `job 62 / run 2260 / build local-a1afcf24` now has formal strict Phase 3B source-PDF QA signoff with `65 pass / 20 na / 0 fail / 0 pending`; the fresh rerun supersedes targeted `run 2246` and verifies raw `LIGHTING` preservation, WIR `[Polytec]` lighting supplier ownership, KICKBOARDS shelf/door spillover cleanup, `STD not Softclose => Not Soft Close`, Momo Graf handle note recovery, five appliance rows on page 9, and kitchen/laundry sinkware `By Others` mounting/taphole text with `Tap` still excluded from Imperial room cards.
  - Imperial `job 61 / run 2269 / build local-363a0642` now has formal strict Phase 3B source-PDF QA signoff with `66 pass / 0 fail / 0 pending`; the fresh rerun verifies clean glass-door cabinetry labels, three-family handle summary, five appliance rows, Imperial `Tap` exclusion, and wet-area basin rows retaining `Taphole location: In Sink - Note: Urbane Brass Taps` by matching fixture-base signature.
  - Imperial grid debug overlays now separate parser truth from diagnostic evidence: `grid_rows` shows the repaired parser view, while `unrepaired_grid_rows` preserves the pre-repair five-column rows for boundary investigation
  - Imperial accessory postprocess now preserves accepted `GPO -> ACCESSORIES` repairs instead of trimming the accepted `GPO` prefix back out of the final snapshot
  - Imperial handle postprocess and display/checklist rendering now repair label/value spillover such as `Momo HANDLES oval`, restoring the table label to `HANDLES`, carrying the valid `Momo` prefix into the handle value, and preventing visible-separated `oval wardrobe tube` text from entering handle summary; the repair also checks provenance when the final row label has already been normalized to `HANDLES` and protects the accepted prefix from later visual-subrow rebuild rollback
  - Imperial room cards now have a locked display rule that `AREA / ITEM` should prefer the original table label text where available; parser normalization is still allowed internally for tags and constrained repair, but the UI should not invent a cleaner replacement title unless the original label is missing
  - Imperial hard-boundary parsing now rejects page header/meta/table-heading contamination before broader layout/vision candidates can override clean cell-grid rows; `IMAGE` cells are ignored as content and only remain usable as geometry signals for future grid recovery
  - Imperial supplier/notes ownership now includes a deterministic cell split for cases such as `Polytec Variation` plus `for Black - Venette`, and summary aggregation now rejects hard-boundary polluted rows before grouping `Door Colours`, `Handles`, or `Bench Tops`
  - Imperial appliance extraction now has a row-first layout path for appliance pages so placeholder rows such as `Specs - TBC` keep page/evidence and image-column text such as `N / A - By others` does not become the model text
  - Imperial `job 67 / run 2207` is the current hard-boundary acceptance sample: strict source-PDF QA passed after fixing benchtop header bleed, `WFE x 1` visual-break preservation, `Mirrorred` source spelling, seven appliance rows, and sinkware/basin `behind sink/basin` taphole tails
  - Imperial `job 64 / run 2212` is the current targeted ACCESSORIES regression sample: non-adjacent `GPO` spillover now merges back into `ACCESSORIES` and survives later self-repair cleanup
  - Imperial `job 64 / run 2251` is now strict Phase 3B source-PDF signed off with `61 pass / 1 na / 0 pending`; the fresh cycle supersedes `run 2225` for current-build sinkware/appliance acceptance and verifies `ACCESSORIES / GPO`, source-case flooring, `450mm BIN`, `By Imperial` feature cabinetry/handles, Door Colours/Handles/Bench Tops summary grouping, seven appliance rows, and sinkware supplier/mounting/taphole tails
- all room cards and exports now support a global `Tall` material field for tall cabinets / tall doors / tall panels when the source provides that split
- room cards and exports now also support optional `Floating Shelf`, conditional `Shelf`, explicit `LED Yes/No`, dedicated `LED Note`, ordered `Accessories`, and curated accessory `Others` rows
  - `Shelf` is now restricted to WIL/WIR/WIP/linen/robe-fit-out style rooms; a plain `PANTRY` keeps `Shelf` only when its local evidence clearly shows walk-in/open-shelving fit-out wording such as `WIP`, `Open Shelving`, or `Shelving Only`
  - `CARCASS & SHELF EDGES`, `SQUARE EDGE RAILS`, and main-room `OPEN FACED SHELVES` wording no longer count as room-level shelf evidence
- final room retention is now gated by true material evidence across builders; plumbing-only, flooring-only, handle-only, LED-only, and accessory-only rooms are dropped after builder finalization
- spec-list room cards now sort grouped vanity titles such as `VANITIES` into the same high-priority vanity bucket as `BATHROOM / ENSUITE / POWDER`, and the shared web UI now renders at a tighter ~75% visual density across jobs, builders, QA, and spec-list pages
  - the raw Spec List summary now shows `Extraction duration`, and `Floating Shelf` materials also contribute to the `Material Summary -> Bench Tops` bucket
  - the Job page temporarily hides the Review cards while the review UX is being redesigned, without removing the backend review model
  - all user-facing timestamps are now rendered in fixed Brisbane time (`YYYY-MM-DD HH:mm AEST`) across job lists, uploads, run history, export tables, and spec-list summary
  - parsed snapshots now carry a `site_address` when the source documents expose one, and the Job Workspace / Raw Spec List headers append that address as `job no - address`
  - Clarendon schedule polish now prefers `raw_text` over vision-normalized `text`, so kitchen schedule notes such as `KICKBOARDS`, `BULKHEAD SHADOWLINE`, `HANDLE 1/2`, `DOOR HINGES`, and `DRAWER RUNNERS` survive even when vision restructuring rewrites the page text
  - Clarendon address extraction now uses page-header stop markers so `Site Address:` lines do not absorb nearby joinery fields such as `BENCHTOP`, `DOOR COLOUR`, or `THERMOLAMINATE NOTES`
  - Material Summary normalization now preserves meaningful `profile`, `style`, and `model no.` detail for `Door Colours` and `Handles` instead of collapsing some values to a bare supplier
  - the Jobs list now shows `Created`, `Last Updated`, sorting controls for both timestamps, and a button-styled `Open` action that opens each job in a new tab
  - Job Workspace run history now shows real `Duration`, separate `Worker / Build`, and an `Open Result` action for succeeded spec runs with stored result payloads
  - succeeded spec runs now have a read-only historical result page at `/jobs/{job_id}/runs/{run_id}/spec-list`
  - the Job Workspace and Raw Spec List pages now start with the left navigation rail hidden and expose a client-side show/hide toggle, while the Jobs homepage keeps the rail visible
  - dense tables on the Jobs page, Job Workspace, Raw Spec List, Builders page, and Run History now collapse into stacked card-style rows below roughly `1280px` so 1080p half-screen windows remain readable without horizontal scrolling
  - Raw Snapshot room cards now collapse into a single-field-flow layout below roughly `1280px`, with wrapped long values and no fixed wide-table minimum widths, so half-screen 1080p windows do not require horizontal dragging
  - Imperial row parsing now recognizes auxiliary row starts such as `ISLAND CABINETRY COLOUR`, `GPO'S`, `BIN`, `HAMPER`, `HANGING RAIL`, `MIRRORED SHAVING CABINET`, and `EXTRA TOP IN ...` as stop markers, preventing benchtops, floating shelves, and handles from swallowing later rows
  - Imperial inline split logic now avoids splitting inside ordinary words such as `CABINETRY`, reducing false row breaks caused by OCR-glued all-caps text
  - Imperial `Hinges & Drawer Runners` rows now recover `Soft Close` even when OCR glues them to `Floor Type & Kick refacing required`
  - Imperial sink/tap pre-heading parsing now keeps local continuation lines such as finish/model notes while still resetting on explicit basin/tub labels, improving tap recovery without reintroducing cross-room sink contamination
  - Imperial jobs `41–48` now form a fixed regression pack for the Excel-to-PDF selection-sheet family: long multi-space sheets, short kitchen-only sheets, combined-room sheets, and single-room compact sheets
  - Half-screen raw snapshot layout now explicitly removes fixed content minima and hides horizontal overflow so 1080p split-screen use can wrap long room values instead of forcing sideways dragging
  - snapshot and run metadata now record parser strategy, worker PID, and app build ID
  - single-worker lease guard to prevent stale local worker processes from racing newer code on queued jobs
  - online-first deployment helper scripts that push the current repo state to `/opt/spec-extraction`, restart production services, and verify live health
  - legacy builder-rules routes retired from the UI and redirected back to the Builders page
  - vertically stacked wide horizontal room-card layout on the raw Spec List page
  - room-card fixture rows for sink, basin, and tap
  - split door-colour rows for overheads, base, island, and bar back
  - global `Tall` material support in room cards, review tables, and exports
  - dedicated `Special Sections` rendering and export for non-room joinery sections such as `FEATURE TALL DOORS`
  - non-kitchen room cards now suppress `Island` and `Bar Back`, and only show `Overheads` when that split is explicitly present in the authoritative room section
  - generic `DOORS/PANELS` values now fall back to `Base` only when the same room section has no explicit cabinetry group markers, preventing grouped-room door colours from being copied into the wrong split
  - normalized drawer/hinge soft-close states
  - canonical brand casing cleanup for supported brands such as Polytec, AEG, Westinghouse, and Fisher & Paykel
  - Yellowwood joinery-page parsing that maps `Back Benchtops` to kitchen wall-run bench tops and keeps island waterfall notes together
  - cabinet-only colour filtering that excludes external paint / Colorbond / garage / door / window finish colours from room joinery output
  - kitchen-only split benchtop display on the raw Spec List page, with non-kitchen rooms collapsed back to one benchtop row
  - Material Summary bench-top normalization now keeps distinct thickness and edge/apron variants instead of collapsing `20mm` and `40mm` entries together
  - Yellowwood grouped schedule pages now route through selective Docling while final field ownership stays row-local and source-driven
  - shared structure output now passes through a builder-finalizer layer so final room-title preservation, overlay merge priority, fixture blacklist enforcement, and grouped-row/property-row cleanup can be owned per builder instead of only by shared post-cleaning
  - Yellowwood finalization now preserves concrete titles such as `PANTRY`, `BED 1 MASTER ENSUITE VANITY`, `GROUND FLOOR POWDER ROOM`, `UPPER-LEVEL POWDER ROOM`, `BED 1 MASTER WALK IN ROBE FIT OUT`, and `BED 2/3/4/5 ROBE FIT OUT`, while suppressing fake room fragments such as `WIP` or generic collapsed `ROBE FIT OUT`
  - Yellowwood kitchen/plumbing overlays now rehydrate room-local `Sink` / `Tap`, keep island and wall-run benchtops separate, and treat `*To Bulkhead*` text as a note rather than a bulkhead material value
  - cleaned door-colour display that removes duplicated location suffixes and common OCR noise
  - plumbing fixtures filtered out of appliance presentation/export
  - auto-upload on file selection for spec and drawing files
  - Jobs-page `job_no` search
  - cache-busted CSS delivery so updated layouts are visible after restart/reload
  - Git helper scripts
- Deployment scripts are present:
  - `run_server.*`
  - `run_worker.*`
  - `install_systemd.sh`
  - `spec-extraction-web.service`
  - `spec-extraction-worker.service`
  - `spec.lxtransport.online.nginx.conf`
  - `spec-extraction.env.example`
  - `DEPLOY_LXTRANSPORT.md`
  - `build_deploy_zip.ps1`
- A production deployment bundle is now built locally as `spec-extraction-deploy.zip`.
- `spec.lxtransport.online` resolves to `43.160.209.86` and is now deployed live on the LXtransport Tencent Cloud server.
- The production stack is running through `nginx + systemd + uvicorn`, with `spec-extraction-web.service` and `spec-extraction-worker.service` active on the server.
- HTTPS for `spec.lxtransport.online` is now issued by Certbot and terminates correctly at Nginx.
- The last recorded live source-PDF QA state for the active 11-job regression matrix was:
  - `passed`: `job 1`, `job 12`, `job 14`, `job 19`, `job 24`, `job 28`, `job 39`, `job 41`, `job 46`, `job 49`, `job 50`
- The latest live reruns and source-PDF acceptance records also include:
  - Yellowwood kitchen `Shelf` is suppressed unless the same room has explicit shelf-source wording
  - Yellowwood handle strings with a prefixed pantry/base note are reformatted into a cleaner handle value instead of leaving the note in front of the model
  - grouped-row builders such as Evoca now re-run benchtop-other dedupe after display cleaning, preventing wall-run/island values from being reintroduced into `bench_tops_other`
  - Clarendon `door_colours_overheads` is recovered when the schedule explicitly labels upper/overhead cabinetry, without leaking generic door colour into `Overheads`
  - Clarendon tap cleanup preserves full source wording such as `Twin Handle Sink Mixer` instead of truncating valid model names at the word `Handle`
  - Imperial appliance dedupe now keeps `N / A - By others` dishwasher placeholders and merges make-bearing/noisy oven rows into clean `Westinghouse + WVE6516DD` output
  - Imperial sinkware semantic parsing now keeps laundry / powder / ensuite ownership separated while preserving sink mounting details such as `UNDERMOUNT`
- Imperial `job 49 / 50` passed source-PDF QA on the latest recorded build after Vision-grid-first joinery parsing, sink/tap recovery, appliance row cleanup, and the backend correction that classifies `job 50` as `Imperial`
  - Simonds grouped-row recovery now restores clean benchtop/shelf/sink/tap/handle values for `Study`, `Butlers/WIP`, `Laundry`, `Bathroom`, `Powder`, and `Rumpus`

## Imperial V6 Replacement Phase
A multi-step replacement of the legacy Imperial Docling + Heavy Vision extraction pipeline with a deterministic cell-aware extractor (`App/services/pdf_to_structured_json.py`) is underway. This phase is Imperial-only; other builders are not affected.

### Completed Steps
- **Step 2** (`step-2-complete`): introduced the v6 adapter surface in `App/services/imperial_v6_adapter.py` with `run_v6_extraction`, `build_material_rows_from_v6_section`, and `build_room_from_v6_section`. Added 10 unit tests (`tests/test_imperial_v6_adapter.py`).
- **Step 2.5** (`step-2-5-complete`): fixed Windows compatibility for the v6 extractor subprocess call, introduced the `PDF_EXTRACTOR_PATH` constant, and added a real Haldham Crescent Imperial PDF fixture.
- **Step 4a** (`step-4a-complete`): introduced the `USE_V6_IMPERIAL` feature flag as `parsing.USE_V6_IMPERIAL_EXTRACTOR`, added `_process_v6_imperial_document` in `parsing.py`, and connected Imperial documents through to v6 when the flag is enabled. Dispatch condition: flag on, builder is Imperial, document is room-master, document has a resolvable path.
- **Step 4b / 4b.5** (`step-4b-complete`): added `App/services/imperial_v6_room_fields.py` (268 lines) to populate 16 room-level fields from v6 section metadata and items. Coverage observed on baseline jobs 61/62/64/67: Tier 1 fields 100%, Tier 2 fields 82.4%, Tier 3 fields 50%. Includes cross-section `(ROOM)` marker lookup for sinkware/tapware, with acceptable markers covering `MASTER ENSUITE` ↔ `(ENSUITE)` and `KITCHEN & PANTRY` ↔ `(KITCHEN)` label variants. Full pytest: 905 passed.
- **Step 4c** (`step-4c-complete`): moved v6 dispatch from inside `parsing.parse_documents` to the top of `extraction_service.build_spec_snapshot` as a fast-path bypass (`_build_imperial_v6_fast_snapshot`). The fast path skips the legacy `_apply_layout_pipeline`, `_try_openai`, `_merge_ai_result`, `_stabilize_snapshot_layout`, `_apply_builder_specific_polish`, `_apply_imperial_row_polish`, `_build_raw_spec_crosscheck_snapshot`, and `_crosscheck_imperial_snapshot_with_raw`. Added `parser_strategy = "imperial_v6"` to `cleaning_rules.PARSER_STRATEGIES` and reconciled `runs.parser_strategy` after snapshot build in `worker.process_run`. The fix addresses a root cause identified in `STEP4C_DIAGNOSIS_REPORT.md`: v6 material rows were previously overwritten by the legacy Imperial row polish. Added 4 new dispatch tests in `tests/test_imperial_v6_path_dispatch.py`. Full pytest: 909 passed.

### Production Validation
- Step 4c was deployed to `spec.lxtransport.online` using `tools/deploy_online.py`. Flag-off Job 61 re-verification produced the same legacy output; flag-on runs produced `parser_strategy = imperial_v6`.
- The v6 fast path was validated on four Imperial source PDFs in production:
  - Job 71 (Haldham Crescent, 3 rooms, run duration 2s)
  - Job 72 (9 Greenland Court / 38211-2, 3 rooms, run duration 2s)
  - Job 73 (Lot 532 Sandpiper Terrace / 37558-2, 12 rooms, run duration 6s)
  - Kelvin Grove (17 Park Street, 12+ rooms, run duration ~5s)
- Observed performance improvement vs legacy Imperial pipeline: approximately 2s vs 57s on single-room/small PDFs.
- Snapshot diagnostics on v6 runs correctly report `layout_attempted = No`, `docling_attempted = No`, `vision_attempted = No`, and `layout_provider = pdf_to_structured_json_v6`.

### Known Limitations (Deferred Backlog)
Confirmed against the 4 production test PDFs. Tracked as non-blocking:

| Bug | Limitation | Severity | Current owner / fix | Notes |
| --- | --- | --- | --- | --- |
| 1 | `HANDLES` rows can repeat multiple times on Kitchen room cards when the source table expresses multiple handle rows | TBD | Step 4d candidate | Observed on Sandpiper |
| 2 | `Handles` material summary entries can end with a dangling ` - and -` fragment when concatenating multi-line supplier notes | TBD | Step 4d candidate | Observed on Greenland, Sandpiper WIR |
| 3 | `FLOORING` can occasionally populate with the column-header text `AREA / ITEM SPECS / DESCRIPTION IMAGE SUPPLIER NOTES` when the source row is missing | TBD | Step 4d candidate | Observed on Kelvin Grove UPPER-BED 3 Astrid, LWR STUDY DESK Evyn. Intended meaning is "no flooring specified." |
| 4 | The v6 extractor can merge visually adjacent `area` cells into a single `area_or_item` label | TBD | Deferred pending longer-term v6 extractor revision | Observed on Sandpiper: `BENCHTOP ISLAND CABINETRY COLOUR`, `BIN ACCESSORIES LED'S`, `LED'S HANDLES`. Underlying supplier/description/notes cells remain correctly associated with the merged row. This is a cell-grid recovery limit inside the v6 extractor itself; users should cross-reference the source PDF when the `area_or_item` label visually combines multiple row labels. 同型现象在 37330 (Job 61) KITCHEN 也出现：HANDLES + BIN 合并为 "HANDLES BIN"。但 v6 的 `_review_hint` 启发式没标记 37330 的合并（Sandpiper 的同型合并被标记了），说明 flag 启发式有覆盖盲区。 |
| 5 | Duplicate `notes` wording on selected rows | TBD | Step 4d candidate | Observed on TV UPPER cabinetry on Kelvin Grove |
| 6 | Room model layer (b) in 37330 KITCHEN drops area "UPPER CABINETRY COLOUR" | 🔴 High | Dedicated investigation after Step 4d (candidate layers below) | Data loss, not a merge |

**Bug 6 detailed observation** (2026-04-22):

- Source: 37330 (Lot 34 #8 Luca Court) Colour Selection PDF
- PDF KITCHEN page 1 has 9 actual areas
- Extractor layer (a) raw JSON KITCHEN has 8 items (`HANDLES` + `BIN` have already merged into 1 item, same pattern as Bug 1)
- Room model layer (b) website display shows only 7 KITCHEN areas
- **Layer (b) has one fewer area than layer (a): "UPPER CABINETRY COLOUR"**
  - Note: this is distinct from "UPPER CABINETRY COLOUR (GLASS DOORS ONLY)"
  - The latter is preserved; the former disappears
- Hypothesis: layer (b) dedupe/merge logic incorrectly folds these two similarly named areas into one

**Not done** (during Path C):

- Did not scan other jobs for the same loss pattern
- Did not open layer (b) code to locate the root cause
- Did not produce a fix plan

**Candidate layer locations** (grep observation, not deeply verified):

1. CABINETRY COLOUR matching logic around `imperial_v6_room_fields.py:91, 100`
   - line 91 explicitly matches "UPPER CABINETRY COLOUR"
   - line 100 uses loose matching with `"CABINETRY COLOUR" in area`, which may let
     "UPPER CABINETRY COLOUR" and "UPPER CABINETRY COLOUR (GLASS DOORS ONLY)"
     overwrite each other
2. Area canonical label mapping table in `extraction_service.py:1011-1326`
   - line 1011 maps "UPPER CABINETRY|OVERHEADS" -> "UPPER CABINETRY COLOUR"
   - the two areas may collide on the same key after canonicalization

The real location may be (1), (2), or the interaction between both. This requires dedicated investigation after Step 4d.

**Risk positioning**:

- Factory uses this output as reference data only, not for direct cutting/production
- Area data loss still affects factory cross-check efficiency, and this loss is hidden (unlike Bug 4 merge, which is visually obvious)
- Severity is in the same class as Bug 4, possibly higher (data loss > data merge)

After the factory feedback window ends, Bug 6 should be investigated before Bug 4.

### Remaining Work
- **Step 4d (candidate)**: patch bugs 1, 2, 3, and 5 above (all localized to `imperial_v6_room_fields.py` or text assembly). Bug 4 is deferred pending a longer-term v6 extractor revision.
- **After Step 4d**: investigate Bug 6 (room model layer area loss) separately.
  Investigation complexity may be higher than Bug 4: it spans `extraction_service.py` (~11700 lines) and
  `imperial_v6_room_fields.py` (268 lines), and the area-processing logic in `extraction_service.py`
  is shared with the legacy path. **Not in Step 4d scope**, to avoid mixing it with the
  small-patch nature of Step 4d.
- **Step 6**: delete the legacy Imperial pipeline code, specifically `_imperial_collect_page_fields` and the associated helper functions, the Imperial branch of `_apply_layout_pipeline`, `_apply_imperial_row_polish`, and `_crosscheck_imperial_snapshot_with_raw`. The `USE_V6_IMPERIAL` flag and `_build_imperial_v6_fast_snapshot` fallback branch are removed together in this step because flag-off behavior no longer has a target. Scope is limited to Imperial; non-Imperial builder code paths remain unchanged.

## Current Goals
1. Keep Builder and Job flows stable while iterating extraction quality
2. Improve extraction accuracy with better appliance parsing, smart material normalization, and bench-top splitting
3. Keep all builders on the same conservative, human-readable output style without reintroducing Builder-level configuration
4. Improve official model lookup coverage across more appliance brands and site structures
5. Add formal comparison UI in a later version
6. Decide later whether the raw Spec List page also needs a reviewed-data variant

## Important Constraints
- Major changes must update:
  - `PRD.md`
  - `Arch.md`
  - `Project_state.md`
- UI must remain English-only
- Secrets must stay outside source control
- The app should work even when OpenAI is not configured, using heuristic extraction
- Parse requests should fail fast on the job page if no matching spec or drawing files have been uploaded yet
- Production upload limits must stay aligned between Nginx and FastAPI, currently at `100 MB`
- Old snapshots without `analysis` metadata should still render safely in the UI
- Old snapshots without expanded appliance link fields or bench-top split fields should still render safely in the UI and exports
- New parse runs always use the fixed global conservative profile
- Smoke tests must not touch the real local app database
- Confirmed implementation work is only done after production deployment and live verification succeed on `spec.lxtransport.online`
- Parser-accuracy work is only done after the affected live rerun is checked against the source PDF, not just against an older webpage or snapshot
- New `spec` parse runs no longer enter a user-visible PDF QA workflow or block exports behind a verification status.
- Parser-accuracy work still requires source-PDF review of the affected fields; non-empty extracted values alone are not acceptance evidence.
- Raw Snapshot Excel and formal export actions should remain available as soon as a latest `raw_spec` snapshot exists.

## Remaining Work
- Continue driving Imperial structure work from `IMPERIAL_GRID_TRACKER.md` instead of ad hoc sample-by-sample cleanup
- Imperial `job 64` post-Phase-2A rollback cycle remains closed, and the latest current-build strict Phase 3B signoff is `run 2251 / build local-c28adee4` with PDF QA `passed` (`61 pass / 1 na / 0 pending`). The verified fixes include accepted `GPO -> ACCESSORIES` preservation, handle label/value spillover recovery, boundary-straddling `450mm BIN`, `By Imperial` supplier-cell backfill, feature-cabinetry summary gating, bench-top separator cleanup, seven appliance rows, and sinkware `By Others` / `Undermount` / taphole-tail repair.
- When reviewing Imperial debug overlays, treat `unrepaired_grid_rows` as evidence of the original row split only; acceptance must use repaired `grid_rows`, live parser output, and source-PDF review
- Do not reopen `job 64` unless a new live regression is reported; the earlier invalid bulk signoff has been replaced by strict source-PDF field QA, most recently on `run 2251`.
- Refine OCR fallback for image-heavy PDFs
- Improve room-section detection for more builder formats
- Improve official product URL lookup accuracy, size extraction coverage, and brand coverage
- Continue checking the new `LED Note` rollout on live reruns so true LED evidence such as `LED STRIP LIGHTING`, `LED LIGHTING`, or `LED's As per drawings` lands on the right room without reintroducing false positives from sinkware noise such as `LED Topmount` or `LED UNDERMOUTNED`
- Keep the active 5-builder / 11-job source-PDF regression matrix green after future parser changes:
  - `Clarendon`: `job 1`, `job 46`
  - `Yellowwood`: `job 12`, `job 24`
  - `Imperial`: `job 28`, `job 41`, `job 49`, `job 50`, `job 60`
  - `Simonds`: `job 14`, `job 19`
  - `Evoca`: `job 39`
- Continue validating the new builder-finalizer split on Yellowwood-heavy grouped schedule jobs such as `job 24`, especially pantry/WIP suppression, robe-fit-out title preservation, powder-room separation, kitchen plumbing reinjection, and vanity-room plumbing cleanup
- Continue tightening noisy field cleanup inside the fixed global conservative profile without reintroducing per-builder configuration
- Continue tightening grouped-room door-colour logic so `Vanities` only shows `Overheads` when the authoritative room section explicitly labels overhead cabinetry
- Continue tightening supplement-file room mapping so only clearly related fixture pages enrich grouped rooms while unrelated finish/glazing notes stay ignored
- Continue strengthening Imperial `AREA / ITEM` label-cell recovery so more rows come from true label cells instead of regex fallback
- Continue enriching Imperial row/cell provenance so every rendered raw row and summary entry can be traced back to specific source cells
- Continue lifting Imperial sinkware/appliance pages toward the same structured-row discipline without reintroducing `Tap` into Imperial primary UI/export
- Formal Phase 3B acceptance is now closed for `67/2250`, `64/2251`, `62/2260`, and `61/2269`; move the next Imperial cycle to the next reported grid/row blocker rather than replaying targeted overlay readbacks
- Continue reducing true row-boundary misses on long merged-cell Imperial pages so fewer rows need second-pass repair or fallback continuation
- Continue deciding which current `needs_review` issue types are safe to upgrade from passive diagnostics into automatic accepted repairs
- Extend deterministic model-page probing beyond the currently supported appliance brand patterns
- Expand model-number coverage for more appliance naming patterns beyond the current explicit rules
- Build the future comparison UI and diff logic
- Decide whether to add a global all-job Spec List index in a later phase
- Continue validating parsing changes through fresh online reruns on the affected jobs instead of relying on older snapshots
- Continue pushing the shared structure layer deeper into strict row-fragment field reconstruction so high-precision layout analysis also controls final field ownership, not just room/section boundaries
- Complete source-PDF review for the current Clarendon and Yellowwood regression samples after each fresh live rerun, rather than treating raw output as accepted

## Risks
- OCR fallback is currently warning-driven unless stronger OCR infrastructure or OpenAI vision is configured
- Official appliance lookup and size extraction depend on brand sites and search-result structures that may change over time
- Old workers can still exist locally, but the app now leases a single active worker at a time and records runtime metadata so stale-code runs are visible
- The OpenAI Responses integration is optional and depends on valid API credentials and model access
- The default configured extraction model is `gpt-4.1-mini`; local and production deployments should be kept in sync.
- Room material fields should remain room-local so supplement files cannot leak another room's benchtop or door-colour text into the current room.
- Imperial PDFs rely on page-top titles and row boundaries; if those titles or labels shift significantly in future templates, the Imperial-specific parser may need another template-family expansion.
- Brisbane time is rendered with a fixed UTC+10 `AEST` presentation helper; if future daylight-saving-sensitive locations are required, the display helper will need revisiting.

## Verification Completed
- Local `.venv` created inside the project
- Installed required runtime packages:
  - `fastapi`
  - `uvicorn`
  - `jinja2`
  - `openpyxl`
  - `pypdf`
  - `python-multipart`
  - `httpx`
  - `itsdangerous`
- `python -m compileall App tests` passed
- `python -m unittest discover -s tests -p smoke_test.py` passed
- Browser-flow smoke test passed for:
  - login
  - create builder
  - create job
  - open job detail page
- Production verification now includes:
  - `spec-extraction-web.service` active after restart
  - `spec-extraction-worker.service` active after restart
  - `https://spec.lxtransport.online/api/health` returns `{"status":"ok"}`
- Raw Spec List smoke coverage added for:
  - login protection
  - empty-state rendering
  - raw-only rendering even when reviewed data exists
  - material summary rendering
  - split wall-run / island bench-top rendering
  - clickable official appliance-link rendering
  - Unicode-preserving Excel export
- Job-page smoke coverage added for:
  - visible Parse action buttons
  - auto-upload inputs with no separate upload buttons
  - run creation blocked when the matching upload set is empty
  - `job_no` search filter
- Runtime and extraction smoke coverage added for:
  - `.env` OpenAI settings load before runtime constants
  - OpenAI fallback metadata when a request fails
  - fenced OpenAI JSON is accepted instead of falling back
  - malformed OpenAI field shapes no longer crash the whole run
  - explicit appliance model parsing for oven, dishwasher, and fridge rows
  - normalized soft-close parsing for drawers and hinges
  - canonical brand-casing cleanup and preserved benchtop text for Clarendon-style rows
  - official appliance-resource enrichment path
  - labeled H/W/D product-page size extraction
  - structured `height/width/depth` product-page size extraction
  - room fixture enrichment and door-colour split overlays
- Strategy and worker-runtime coverage added for:
  - all builders default to the global conservative profile
  - single-worker lease blocking a second owner
  - run-history rendering of parser strategy and runtime metadata
- UI coverage added for:
  - retired Builder rules routes redirect to `/builders`
  - Builders page no longer exposes a Cleaning Rules button
  - Job page shows the global extraction profile instead of Builder-specific rule summaries
- Spec List UI coverage added for:
  - room-card rendering
  - sink/basin/tap room display
  - plumbing appliance filtering
  - material summary counts
  - official product-link export column
  - analysis metadata export in the Excel Meta sheet
- Run History UI coverage added for:
  - htmx partial polling route
  - live OpenAI-stage message rendering
  - Clarendon polish stage rendering
  - official-size stage rendering
- Jobs/Workspace UI coverage added for:
  - `Created` / `Last Updated` sorting controls on `/jobs`
  - button-styled `Open` action on `/jobs`
  - `Duration` and `Worker / Build` rendering on run history
  - read-only historical spec result pages backed by stored run payloads
- Clarendon deterministic-polish coverage added for:
  - clean kitchen wall-run / island benchtop reconstruction
  - stable source-driven room field rebuilding from schedule and fixture pages, without forcing pantry/vanities/theatre/rumpus buckets
  - splashback cleanup to `Tiled splashback by others` when the source clearly indicates builder-tile handoff
  - handle cleanup that strips mounting-position noise while keeping the model/finish text
  - fixture cleanup that collapses multiline OCR fragments into single readable lines
  - soft-close fallback logic that prefers overlay values but still falls back to the parsed room field when the overlay is blank
- Multi-file room-master coverage added for:
  - automatic selection of a schedule-heavy room-master file over miscellany supplement files
  - recovery of glued `COLOUR SCHEDULE` headings into clean room labels
  - grouped `Vanities` master-room preservation while supplement bathroom pages enrich the grouped row
  - warning-driven ignore behavior for unmatched supplement room-like headings
- Clarendon multi-template coverage added for:
  - dense single-line schedule pages with `Mirror Splashback`
  - `Square Edge Handleless` extraction into `handles`
  - single-line kitchen benchtop splitting into wall-run and island values without pulling door/kickboard text into the benchtop field
  - `Drawings and Colours` room-master selection ahead of AFC files
  - glued room headers such as `VanitiesDate`, `LaundryDate`, and `TheatreDate`
- Yellowwood coverage added for:
  - selective Docling on grouped schedule/table pages
  - `Island Bench Top` recovery from grouped kitchen schedule rows
  - removal of pseudo-room names such as pantry cell content
  - preference for concrete room titles such as `BED 1 ENSUITE VANITY` and `BED 3 ROBE`
  - dropping rooms without joinery/material evidence while preserving real robe/media rooms when they contain material evidence
- Imperial parsing coverage added for:
  - page-top section-title detection and continuation-page handling
  - footer/signature exclusion
  - Phase 1A grid-truth diagnostics: page-structure bboxes, cell-ownership provenance, segment source/confidence, and dev-only JSON/SVG overlays via `tools/imperial_grid_debug.py`
  - Phase 1B separator-aware row-band coalescing for no-boundary / `inferred_low` same-cell continuation, with hard-stop behavior for `visible` / `inferred_high` separators and supplier-only prelude regression coverage
  - Phase 2A `AREA / ITEM` anchored row assembly for weak-boundary leading fragments such as `GPO` before `ACCESSORIES`, including hard-boundary non-merge coverage
  - Phase 2A grid-debug overlay semantics where repaired `grid_rows` match the parser path and `unrepaired_grid_rows` remain available for diagnosis
  - Phase 2A accessory postprocess protection so accepted leading-fragment repairs survive final material-row cleanup
  - Phase 2A handle label/value spillover repair for contaminated labels such as `Momo HANDLES oval`
  - Phase 3A handle semantic subitem summary coverage for `55 / 56 / 59 / 60 / 62`, including independent `No handles` / `Touch catch` / `finger space` families, PM2817 / HT576 / Voda identity dedupe, and rejection of non-handle absorbed material such as timber finish text or `Casters`
  - row-boundary extraction of kitchen benchtops, splashback, base, overheads, tall, toe kick, handles, and bulkhead values
  - `FEATURE TALL DOORS` export into `special_sections[]` instead of room cards
  - `Tall` rendering on room cards and dedicated Excel export for `special_sections`
  - grouped-title recovery when the `... JOINERY SELECTION SHEET` text appears after body rows in extracted PDF order
  - `Floating Shelf`, `LED`, `Accessories`, and curated accessory `Others` rendering/export safety
- Smoke tests now use an isolated temporary data directory instead of `App/data/`
- PDF-grounded regression coverage now includes high-risk Clarendon and Imperial fixtures so parser fixes are checked against source-PDF page text instead of only older webpage outputs
- Source-PDF acceptance is now the default parser QA rule: parser changes are considered correct only after the live rerun is checked against the source PDF, not just an older webpage or snapshot
- Worker smoke test passed for:
  - upload DOCX spec
  - queue spec extraction
  - process run
  - save raw snapshot to SQLite
