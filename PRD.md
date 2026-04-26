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
4. Choose one or more spec or drawing files and let the page upload them immediately.
5. Click `Parse Spec Files` to create a parse run, then wait for the worker to finish.
6. Review the extracted `Rooms`, `Hardware`, `Appliances`, and `Others`.
7. Edit incorrect values and save the reviewed result.
8. Export the reviewed result to Excel or CSV.
9. Open a dedicated raw Spec List page for a job.
10. Export the raw Spec List page to Excel.
11. Search finished jobs by `job_no` from the main Jobs page.
12. Upload production drawing PDFs so compare-ready data is stored for a future release.

## 4. Functional Requirements

### 4.1 Builder Library
- Create Builder records with name, slug, and notes.
- Upload, list, and delete template files for each Builder.
- Store template files under a Builder-specific folder.
- All builders must use the same fixed global parsing profile.
- Builder records no longer expose user-editable parsing rules or parser-strategy settings.

### 4.2 Job Management
- Create jobs with unique `job_no`.
- Each job must belong to exactly one Builder.
- The website job's assigned Builder is the authoritative routing identity for parsing, QA, and regression coverage. PDF header text such as `Client`, `Builder`, logos, or sheet styling must not override that Builder classification.
- List jobs with status summary.
- Allow sorting the Jobs list by `Created` or `Last Updated`.
- Allow `job_no` search from the main job list using partial-match input and explicit submit.
- View a job detail page with files, runs, results, and exports.
- The Jobs list `Open` action must render as a button-styled control and open each job in a new browser tab so the list page remains available.
- The Job Workspace run history must show actual parse `Duration`, separate `Worker / Build` metadata, and an `Open Result` action for succeeded spec runs with stored result JSON.
- The app must provide a read-only historical spec-result page for succeeded spec runs, using stored run payloads instead of the latest snapshot, without enabling export from that historical view.

### 4.3 File Support
- Spec files: `PDF`, `DOCX`
- Drawing files: `PDF`
- Allow multiple uploaded spec files per job.
- Allow multiple uploaded drawing files per job.

### 4.4 Extraction
- Extract text directly from digital PDFs and DOCX files.
- Mark low-text PDF pages for OCR or vision fallback.
- All spec parsing must follow a structure-first pipeline: detect page layout first, then fill field values only from matched room and row blocks.
- Only allow parsing to start when the job already has at least one matching uploaded file.
- Store extraction metadata for each raw snapshot, including whether OpenAI was attempted, whether it succeeded, and which model was used.
- Produce a canonical JSON result containing:
  - room rows,
  - appliance rows,
  - other notes,
  - evidence and confidence,
  - source references.
- Merge information across multiple spec files in the same job.
- For multi-file spec jobs, select one authoritative room-master file automatically and use it to define the room list; other spec files may only enrich existing rooms and appliances.
- For multi-file Clarendon jobs, if any uploaded spec file name contains `Drawings and Colours`, that file must become the room-name master ahead of score-based selection.
- For Clarendon jobs, final room names may only come from titles found in the selected `Drawings and Colours` master file; AFC or supplement files may enrich those rooms only and may not create new room names.
- Room-master detection must normalize glued headings such as `KITCHEN COLOUR SCHEDULEBENCHTOP...` so only the clean room heading becomes the room label.
- Clarendon glued headers such as `VanitiesDate`, `LaundryDate`, and `TheatreDate` must normalize back to clean room titles before room-master whitelisting is applied.
- The room-master room set must be established before supplement files are processed, so supplement-file upload order cannot create extra rooms.
- The default hot path is now speed-first:
  - all builders run `layout + row-local parser`
  - `Imperial`, `Simonds`, `Evoca`, and `Yellowwood` may additionally run selective `Docling` on difficult schedule/table pages
  - default automatic `Heavy Vision` is disabled for every builder except Imperial joinery/material selection sheets
  - Imperial joinery/material pages now default to Vision-assisted table/grid boundary detection so Excel-style PDFs are parsed as visible tables instead of free text
  - default automatic `AI merge` is disabled
- Builder × page-family extraction matrix is now explicit:
  - `Imperial joinery/material`: Vision-grid-first
  - `Imperial sinkware/appliances`: deterministic row parser first, with table/grid recovery when available
  - `Yellowwood cabinetry/vanity/flooring/tiling`: table/grid-first without default Vision
  - `Simonds grouped property schedules`: table/grid-first without default Vision
  - `Evoca finishes/flooring/plumbing/appliances`: table/grid-first without default Vision
  - `Clarendon colour schedule`: heuristic-grid-first
  - `Clarendon AFC sinkware/appliances/flooring`: table/grid-first without default Vision
  - `Clarendon drawing pages`: heuristic-only
- OpenAI-powered `AI merge` remains a manual rescue tool for targeted parser-debug or QA-failed jobs; it is not part of the normal production pipeline.
- Imperial joinery/material Vision is a boundary-recognition layer, not a free-form final extractor. It is used to recover header rows, column boundaries, merged-cell carry-forward, and footer/signature isolation before cell-aware raw-row reconstruction runs.
- Imperial joinery/material output now uses a `material_rows` truth layer. Each retained row must preserve the source table's `AREA / ITEM` label plus the lightly cleaned `SUPPLIER / SPECS / DESCRIPTION / NOTES` text, along with `row_order`, `confidence`, `needs_review`, and row/cell provenance.
- Imperial parser behavior now includes a constrained self-repair pass after initial cell-aware row assembly. That pass may only repair row order, missing label cells, column spillover, room ownership, and summary tags; it must not freely rewrite whole-room JSON.
- Imperial parser behavior now also includes an explicit second-pass `validator -> repair -> re-validate` flow on `material_rows`. Each affected row may carry structured issues, repair candidates, repair verdicts, repair logs, and revalidation status so row-local problems can be detected, repaired conservatively, and either accepted, left `needs_review`, or excluded from summary output.
- Imperial row-order diagnostics are now advisory only unless a true canonical-order conflict is proven. Mere disagreement between legacy order signals must not mark every row as review-failed or wipe the summary.
- Imperial grid-truth work must produce inspectable page-structure evidence before downstream cleanup: table-header/content-grid/footer bboxes, separator source/confidence, image obstruction boxes, row bands, and cell ownership should be available in backend provenance or dev-only debug overlays.
- Imperial dev debugging may write JSON/SVG grid overlays under `tmp/imperial_grid_debug/`; these artifacts are not product UI and must not be committed as source data.
- Imperial debug overlays must distinguish parser output from intermediate evidence. `grid_rows` should match the repaired parser row view used downstream, while `unrepaired_grid_rows` may expose pre-repair five-column rows for boundary diagnosis.
- Imperial row-band assembly must respect separator confidence before cell extraction. `visible` and `inferred_high` separators are hard row boundaries; `none` and `inferred_low` may keep adjacent bands mergeable only for same-cell continuation or label continuation, not for supplier-only preludes that belong to a later row.
- Imperial `AREA / ITEM` anchored row assembly must handle weak-boundary leading fragments that visually precede their owning label. Example: a `GPO` fragment with power-point/socket wording immediately before `ACCESSORIES` belongs to the `ACCESSORIES` raw row unless a visible or high-confidence separator proves it is a separate source row.
- Imperial postprocess and display/checklist rendering must correct boundary-straddling size prefixes that the column model placed into `AREA / ITEM` when visible grid evidence shows they belong to `SPECS / DESCRIPTION`; for example `450mm BIN` should render the original label as `BIN` and move `450mm` back into the value.
- Imperial supplier-cell ownership must preserve clean supplier cells such as `By Imperial` in room-card raw rows and raw export values. Handle summary may drop suppliers for semantic grouping, but raw rows must still display the supplier cell when it exists in the PDF. If the same supplier text also appears in `notes`, remove the duplicate notes value rather than rendering `[By Imperial] ... (By Imperial)`.
- Imperial postprocess must preserve accepted leading-fragment repairs. Once provenance records `leading_fragment_repair = gpo_to_accessories` or equivalent accepted `GPO` spillover evidence, later accessory cleanup must not trim the recovered `GPO` prefix out of the final `material_rows` value.
- Imperial postprocess and display/export rendering must repair handle label/value spillover before summary. If value text such as `Momo` or `oval wardrobe tube` leaks into `AREA / ITEM` around a `HANDLES` label, the displayed row label should return to the source label `HANDLES`, valid handle-brand prefix text should move into the value, and visible-separated non-handle text must not enter handle summary. If the final label has already been normalized to `HANDLES`, the repair must still inspect row/cell provenance such as `raw_area_or_item`, `layout_row_label`, and label-cell text to recover valid same-cell brand prefixes. Later visual-subrow cleanup and export rendering must not trim a provenance-backed valid handle brand prefix back out.
- Imperial continuation handling must prefer complete same-cell / same-band continuation over truncated fragment display. Legitimate continuation fragments such as `Colour Code:`, `Vertical Grain`, `steel support`, `Bullnose edge`, `Square edge`, `anthracite`, `Part Number`, `SKU`, `Std Whiteboard internal`, and `Flat fronts, not curved` must stay with the owning row unless a stronger new-row anchor exists.
- Imperial sinkware cleanup must preserve same-room supplier, mounting, and taphole evidence after overlay selection. If source-backed parsing leaves tails such as `Sink Mounting Undermount sink`, `behind`, or `behind basin sink`, cleanup may normalize them to the same source meaning (`Sink Mounting - By Others Undermount`, `behind sink`, `behind basin`) but must not invent a sink/basin row that is absent from the source page.
- For all non-drawing table/grid-first page families, values must be read from the recovered table/grid rows first and only lightly normalized afterward. Field ordering and UI presentation must happen after extraction, not before it.
- Builder routing must stay job-scoped. If a job is assigned to `Imperial` in the app, the parser, QA, and regression expectations must follow the Imperial route even when the uploaded PDF is an Evoca-style, Simonds-style, or otherwise delegated selection sheet.
- Layout analysis must emit `page_type`, `section_label`, `room_label`, `room_blocks`, and `rows`, and later extraction stages may only read values from those matched blocks instead of scanning freely across the page.
- After shared structure extraction, every builder must pass through a builder-specific finalizer stage. The shared layer owns page classification, room/row block detection, and common noise cleanup; builder finalizers own final room-title preservation, overlay merge priority, fixture blacklist enforcement, and grouped-row/property-row cleanup.
- All builders must use the fixed `Global Conservative` profile based on the accepted `37016` output style.
- Under `Global Conservative`, heuristic room structure and row-local field ownership remain primary; parser output must not invent extra rooms, collapse distinct rooms into broad buckets, or overwrite already-clean source text with noisier guesses.
- Room rows must come from actual source headings or labels, and only the same room should merge across pages/files. Bathroom, ensuite, powder, vanity, pantry, WIP, theatre, rumpus, study, office, and similar rooms must stay separate unless the source clearly uses the same room label.
- Full source room names from authoritative colour-schedule pages must be preserved in `original_room_label`, including labels such as `WALK-IN-PANTRY` and `MEALS ROOM`.
- For Imperial jobs, room names must use the currently extractable `... JOINERY SELECTION SHEET` title body as-is, such as `WALK-BEHIND PANTRY`, `BENCH SEAT`, or `OFFICE`, without shorthand aliases or manual remapping.
- Distinct pantry spaces must stay separate when the source distinguishes them; `BUTLERS PANTRY` and `WALK-IN-PANTRY` must not be auto-merged.
- When a room-master file uses grouped room headings such as `Vanities`, grouped output should be preserved and supplement files must enrich that grouped room instead of splitting it into extra bathroom/ensuite/powder rows.
- Room material fields must remain room-local. Benchtops, door colours, handles, toe kicks, bulkheads, and splashbacks should come only from the matched room section, and supplement files must not leak another room's material text into the current room.
- Material ownership must be same-room-only, same-section-only, and same-row-or-adjacent-only: the parser may not borrow benchtop, splashback, accessory, tap, or other material text from another room, another section, or a later unrelated row.
- Across all builders, supplier, note, model, and profile text must also stay row-local. Those fragments may only be attached to the field that owns the same source row or row fragment.
- Room-local material ownership must also hold inside grouped rooms. If the authoritative room is `Vanities`, only the `VANITIES COLOUR SCHEDULE` section may define its benchtops and door colours; fixture supplements may add basin/tap/sink details only.
- Composite supplement headings such as `Kitchen/Pantry/Family/Meals` must not generate a single synthetic room; independent rooms should only be created when the authoritative room-master file contains explicit room-specific colour-schedule pages such as `MEALS ROOM COLOUR SCHEDULE`.
- Generic `DOORS/PANELS` or `Door/Panel Colour` values may fall back to `Base` only when the same room section has no explicit `Overhead Cupboards`, `Base Cupboards & Drawers`, `Island Bench Base Cupboards & Drawers`, or `Island Bar Back` group markers.
- Supplement-file room-like lines that do not belong to the room-master set, such as glazing, door-finish, waste-colour, or stray room headings, must be ignored and surfaced as warnings instead of becoming new rooms.
- Clarendon jobs must still pass through a deterministic post-polish stage, but that polish now runs per detected room instead of compressing output into a fixed 6-room layout.
- Clarendon remains `heuristic-only`; it does not use Docling in the default runtime path.
- Clarendon post-polish should prefer clean schedule-page text for benchtops, door colours, toe kicks, bulkheads, handles, sink/basin/tap fixtures, and soft-close states instead of falling back to OCR-noisy field fragments when the schedule pages already provide a cleaner source.
- Clarendon polish and address extraction must prefer `raw_text` from the source PDF whenever it is present; vision-normalized `text` is only a fallback and must not erase schedule-note fields such as `KICKBOARDS`, `BULKHEAD SHADOWLINE`, `HANDLE 1/2`, `DOOR HINGES`, or `DRAWER RUNNERS`.
- Clarendon address extraction must use page-header stop markers so `Site Address:` lines do not absorb nearby joinery body text such as `BENCHTOP`, `DOOR COLOUR`, `HANDLE`, or `THERMOLAMINATE NOTES`.
- Clarendon AFC pages such as `CARPET & MAIN FLOOR TILE` must be parsed as room-local flooring overlays. Their area labels should enrich only clearly matching master rooms like `KITCHEN`, `BUTLERS PANTRY`, `THEATRE ROOM`, and `RUMPUS ROOM` without creating synthetic AFC-only rooms or inferred `LAUNDRY` flooring from broad labels such as `WIL/Linen/s Ground Floor`.
- Clarendon fixture cleanup must preserve legitimate tap wording such as `Twin Handle Sink Mixer`; generic wet-area cleanup markers must not truncate valid tap model names just because they contain words like `Handle`.
- Appliance parsing must prefer explicit `model_no` values from labeled rows or table columns and must not use brand-only words or generic notes as model numbers.
- Appliance placeholders such as `As Above`, `By Client`, `N/A - By others`, or `N/A CLIENT TO CHECK` should keep their source wording, but placeholder-only rows should be deduplicated away when the same source file already contains a concrete model for that appliance type.
- Sink, basin, and tap selections must be captured as room-level fixture fields instead of appliance rows.
- Wet-area plumbing items that do not affect cabinetry or benchtop depth must not appear in final room fields. `Shower Mixer`, `Shower Screen`, `Shower Base`, `Shower Frame`, `Towel Rail`, `Toilet Roll Holder`, `Toilet Suite`, `Toilet`, `Floor Waste`, `Feature Waste`, `Bath`, `Bath Mixer`, `Bath Spout`, `Bath Waste`, `Shower on Rail`, `Shower Rose`, `Basin Waste`, `Bottle Trap`, and similar wet-area hooks or in-wall mixer-only rows are blacklisted from final room output.
- The only wet-area fixture exceptions that may stay in final room output are `Sink`, `Basin`, `Sink Mixer`, and `Basin Mixer`, because they affect benchtop or stone cutout/depth decisions.
- `LED` is a dedicated room field with explicit `Yes/No` output. When source wording such as `LED`, `LED LIGHTING`, `LED STRIP LIGHTING`, or `LED's As per drawings` is present, the room must output `LED = Yes` and preserve that wording in a separate `LED Note` field; pages should only render the LED row when the value is `Yes`.
- `Shelf` is a dedicated room material field, but it is restricted to simple fit-out/storage room families such as `WIP / Walk In Pantry`, `WIR / Walk In Robe`, `WIL / Walk In Linen`, `Linen Cupboard / Linen Fit Out`, and robe-fit-out style rooms. Main joinery rooms such as `Kitchen`, `Butlers Pantry`, `Laundry`, `Vanity`, `Bathroom`, `Ensuite`, `Powder`, `Bar`, `Study`, and `Rumpus` must not render `Shelf`.
- Populate `Shelf` only when the same room's source text explicitly ties a material or finish to shelf shelving, such as `Open Shelving ... White Melamine`, `Shelving Only ... White Melamine`, or `Shelves ... Polytec ...`. Do not infer it from rail-only rows, generic fit-out notes, unrelated material fields, or shelf text that belongs to another room on the page.
- `CARCASS & SHELF EDGES`, `SQUARE EDGE RAILS`, and main-room `OPEN FACED SHELVES` / cabinetry phrasing are not room-level `Shelf` evidence by themselves.
- Final room retention is now global across builders: a room survives only when it contains true joinery/material evidence in fields such as bench tops, door colours, splashback, toe kick, bulkheads, floating shelf, or `Shelf`. Handles, sink/basin/tap, flooring, LED, accessories, and other notes do not keep a room alive on their own.
- Door colour information should expose room-level splits for `Overheads`, `Base`, `Island`, and `Bar Back` whenever the source text makes those categories explicit.
- Door colour information should also expose a room-level `Tall` split when the source explicitly labels tall cabinets, tall doors, tall panels, or combined `Upper Cabinetry Colour + Tall Cabinets` rows.
- Grouped rooms such as `Vanities` must treat door-colour splits as explicit-marker-driven: `Overheads` may only appear when the authoritative room section explicitly labels overhead cabinetry; otherwise grouped door colours default to `Base`.
- Door-colour display should trim obvious installation-context suffixes and suppress OCR noise so room cards show material names instead of repeated positional phrases or unrelated kickboard/benchtop text.
- Kitchen and similar room bench-top data should split into `Wall Run Bench Top` and `Island Bench Top` when the source text clearly describes separate wall-run and island materials.
- If no explicit `Wall Run Bench Top` is present, a plain `Bench Top` or `Cooktop Run` description defaults to `Wall Run Bench Top`.
- Yellowwood-style joinery schedules must map `Back Benchtops` to `Wall Run Bench Top` and preserve `Waterfall Ends` as part of `Island Bench Top`.
- Yellowwood jobs must use `layout + row-local parser + selective Docling` on grouped schedule pages such as cabinetry, vanity, tiling, and `Area / Item / Colour / Supplier` tables.

### 4.5 Engineering Workflow
- The project should be ready to live in a GitHub repository with Codex-centered PR review.
- Imperial structural-parser work must follow the dedicated tracker flow:
  - read `IMPERIAL_GRID_TRACKER.md` before starting work,
  - pick one primary blocker per cycle,
- complete local checks -> deploy -> fresh rerun -> source-PDF review,
  - then update `IMPERIAL_GRID_TRACKER.md` before closing the cycle.
- Default collaboration flow should be:
  - stable default branch
  - short-lived feature branches
  - GitHub pull requests
  - Codex review against the PR diff
- PR descriptions should explicitly capture affected builders/jobs, key sample PDFs, rerun requirements, and whether the four root docs changed.
- The repository should include a PR template and CODEOWNERS file so parser and UI changes can be reviewed consistently.
- Day-to-day parser work should default to `fix this bug` when a live issue is already specific and PDF-grounded; `review this PR` is reserved for shared parser, finalizer, workflow, or export changes that carry cross-builder regression risk.
- For Yellowwood, final room names must preserve the more specific spec-title form, such as `BED 1 ENSUITE VANITY`, `BATHROOM VANITY`, `BED 1 WALK IN ROBE`, and `BED 2/3/4 ROBE`.
- For Yellowwood, builder-specific finalization must also preserve more detailed titles where the source provides them, including `PANTRY`, `BED 1 MASTER ENSUITE VANITY`, `GROUND FLOOR POWDER ROOM`, `UPPER-LEVEL POWDER ROOM`, `BED 2/3/4/5 ROBE FIT OUT`, and `BED 1 MASTER WALK IN ROBE FIT OUT`.
- For Yellowwood, rooms are kept only when they have real joinery/material evidence. Pure plumbing, tiling, accessory, or flooring-only rooms must be dropped, while `robe` or `media` rooms may stay only when they contain material evidence such as `Polytec` or `Laminex`.
- For Yellowwood, fake room fragments such as `WIP`, row notes, shelving-only cells, or collapsed generic labels like a single `ROBE FIT OUT` room must never survive as final room cards.
- For Yellowwood, wet-area plumbing pages may enrich the corresponding vanity room, but fixture-only parent rooms such as plain `BED 1 ENSUITE`, `BATHROOM`, `WC`, or `LAUNDRY` must not survive as standalone rooms when they have no joinery/material evidence.
- For Yellowwood vanity rooms, wet-area plumbing enrichment must stay room-relevant: only `Basin`, `Basin Mixer`, `Flooring`, and joinery/material fields may survive. Blacklisted wet-area items such as towel rails, toilet-roll holders, toilets, shower items, bath items, floor waste, basin waste, and bottle traps must be removed from final room output.
- For Yellowwood, non-wet-area `FLOORING` pages and wet-area `TILING SCHEDULE` pages must enrich the retained room cards as room-local overlays. Room flooring should land on `Kitchen`, robe rooms, and vanity rooms when the schedule area labels match, and contents-page flooring lines must never populate `others.flooring_notes`.
- For Yellowwood kitchens, builder-specific finalization must keep wall-run, island, and other benchtops separate, preserve `Overhead Cupboards`, treat `*To Bulkhead*` text as a note rather than a bulkhead material value, and repopulate kitchen `Sink` / `Tap` from the plumbing overlay when the joinery page itself is sparse.
- Imperial-style joinery selection sheets must use page-top section titles as authoritative section boundaries, keep continuation pages with the current section until the next section title, and stop extraction at footer section-break markers such as `CLIENT NAME / SIGNATURE / SIGNED DATE`, their glued variants like `CLIENT NAME: SIGNATURE: SIGNED DATE:` or `CLIENTNAMESIGNATURESIGNEDDATE`, and related footer noise such as `NOTESSUPPLIER`.
- `DOCUMENT REF` is also Imperial footer noise and must never be treated as appliance, sinkware, or material content.
- Imperial joinery/material selection sheets must be treated as table-first Excel-to-PDF layouts. Vision is enabled by default on those pages to recover the visible table grid, column boundaries, merged cells, and footer/signature isolation before deterministic row-to-field mapping runs.
- Imperial section parsing must treat obvious in-section row labels such as `ISLAND CABINETRY COLOUR`, `GPO'S`, `BIN`, `HAMPER`, `HANGING RAIL`, `MIRRORED SHAVING CABINET`, and `EXTRA TOP IN ...` as row boundaries even when they are not final business fields, so preceding benchtop, floating-shelf, handle, and cabinetry rows do not continue through them.
- Imperial OCR-glued lines must not split inside ordinary words such as `CABINETRY`; inline marker detection should only split at real row starts or glued lowercase-to-uppercase row transitions.
- Imperial `Hinges & Drawer Runners` rows must recover `Soft Close` even when OCR glues `Floor Type & Kick refacing required` into the same line or reorders the line fragments.
- Imperial room-level joinery fields must be rebuilt from the builder-specific heuristic section parser and row-local reconstruction so room cards keep same-room, same-section, same-row ownership even when OCR or layout extraction produces broader or noisier spans.
- Imperial continuation must also stop when a later page switches into non-joinery full-page headings such as `APPLIANCES` or `SINKWARE & TAPWARE`.
- Imperial-style non-room sections such as `FEATURE TALL DOORS` must be preserved separately from rooms and must never be merged into the surrounding kitchen or pantry room output.
- Imperial accessory lists must be deduplicated within the same room so repeated `Accessories` rows do not render multiple times with the same value.
- Imperial sinkware semantic parsing must ignore unrelated pre-heading basin/tub noise, keep mounting suffixes such as `UNDERMOUNT` attached to the correct sink row, and apply generic taphole notes to the correct sink cluster without cross-room leakage.
- Orientation-only notes such as `Vertical on Tall doors only` or `Horizontal on all` must not be treated as room-material values.
- Imperial fixture overlay pages should be preferred over AI guesses for sink, basin, and tap text whenever the builder-specific overlay parser can read a cleaner local value.
- Imperial fixture cleanup must preserve source-equivalent cell order for sink/basin rows: mounting text stays with the description, supplier stays after that (`Sink Mounting - Undermount - By Others`), and taphole notes stay in notes. Deterministic PDF text-layer encoding glitches may be normalized when the visual source is unambiguous, such as `MounƟng` to `Mounting`.
- Imperial room-card order must follow source spec order, and each room's `material_rows` must also render strictly in source row order. No later finalize/UI stage may re-sort Imperial rows by tag, label, or inferred semantic priority.
- Imperial `material_rows` must obey a hard content-grid boundary: row values may only come from the current row's recovered `AREA / ITEM`, `SPECS / DESCRIPTION`, `SUPPLIER`, and `NOTES` cells. Header/meta/footer/table-heading tokens such as address/client/date, ceiling/cabinetry height, bulkhead/shadowline labels, sheet titles, and table headers are pollution candidates and must not override clean cell-grid rows.
- Imperial `IMAGE` cells are geometry-only for row/edge recovery. They must not be OCRed, displayed, or used as appliance/material/summary text.
- Imperial supplier and notes ownership must be x/cell based. If a layout candidate places a supplier plus note tail in the supplier cell, the deterministic row assembler must split the recognized supplier from the note tail instead of treating the whole phrase as supplier text.
- Imperial `HANDLES` rows must preserve the original `SPECS / DESCRIPTION` wording order. Cleaning may remove footer noise, exact duplicate fragments, or duplicated supplier prefixes, but it must not over-split a handle block into artificial description/notes fragments or drop later sub-items.
- Imperial v6 `HANDLES` rows may now carry two display surfaces: flat `display_lines` for summary/fallback use and structured `display_groups` for room-card rendering. `display_groups` is HANDLES-only and is allowed only when supplier-to-spec ownership is safe enough to express as grouped UI, including the accepted equal-share case where `M` handle spec lines are split evenly across `N` supplier lines.
- Imperial `HANDLES` summary aggregation must be subitem-first. Parser output may attach internal `handle_subitems` to raw handle rows; room cards still show the source-like raw row, but `Material Summary / Handles` must read subitem `summary_text` / `text` before raw full-row text. Subitem provenance fields such as `raw_text`, `layout_value_text`, and `page_text_handle_block` are debug/evidence only and must never become summary material.
- Yellowwood cabinetry/joinery pages must now be treated as table/grid-first schedules as well, so rows such as `BENCHTOP`, `BASE CABINETRY COLOUR`, `UPPER CABINETRY COLOUR`, `ISLAND CABINETRY COLOUR`, `HANDLES`, `BIN`, `LIGHTING`, and `KICKBOARDS` are split before field mapping.
- Under the conservative merge profile, accessory lists and door-colour subgroup values should prefer clean heuristic output over noisier AI-only guesses when the AI result appears to come from another row or section.
- Joinery schedule parsing must ignore non-cabinet finish pages and exclude colours that only appear in paint, Colorbond, garage-door, entry-door, window-frame, or other non-joinery contexts.
- Drawer and hinge states must normalize to `Soft Close`, `Not Soft Close`, or blank.
- OpenAI extraction should tolerate markdown code fences or short explanatory prefixes around JSON instead of failing the whole AI pass immediately.
- Brand names must normalize to canonical casing for supported brands such as `Polytec`, `Westinghouse`, `AEG`, `Fisher & Paykel`, `Phoenix`, `Johnson Suisse`, `Parisi`, and `Everhard`.
- Benchtop text should preserve the full material wording, including thickness, edge, apron, and waterfall language, instead of shortening material descriptions aggressively.
- Official overall-size extraction should support structured product-page metadata blocks such as JSON-LD `height/width/depth` objects in addition to visible page text and official PDFs.
- Production deployment target is `https://spec.lxtransport.online/`, using the same fixed `Global Conservative` profile as local runs.
- After extraction, appliance enrichment must look up official model resources and store:
  - product page URL
  - spec sheet URL
  - user manual URL
  - overall size from official resources only
- For supported brands, official product lookup should probe deterministic brand-site model URLs before falling back to external search engines.
- If no exact official model match is found, appliance links and overall size should stay blank instead of falling back to guessed spec-text values.
- The visible appliance link in the UI and exports only needs the official product page URL; spec and manual files may still be used internally for size extraction when needed.
- Official overall size extraction must support both single-line `W x D x H` style text and labeled `51 mm (H) / 900 mm (W) / 520 mm (D)` style product-page dimensions.
- Snapshot payloads should carry a parsed `site_address` when the source documents expose a clear address line, so job-workspace and raw-spec headers can show `job no - address` without a separate manual job-address field.
- For Imperial jobs, delayed handle model lines that appear after joinery tables or near footer metadata should still be recoverable as long as they remain within the same room section, while adjacent cabinet-colour rows must never be mistaken for handle values.

### 4.5 Review
- Show reviewable data in English-only sections:
  - `Rooms`
  - `Hardware`
  - `Appliances`
  - `Others`
- Allow direct editing in the browser.
- Preserve reviewed data separately from raw machine extraction.
- The Job page should temporarily hide the Review cards until a later redesign is ready; the underlying review data model and save/export behavior may remain in the backend.

### 4.5A PDF QA User Flow Retirement
- The user-visible PDF QA workflow is removed from Job Workspace and Raw Snapshot pages.
- New `spec` parse runs no longer create a `snapshot_verifications` row automatically.
- The `snapshot_verifications` table and internal checklist builder helpers may remain for compatibility and historical data, but they are not part of the active user workflow.
- Raw snapshots remain visible immediately after parsing and do not display `Pending PDF QA`, export locks, PDF QA links, or signoff status.
- Parser-accuracy changes must still be checked against the source PDF itself, but acceptance is no longer represented by the retired in-app PDF QA flow.
- The active cross-builder parser regression matrix currently includes:
  - `Clarendon`: `job 1`, `job 23`, `job 25`
  - `Yellowwood`: `job 37`
  - `Imperial`: `job 34`, `job 35`, `job 36`, `job 38`
  - `Simonds`: `job 19`
  - `Evoca`: `job 39`

### 4.6 Export
- Export reviewed data to:
  - one Excel workbook with multiple sheets
  - one CSV file
- Include source file and page references in the exported data.
- Export the latest raw spec snapshot directly from the Spec List page as a standalone Excel workbook whenever a latest `raw_spec` snapshot exists.
- The raw Spec List Excel workbook must use the Claude-style review shape:
  - `Summary`
  - `By Section`
  - optional `Flagged` when parser warnings, row issues, review hints, or `needs_review` rows exist
  - `Material Summary` when Bench Tops, Door Colours, or Handles rows exist
- `By Section` columns are `Section / Area`, `Specs / Description`, `Supplier`, `Notes`, `Page`, and `Flag`.
- `By Section` must follow the Claude visual format: blue Arial 11 header row, green Arial 11 section header rows, Arial 10 wrapped item rows, yellow-highlighted flagged rows, frozen header row, and item rows that keep `Section` in the section header instead of flattening it into each area label.
- `Summary` must use the Claude report format with Arial 14 title, Arial 10 source line, metric labels with the light-blue fill, and row counts for Bench Tops, Door Colours, and Handles.
- `Material Summary` must use the Claude filtered-review format for Bench Tops, Door Colours, and Handles only, with columns `Category`, `Section`, `Area`, `Supplier`, `Specs / Description`, and `Notes`.
- Imperial raw Spec List Excel export must prefer `rooms[].material_rows` and preserve source room order and row order.
- When Imperial v6 snapshots carry `rooms[].v6_review_rows`, the raw Spec List Excel export must use those rows for the Claude workbook so original v6 item boundaries, row wording, notes, and metadata survive parser finalization. `material_rows` remain the parser/UI row model and the fallback for older snapshots.
- Non-Imperial raw Spec List Excel export must flatten the Raw Snapshot room fields into section rows, with appliances, special sections, others, and warnings included in `By Section`.
- Preserve Unicode content, including Chinese and special characters, in Excel and CSV outputs.
- Export official `Product`, `Spec`, and `Manual` appliance links as dedicated columns and keep them clickable in Excel.
- Formal export generation and export downloads are not blocked by PDF QA state.

### 4.7 Raw Spec List Page
- Provide a separate login-protected page for a single job that displays the raw spec snapshot as read-only lists/tables.
- The page must show:
  - `Rooms`
  - `Appliances`
  - `Others`
  - `Warnings`
  - source documents
  - `Material Summary`
- The page must use `raw_spec` only and must not switch to reviewed data.
- The page title must show `Spec List for job no - site address` when the latest parsed snapshot provides a `site_address`; if no address exists, omit the separator and address text.
- The page must provide a left-side navigation hide/show control and should load with the navigation rail hidden by default on every visit.
- The latest Raw Snapshot page must provide a clickable `Export Excel` action whenever the latest `raw_spec` snapshot exists.
- Historical run result pages must stay read-only and must not provide Excel export, so users do not accidentally export an older run instead of the latest snapshot.
- The page must not render user-visible PDF QA cards, `Pending PDF QA` warnings, PDF QA links, export disabled states, or export-lock copy.
- In 1080p half-screen windows, the Raw Spec List page must remain readable without a horizontal scroll bar; dense tables should switch to stacked cards and room fields should wrap vertically instead of forcing sideways dragging.
- In 1080p half-screen windows, the responsive layout must also remove fixed content minima and suppress page-level horizontal overflow so wrapped room cards do not still trigger a horizontal scrollbar.
- The `Rooms` section should use one wide horizontal block per room on desktop, stacked vertically one below the next, so each field can be read without cramped narrow cards.
- Non-Imperial room cards must show room fixture rows for `Sink`, `Basin`, and `Tap`.
- Non-Imperial room cards should continue to show `Door Colours` as separate `Overheads`, `Base`, `Island`, and `Bar Back` rows when those splits exist.
- Non-Imperial room cards should continue to show a `Tall` row when the source provides tall-cabinet material.
- Imperial room cards now render a `material_rows` block instead of the older split field stack. Each line uses `AREA / ITEM` as the title and displays `SUPPLIER - SPECS / DESCRIPTION - NOTES` with only light whitespace/noise cleanup.
- Imperial room-card display must prefer the most complete accepted raw-row/layout continuation text over truncated visual-subrow snippets. Truncated fragment-only display must not hide valid same-row continuation on desk/shelf/robe/study style pages.
- Imperial room cards only retain `Drawers`, `Hinges`, `Flooring`, and `Sink` beneath the raw material rows. `Tap` is intentionally omitted from Imperial room cards, Imperial material summary, and Imperial primary raw export/display.
- Each room card must support optional `Floating Shelf`, `Shelf`, `LED`, `LED Note`, `Accessories 1..n`, and curated `Others` accessory rows, and only render `Shelf` or the LED block when those values are non-empty / `LED = Yes`.
- Non-kitchen room cards must never render `Island` or `Bar Back`, and non-kitchen `Overheads` should only render when the authoritative room section explicitly provides that split.
- Each room card should prefer separate `Wall Run Bench Top` and `Island Bench Top` rows when the source text supports that split.
- Only the `Kitchen` room card should render split `Wall Run Bench Top` and `Island Bench Top` rows; other rooms should render a single `Benchtop` row even when internal split fields exist.
- Plumbing fixtures shown on room cards must not also appear in the `Appliances` table.
- The `Material Summary` block must deduplicate and count room-level `Door Colours`, `Handles`, and `Bench Tops` using smart normalization.
- `Material Summary -> Bench Tops` must preserve full material, thickness, and edge/apron/waterfall details while stripping only location suffixes such as `to cooktop run`, `to island bench`, or `to powder room 2`.
- `Material Summary -> Bench Tops` must also include floating-shelf materials when the room card captures a `Floating Shelf` material.
- `Material Summary -> Door Colours` and `Material Summary -> Handles` must preserve real `profile`, `style`, `model no.`, and handle-family descriptions; normalization may trim pure installation-location tails, but it must not collapse a value to a bare supplier name.
- For Imperial only, `Material Summary` must aggregate directly from tagged `material_rows`, not from legacy split fields. Each summary entry must render the normalized material text on the first line and `Room: ...` on the second line, where the room list is de-duplicated and ordered by source spec appearance order using ` | ` as the separator.
- Imperial `Material Summary` must exclude rows whose second-pass revalidation fails or remains in a non-handle-specific unresolved state. Handle-specific provenance fallback is allowed only when the remaining row risk is limited and the fallback stays source-ordered and source-worded.
- Imperial `Material Summary` must also reject source rows containing hard-boundary pollution tokens. Summary cleanup must not manufacture entries from notes-only or header/meta fragments such as `Bulkhead:Colourboard`.
- Imperial `Material Summary -> Door Colours` must not discard a tagged `FEATURE CABINETRY` row solely because the row also mentions `Standard Whiteboard Internals` when the same source row contains valid feature-cabinetry evidence such as shaving cabinet, mirrored doors, or colourboard shelf wording.
- Imperial `Material Summary -> Bench Tops` must remove dangling separators left after stripping WFE/cutout tails so grouped values do not render as `20mm Stone | (Room: ...)`.
- Imperial sinkware overlay must keep room-local single-word mounting lines such as `Undermount`, complete split taphole notes such as `behind` + `sink`, and prefer a fuller source-backed sink/basin candidate when it adds missing supplier or mounting evidence. Product names containing `Undermount Sink` remain product text and must not be demoted to notes.
- Imperial raw-row review/order diagnostics are backend-only by default. The parser and summary pipeline may still use them, but the standard raw Spec List frontend must not display `Order hint`, `Review`, `Issues`, `Repairs`, `Pending`, or `Revalidation` banners unless explicitly re-enabled for debugging.
- Grouped vanity titles such as `VANITIES` must sort into the same room-priority band as `Bathroom / Ensuite / Powder`, ahead of robe, rumpus, and generic rooms.
- Appliance rows on the page must expose a clickable official `Product` link and allow long URLs to wrap across multiple lines.
- The page must also render a `Special Sections` area for non-room joinery sections such as `FEATURE TALL DOORS`.
- The page must show `Extraction duration` in `Snapshot Summary`.
- Shared UI pages should render at a tighter visual density, roughly 75% of the earlier default, by shrinking application-level fonts and spacing in CSS rather than relying on browser zoom.

### 4.8 Upload UX
- Job detail uploads should start automatically as soon as files are selected.
- Separate `Upload Specs` and `Upload Drawings` submit buttons should not be required.
- The file list should update after upload and existing files must remain visible if an upload fails.

### 4.9 Run History
- The Job page run history must refresh automatically while parsing is active.
- Run messages should show real worker progress such as loading files, heuristic extraction, Clarendon polish, Docling structure, official resource lookup, and snapshot save.
- Clarendon runs should expose a dedicated `Clarendon polish` stage in Run History when the deterministic post-polish step executes.
- Run stages should also show official resource work such as model lookup, spec/manual discovery, and official size extraction.
- Run metadata must also show actual parse `Duration`, `Worker / Build`, and an `Open Result` action for succeeded spec runs.
- Only one worker should actively lease the queue at a time, so stale local processes cannot race newer code on queued jobs.

### 4.10 Drawing Foundation
- Upload drawing PDFs from the job page.
- Parse drawing-side summary blocks into the same canonical schema.
- Save compare-ready data, but do not expose a formal comparison UI in v1.

### 4.11 Security
- Require login for all working pages.
- Use CSRF protection for forms.
- Keep session cookies scoped to the app domain.
- Support secure HTTPS-only cookies in production through environment configuration.
- Keep secrets in environment variables or config files outside source control.
- Production uploads must support a consistent 100 MB limit through both Nginx and the FastAPI validation layer.
- Automated test runs must use isolated temporary app data and must never modify the live local job database.

### 4.12 Frontend Delivery
- Static CSS assets should include cache-busting so layout changes become visible immediately after restart or deploy.
- All frontend timestamps must display in Brisbane time using the fixed format `YYYY-MM-DD HH:mm AEST`.
- The Jobs homepage keeps its navigation rail visible, while the Job Workspace and Raw Spec List pages default that rail to hidden and allow the user to toggle it open when needed.
- The Jobs page `Open` action must continue to open in a new browser tab.
- The Job Workspace title must show `job no - site address` when the latest parsed raw or drawing snapshot exposes a `site_address`.
- At roughly `1280px` width and below, main pages should switch from wide tables to stacked card-style layouts so a 1080p half-screen browser can be read without horizontal scrolling.
- Job diagnostics should expose structure-analysis metadata, including whether layout analysis ran, whether it succeeded, which pages were analyzed, which pages escalated to heavy vision, and a short layout note.

### 4.13 Git Rollback Tooling
- Provide local Git helper scripts to initialize, checkpoint, inspect history, and restore from previous commits.
- Require synchronized doc updates for major changes.

### 4.14 Online-First Delivery Workflow
- `https://spec.lxtransport.online/` is the only formal running environment.
- Confirmed implementation work is only complete after:
  - local checks pass,
  - the latest code is deployed to production,
  - production web and worker services restart successfully,
  - the affected live page or job is verified.
- Parsing changes must be validated through a fresh online parse run for the affected job, not by inspecting an older snapshot.
- Parser-accuracy changes must also be checked against the source PDF itself; older webpages or older snapshots are reference material only and are not acceptance criteria.
- The repo should provide a repeatable local deployment helper so production updates do not rely on ad hoc terminal commands.

### 4.15 Imperial V6 Parser Path
- Imperial spec PDFs may be parsed through an alternative cell-aware extractor, `App/services/pdf_to_structured_json.py`, that reads the visible table grid directly instead of relying on Docling plus Heavy Vision. This path is gated by the `USE_V6_IMPERIAL` environment feature flag.
- The v6 path is a temporary transition tool. It exists to validate the replacement of the legacy Imperial Docling/Heavy Vision extraction pipeline with a faster deterministic cell-aware extractor. The flag and the legacy Imperial branch are expected to be removed together when the legacy code is deleted; the flag is not intended as a long-term runtime switch.
- The v6 path applies to Imperial jobs only. Yellowwood, Simonds, Evoca, and Clarendon are out of scope for v6 and continue to use their existing extraction pipelines regardless of the flag.
- When `USE_V6_IMPERIAL=1`, the website builder is `Imperial`, and at least one uploaded spec file has a resolvable path, `build_spec_snapshot` runs the v6 fast path before the legacy layout pipeline. The fast path skips Docling, Heavy Vision, OpenAI merge, builder-specific polish, the Imperial row-polish rebuild, and the raw-spec crosscheck, because the v6 extractor owns the grid-to-row mapping directly.
- When `USE_V6_IMPERIAL` is unset or `0`, the Imperial runtime falls back to the existing Phase 3B pipeline with no behavior change. The flag must not alter non-Imperial builders in either state.
- The v6 path records `parser_strategy = "imperial_v6"` on the run and on the snapshot `analysis` block. Snapshot diagnostics (`layout_attempted`, `docling_attempted`, `vision_attempted`, `layout_provider`, `layout_mode`, `layout_note`, `docling_note`, `vision_note`) are set to reflect that no legacy layout/Docling/Heavy Vision work was performed on that run.
- Fallback behavior: if the v6 extractor or `parse_documents` raises, or if the produced snapshot contains no material rows with v6 provenance (`source_provider = "v6"` or `source_extractor = "pdf_to_structured_json_v6"`), the fast path emits an `imperial_v6_fallback` progress warning and returns to the legacy Imperial pipeline. The run must not fail solely because v6 failed.
- The v6 path preserves Imperial source-row truth: material rows surfaced to room cards and `Material Summary` keep v6 provenance (`source_provider = "v6"`, `source_extractor = "pdf_to_structured_json_v6"`, original `_source` page and row hints), so downstream regression coverage can verify Imperial rows actually came from v6 and not from the legacy path.
- The v6 path is designed to preserve the existing Imperial Raw Spec List page rendering rules: `material_rows` remain the primary truth layer, retained footer fields are `Drawers`, `Hinges`, `Flooring`, and `Sink`, `Tap` is intentionally still excluded from Imperial room cards and Imperial primary raw export/display, and `Material Summary` continues to aggregate from tagged `material_rows`.
- Imperial v6 raw snapshots use the same direct Raw Snapshot Excel export path as other snapshots. Formal exports and Raw Snapshot Excel downloads are not blocked by PDF QA state.
- Imperial raw Spec List room cards should prefer grouped handle rendering when `display_groups` is present: render supplier once as a group header and render the group's lines beneath it. When `display_groups` is absent, fall back to the existing flat `display_lines` rendering.
- Imperial raw Spec List room-card `material_rows` must also render row-local `notes` without changing summary behavior: append notes inline only when the visible payload is one line and the notes string contains no `\n`; otherwise render notes as a separate trailing `(notes)` line. That separate notes line uses CSS class `.row-note-multiline` with `white-space: pre-line` so source-backed line breaks survive on the room card. Empty notes render nothing, and this rule does not modify `Material Summary`.
- Imperial synth-handle room-card display must keep the existing row-level `notes` field but must not inline those notes into the helper-produced display line. For `synthesized_from_room_handles` rows, the helper emits supplier/description only and the room-card notes layer renders the note once. This prevents `... - (...) - (...)` double-rendering without changing Material Summary or Excel source rows.
- Imperial v6 raw Spec List room cards now also fall back to `rooms[].v6_review_rows[*].notes` when the flattened `material_rows[*].notes` field is empty after parser finalization, mirroring the existing Excel export behavior. Match is conservative-degrading: strict `(area, page, row_order)` -> `(area, page)` -> `area` only, returning no fallback on ambiguity.
- Imperial v6 handle backfill inside `_imperial_reconcile_material_rows_with_room_fields(...)` must be idempotent across repeated builder-finalizer passes: if an existing `material_rows` entry already has `provenance.synthesized_from_room_handles = True` and the same normalized `provenance.layout_value_text`, the reconcile emit gate must skip re-emitting that synth row. This bypasses the known coarse-vs-fine handle identity mismatch at the emit gate only; `_imperial_handle_reconcile_identity(...)` itself remains unchanged.
- Imperial raw Spec List `Material Summary -> Handles` should also prefer grouped `display_groups` when they are present on v6 handle rows. Each grouped entry renders one supplier header with indented lines beneath it, counts as one distinct item, and deduplicates across rooms only when both supplier and grouped lines match exactly. When `display_groups` is absent, fall back to the existing flat handle-summary candidate path. `Door Colours` and `Bench Tops` remain unchanged.
- Imperial raw Spec List rendering must drop same-source v6 `HANDLES` subset rows before room-card rendering and before `Material Summary -> Handles` aggregation. The canonical row is the longest same-room `HANDLES` line set; synthetic subset-only rows such as `synthesized_from_room_handles` backfill rows must not render beside the grouped source row.

### 4.16 Imperial V6 Known Limitations
These limitations have been observed during v6 production validation. They are listed here to set reader expectations and are tracked as a deferred backlog rather than release blockers.
- The v6 snapshot can still carry synthetic subset-only `HANDLES` backfill rows beside the canonical grouped source row. Raw storage remains unchanged for now, but Spec List room cards and `Material Summary -> Handles` must deduplicate those same-source subset rows before display. The underlying emitter remains deferred as a separate extractor/postprocess backlog item.
- Repeated builder-finalizer passes can exercise the same Imperial v6 handle backfill logic more than once. **FIXED** in commit `9578fd7`: `_imperial_reconcile_material_rows_with_room_fields(...)` now skips re-emitting a synth handle row when the room already contains a `synthesized_from_room_handles` row with the same normalized `layout_value_text`. The deeper coarse-vs-fine identity mismatch remains deferred.
- Imperial synth-handle room-card rows could double-render the same note text as `Overheads: - (...) - (...)` because the synth helper line already embedded notes before the room-card notes layer ran. **FIXED** in commit `23e233a`: synth handle helper lines now omit inline notes and leave the row-level `notes` field for the room-card notes layer.
- `Handles` material summary trailing separator: composed handle summary text can end in a dangling ` - and -` fragment when the source description joins multiple orientation phrases. Summary content remains correct but cosmetically untidy.
- `FLOORING` field may absorb PDF header text: in rooms where the source table does not provide a `FLOORING` row, the v6 extractor can occasionally populate `flooring` with the column-header text `AREA / ITEM SPECS / DESCRIPTION IMAGE SUPPLIER NOTES`. The intended meaning is "no flooring specified" and the header-text value is not a real flooring selection. **FIXED** in commit `82cc920`: downstream deny-list in `_populate_flooring` rejects pure-header-token values.
- Merged adjacent `area` values: the cell-aware extractor can merge visually adjacent row labels into a single `area_or_item` string, for example `BENCHTOP ISLAND CABINETRY COLOUR` or `BIN ACCESSORIES LED'S`. The underlying supplier/description/notes cells are preserved and remain linked to the merged row. This is a cell-grid recovery limit in the v6 extractor itself rather than a downstream cleanup bug. Users should cross-reference the source PDF when the `area_or_item` label visually combines multiple row labels. Investigated 2026-04-25, see `docs/BUG_H_RECON_REPORT.md` (commit `ae37358`): on real prod job 73 KITCHEN, the merged rows are shape (b) — the value side (`specs_or_description` / `supplier`) is also merged into a single blob (e.g. `KICKBOARDS\nGPO'S` value reads `AS DOORS Island Drawer GPO 1 ... GPO 2 ...` with no clean per-row segmentation), so a main repo adapter split is **not** safely possible. Held as known limitation per project rule (memory `project_claude_spec_extraction.md`) that PDF-geometry limits belong in the downstream rules layer, but no safe downstream split exists either; reopen if factory feedback warrants Path A or Path C as documented in the recon report.
- Duplicated `notes` wording on selected rows: some rows (observed on TV-upper style open cabinetry) can carry the same note text twice, typically as `Note: X - (Note: X)`. The underlying material/supplier is correct; only the notes wording is duplicated.
- Room model layer drops distinct area entries: in 37330 KITCHEN, the downstream room model layer (b) drops the area `UPPER CABINETRY COLOUR` while preserving the similarly-named `UPPER CABINETRY COLOUR (GLASS DOORS ONLY)`, suggesting the two are folded into a single entry. This is data loss rather than a merge, lives in the post-processing layer (`extraction_service.py` + `imperial_v6_room_fields.py`), and is queued for dedicated investigation after Step 4d. See `Project_state.md` "Bug 6 detailed observation" for full context.

## 5. Canonical Data Requirements

### 5.0 Snapshot Metadata
- `analysis.mode`
- `analysis.parser_strategy`
- `analysis.openai_attempted`
- `analysis.openai_succeeded`
- `analysis.openai_model`
- `analysis.note`
- `analysis.worker_pid`
- `analysis.app_build_id`
- `analysis.vision_attempted`
- `analysis.vision_succeeded`
- `analysis.vision_pages`
- `analysis.vision_page_count`
- `analysis.vision_note`
- `site_address`
- `pdf_qa_status`

### 5.1 Room Fields
- `room_key`
- `original_room_label`
- `bench_tops`
- `door_panel_colours[]`
- `door_colours_tall`
- `floating_shelf`
- `led`
- `accessories`
- `other_items`
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
- `product_url`
- `spec_url`
- `manual_url`
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
- User can open a dedicated single-job raw Spec List page and export it to Excel.
- User can tell from the Job page which runtime strategy produced the latest extraction, including heuristic-only or selective Docling.
- User can tell from the Job page which global extraction profile, worker PID, and build ID generated the latest snapshot.
- Clarendon jobs use an additional deterministic post-polish step so repeated parses keep source-driven room ownership from the `Drawings and Colours` master while stripping handle-location noise, fixture line breaks, and noisy field spillover.
- Clarendon jobs support both the original `37016` schedule family and the denser single-line `handleless / mirror splashback / laminate` family, with the same compact-summary output style.
- Imperial jobs use title-driven section parsing so kitchen, pantry, laundry, bar, bath/ensuite, and other selection-sheet sections stay isolated, footer/signature blocks are ignored, and `FEATURE TALL DOORS` is shown separately from room cards.
- Imperial jobs now use cell-aware raw material rows as the primary joinery/material output, constrained self-repair to catch row-order and column-spill bugs before persistence, and a dedicated `37867` source-PDF gold fixture as the highest-priority regression gate for room order, row order, handle preservation, and summary accuracy.
- Imperial jobs now also rely on explicit second-pass issue detection and revalidation to decide whether repaired rows may participate in `Material Summary`, while frontend diagnostics remain hidden by default and live acceptance is anchored by `job 60 / run 2037` for the continuation-heavy desk/shelf family.
- Imperial structural parser work now uses `IMPERIAL_GRID_TRACKER.md` as the durable execution tracker. That file is the authoritative place for locked decisions, staged grid/row/semantic phases, the live Imperial regression matrix, open blockers, and the next acceptance target.
- Imperial structural work now treats `grid boundary recovery` as the first truth layer. If `AREA / ITEM` and `SPECS / DESCRIPTION` bleed together, that is a structure-layer defect and must be fixed before relying on downstream cleanup, summary normalization, or source-PDF acceptance review.
- For Imperial room cards, `AREA / ITEM` must prefer the original table label text from the source cell. Internal normalization may still support tags and constrained repair, but the UI must not synthesize a different title unless the original label is actually missing.
- Imperial hard-boundary work now rejects header/meta/table-heading contamination before summary generation and treats `IMAGE` column text as non-content. `job 67` is the current acceptance target for first-row header bleed, supplier/notes split, row-first appliances, and sinkware/appliance overlay tightening.
- Imperial Phase 1B now adds separator-aware row-band coalescing before cell extraction so `BENCHTOP / WFE x 1` and long labels such as `UPPER CABINETRY COLOUR INCLUDING TALL OPEN SHELVING` can stay in one raw row when the separator evidence is soft.
- Imperial Phase 2A now adds constrained leading-fragment ownership for accessory rows, starting with `GPO -> ACCESSORIES`, so row assembly can preserve the source `AREA / ITEM` label while retaining the visually preceding value fragment.
- Imperial Phase 2A debug overlays now show the post-repair parser view in `grid_rows` and keep pre-repair rows only in `unrepaired_grid_rows`, so live debugging does not mistake intermediate row fragments for final parser output.
- Imperial Phase 2A postprocess now protects accepted `GPO -> ACCESSORIES` repairs from older accessory trimming rules, so a fixed grid/row assembler cannot be silently rolled back before persistence.
- Imperial Phase 2A postprocess now also repairs handle labels polluted by adjacent value fragments, preventing `Momo HANDLES oval` style labels from creating false handle summary families or absorbing hanging-rail text; the same repair can recover valid brand prefixes from provenance even when the visible row label was already cleaned to `HANDLES`.
- Imperial Phase 3A handle summary is semantic-subitem-first. It must preserve distinct `No handles`, `Touch catch`, `finger space`, PM2817, HT576, Voda, knob, and PTO handle families, canonicalize short/full identity variants, and reject non-handle absorbed-inline material such as timber finish text or `Casters`.
- Imperial Phase 3B sinkware/appliance overlay rules are source-row-first. Sinkware taphole backfill may only cross rooms when the sink fixture base is equivalent, or when multiple same-base generic sink rows clearly share one sink-oriented note; it must not copy a kitchen/pantry taphole onto a different laundry fixture, and utility rooms must not keep sink-derived pseudo-`basin_info`. Appliance layout rows must strip image-column and `N / A - By others` placeholder contamination before model extraction while still retaining source-backed `Specs - TBC` placeholder rows.
- Imperial Phase 3B sinkware/basin note backfill is fixture-signature constrained. Notes such as `Note: Urbane Brass Taps` may be shared only across matching sink/basin fixture bases, supplier order must render as mounting/product first and supplier second, and `Taphole location` should render before the matching note when that is the visual source order.
- The completion workflow for confirmed changes includes deployment to `spec.lxtransport.online`, successful service restarts, and live verification on the affected page or job.
- SQLite persists Builders, Jobs, files, run history, raw results, and reviewed results.
- Worker can process queued spec and drawing runs separately from the web process.
- Repeated parses should be traceable through recorded parser strategy, worker PID, and app build metadata.
- The app boots locally with documented commands.
- Git helper scripts work inside the project folder.

## 8. Implemented V1 Surface
- Pages:
  - `/login`
- `/builders`
- `/jobs`
- `/jobs?q=<job_no_fragment>`
- `/jobs/{job_id}`
- `/jobs/{job_id}/spec-list`
- Actions:
  - create builder
  - upload and delete template files
  - create job
  - upload and delete spec files
  - upload and delete drawing files
  - parse spec files
  - parse drawing files
  - save reviewed result
  - generate Excel and CSV exports
  - export raw spec list Excel
- Diagnostics:
  - `/api/health`

## 9. Document Sync Rule
Any major change must update:
- `PRD.md`
- `Arch.md`
- `Project_state.md`
