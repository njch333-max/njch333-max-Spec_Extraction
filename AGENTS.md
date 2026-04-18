# Spec_Extraction Agent Rules

## Project Goal
This project delivers an English-only Builder Spec extraction web app with:
- Builder template management
- Job management
- Spec upload and extraction
- Review and editing pages
- Excel and CSV export
- Production drawing upload and compare-ready parsing
- Local Git tooling for safe rollback and history review

## Mandatory Document Sync
For every major change, these files must be updated together:
- `PRD.md`
- `Arch.md`
- `Project_state.md`
- `AGENTS.md`

For Imperial structural parser work, also update `IMPERIAL_GRID_TRACKER.md` as the durable execution tracker.

If a change affects user-visible behavior, architecture, storage, deployment, workflow, extraction logic, or Git tooling, treat it as a major change.

## Source Of Truth
- Application code lives under `App/`
- HTML templates live under `App/templates/`
- Static assets live under `App/static/`
- Deployment scripts live under `App/scripts/`
- Git helper tools live under `tools/`
- Project docs live at the project root

## Working Rules
1. Keep the web UI in English only.
2. Preserve the canonical extraction schema unless the docs are updated together.
3. Keep uploads, exports, and database paths configurable through environment variables.
4. Do not hardcode production passwords, domains, or OpenAI keys into source files.
5. Before major commits, update `PRD.md`, `Arch.md`, `Project_state.md`, and `AGENTS.md`.
6. Prefer the local Git helper scripts instead of ad hoc Git commands when creating checkpoints or reviewing history.
7. `spec.lxtransport.online` is the default live environment. After a confirmed implementation change, deploy to production unless the user explicitly says not to.
8. Treat a task as complete only after local checks pass, production services are restarted successfully, and the affected live page or job is verified.
9. For parser-accuracy work, the source PDF is the acceptance source of truth. Do not sign off based only on older webpages or older snapshots.
9a. Builder routing, QA scope, and regression ownership must come from the website job's assigned Builder record, not from PDF header text such as `Client`, `Builder`, logos, or sheet styling. If the app classifies a job as `Imperial`, treat it as Imperial even when the uploaded PDF visually resembles another builder's delegated selection sheet.
10. When a builder-specific polish path has access to both `raw_text` and vision-normalized `text`, prefer `raw_text` for field recovery and use normalized `text` only as a fallback.
11. Treat spec parsing as structure-first and row-local work: `layout_rows -> row-fragment -> row-local mapping` is the default field path, and supplier, note, model, or profile text must not bleed across rows.
12. Keep field ownership same-room-only, same-section-only, and same-row-or-row-fragment-only. Do not borrow supplier, note, or model text across adjacent rows.
13. Treat the shared parser as a structure layer, not the final business-output layer. Major builder logic belongs in an explicit builder finalizer stage that owns final room-title preservation, overlay merge priority, fixture blacklists, and grouped-row/property-row cleanup.
14. Default runtime tool policy is speed-first:
    - `Clarendon`: heuristic-only
    - `Imperial / Simonds / Evoca / Yellowwood`: layout + row-local parser + selective Docling on difficult schedule/table pages
    - default automatic `Heavy Vision`: off, except Imperial joinery/material selection sheets where Vision is on by default as a table/grid boundary layer
    - default automatic `AI merge`: off
14a. Builder × page-family extraction matrix is now explicit:
    - `Imperial`
      - `joinery/material/colour schedule`: Vision-grid-first, then Docling, then `pdfplumber`, then heuristic text-grid
      - `sinkware & tapware`: table/grid-first with Docling/`pdfplumber`/deterministic row parsing; Vision is not default
      - `appliances`: deterministic row parser first, with table/grid support when page structure is available; Vision is not default
    - `Yellowwood`
      - cabinetry/joinery, vanity, flooring, and tiling schedules: table/grid-first via Docling/`pdfplumber`/heuristic text-grid
      - sinkware/appliances: deterministic row parser first, still source-text driven
    - `Simonds`
      - grouped-row/property-row schedules: table/grid-first so `Manufacturer / Finish / Profile / Colour / Model / Supplier` are treated as columns before mapping
    - `Evoca`
      - finishes/flooring/plumbing/appliance schedules: table/grid-first, then room-local mapping
    - `Clarendon`
      - `Drawings and Colours / Colour Schedule`: heuristic-grid-first, not Vision-first
      - AFC/supplement `sinkware / appliances / flooring`: table/grid-first without default Vision
      - drawing pages remain heuristic-only
15. Clarendon room names must come only from the `Drawings and Colours` room-master file when that file exists. AFC/supplement files may enrich existing rooms only and may not create new room names.
16. Clarendon AFC flooring pages such as `CARPET & MAIN FLOOR TILE` must enrich existing room-master rooms only. Room-specific flooring should land on the relevant room cards, not in `others.flooring_notes`, and broad AFC labels such as `WIL/Linen/s Ground Floor` must not be inferred back into `LAUNDRY`.
17. Yellowwood room names must prefer the concrete joinery/spec title, and rooms without joinery/material evidence must be dropped. `robe` and `media` rooms stay only when they contain real material evidence such as `Polytec` or `Laminex`.
18. For Yellowwood, preserve concrete titles such as `PANTRY`, `BED 1 MASTER ENSUITE VANITY`, `GROUND FLOOR POWDER ROOM`, `UPPER-LEVEL POWDER ROOM`, `BED 1 MASTER WALK IN ROBE FIT OUT`, and `BED 2/3/4/5 ROBE FIT OUT`; suppress fake room fragments such as `WIP`, cell text, row notes, and collapsed generic `ROBE FIT OUT` labels.
19. Yellowwood flooring and tiling schedule pages must enrich retained rooms such as `Kitchen`, robe rooms, and vanity rooms without creating new plumbing-only rooms; contents-page flooring text must never populate `others.flooring_notes`.
20. Keep `colour/material` values and appliance placeholders close to source wording with light cleanup only. Placeholder appliance rows such as `As Above`, `By Client`, or `N/A CLIENT TO CHECK` may be deduplicated only when the same source already contains a concrete model for that appliance type.
21. Wet-area plumbing rows that are not joinery/cabinet related must be blacklisted from final room output across builders. This includes shower, bath, toilet, towel-rail, towel-hook, floor-waste, feature-waste, shower-base/frame, basin-waste, bottle-trap, and in-wall-mixer-only items. The only fixture exceptions that may survive are `Sink`, `Basin`, `Sink Mixer`, and `Basin Mixer`.
22. Yellowwood vanity plumbing enrichment must stay room-relevant: only `Basin`, `Basin Mixer`, room-local flooring, and joinery/material fields may survive on final vanity room cards.
23. `LED` is a first-class room field. Store it internally as explicit `Yes/No`, keep matched source wording in a separate `LED Note`, and only render the LED block on user-facing pages when `LED = Yes`.
24. `Shelf` is a first-class conditional material field. Populate it only when the same room's source text explicitly assigns a material or finish to shelf shelving; never infer it from rail-only rows, generic fit-out notes, or nearby room content, and do not render it when blank.
25. `Shelf` belongs only to simple fit-out/storage room families such as `WIP`, `WIR`, `WIL`, `Linen Cupboard/Fit Out`, and robe-fit-out rooms. A plain `PANTRY` may keep `Shelf` only when that same room's local evidence clearly shows walk-in/open-shelving fit-out wording such as `WIP`, `Open Shelving`, or `Shelving Only`. Main rooms such as kitchens, butlers pantries, laundries, vanities/bathrooms, bars, studies, and rumpus rooms must not keep `Shelf`, even if they mention `CARCASS & SHELF EDGES`, `SQUARE EDGE RAILS`, or `OPEN FACED SHELVES`.
26. Final room retention is global across builders: a room survives only when it has true joinery/material evidence such as bench tops, door colours, splashback, toe kick, bulkheads, floating shelf, or `Shelf`. Handles, plumbing fixtures, flooring, LED, accessories, and other notes do not keep a room alive.
26. Fixture cleanup must not over-trim legitimate product wording. In particular, Clarendon tap values containing phrases like `Twin Handle Sink Mixer` must survive intact, and Imperial sinkware notes such as `UNDERMOUNT` or generic taphole hints must stay attached only to the correct same-room sink row.
27. For Imperial, `CLIENT NAME`, `SIGNATURE`, and `SIGNED DATE` are section-break footer markers, not content. Treat glued forms such as `CLIENT NAME: SIGNATURE: SIGNED DATE:`, `CLIENTNAMESIGNATURESIGNEDDATE`, and related footer noise such as `NOTESSUPPLIER` as extraction-stop markers too.
28. For Imperial, `DOCUMENT REF` is also footer noise. Imperial joinery/material pages must be treated as Excel-to-PDF tables first: Vision provides the visible grid, merged-cell, and footer boundary layer, and deterministic row-to-field mapping runs after that.
29. For Imperial structure work, treat `grid boundary recovery` as the first truth layer. If `AREA / ITEM` is contaminated by `SPECS / DESCRIPTION`, or merged-cell continuation crosses row boundaries, fix separator / row-assembly logic before relying on summary cleanup or PDF QA.
30. For Imperial room-card display, `AREA / ITEM` should prefer the original table label text when that cell text exists. Internal normalization may still support tags, matching, and constrained repair, but do not invent a replacement display title unless the original label is actually missing.
28a. For Imperial joinery/material, the primary truth layer is now `material_rows`, not the legacy split door-colour/bench-top fields. Persist source-order rows with `AREA / ITEM` as the title and lightly cleaned `SUPPLIER / SPECS / DESCRIPTION / NOTES` as the value payload.
28b. For Imperial, room order must follow source spec order and each room's `material_rows` must follow source row order. Do not re-sort Imperial rows later by tag, label, or inferred semantic priority.
28c. For Imperial `HANDLES`, prefer original source wording over pretty splitting. Remove footer noise, exact duplicate fragments, and duplicated supplier prefixes only; do not over-split a handle block into artificial description/notes fragments or drop later handle sub-items.
28d. For Imperial, run constrained self-repair after cell-aware row reconstruction. It may repair row order, missing label cells, column spillover, room ownership, and summary tags using provenance, but it must not freely rewrite whole-room JSON. Low-confidence repairs become `needs_review` instead of formal values.
28e. Imperial `Material Summary` must aggregate only from tagged `material_rows` and render `Door Colours`, `Handles`, and `Bench Tops` grouped by normalized material text with a de-duplicated `Room: ...` list in source order.
28e-1. Imperial handle summary is subitem-first. If a handle row has `handle_subitems`, summary and PDF-QA source matching must use only subitem `summary_text` / `text`; subitem `raw_text`, `layout_value_text`, `page_text_handle_block`, or other provenance strings are evidence for repair/debug only and must not become summary material.
28e-2. Imperial handle summary must apply a handle-identity gate before grouping. Keep distinct `No handles`, `Touch catch`, `finger space`, `PTO`, knob, and pull-handle families; canonicalize short/full PM2817, HT576, and Voda variants; reject non-handle absorbed-inline material such as timber finishes or `Casters` even if it came from handle-adjacent provenance.
28f. Imperial primary room cards and primary PDF QA keep only `Drawers`, `Hinges`, `Flooring`, and `Sink` beneath the raw material rows. `Tap` is intentionally excluded from Imperial primary display, primary summary, and primary QA signoff.
28g. `tests/fixtures/imperial_37867_gold.json` is the highest-priority Imperial regression fixture. Structural or UI changes affecting Imperial raw rows, summary grouping, row order, or handle preservation must pass that fixture before broader Imperial reruns.
28h. Imperial parser-side review is now explicit: keep `FieldIssue`, `RepairCandidate`, `RepairVerdict`, `repair_log`, and `revalidation_status` data in backend payloads so second-pass repair remains observable and testable.
28i. Imperial summary gating must respect `revalidation_status`. Rows that fail or remain unresolved for non-handle-specific reasons must not contribute to `Door Colours / Handles / Bench Tops`.
28j. Imperial raw Spec List pages must hide parser review/order diagnostics by default. Backend diagnostics remain available for analysis, but user-facing `spec-list` should not render `Order hint`, `Review`, `Issues`, `Repairs`, `Pending`, or `Revalidation` banners unless a dedicated debug mode is added later.
28k. For Imperial continuation-heavy rows, prefer complete accepted raw/layout continuation over truncated visual-subrow snippets. Do not let fragment-only display paths hide valid same-row continuation on desk, shelf, robe, or study style pages.
28l. For Imperial hard-boundary work, row values must come from the recovered content-grid cells, not page header/meta/footer text, image OCR, or whole-page fallback text. `IMAGE` cells are geometry-only, supplier/notes must be split by cell ownership, and summary aggregation must reject header/meta polluted rows.
28m. For Imperial grid-truth work, produce and preserve inspectable geometry evidence before adding downstream cleanup. Use page-structure bboxes, cell ownership, separator segment source/confidence, and `tools/imperial_grid_debug.py` JSON/SVG overlays under `tmp/imperial_grid_debug/` to diagnose boundary errors.
28n. For Imperial row-band coalescing, `visible` and `inferred_high` separators are hard boundaries. Only `none` or `inferred_low` boundaries may be merged, and only when the band is same-cell continuation or label continuation; supplier-only preludes must remain available for the next row instead of being swallowed by the previous complete row.
28o. For Imperial `AREA / ITEM` anchored row assembly, weak-boundary leading fragments may be reassigned to the following source label only under constrained evidence. Example: `GPO` with power-point/socket wording can feed the next `ACCESSORIES` row, but this must not cross a `visible` or `inferred_high` separator.
28o-1. For Imperial boundary-straddling size prefixes, keep the source `AREA / ITEM` label clean. If recovered geometry shows a size token such as `450mm` belongs to the value side while `BIN` is the source label, move the size token into `SPECS / DESCRIPTION` instead of displaying `450mm BIN` as the row title. The same repair must survive visual-fragment display and PDF-QA checklist generation.
28o-2. For Imperial supplier-cell ownership, if `supplier` is empty but cell-aware provenance has a clean supplier cell such as `By Imperial`, backfill the supplier from that cell. Do not drop `By Imperial` from room-card raw rows just because handle summary later removes suppliers.
28o-3. Imperial summary gates must not drop a valid tagged `FEATURE CABINETRY` door-colour row solely because the same source row mentions `Standard Whiteboard Internals`. Exclude true internals/robe noise, but keep source-backed feature cabinetry such as shaving cabinet / mirrored-door / colourboard-shelf rows. Bench-top summary cleanup must also remove dangling separators left after stripping WFE/cutout tails.
28o-4. Imperial sinkware overlay must keep single-word mounting lines such as `Undermount` attached to the same room cluster and must prefer a source-backed candidate that contains both supplier (`By Others`) and mounting evidence over a shorter candidate that only has the taphole note. Do not apply this rule to product names containing `Undermount Sink`. Cleanup may normalize deterministic tails such as `Sink Mounting Undermount sink`, `behind`, and `behind basin sink` to the source-equivalent same-room wording, but it must not invent absent sink/basin rows.
28p. For Imperial debug overlays, `grid_rows` must represent the repaired parser view used downstream. Keep pre-repair five-column rows only as `unrepaired_grid_rows` so debug artifacts do not get mistaken for final extraction output.
28q. For Imperial postprocess, accepted leading-fragment repairs must survive later cleanup. If provenance records `leading_fragment_repair = gpo_to_accessories` or accepted `merged_gpo_spillover`, do not trim the recovered `GPO` prefix out of `ACCESSORIES`.
28r. For Imperial handle postprocess and display/checklist rendering, repair value text that leaked into `AREA / ITEM` before display or summary. A contaminated label such as `Momo HANDLES oval` should become `HANDLES`, valid handle-brand prefix text may move into the value, and visible-separated non-handle text must not enter handle summary. If the final row label is already `HANDLES`, still inspect row/cell provenance for valid same-cell brand prefixes before persisting or rendering the value, and do not let later visual-subrow cleanup trim that accepted prefix back out.
29. All new `spec` parse runs for all builders must enter field-level PDF QA automatically. Raw results may be viewed before signoff, but they are not formally accepted until PDF QA passes.
30. Formal spec exports are locked behind PDF QA. Do not treat a raw spec snapshot as complete, export-ready, or fixed until the current raw snapshot verification is `passed`.
31. Parser-accuracy work is only complete after the affected live rerun passes PDF QA against the source PDF page-by-page. Older webpages and older snapshots are reference material only.
31a. `Field-level PDF QA signoff` means each checklist item is checked against the source PDF itself. A populated `extracted_value` is not enough for `pass`.
31b. Bulk `pass/na` write-backs based only on non-empty extracted values are prohibited as final QA. They may be used for temporary analysis only and must never be treated as signoff.
32. When adjusting shared frontend presentation, keep grouped vanity titles such as `VANITIES` in the same room-priority bucket as `Bathroom / Ensuite / Powder`, and preserve the tighter ~75% application-level UI density unless the user explicitly asks to change it.
33. `IMPERIAL_GRID_TRACKER.md` is the authoritative execution tracker for Imperial structure work. Before starting Imperial structural changes, read it first; after each focused work cycle, update What changed / What regressed / What is still broken / Next target there before treating the cycle as closed.

## Verification Expectations
- The app should boot with `uvicorn App.main:app`
- Database initialization should be automatic
- Builder, job, and file flows should persist to SQLite
- Worker should be runnable separately
- Review edits should survive refresh and export

## Git Workflow
Use these scripts from `tools/`:
- `git-setup.ps1`
- `connect-github-remote.ps1`
- `new-feature-branch.ps1`
- `checkpoint.ps1`
- `history.ps1`
- `restore.ps1`

For major changes, use:
- `tools/checkpoint.ps1 -MajorChange -Message "..."`

## GitHub Review Workflow
1. Keep the default branch stable and do parser/UI/export work on short-lived feature branches.
2. Prefer one builder, one field family, or one UI/workflow topic per branch and PR.
3. Use GitHub PRs as the default Codex review surface whenever a remote repo is available.
4. PR descriptions must call out:
   - affected builders and jobs
   - key sample PDFs or live jobs
   - whether `PRD.md`, `Arch.md`, `Project_state.md`, and `AGENTS.md` changed
   - whether reruns and PDF QA are required
5. Default Codex review focus is bug risk, parser regression, builder cross-contamination, PDF QA gating, and field-name drift across UI/export/storage.
6. Use `.github/PULL_REQUEST_TEMPLATE.md` and `.github/CODEOWNERS` as the default repo review conventions once the GitHub remote is connected.
7. For this project, default to `fix this bug` when a live issue is already specific and PDF-grounded. Prefer `review this PR` only when the change touches shared parser flow, grouped-row cleanup, builder finalizers, or PDF QA state handling.

## Online Deployment
Use the online deploy helper from `tools/`:
- `tools/deploy_online.ps1`

Expected workflow after confirmed implementation:
1. Run local verification.
2. Deploy to `spec.lxtransport.online`.
3. Restart `spec-extraction-web.service` and `spec-extraction-worker.service`.
4. Verify `/api/health`.
5. If parsing logic changed, re-run the affected online job, confirm the latest run uses the new build, complete PDF QA against the source PDF, and only then close the task.
