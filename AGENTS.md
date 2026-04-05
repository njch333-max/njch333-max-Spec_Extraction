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
10. When a builder-specific polish path has access to both `raw_text` and vision-normalized `text`, prefer `raw_text` for field recovery and use normalized `text` only as a fallback.
11. Treat spec parsing as structure-first and row-local work: `layout_rows -> row-fragment -> row-local mapping` is the default field path, and supplier, note, model, or profile text must not bleed across rows.
12. Keep field ownership same-room-only, same-section-only, and same-row-or-row-fragment-only. Do not borrow supplier, note, or model text across adjacent rows.
13. Treat the shared parser as a structure layer, not the final business-output layer. Major builder logic belongs in an explicit builder finalizer stage that owns final room-title preservation, overlay merge priority, fixture blacklists, and grouped-row/property-row cleanup.
14. Default runtime tool policy is speed-first:
    - `Clarendon`: heuristic-only
    - `Imperial / Simonds / Evoca / Yellowwood`: layout + row-local parser + selective Docling on difficult schedule/table pages
    - default automatic `Heavy Vision`: off
    - default automatic `AI merge`: off
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
25. Final room retention is global across builders: a room survives only when it has true joinery/material evidence such as bench tops, door colours, splashback, toe kick, bulkheads, floating shelf, or `Shelf`. Handles, plumbing fixtures, flooring, LED, accessories, and other notes do not keep a room alive.
26. Fixture cleanup must not over-trim legitimate product wording. In particular, Clarendon tap values containing phrases like `Twin Handle Sink Mixer` must survive intact, and Imperial sinkware notes such as `UNDERMOUNT` or generic taphole hints must stay attached only to the correct same-room sink row.
27. All new `spec` parse runs for all builders must enter field-level PDF QA automatically. Raw results may be viewed before signoff, but they are not formally accepted until PDF QA passes.
28. Formal spec exports are locked behind PDF QA. Do not treat a raw spec snapshot as complete, export-ready, or fixed until the current raw snapshot verification is `passed`.
29. Parser-accuracy work is only complete after the affected live rerun passes PDF QA against the source PDF page-by-page. Older webpages and older snapshots are reference material only.

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
