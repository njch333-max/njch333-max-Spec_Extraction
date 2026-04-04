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
13. Default runtime tool policy is speed-first:
    - `Clarendon`: heuristic-only
    - `Imperial / Simonds / Evoca / Yellowwood`: layout + row-local parser + selective Docling on difficult schedule/table pages
    - default automatic `Heavy Vision`: off
    - default automatic `AI merge`: off
14. Clarendon room names must come only from the `Drawings and Colours` room-master file when that file exists. AFC/supplement files may enrich existing rooms only and may not create new room names.
15. Clarendon AFC flooring pages such as `CARPET & MAIN FLOOR TILE` must enrich existing room-master rooms only. Room-specific flooring should land on the relevant room cards, not in `others.flooring_notes`.
16. Yellowwood room names must prefer the concrete joinery/spec title, and rooms without joinery/material evidence must be dropped. `robe` and `media` rooms stay only when they contain real material evidence such as `Polytec` or `Laminex`.
17. Yellowwood flooring and tiling schedule pages must enrich retained rooms such as `Kitchen`, robe rooms, and vanity rooms without creating new plumbing-only rooms; contents-page flooring text must never populate `others.flooring_notes`.
18. Keep `colour/material` values and appliance placeholders close to source wording with light cleanup only. Placeholder appliance rows such as `As Above`, `By Client`, or `N/A CLIENT TO CHECK` may be deduplicated only when the same source already contains a concrete model for that appliance type.
19. Yellowwood vanity plumbing enrichment must stay room-relevant: accessory text may keep towel rails or toilet-roll holders, but shower/floor-waste/basin-waste tails and repeated room-heading tails should be trimmed out.
20. All new `spec` parse runs for all builders must enter field-level PDF QA automatically. Raw results may be viewed before signoff, but they are not formally accepted until PDF QA passes.
21. Formal spec exports are locked behind PDF QA. Do not treat a raw spec snapshot as complete, export-ready, or fixed until the current raw snapshot verification is `passed`.
22. Parser-accuracy work is only complete after the affected live rerun passes PDF QA against the source PDF page-by-page. Older webpages and older snapshots are reference material only.

## Verification Expectations
- The app should boot with `uvicorn App.main:app`
- Database initialization should be automatic
- Builder, job, and file flows should persist to SQLite
- Worker should be runnable separately
- Review edits should survive refresh and export

## Git Workflow
Use these scripts from `tools/`:
- `git-setup.ps1`
- `checkpoint.ps1`
- `history.ps1`
- `restore.ps1`

For major changes, use:
- `tools/checkpoint.ps1 -MajorChange -Message "..."`

## Online Deployment
Use the online deploy helper from `tools/`:
- `tools/deploy_online.ps1`

Expected workflow after confirmed implementation:
1. Run local verification.
2. Deploy to `spec.lxtransport.online`.
3. Restart `spec-extraction-web.service` and `spec-extraction-worker.service`.
4. Verify `/api/health`.
5. If parsing logic changed, re-run the affected online job, confirm the latest run uses the new build, complete PDF QA against the source PDF, and only then close the task.
