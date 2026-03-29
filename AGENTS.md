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
5. Before major commits, update `PRD.md`, `Arch.md`, and `Project_state.md`.
6. Prefer the local Git helper scripts instead of ad hoc Git commands when creating checkpoints or reviewing history.
7. `spec.lxtransport.online` is the default live environment. After a confirmed implementation change, deploy to production unless the user explicitly says not to.
8. Treat a task as complete only after local checks pass, production services are restarted successfully, and the affected live page or job is verified.
9. For parser-accuracy work, the source PDF is the acceptance source of truth. Do not sign off based only on older webpages or older snapshots.
10. When a builder-specific polish path has access to both `raw_text` and vision-normalized `text`, prefer `raw_text` for field recovery and use normalized `text` only as a fallback.

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
5. If parsing logic changed, re-run the affected online job, confirm the latest run uses the new build, and compare the result against the source PDF before closing the task.
