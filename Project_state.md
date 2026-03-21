# Spec_Extraction Project State

## Current Status
- Runnable project created under `Spec_Extraction`
- Root docs are present and synchronized:
  - `PRD.md`
  - `Arch.md`
  - `Project_state.md`
  - `AGENTS.md`
- Application code is implemented:
  - FastAPI web app
  - SQLite persistence
  - Jinja templates
  - separate worker loop
  - Excel and CSV export
  - Git helper scripts
- Deployment scripts are present:
  - `run_server.*`
  - `run_worker.*`
  - `install_systemd.sh`
  - `spec-extraction-web.service`
  - `spec-extraction-worker.service`
  - `build_deploy_zip.ps1`

## Current Goals
1. Keep Builder and Job flows stable while iterating extraction quality
2. Improve extraction accuracy with better parsing and OpenAI prompts
3. Expand product-link enrichment beyond brand homepages
4. Add formal comparison UI in a later version

## Important Constraints
- Major changes must update:
  - `PRD.md`
  - `Arch.md`
  - `Project_state.md`
- UI must remain English-only
- Secrets must stay outside source control
- The app should work even when OpenAI is not configured, using heuristic extraction

## Remaining Work
- Refine OCR fallback for image-heavy PDFs
- Improve room-section detection for more builder formats
- Improve official product URL lookup accuracy
- Build the future comparison UI and diff logic
- Verify Linux deployment on the actual Tencent Cloud server

## Risks
- OCR fallback is currently warning-driven unless stronger OCR infrastructure or OpenAI vision is configured
- Product link enrichment currently starts from brand-domain defaults and heuristic guesses
- The OpenAI Responses integration is optional and depends on valid API credentials and model access

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
- Worker smoke test passed for:
  - upload DOCX spec
  - queue spec extraction
  - process run
  - save raw snapshot to SQLite
