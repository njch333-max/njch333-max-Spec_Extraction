# Evoca Private Real-PDF E2E CI Setup

The Evoca real-PDF e2e suite must not commit customer PDFs to git. The CI
workflow therefore supports a self-hosted runner that can read private PDF
fixtures from a local folder on that runner.

## Why self-hosted

GitHub repository secrets are too small for a nine-PDF fixture archive, and
committing an encrypted PDF archive still puts customer files into repository
history. A self-hosted runner keeps the PDFs outside git and outside GitHub
artifact storage while still letting GitHub trigger the test.

## Runner Requirements

Register a Windows self-hosted GitHub Actions runner with these labels:

- `self-hosted`
- `Windows`
- `evoca-fixtures`

On that runner, place the nine validated Evoca PDFs in a private folder that is
not inside the repository checkout. The filenames must contain these IDs:

- `EVOC447`
- `EVOC467`
- `EVOC473`
- `EVOC471`
- `EVOC482`
- `EVOC436`
- `EVOC449`
- `EVOC479`
- `EVOC480`

## Repository Variables

Set these GitHub repository variables:

- `EVOCA_E2E_ENABLED=1`
- `EVOCA_E2E_PDF_DIR=<absolute path to the private PDF folder on the runner>`

Leave `EVOCA_E2E_ENABLED` unset or `0` until the self-hosted runner is online.
Otherwise the private e2e job may wait for a matching runner.

## Workflow Behavior

`.github/workflows/ci.yml` has two jobs:

- `python-tests`: runs on GitHub-hosted `windows-latest`, installs the project,
  runs `compileall`, and runs Evoca structured tests. The real-PDF e2e test is
  allowed to skip there because no private PDFs are available.
- `evoca-real-pdf-e2e`: runs only when `EVOCA_E2E_ENABLED=1`, uses the
  self-hosted `evoca-fixtures` runner, and calls
  `tools/run_evoca_real_pdf_e2e.ps1` in gating mode. Missing any one of the nine
  PDFs fails the job.

## Local Equivalent

Use the same gate locally:

```powershell
powershell -ExecutionPolicy Bypass -File tools\run_evoca_real_pdf_e2e.ps1
```

For a one-off non-gating smoke run:

```powershell
powershell -ExecutionPolicy Bypass -File tools\run_evoca_real_pdf_e2e.ps1 -AllowSkip
```
