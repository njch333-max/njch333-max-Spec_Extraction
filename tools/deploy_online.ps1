$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Local virtual environment not found at $python"
}

& $python (Join-Path $PSScriptRoot "deploy_online.py") @Args
