$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$venvPython = Join-Path $projectRoot ".venv\\Scripts\\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }
$port = if ($env:SPEC_EXTRACTION_WEB_PORT) { $env:SPEC_EXTRACTION_WEB_PORT } else { "8010" }

Set-Location $projectRoot
& $pythonExe -m uvicorn App.main:app --host 127.0.0.1 --port $port
