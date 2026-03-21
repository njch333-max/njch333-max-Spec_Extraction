$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$venvPython = Join-Path $projectRoot ".venv\\Scripts\\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }

Set-Location $projectRoot
& $pythonExe -m App.worker_main
