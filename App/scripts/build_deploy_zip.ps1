$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$outputZip = Join-Path $projectRoot "spec-extraction-deploy.zip"
$stagingRoot = Join-Path $env:TEMP ("spec-extraction-stage-" + [guid]::NewGuid().ToString("N"))

New-Item -ItemType Directory -Path $stagingRoot | Out-Null

$includeItems = @(
    "App",
    "tools",
    "AGENTS.md",
    "PRD.md",
    "Arch.md",
    "Project_state.md",
    ".env.example",
    ".gitignore",
    "requirements.txt"
)

function Copy-CleanTree([string]$source, [string]$target) {
    New-Item -ItemType Directory -Path $target -Force | Out-Null
    Get-ChildItem -LiteralPath $source -Force | ForEach-Object {
        if ($_.Name -in @(".venv", "__pycache__", "data")) { return }
        if ($_.Extension -eq ".pyc") { return }
        $dest = Join-Path $target $_.Name
        if ($_.PSIsContainer) {
            Copy-CleanTree -source $_.FullName -target $dest
        } else {
            Copy-Item -LiteralPath $_.FullName -Destination $dest -Force
        }
    }
}

foreach ($item in $includeItems) {
    $source = Join-Path $projectRoot $item
    if (-not (Test-Path $source)) { continue }
    $target = Join-Path $stagingRoot $item
    if ((Get-Item $source).PSIsContainer) {
        Copy-CleanTree -source $source -target $target
    } else {
        Copy-Item -LiteralPath $source -Destination $target -Force
    }
}

if (Test-Path $outputZip) {
    Remove-Item $outputZip -Force
}

Compress-Archive -Path (Join-Path $stagingRoot "*") -DestinationPath $outputZip -Force
Remove-Item $stagingRoot -Recurse -Force

Write-Output $outputZip
