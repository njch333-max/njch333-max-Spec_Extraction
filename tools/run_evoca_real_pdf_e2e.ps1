[CmdletBinding()]
param(
    [string]$PdfDir = '',
    [switch]$AllowSkip,
    [string]$RepoRoot = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

$python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    throw "Local virtual environment not found at $python"
}

if (-not $PdfDir) {
    $PdfDir = Join-Path $RepoRoot 'tests\fixtures\evoca_real_pdfs'
}
$resolvedPdfDir = (Resolve-Path -LiteralPath $PdfDir -ErrorAction SilentlyContinue)
if (-not $resolvedPdfDir) {
    throw "Evoca real-PDF fixture directory not found: $PdfDir"
}

$expectedPdfIds = @(
    'EVOC434',
    'EVOC447',
    'EVOC467',
    'EVOC473',
    'EVOC471',
    'EVOC482',
    'EVOC436',
    'EVOC449',
    'EVOC479',
    'EVOC480'
)

$missing = @()
foreach ($pdfId in $expectedPdfIds) {
    $match = Get-ChildItem -LiteralPath $resolvedPdfDir.Path -Recurse -File -Filter "*$pdfId*.pdf" | Select-Object -First 2
    if (-not $match) {
        $missing += $pdfId
    }
}

if ($missing.Count -gt 0 -and -not $AllowSkip) {
    throw ("Missing private Evoca real-PDF fixtures: " + ($missing -join ', ') + ". Use -AllowSkip only for a non-gating smoke run.")
}

$env:EVOCA_E2E_PDF_DIR = $resolvedPdfDir.Path
if ($AllowSkip) {
    Remove-Item Env:EVOCA_E2E_REQUIRE_PDFS -ErrorAction SilentlyContinue
} else {
    $env:EVOCA_E2E_REQUIRE_PDFS = '1'
}

& $python -m pytest (Join-Path $RepoRoot 'tests\test_evoca_end_to_end.py') -q
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
