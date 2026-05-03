[CmdletBinding()]
param(
    [string]$RegistrationToken = '',
    [string]$RepoUrl = 'https://github.com/njch333-max/njch333-max-Spec_Extraction',
    [string]$RunnerDir = 'C:\SpecExtraction\actions-runner',
    [string]$PdfDir = 'C:\SpecExtraction\EvocaRealPdfs',
    [string]$RunnerName = '',
    [switch]$InstallService,
    [switch]$ValidateOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$expectedPdfIds = @(
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

function Resolve-RequiredDirectory {
    param([Parameter(Mandatory = $true)][string]$PathValue)
    $resolved = Resolve-Path -LiteralPath $PathValue -ErrorAction SilentlyContinue
    if (-not $resolved) {
        throw "Directory not found: $PathValue"
    }
    return $resolved.Path
}

function Test-EvocaPdfFixtures {
    param([Parameter(Mandatory = $true)][string]$Directory)
    $missing = @()
    foreach ($pdfId in $expectedPdfIds) {
        $matches = Get-ChildItem -LiteralPath $Directory -Recurse -File -Filter "*$pdfId*.pdf" | Select-Object -First 2
        if (-not $matches) {
            $missing += $pdfId
        }
    }
    if ($missing.Count -gt 0) {
        throw ("Missing private Evoca PDF fixtures in " + $Directory + ": " + ($missing -join ', '))
    }
}

$resolvedPdfDir = Resolve-RequiredDirectory -PathValue $PdfDir
Test-EvocaPdfFixtures -Directory $resolvedPdfDir
Write-Host "Evoca private PDF fixtures verified at $resolvedPdfDir"

if ($ValidateOnly) {
    Write-Host 'ValidateOnly complete. No runner was downloaded or configured.'
    exit 0
}

if (-not $RegistrationToken) {
    throw 'RegistrationToken is required unless -ValidateOnly is used. Get it from GitHub: Settings -> Actions -> Runners -> New self-hosted runner.'
}

if (-not $RunnerName) {
    $RunnerName = "spec-extraction-$env:COMPUTERNAME"
}

if (-not (Test-Path -LiteralPath $RunnerDir)) {
    New-Item -ItemType Directory -Path $RunnerDir | Out-Null
}
$resolvedRunnerDir = (Resolve-Path -LiteralPath $RunnerDir).Path

$configCmd = Join-Path $resolvedRunnerDir 'config.cmd'
if (-not (Test-Path -LiteralPath $configCmd)) {
    Write-Host 'Downloading latest GitHub Actions runner for Windows x64...'
    $release = Invoke-RestMethod -Uri 'https://api.github.com/repos/actions/runner/releases/latest' -Headers @{ 'User-Agent' = 'SpecExtractionRunnerSetup' }
    $asset = $release.assets | Where-Object { $_.name -like 'actions-runner-win-x64-*.zip' } | Select-Object -First 1
    if (-not $asset) {
        throw 'Could not find a Windows x64 runner asset in the latest actions/runner release.'
    }
    $zipPath = Join-Path $resolvedRunnerDir $asset.name
    Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $zipPath
    Expand-Archive -LiteralPath $zipPath -DestinationPath $resolvedRunnerDir -Force
}

Push-Location $resolvedRunnerDir
try {
    Write-Host "Configuring runner '$RunnerName' for $RepoUrl..."
    & .\config.cmd `
        --url $RepoUrl `
        --token $RegistrationToken `
        --name $RunnerName `
        --labels 'evoca-fixtures' `
        --work '_work' `
        --unattended `
        --replace
    if ($LASTEXITCODE -ne 0) {
        throw "config.cmd failed with exit code $LASTEXITCODE"
    }

    if ($InstallService) {
        Write-Host 'Installing and starting the runner service...'
        & .\svc.cmd install
        if ($LASTEXITCODE -ne 0) {
            throw "svc.cmd install failed with exit code $LASTEXITCODE"
        }
        & .\svc.cmd start
        if ($LASTEXITCODE -ne 0) {
            throw "svc.cmd start failed with exit code $LASTEXITCODE"
        }
    } else {
        Write-Host 'Runner configured. Start it interactively with:'
        Write-Host "  cd `"$resolvedRunnerDir`""
        Write-Host '  .\run.cmd'
    }
}
finally {
    Pop-Location
}

Write-Host ''
Write-Host 'Set these GitHub repository variables before enabling the private e2e job:'
Write-Host '  EVOCA_E2E_ENABLED=1'
Write-Host "  EVOCA_E2E_PDF_DIR=$resolvedPdfDir"
