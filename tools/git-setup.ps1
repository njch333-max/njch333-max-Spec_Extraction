[CmdletBinding()]
param(
    [string]$RepoRoot = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-GitPath {
    $candidates = @(
        (Get-Command git -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue),
        'C:\Program Files\Git\cmd\git.exe',
        'C:\Program Files\Git\bin\git.exe',
        'C:\Program Files (x86)\Git\cmd\git.exe'
    ) | Where-Object { $_ }

    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    return $null
}

function Ensure-GitInstalled {
    $gitPath = Get-GitPath
    if ($gitPath) {
        return $gitPath
    }

    Write-Host 'Git not found. Installing Git for Windows via winget...'
    & winget install --id Git.Git --exact --accept-source-agreements --accept-package-agreements --silent | Out-Host

    $env:Path = [System.Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + [System.Environment]::GetEnvironmentVariable('Path', 'User')
    $gitPath = Get-GitPath
    if (-not $gitPath) {
        throw 'Git installation finished but git.exe is still not available.'
    }

    return $gitPath
}

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
}

$git = Ensure-GitInstalled

if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot '.git'))) {
    & $git -C $RepoRoot init | Out-Host
}

$name = (& $git -C $RepoRoot config --get user.name) 2>$null
$email = (& $git -C $RepoRoot config --get user.email) 2>$null
if (-not $name) {
    & $git -C $RepoRoot config user.name 'Codex Local' | Out-Null
}
if (-not $email) {
    & $git -C $RepoRoot config user.email 'codex@local.invalid' | Out-Null
}

& $git -C $RepoRoot add . | Out-Null
& $git -C $RepoRoot diff --cached --quiet
if ($LASTEXITCODE -ne 0) {
    & $git -C $RepoRoot commit -m 'Initial Spec_Extraction scaffold' | Out-Host
} else {
    Write-Host 'Repository is already initialized and has no new changes to commit.'
}

Write-Host "Git is ready at $RepoRoot"
