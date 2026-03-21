[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Message,
    [switch]$MajorChange,
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

    throw 'git.exe is not available. Run tools/git-setup.ps1 first.'
}

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
}

$git = Get-GitPath
& $git -C $RepoRoot status --short --branch | Out-Host

if ($MajorChange) {
    $requiredDocs = @('PRD.md', 'Arch.md', 'Project_state.md')
    $missingDocs = @()
    foreach ($doc in $requiredDocs) {
        $docStatus = & $git -C $RepoRoot status --porcelain -- $doc
        if (-not $docStatus) {
            $missingDocs += $doc
        }
    }
    if ($missingDocs.Count -gt 0) {
        throw ("MajorChange requires updates to all core docs. Missing: " + ($missingDocs -join ', '))
    }
}

& $git -C $RepoRoot add . | Out-Null
& $git -C $RepoRoot diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host 'No staged changes to commit.'
    exit 0
}

& $git -C $RepoRoot commit -m $Message | Out-Host
