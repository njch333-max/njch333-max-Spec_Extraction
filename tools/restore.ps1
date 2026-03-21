[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Ref,
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
$dirty = & $git -C $RepoRoot status --porcelain
if ($dirty) {
    throw 'Working tree is not clean. Commit or stash changes before restore.'
}

$safeRef = ($Ref -replace '[^a-zA-Z0-9._-]', '-')
$branchName = "restore-$safeRef-$(Get-Date -Format 'yyyyMMddHHmmss')"

& $git -C $RepoRoot switch -c $branchName $Ref | Out-Host
Write-Host "Created restore branch $branchName at $Ref"
