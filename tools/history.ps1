[CmdletBinding()]
param(
    [int]$Count = 20,
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
& $git -C $RepoRoot log --oneline --decorate --graph -n $Count | Out-Host
