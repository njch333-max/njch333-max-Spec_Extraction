[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Name,
    [string]$Base = 'master',
    [string]$RepoRoot = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
}

$git = (Get-Command git -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue)
if (-not $git) {
    throw 'git.exe is not available on PATH.'
}

if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot '.git'))) {
    throw "No git repository found at $RepoRoot"
}

& $git -C $RepoRoot checkout $Base | Out-Host
& $git -C $RepoRoot checkout -b $Name | Out-Host
Write-Host "Created feature branch '$Name' from '$Base'."
