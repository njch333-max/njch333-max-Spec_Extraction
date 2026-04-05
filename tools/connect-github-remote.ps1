[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Owner,
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [string]$RemoteName = 'origin',
    [string]$RepoRoot = '',
    [switch]$TestOnly
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

$remoteUrl = "git@github.com:$Owner/$Repo.git"

if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot '.git'))) {
    throw "No git repository found at $RepoRoot"
}

$existingUrl = (& $git -C $RepoRoot remote get-url $RemoteName) 2>$null
if ($existingUrl) {
    & $git -C $RepoRoot remote set-url $RemoteName $remoteUrl | Out-Null
    Write-Host "Updated remote '$RemoteName' -> $remoteUrl"
} else {
    & $git -C $RepoRoot remote add $RemoteName $remoteUrl | Out-Null
    Write-Host "Added remote '$RemoteName' -> $remoteUrl"
}

Write-Host 'Testing remote connectivity...'
& $git -C $RepoRoot ls-remote $RemoteName | Out-Host

if (-not $TestOnly) {
    Write-Host "Remote '$RemoteName' is configured. Push with: git -C `"$RepoRoot`" push -u $RemoteName master"
}
