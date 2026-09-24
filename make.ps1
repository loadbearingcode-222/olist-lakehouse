#!/usr/bin/env pwsh
# The project's single task runner. GNU Make is not installed on this Windows machine and the
# project path contains a space, so every command a story documents goes through here.
# Keep this file ASCII-only: Windows PowerShell 5.1 reads a BOM-less script as ANSI.
#
# Usage:  .\make.ps1 help
#         .\make.ps1 check

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = 'help',
    # Only for `reset`: confirms that the named volumes (all lake data) may be deleted.
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Checked {
    param([string]$Exe, [string[]]$Arguments)
    # Windows PowerShell 5.1 turns a native tool's stderr into error records whenever output
    # is redirected, and 'Stop' would abort on uv's progress lines. Judge by exit code only.
    $ErrorActionPreference = 'Continue'
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Exe $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Uv { param([string[]]$Arguments) Invoke-Checked 'uv' $Arguments }
function Invoke-Compose { param([string[]]$Arguments) Invoke-Checked 'docker' (@('compose') + $Arguments) }

function New-Secret {
    param([int]$Length = 32)
    # Alphanumeric only, so a secret can sit inside a connection URL unescaped.
    $alphabet = [char[]]'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'
    $bytes = New-Object byte[] $Length
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return -join ($bytes | ForEach-Object { $alphabet[$_ % $alphabet.Length] })
}

function Invoke-Env {
    if (Test-Path -LiteralPath '.env') {
        Write-Host '.env already exists - leaving it alone.'
        return
    }
    $values = @{
        'MINIO_ROOT_USER'           = 'olist-admin'
        'MINIO_ROOT_PASSWORD'       = (New-Secret)
        'PLATFORM_DB_PASSWORD'      = (New-Secret)
        'LAKEKEEPER_ENCRYPTION_KEY' = (New-Secret 48)
    }
    $lines = Get-Content -LiteralPath '.env.example' | ForEach-Object {
        if ($_ -match '^([A-Z0-9_]+)=$' -and $values.ContainsKey($Matches[1])) { "$($Matches[1])=$($values[$Matches[1]])" } else { $_ }
    }
    $missing = @(Get-Content -LiteralPath '.env.example' | Where-Object { $_ -match '^([A-Z0-9_]+)=$' -and -not $values.ContainsKey($Matches[1]) })
    if ($missing.Count -gt 0) { throw "make.ps1 env has no generator for: $($missing -join ', ')" }
    # UTF-8 without BOM, LF endings: compose and containers read this file.
    [System.IO.File]::WriteAllText((Join-Path $PSScriptRoot '.env'), (($lines -join "`n") + "`n"))
    Write-Host '.env written with generated secrets (never commit it).'
}

switch ($Target.ToLowerInvariant()) {
    'help' {
        Write-Host ''
        Write-Host '  olist-lakehouse targets' -ForegroundColor Cyan
        Write-Host ''
        @(
            @('hooks',     'Install git hooks (refuses direct pushes to main)'),
            @('sync',      'Create/refresh the local venv from uv.lock'),
            @('lint',      'Check formatting and lint rules'),
            @('format',    'Auto-fix formatting and lint rules'),
            @('typecheck', 'Type-check with mypy (strict)'),
            @('test',      'Run unit tests (fast, no containers)'),
            @('check',     'Everything the CI quality job runs, locally'),
            @('fetch',     'Download Olist from Kaggle, verify sha256 + contract'),
            @('env',       'Create .env with freshly generated secrets'),
            @('doctor',    'Preflight: port holders and free Docker memory'),
            @('up',        'env + doctor, then start the stack and wait until healthy'),
            @('down',      'Stop the stack, keep volumes'),
            @('ps',        'Show container status'),
            @('logs',      'Tail logs from all services'),
            @('reset',     'Stop the stack AND delete its volumes (needs -Force)'),
            @('test-integration', 'Integration tests against the running stack')
        ) | ForEach-Object { '    {0,-18} {1}' -f $_[0], $_[1] }
        Write-Host ''
    }
    'hooks'     { Invoke-Checked 'git' @('config', 'core.hooksPath', '.githooks'); Write-Host 'git hooks installed from .githooks/' }
    'sync'      { Invoke-Uv @('sync', '--locked', '--group', 'dev') }
    'lint'      { Invoke-Uv @('run', 'ruff', 'check', '.'); Invoke-Uv @('run', 'ruff', 'format', '--check', '.') }
    'format'    { Invoke-Uv @('run', 'ruff', 'check', '--fix', '.'); Invoke-Uv @('run', 'ruff', 'format', '.') }
    'typecheck' { Invoke-Uv @('run', 'mypy') }
    'test'      { Invoke-Uv @('run', 'pytest', 'tests/unit') }
    'fetch'     { Invoke-Uv @('run', 'python', '-m', 'olist_lakehouse.fetch') }
    'env'       { Invoke-Env }
    'doctor'    { Invoke-Checked 'powershell' @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $PSScriptRoot 'scripts\doctor.ps1')) }
    'up'        {
        Invoke-Env
        & $PSCommandPath doctor
        Invoke-Compose @('up', '-d', '--wait')
        Write-Host ''
        Write-Host '  MinIO console  ->  http://localhost:29001   (MINIO_ROOT_USER / _PASSWORD in .env)' -ForegroundColor Green
        Write-Host '  Lakekeeper UI  ->  http://localhost:28181/ui' -ForegroundColor Green
        Write-Host '  Trino UI       ->  http://localhost:28090   (any user name, no password)' -ForegroundColor Green
        Write-Host ''
    }
    'down'      { Invoke-Compose @('down') }
    'ps'        { Invoke-Compose @('ps', '-a') }
    'logs'      { Invoke-Compose @('logs', '-f', '--tail=100') }
    'reset'     {
        if (-not $Force) { throw 'reset deletes every volume (all lake data). Re-run as: .\make.ps1 reset -Force' }
        Invoke-Compose @('down', '-v')
    }
    'test-integration' {
        $env:OLIST_STACK_REQUIRED = '1'
        Invoke-Uv @('run', '--group', 'integration', 'pytest', 'tests/integration', '-m', 'integration')
    }
    'check'     {
        & $PSCommandPath sync
        & $PSCommandPath lint
        & $PSCommandPath typecheck
        & $PSCommandPath test
        Write-Host 'check: lint, types and unit tests all passed' -ForegroundColor Green
    }

    default {
        Write-Host "Unknown target: $Target" -ForegroundColor Red
        & $PSCommandPath help
        exit 1
    }
}
