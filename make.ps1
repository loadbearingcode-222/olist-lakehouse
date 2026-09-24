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
    # Only for `dbt`: everything after the target is passed to dbt, e.g. .\make.ps1 dbt build
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @(),
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

function New-FernetKey {
    # A Fernet key is 32 random bytes, URL-safe base64 (what Airflow encrypts connections with).
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    return [Convert]::ToBase64String($bytes).Replace('+', '-').Replace('/', '_')
}

function Invoke-Env {
    $values = @{
        'MINIO_ROOT_USER'           = 'olist-admin'
        'MINIO_ROOT_PASSWORD'       = (New-Secret)
        'PLATFORM_DB_PASSWORD'      = (New-Secret)
        'LAKEKEEPER_ENCRYPTION_KEY' = (New-Secret 48)
        'SOURCE_DB_PASSWORD'        = (New-Secret)
        'DEBEZIUM_DB_PASSWORD'      = (New-Secret)
        'AIRFLOW_ADMIN_PASSWORD'    = (New-Secret 16)
        'AIRFLOW_FERNET_KEY'        = (New-FernetKey)
        'AIRFLOW_JWT_SECRET'        = (New-Secret 48)
    }
    $keys = @(Get-Content -LiteralPath '.env.example' | Where-Object { $_ -match '^([A-Z0-9_]+)=$' } | ForEach-Object { $_.TrimEnd('=') })
    $missing = @($keys | Where-Object { -not $values.ContainsKey($_) })
    if ($missing.Count -gt 0) { throw "make.ps1 env has no generator for: $($missing -join ', ')" }
    $path = Join-Path $PSScriptRoot '.env'
    if (Test-Path -LiteralPath $path) {
        # Never rewrite an existing secret (volumes were initialised with it); only add new keys.
        $present = @(Get-Content -LiteralPath $path | Where-Object { $_ -match '^([A-Z0-9_]+)=' } | ForEach-Object { ($_ -split '=', 2)[0] })
        $added = @($keys | Where-Object { $present -notcontains $_ })
        if ($added.Count -eq 0) { Write-Host '.env already complete - leaving it alone.'; return }
        $text = [System.IO.File]::ReadAllText($path)
        if ($text.Length -gt 0 -and -not $text.EndsWith("`n")) { $text += "`n" }
        $text += (($added | ForEach-Object { "$_=$($values[$_])" }) -join "`n") + "`n"
        [System.IO.File]::WriteAllText($path, $text)
        Write-Host ".env: added $($added -join ', ') (existing values untouched)."
        return
    }
    $lines = Get-Content -LiteralPath '.env.example' | ForEach-Object {
        if ($_ -match '^([A-Z0-9_]+)=$') { "$($Matches[1])=$($values[$Matches[1]])" } else { $_ }
    }
    # UTF-8 without BOM, LF endings: compose and containers read this file.
    [System.IO.File]::WriteAllText($path, (($lines -join "`n") + "`n"))
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
            @('test-integration', 'Integration tests against the running stack'),
            @('replay',        'Start (or resume) the replayer and the clickstream producer'),
            @('replay-stop',   'Stop both (each resumes from its checkpoint later)'),
            @('replay-status', 'Where the replay and the clickstream are'),
            @('replay-verify', 'Row counts + chaos ledger match the checkpoint?'),
            @('bronze-logs',   'Tail the bronze streaming job (Spark UI: http://localhost:24040)'),
            @('gate-redteam',  'Plant one defect per quality rule in a throwaway Spark; each must be refused'),
            @('dbt',           'Run dbt against the lake, e.g. .\make.ps1 dbt build (host -> Trino 127.0.0.1:28090)'),
            @('fx',            'Load BRL->USD ECB rates into bronze: -Rest 2016-08-25,2018-10-31 (default range)'),
            @('reconcile',     'Silver vs Postgres, every live row and column (chaos keys counted, not hidden)'),
            @('drills',        'Failure drills: kill Connect, kill Spark, restart Kafka (evidence -> automation/runs/m2-s3)')
        ) | ForEach-Object { '    {0,-18} {1}' -f $_[0], $_[1] }
        Write-Host ''
    }
    'hooks'     { Invoke-Checked 'git' @('config', 'core.hooksPath', '.githooks'); Write-Host 'git hooks installed from .githooks/' }
    'sync'      { Invoke-Uv @('sync', '--locked', '--group', 'dev', '--group', 'quality') }
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
        Write-Host '  Airflow        ->  http://localhost:28082   (admin / AIRFLOW_ADMIN_PASSWORD in .env)' -ForegroundColor Green
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
    'replay'        { Invoke-Compose @('--profile', 'replay', 'up', '-d', '--build', 'replayer', 'clickstream') }
    'replay-stop'   { Invoke-Compose @('--profile', 'replay', 'stop', 'replayer', 'clickstream') }
    'replay-status' {
        Invoke-Compose @('--profile', 'replay', 'run', '--rm', '--no-deps', 'replayer', 'olist_lakehouse.replayer', '--status')
        Invoke-Compose @('--profile', 'replay', 'run', '--rm', '--no-deps', 'clickstream', 'olist_lakehouse.clickstream', '--status')
    }
    'replay-verify' { Invoke-Compose @('--profile', 'replay', 'run', '--rm', '--no-deps', 'replayer', 'olist_lakehouse.replayer', '--verify') }
    'bronze-logs'   { Invoke-Compose @('logs', '-f', '--tail=50', 'bronze-stream') }
    'gate-redteam'  { Invoke-Compose @('run', '--rm', '--no-deps', 'bronze-stream', '/opt/spark/bin/spark-submit', '/opt/app/src/olist_lakehouse/quality/redteam.py') }
    'dbt'           {
        $env:DBT_PROJECT_DIR = Join-Path $PSScriptRoot 'dbt\olist'
        $env:DBT_PROFILES_DIR = $env:DBT_PROJECT_DIR
        Invoke-Uv (@('run', '--group', 'dbt', 'dbt') + $Rest)
    }
    'fx'            {
        $range = if ($Rest.Count -ge 2) { $Rest } else { @('2016-08-25', '2018-10-31') }
        Invoke-Uv @('run', '--group', 'integration', 'python', '-m', 'olist_lakehouse.fx', '--start', $range[0], '--end', $range[1])
    }
    'reconcile'     { Invoke-Uv @('run', '--group', 'integration', 'python', 'scripts/reconcile.py', '--write') }
    'drills'        {
        $env:OLIST_STACK_REQUIRED = '1'
        Invoke-Uv @('run', '--group', 'integration', 'pytest', 'tests/integration/test_drills.py', '-s')
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
