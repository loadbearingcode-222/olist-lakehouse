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
    [string]$Target = 'help'
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
            @('check',     'Everything the CI quality job runs, locally')
        ) | ForEach-Object { '    {0,-12} {1}' -f $_[0], $_[1] }
        Write-Host ''
    }
    'hooks'     { Invoke-Checked 'git' @('config', 'core.hooksPath', '.githooks'); Write-Host 'git hooks installed from .githooks/' }
    'sync'      { Invoke-Uv @('sync', '--locked', '--group', 'dev') }
    'lint'      { Invoke-Uv @('run', 'ruff', 'check', '.'); Invoke-Uv @('run', 'ruff', 'format', '--check', '.') }
    'format'    { Invoke-Uv @('run', 'ruff', 'check', '--fix', '.'); Invoke-Uv @('run', 'ruff', 'format', '.') }
    'typecheck' { Invoke-Uv @('run', 'mypy') }
    'test'      { Invoke-Uv @('run', 'pytest', 'tests/unit') }
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
