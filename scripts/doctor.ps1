#!/usr/bin/env pwsh
# Preflight for `up`: can this stack start here, right now? Read-only; changes nothing.
#
# Refusals, each with its own code (exit 1 if any fires):
#   DOCTOR-NO-DOCKER     the Docker daemon is not answering
#   DOCTOR-PORT-FOREIGN  a port this project claims is held by something that is not ours
#   DOCTOR-RAM-SHORT     Docker memory minus what foreign containers use is below our budget
#
# A port held by THIS project's own container is fine (the stack is already up), so the holder
# is resolved rather than just "is the port busy?". Keep this file ASCII-only (PowerShell 5.1).

[CmdletBinding()]
param(
    [int]$BudgetGB = 20
)

$ErrorActionPreference = 'Stop'
# COMPOSE_PROJECT_NAME overrides compose.yaml's `name:` (the clean-clone demo runs as its own project).
$Project = if ($env:COMPOSE_PROJECT_NAME) { $env:COMPOSE_PROJECT_NAME } else { 'olist-lakehouse' }
# The 2xxxx port family (CLAUDE.md fleet ledger). Later stories' ports are claimed now, so a
# collision shows up on day one rather than in M4.
$Ports = [ordered]@{
    'postgres-source' = 25432; 'kafka' = 29092; 'kafka-connect' = 28083; 'kafka-console' = 28080
    'minio-api' = 29000; 'minio-console' = 29001; 'lakekeeper' = 28181; 'trino' = 28090
    'spark-ui' = 24040; 'spark-master-ui' = 28070; 'airflow' = 28082; 'superset' = 28088
    'data-docs' = 28089
}

$refusals = New-Object System.Collections.Generic.List[string]

function Invoke-Docker {
    param([string[]]$Arguments)
    $ErrorActionPreference = 'Continue'
    $out = & docker @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $out
}

function ConvertTo-Bytes {
    param([string]$Text)
    if ($Text -notmatch '^\s*([\d.]+)\s*([KMGT]?i?B)\s*$') { return 0 }
    $n = [double]$Matches[1]
    $unit = $Matches[2]
    # Hashtable keys are case-insensitive, so 'kB' also covers 'KB'.
    $scale = @{ 'B' = 1; 'KiB' = 1KB; 'kB' = 1000; 'MiB' = 1MB; 'MB' = 1000000
                'GiB' = 1GB; 'GB' = 1000000000; 'TiB' = 1TB }
    if (-not $scale.ContainsKey($unit)) { return 0 }
    return $n * $scale[$unit]
}

# 1. Docker ---------------------------------------------------------------------------------
$memTotal = Invoke-Docker @('info', '--format', '{{.MemTotal}}')
if (-not $memTotal) {
    Write-Host 'DOCTOR-NO-DOCKER: the Docker daemon is not answering. Start Docker Desktop.' -ForegroundColor Red
    exit 1
}
$memTotal = [double]$memTotal

# `{{.Labels}}`, not `{{.Label "..."}}`: Windows PowerShell 5.1 strips the inner quotes when it
# passes the argument to docker, and the template then fails silently.
$containers = @(Invoke-Docker @('ps', '--format', '{{.Names}}|{{.Labels}}|{{.Ports}}') |
    Where-Object { $_ } |
    ForEach-Object {
        $name, $labels, $portText = $_ -split '\|', 3
        $proj = if ($labels -match '(?:^|,)com\.docker\.compose\.project=([^,]*)') { $Matches[1] } else { '' }
        [pscustomobject]@{ Name = $name; Project = $proj; Ports = $portText }
    })

# 2. Ports ----------------------------------------------------------------------------------
Write-Host ''
Write-Host '  ports' -ForegroundColor Cyan
foreach ($entry in $Ports.GetEnumerator()) {
    $port = $entry.Value
    $holder = $containers | Where-Object { $_.Ports -match ":$port->" } | Select-Object -First 1
    if ($holder) {
        if ($holder.Project -eq $Project) {
            '    {0,-16} {1,5}  ours ({2})' -f $entry.Key, $port, $holder.Name | Write-Host
        } else {
            $label = if ($holder.Project) { "project $($holder.Project)" } else { 'no compose project' }
            $refusals.Add("DOCTOR-PORT-FOREIGN: $port ($($entry.Key)) is held by container $($holder.Name), $label")
            '    {0,-16} {1,5}  FOREIGN container {2} ({3})' -f $entry.Key, $port, $holder.Name, $label | Write-Host -ForegroundColor Red
        }
        continue
    }
    $listener = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listener) {
        $proc = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
        $procName = if ($proc) { $proc.ProcessName } else { "pid $($listener.OwningProcess)" }
        $refusals.Add("DOCTOR-PORT-FOREIGN: $port ($($entry.Key)) is held by host process $procName")
        '    {0,-16} {1,5}  FOREIGN process {2}' -f $entry.Key, $port, $procName | Write-Host -ForegroundColor Red
    } else {
        '    {0,-16} {1,5}  free' -f $entry.Key, $port | Write-Host
    }
}

# 3. Memory ---------------------------------------------------------------------------------
$foreignBytes = 0.0
$foreignStacks = @{}
$stats = Invoke-Docker @('stats', '--no-stream', '--format', '{{.Name}}|{{.MemUsage}}')
foreach ($line in @($stats | Where-Object { $_ })) {
    $name, $usage = $line -split '\|', 2
    $container = $containers | Where-Object { $_.Name -eq $name } | Select-Object -First 1
    if ($container -and $container.Project -eq $Project) { continue }
    $used = ConvertTo-Bytes (($usage -split '/')[0])
    $foreignBytes += $used
    $stack = if ($container -and $container.Project) { $container.Project } else { $name }
    $foreignStacks[$stack] = [double]($foreignStacks[$stack]) + $used
}
$availableGB = ($memTotal - $foreignBytes) / 1GB

Write-Host ''
Write-Host '  memory' -ForegroundColor Cyan
'    docker total      {0,6:N1} GB' -f ($memTotal / 1GB) | Write-Host
foreach ($s in $foreignStacks.GetEnumerator()) {
    '    foreign {0,-10} {1,6:N1} GB' -f $s.Key, ($s.Value / 1GB) | Write-Host
}
'    available         {0,6:N1} GB   (budget {1} GB)' -f $availableGB, $BudgetGB | Write-Host
if ($availableGB -lt $BudgetGB) {
    $refusals.Add(('DOCTOR-RAM-SHORT: {0:N1} GB available after foreign containers, budget is {1} GB; stop other stacks first' -f $availableGB, $BudgetGB))
}

# Verdict -----------------------------------------------------------------------------------
Write-Host ''
if ($refusals.Count -gt 0) {
    $refusals | ForEach-Object { Write-Host $_ -ForegroundColor Red }
    exit 1
}
Write-Host 'doctor: no port or memory conflicts' -ForegroundColor Green
exit 0
