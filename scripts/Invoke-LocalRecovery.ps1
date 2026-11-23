[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Start,
    [Parameter(Mandatory)][string]$End,
    [string]$PlatformDir = (Join-Path (Split-Path $PSScriptRoot -Parent | Split-Path -Parent) 'airflow-platform'),
    [string]$Python = 'python'
)

$ErrorActionPreference = 'Stop'
$PlatformDir = (Resolve-Path -LiteralPath $PlatformDir).Path

function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE)." }
}

$scheduler = Invoke-Docker ps --filter 'name=^airflow-platform-airflow-scheduler-1$' --format '{{.Names}}'
if ($scheduler) { throw 'Stop the shared scheduler after pausing LapLapTech and draining runs.' }
$activeTasks = Invoke-Docker ps --filter 'label=com.airflow.resource-policy=v1' --format '{{.Names}}'
$laplaptechJobs = Invoke-Docker ps --filter 'label=com.laplaptech.project=laplaptech' --format '{{.Names}}'
if ($activeTasks -or $laplaptechJobs) { throw 'Wait for active task containers or LapLapTech jobs first.' }

$readyQuery = @'
SELECT is_paused AND NOT EXISTS (SELECT 1 FROM dag_run WHERE state IN ('queued','running'))
FROM dag WHERE dag_id='laplaptech_pipeline';
'@
$ready = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -v ON_ERROR_STOP=1 -Atc $readyQuery
if ($ready -ne 't') { throw 'LapLapTech must be paused and all Airflow runs drained before recovery.' }
$writersQuery = @'
SELECT count(*) FROM pg_stat_activity WHERE datname='laplaptech_pipeline_dev'
AND backend_type='client backend' AND usename<>'laplaptech_dashboard'
AND (state='active' OR xact_start IS NOT NULL);
'@
$writers = Invoke-Docker exec airflow-platform-warehouse-laplaptech-1 psql -U laplaptech -d postgres -v ON_ERROR_STOP=1 -Atc $writersQuery
if ($writers -ne '0') { throw 'Wait for active warehouse clients before starting recovery.' }

$policy = Get-Content -LiteralPath (Join-Path $PlatformDir 'resources.json') -Raw | ConvertFrom-Json
$stages = @('ingest', 'freshness', 'build', 'reconcile') | ForEach-Object { $policy.projects.laplaptech.tasks.$_ }
if (@($stages | Where-Object { $_.memory_mib -gt 0 -and $_.cpus -gt 0 }).Count -ne 4) {
    throw 'LapLapTech resource policy must register all four stages.'
}
$memory = ($stages.memory_mib | Measure-Object -Maximum).Maximum
$cpu = ($stages.cpus | Measure-Object -Maximum).Maximum
$auditText = & $Python (Join-Path $PlatformDir 'check_resources.py') --platform-dir $PlatformDir
$auditCode = $LASTEXITCODE
$audit = $auditText | ConvertFrom-Json
if ($auditCode -ne 0 -or $audit.status -ne 'success' -or @($audit.unmanaged_running).Count) {
    throw 'Resource audit must pass with all running containers registered before recovery.'
}
$required = $audit.budget.effective_required_host_mib + $memory
if ($required -gt $policy.host.memory_mib -or
    ($required - $policy.host.engine_overhead_mib) -gt $audit.engine.memory_mib) {
    throw 'Recovery does not fit the current Docker memory budget.'
}

$dockerArgs = @(
    'run', '--rm', '--name', 'laplaptech-recovery',
    '--memory', "$($memory)m", '--memory-swap', "$($memory)m",
    '--cpus', $cpu.ToString([Globalization.CultureInfo]::InvariantCulture), '--pids-limit', '256',
    '--label', 'com.laplaptech.project=laplaptech',
    '--log-driver', 'json-file', '--log-opt', 'max-size=10m', '--log-opt', 'max-file=3',
    '--network', 'laplaptech-lab',
    '--mount', 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech,readonly',
    '--mount', 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts',
    '--entrypoint', 'python', 'laplaptech-runtime:local-v1',
    '-m', 'scripts.recover_pipeline', '--start', $Start, '--end', $End
)
Invoke-Docker @dockerArgs
