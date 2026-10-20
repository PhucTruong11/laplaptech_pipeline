[CmdletBinding()]
param(
    [string]$PlatformDir = (Join-Path (Split-Path $PSScriptRoot -Parent | Split-Path -Parent) 'airflow-platform'),
    [string]$SourceEnvFile = (Join-Path (Split-Path $PSScriptRoot -Parent) '.env'),
    [ValidateRange(1024, 65535)][int]$WarehousePort = 55434,
    [ValidateRange(1024, 65535)][int]$DashboardPort = 8501
)
$ErrorActionPreference = 'Stop'
$projectDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$SourceEnvFile = (Resolve-Path -LiteralPath $SourceEnvFile).Path
$PlatformDir = [IO.Path]::GetFullPath($PlatformDir)
if ($PlatformDir -eq $projectDir -or $PlatformDir.StartsWith($projectDir + [IO.Path]::DirectorySeparatorChar)) {
    throw 'Use a separate sibling airflow-platform directory for operational keys and metadata backups.'
}
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE)." }
}
Invoke-Docker info --format '{{.OSType}}'
$pausedForUpdate = $false
$updateStarted = $false
$updateCompleted = $false
try {
# Pause only this project while updating mutable images and mounted config.
# Checking active runs without pausing leaves a race with the next scheduled run.
$active = Invoke-Docker ps --filter 'name=^airflow-platform-metadata-1$' --format '{{.Names}}'
if ($active -eq 'airflow-platform-metadata-1') {
    $pauseState = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT is_paused FROM dag WHERE dag_id='laplaptech_pipeline';"
    if ($pauseState -eq 'f') {
        $pausedForUpdate = $true
        Invoke-Docker exec airflow-platform-airflow-scheduler-1 airflow dags pause laplaptech_pipeline
        Start-Sleep -Seconds 2 # Allow an in-flight scheduler transaction to settle.
    }
    $runs = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT count(*) FROM dag_run WHERE dag_id='laplaptech_pipeline' AND state IN ('running','queued');"
    if ([int]$runs -gt 0) { throw 'Wait for queued/running LaplapTech runs to finish before bootstrap/config refresh.' }
}
$runtimeContainers = Invoke-Docker ps --filter 'label=com.laplaptech.project=laplaptech' --format '{{.Names}}'
if ($runtimeContainers) { throw 'A LaplapTech task container is still running. Finish or investigate that task before refreshing config.' }
$updateStarted = $true
New-Item -ItemType Directory -Path $PlatformDir -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $PlatformDir '.env')) {
    $backupDir = Join-Path $PlatformDir ('backups/bootstrap-' + (Get-Date -Format 'yyyyMMddTHHmmssfff'))
    New-Item -ItemType Directory -Path $backupDir | Out-Null
    foreach ($name in @('.env', 'compose.yaml', 'compose.laplaptech.yaml', 'README.md')) {
        $existing = Join-Path $PlatformDir $name
        if (Test-Path -LiteralPath $existing) { Copy-Item -LiteralPath $existing -Destination $backupDir }
    }
}
$template = Join-Path $projectDir 'infra/airflow-platform'
foreach ($name in @('compose.yaml', 'compose.laplaptech.yaml', 'Dockerfile', '.dockerignore', '.gitignore', '.env.example', 'Initialize-LocalEnv.ps1')) {
    Copy-Item -LiteralPath (Join-Path $template $name) -Destination $PlatformDir -Force
}
# Repo README links are relative to infra/airflow-platform; copying them into a
# sibling workspace breaks those links. Generate a workspace-specific entry point.
$workspaceReadme = @(
    '# Airflow operational workspace',
    '',
    ('Source repository: ' + $projectDir),
    ('Runbook: ' + (Join-Path $projectDir 'docs/guides/docker.md')),
    ('Guide index: ' + (Join-Path $projectDir 'docs/README.md')),
    '',
    'Use both Compose files from this workspace:',
    '',
    '```powershell',
    'docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml ps',
    'docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d',
    '```',
    '',
    'Keep platform .env keys with metadata backups. Edit pipeline source in the repository.'
)
[IO.File]::WriteAllLines((Join-Path $PlatformDir 'README.md'), $workspaceReadme, [Text.UTF8Encoding]::new($false))
$dagDir = Join-Path $PlatformDir 'dags'
New-Item -ItemType Directory -Path (Join-Path $dagDir 'laplaptech') -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $template 'dags/platform_smoke_test.py') -Destination $dagDir -Force
$platformEnv = Join-Path $PlatformDir '.env'
if (-not (Test-Path -LiteralPath $platformEnv)) { & (Join-Path $PlatformDir 'Initialize-LocalEnv.ps1') }
$content = [IO.File]::ReadAllText($platformEnv)
$settings = @{
    LAPLAPTECH_PROJECT_DIR = $projectDir.Replace('\', '/')
    WAREHOUSE_HOST_PORT = "$WarehousePort"
    DASHBOARD_PORT = "$DashboardPort"
    DOCKER_SOCKET_GID = '0' # Docker Desktop Linux socket; set actual GID on native Linux.
}
foreach ($key in $settings.Keys) {
    $line = $key + '=' + $settings[$key]
    if ($content -match "(?m)^$key=") { $content = [regex]::Replace($content, "(?m)^$key=.*$", $line) }
    else { $content += "`n$line`n" }
}
[IO.File]::WriteAllText($platformEnv, $content, [Text.UTF8Encoding]::new($false))
$compose = @('compose', '--project-directory', $PlatformDir, '--env-file', $platformEnv,
    '-f', (Join-Path $PlatformDir 'compose.yaml'), '-f', (Join-Path $PlatformDir 'compose.laplaptech.yaml'))
Invoke-Docker @compose config --quiet
Invoke-Docker build -f (Join-Path $projectDir 'infra/laplaptech/runtime.Dockerfile') -t laplaptech-runtime:local-v1 $projectDir
Invoke-Docker @compose build airflow-scheduler dashboard
foreach ($volume in @('laplaptech-lab-config', 'laplaptech-lab-artifacts')) { Invoke-Docker volume create $volume }
# Import only ClickHouse/ingestion options. Never copy a source file's PG/Neon target.
$importCode = @'
import os
from pathlib import Path
from dotenv import dotenv_values
source=dotenv_values('/import.env',interpolate=False)
password=source.get('CLICKHOUSE_PASSWORD')
if not password or password in ('your_password_here','REPLACE_WITH_PUBLIC_DATASET_PASSWORD'):
    raise SystemExit('Fill CLICKHOUSE_PASSWORD in the source env first')
config={k:v for k,v in source.items() if k.startswith(('CLICKHOUSE_','INGESTION_')) and v is not None}
config.update(POSTGRES_HOST='warehouse-laplaptech',POSTGRES_PORT='5432',POSTGRES_USER='laplaptech',
    POSTGRES_PASSWORD='laplaptech_lab_only',POSTGRES_DATABASE='laplaptech_pipeline_dev',
    POSTGRES_SSLMODE='prefer',POSTGRES_CREATE_DATABASE='false')
def quote(v): return "'"+v.replace('\\','\\\\').replace("'","\\'")+"'"
directory=Path('/run/laplaptech'); directory.mkdir(parents=True,exist_ok=True)
path=directory/'pipeline.env'
path.write_text('\n'.join(k+'='+quote(v) for k,v in config.items())+'\n')
os.chown(path,10001,10001); path.chmod(0o400)
os.chown('/artifacts',10001,10001)
print('Local target configured; source credentials were not printed.')
'@
Invoke-Docker run --rm --user 0:0 --entrypoint python --mount "type=bind,source=$SourceEnvFile,target=/import.env,readonly" --mount 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech' --mount 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts' laplaptech-runtime:local-v1 -c $importCode
Invoke-Docker @compose up -d
Invoke-Docker @compose exec -T airflow-scheduler airflow pools set laplaptech_warehouse 1 'Single local warehouse writer'
$updateCompleted = $true
}
finally {
    if ($pausedForUpdate -and ($updateCompleted -or -not $updateStarted)) {
        Invoke-Docker exec airflow-platform-airflow-scheduler-1 airflow dags unpause laplaptech_pipeline
    }
    elseif ($pausedForUpdate) {
        Write-Warning 'Update failed. LaplapTech remains paused; fix the error and rerun bootstrap, then unpause after verification. Other project DAGs were not paused.'
    }
}
Write-Host "Stack started. Airflow: http://localhost:8080 ; dashboard: http://localhost:$DashboardPort"
Write-Host 'Dashboard becomes healthy after the first successful build. In Airflow, unpause laplaptech_pipeline and trigger with full_refresh=true.'
Write-Host "Airflow login: read $platformEnv locally. The source DAG is mounted directly from this repository."
