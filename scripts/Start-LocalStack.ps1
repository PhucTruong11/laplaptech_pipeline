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
$pausedDags = @()
$updateStarted = $false
$updateCompleted = $false
try {
# Drain the shared platform before updating its control plane and resource policy.
# Checking active runs without pausing leaves a race with the next scheduled run.
$active = Invoke-Docker ps --filter 'name=^airflow-platform-metadata-1$' --format '{{.Names}}'
if ($active -eq 'airflow-platform-metadata-1') {
    $dagStates = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT coalesce(json_agg(json_build_object('dag_id',dag_id,'is_paused',is_paused)),'[]') FROM dag;"
    foreach ($dag in ($dagStates | ConvertFrom-Json)) {
        if (-not $dag.is_paused) {
            Invoke-Docker exec airflow-platform-airflow-scheduler-1 airflow dags pause $dag.dag_id
            $pausedDags += $dag.dag_id
        }
    }
    if ($pausedDags.Count) { Start-Sleep -Seconds 2 }
    $runs = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT count(*) FROM dag_run WHERE state IN ('running','queued');"
    if ([int]$runs -gt 0) { throw 'Wait for queued/running platform runs to finish before bootstrap/config refresh.' }
}
$runtimeContainers = Invoke-Docker ps --filter 'label=com.airflow.resource-policy=v1' --format '{{.Names}}'
$legacyRuntimeContainers = Invoke-Docker ps --filter 'label=com.laplaptech.project=laplaptech' --format '{{.Names}}'
if ($runtimeContainers -or $legacyRuntimeContainers) { throw 'A task container is still running. Finish or investigate that task before refreshing config.' }
$updateStarted = $true
New-Item -ItemType Directory -Path $PlatformDir -Force | Out-Null
if (Test-Path -LiteralPath (Join-Path $PlatformDir '.env')) {
    $backupDir = Join-Path $PlatformDir ('backups/bootstrap-' + (Get-Date -Format 'yyyyMMddTHHmmssfff'))
    New-Item -ItemType Directory -Path $backupDir | Out-Null
    ConvertTo-Json -InputObject ([object[]]$pausedDags) | Set-Content -LiteralPath (Join-Path $backupDir 'previously-unpaused-dags.json') -Encoding utf8
    foreach ($name in @('.env', 'compose.yaml', 'compose.laplaptech.yaml', 'README.md', 'resources.json')) {
        $existing = Join-Path $PlatformDir $name
        if (Test-Path -LiteralPath $existing) { Copy-Item -LiteralPath $existing -Destination $backupDir }
    }
}
$template = Join-Path $projectDir 'infra/airflow-platform'
foreach ($name in @('compose.yaml', 'compose.laplaptech.yaml', 'Dockerfile', '.dockerignore', '.gitignore', '.env.example', 'Initialize-LocalEnv.ps1', 'Configure-Resources.ps1', 'check_resources.py')) {
    Copy-Item -LiteralPath (Join-Path $template $name) -Destination $PlatformDir -Force
}
# Repo README links are relative to infra/airflow-platform; copying them into a
# sibling workspace breaks those links. Generate a workspace-specific entry point.
$workspaceReadme = @(
    '# Airflow operational workspace',
    '',
    ('Source repository: ' + $projectDir),
    ('Runbook: ' + (Join-Path $projectDir 'docs/guides/docker.md')),
    ('Guide index: ' + (Join-Path $projectDir 'README.md')),
    ('Shared resources and project onboarding: ' + (Join-Path $projectDir 'docs/guides/resources.md')),
    'Read AGENTS.md here before connecting a new project. Agents in other repositories must be explicitly directed to it.',
    '',
    'Use both Compose files from this workspace; dashboard has its own profile:',
    '',
    '```powershell',
    'docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml ps',
    'docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d',
    'docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard',
    '```',
    '',
    'Keep platform .env keys with metadata backups. Edit pipeline source in the repository.'
)
[IO.File]::WriteAllLines((Join-Path $PlatformDir 'README.md'), $workspaceReadme, [Text.UTF8Encoding]::new($false))
$integrationFile = Join-Path $PlatformDir 'AGENTS.md'
# Create a discoverable local contract; never overwrite existing owner instructions.
if (-not (Test-Path -LiteralPath $integrationFile)) {
    $integrationNotice = @(
        '# Shared local data platform',
        '',
        ('Platform workspace: ' + $PlatformDir),
        ('Canonical onboarding and resource runbook: ' + (Join-Path $projectDir 'docs/guides/resources.md')),
        '',
        '- Reuse this Airflow control plane and warehouse PostgreSQL for trusted local projects.',
        '- Airflow metadata is separate; never store project data in metadata/airflow_meta.',
        '- Warehouse Docker DNS: warehouse-laplaptech:5432 on network laplaptech-lab.',
        '- Warehouse host endpoint: 127.0.0.1; read WAREHOUSE_HOST_PORT from platform .env (default 55434).',
        '- Give each project a unique database and non-superuser login; do not reuse the laplaptech admin login.',
        '- Keep each project runtime, DAG IDs and config/artifacts separate; no credentials in Git or this file.',
        '- Register task memory/CPU in resources.json and use BudgetDockerOperator with the shared platform_memory pool.',
        '- Register external Compose services in resources.json; optional services count toward the host budget while running.',
        '- Drain all platform runs before changing shared services/policy; preserve other project registrations and keys.',
        '- This file applies to this workspace. New repositories need their own AGENTS.md/reference pointing here.'
    )
    [IO.File]::WriteAllLines($integrationFile, $integrationNotice, [Text.UTF8Encoding]::new($false))
}
$dagDir = Join-Path $PlatformDir 'dags'
New-Item -ItemType Directory -Path (Join-Path $dagDir 'laplaptech') -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $template 'dags/platform_smoke_test.py') -Destination $dagDir -Force
foreach ($name in @('platform_resource_policy.py', 'budget_docker.py')) {
    Copy-Item -LiteralPath (Join-Path $template ('dags/' + $name)) -Destination $dagDir -Force
}
# Preserve registrations and budgets of other projects on an existing platform.
if (-not (Test-Path -LiteralPath (Join-Path $PlatformDir 'resources.json'))) {
    Copy-Item -LiteralPath (Join-Path $template 'resources.json') -Destination $PlatformDir
}
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
& (Join-Path $PlatformDir 'Configure-Resources.ps1') -PlatformDir $PlatformDir
$resourcePolicy = Get-Content -LiteralPath (Join-Path $PlatformDir 'resources.json') -Raw | ConvertFrom-Json
$dbtThreads = $resourcePolicy.projects.laplaptech.dbt_threads
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
config['DBT_THREADS']=os.environ['DBT_THREADS']
def quote(v): return "'"+v.replace('\\','\\\\').replace("'","\\'")+"'"
directory=Path('/run/laplaptech'); directory.mkdir(parents=True,exist_ok=True)
path=directory/'pipeline.env'
path.write_text('\n'.join(k+'='+quote(v) for k,v in config.items())+'\n')
os.chown(path,10001,10001); path.chmod(0o400)
os.chown('/artifacts',10001,10001)
print('Local target configured; source credentials were not printed.')
'@
Invoke-Docker run --rm --user 0:0 --env "DBT_THREADS=$dbtThreads" --entrypoint python --mount "type=bind,source=$SourceEnvFile,target=/import.env,readonly" --mount 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech' --mount 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts' laplaptech-runtime:local-v1 -c $importCode
Invoke-Docker @compose stop dashboard
Invoke-Docker @compose up -d
Invoke-Docker @compose exec -T airflow-scheduler airflow pools set $resourcePolicy.airflow.pool $resourcePolicy.airflow.pool_slots 'Shared RAM admission: slots weighted by task memory'
$updateCompleted = $true
}
finally {
    if ($pausedDags.Count -and ($updateCompleted -or -not $updateStarted)) {
        foreach ($dagId in $pausedDags) { Invoke-Docker exec airflow-platform-airflow-scheduler-1 airflow dags unpause $dagId }
    }
    elseif ($pausedDags.Count) {
        Write-Warning 'Update failed. Platform DAGs remain paused; fix the error and verify the shared services before restoring their saved schedules.'
    }
}
Write-Host 'Stack started. Airflow: http://localhost:8080. Dashboard is stopped until explicitly started.'
Write-Host 'The dashboard needs completed Gold tables. Check the shared resource budget before starting it alongside other project services.'
Write-Host "Airflow login: read $platformEnv locally. The source DAG is mounted directly from this repository."
