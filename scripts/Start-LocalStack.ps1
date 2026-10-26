[CmdletBinding()]
param(
    [string]$PlatformDir = (Join-Path (Split-Path $PSScriptRoot -Parent | Split-Path -Parent) 'airflow-platform'),
    [string]$SourceEnvFile = (Join-Path (Split-Path $PSScriptRoot -Parent) '.env'),
    [switch]$InitializePlatform,
    [ValidateRange(1024, 65535)][int]$WarehousePort = 55434,
    [ValidateRange(1024, 65535)][int]$DashboardPort = 8501
)
$ErrorActionPreference = 'Stop'
$projectDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$SourceEnvFile = (Resolve-Path -LiteralPath $SourceEnvFile).Path
$PlatformDir = [IO.Path]::GetFullPath($PlatformDir)
if ($PlatformDir -eq $projectDir -or $PlatformDir.StartsWith($projectDir + [IO.Path]::DirectorySeparatorChar)) {
    throw 'Use a separate platform directory outside the project repository.'
}
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE)." }
}
function Read-Setting([string]$Content, [string]$Key) {
    $match = [regex]::Match($Content, '(?m)^' + [regex]::Escape($Key) + '=(.*)\r?$')
    if (-not $match.Success) { throw "Missing $Key in platform .env; configure the platform first." }
    return $match.Groups[1].Value.Trim().Trim('"').Trim("'")
}
function Pause-PlatformDags {
    $dagStates = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT coalesce(json_agg(json_build_object('dag_id',dag_id,'is_paused',is_paused)),'[]') FROM dag;"
    foreach ($dag in ($dagStates | ConvertFrom-Json)) {
        if (-not $dag.is_paused) {
            # Compose run works even when metadata alone is running. It does not start a scheduler.
            Invoke-Docker @compose run --rm --no-deps -T airflow-scheduler airflow dags pause $dag.dag_id
            $script:pausedDags += $dag.dag_id
            if ($script:stateDir) {
                ConvertTo-Json -InputObject ([object[]]$script:pausedDags) | Set-Content -LiteralPath (Join-Path $script:stateDir 'previously-unpaused-dags.json') -Encoding utf8
            }
        }
    }
    $runs = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT count(*) FROM dag_run WHERE state IN ('running','queued');"
    if ([int]$runs -gt 0) { throw 'Wait for queued/running platform runs to finish before bootstrap/config refresh.' }
}
$platformEnv = Join-Path $PlatformDir '.env'
$newPlatform = $InitializePlatform.IsPresent
if ($newPlatform) {
    if ((Test-Path -LiteralPath $PlatformDir) -and @(Get-ChildItem -LiteralPath $PlatformDir -Force).Count) {
        throw '-InitializePlatform requires a new/empty directory; existing platform files are never replaced.'
    }
} else {
    foreach ($file in @('.env', 'compose.yaml', 'compose.laplaptech.yaml', 'resources.json', 'Configure-Resources.ps1', 'dags/budget_docker.py')) {
        if (-not (Test-Path -LiteralPath (Join-Path $PlatformDir $file) -PathType Leaf)) {
            throw "Existing platform is missing $file. For a NEW empty directory only, use -InitializePlatform."
        }
    }
    $initialEnv = [IO.File]::ReadAllText($platformEnv)
    $registeredPath = [IO.Path]::GetFullPath((Read-Setting $initialEnv 'LAPLAPTECH_PROJECT_DIR'))
    if ($registeredPath.TrimEnd('\','/') -ne $projectDir.TrimEnd('\','/')) { throw 'Platform mounts a different LapLapTech checkout; review its integration before continuing.' }
    foreach ($entry in @(@('WarehousePort','WAREHOUSE_HOST_PORT'), @('DashboardPort','DASHBOARD_PORT'))) {
        if ($PSBoundParameters.ContainsKey($entry[0]) -and (Read-Setting $initialEnv $entry[1]) -ne [string](Get-Variable $entry[0] -ValueOnly)) {
            throw 'Existing platform ports are preserved. Change them in the platform workspace deliberately.'
        }
    }
    Read-Setting $initialEnv 'WAREHOUSE_PASSWORD' | Out-Null
}
Invoke-Docker info --format '{{.OSType}}'
if ($newPlatform) {
    $existingStack = Invoke-Docker ps -a --filter 'label=com.docker.compose.project=airflow-platform' --format '{{.Names}}'
    if ($existingStack) { throw 'An airflow-platform stack already exists. Use its registered workspace; do not initialize a second stack with the same identity.' }
}
$compose = @('compose', '--project-directory', $PlatformDir, '--env-file', $platformEnv,
    '-f', (Join-Path $PlatformDir 'compose.yaml'), '-f', (Join-Path $PlatformDir 'compose.laplaptech.yaml'))
$pausedDags = @()
$stateDir = $null
$updateStarted = $false
$updateCompleted = $false
try {
    $metadataRunning = Invoke-Docker ps --filter 'name=^airflow-platform-metadata-1$' --format '{{.Names}}'
    if ($metadataRunning -eq 'airflow-platform-metadata-1') { Pause-PlatformDags }
    $tasks = Invoke-Docker ps --filter 'label=com.airflow.resource-policy=v1' --format '{{.Names}}'
    $legacyTasks = Invoke-Docker ps --filter 'label=com.laplaptech.project=laplaptech' --format '{{.Names}}'
    if ($tasks -or $legacyTasks) { throw 'A task container is still running; finish it before refreshing project config.' }
    if (-not $newPlatform) {
        & (Join-Path $PlatformDir 'Configure-Resources.ps1') -PlatformDir $PlatformDir -CheckOnly
        if ($LASTEXITCODE -ne 0) { throw 'Resource policy validation failed.' }
    }
    $updateStarted = $true
    $stateDir = Join-Path $PlatformDir ('backups/project-update-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
    # A fresh directory must stay empty until its template is installed below.
    if (-not $newPlatform) {
        New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
        ConvertTo-Json -InputObject ([object[]]$pausedDags) | Set-Content -LiteralPath (Join-Path $stateDir 'previously-unpaused-dags.json') -Encoding utf8
    }
    if ($newPlatform) {
        New-Item -ItemType Directory -Path $PlatformDir -Force | Out-Null
        # Initial installation only. No template copies in the existing-platform branch.
        Copy-Item -Path (Join-Path $projectDir 'infra/airflow-platform/*') -Destination $PlatformDir -Recurse -Force
        foreach ($hidden in @('.dockerignore','.gitignore','.env.example')) {
            Copy-Item -LiteralPath (Join-Path $projectDir ('infra/airflow-platform/'+$hidden)) -Destination $PlatformDir
        }
        & (Join-Path $PlatformDir 'Initialize-LocalEnv.ps1') -LapLapTechProjectDir $projectDir -WarehousePort $WarehousePort -DashboardPort $DashboardPort
        & (Join-Path $PlatformDir 'Configure-Resources.ps1') -PlatformDir $PlatformDir
        [IO.File]::WriteAllText((Join-Path $PlatformDir 'README.md'), "Standalone LapLapTech platform. Runbook: $projectDir/docs/guides/docker.md`nTemplate installation is initial-only; later bootstrap preserves this workspace.`n", [Text.UTF8Encoding]::new($false))
        [IO.File]::WriteAllText((Join-Path $PlatformDir 'AGENTS.md'), "# Standalone LapLapTech platform`nRead $projectDir/docs/architecture/shared_platform.md and resources.json before changes. Metadata is separate; project login is laplaptech_pipeline and reader is laplaptech_dashboard. Preserve other projects and existing keys. Do not use -InitializePlatform on this directory.`n", [Text.UTF8Encoding]::new($false))
    }
    $content = [IO.File]::ReadAllText($platformEnv)
    $missing = @('LAPLAPTECH_PIPELINE_PASSWORD','LAPLAPTECH_DASHBOARD_PASSWORD') | Where-Object { $content -notmatch ('(?m)^'+$_+'=.+') }
    if ($missing.Count) {
        $backupDir = Join-Path $PlatformDir ('backups/laplaptech-access-'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
        New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
        Copy-Item -LiteralPath $platformEnv -Destination (Join-Path $backupDir '.env')
        foreach ($key in $missing) {
            $bytes = New-Object byte[] 32
            $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
            try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
            $line = $key+'='+[BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
            if ($content -match ('(?m)^'+$key+'=')) { $content = [regex]::Replace($content, '(?m)^'+$key+'=.*$', $line) }
            else { $content += "`n$line`n" }
        }
        [IO.File]::WriteAllText($platformEnv, $content, [Text.UTF8Encoding]::new($false))
    }
    $policy = Get-Content -LiteralPath (Join-Path $PlatformDir 'resources.json') -Raw | ConvertFrom-Json
    if (-not $policy.projects.laplaptech) { throw 'Register laplaptech in the existing platform first.' }
    Invoke-Docker @compose config --quiet
    Invoke-Docker build -f (Join-Path $projectDir 'infra/laplaptech/runtime.Dockerfile') -t laplaptech-runtime:local-v1 $projectDir
    Invoke-Docker build -f (Join-Path $projectDir 'infra/laplaptech/dashboard.Dockerfile') -t laplaptech-dashboard:local-v1 $projectDir
    if ($newPlatform) { Invoke-Docker @compose build airflow-scheduler }
    Invoke-Docker @compose up -d --no-build --no-deps --wait warehouse-laplaptech
    foreach ($volume in @('laplaptech-lab-config','laplaptech-lab-artifacts')) { Invoke-Docker volume create $volume }
    Invoke-Docker run --rm --user 0:0 --network laplaptech-lab --memory 512m --memory-swap 512m --cpus 1 --pids-limit 256 --env "DBT_THREADS=$($policy.projects.laplaptech.dbt_threads)" --entrypoint python --mount "type=bind,source=$platformEnv,target=/platform.env,readonly" --mount "type=bind,source=$SourceEnvFile,target=/import.env,readonly" --mount 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech' --mount 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts' laplaptech-runtime:local-v1 -m scripts.provision_access
    Invoke-Docker run --rm --network laplaptech-lab --memory 512m --memory-swap 512m --cpus 1 --pids-limit 256 --mount 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech,readonly' --mount 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts' laplaptech-runtime:local-v1 preflight
    if ($metadataRunning -ne 'airflow-platform-metadata-1') {
        Invoke-Docker @compose up -d --no-build --no-deps --wait metadata
        if (-not $newPlatform) { Pause-PlatformDags }
    }
    Invoke-Docker @compose up -d --no-build --wait
    $dashboardRunning = Invoke-Docker ps --filter 'name=^airflow-platform-dashboard-1$' --format '{{.Names}}'
    if ($dashboardRunning -eq 'airflow-platform-dashboard-1') {
        Invoke-Docker @compose --profile dashboard up -d --no-build --no-deps --wait dashboard
    }
    Invoke-Docker @compose exec -T airflow-scheduler airflow pools set $policy.airflow.pool $policy.airflow.pool_slots 'Shared RAM admission from platform policy'
    $updateCompleted = $true
} finally {
    if ($pausedDags.Count -and ($updateCompleted -or -not $updateStarted)) {
        foreach ($id in $pausedDags) { Invoke-Docker @compose run --rm --no-deps -T airflow-scheduler airflow dags unpause $id }
    } elseif ($pausedDags.Count) {
        Write-Warning 'Update failed. DAGs remain paused; verify config and restore only their previous enabled state.'
    }
}
Write-Host 'Project runtime/config ready. Shared platform files were preserved; dashboard uses its existing on/off state.'
Write-Host 'Pipeline/dashboard credentials are separate private values in platform .env; admin credentials are unchanged.'
