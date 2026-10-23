[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('On', 'Off', 'Status')]
    [string]$State,
    [string]$PlatformDir = (Join-Path (Split-Path $PSScriptRoot -Parent | Split-Path -Parent) 'airflow-platform')
)

$ErrorActionPreference = 'Stop'
$PlatformDir = [IO.Path]::GetFullPath($PlatformDir)
$envFile = Join-Path $PlatformDir '.env'
foreach ($name in @('.env', 'compose.yaml', 'compose.laplaptech.yaml')) {
    if (-not (Test-Path -LiteralPath (Join-Path $PlatformDir $name))) {
        throw "Missing $name in $PlatformDir. Run scripts/Start-LocalStack.ps1 to bootstrap first."
    }
}

$compose = @('compose', '--project-directory', $PlatformDir, '--env-file', $envFile,
    '-f', (Join-Path $PlatformDir 'compose.yaml'),
    '-f', (Join-Path $PlatformDir 'compose.laplaptech.yaml'))

function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE)." }
}

switch ($State) {
    'Status' {
        Invoke-Docker @compose --profile dashboard ps -a
        break
    }
    'On' {
        # The dashboard is intentionally excluded from the default profile.
        Invoke-Docker @compose up -d
        Write-Host 'Shared Airflow and LaplapTech warehouse are on. Dashboard stays off.'
        break
    }
    'Off' {
        $metadata = Invoke-Docker ps --filter 'name=^airflow-platform-metadata-1$' --format '{{.Names}}'
        if ($metadata -eq 'airflow-platform-metadata-1') {
            $active = Invoke-Docker exec airflow-platform-metadata-1 psql -U airflow -d airflow_meta -Atc "SELECT count(*) FROM dag_run WHERE state IN ('running','queued');"
            if ([int]$active -gt 0) {
                throw "There are $active queued/running Airflow DAG runs. Wait for them to finish before stopping the shared stack."
            }
        }
        $tasks = Invoke-Docker ps --filter 'label=com.airflow.resource-policy=v1' --format '{{.Names}}'
        $legacyTasks = Invoke-Docker ps --filter 'label=com.laplaptech.project=laplaptech' --format '{{.Names}}'
        if ($tasks -or $legacyTasks) {
            throw 'A task container is still running. Wait for it to finish before stopping the shared stack.'
        }
        Invoke-Docker @compose --profile dashboard stop -t 60
        Write-Host 'Shared Airflow and LaplapTech warehouse/dashboard are off. Named volumes and BrewLite are untouched.'
        break
    }
}
