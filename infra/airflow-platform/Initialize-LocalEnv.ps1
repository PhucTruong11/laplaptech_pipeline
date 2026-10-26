[CmdletBinding()]
param(
    [ValidateRange(1024, 65535)]
    [int]$UiPort = 8080,
    [string]$LapLapTechProjectDir = (Join-Path (Split-Path -Parent $PSScriptRoot) 'laplaptech_pipeline'),
    [int]$WarehousePort = 55434,
    [int]$DashboardPort = 8501
)

$ErrorActionPreference = 'Stop'
$envPath = Join-Path $PSScriptRoot '.env'
if (Test-Path -LiteralPath $envPath) {
    throw '.env already exists. Keep the existing passwords and encryption keys.'
}

function New-RandomBytes([int]$Count) {
    $bytes = New-Object byte[] $Count
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
    }
    finally {
        $rng.Dispose()
    }
    return ,$bytes
}

function New-RandomHex([int]$Count) {
    return [BitConverter]::ToString((New-RandomBytes $Count)).Replace('-', '').ToLowerInvariant()
}

$fernetKey = [Convert]::ToBase64String((New-RandomBytes 32)).Replace('+', '-').Replace('/', '_')
$content = @"
# Generated locally. Keep this file with your metadata database backup.
AIRFLOW_UID=50000
AIRFLOW_UI_PORT=$UiPort
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=$(New-RandomHex 24)
METADATA_PASSWORD=$(New-RandomHex 24)
AIRFLOW_FERNET_KEY=$fernetKey
AIRFLOW_API_SECRET_KEY=$(New-RandomHex 32)
AIRFLOW_JWT_SECRET=$(New-RandomHex 32)
WAREHOUSE_PASSWORD=$(New-RandomHex 32)
LAPLAPTECH_PIPELINE_PASSWORD=$(New-RandomHex 32)
LAPLAPTECH_DASHBOARD_PASSWORD=$(New-RandomHex 32)
LAPLAPTECH_PROJECT_DIR=$($LapLapTechProjectDir.Replace('\', '/'))
WAREHOUSE_HOST_PORT=$WarehousePort
DASHBOARD_PORT=$DashboardPort
DOCKER_SOCKET_GID=0
"@

# CreateNew prevents accidentally replacing keys, even if two invocations overlap.
$stream = [System.IO.File]::Open($envPath, [System.IO.FileMode]::CreateNew)
try {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($content + [Environment]::NewLine)
    $stream.Write($bytes, 0, $bytes.Length)
}
finally {
    $stream.Dispose()
}
Write-Host 'Created .env. Read AIRFLOW_ADMIN_USERNAME/PASSWORD locally to sign in.'
Write-Host 'Next: docker compose --env-file .env config --quiet'
