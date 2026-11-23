[CmdletBinding()]
param([string]$Image = 'laplaptech-runtime:local-v1')

$ErrorActionPreference = 'Stop'
$project = Split-Path $PSScriptRoot -Parent
$local = Join-Path $project 'infra/laplaptech/dags/.cosmos'
New-Item -ItemType Directory -Path $local -Force | Out-Null
$pending = Join-Path $local ('.manifest-' + [Guid]::NewGuid() + '.json')

function Invoke-DockerChecked {
    docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker failed (exit $LASTEXITCODE)." }
}

$container = Invoke-DockerChecked create --network none $Image
try {
    Invoke-DockerChecked cp "$($container):/app/cosmos/manifest.json" $pending
    $content = Get-Content -LiteralPath $pending -Raw | ConvertFrom-Json -AsHashtable
    $models = @($content.nodes.Values | Where-Object resource_type -eq 'model').Count
    $tests = @($content.nodes.Values | Where-Object resource_type -eq 'test').Count
    if ($models -lt 1 -or $tests -lt 1) { throw 'Manifest must contain enabled models and data tests.' }
    $digest = (Get-FileHash -LiteralPath $pending -Algorithm SHA256).Hash.ToLowerInvariant()
    $releaseImage = 'laplaptech-runtime:cosmos-' + $digest.Substring(0, 32)
    $existing = @(Invoke-DockerChecked image ls --filter "reference=$releaseImage" --format '{{.Repository}}:{{.Tag}}')
    if ($existing.Count -eq 0) {
        Invoke-DockerChecked tag $Image $releaseImage
    } else {
        $existingContainer = Invoke-DockerChecked create --network none $releaseImage
        try {
            $format = '{{.ImageManifestDescriptor.Digest}}'
            if ((Invoke-DockerChecked inspect $existingContainer --format $format) -ne
                (Invoke-DockerChecked inspect $container --format $format)) {
                throw 'This Cosmos manifest already points to a different image. Build a fresh release.'
            }
        } finally {
            Invoke-DockerChecked rm $existingContainer | Out-Null
        }
    }
    [IO.File]::Move($pending, (Join-Path $local 'manifest.json'), $true)
    "Cosmos release $digest | $models models | $tests tests | $releaseImage"
} finally {
    Invoke-DockerChecked rm $container | Out-Null
    if (Test-Path -LiteralPath $pending) { Remove-Item -LiteralPath $pending }
}
