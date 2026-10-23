[CmdletBinding()]
param(
    [string]$PlatformDir = $PSScriptRoot,
    [string]$PolicyPath = (Join-Path $PSScriptRoot 'resources.json'),
    [switch]$CheckOnly,
    [switch]$ApplyWslConfig
)
$ErrorActionPreference = 'Stop'
$policy = Get-Content -LiteralPath $PolicyPath -Raw | ConvertFrom-Json
function Assert-PositiveInteger($Value, [string]$Label) {
    if (($Value -isnot [int] -and $Value -isnot [long]) -or $Value -le 0) { throw "$Label must be a positive integer." }
}
function Assert-Limits($Item, [string]$Label) {
    Assert-PositiveInteger $Item.memory_mib "$Label.memory_mib"
    if ($Item.cpus -is [bool] -or $Item.cpus -isnot [ValueType] -or $Item.cpus -le 0 -or [double]::IsInfinity($Item.cpus) -or [double]::IsNaN($Item.cpus)) { throw "$Label.cpus must be finite and positive." }
}
if ($policy.version -ne 1) { throw 'Unsupported resource policy version.' }
foreach ($key in @('memory_mib', 'processors', 'swap_mib', 'engine_overhead_mib')) { Assert-PositiveInteger $policy.host.$key "host.$key" }
foreach ($key in @('parallelism', 'max_active_tasks_per_dag', 'pool_slots', 'slot_mib')) { Assert-PositiveInteger $policy.airflow.$key "airflow.$key" }
Assert-PositiveInteger $policy.headroom_mib 'headroom_mib'
if ($policy.airflow.pool -notmatch '^[A-Za-z0-9_]+$') { throw 'Shared pool name must use letters, digits and underscores.' }
$serviceNames = @{}
foreach ($property in $policy.services.PSObject.Properties) { $serviceNames[$property.Name] = $true }
$settings = @{
    RESOURCE_AIRFLOW_PARALLELISM = $policy.airflow.parallelism
    RESOURCE_AIRFLOW_MAX_TASKS_PER_DAG = $policy.airflow.max_active_tasks_per_dag
}
$steadyMiB = 0
function Add-ServiceSettings($Services, [string]$Prefix) {
    foreach ($property in $Services.PSObject.Properties) {
        $service = $property.Value
        Assert-Limits $service $property.Name
        $key = $Prefix + $property.Name.ToUpperInvariant().Replace('-', '_')
        $settings[$key + '_MEMORY_MIB'] = $service.memory_mib
        $settings[$key + '_CPUS'] = [string]::Format([Globalization.CultureInfo]::InvariantCulture, '{0}', $service.cpus)
        if (-not $service.transient -and -not $service.optional) { $script:steadyMiB += $service.memory_mib }
    }
}
Add-ServiceSettings $policy.services 'RESOURCE_'
foreach ($project in $policy.projects.PSObject.Properties) {
    if ($project.Name -notmatch '^[a-z][a-z0-9_]*$') { throw 'Project names must use lowercase letters, digits and underscores.' }
    $config = $project.Value
    Assert-PositiveInteger $config.dbt_threads ($project.Name + '.dbt_threads')
    if ($config.dbt_threads -gt 16) { throw 'dbt_threads must be between 1 and 16.' }
    foreach ($service in $config.services.PSObject.Properties) {
        $composeName = $service.Value.compose_service
        if ($composeName -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$' -or $serviceNames.ContainsKey($composeName)) { throw 'Every project service needs a unique compose_service name.' }
        $serviceNames[$composeName] = $true
    }
    Add-ServiceSettings $config.services ('RESOURCE_' + $project.Name.ToUpperInvariant() + '_')
    foreach ($task in $config.tasks.PSObject.Properties) {
        Assert-Limits $task.Value ($project.Name + '.' + $task.Name)
        if ([Math]::Ceiling($task.Value.memory_mib / $policy.airflow.slot_mib) -gt $policy.airflow.pool_slots) { throw ($project.Name + '.' + $task.Name + ' exceeds the shared pool capacity.') }
    }
}
if ($policy.external_services) {
    foreach ($external in $policy.external_services.PSObject.Properties) {
        $service = $external.Value
        Assert-Limits $service ('external_services.' + $external.Name)
        if ($external.Name -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$' -or
            $service.compose_project -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$' -or
            $service.compose_service -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]*$') {
            throw 'External services need valid container, Compose project and service names.'
        }
        if (-not $service.optional) { $steadyMiB += $service.memory_mib }
    }
}
$taskMiB = $policy.airflow.pool_slots * $policy.airflow.slot_mib
$requiredMiB = $steadyMiB + $taskMiB + $policy.headroom_mib + $policy.host.engine_overhead_mib
if ($requiredMiB -gt $policy.host.memory_mib) { throw "Budget $requiredMiB MiB exceeds WSL $($policy.host.memory_mib) MiB. Reduce idle services or task pool before adding a project." }
Write-Output "Resource budget: services=$steadyMiB MiB; tasks=$taskMiB MiB; headroom=$($policy.headroom_mib) MiB; VM overhead=$($policy.host.engine_overhead_mib) MiB; total=$requiredMiB/$($policy.host.memory_mib) MiB."
if ($CheckOnly) { return }
$envFile = Join-Path $PlatformDir '.env'
$content = [IO.File]::ReadAllText($envFile)
$backupDir = Join-Path $PlatformDir ('backups/resources-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
New-Item -ItemType Directory -Path $backupDir -ErrorAction Stop | Out-Null
Copy-Item -LiteralPath $envFile -Destination (Join-Path $backupDir '.env') -ErrorAction Stop
foreach ($key in $settings.Keys) {
    $line = $key + '=' + $settings[$key]
    if ($content -match "(?m)^$key=") { $content = [regex]::Replace($content, "(?m)^$key=[^\r\n]*", $line) }
    else { $content += "`n$line`n" }
}
[IO.File]::WriteAllText($envFile, $content, [Text.UTF8Encoding]::new($false))
if ($ApplyWslConfig) {
    $hardware = Get-CimInstance Win32_ComputerSystem
    if ($policy.host.memory_mib -gt ($hardware.TotalPhysicalMemory / 1MB - 6144)) { throw 'Keep at least 6 GiB for Windows/apps; the requested WSL budget is too large.' }
    if ($policy.host.processors -gt $hardware.NumberOfLogicalProcessors) { throw 'WSL processors exceeds host logical processors.' }
    $wslFile = Join-Path ([Environment]::GetFolderPath('UserProfile')) '.wslconfig'
    $wsl = ''
    if (Test-Path -LiteralPath $wslFile) {
        Copy-Item -LiteralPath $wslFile -Destination (Join-Path $backupDir 'wslconfig') -ErrorAction Stop
        $wsl = [IO.File]::ReadAllText($wslFile)
    }
    function Set-IniValue([string]$Text, [string]$Section, [string]$Key, [string]$Value) {
        $pattern = '(?ms)^\[' + [regex]::Escape($Section) + '\][^\r\n]*\r?\n(?<body>.*?)(?=^\[|\z)'
        $match = [regex]::Match($Text, $pattern)
        if (-not $match.Success) { return $Text.TrimEnd() + "`n[$Section]`n$Key=$Value`n" }
        $sectionText = $match.Value
        $keyPattern = '(?m)^\s*' + [regex]::Escape($Key) + '\s*=[^\r\n]*'
        if ([regex]::Matches($sectionText, $keyPattern).Count -gt 1) { throw "Duplicate WSL key: $Key" }
        if ($sectionText -match $keyPattern) { $sectionText = [regex]::Replace($sectionText, $keyPattern, "$Key=$Value") }
        else { $sectionText = $sectionText.TrimEnd() + "`n$Key=$Value`n" }
        return $Text.Substring(0, $match.Index) + $sectionText + $Text.Substring($match.Index + $match.Length)
    }
    $wsl = Set-IniValue $wsl 'wsl2' 'memory' ($policy.host.memory_mib.ToString() + 'MB')
    $wsl = Set-IniValue $wsl 'wsl2' 'processors' $policy.host.processors.ToString()
    $wsl = Set-IniValue $wsl 'wsl2' 'swap' ($policy.host.swap_mib.ToString() + 'MB')
    $wsl = Set-IniValue $wsl 'experimental' 'autoMemoryReclaim' 'gradual'
    [IO.File]::WriteAllText($wslFile, $wsl.TrimStart() + "`n", [Text.UTF8Encoding]::new($false))
    Write-Output 'WSL config saved. It applies after all WSL distributions stop/restart; this script does not stop unrelated projects.'
}
Write-Output 'Resource variables updated; passwords and encryption keys preserved. Recreate services after draining Airflow tasks.'
