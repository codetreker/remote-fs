param(
    [Parameter(Mandatory)]
    [ValidateSet('Prepare', 'Run', 'Verify')]
    [string]$Phase
)

$ErrorActionPreference = 'Stop'
$workspace = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$gateRoot = Join-Path $workspace '.tmp/native-cache-gate'
$fixture = Join-Path $gateRoot 'fixture'
$results = Join-Path $gateRoot 'results'
$expectedSource = '1cb9ad7f49d998de4daa4d562d766b18cf06ce16'

function Get-CachePolicy {
    $policy = Get-SmbClientConfiguration
    return [ordered]@{
        FileNotFoundCacheLifetime = $policy.FileNotFoundCacheLifetime
        DirectoryCacheLifetime = $policy.DirectoryCacheLifetime
        FileInfoCacheLifetime = $policy.FileInfoCacheLifetime
    }
}

function Get-MappingKeys {
    return @(Get-SmbMapping | ForEach-Object { "$($_.LocalPath)|$($_.RemotePath)" } | Sort-Object)
}

function Replace-ExactlyOnce([string]$Path, [string]$Before, [string]$After) {
    $text = [IO.File]::ReadAllText($Path).Replace("`r`n", "`n")
    if ([regex]::Matches($text, [regex]::Escape($Before)).Count -ne 1) {
        throw "Pinned diagnostic source has an unexpected instrumentation anchor: $Path"
    }
    [IO.File]::WriteAllText($Path, $text.Replace($Before, $After), [Text.UTF8Encoding]::new($false))
}

function Invoke-DiagnosticTests([string]$Package, [string]$Pattern, [string[]]$Expected, [string]$LogName) {
    & go test -json -count=1 -p=1 -timeout 3m -run $Pattern $Package 2>&1 |
        Tee-Object -FilePath (Join-Path $results $LogName)
    $testExit = $LASTEXITCODE
    if ($testExit -ne 0) { exit $testExit }
    # Module diagnostics remain in the raw stream; every expected verdict is required.
    $events = @(Get-Content (Join-Path $results $LogName) | Where-Object { $_.StartsWith('{') } | ForEach-Object { $_ | ConvertFrom-Json })
    if (@($events | Where-Object { $_.Action -in @('fail', 'skip') }).Count -ne 0) {
        throw 'Diagnostic tests failed or skipped.'
    }
    foreach ($name in $Expected) {
        $verdict = @($events | Where-Object { $_.Test -eq $name -and $_.Action -in @('pass', 'fail', 'skip') })
        if ($verdict.Count -ne 1 -or $verdict[0].Action -ne 'pass') {
            throw "The expected diagnostic test did not execute and pass: $name"
        }
    }
}

if ($Phase -eq 'Prepare') {
    New-Item -ItemType Directory -Force -Path $results | Out-Null
    $actualSource = (& git -C $fixture rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $actualSource -ne $expectedSource) {
        throw 'The diagnostic prototype must match its pinned commit.'
    }
    $os = Get-CimInstance Win32_OperatingSystem
    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    $policy = Get-CachePolicy
    [ordered]@{
        EvidenceKind = 'Pinned prototype diagnostic; not PR implementation acceptance'
        FixtureSHA = $actualSource
        GateSHA = $env:GITHUB_SHA
        Variant = $env:RFS_GATE_VARIANT
        Caption = $os.Caption
        Version = $os.Version
        BuildNumber = $os.BuildNumber
        ProductType = $os.ProductType
        Architecture = $architecture
        ImageVersion = $env:ImageVersion
        CachePolicy = $policy
    } | ConvertTo-Json -Depth 5 | Tee-Object -FilePath (Join-Path $results 'environment.json')
    if ($os.ProductType -ne 1 -or ([version]$os.Version).Major -ne 10 -or [int]$os.BuildNumber -lt 26100 -or $architecture -ne 'Arm64') {
        throw 'The probe requires native Windows 11 24H2+ ARM64.'
    }
    $lifetimes = @($policy.FileNotFoundCacheLifetime, $policy.DirectoryCacheLifetime, $policy.FileInfoCacheLifetime)
    if (@($lifetimes | Where-Object { $null -eq $_ -or $_ -le 1 }).Count -ne 0) {
        throw 'Cache lifetimes above one second are required to exclude an existing global cache override.'
    }
    ConvertTo-Json -InputObject (Get-MappingKeys) | Set-Content (Join-Path $results 'mappings-before.json') -Encoding utf8
    foreach ($pair in @{'GOCACHE'='go-build'; 'GOMODCACHE'='go-mod'; 'GOPATH'='go-path'; 'TMPDIR'='tmp'; 'TMP'='tmp'; 'TEMP'='tmp'}.GetEnumerator()) {
        $path = Join-Path $gateRoot $pair.Value
        New-Item -ItemType Directory -Force -Path $path | Out-Null
        Add-Content $env:GITHUB_ENV -Value "$($pair.Key)=$($path.Replace('\', '/'))" -Encoding utf8
    }
    Add-Content $env:GITHUB_ENV -Value 'GOTOOLCHAIN=local' -Encoding utf8
    Add-Content $env:GITHUB_ENV -Value "RFS_GATE_MIN_CACHE_TTL=$(($lifetimes | Measure-Object -Minimum).Minimum)" -Encoding utf8
    Add-Content $env:GITHUB_ENV -Value "RFS_GATE_OUTPUT_ROOT=$results" -Encoding utf8
    exit 0
}

if ($Phase -eq 'Run') {
    $target = & go env GOOS GOARCH
    if ($LASTEXITCODE -ne 0 -or ($target -join '/') -ne 'windows/arm64') {
        throw 'The diagnostic must execute Go natively on Windows ARM64.'
    }
    & go version
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $overlay = Join-Path $PSScriptRoot 'native-smb-notify-continuity.patch'
    & git -C $fixture apply --unidiff-zero --check $overlay
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & git -C $fixture apply --unidiff-zero $overlay
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $env:RFS_GATE_OVERLAY_SHA = (Get-FileHash $overlay -Algorithm SHA256).Hash.ToLowerInvariant()
    [ordered]@{ Kind = 'Platform notification continuity overlay'; SHA256 = $env:RFS_GATE_OVERLAY_SHA } |
        ConvertTo-Json | Set-Content (Join-Path $results 'overlay.json') -Encoding utf8
    Copy-Item (Join-Path $PSScriptRoot 'native-smb-notify-continuity_test.go.txt') (Join-Path $fixture 'packages/smb/gate_notify_continuity_test.go')
    $testRoot = Join-Path $fixture 'packages/smb/windows'
    Copy-Item (Join-Path $PSScriptRoot 'native-smb-cache-gate_test.go.txt') (Join-Path $testRoot 'native_cache_gate_windows_test.go')
    $wire = Join-Path $testRoot 'native_wire_windows_test.go'
    foreach ($field in @('headers', 'creates', 'createReplies')) {
        $limit = if ($field -eq 'headers') { 16 } else { 8 }
        $anchor = "c.observation.$field = nativeRetainLast(c.observation.$field, h, $limit)"
        $replacement = $anchor + "`n" + ('cacheGateObserve("smb_' + $field + '", h)')
        Replace-ExactlyOnce $wire $anchor $replacement
    }
    $authority = Join-Path $testRoot 'native_fixture_windows_test.go'
    Replace-ExactlyOnce $authority 'if len(a.changes) == 1024 {' @'
cacheGateObserve("authority_event", map[string]any{"position": c.Position, "kind": c.Kind, "name": string(c.Name)})
if len(a.changes) == 1024 {
'@
    $bridge = Join-Path $testRoot 'native_acceptance_windows_test.go'
    Replace-ExactlyOnce $bridge 'Limits: smb.DefaultLimits()' 'Limits: cacheGateLimits()'
    & gofmt -w $wire $authority $bridge (Join-Path $testRoot 'native_cache_gate_windows_test.go')
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & git -C $fixture diff --stat | Write-Output
    $PSNativeCommandUseErrorActionPreference = $false
    Push-Location $fixture
    try {
        Invoke-DiagnosticTests './packages/smb' '^Test(Notification|GateNotify)' @('TestGateNotifyRescanRetainsGapEvents', 'TestGateNotifyRescanRejectsReplacedSource') 'notify-unit-test.jsonl'
        Invoke-DiagnosticTests './packages/smb/windows' '^TestNativeNegativeNameCacheGate$' @('TestNativeNegativeNameCacheGate') 'go-test.jsonl'
    } finally {
        Pop-Location
    }
    exit 0
}

$before = Get-Content (Join-Path $results 'environment.json') -Raw | ConvertFrom-Json
$afterPolicy = Get-CachePolicy
$afterMappings = Get-MappingKeys
[ordered]@{ CachePolicy = $afterPolicy; Mappings = $afterMappings } |
    ConvertTo-Json -Depth 5 | Tee-Object -FilePath (Join-Path $results 'environment-after.json')
if (($before.CachePolicy | ConvertTo-Json -Compress) -ne ($afterPolicy | ConvertTo-Json -Compress)) {
    throw 'The diagnostic changed the workstation cache policy.'
}
$beforeMappings = @(Get-Content (Join-Path $results 'mappings-before.json') -Raw | ConvertFrom-Json)
$remaining = @($afterMappings | Where-Object { $_ -notin $beforeMappings })
if ($remaining.Count -ne 0) {
    throw "Mappings outlived the diagnostic: $($remaining -join ', ')"
}
