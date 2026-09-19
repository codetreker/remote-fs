param(
    [Parameter(Mandatory)]
    [ValidateSet('Prepare', 'PrepareFixture', 'Run', 'Verify')]
    [string]$Phase
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$workspace = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$probeRoot = Join-Path $workspace '.tmp/native-parent-invalidation'
$fixture = Join-Path $probeRoot 'fixture'
$results = Join-Path $probeRoot 'results'
$prototype = '1cb9ad7f49d998de4daa4d562d766b18cf06ce16'

function Write-JSON([string]$Name, $Value) {
    ConvertTo-Json -InputObject $Value -Depth 12 | Set-Content (Join-Path $results $Name) -Encoding utf8
}

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

function Get-CanonicalHash([string]$Path) {
    $bytes = [Text.Encoding]::UTF8.GetBytes([IO.File]::ReadAllText($Path).Replace("`r`n", "`n"))
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Normalize-File([string]$Path) {
    [IO.File]::WriteAllText($Path, [IO.File]::ReadAllText($Path).Replace("`r`n", "`n"), [Text.UTF8Encoding]::new($false))
}

function Replace-ExactlyOnce([string]$Path, [string]$Before, [string]$After) {
    $text = [IO.File]::ReadAllText($Path).Replace("`r`n", "`n")
    if ([regex]::Matches($text, [regex]::Escape($Before)).Count -ne 1) {
        throw "The pinned instrumentation anchor is not unique: $Path"
    }
    [IO.File]::WriteAllText($Path, $text.Replace($Before, $After), [Text.UTF8Encoding]::new($false))
}

function Invoke-ProbeTests([string]$Package, [string]$Pattern, [string[]]$Expected, [string]$Name) {
    & go test -json -count=1 -p=1 -timeout 3m -run $Pattern $Package 2>&1 |
        Tee-Object -FilePath (Join-Path $results $Name)
    $testExit = $LASTEXITCODE
    $events = @(Get-Content (Join-Path $results $Name) | Where-Object { $_.StartsWith('{') } | ForEach-Object { $_ | ConvertFrom-Json })
    if ($testExit -ne 0) { throw "Probe tests failed ($testExit): $Name" }
    if (@($events | Where-Object { $_.Action -in @('fail', 'skip', 'build-fail') }).Count -ne 0) {
        throw "Probe tests failed or skipped: $Name"
    }
    foreach ($test in $Expected) {
        $verdict = @($events | Where-Object { $_.Test -eq $test -and $_.Action -in @('pass', 'fail', 'skip') })
        if ($verdict.Count -ne 1 -or $verdict[0].Action -ne 'pass') {
            throw "The required probe did not execute and pass: $test"
        }
    }
    $unfinished = @($events | Where-Object { $_.Action -eq 'run' } | ForEach-Object {
        $test = $_.Test
        if (@($events | Where-Object { $_.Test -eq $test -and $_.Action -eq 'pass' }).Count -ne 1) { $test }
    })
    if ($unfinished.Count -ne 0) { throw "Probe tests have no unique passing verdict: $($unfinished -join ', ')" }
}

if ($Phase -eq 'Prepare') {
    New-Item -ItemType Directory -Force -Path $results | Out-Null
    $source = (& git -C $workspace rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $source -ne $env:RFS_PARENT_SOURCE_SHA) {
        throw 'The probe checkout differs from the selected source revision.'
    }
    $actualPrototype = (& git -C $fixture rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $actualPrototype -ne $prototype) {
        throw 'The prototype checkout differs from its immutable diagnostic revision.'
    }
    foreach ($checkout in @($workspace, $fixture)) {
        & git -C $checkout diff --exit-code --quiet
        if ($LASTEXITCODE -ne 0) { throw 'The selected source has uncommitted modifications.' }
    }
    if ($env:GITHUB_RUN_ATTEMPT -ne '1') { throw 'A new probe run requires a new reviewed source revision.' }
    $os = Get-CimInstance Win32_OperatingSystem
    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    $policy = Get-CachePolicy
    Write-JSON 'environment.json' ([ordered]@{
        EvidenceKind = 'Pinned prototype ancestor notification experiment; not implementation acceptance'
        SourceSHA = $source; PrototypeSHA = $actualPrototype; RunID = $env:GITHUB_RUN_ID
        Caption = $os.Caption; Version = $os.Version; BuildNumber = $os.BuildNumber
        ProductType = $os.ProductType; Architecture = $architecture; ImageVersion = $env:ImageVersion
        CachePolicy = $policy
    })
    if ($os.ProductType -ne 1 -or ([version]$os.Version).Major -ne 10 -or [int]$os.BuildNumber -lt 26100 -or $architecture -ne 'Arm64') {
        throw 'Native Windows 11 24H2+ ARM64 is required.'
    }
    $lifetimes = @($policy.FileNotFoundCacheLifetime, $policy.DirectoryCacheLifetime, $policy.FileInfoCacheLifetime)
    if (@($lifetimes | Where-Object { $null -eq $_ -or $_ -le 1 }).Count -ne 0) {
        throw 'All measured cache lifetimes must exceed one second.'
    }
    Write-JSON 'mappings-before.json' @(Get-MappingKeys)
    Write-JSON 'processes-before.json' @(Get-Process | Where-Object { $_.ProcessName -match '^(windows|smb)\.test$' } | Select-Object -ExpandProperty Id)
    foreach ($pair in @{'GOCACHE'='go-build'; 'GOMODCACHE'='go-mod'; 'GOPATH'='go-path'; 'TMPDIR'='tmp'; 'TMP'='tmp'; 'TEMP'='tmp'}.GetEnumerator()) {
        $path = Join-Path $probeRoot $pair.Value
        New-Item -ItemType Directory -Force -Path $path | Out-Null
        Add-Content $env:GITHUB_ENV -Value "$($pair.Key)=$($path.Replace('\', '/'))" -Encoding utf8
    }
    Add-Content $env:GITHUB_ENV -Value 'GOTOOLCHAIN=local' -Encoding utf8
    Add-Content $env:GITHUB_ENV -Value "RFS_GATE_MIN_CACHE_TTL=$(($lifetimes | Measure-Object -Minimum).Minimum)" -Encoding utf8
    Add-Content $env:GITHUB_ENV -Value "RFS_GATE_OUTPUT_ROOT=$results" -Encoding utf8
    exit 0
}

if ($Phase -in @('PrepareFixture', 'Run')) {
    New-Item -ItemType Directory -Force -Path $results | Out-Null
    $target = & go env GOOS GOARCH
    if ($LASTEXITCODE -ne 0 -or ($Phase -eq 'Run' -and ($target -join '/') -ne 'windows/arm64')) {
        throw 'The probe must execute Go natively on Windows ARM64.'
    }
    $version = & go version
    if ($LASTEXITCODE -ne 0 -or $version -notmatch '^go version go1\.26\.8 ' -or ($Phase -eq 'Run' -and $version -ne 'go version go1.26.8 windows/arm64')) {
        throw 'The probe requires Go 1.26.8.'
    }
    Write-Output $version
    $patches = @(
        @{ Name = 'native-smb-notify-continuity.patch'; SHA256 = 'f92edc59b55cc317fa0b5dcc1d1a55dd687b979e0e9a6bcb2112606ea4318e26'; Files = @(
            @{ Path = 'packages/smb/notify.go'; Before = 'ce021f244ec87d16e1556a7d2ad20f1e7459348b18da3a1c5f1fa04e0c7d81b6'; After = 'd372ec4385c68c176631f48b75c3e0d19ad107ed7e84348a4214400b6e42c640' },
            @{ Path = 'packages/smb/notify_test.go'; Before = 'b4e3df6bebb3bac401bdaa31521f26962162c1488062457ba2042b3fcfa3f2fb'; After = 'd484f6c36304adc713db04549f95b0a6e9732cc9d8ea21937347fd719164eada' }
        ) },
        @{ Name = 'native-smb-missing-status.patch'; SHA256 = 'a53e64d479138497e99f96ded82af01ec69be8434eac4cf0952f9c534dfc8d8a'; Files = @(
            @{ Path = 'packages/smb/client_open.go'; Before = '04b276bf22335b0e35357f986a4c40e12be17fcba01b7cb04d4e4592edfec19a'; After = 'c99b69f47ec4cd70f6af0e4a429b1a2944bc8d2b754147a466420ef9af38e573' },
            @{ Path = 'packages/smb/commands_files.go'; Before = '4366f2b5cc342520b9514576314ab9440cf16fb9c5a83ae7b19b34f4350a304f'; After = 'ff4394ec0f6396610288253da131cfe5bd3329affac3c3e82266fcd0fa117b07' }
        ) },
        @{ Name = 'native-smb-no-leasing.patch'; SHA256 = '62538e9c1eb5b2b12a4d6fed71937fdd675b378d253c8f1c6c9d875a84f06734'; Files = @(
            @{ Path = 'packages/smb/server.go'; Before = 'e2fd4f9e6bf6ed458862ee59e2c0f6b9c53b32a1580d71a70651c02c81039325'; After = '3dcfe03a8c9c8b9dc2b2840e52be0e290d1a973bd2edd367b1f21fdec185d406' },
            @{ Path = 'packages/smb/commands_session.go'; Before = '6228ca6e9813d7915d8f01d34ca15cba517f1450a9463f6ed70f4f10661ee26d'; After = 'c7e12376dd9b8100a8b224ffa1732b05bbd29096f006fdbe7960beddeb6d78df' },
            @{ Path = 'packages/smb/commands_files.go'; Before = 'ff4394ec0f6396610288253da131cfe5bd3329affac3c3e82266fcd0fa117b07'; After = '88570edf50156ea4b275a33722bc1a90aca88cbcd427f1b0cc148e68bc9ef199' }
        ) }
    )
    $inputs = @(
        'native-smb-parent-invalidation.ps1', 'native-smb-parent-invalidation_test.go.txt',
        'native-smb-cache-gate_test.go.txt', 'native-smb-positive-cache_test.go.txt',
        'native-smb-positive-wire_test.go.txt', 'native-smb-positive-wire-checks_test.go.txt',
        'native-smb-notify-continuity_test.go.txt', 'native-smb-missing-status_test.go.txt',
        'native-smb-no-leasing_test.go.txt'
    ) | ForEach-Object {
        $path = Join-Path $PSScriptRoot $_
        [ordered]@{ Path = ".github/scripts/$_"; RawSHA256 = (Get-FileHash $path -Algorithm SHA256).Hash.ToLowerInvariant(); CanonicalSHA256 = Get-CanonicalHash $path }
    }
    $reused = @{
        'native-smb-cache-gate_test.go.txt' = 'fecf5000fcb8545713b34e43c36768beb1b89a9dd40c9f2e85cc89d216083db0'
        'native-smb-positive-cache_test.go.txt' = '4f2395a7153dbdd8bce8cb1b4327bebf6fbd20a05a3861ff434a97d27ff5622b'
        'native-smb-positive-wire_test.go.txt' = 'dd9f41544fe4fc3ad5e7ba2389d700ce1af2b6ea26307c76db11efce5536cc80'
        'native-smb-positive-wire-checks_test.go.txt' = '72bfb77a27fc297c478c0234acc1244183f0dcee212daeb27c11e1f3acc64535'
        'native-smb-notify-continuity_test.go.txt' = '76e923eafad36d9cadf718166c822cc587422721dcaa3b1c863ba365a82a7bb9'
        'native-smb-missing-status_test.go.txt' = 'b5b16915eb5530c66ae9883c07dc308b904199762779055adbde728377cf31a6'
        'native-smb-no-leasing_test.go.txt' = '7172efdbf072473f0a6d4b81e4e5204ee5c03219e287aa0e260c532ad7d71342'
    }
    foreach ($entry in $reused.GetEnumerator()) {
        if ((Get-CanonicalHash (Join-Path $PSScriptRoot $entry.Key)) -ne $entry.Value) {
            throw "The reused diagnostic helper differs: $($entry.Key)"
        }
    }
    $workflow = Join-Path $workspace '.github/workflows/native-smb-parent-invalidation.yml'
    $inputs += [ordered]@{ Path = '.github/workflows/native-smb-parent-invalidation.yml'; RawSHA256 = (Get-FileHash $workflow -Algorithm SHA256).Hash.ToLowerInvariant(); CanonicalSHA256 = Get-CanonicalHash $workflow }
    Write-JSON 'inputs.json' ([ordered]@{ Mode = $Phase; SourceSHA = $env:RFS_PARENT_SOURCE_SHA; PrototypeSHA = $prototype; Files = @($inputs); Patches = $patches })
    foreach ($patch in $patches) {
        $source = Join-Path $PSScriptRoot $patch.Name
        $applied = Join-Path $results $patch.Name
        [IO.File]::WriteAllText($applied, [IO.File]::ReadAllText($source).Replace("`r`n", "`n"), [Text.UTF8Encoding]::new($false))
        if ((Get-CanonicalHash $applied) -ne $patch.SHA256) { throw "Unrecognized diagnostic patch: $($patch.Name)" }
        foreach ($file in $patch.Files) {
            $path = Join-Path $fixture $file.Path
            Normalize-File $path
            if ((Get-CanonicalHash $path) -ne $file.Before) { throw "Patch input differs: $($file.Path)" }
        }
        & git -c core.autocrlf=false -C $fixture apply --unidiff-zero --check $applied
        if ($LASTEXITCODE -ne 0) { throw "Cannot apply diagnostic patch: $($patch.Name)" }
        & git -c core.autocrlf=false -C $fixture apply --unidiff-zero $applied
        if ($LASTEXITCODE -ne 0) { throw "Applying diagnostic patch failed: $($patch.Name)" }
        foreach ($file in $patch.Files) {
            if ((Get-CanonicalHash (Join-Path $fixture $file.Path)) -ne $file.After) { throw "Patch output differs: $($file.Path)" }
        }
    }
    $env:RFS_GATE_OVERLAY_SHA = $patches[0].SHA256
    $env:RFS_GATE_MISSING_STATUS_SHA = $patches[1].SHA256
    $env:RFS_GATE_NO_LEASING_SHA = $patches[2].SHA256
    $testRoot = Join-Path $fixture 'packages/smb/windows'
    foreach ($name in @('notify-continuity', 'missing-status', 'no-leasing')) {
        Copy-Item (Join-Path $PSScriptRoot "native-smb-$name`_test.go.txt") (Join-Path $fixture "packages/smb/gate_$($name.Replace('-', '_'))_test.go")
    }
    Copy-Item (Join-Path $PSScriptRoot 'native-smb-cache-gate_test.go.txt') (Join-Path $testRoot 'native_cache_gate_windows_test.go')
    Copy-Item (Join-Path $PSScriptRoot 'native-smb-parent-invalidation_test.go.txt') (Join-Path $testRoot 'gate_parent_invalidation_windows_test.go')
    foreach ($name in @('cache', 'wire', 'wire-checks')) {
        Copy-Item (Join-Path $PSScriptRoot "native-smb-positive-$name`_test.go.txt") (Join-Path $testRoot "gate_positive_$($name.Replace('-', '_'))_windows_test.go")
    }
    $wire = Join-Path $testRoot 'native_wire_windows_test.go'
    foreach ($field in @('headers', 'creates', 'createReplies')) {
        $limit = if ($field -eq 'headers') { 16 } else { 8 }
        $anchor = "c.observation.$field = nativeRetainLast(c.observation.$field, h, $limit)"
        Replace-ExactlyOnce $wire $anchor ($anchor + "`n" + ('cacheGateObserve("smb_' + $field + '", h)'))
    }
    Replace-ExactlyOnce $wire 'received nativeFrameObserver' "received nativeFrameObserver`npositive *cacheGatePositiveFrameObserver"
    Replace-ExactlyOnce $wire 'return &nativeObservedConnection{Conn: connection,' 'return &nativeObservedConnection{Conn: connection, positive: cacheGateOptionalPositiveObserver(),'
    Replace-ExactlyOnce $wire 'c.received.observe(buffer[:n])' "c.received.observe(buffer[:n])`nif c.positive != nil { c.positive.observe(buffer[:n], false) }"
    Replace-ExactlyOnce $wire 'c.sent.observe(buffer[:n])' "c.sent.observe(buffer[:n])`nif c.positive != nil { c.positive.observe(buffer[:n], true) }"
    $positiveWire = Join-Path $testRoot 'gate_positive_wire_windows_test.go'
    Replace-ExactlyOnce $positiveWire 'r.Connection = o.id' "r.Connection = o.id`ncacheGateParentWireObserved(r)"
    Replace-ExactlyOnce $positiveWire 'r := cacheGatePositiveWireRecord{Response: response, MessageID:' 'r := cacheGatePositiveWireRecord{Connection: o.id, Response: response, MessageID:'
    Replace-ExactlyOnce $positiveWire "`tcase wire.Close, wire.Flush:" "`tcase wire.ChangeNotify:`n`t`treturn cacheGateParentNotifyRequest(packet, r)`n`tcase wire.Close, wire.Flush:"
    Replace-ExactlyOnce $positiveWire "`tswitch r.Command {`n`tcase wire.Create:" "`tswitch r.Command {`n`tcase wire.ChangeNotify:`n`t`treturn cacheGateParentNotifyResponse(packet, r)`n`tcase wire.Create:"
    $files = Join-Path $testRoot 'native_files_windows_test.go'
    Replace-ExactlyOnce $files "result.Removal = f.removal()`n`treturn result, nil" @'
result.Removal = f.removal()
cacheGatePositiveCapture("stat", f.session.id, f.reference, result.Attr, 0, nil)
return result, nil
'@
    Replace-ExactlyOnce $files 'return storage.FileRead{Attr: nativeAttr(f.node), Data: bytes.Clone(f.node.data[start:end])}, nil' @'
result := storage.FileRead{Attr: nativeAttr(f.node), Data: bytes.Clone(f.node.data[start:end])}
cacheGatePositiveCapture("read", f.session.id, f.reference, result.Attr, r.Offset, result.Data)
return result, nil
'@
    $authority = Join-Path $testRoot 'native_fixture_windows_test.go'
    Replace-ExactlyOnce $authority 'if len(a.changes) == 1024 {' @'
cacheGateObserve("authority_event", map[string]any{"position": c.Position, "kind": c.Kind, "name": string(c.Name)})
if len(a.changes) == 1024 {
'@
    $bridge = Join-Path $testRoot 'native_acceptance_windows_test.go'
    Replace-ExactlyOnce $bridge 'Limits: smb.DefaultLimits()' 'Limits: cacheGateLimits()'
    Replace-ExactlyOnce $bridge 'b.mapping, err = Map(ctx, MappingOptions{LocalPath: b.path, Share: b.share, TCPPort: b.port})' @'
if err := cacheGatePositiveOwnMapping(b.path, b.share); err != nil { return err }
b.mapping, err = Map(ctx, MappingOptions{LocalPath: b.path, Share: b.share, TCPPort: b.port})
'@
    $modified = @($wire, $positiveWire, $files, $authority, $bridge, (Join-Path $testRoot 'gate_parent_invalidation_windows_test.go'))
    & gofmt -w @modified
    if ($LASTEXITCODE -ne 0) { throw 'Formatting the injected probe failed.' }
    $outputs = $modified | ForEach-Object {
        [ordered]@{ Path = [IO.Path]::GetRelativePath($fixture, $_).Replace('\', '/'); SHA256 = (Get-FileHash $_ -Algorithm SHA256).Hash.ToLowerInvariant() }
    }
    Write-JSON 'instrumented-source.json' @($outputs)
    if ($Phase -eq 'PrepareFixture') { exit 0 }
    Push-Location $fixture
    try {
        Invoke-ProbeTests './packages/smb' '^TestGateNoLeasing|^TestGateMissingStatus|^TestGateNotify' @(
            'TestGateNoLeasingNegotiationAndSigning', 'TestGateNoLeasingIgnoresOnlyInnerLeaseContext',
            'TestGateNoLeasingKeepsOuterBounds', 'TestGateNoLeasingReplacementKeepsSeparateOpenIdentities',
            'TestGateNoLeasingPreservesOpenFailuresAndReconciliation', 'TestGateNoLeasingRetainsAuthorizationAndRejectsUnsolicitedAck',
            'TestGateNoLeasingIdleLifecycle', 'TestGateMissingStatusRequiresVerifiedParent',
            'TestGateMissingStatusOnlyChangesFinalCreate', 'TestGateNotifyRescanRetainsGapEvents',
            'TestGateNotifyRescanRejectsReplacedSource'
        ) 'overlay-controls.jsonl'
        Invoke-ProbeTests './packages/smb/windows' '^TestParentInvalidation' @(
            'TestParentInvalidationNotifyParsing', 'TestParentInvalidationHookCorrelation', 'TestParentInvalidationQualification'
        ) 'probe-controls.jsonl'
        Invoke-ProbeTests './packages/smb/windows' '^TestNativeParentInvalidation$' @(
            'TestNativeParentInvalidation', 'TestNativeParentInvalidation/noWatcher',
            'TestNativeParentInvalidation/share0_watcher_first', 'TestNativeParentInvalidation/share0_app_first',
            'TestNativeParentInvalidation/share6_watcher_first', 'TestNativeParentInvalidation/share6_app_first',
            'TestNativeParentInvalidation/share0_app_first_shrink', 'TestNativeParentInvalidation/share0_app_first_replace_identity'
        ) 'native-probe.jsonl'
    } finally {
        Pop-Location
    }
    exit 0
}

$before = Get-Content (Join-Path $results 'environment.json') -Raw | ConvertFrom-Json
$baselineMappings = @(Get-Content (Join-Path $results 'mappings-before.json') -Raw | ConvertFrom-Json)
$baselineProcesses = @(Get-Content (Join-Path $results 'processes-before.json') -Raw | ConvertFrom-Json)
$recovered = @()
$ledgers = @(Get-ChildItem $results -Filter 'owned-mapping-*.json' -File | ForEach-Object { Get-Content $_.FullName -Raw | ConvertFrom-Json })
foreach ($mapping in @(Get-SmbMapping)) {
    $key = "$($mapping.LocalPath)|$($mapping.RemotePath)"
    if ($key -in $baselineMappings) { continue }
    if (@($ledgers | Where-Object { $_.Local -eq $mapping.LocalPath -and $_.Remote -eq $mapping.RemotePath }).Count -eq 0) { continue }
    Remove-SmbMapping -LocalPath $mapping.LocalPath -RemotePath $mapping.RemotePath -Force -Confirm:$false
    $recovered += $key
}
$policy = Get-CachePolicy
$remainingMappings = @(Get-MappingKeys | Where-Object { $_ -notin $baselineMappings })
$remainingProcesses = @(Get-Process | Where-Object { $_.ProcessName -match '^(windows|smb)\.test$' -and $_.Id -notin $baselineProcesses } | Select-Object Id, ProcessName)
Write-JSON 'cleanup.json' ([ordered]@{ CachePolicy = $policy; RecoveredMappings = $recovered; RemainingMappings = $remainingMappings; RemainingProcesses = $remainingProcesses })
if (($before.CachePolicy | ConvertTo-Json -Compress) -ne ($policy | ConvertTo-Json -Compress)) {
    throw 'The probe changed the machine SMB client cache policy.'
}
if ($recovered.Count -ne 0 -or $remainingMappings.Count -ne 0 -or $remainingProcesses.Count -ne 0) {
    throw 'The probe did not release all owned mappings and processes cleanly.'
}
