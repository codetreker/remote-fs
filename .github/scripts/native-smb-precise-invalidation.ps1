param(
    [Parameter(Mandatory)]
    [ValidateSet('Prepare', 'PrepareFixture', 'Run', 'Verify')]
    [string]$Phase
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$workspace = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$probeRoot = Join-Path $workspace '.tmp/native-precise-invalidation'
$fixture = Join-Path $probeRoot 'fixture'
$results = Join-Path $probeRoot 'results'
$parent = Join-Path $PSScriptRoot 'native-smb-parent-invalidation.ps1'

function Canonical-Hash([string]$Path) {
    $bytes = [Text.Encoding]::UTF8.GetBytes([IO.File]::ReadAllText($Path).Replace("`r`n", "`n"))
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}
function Write-JSON([string]$Name, $Value) {
    ConvertTo-Json -InputObject $Value -Depth 20 | Set-Content (Join-Path $results $Name) -Encoding utf8
}
function Replace-Once([string]$Path, [string]$Before, [string]$After) {
    $text = [IO.File]::ReadAllText($Path).Replace("`r`n", "`n")
    if ([regex]::Matches($text, [regex]::Escape($Before)).Count -ne 1) { throw "Precise instrumentation anchor is not unique: $Path" }
    [IO.File]::WriteAllText($Path, $text.Replace($Before, $After), [Text.UTF8Encoding]::new($false))
}
function Assert-Verdicts([string]$Path, [int]$ExitCode, [string[]]$Expected) {
    $events = @(Get-Content $Path | Where-Object { $_.StartsWith('{') } | ForEach-Object { $_ | ConvertFrom-Json })
    if ($ExitCode -ne 0 -or @($events | Where-Object { $_.Action -in @('fail', 'skip', 'build-fail') }).Count -ne 0) { throw "Precise probe failed: $Path" }
    foreach ($test in $Expected) {
        if (@($events | Where-Object { $_.Test -eq $test -and $_.Action -eq 'pass' }).Count -ne 1) { throw "Required precise test did not pass exactly once: $test" }
    }
    foreach ($run in @($events | Where-Object { $_.Action -eq 'run' })) {
        if (@($events | Where-Object { $_.Test -eq $run.Test -and $_.Action -eq 'pass' }).Count -ne 1) { throw "Precise test has no unique verdict: $($run.Test)" }
    }
}
function Invoke-Controls([string]$Package, [string]$Pattern, [string[]]$Expected, [string]$Name) {
    $path = Join-Path $results $Name
    & go test -json -count=1 -p=1 -timeout 3m -run $Pattern $Package 2>&1 | Tee-Object -FilePath $path
    Assert-Verdicts $path $LASTEXITCODE $Expected
}

if ((Canonical-Hash $parent) -ne 'b705eceabd1ba519d2fa3368dc84efb5f81e385129ab543487a3cd8a77d8985a') { throw 'Shared preparation harness differs from the reviewed precise input.' }
if ((Canonical-Hash (Join-Path $PSScriptRoot 'native-smb-parent-invalidation_test.go.txt')) -ne '7f70553d74ac9811b9680deb0b36ab5b63787889911735c76ed53c7c13ef0395') { throw 'Shared typed probe differs from the reviewed precise input.' }
if ($Phase -in @('Prepare', 'Verify')) {
    & $parent -Phase $Phase -ProbeDirectory 'native-precise-invalidation'
    if ($LASTEXITCODE -ne 0) { throw "Shared precise $Phase failed." }
    exit 0
}
& $parent -Phase PrepareFixture -ProbeDirectory 'native-precise-invalidation'
if ($LASTEXITCODE -ne 0) { throw 'Preparing the immutable precise fixture failed.' }
if ($Phase -eq 'Run' -and ((& go env GOOS GOARCH) -join '/') -ne 'windows/arm64') { throw 'Precise native execution requires Windows ARM64.' }

$patch = Join-Path $PSScriptRoot 'native-smb-precise-history.patch'
$patchHash = '8d1c8d3c6efe16bb1424ca44ed77f6e860152c0c672aae3fd43d9327ede94029'
if ((Canonical-Hash $patch) -ne $patchHash) { throw 'Historical notification overlay differs from the reviewed patch.' }
$patchFiles = @(
    @{ Path = 'packages/smb/notify.go'; Before = 'd372ec4385c68c176631f48b75c3e0d19ad107ed7e84348a4214400b6e42c640'; After = 'd2004c09da9cba70706fb212bfe96a3bba12b12a95c15d270fb661fb3a22863e' },
    @{ Path = 'packages/smb/client_notify.go'; Before = '67b2575cf75a88f1a13fc0ac74322c742b5e1bbe943ef4d0a610a40c6bfe1ed9'; After = '972f89b2a164c02047d5f6e900dcbf74c3de182b85ddb59794ea4be86cc2ed5e' },
    @{ Path = 'packages/smb/commands_notify.go'; Before = '2c6e83b6bc87a785b2c0a59755887af4fba53ce3e3417d364f6e4b875eae1ded'; After = '1b3317350512ee74e514dbe2268af75c7f071f511525d7800522f89dd74a472c' }
)
foreach ($file in $patchFiles) { if ((Canonical-Hash (Join-Path $fixture $file.Path)) -ne $file.Before) { throw "Precise patch input differs: $($file.Path)" } }
$applied = Join-Path $results 'native-smb-precise-history.patch'
[IO.File]::WriteAllText($applied, [IO.File]::ReadAllText($patch).Replace("`r`n", "`n"), [Text.UTF8Encoding]::new($false))
& git -c core.autocrlf=false -C $fixture apply --unidiff-zero --check $applied
if ($LASTEXITCODE -ne 0) { throw 'Precise notification patch cannot be applied.' }
& git -c core.autocrlf=false -C $fixture apply --unidiff-zero $applied
if ($LASTEXITCODE -ne 0) { throw 'Applying precise notification patch failed.' }
foreach ($file in $patchFiles) { if ((Canonical-Hash (Join-Path $fixture $file.Path)) -ne $file.After) { throw "Precise patch output differs: $($file.Path)" } }

$testRoot = Join-Path $fixture 'packages/smb/windows'
foreach ($name in @('history', 'history-controls', 'identity')) {
    Copy-Item (Join-Path $PSScriptRoot "native-smb-precise-$name`_test.go.txt") (Join-Path $testRoot "gate_precise_$($name.Replace('-', '_'))_windows_test.go")
}
Copy-Item (Join-Path $PSScriptRoot 'native-smb-precise-notify-controls_test.go.txt') (Join-Path $fixture 'packages/smb/gate_precise_notify_controls_test.go')
$authority = Join-Path $testRoot 'native_fixture_windows_test.go'
$bridge = Join-Path $testRoot 'native_acceptance_windows_test.go'
$files = Join-Path $testRoot 'native_files_windows_test.go'
$probe = Join-Path $testRoot 'gate_parent_invalidation_windows_test.go'
$wire = Join-Path $testRoot 'gate_positive_wire_windows_test.go'
Replace-Once $authority 'type nativeAuthority struct {' "type nativeAuthority struct {`nprecise *preciseHistory"
Replace-Once $bridge 'type nativeBridge struct {' "type nativeBridge struct {`nprecise *preciseHistory"
Replace-Once $bridge 'changes := smb.ChangeSource{' "b.precise = newPreciseHistory(b.backend)`nchanges := smb.ChangeSource{"
Replace-Once $bridge 'if _, err := b.smb.Publish(' "changes = b.precise.wrap(changes)`nif _, err := b.smb.Publish("
Replace-Once $bridge "`t`tif handler != nil {`n`t`t`thandler.Stop()" @'
        if b.precise != nil {
            preciseHistoryAudit(t, b.precise)
            if err := b.precise.Close(); err != nil { t.Errorf("precise history cleanup: %v", err) }
            cacheGateObserve("precise_history_closed", b.precise.Evidence())
        }
        if handler != nil {
            handler.Stop()
'@
Replace-Once $files 'a.record(other, metastore.Removed, metastore.ChangeName, a.image(other))' "preciseBeginLocked(f, r, id, other)`na.record(other, metastore.Removed, metastore.ChangeName, a.image(other))"
Replace-Once $files 'a.record(n, metastore.Renamed, metastore.ChangeName|metastore.ChangeTime, before)' "a.record(n, metastore.Renamed, metastore.ChangeName|metastore.ChangeTime, before)`npreciseEndLocked(f, id)"
Replace-Once $files 's.actions[id] = nativeReceipt{fingerprint: fingerprint, result: nativeReceiptCopy(result), err: err}' "s.actions[id] = nativeReceipt{fingerprint: fingerprint, result: nativeReceiptCopy(result), err: err}`npreciseFinalizeLocked(s, id)"
Replace-Once $probe '"github.com/codetreker/remote-fs/packages/storage"' "`"github.com/codetreker/remote-fs/packages/storage`"`n`"github.com/codetreker/remote-fs/packages/transport/httprest`""
Replace-Once $probe 'mappingCall := beginCall("map",' "if err := bridge.precise.Prepare(t.Context(), bridge.remote); err != nil { t.Fatal(`"precise snapshot:`", err) }`nmappingCall := beginCall(`"map`","
Replace-Once $probe 'q.WriteStarted = time.Now()' "if err := bridge.precise.Arm(replacement, rename, action); err != nil { t.Fatal(`"precise action binding:`", err) }`nq.WriteStarted = time.Now()"
Replace-Once $probe 'var receipt storage.FileActionReceipt' "var receipt storage.FileActionReceipt`nvar mutationBarrier *httprest.MutationBarrier"
Replace-Once $probe 'receipt, err = replacement.Rename(t.Context(), rename, action)' 'receipt, mutationBarrier, err = replacement.(httprest.FileWithBarrier).RenameWithBarrier(t.Context(), rename, action)'
Replace-Once $probe 'q.ACK = time.Now()' "q.ACK = time.Now()`nif proofErr := bridge.precise.Acknowledge(receipt, mutationBarrier, err); proofErr != nil { t.Errorf(`"precise action provenance: %v`", proofErr) }"
Replace-Once $probe 'report.NotifyOutcomes = append(report.NotifyOutcomes, outcome)' @'
report.NotifyOutcomes = append(report.NotifyOutcomes, outcome)
if detailErr := preciseRequireDetail(outcome); detailErr != nil {
    report.NotificationMode = outcome.Mode
    report.RescanRequired = outcome.Mode == parentVerifiedRescan
    return detailErr
}
'@
Replace-Once $probe "report.Wire, report.WireNotify = records, results`n" "report.Wire, report.WireNotify = records, results`npreciseIdentityAudit(t)`n"
Replace-Once $wire "`t`tif !valid {`n`t`t`tr.Incomplete =" "`t`tif valid { valid = cacheGatePreciseIdentity(member, r) }`n`t`tif !valid {`n`t`t`tr.Incomplete ="

$generated = @($authority, $bridge, $files, $probe, $wire,
    (Join-Path $testRoot 'gate_precise_history_windows_test.go'),
    (Join-Path $testRoot 'gate_precise_history_controls_windows_test.go'),
    (Join-Path $testRoot 'gate_precise_identity_windows_test.go'),
    (Join-Path $fixture 'packages/smb/gate_precise_notify_controls_test.go'))
& gofmt -w @generated
if ($LASTEXITCODE -ne 0) { throw 'Formatting generated precise fixture failed.' }
$inputNames = @('native-smb-precise-invalidation.ps1', 'native-smb-precise-history.patch', 'native-smb-precise-history_test.go.txt', 'native-smb-precise-history-controls_test.go.txt', 'native-smb-precise-notify-controls_test.go.txt', 'native-smb-precise-identity_test.go.txt', 'native-smb-parent-invalidation.ps1', 'native-smb-parent-invalidation_test.go.txt')
$inputs = @($inputNames | ForEach-Object { [ordered]@{ Path = ".github/scripts/$_"; CanonicalSHA256 = Canonical-Hash (Join-Path $PSScriptRoot $_) } })
$inputs += [ordered]@{ Path = '.github/workflows/native-smb-precise-invalidation.yml'; CanonicalSHA256 = Canonical-Hash (Join-Path $workspace '.github/workflows/native-smb-precise-invalidation.yml') }
Write-JSON 'precise-inputs.json' ([ordered]@{ SourceSHA = $env:RFS_PARENT_SOURCE_SHA; Mode = $Phase; Mechanism = 'historical_detail'; PriorControlRun = '35419230739'; Files = $inputs; PatchSHA256 = $patchHash; PatchFiles = $patchFiles })
Write-JSON 'precise-generated-source.json' @($generated | ForEach-Object { [ordered]@{ Path = [IO.Path]::GetRelativePath($fixture, $_).Replace('\', '/'); SHA256 = Canonical-Hash $_ } })
if ($Phase -eq 'PrepareFixture') { exit 0 }

Push-Location $fixture
try {
    Invoke-Controls './packages/smb' '^TestDiagnosticNotification' @(
        'TestDiagnosticNotificationQueueOwnership', 'TestDiagnosticNotificationClosePinsValidation',
        'TestDiagnosticNotificationDisclosure', 'TestDiagnosticNotificationRetirementWhileValidating',
        'TestDiagnosticNotificationCloseDrainsDiscard'
    ) 'precise-notify-controls.jsonl'
    Invoke-Controls './packages/smb/windows' '^TestPrecise|^TestParentInvalidation' @(
        'TestPreciseHistoryHTTPBinding', 'TestPreciseHistoryAcknowledgmentRefusals',
        'TestPreciseHistorySnapshotRefusals', 'TestPreciseHistoryIntervalRefusals',
        'TestPreciseHistoryMutationCaptureRefusals', 'TestPreciseHistoryProofOwnership',
        'TestPreciseHistoryShutdownRace', 'TestPreciseHistoryWholeIntervalDelivery',
        'TestPreciseHistorySnapshotMustReachSemanticEOF', 'TestPreciseHistorySnapshotSourceIncarnation',
        'TestPreciseHistoryEvidenceOwnsBoundedFacts', 'TestPreciseHistoryEvidenceBoundPreservesFirstFacts',
        'TestPreciseIdentityContexts', 'TestPreciseIdentityQueries', 'TestPreciseIdentityCorrelation', 'TestPreciseIdentityHandleBinding', 'TestPreciseIdentityInheritedSession', 'TestPreciseNotificationMode',
        'TestParentInvalidationNotifyParsing', 'TestParentInvalidationHookCorrelation',
        'TestParentInvalidationQualification'
    ) 'precise-producer-observer-controls.jsonl'
    $binary = Join-Path $results 'native-precise.test.exe'
    & go test -c -o $binary './packages/smb/windows'
    if ($LASTEXITCODE -ne 0) { throw 'Building the precise native test executable failed.' }
    $binaryHash = (Get-FileHash $binary -Algorithm SHA256).Hash.ToLowerInvariant()
    Write-JSON 'precise-native-binary.json' ([ordered]@{ SHA256 = $binaryHash; Size = (Get-Item $binary).Length; GoVersion = (& go version); SourceSHA = $env:RFS_PARENT_SOURCE_SHA; Test = 'TestNativePreciseReplacement' })
    $path = Join-Path $results 'precise-native.jsonl'
    & go tool test2json -t -p 'github.com/codetreker/remote-fs/packages/smb/windows' $binary '-test.v=test2json' '-test.count=1' '-test.timeout=3m' '-test.run=^TestNativePreciseReplacement$' 2>&1 | Tee-Object -FilePath $path
    $nativeExit = $LASTEXITCODE
    if ((Get-FileHash $binary -Algorithm SHA256).Hash.ToLowerInvariant() -ne $binaryHash) { throw 'The executed precise test binary changed during the run.' }
    Assert-Verdicts $path $nativeExit @('TestNativePreciseReplacement', 'TestNativePreciseReplacement/share0_app_first_replace_identity_precise')
} finally {
    Pop-Location
}
