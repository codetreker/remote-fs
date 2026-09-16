param(
    [Parameter(Mandatory)]
    [ValidateSet('Prepare', 'Run')]
    [string]$Phase
)

$ErrorActionPreference = 'Stop'
$workspace = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$root = Join-Path $workspace '.tmp/native-smb-auth'
$results = Join-Path $root 'results'
$packages = @('./packages/smb', './packages/smb/internal/wire', './packages/smb/internal/signing', './packages/smb/windows')

if ($Phase -eq 'Prepare') {
    New-Item -ItemType Directory -Force -Path $results | Out-Null
    $os = Get-CimInstance Win32_OperatingSystem
    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    $source = (& git -C $workspace rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot identify the checked-out source.' }
    [ordered]@{
        EvidenceKind = 'Current checkout protocol and authentication acceptance'
        CheckoutSHA = $source
        WorkflowSHA = $env:GITHUB_SHA
        PullRequestHeadSHA = $env:RFS_PR_HEAD_SHA
        Caption = $os.Caption
        Version = $os.Version
        BuildNumber = $os.BuildNumber
        ProductType = $os.ProductType
        Architecture = $architecture
        ImageVersion = $env:ImageVersion
    } | ConvertTo-Json | Tee-Object -FilePath (Join-Path $results 'environment.json')
    if ($os.ProductType -ne 1 -or ([version]$os.Version).Major -ne 10 -or [int]$os.BuildNumber -lt 26100 -or $architecture -ne 'Arm64') {
        throw 'Native authentication acceptance requires Windows 11 24H2+ ARM64.'
    }
    foreach ($pair in @{'GOCACHE'='go-build'; 'GOMODCACHE'='go-mod'; 'GOPATH'='go-path'; 'TMPDIR'='tmp'; 'TMP'='tmp'; 'TEMP'='tmp'}.GetEnumerator()) {
        $path = Join-Path $root $pair.Value
        New-Item -ItemType Directory -Force -Path $path | Out-Null
        Add-Content $env:GITHUB_ENV -Value "$($pair.Key)=$($path.Replace('\', '/'))" -Encoding utf8
    }
    Add-Content $env:GITHUB_ENV -Value 'GOTOOLCHAIN=local' -Encoding utf8
    exit 0
}

$target = & go env GOOS GOARCH
if ($LASTEXITCODE -ne 0 -or ($target -join '/') -ne 'windows/arm64') {
    throw 'Authentication tests must execute natively on Windows ARM64.'
}
& go version | Tee-Object -FilePath (Join-Path $results 'go-version.txt')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$sourceFiles = @(& git -C $workspace ls-files packages/smb)
if ($LASTEXITCODE -ne 0 -or $sourceFiles.Count -eq 0) { throw 'Current SMB source files are missing.' }
$manifest = @($sourceFiles | ForEach-Object {
    [ordered]@{ Path = $_; SHA256 = (Get-FileHash (Join-Path $workspace $_) -Algorithm SHA256).Hash.ToLowerInvariant() }
})
ConvertTo-Json -InputObject $manifest | Set-Content (Join-Path $results 'source.json') -Encoding utf8
$PSNativeCommandUseErrorActionPreference = $false
& go test -json -count=1 -p=1 -timeout=3m -list . @packages 2>&1 |
    Tee-Object -FilePath (Join-Path $results 'test-list.jsonl')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$listEvents = @(Get-Content (Join-Path $results 'test-list.jsonl') | Where-Object { $_.StartsWith('{') } | ForEach-Object { $_ | ConvertFrom-Json })
$expected = @($listEvents | Where-Object { $_.Action -eq 'output' -and $_.Output.Trim() -match '^(Test|Fuzz)[A-Za-z0-9_]+$' } | ForEach-Object {
    [ordered]@{ Package = $_.Package; Test = $_.Output.Trim() }
})
$listedPackages = @($expected | ForEach-Object { $_.Package } | Sort-Object -Unique)
if ($listedPackages.Count -ne $packages.Count) { throw 'Every selected package must list tests.' }
$coverage = Join-Path $results 'coverage.out'
& go test -json -count=1 -p=1 -timeout=3m "-coverprofile=$coverage" @packages 2>&1 |
    Tee-Object -FilePath (Join-Path $results 'test-run.jsonl')
$testExit = $LASTEXITCODE
$events = @(Get-Content (Join-Path $results 'test-run.jsonl') | Where-Object { $_.StartsWith('{') } | ForEach-Object { $_ | ConvertFrom-Json })
$problems = [Collections.Generic.List[string]]::new()
if ($testExit -ne 0) { $problems.Add("go test exited $testExit") }
foreach ($event in $events) {
    if ($event.Action -in @('fail', 'skip')) { $problems.Add("$($event.Action): $($event.Package) $($event.Test)") }
}
foreach ($item in $expected) {
    $verdict = @($events | Where-Object { $_.Package -eq $item.Package -and $_.Test -eq $item.Test -and $_.Action -in @('pass', 'fail', 'skip') })
    if ($verdict.Count -ne 1 -or $verdict[0].Action -ne 'pass') { $problems.Add("Missing passing verdict: $($item.Package) $($item.Test)") }
}
foreach ($package in $listedPackages) {
    $verdict = @($events | Where-Object { $_.Package -eq $package -and -not $_.Test -and $_.Action -in @('pass', 'fail', 'skip') })
    if ($verdict.Count -ne 1 -or $verdict[0].Action -ne 'pass') { $problems.Add("Missing package verdict: $package") }
}
foreach ($native in @('TestNativeSSPINegotiateAuthenticatesCurrentWindowsIdentity', 'TestNativeSSPICancellationRetiresItsContext')) {
    if (@($expected | Where-Object { $_.Package -eq 'github.com/codetreker/remote-fs/packages/smb/windows' -and $_.Test -eq $native }).Count -ne 1) {
        $problems.Add("Native SSPI test was not listed: $native")
    }
}

$packageCoverage = @{}
$totalStatements = 0L
$coveredStatements = 0L
if (Test-Path $coverage) {
    foreach ($line in Get-Content $coverage) {
        if ($line -eq 'mode: set' -or $line -eq 'mode: count' -or $line -eq 'mode: atomic') { continue }
        if ($line -notmatch '^(?<file>.+)/[^/]+\.go:\d+\.\d+,\d+\.\d+ (?<statements>\d+) (?<count>\d+)$') {
            $problems.Add('Unrecognized native coverage record.')
            continue
        }
        $package = $Matches.file
        $statements = [long]$Matches.statements
        $covered = if ([long]$Matches.count -gt 0) { $statements } else { 0L }
        if (-not $packageCoverage.ContainsKey($package)) { $packageCoverage[$package] = @{ Covered = 0L; Statements = 0L } }
        $packageCoverage[$package].Covered += $covered
        $packageCoverage[$package].Statements += $statements
        $coveredStatements += $covered
        $totalStatements += $statements
    }
    foreach ($package in $listedPackages) {
        if (-not $packageCoverage.ContainsKey($package) -or $packageCoverage[$package].Statements -eq 0) {
            $problems.Add("Missing own-package native coverage: $package")
        } elseif (100.0 * $packageCoverage[$package].Covered / $packageCoverage[$package].Statements -lt 70) {
            $problems.Add("Own-package coverage below 70%: $package")
        }
    }
    if ($totalStatements -eq 0 -or 100.0 * $coveredStatements / $totalStatements -lt 85) {
        $problems.Add('Aggregate native coverage is below 85%.')
    }
    & go tool cover "-func=$coverage" | Tee-Object -FilePath (Join-Path $results 'functions.txt')
    if ($LASTEXITCODE -ne 0) { $problems.Add('Cannot inspect native function coverage.') }
    foreach ($line in Get-Content (Join-Path $results 'functions.txt')) {
        if ($line -match '^total:') { continue }
        if ($line -notmatch '\s+(?<percent>\d+(?:\.\d+)?)%\s*$') {
            $problems.Add('Unrecognized native function coverage record.')
        } elseif ([double]::Parse($Matches.percent, [Globalization.CultureInfo]::InvariantCulture) -lt 50) {
            $problems.Add("Function coverage below 50%: $line")
        }
    }
} else { $problems.Add('Native coverage profile is missing.') }
foreach ($entry in $manifest) {
    if ((Get-FileHash (Join-Path $workspace $entry.Path) -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.SHA256) {
        $problems.Add("Source changed during native acceptance: $($entry.Path)")
    }
}
[ordered]@{
    EvidenceKind = 'Current checkout protocol and authentication acceptance'
    TestExit = $testExit
    ExpectedRoots = $expected.Count
    PassingVerdicts = @($events | Where-Object { $_.Action -eq 'pass' -and $_.Test }).Count
    PackageCoverage = $packageCoverage
    CoveredStatements = $coveredStatements
    TotalStatements = $totalStatements
    Problems = @($problems)
} | ConvertTo-Json -Depth 5 | Tee-Object -FilePath (Join-Path $results 'receipt.json')
if ($problems.Count -ne 0) { throw ($problems -join "`n") }
