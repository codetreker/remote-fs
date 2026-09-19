[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string] $ArtifactDirectory,
    [string] $RunId = ([Guid]::NewGuid().ToString('N')),
    [switch] $CurrentSMBCold,
    [switch] $RequireNativeAcceptance
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $IsWindows) { throw 'This fixture requires PowerShell 7 on Windows.' }
if ([Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne [Runtime.InteropServices.Architecture]::Arm64) {
    throw 'This fixture requires a Windows ARM64 host.'
}
if ($RunId -notmatch '^[A-Za-z0-9_-]{1,96}$') { throw 'RunId must contain 1-96 ASCII letters, digits, underscores, or hyphens.' }
if ($RequireNativeAcceptance) { throw 'Current adapter native acceptance is unavailable; fixture readiness cannot satisfy it.' }

$repo = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$sourceSha = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot read checkout commit.' }
$treeSha = (& git -C $repo rev-parse 'HEAD^{tree}').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot read checkout tree.' }
$dirty = & git -C $repo status --porcelain --untracked-files=all
if ($LASTEXITCODE -ne 0 -or $dirty) { throw 'The fixture requires a clean checkout, including untracked inputs.' }
if ($env:GITHUB_ACTIONS -eq 'true') {
    if ($env:RFS_FIXTURE_SOURCE_SHA -cnotmatch '^[0-9a-f]{40}$') {
        throw 'GitHub Actions requires RFS_FIXTURE_SOURCE_SHA to identify the exact workflow source commit.'
    }
    if ($env:RFS_FIXTURE_SOURCE_SHA -cne $sourceSha) { throw 'Checkout commit differs from RFS_FIXTURE_SOURCE_SHA.' }
}

$root = Join-Path $repo '.tmp/native-current-authority-fixture'
$artifact = (Resolve-Path $ArtifactDirectory).Path
$rootPrefix = [IO.Path]::GetFullPath($root) + [IO.Path]::DirectorySeparatorChar
if (-not $artifact.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Artifacts must be downloaded under the repository .tmp/native-current-authority-fixture directory.'
}
function Assert-NoReparse([string] $Path) {
    $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    while ($null -ne $item) {
        if ($item -is [IO.DirectoryInfo]) {
            $parent = $item.Parent
        } elseif ($item -is [IO.FileInfo]) {
            $parent = $item.Directory
        } else {
            throw "Unsupported filesystem item type: $($item.GetType().FullName)"
        }
        if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Reparse path is forbidden: $($item.FullName)" }
        $item = $parent
    }
}
Assert-NoReparse $artifact
$manifest = Get-Content -LiteralPath (Join-Path $artifact 'manifest.json') -Raw | ConvertFrom-Json
if ($manifest.format -ne 1 -or $manifest.source_sha -ne $sourceSha -or $manifest.source_tree_sha -ne $treeSha -or $manifest.source_dirty) {
    throw 'Artifact does not belong to this clean checkout.'
}
$controllerEntry = $manifest.artifacts.controller_windows_arm64
if ($controllerEntry.path -cne 'host/controller-windows-arm64.exe') { throw 'Invalid controller artifact path.' }
$controller = Join-Path $artifact $controllerEntry.path
Assert-NoReparse $controller
if ((Get-FileHash -LiteralPath $controller -Algorithm SHA256).Hash.ToLowerInvariant() -ne $controllerEntry.sha256 -or (Get-Item -LiteralPath $controller).Length -ne $controllerEntry.size) {
    throw 'Controller artifact hash or size mismatch.'
}

$cache = Join-Path $root 'qemu-arm64-cache'
$package = Join-Path $cache 'qemu-arm64-11.1.0'
$bootstrap = Join-Path $root "bootstrap-$RunId"
New-Item -ItemType Directory -Path $cache -Force | Out-Null
Assert-NoReparse $cache
if (Test-Path -LiteralPath $bootstrap) { throw 'Bootstrap run directory already exists.' }
& $controller -prepare-directory $bootstrap -repo-root $repo
if ($LASTEXITCODE -ne 0) { throw "Cannot create private bootstrap directory: exit $LASTEXITCODE." }
$audit = Join-Path $root "packaging-evidence/$RunId"
New-Item -ItemType Directory -Path $audit | Out-Null
$lockPath = Join-Path $cache 'prepare.lock'
$lock = [IO.File]::Open($lockPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
$oldTemp, $oldTmp = $env:TEMP, $env:TMP
$env:TEMP, $env:TMP = $bootstrap, $bootstrap
$archive = Join-Path $cache 'qemu-arm-setup-20260811.exe'
$archiveHash = '51db3e9f8afe9e3046ecab114d5c4718f6f8844e956dd1cb60591a903eb99b74feaf7cf7eb295cc2faacc0b2befd19a646a58a86db980be4dbf1b4a7d9d8d6a7'
try {
    if (-not (Test-Path -LiteralPath $archive)) {
        $partial = Join-Path $bootstrap 'qemu-download.partial'
        Invoke-WebRequest -Uri 'https://qemu.weilnetz.de/aarch64/qemu-arm-setup-20260811.exe' -OutFile $partial -TimeoutSec 180 -MaximumRetryCount 1 -RetryIntervalSec 2
        if ((Get-FileHash -LiteralPath $partial -Algorithm SHA512).Hash.ToLowerInvariant() -ne $archiveHash) { throw 'Downloaded QEMU archive SHA512 mismatch.' }
        Move-Item -LiteralPath $partial -Destination $archive
    }
    Assert-NoReparse $archive
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA512).Hash.ToLowerInvariant() -ne $archiveHash) { throw 'Cached QEMU archive SHA512 mismatch.' }
    if (-not (Test-Path -LiteralPath $package)) {
        $sevenZip = (Get-Command '7z.exe' -ErrorAction Stop).Source
        $sevenVersion = (Get-Item -LiteralPath $sevenZip).VersionInfo.FileVersion
        if (-not $sevenVersion) { throw 'Cannot identify the installed 7-Zip extractor.' }
        $extract = Join-Path $bootstrap 'extracted'
        # The verified Go controller owns 7-Zip's entire process tree.
        & $controller -extract-archive $archive -extractor $sevenZip -extract-dir $bootstrap
        if ($LASTEXITCODE -ne 0) { throw "Pinned archive extraction failed with exit code $LASTEXITCODE." }
        Get-ChildItem -LiteralPath $extract -Recurse -Force | ForEach-Object {
            if ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "Archive contains a reparse path: $($_.FullName)" }
        }
        $executables = @(Get-ChildItem -LiteralPath $extract -Recurse -File -Filter 'qemu-system-x86_64.exe')
        $biosFiles = @(Get-ChildItem -LiteralPath $extract -Recurse -File -Filter 'bios-256k.bin')
        if ($executables.Count -ne 1 -or $biosFiles.Count -ne 1) { throw 'Archive must contain one x86-64 guest-target QEMU executable and one BIOS data tree.' }
        $files = [ordered]@{}
        Get-ChildItem -LiteralPath $extract -Recurse -File -Force | Sort-Object FullName | ForEach-Object {
            $relative = [IO.Path]::GetRelativePath($extract, $_.FullName).Replace('\', '/')
            $files[$relative] = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
        $packageManifest = [ordered]@{
            format = 1
            archive_sha512 = $archiveHash
            executable = [IO.Path]::GetRelativePath($extract, $executables[0].FullName).Replace('\', '/')
            data_dir = [IO.Path]::GetRelativePath($extract, $biosFiles[0].DirectoryName).Replace('\', '/')
            seven_zip_version = $sevenVersion
            files = $files
        }
        $packageManifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $extract 'package-manifest.json') -Encoding utf8NoBOM
        Move-Item -LiteralPath $extract -Destination $package
    }
    Assert-NoReparse $package
    $controllerArguments = @('-repo-root', $repo, '-artifact-dir', $artifact, '-package-dir', $package,
        '-source-sha', $sourceSha, '-source-tree-sha', $treeSha, '-run-id', $RunId)
    if ($CurrentSMBCold) { $controllerArguments += '-current-smb-cold' }
    & $controller @controllerArguments
    if ($LASTEXITCODE -ne 0) { throw "Fixture controller failed with exit code $LASTEXITCODE." }
 } catch {
    [ordered]@{ run_id = $RunId; source_sha = $sourceSha; phase = 'windows-bootstrap'; subsystem = 'fixture-packaging'; category = 'failure'; cause = $_.Exception.Message; native_acceptance = 'not-run' } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $audit 'failure.json') -Encoding utf8NoBOM
    throw
} finally {
    $cleanupErrors = [Collections.Generic.List[Exception]]::new()
    foreach ($action in @(
        { Get-ChildItem -LiteralPath $bootstrap -Filter '*-stdout.log' -File | Move-Item -Destination $audit },
        { Get-ChildItem -LiteralPath $bootstrap -Filter '*-stderr.log' -File | Move-Item -Destination $audit },
        { $env:TEMP, $env:TMP = $oldTemp, $oldTmp },
        { $lock.Dispose() },
        { Remove-Item -LiteralPath $lockPath -Force },
        { Remove-Item -LiteralPath $bootstrap -Recurse -Force }
    )) {
        try { & $action } catch { $cleanupErrors.Add($_.Exception) }
    }
    if ($cleanupErrors.Count -gt 0) { throw [AggregateException]::new('Fixture bootstrap cleanup failed.', $cleanupErrors) }
}
