#Requires -Version 7.0
[CmdletBinding()]
param(
    [string] $FixtureScript = [IO.Path]::Combine($PSScriptRoot, 'run-windows.ps1')
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$repo = [IO.Path]::GetFullPath([IO.Path]::Combine($PSScriptRoot, '../../..'))
$testRoot = [IO.Path]::Combine($repo, '.tmp', 'native-current-authority-fixture', 'reparse-tests')
$run = [IO.Path]::Combine($testRoot, 'runs', [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($run) | Out-Null
$environment = @{
    TEMP = [IO.Path]::Combine($testRoot, 'tmp')
    TMP = [IO.Path]::Combine($testRoot, 'tmp')
    TMPDIR = [IO.Path]::Combine($testRoot, 'tmp')
    XDG_CACHE_HOME = [IO.Path]::Combine($testRoot, 'cache')
    XDG_CONFIG_HOME = [IO.Path]::Combine($testRoot, 'config')
    XDG_DATA_HOME = [IO.Path]::Combine($testRoot, 'data')
    XDG_STATE_HOME = [IO.Path]::Combine($testRoot, 'state')
    XDG_RUNTIME_DIR = [IO.Path]::Combine($testRoot, 'runtime')
    PSModuleAnalysisCachePath = [IO.Path]::Combine($testRoot, 'cache', 'ModuleAnalysisCache')
}
$previous = @{}
$links = [Collections.Generic.List[object]]::new()
$passed = [Collections.Generic.List[string]]::new()
$providerVariable = 'RFS_FIXTURE_NONFILESYSTEM_ITEM'
$previousProvider = [Environment]::GetEnvironmentVariable($providerVariable, 'Process')

function Invoke-Case([string] $Name, [scriptblock] $Body) {
    try {
        & $Body
        $passed.Add($Name)
        [ordered]@{ case = $Name; status = 'pass' } | ConvertTo-Json -Compress
    } catch {
        [ordered]@{ case = $Name; status = 'fail'; error_id = $_.FullyQualifiedErrorId; cause = $_.Exception.Message } | ConvertTo-Json -Compress
        throw
    }
}

function Assert-Rejected([string] $Path, [string] $Message, [string] $ErrorId = '') {
    $failure = $null
    try { Assert-NoReparse $Path } catch { $failure = $_ }
    if ($null -eq $failure) { throw "Invalid path was accepted: $Path" }
    if ($Message -and $failure.Exception.Message -notlike $Message) { throw "Unexpected path failure: $($failure.Exception.Message)" }
    if ($ErrorId -and $failure.FullyQualifiedErrorId -notlike $ErrorId) { throw "Unexpected error ID: $($failure.FullyQualifiedErrorId)" }
}

try {
    foreach ($name in $environment.Keys) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        $directory = if ($name -eq 'PSModuleAnalysisCachePath') { [IO.Path]::GetDirectoryName($environment[$name]) } else { $environment[$name] }
        [IO.Directory]::CreateDirectory($directory) | Out-Null
        [Environment]::SetEnvironmentVariable($name, $environment[$name], 'Process')
    }
    $FixtureScript = [IO.Path]::GetFullPath($FixtureScript)
    $tokens, $parseErrors = $null, $null
    $ast = [Management.Automation.Language.Parser]::ParseFile($FixtureScript, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count -ne 0) { throw "Fixture script has parse errors: $($parseErrors -join '; ')" }
    $definitions = @($ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Assert-NoReparse' }, $true))
    if ($definitions.Count -ne 1) { throw 'Expected exactly one Assert-NoReparse function.' }
    . ([scriptblock]::Create($definitions[0].Extent.Text))

    $directory = [IO.Path]::Combine($run, 'parent', 'child', 'leaf')
    [IO.Directory]::CreateDirectory($directory) | Out-Null
    $file = [IO.Path]::Combine($directory, 'data.txt')
    [IO.File]::WriteAllText($file, 'fixture')

    Invoke-Case 'provider-directory-and-raw-parent-chain' {
        $entry = Get-Item -LiteralPath $directory -Force
        if ($entry -isnot [IO.DirectoryInfo] -or $null -eq $entry.PSObject.Properties['PSIsContainer']) { throw 'Expected a filesystem provider DirectoryInfo entry.' }
        if ($entry.Parent -isnot [IO.DirectoryInfo] -or $null -ne $entry.Parent.PSObject.Properties['PSIsContainer']) { throw 'Expected a raw DirectoryInfo parent without provider metadata.' }
        Assert-NoReparse $directory
    }
    Invoke-Case 'provider-file-and-raw-directory-chain' {
        $entry = Get-Item -LiteralPath $file -Force
        if ($entry -isnot [IO.FileInfo] -or $entry.Directory -isnot [IO.DirectoryInfo]) { throw 'Expected FileInfo followed by DirectoryInfo.' }
        if ($null -ne $entry.Directory.PSObject.Properties['PSIsContainer']) { throw 'Expected a raw file parent without provider metadata.' }
        Assert-NoReparse $file
    }
    Invoke-Case 'qualified-filesystem-provider' { Assert-NoReparse ("Microsoft.PowerShell.Core\FileSystem::$file") }

    foreach ($kind in @('SymbolicLink', $(if ($IsWindows) { 'Junction' }))) {
        if ($null -eq $kind) { continue }
        $link = [IO.Path]::Combine($run, "$kind-directory")
        New-Item -ItemType $kind -Path $link -Target $directory | Out-Null
        $links.Add([pscustomobject]@{ Path = $link; Directory = $true })
        Invoke-Case "$kind-directory-rejected" { Assert-Rejected $link 'Reparse path is forbidden:*' }
        Invoke-Case "$kind-file-descendant-rejected" { Assert-Rejected ([IO.Path]::Combine($link, 'data.txt')) 'Reparse path is forbidden:*' }
        $nested = [IO.Path]::Combine($directory, 'nested')
        [IO.Directory]::CreateDirectory($nested) | Out-Null
        Invoke-Case "$kind-directory-descendant-rejected" { Assert-Rejected ([IO.Path]::Combine($link, 'nested')) 'Reparse path is forbidden:*' }
    }
    $fileLink = [IO.Path]::Combine($run, 'file-link')
    New-Item -ItemType SymbolicLink -Path $fileLink -Target $file | Out-Null
    $links.Add([pscustomobject]@{ Path = $fileLink; Directory = $false })
    Invoke-Case 'file-reparse-rejected' { Assert-Rejected $fileLink 'Reparse path is forbidden:*' }

    [Environment]::SetEnvironmentVariable($providerVariable, 'fixture', 'Process')
    Invoke-Case 'non-filesystem-env-provider-rejected' { Assert-Rejected "Env:$providerVariable" 'Unsupported filesystem item type:*' }
    Invoke-Case 'missing-path-rejected' { Assert-Rejected ([IO.Path]::Combine($run, 'missing')) '' 'PathNotFound*' }
    [ordered]@{ status = 'passed'; cases = $passed.Count; platform = [Environment]::OSVersion.Platform.ToString(); junction_executed = [bool]$IsWindows } | ConvertTo-Json -Compress
} finally {
    $cleanupErrors = [Collections.Generic.List[Exception]]::new()
    foreach ($link in $links) {
        try {
            if ($link.Directory) { [IO.Directory]::Delete($link.Path) } else { [IO.File]::Delete($link.Path) }
        } catch { $cleanupErrors.Add($_.Exception) }
    }
    try { [IO.Directory]::Delete($run, $true) } catch { $cleanupErrors.Add($_.Exception) }
    foreach ($name in $previous.Keys) {
        try { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') } catch { $cleanupErrors.Add($_.Exception) }
    }
    try { [Environment]::SetEnvironmentVariable($providerVariable, $previousProvider, 'Process') } catch { $cleanupErrors.Add($_.Exception) }
    if ($cleanupErrors.Count -ne 0) { throw [AggregateException]::new('Reparse regression cleanup failed.', $cleanupErrors) }
}
