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

function Assert-ColdContextRejected([hashtable] $Context, [string] $Message) {
    $failure = $null
    try { Assert-CurrentSMBColdContext @Context } catch { $failure = $_ }
    if ($null -eq $failure) { throw 'Unsupported cold context was accepted.' }
    if ($failure.Exception.Message -notlike $Message) { throw "Unexpected cold-context failure: $($failure.Exception.Message)" }
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
    foreach ($functionName in @('Assert-NoReparse', 'Assert-CurrentSMBColdContext')) {
        $definitions = @($ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $functionName }, $true))
        if ($definitions.Count -ne 1) { throw "Expected exactly one $functionName function." }
        . ([scriptblock]::Create($definitions[0].Extent.Text))
    }

    Invoke-Case 'cold-context-admission-before-bootstrap' {
        $calls = @($ast.FindAll({ param($node) $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Assert-CurrentSMBColdContext' }, $true))
        if ($calls.Count -ne 1) { throw 'Expected one cold-context admission call.' }
        $condition = $calls[0].Parent
        while ($null -ne $condition -and $condition -isnot [Management.Automation.Language.IfStatementAst]) { $condition = $condition.Parent }
        if ($null -eq $condition -or $condition.Clauses.Count -ne 1 -or $condition.Clauses[0].Item1.Extent.Text -cne '$CurrentSMBCold') {
            throw 'Cold-context admission must guard CurrentSMBCold.'
        }
        $expectedCall = @('Assert-CurrentSMBColdContext', '-Repository', '$repo', '-Artifact', '$artifact', '-Identity', '$RunId', '-Environment', "([Environment]::GetEnvironmentVariables('Process'))")
        $elements = $calls[0].CommandElements
        if ($elements.Count -ne $expectedCall.Count) { throw 'Cold-context admission argument count differs.' }
        for ($index = 0; $index -lt $elements.Count; $index++) {
            if ($elements[$index].Extent.Text -cne $expectedCall[$index]) { throw 'Cold-context admission must bind the actual run, artifact and process environment.' }
        }
        $bootstrapWrites = @($ast.FindAll({ param($node) $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'New-Item' }, $true))
        if ($bootstrapWrites.Count -eq 0) { throw 'Expected existing bootstrap directory writes.' }
        foreach ($write in $bootstrapWrites) {
            if ($write.Extent.StartOffset -lt $condition.Extent.EndOffset) { throw 'Bootstrap write precedes cold-context admission.' }
        }
    }

    $coldEnvironment = @{
        GITHUB_ACTIONS = 'true'
        RFS_FIXTURE_RUNNER_ENVIRONMENT = 'github-hosted'
        GITHUB_RUN_ID = '123456789'
        GITHUB_RUN_ATTEMPT = '2'
    }
    $coldContext = @{
        Repository = $run
        Artifact = [IO.Path]::Combine($run, '.tmp', 'native-current-authority-fixture', 'artifact')
        Identity = '123456789-2'
        Environment = $coldEnvironment
    }
    Invoke-Case 'cold-context-canonical-hosted-attempt' {
        $before = $coldEnvironment.Clone()
        Assert-CurrentSMBColdContext @coldContext
        if ($coldEnvironment.Count -ne $before.Count) { throw 'Cold-context check changed its environment input.' }
        foreach ($key in $before.Keys) {
            if ($coldEnvironment[$key] -cne $before[$key]) { throw "Cold-context check changed $key." }
        }
        if ([IO.Directory]::Exists($coldContext.Artifact)) { throw 'Cold-context check created an artifact directory.' }
    }
    foreach ($case in @(
        @{ Name = 'manual'; Key = 'GITHUB_ACTIONS'; Value = $null },
        @{ Name = 'actions-false'; Key = 'GITHUB_ACTIONS'; Value = 'false' },
        @{ Name = 'actions-case'; Key = 'GITHUB_ACTIONS'; Value = 'True' },
        @{ Name = 'missing-runner'; Key = 'RFS_FIXTURE_RUNNER_ENVIRONMENT'; Value = $null },
        @{ Name = 'self-hosted'; Key = 'RFS_FIXTURE_RUNNER_ENVIRONMENT'; Value = 'self-hosted' },
        @{ Name = 'runner-case'; Key = 'RFS_FIXTURE_RUNNER_ENVIRONMENT'; Value = 'GitHub-Hosted' }
    )) {
        Invoke-Case "cold-context-$($case.Name)-rejected" {
            $invalid = $coldContext.Clone()
            $invalid.Environment = $coldEnvironment.Clone()
            $invalid.Environment[$case.Key] = $case.Value
            Assert-ColdContextRejected $invalid 'CurrentSMBCold requires a fresh GitHub-hosted Actions job*'
        }
    }
    foreach ($key in @('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')) {
        foreach ($case in @(
            @{ Name = 'missing'; Value = $null },
            @{ Name = 'zero'; Value = '0' },
            @{ Name = 'leading-zero'; Value = '02' },
            @{ Name = 'negative'; Value = '-2' },
            @{ Name = 'suffix'; Value = '2x' },
            @{ Name = 'newline'; Value = "2`n" }
        )) {
            Invoke-Case "cold-context-$key-$($case.Name)-rejected" {
                $invalid = $coldContext.Clone()
                $invalid.Environment = $coldEnvironment.Clone()
                $invalid.Environment[$key] = $case.Value
                $invalid.Identity = "$($invalid.Environment.GITHUB_RUN_ID)-$($invalid.Environment.GITHUB_RUN_ATTEMPT)"
                Assert-ColdContextRejected $invalid 'CurrentSMBCold RunId must equal the current GitHub run ID and attempt.'
            }
        }
    }
    foreach ($identity in @('123456789-1', '123456788-2', 'manual', '123456789-2-extra')) {
        Invoke-Case "cold-context-run-$identity-rejected" {
            $invalid = $coldContext.Clone()
            $invalid.Identity = $identity
            Assert-ColdContextRejected $invalid 'CurrentSMBCold RunId must equal the current GitHub run ID and attempt.'
        }
    }
    foreach ($case in @(
        @{ Name = 'alternate'; Path = [IO.Path]::Combine($run, '.tmp', 'native-current-authority-fixture', 'artifact-other') },
        @{ Name = 'descendant'; Path = [IO.Path]::Combine($coldContext.Artifact, 'child') },
        @{ Name = 'other-root'; Path = [IO.Path]::Combine($run, 'other', '.tmp', 'native-current-authority-fixture', 'artifact') },
        @{ Name = 'relative'; Path = '.tmp/native-current-authority-fixture/artifact' }
    )) {
        Invoke-Case "cold-context-artifact-$($case.Name)-rejected" {
            $invalid = $coldContext.Clone()
            $invalid.Artifact = $case.Path
            Assert-ColdContextRejected $invalid 'CurrentSMBCold requires the canonical .tmp/native-current-authority-fixture/artifact directory.'
        }
    }

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
