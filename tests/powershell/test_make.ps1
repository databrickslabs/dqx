$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$scriptPath = Join-Path $repoRoot 'make.ps1'
$global:Calls = @()
$global:CliVersion = 'Databricks CLI v1.10.0'
$global:FailCommand = ''
$global:HideUv = $false
$global:FailPush = $false
$previousFrozen = $env:UV_FROZEN
$previousBuildConstraint = $env:UV_BUILD_CONSTRAINT

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-Throws {
    param([scriptblock]$Action, [string]$Message, [string]$ExpectedError = '')
    $threw = $false
    try { & $Action } catch {
        $threw = $true
        if ($ExpectedError) {
            Assert-True ($_.Exception.Message -like "*$ExpectedError*") "$Message (wrong error: $($_.Exception.Message))"
        }
    }
    Assert-True $threw $Message
}

function global:databricks {
    $global:Calls += [pscustomobject]@{ Command = 'databricks'; Arguments = @($args); Directory = (Get-Location).Path }
    if ($args[0] -eq '--version') { $global:CliVersion }
    $global:LASTEXITCODE = if ($global:FailCommand -eq "databricks $($args[1])") { 1 } else { 0 }
}

function global:uv {
    $global:Calls += [pscustomobject]@{
        Command = 'uv'
        Arguments = @($args)
        Directory = (Get-Location).Path
        Frozen = $env:UV_FROZEN
        BuildConstraint = $env:UV_BUILD_CONSTRAINT
    }
    $global:LASTEXITCODE = if ($global:FailCommand -eq 'uv') { 1 } else { 0 }
}

function global:Get-Command {
    [CmdletBinding()]
    param([string]$Name)
    if ($Name -eq 'uv' -and $global:HideUv) { return $null }
    Microsoft.PowerShell.Core\Get-Command $Name
}

function global:Push-Location {
    [CmdletBinding()]
    param([string]$Path)
    if ($global:FailPush) { throw 'Simulated missing app directory' }
    Microsoft.PowerShell.Management\Push-Location $Path
}

Assert-Throws { & $scriptPath app-deploy -Profile '' -Target dev } 'Empty profile must fail'
Assert-True ($global:Calls.Count -eq 0) 'Validation must run before external commands'

& $scriptPath app-deploy -Profile test-profile -Target dev -Force -BundleVars @('catalog_name=sample', 'label=two words')
Assert-True ($global:Calls.Count -eq 4) 'Expected CLI check, build, deploy, and run'
Assert-True ($global:Calls[0].Arguments -join ' ' -eq '--version') 'CLI version must be checked first'
Assert-True ($global:Calls[1].Arguments -join ' ' -eq 'run --exact --all-extras python scripts/build_app.py') 'App build command must match Makefile'
Assert-True ($global:Calls[2].Arguments -join '|' -eq 'bundle|deploy|-p|test-profile|-t|dev|--force|--var|catalog_name=sample|--var|label=two words') 'Deploy flags must be forwarded as separate arguments'
Assert-True ($global:Calls[3].Arguments -join '|' -eq 'bundle|run|dqx-studio|-p|test-profile|-t|dev|--var|catalog_name=sample|--var|label=two words') 'Run flags must match deploy flags without force'
Assert-True ($global:Calls[1].Directory -eq (Join-Path $repoRoot 'app')) 'Build must run from app directory'
Assert-True ($global:Calls[2].Directory -eq (Join-Path $repoRoot 'app')) 'Bundle deploy must run from app directory'
Assert-True ($global:Calls[1].Frozen -eq '1') 'Build must not update the lockfile'
Assert-True ($global:Calls[1].BuildConstraint -eq '.build-constraints.txt') 'Build must use app constraints'
Assert-True ((Get-Location).Path -eq $repoRoot) 'Caller directory must be restored'
Assert-True ($previousFrozen -eq $env:UV_FROZEN) 'UV_FROZEN must be restored'
Assert-True ($previousBuildConstraint -eq $env:UV_BUILD_CONSTRAINT) 'UV_BUILD_CONSTRAINT must be restored'

$global:Calls = @()
$global:HideUv = $true
& $scriptPath app-deploy -Profile test-profile -Release -BundleVars @('catalog_name=sample')
Assert-True ($global:Calls.Count -eq 3) 'Release must check CLI, deploy, and run without building'
Assert-True ($global:Calls[1].Arguments -join '|' -eq 'bundle|deploy|-p|test-profile|-t|release|--var|catalog_name=sample') 'Release must deploy the release target'
Assert-True ($global:Calls[2].Arguments -join '|' -eq 'bundle|run|dqx-studio|-p|test-profile|-t|release|--var|catalog_name=sample') 'Release must run the release target'
$global:HideUv = $false

Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev -Release } 'Conflicting target and release flag must fail' '-Release cannot be combined'

$global:Calls = @()
& $scriptPath app-deploy -Profile test-profile -Target release
Assert-True ($global:Calls.Count -eq 3) 'Explicit release target must skip the build too'

$global:Calls = @()
$global:CliVersion = @('Databricks CLI', 'v1.10.0')
& $scriptPath app-deploy -Profile test-profile -Release
Assert-True ($global:Calls.Count -eq 3) 'Multiline CLI version must be parsed before release deploy'

$global:Calls = @()
$global:HideUv = $true
Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev } 'Source deploy must reject missing uv' 'uv not found on PATH'
Assert-True ($global:Calls.Count -eq 1) 'Missing uv must fail before the build'
$global:HideUv = $false

$global:Calls = @()
$global:FailPush = $true
Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev } 'Directory change failure must stop deploy' 'Simulated missing app directory'
Assert-True ($previousFrozen -eq $env:UV_FROZEN) 'UV_FROZEN must be restored if Push-Location fails'
Assert-True ($previousBuildConstraint -eq $env:UV_BUILD_CONSTRAINT) 'UV_BUILD_CONSTRAINT must be restored if Push-Location fails'
$global:FailPush = $false

$global:Calls = @()
$global:CliVersion = 'Databricks CLI v1.3.9'
Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev } 'Old CLI must fail'
Assert-True ($global:Calls.Count -eq 1) 'Old CLI must fail before build'

$global:Calls = @()
$global:CliVersion = 'Databricks CLI v1.10.0'
$global:FailCommand = 'uv'
Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev } 'Build failure must stop deploy'
Assert-True ($global:Calls.Count -eq 2) 'Deploy must not run after build failure'

$global:Calls = @()
$global:FailCommand = 'databricks deploy'
Assert-Throws { & $scriptPath app-deploy -Profile test-profile -Target dev } 'Deploy failure must stop run'
Assert-True ($global:Calls.Count -eq 3) 'Run must not start after deploy failure'

Write-Host 'PowerShell deployment tests passed.'
