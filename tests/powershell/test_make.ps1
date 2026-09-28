$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$scriptPath = Join-Path $repoRoot 'make.ps1'
$global:Calls = @()
$global:CliVersion = 'Databricks CLI v1.10.0'
$global:FailCommand = ''
$previousFrozen = $env:UV_FROZEN
$previousBuildConstraint = $env:UV_BUILD_CONSTRAINT

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-Throws {
    param([scriptblock]$Action, [string]$Message)
    $threw = $false
    try { & $Action } catch { $threw = $true }
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
