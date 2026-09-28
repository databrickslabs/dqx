param(
    [Parameter(Position = 0)]
    [string]$Task,
    [string]$Profile,
    [string]$Target,
    [string]$AppName = 'dqx-studio',
    [switch]$Force,
    [string[]]$BundleVars = @()
)

$ErrorActionPreference = 'Stop'

if ($Task -ne 'app-deploy') {
    throw 'Supported target: app-deploy'
}
if ([string]::IsNullOrWhiteSpace($Profile) -or [string]::IsNullOrWhiteSpace($Target)) {
    throw 'Usage: ./make.ps1 app-deploy -Profile <databricks-profile> -Target <bundle-target> [-Force] [-BundleVars name=value,...]'
}
if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI not found on PATH. Install it from https://docs.databricks.com/dev-tools/cli/install.html"
}

$cliVersionOutput = & databricks --version
if ($LASTEXITCODE -ne 0 -or $cliVersionOutput -notmatch '(\d+\.\d+\.\d+)') {
    throw 'Could not read the Databricks CLI version.'
}
$cliVersion = [version]$Matches[1]
if ($cliVersion -lt [version]'1.4.0') {
    throw "Databricks CLI v$cliVersion is too old; v1.4.0 or later is required for DQX Studio deployment."
}

$bundleArgs = @('-p', $Profile, '-t', $Target)
$variableArgs = @()
foreach ($variable in $BundleVars) {
    $variableArgs += @('--var', $variable)
}

$previousFrozen = $env:UV_FROZEN
$previousBuildConstraint = $env:UV_BUILD_CONSTRAINT
$env:UV_FROZEN = '1'
$env:UV_BUILD_CONSTRAINT = '.build-constraints.txt'

Push-Location (Join-Path $PSScriptRoot 'app')
try {
    & uv run --exact --all-extras python scripts/build_app.py
    if ($LASTEXITCODE -ne 0) { throw 'DQX Studio build failed.' }

    $deployArgs = @('bundle', 'deploy') + $bundleArgs
    if ($Force) { $deployArgs += '--force' }
    $deployArgs += $variableArgs
    & databricks @deployArgs
    if ($LASTEXITCODE -ne 0) { throw 'DQX Studio bundle deploy failed.' }

    & databricks bundle run $AppName @bundleArgs @variableArgs
    if ($LASTEXITCODE -ne 0) { throw 'DQX Studio bundle run failed.' }
}
finally {
    Pop-Location
    if ($null -eq $previousFrozen) { Remove-Item Env:UV_FROZEN -ErrorAction SilentlyContinue }
    else { $env:UV_FROZEN = $previousFrozen }
    if ($null -eq $previousBuildConstraint) { Remove-Item Env:UV_BUILD_CONSTRAINT -ErrorAction SilentlyContinue }
    else { $env:UV_BUILD_CONSTRAINT = $previousBuildConstraint }
}
