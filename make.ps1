param(
    [Parameter(Position = 0)]
    [string]$Task,
    [string]$Profile,
    [string]$Target,
    [switch]$Release,
    [string]$AppName = 'dqx-studio',
    [switch]$Force,
    [string[]]$BundleVars = @()
)

$ErrorActionPreference = 'Stop'

if ($Task -ne 'app-deploy') {
    throw 'Supported target: app-deploy'
}
if ($Release) {
    if ($Target -and $Target -ne 'release') {
        throw '-Release cannot be combined with a different -Target.'
    }
    $Target = 'release'
}
if ([string]::IsNullOrWhiteSpace($Profile) -or [string]::IsNullOrWhiteSpace($Target)) {
    throw 'Usage: ./make.ps1 app-deploy -Profile <databricks-profile> (-Target <bundle-target> | -Release) [-Force] [-BundleVars name=value,...]'
}
if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    throw "Databricks CLI not found on PATH. Install it from https://docs.databricks.com/dev-tools/cli/install.html"
}

$cliVersionOutput = (& databricks --version) -join "`n"
if ($LASTEXITCODE -ne 0 -or $cliVersionOutput -notmatch '(\d+\.\d+\.\d+)') {
    throw 'Could not read the Databricks CLI version.'
}
$cliVersion = [version]$Matches[1]
if ($cliVersion -lt [version]'1.4.0') {
    throw "Databricks CLI v$cliVersion is too old; v1.4.0 or later is required for DQX Studio deployment."
}
if ($Target -ne 'release' -and -not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw 'uv not found on PATH. Install uv before building DQX Studio from source.'
}

$bundleArgs = @('-p', $Profile, '-t', $Target)
$variableArgs = @()
foreach ($variable in $BundleVars) {
    $variableArgs += @('--var', $variable)
}

$previousFrozen = $env:UV_FROZEN
$previousBuildConstraint = $env:UV_BUILD_CONSTRAINT
$locationPushed = $false

try {
    Push-Location (Join-Path $PSScriptRoot 'app')
    $locationPushed = $true
    if ($Target -ne 'release') {
        $env:UV_FROZEN = '1'
        $env:UV_BUILD_CONSTRAINT = '.build-constraints.txt'
        & uv run --exact --all-extras python scripts/build_app.py
        if ($LASTEXITCODE -ne 0) { throw 'DQX Studio build failed.' }
    }

    $deployArgs = @('bundle', 'deploy') + $bundleArgs
    if ($Force) { $deployArgs += '--force' }
    $deployArgs += $variableArgs
    & databricks @deployArgs
    if ($LASTEXITCODE -ne 0) { throw 'DQX Studio bundle deploy failed.' }

    & databricks bundle run $AppName @bundleArgs @variableArgs
    if ($LASTEXITCODE -ne 0) { throw 'DQX Studio bundle run failed.' }
}
finally {
    try {
        if ($locationPushed) { Pop-Location }
    }
    finally {
        if ($null -eq $previousFrozen) { Remove-Item Env:UV_FROZEN -ErrorAction SilentlyContinue }
        else { $env:UV_FROZEN = $previousFrozen }
        if ($null -eq $previousBuildConstraint) { Remove-Item Env:UV_BUILD_CONSTRAINT -ErrorAction SilentlyContinue }
        else { $env:UV_BUILD_CONSTRAINT = $previousBuildConstraint }
    }
}
