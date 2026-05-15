<#
.SYNOPSIS
    Runs the test suite against a specified target backend.
.PARAMETER Target
    Backend to test against: inmemory, emulator-linux, or emulator-windows.
.PARAMETER Project
    Which test project(s) to run: unit, integration, or both. Default: both.
.PARAMETER Framework
    Target framework. Default net8.0.
.PARAMETER Filter
    Additional dotnet test filter expression.
.PARAMETER OutputDir
    Directory for TRX output files. Default ./test-results.
.PARAMETER EmulatorEndpoint
    Override the emulator endpoint URL. When unset, defaults to https://localhost:8081
    for emulator targets. Use http://localhost:8081 for the vnext-preview image.
.EXAMPLE
    .\scripts\run-tests.ps1 -Target inmemory
    .\scripts\run-tests.ps1 -Target emulator-linux -Project integration
    .\scripts\run-tests.ps1 -Target emulator-linux -EmulatorEndpoint http://localhost:8081
    .\scripts\run-tests.ps1 -Target inmemory -Project unit -Filter "FullyQualifiedName~Crud"
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('inmemory', 'emulator-linux', 'emulator-windows')]
    [string]$Target,

    [ValidateSet('unit', 'integration', 'both')]
    [string]$Project = 'both',

    [string]$Framework = 'net8.0',
    [string]$Filter,
    [string]$OutputDir = './test-results',
    [string]$EmulatorEndpoint
)

$ErrorActionPreference = 'Stop'
$env:COSMOS_TEST_TARGET = $Target

# Set emulator endpoint — default is HTTPS (legacy emulator). Pass -EmulatorEndpoint to override.
if ($Target -ne 'inmemory') {
    $env:COSMOS_EMULATOR_ENDPOINT = if ($EmulatorEndpoint) { $EmulatorEndpoint } else { 'https://localhost:8081' }
} else {
    Remove-Item Env:COSMOS_EMULATOR_ENDPOINT -ErrorAction SilentlyContinue
}

# Build filter: exclude InMemoryOnly tests when targeting emulator
$filterExpr = ''
if ($Target -ne 'inmemory') {
    $filterExpr = 'Target!=InMemoryOnly'
}
if ($Filter) {
    if ($filterExpr -and $Filter -match '\|') {
        # Distribute the base filter across each OR segment to maintain correct precedence.
        # Without this: Target!=InMemoryOnly&A|B|C evaluates as (Target!=InMemoryOnly&A)|B|C
        # With this:    Target!=InMemoryOnly&A|Target!=InMemoryOnly&B|Target!=InMemoryOnly&C
        $filterExpr = ($Filter -split '\|' | ForEach-Object { "$filterExpr&$_" }) -join '|'
    } elseif ($filterExpr) {
        $filterExpr = "$filterExpr&$Filter"
    } else {
        $filterExpr = $Filter
    }
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

# Pre-create every emulator-backed test container so the 503/1007 ("high demand in
# this region") and 404/1013 ("collection is not yet available for read") warmup
# window is absorbed once here instead of being charged to per-test fixtures.
# Without this, every test class that creates a container can wait minutes for the
# emulator's partition services + read replica to come online — fast tests stack
# up to the 45m CI job timeout. We only need this when integration tests are about
# to run against a real emulator.
$needsIntegrationWarmup = ($Target -ne 'inmemory') -and ($Project -in @('integration', 'both'))
if ($needsIntegrationWarmup) {
    $manifestPath = Join-Path $PSScriptRoot '..' 'tests' 'emulator-containers.json'
    $warmupProject = Join-Path $PSScriptRoot '..' 'tests' 'EmulatorWarmup'
    Write-Host "`nPre-creating emulator containers from manifest..." -ForegroundColor Cyan
    # `dotnet run` self-builds if needed — the warmup tool isn't in the solution,
    # so it's not covered by the workflow's solution-level build step.
    & dotnet run --project $warmupProject --configuration Release -- $manifestPath
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Emulator warmup failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# Determine which projects to run
$projects = switch ($Project) {
    'unit'        { @(@{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Unit';        Label = 'unit' }) }
    'integration' { @(@{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'; Label = 'integration' }) }
    'both'        { @(
        @{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Unit';        Label = 'unit' }
        @{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'; Label = 'integration' }
    )}
}

$overallExit = 0
foreach ($proj in $projects) {
    $trxFile = "$Target-$($proj.Label)-results.trx"

    Write-Host "`nRunning $($proj.Label) tests against '$Target' (framework: $Framework)..." -ForegroundColor Cyan
    if ($filterExpr) { Write-Host "  Filter: $filterExpr" -ForegroundColor DarkGray }

    $testArgs = @(
        'test', $proj.Path,
        '--configuration', 'Release',
        '--framework', $Framework,
        '--no-build',
        '--logger', "trx;LogFileName=$trxFile",
        '--results-directory', $OutputDir
    )
    if ($filterExpr) {
        $testArgs += '--filter'
        $testArgs += $filterExpr
    }

    # Disable parallel test collections for emulator targets to avoid overwhelming the emulator
    if ($Target -ne 'inmemory') {
        $testArgs += '--'
        $testArgs += 'xunit.parallelizeTestCollections=false'
    }

    & dotnet @testArgs
    if ($LASTEXITCODE -ne 0) { $overallExit = $LASTEXITCODE }

    Write-Host "Results: $OutputDir/$trxFile" -ForegroundColor Cyan
}

exit $overallExit
