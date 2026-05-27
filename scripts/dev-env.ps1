<#
.SYNOPSIS
    Manages the cross-platform development environment for local testing.

.DESCRIPTION
    Provides a unified interface for running tests on different platform + backend
    combinations. Manages Docker containers for Linux test execution and the Linux
    Cosmos DB emulator.

    Supported scenarios:
      Platform=windows, Target=inmemory        — runs tests directly on host
      Platform=windows, Target=emulator-windows — runs tests on host against Windows emulator
      Platform=windows, Target=emulator-linux  — starts Linux emulator, runs tests on host
      Platform=linux,   Target=inmemory        — runs tests inside Linux container
      Platform=linux,   Target=emulator-linux  — runs tests in Linux container against emulator

.PARAMETER Command
    The action to perform: start, stop, status, exec, test.

.PARAMETER Platform
    Test runner platform: windows or linux. Required for 'test' command.

.PARAMETER Target
    Cosmos backend target: inmemory, emulator-linux, or emulator-windows. Required for 'test'.

.PARAMETER Project
    Test project: unit, integration, or both. Default: both.

.PARAMETER Framework
    Target framework. Default: net8.0.

.PARAMETER Filter
    dotnet test filter expression.

.PARAMETER WithEmulator
    For 'start' command: also start the Linux Cosmos emulator service.

.PARAMETER EmulatorOnly
    For 'start' command: start only the emulator (not the dev container).

.PARAMETER Cmd
    For 'exec' command: the command to run inside the Linux dev container.

.EXAMPLE
    ./scripts/dev-env.ps1 start
    ./scripts/dev-env.ps1 start -WithEmulator
    ./scripts/dev-env.ps1 test -Platform linux -Target inmemory -Project integration
    ./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux -Filter "FullyQualifiedName~CrudTests"
    ./scripts/dev-env.ps1 test -Platform windows -Target emulator-linux -Project integration
    ./scripts/dev-env.ps1 exec -Cmd "dotnet build -c Release"
    ./scripts/dev-env.ps1 status
    ./scripts/dev-env.ps1 stop
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status', 'exec', 'test')]
    [string]$Command = 'status',

    [ValidateSet('windows', 'linux')]
    [string]$Platform,

    [ValidateSet('inmemory', 'emulator-linux', 'emulator-windows')]
    [string]$Target,

    [ValidateSet('unit', 'integration', 'both')]
    [string]$Project = 'both',

    [string]$Framework = 'net8.0',
    [string]$Filter,

    [switch]$WithEmulator,
    [switch]$EmulatorOnly,

    [string]$Cmd
)

$ErrorActionPreference = 'Stop'
$ComposeFile = Join-Path $PSScriptRoot '..' 'docker-compose.dev.yml'

# ─── Helper Functions ─────────────────────────────────────────────────────────

function Test-DockerAvailable {
    $version = docker version --format '{{.Server.Version}}' 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker is not running. Start Docker Desktop (or Rancher Desktop) and try again."
        exit 1
    }
    return $version
}

function Test-ContainerRunning([string]$Name) {
    $state = docker inspect --format '{{.State.Running}}' $Name 2>&1
    return ($LASTEXITCODE -eq 0 -and $state -eq 'true')
}

function Test-EmulatorHealthy {
    $health = docker inspect --format '{{.State.Health.Status}}' 'cosmosdb-emulator-linux' 2>&1
    return ($LASTEXITCODE -eq 0 -and $health -eq 'healthy')
}

function Start-Service([string]$ServiceName) {
    Write-Host "Starting service '$ServiceName'..." -ForegroundColor Cyan
    docker compose -f $ComposeFile up -d $ServiceName
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to start service '$ServiceName'"
        exit 1
    }
}

function Wait-ForEmulator {
    param([int]$TimeoutSeconds = 300)

    Write-Host "Waiting for Cosmos DB emulator to become healthy..." -ForegroundColor Cyan
    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        if (Test-EmulatorHealthy) {
            Write-Host "Emulator healthy after ${elapsed}s" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 5
        $elapsed += 5
        if ($elapsed % 30 -eq 0) {
            Write-Host "  Still waiting... (${elapsed}s)" -ForegroundColor DarkGray
        }
    }
    Write-Error "Emulator did not become healthy within ${TimeoutSeconds}s"
    exit 1
}

function Ensure-DevContainer {
    if (-not (Test-ContainerRunning 'cosmosdb-dev-linux')) {
        Start-Service 'dev'
        # Wait for PowerShell to be installed (container command installs it on first start)
        Write-Host "Waiting for container to be ready (installing PowerShell if needed)..." -ForegroundColor Cyan
        $elapsed = 0
        while ($elapsed -lt 120) {
            $result = docker exec cosmosdb-dev-linux bash -c "command -v pwsh" 2>&1
            if ($LASTEXITCODE -eq 0) { break }
            Start-Sleep -Seconds 5
            $elapsed += 5
        }
        if ($elapsed -ge 120) {
            Write-Error "Container did not become ready within 120s"
            exit 1
        }
        Write-Host "Container ready." -ForegroundColor Green

        # Restore NuGet packages
        Write-Host "Restoring NuGet packages in container..." -ForegroundColor Cyan
        docker exec cosmosdb-dev-linux dotnet restore --verbosity quiet
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "NuGet restore had issues — tests may still work if packages are cached."
        }
    }
}

function Ensure-Emulator {
    if (-not (Test-ContainerRunning 'cosmosdb-emulator-linux')) {
        Start-Service 'emulator'
    }
    if (-not (Test-EmulatorHealthy)) {
        Wait-ForEmulator
    }
}

function Invoke-InContainer {
    param(
        [string]$Command,
        [hashtable]$EnvVars = @{}
    )

    $envArgs = @()
    foreach ($kv in $EnvVars.GetEnumerator()) {
        $envArgs += '-e'
        $envArgs += "$($kv.Key)=$($kv.Value)"
    }

    docker exec @envArgs cosmosdb-dev-linux bash -c $Command
    return $LASTEXITCODE
}

# ─── Commands ─────────────────────────────────────────────────────────────────

function Invoke-Start {
    $dockerVersion = Test-DockerAvailable
    Write-Host "Docker $dockerVersion detected" -ForegroundColor Cyan

    if ($EmulatorOnly) {
        Ensure-Emulator
        Write-Host "`nEmulator running at https://localhost:8081" -ForegroundColor Green
    } elseif ($WithEmulator) {
        Ensure-Emulator
        Ensure-DevContainer
        Write-Host "`nDev container + emulator running." -ForegroundColor Green
        Write-Host "  Dev container: cosmosdb-dev-linux" -ForegroundColor DarkGray
        Write-Host "  Emulator:      https://localhost:8081 (host) / https://emulator:8081 (from dev container)" -ForegroundColor DarkGray
    } else {
        Ensure-DevContainer
        Write-Host "`nDev container running: cosmosdb-dev-linux" -ForegroundColor Green
    }
}

function Invoke-Stop {
    Write-Host "Stopping dev environment..." -ForegroundColor Cyan
    docker compose -f $ComposeFile down
    Write-Host "Done." -ForegroundColor Green
}

function Invoke-Status {
    $devRunning = Test-ContainerRunning 'cosmosdb-dev-linux'
    $emulatorRunning = Test-ContainerRunning 'cosmosdb-emulator-linux'
    $emulatorHealthy = if ($emulatorRunning) { Test-EmulatorHealthy } else { $false }

    Write-Host "Cross-Platform Dev Environment Status" -ForegroundColor Cyan
    Write-Host "─────────────────────────────────────" -ForegroundColor DarkGray
    $devStatus = if ($devRunning) { "Running" } else { "Stopped" }
    $devColor = if ($devRunning) { "Green" } else { "DarkGray" }
    Write-Host "  Dev container (Linux):   $devStatus" -ForegroundColor $devColor

    $emStatus = if ($emulatorHealthy) { "Healthy" } elseif ($emulatorRunning) { "Starting..." } else { "Stopped" }
    $emColor = if ($emulatorHealthy) { "Green" } elseif ($emulatorRunning) { "Yellow" } else { "DarkGray" }
    Write-Host "  Emulator (Linux):        $emStatus" -ForegroundColor $emColor

    Write-Host ""
    Write-Host "Available test scenarios:" -ForegroundColor Cyan
    Write-Host "  ./scripts/dev-env.ps1 test -Platform windows -Target inmemory" -ForegroundColor DarkGray
    Write-Host "  ./scripts/dev-env.ps1 test -Platform windows -Target emulator-windows" -ForegroundColor DarkGray
    if ($emulatorRunning -or $emulatorHealthy) {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform windows -Target emulator-linux" -ForegroundColor White
    } else {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform windows -Target emulator-linux  (needs: start -EmulatorOnly)" -ForegroundColor DarkGray
    }
    if ($devRunning) {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform linux -Target inmemory" -ForegroundColor White
    } else {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform linux -Target inmemory           (needs: start)" -ForegroundColor DarkGray
    }
    if ($devRunning -and $emulatorHealthy) {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux" -ForegroundColor White
    } else {
        Write-Host "  ./scripts/dev-env.ps1 test -Platform linux -Target emulator-linux    (needs: start -WithEmulator)" -ForegroundColor DarkGray
    }
}

function Invoke-Exec {
    if (-not $Cmd) {
        Write-Error "The -Cmd parameter is required for the 'exec' command. Example: ./scripts/dev-env.ps1 exec -Cmd 'dotnet build -c Release'"
        exit 1
    }

    Test-DockerAvailable | Out-Null
    Ensure-DevContainer

    Write-Host "Executing in Linux container:" -ForegroundColor Cyan
    Write-Host "  $Cmd" -ForegroundColor DarkGray
    Write-Host ""

    docker exec cosmosdb-dev-linux bash -c $Cmd
    exit $LASTEXITCODE
}

function Invoke-Test {
    if (-not $Platform) {
        Write-Error "The -Platform parameter is required for the 'test' command. Use: -Platform windows or -Platform linux"
        exit 1
    }
    if (-not $Target) {
        Write-Error "The -Target parameter is required for the 'test' command. Use: -Target inmemory, emulator-linux, or emulator-windows"
        exit 1
    }

    # Validate combination
    if ($Platform -eq 'linux' -and $Target -eq 'emulator-windows') {
        Write-Error "Invalid combination: Linux platform cannot use the Windows emulator."
        exit 1
    }

    $runTestsScript = './scripts/run-tests.ps1'

    # Build the run-tests.ps1 arguments
    $testArgs = "-Target $Target -Project $Project -Framework $Framework"
    if ($Filter) {
        $testArgs += " -Filter '$Filter'"
    }

    switch ("$Platform|$Target") {
        # ── Scenario 1: Windows + in-memory ──
        'windows|inmemory' {
            Write-Host "Scenario 1: Windows + in-memory" -ForegroundColor Cyan
            $scriptPath = Join-Path $PSScriptRoot 'run-tests.ps1'
            $invokeArgs = @{ Target = $Target; Project = $Project; Framework = $Framework }
            if ($Filter) { $invokeArgs.Filter = $Filter }
            & $scriptPath @invokeArgs
            exit $LASTEXITCODE
        }

        # ── Scenario 2: Windows + Windows emulator ──
        'windows|emulator-windows' {
            Write-Host "Scenario 2: Windows + Windows emulator" -ForegroundColor Cyan
            $scriptPath = Join-Path $PSScriptRoot 'run-tests.ps1'
            $invokeArgs = @{ Target = $Target; Project = $Project; Framework = $Framework }
            if ($Filter) { $invokeArgs.Filter = $Filter }
            & $scriptPath @invokeArgs
            exit $LASTEXITCODE
        }

        # ── Scenario 3: Windows + Linux emulator ──
        'windows|emulator-linux' {
            Write-Host "Scenario 3: Windows host + Linux emulator" -ForegroundColor Cyan
            Test-DockerAvailable | Out-Null
            Ensure-Emulator
            Write-Host ""

            $scriptPath = Join-Path $PSScriptRoot 'run-tests.ps1'
            $invokeArgs = @{
                Target = 'emulator-linux'
                Project = $Project
                Framework = $Framework
                EmulatorEndpoint = 'https://localhost:8081'
            }
            if ($Filter) { $invokeArgs.Filter = $Filter }
            & $scriptPath @invokeArgs
            exit $LASTEXITCODE
        }

        # ── Scenario 4: Linux + in-memory ──
        'linux|inmemory' {
            Write-Host "Scenario 4: Linux container + in-memory" -ForegroundColor Cyan
            Test-DockerAvailable | Out-Null
            Ensure-DevContainer
            Write-Host ""

            $envVars = @{
                COSMOS_TEST_TARGET = 'inmemory'
            }
            $cmd = "pwsh -NoProfile -Command `"& $runTestsScript -Target inmemory -Project $Project -Framework $Framework$(if ($Filter) { " -Filter '$Filter'" })`""
            $exitCode = Invoke-InContainer -Command $cmd -EnvVars $envVars
            exit $exitCode
        }

        # ── Scenario 5: Linux + Linux emulator ──
        'linux|emulator-linux' {
            Write-Host "Scenario 5: Linux container + Linux emulator" -ForegroundColor Cyan
            Test-DockerAvailable | Out-Null
            Ensure-DevContainer
            Ensure-Emulator
            Write-Host ""

            $envVars = @{
                COSMOS_TEST_TARGET = 'emulator-linux'
                COSMOS_EMULATOR_ENDPOINT = 'https://emulator:8081'
            }
            $cmd = "pwsh -NoProfile -Command `"& $runTestsScript -Target emulator-linux -Project $Project -Framework $Framework -EmulatorEndpoint 'https://emulator:8081'$(if ($Filter) { " -Filter '$Filter'" })`""
            $exitCode = Invoke-InContainer -Command $cmd -EnvVars $envVars
            exit $exitCode
        }
    }
}

# ─── Dispatch ─────────────────────────────────────────────────────────────────

switch ($Command) {
    'start'  { Invoke-Start }
    'stop'   { Invoke-Stop }
    'status' { Invoke-Status }
    'exec'   { Invoke-Exec }
    'test'   { Invoke-Test }
}
