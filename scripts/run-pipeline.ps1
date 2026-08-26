# ============================================================
# run-pipeline.ps1
# One-shot orchestrator for the Smart Agriculture Irrigation
# System. It will:
#   1. Validate prerequisites (Docker, .env, placeholders)
#   2. Start ALL Docker services (docker compose up --build)
#   3. Wait for every service to become healthy
#   4. Unpause + trigger the Airflow DAGs
#   5. Start the sensor simulator on the host (background)
#   6. Print a final status / URLs report
#
# Any problem along the way is reported explicitly and the
# script exits non-zero on hard failures.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\run-pipeline.ps1
#   powershell ... -SkipBuild       # don't rebuild images
#   powershell ... -NoSimulator     # don't start the sensor simulator
#   powershell ... -NoDagTrigger    # don't trigger Airflow DAGs
# ============================================================

param(
    [switch]$SkipBuild,
    [switch]$NoSimulator,
    [switch]$NoDagTrigger,
    [int]$HealthTimeoutSeconds = 420
)

# Native commands (docker etc.) write warnings to stderr; capturing them via 2>&1 as
# error records would trip EAP when loud, so we use "Continue" and rely on explicit
# $LASTEXITCODE checks for actual failures.
$ErrorActionPreference = "Continue"
$script:Root = Split-Path -Parent $PSScriptRoot
$script:EnvFile = Join-Path $script:Root ".env"
$script:EnvExample = Join-Path $script:Root ".env.example"
$script:ComposeFile = Join-Path $script:Root "docker-compose.yml"
$script:SimulatorDir = Join-Path $script:Root "src\ingestion\sensor_simulator"

# ----- Logging helpers ----------------------------------------------------
$script:ColorInfo = "Cyan"
$script:ColorOk = "Green"
$script:ColorWarn = "Yellow"
$script:ColorErr = "Red"

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor $script:ColorInfo
}

function Write-Ok([string]$msg) {
    Write-Host "    [OK] $msg" -ForegroundColor $script:ColorOk
}

function Write-Warn([string]$msg) {
    Write-Host "    [WARN] $msg" -ForegroundColor $script:ColorWarn
}

function Write-Err([string]$msg) {
    Write-Host "    [ERROR] $msg" -ForegroundColor $script:ColorErr
}

# ----- Env file loading ---------------------------------------------------
function Get-EnvValue([string]$name) {
    if (-not (Test-Path -LiteralPath $script:EnvFile)) { return $null }
    $line = Select-String -LiteralPath $script:EnvFile -Pattern "^\s*$([regex]::Escape($name))\s*=" | Select-Object -First 1
    if (-not $line) { return $null }
    $value = ($line.Line -split "=", 2)[1].Trim().Trim('"').Trim("'")
    return $value
}

# ----- Prerequisite checks ------------------------------------------------
function Test-Prerequisites {
    Write-Step "Checking prerequisites"

    # Docker CLI
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $docker) {
        Write-Err "Docker CLI not found. Install Docker Desktop and add it to PATH."
        return $false
    }
    Write-Ok "Docker CLI found"

    # Docker daemon reachable
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Docker daemon is not running / not reachable."
        Write-Err ($dockerInfo | Select-Object -First 3)
        return $false
    }
    Write-Ok "Docker daemon reachable"

    # docker compose plugin
    $composeVersion = docker compose version 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Err "`docker compose` plugin not available."
        return $false
    }
    Write-Ok "docker compose: $($composeVersion -join ' ')"

    # .env file
    if (-not (Test-Path -LiteralPath $script:EnvFile)) {
        Write-Err ".env not found at $($script:EnvFile)"
        Write-Err "Run:  Copy-Item .env.example .env   then fill in your values."
        return $false
    }
    Write-Ok ".env found"

    # compose file
    if (-not (Test-Path -LiteralPath $script:ComposeFile)) {
        Write-Err "docker-compose.yml not found at $($script:ComposeFile)"
        return $false
    }
    Write-Ok "docker-compose.yml found"

    # Placeholder detection
    $placeholders = @(
        "your_pg_user", "change_me",
        "your_minio_user", "your_minio_access_key",
        "your_ch_user", "your_airflow_user",
        "your_grafana_user", "your_jmx_user",
        "generate_a_fernet_key_with_python", "generate_a_random_secret_key",
        "your_openweather_api_key_here"
    )
    $foundPlaceholders = @()
    foreach ($ph in $placeholders) {
        $raw = Select-String -LiteralPath $script:EnvFile -Pattern $ph -SimpleMatch -Quiet
        if ($raw) { $foundPlaceholders += $ph }
    }
    if ($foundPlaceholders.Count -gt 0) {
        Write-Warn "Your .env still contains placeholder values: $($foundPlaceholders -join ', ')"
        Write-Warn "This is fine for a local test, but change them for any real deployment."
    } else {
        Write-Ok "No placeholder values detected in .env"
    }

    return $true
}

# ----- Container status helpers ------------------------------------------
function Get-ContainerStatus([string]$name) {
    $state = docker inspect -f "{{.State.Status}}" $name 2>$null
    if (-not $state) { return "absent" }
    if ($state -ne "running") { return $state }
    $health = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $name 2>$null
    if (-not $health -or $health -eq "none") { return "running" }
    return $health
}

function Test-ServiceReady {
    param([string]$Name, [string]$Expect)
    $status = Get-ContainerStatus $Name
    if ($status -eq $Expect) { return $true }
    if ($Expect -eq "running" -and $status -eq "healthy") { return $true }
    if ($Expect -eq "exited" -and $status -eq "exited") {
        $code = docker inspect -f "{{.State.ExitCode}}" $Name 2>$null
        if ($code -eq "0") { return $true }
    }
    return $false
}

# ----- Compose up ---------------------------------------------------------
function Start-Containers {
    Write-Step "Starting Docker containers"

    $composeArgs = @("compose", "-f", $script:ComposeFile, "up", "-d")
    if (-not $script:SkipBuild) { $composeArgs += "--build" }

    Write-Host "    Running: docker $($composeArgs -join ' ')"
    $output = & docker @composeArgs 2>&1
    $exitCode = $LASTEXITCODE
    $output | ForEach-Object { Write-Host "      $_" }
    if ($exitCode -ne 0) {
        Write-Err "docker compose up failed (exit=$exitCode). See output above."
        return $false
    }
    Write-Ok "docker compose up completed"
    return $true
}

# ----- Health wait --------------------------------------------------------
function Wait-ForHealth {
    $services = @(
        @{ Name = "agri-postgres";       Expect = "healthy" },
        @{ Name = "agri-kafka";          Expect = "healthy" },
        @{ Name = "agri-kafka-init";     Expect = "exited" },
        @{ Name = "agri-kafka-ui";       Expect = "running" },
        @{ Name = "agri-minio";          Expect = "healthy" },
        @{ Name = "agri-minio-init";     Expect = "exited" },
        @{ Name = "agri-clickhouse";     Expect = "healthy" },
        @{ Name = "agri-spark-master";   Expect = "healthy" },
        @{ Name = "agri-spark-worker";   Expect = "healthy" },
        @{ Name = "agri-spark-history";  Expect = "healthy" },
        @{ Name = "agri-airflow-init";   Expect = "exited" },
        @{ Name = "agri-airflow-webserver"; Expect = "healthy" },
        @{ Name = "agri-airflow-scheduler"; Expect = "healthy" }
        # NOTE: agri-redis, agri-airflow-worker and agri-airflow-triggerer
        # were removed with the CeleryExecutor → LocalExecutor migration.
    )

    Write-Step "Waiting for all services to become ready (timeout=${HealthTimeoutSeconds}s)"
    $deadline = (Get-Date).AddSeconds($HealthTimeoutSeconds)

    while ($true) {
        $ready = $true
        $report = @()
        foreach ($svc in $services) {
            $status = Get-ContainerStatus $svc.Name
            $ok = $false
            if ($status -eq $svc.Expect) {
                $ok = $true
            } elseif ($svc.Expect -eq "exited" -and $status -eq "exited") {
                $code = docker inspect -f "{{.State.ExitCode}}" $svc.Name 2>$null
                $ok = ($code -eq "0")
            }
            if ($ok) {
                $report += "  [OK]     $($svc.Name) -> $status"
            } else {
                $report += "  [WAIT]   $($svc.Name) -> $status (expect $($svc.Expect))"
                $ready = $false
            }
        }

        if ($ready) {
            Write-Ok "All services healthy:"
            $report | ForEach-Object { Write-Host $_ }
            return $true
        }

        if ((Get-Date) -gt $deadline) {
            Write-Err "Timed out waiting for all services. Current status:"
            $report | ForEach-Object { Write-Host $_ }
            Write-Host ""
            Write-Err "Run 'docker compose ps' and 'docker compose logs <service>' to diagnose."
            return $false
        }

        Start-Sleep -Seconds 5
    }
}

# ----- Airflow DAG trigger ------------------------------------------------
function Trigger-Dags {
    Write-Step "Unpausing and triggering Airflow DAGs"

    $dags = @("sensor_pipeline", "weather_api_pipeline", "ml_pipeline")
    $failed = @()

    foreach ($dag in $dags) {
        Write-Host "    -> $dag"
        $unpause = & docker exec agri-airflow-scheduler airflow dags unpause $dag 2>&1
        $unpauseExit = $LASTEXITCODE
        if ($unpauseExit -ne 0) {
            Write-Err "  unpause failed: $($unpause -join ' ')"
            $failed += $dag
            continue
        }
        $trigger = & docker exec agri-airflow-scheduler airflow dags trigger $dag 2>&1
        $triggerExit = $LASTEXITCODE
        if ($triggerExit -ne 0) {
            Write-Warn "  trigger failed (DAG may run on schedule anyway): $($trigger -join ' ')"
        } else {
            Write-Ok "  $dag triggered"
        }
    }

    if ($failed.Count -gt 0) {
        Write-Warn "The following DAGs could not be unpaused: $($failed -join ', ')"
        Write-Warn "Open http://localhost:8085 and unpause them manually."
        return $false
    }
    return $true
}

# ----- Sensor simulator ---------------------------------------------------
function Start-Simulator {
    Write-Step "Starting sensor simulator (host, background)"

    $pgPort = Get-EnvValue "POSTGRES_PORT"
    if (-not $pgPort) { $pgPort = "5433" }
    $sendInterval = Get-EnvValue "SEND_INTERVAL"
    if (-not $sendInterval) { $sendInterval = "5" }
    $anomalyRate = Get-EnvValue "ANOMALY_RATE"
    if (-not $anomalyRate) { $anomalyRate = "0.03" }
    $sensorTopic = Get-EnvValue "KAFKA_TOPIC_SENSORS"
    if (-not $sensorTopic) { $sensorTopic = "agri.sensors.raw" }
    $pgUser = Get-EnvValue "POSTGRES_USER"
    $pgPass = Get-EnvValue "POSTGRES_PASSWORD"
    $pgDb = Get-EnvValue "POSTGRES_DB"
    if (-not $pgDb) { $pgDb = "smart_agriculture" }

    $venvPython = Join-Path $script:Root "venv\Scripts\python.exe"
    $pythonExe = $null
    if (Test-Path $venvPython) {
        $pythonExe = $venvPython
    } else {
        $pyCmd = Get-Command py -ErrorAction SilentlyContinue
        if ($pyCmd) {
            $pythonExe = $pyCmd.Source
        } else {
            $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
            if ($pythonCmd) { $pythonExe = $pythonCmd.Source }
        }
    }

    if (-not $pythonExe) {
        Write-Warn "python not found on PATH or in venv - skipping simulator."
        Write-Warn "To run it later:  python $($script:SimulatorDir)\simulator.py"
        return $false
    }

    $env:POSTGRES_HOST = "localhost"
    $env:POSTGRES_PORT = $pgPort
    $env:POSTGRES_USER = $pgUser
    $env:POSTGRES_PASSWORD = $pgPass
    $env:POSTGRES_DB = $pgDb
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
    $env:KAFKA_TOPIC_SENSORS = $sensorTopic
    $env:SEND_INTERVAL = $sendInterval
    $env:ANOMALY_RATE = $anomalyRate

    $logFile = Join-Path $script:Root "logs\simulator.log"
    New-Item -ItemType Directory -Path (Split-Path -Parent $logFile) -Force | Out-Null

    Write-Host "    Log: $logFile"
    Write-Host "    Kafka: localhost:9094  Postgres: localhost:$pgPort  Topic: $sensorTopic"

    try {
        $process = Start-Process -FilePath $pythonExe `
            -ArgumentList "`"$(Join-Path $script:SimulatorDir 'simulator.py')`"" `
            -WorkingDirectory $script:SimulatorDir `
            -RedirectStandardOutput $logFile `
            -RedirectStandardError "$logFile.err" `
            -WindowStyle Hidden -PassThru
        Write-Ok "Simulator started (PID=$($process.Id))"
    } catch {
        Write-Err "Failed to start simulator: $($_.Exception.Message)"
        Write-Warn "If psycopg2 / kafka-python are missing, run:  pip install -r requirements.txt"
        return $false
    }

    Start-Sleep -Seconds 3
    if ($process.HasExited) {
        Write-Err "Simulator exited immediately with code $($process.ExitCode). Log tail:"
        if (Test-Path $logFile) { Get-Content $logFile -Tail 20 | ForEach-Object { Write-Host "      $_" } }
        if (Test-Path "$logFile.err") { Get-Content "$logFile.err" -Tail 20 | ForEach-Object { Write-Host "      $_" } }
        return $false
    }
    return $true
}

# ----- URL report ---------------------------------------------------------
function Show-Urls {
    Write-Step "URLs"
    Write-Host "    Airflow UI      : http://localhost:8085   (user/pass from .env)"
    Write-Host "    MinIO Console   : http://localhost:9001"
    Write-Host "    Kafka UI        : http://localhost:8080"
    Write-Host "    Spark Master    : http://localhost:8090"
    Write-Host "    Spark Worker    : http://localhost:8091"
    Write-Host "    Spark History   : http://localhost:18080"
    Write-Host "    ClickHouse HTTP : http://localhost:8123"
    Write-Host "    Prometheus      : http://localhost:9090"
    Write-Host "    Grafana         : http://localhost:3000"
}

# ----- Main ---------------------------------------------------------------
function Main {
    Write-Host "============================================================" -ForegroundColor $script:ColorInfo
    Write-Host " Smart Agriculture Irrigation System - Pipeline Runner" -ForegroundColor $script:ColorInfo
    Write-Host "============================================================" -ForegroundColor $script:ColorInfo

    if (-not (Test-Prerequisites)) {
        Write-Err "Prerequisites not satisfied - aborting."
        exit 1
    }

    $ok = Start-Containers
    if (-not $ok) { exit 1 }

    $ok = Wait-ForHealth
    if (-not $ok) { exit 1 }

    if (-not $script:NoDagTrigger) {
        $ok = Trigger-Dags
        if (-not $ok) {
            Write-Warn "DAG trigger had issues - continuing (see messages above)."
        }
    } else {
        Write-Step "Skipping DAG trigger (-NoDagTrigger)"
    }

    if (-not $script:NoSimulator) {
        $ok = Start-Simulator
        if (-not $ok) {
            Write-Warn "Simulator not running - streaming jobs will wait for data."
        }
    } else {
        Write-Step "Skipping sensor simulator (-NoSimulator)"
    }

    Show-Urls

    Write-Host ""
    Write-Step "Pipeline startup finished. Watch DAG runs at http://localhost:8085"
    Write-Host "    Tip: docker compose logs -f airflow-scheduler   (live DAG scheduling)"
    Write-Host "    Tip: docker compose ps                         (overall health)"
    Write-Host "    Tip: scripts\stop-pipeline.ps1                 (tear everything down)"
}

& Main
