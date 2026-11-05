# Start the app stack. Optional interfaces are explicit; volumes are preserved.
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$NoSimulator,
    [switch]$NoDagTrigger,
    [switch]$Monitoring,
    [switch]$Tools,
    [ValidateRange(10, 3600)][int]$HealthTimeoutSeconds = 420
)
$ErrorActionPreference = "Stop"
$script:Root = Split-Path -Parent $PSScriptRoot
$script:ComposeArgs = @("compose", "--project-directory", $script:Root,
    "--env-file", (Join-Path $script:Root ".env"), "-f", (Join-Path $script:Root "docker-compose.yml"))
if ($Monitoring) { $script:ComposeArgs += @("--profile", "monitoring") }
if ($Tools) { $script:ComposeArgs += @("--profile", "tools") }
if (-not $NoSimulator) { $script:ComposeArgs += @("--profile", "simulation") }

function Test-ServiceReady {
    param([string]$Name, [string]$Expect)
    $status = & docker inspect -f '{{.State.Status}}' $Name 2>$null
    if ($LASTEXITCODE -ne 0) { return $false }
    # Check one-shot exit status before generic matching. A failed initializer
    # must never be accepted solely because its state is "exited".
    if ($Expect -eq "exited") {
        if ($status -ne "exited") { return $false }
        $code = & docker inspect -f '{{.State.ExitCode}}' $Name 2>$null
        return ($LASTEXITCODE -eq 0 -and "$code" -eq "0")
    }
    if ($status -ne "running") { return $false }
    if ($Expect -eq "healthy") {
        $health = & docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' $Name 2>$null
        return ($LASTEXITCODE -eq 0 -and $health -eq "healthy")
    }
    return $true
}

function Wait-ForHealth {
    $activeNames = & docker @script:ComposeArgs config --services
    if ($LASTEXITCODE -ne 0) { throw "Cannot resolve selected Compose services" }
    $rawConfig = & docker @script:ComposeArgs config --format json
    if ($LASTEXITCODE -ne 0) { throw "Invalid Compose configuration" }
    $config = ($rawConfig -join "`n") | ConvertFrom-Json
    $deadline = (Get-Date).AddSeconds($HealthTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $pending = @()
        foreach ($name in $activeNames) {
            $service = $config.services.$name
            $expect = if ($name -in @("kafka-init", "minio-init", "airflow-init")) { "exited" }
                elseif ($service.healthcheck) { "healthy" } else { "running" }
            if (-not (Test-ServiceReady -Name $service.container_name -Expect $expect)) { $pending += $name }
        }
        if ($pending.Count -eq 0) { Write-Host "All selected services are ready."; return }
        Write-Host "Waiting for: $($pending -join ', ')"
        Start-Sleep -Seconds 5
    }
    throw "Startup health deadline exceeded. Inspect docker compose ps -a and service logs."
}

function Main {
    if (-not (Test-Path -LiteralPath (Join-Path $script:Root ".env"))) { throw "Create and configure .env from .env.example first." }
    & docker info --format '{{.ServerVersion}}'
    if ($LASTEXITCODE -ne 0) { throw "Docker Desktop Linux engine is unavailable." }
    & docker @script:ComposeArgs config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Compose configuration is invalid." }
    $upArgs = @("up", "-d")
    if (-not $SkipBuild) { $upArgs += "--build" }
    & docker @script:ComposeArgs @upArgs
    if ($LASTEXITCODE -ne 0) { throw "Compose startup failed." }
    Wait-ForHealth
    if (-not $NoDagTrigger) {
        foreach ($dag in @("sensor_pipeline", "weather_api_pipeline", "ml_pipeline")) {
            & docker @script:ComposeArgs exec -T airflow-scheduler airflow dags unpause $dag
            if ($LASTEXITCODE -ne 0) { throw "Could not unpause DAG $dag. Check DAG import errors." }
            & docker @script:ComposeArgs exec -T airflow-scheduler airflow dags trigger $dag
            if ($LASTEXITCODE -ne 0) { throw "Could not trigger DAG $dag." }
        }
        Write-Host "DAGs submitted; submission does not imply completed processing."
    }
    Write-Host "API: http://localhost:8000 | Airflow: http://localhost:8085"
    if ($Monitoring) { Write-Host "Grafana: http://localhost:3000 | Prometheus: http://localhost:9090" }
    if ($Tools) { Write-Host "Kafka UI: http://localhost:8080 | Spark History: http://localhost:18080" }
    Write-Host "Initialize the application administrator explicitly; see docs/backend-application.md."
}
# Dot-sourcing loads the readiness helper for isolated operational tests.
if ($MyInvocation.InvocationName -ne '.') { Main }
