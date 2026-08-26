# ============================================================
# stop-pipeline.ps1
# Tears down the Smart Agriculture Irrigation System.
#   - Kills any host-side sensor simulator python process
#   - Stops the Docker Compose stack
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\stop-pipeline.ps1
#   powershell ... -RemoveVolumes    # also delete persistent volumes
# ============================================================

param(
    [switch]$Silent
)

$ErrorActionPreference = "Stop"
$script:Root = Split-Path -Parent $PSScriptRoot
$script:ComposeFile = Join-Path $script:Root "docker-compose.yml"

function Write-Line([string]$msg, [string]$color = "Cyan") {
    Write-Host $msg -ForegroundColor $color
}

Write-Line "================ Smart Agriculture - Pipeline Tear Down ================"

# 1) Stop host-side sensor simulator (python running simulator.py)
Write-Line "Stopping host sensor simulator process(es)..."
$simProcs = Get-CimInstance Win32_Process -Filter "Name = 'python.exe' OR Name = 'python3.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like "*sensor_simulator*simulator.py*" }
if ($simProcs) {
    foreach ($p in $simProcs) {
        Write-Line "  Killing PID $($p.ProcessId): $($p.CommandLine)" "Yellow"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Line "  No simulator process found running."
}

# 2) Docker compose down
if (Test-Path -LiteralPath $script:ComposeFile) {
    Write-Line "Stopping containers (persistent volumes are always preserved)..."
    docker compose -f $script:ComposeFile stop
    if ($LASTEXITCODE -ne 0) {
        Write-Line "  'docker compose stop' failed (exit=$LASTEXITCODE)." "Red"
    } else {
        Write-Line "Containers stopped successfully. All data volumes preserved." "Green"
    }
} else {
    Write-Line "docker-compose.yml not found at $($script:ComposeFile) - skipping compose down." "Yellow"
}

Write-Line ""
Write-Line "TearDown complete. Run scripts\run-pipeline.ps1 to start again." "Green"