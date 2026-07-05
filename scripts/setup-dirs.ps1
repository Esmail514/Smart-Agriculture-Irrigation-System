# Create required empty directories with .gitkeep placeholders
# Run this script once to set up the directory structure

$dirs = @(
    "kafka\data",
    "spark\jars",
    "airflow\dags",
    "airflow\plugins",
    "airflow\config",
    "grafana\dashboards",
    "configs\kafka"
)

$parentDir = Split-Path -Parent $PSScriptRoot

foreach ($dir in $dirs) {
    $fullPath = Join-Path $parentDir $dir
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        New-Item -ItemType File -Path (Join-Path $fullPath ".gitkeep") -Force | Out-Null
        Write-Host "Created: $dir"
    } else {
        Write-Host "Already exists: $dir"
    }
}

Write-Host ""
Write-Host "Directory structure setup complete!"
