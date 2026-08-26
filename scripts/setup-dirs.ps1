# Create required empty directories with .gitkeep placeholders
# Run this script once to set up the directory structure

$dirs = @(
    "src\common\config",
    "src\ingestion\sensor_simulator",
    "src\processing\bronze",
    "src\processing\silver",
    "src\processing\gold",
    "src\processing\weather\bronze",
    "src\processing\weather\silver",
    "src\processing\weather\gold",
    "src\ml\config",
    "src\ml\datasets",
    "src\ml\models",
    "src\ml\notebooks",
    "src\ml\pipelines",
    "orchestration\airflow\dags\utils",
    "orchestration\airflow\plugins",
    "orchestration\airflow\config",
    "infra\postgres",
    "infra\clickhouse\config",
    "infra\kafka",
    "infra\spark\conf",
    "infra\spark\jars",
    "infra\airflow",
    "infra\prometheus",
    "infra\grafana",
    "sql\postgres\init",
    "sql\clickhouse\init",
    "tests\unit",
    "tests\integration",
    "tests\smoke",
    "docs\diagrams",
    ".github\workflows"
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
