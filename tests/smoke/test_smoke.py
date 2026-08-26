"""
Smoke tests — verify imports, Spark session init, and config loading.
Run these after restructuring to confirm nothing is broken.
"""
import importlib
import os
import sys


def test_common_modules_import():
    """All shared common modules must import without error."""
    modules = [
        "src.common.logger",
        "src.common.config.kafka",
        "src.common.config.spark",
        "src.common.config.minio",
        "src.common.config.postgres",
        "src.common.config.clickhouse",
        "src.common.config.weather",
    ]
    for mod in modules:
        importlib.import_module(mod)
        print(f"  OK: {mod}")


def test_config_values_load():
    """Config values must load from environment (or defaults) without throwing."""
    from src.common.config.kafka import KAFKA_BOOTSTRAP_SERVERS, SENSOR_TOPIC
    from src.common.config.minio import MINIO_ENDPOINT, LAKEHOUSE_BUCKET
    from src.common.config.postgres import POSTGRES_HOST, POSTGRES_DB
    from src.common.config.clickhouse import CLICKHOUSE_HOST, CLICKHOUSE_DATABASE

    assert KAFKA_BOOTSTRAP_SERVERS, "KAFKA_BOOTSTRAP_SERVERS must be set"
    assert MINIO_ENDPOINT, "MINIO_ENDPOINT must be set"
    assert LAKEHOUSE_BUCKET, "LAKEHOUSE_BUCKET must be set"
    assert POSTGRES_HOST, "POSTGRES_HOST must be set"
    assert CLICKHOUSE_HOST, "CLICKHOUSE_HOST must be set"
    print("  OK: all config values loaded")


def test_logger_factory():
    """get_logger must return a usable logger."""
    from src.common.logger import get_logger, setup_logging
    setup_logging()
    logger = get_logger("smoke_test")
    logger.info("Smoke test logger OK")
    print("  OK: logger factory")


def test_processing_modules_import():
    """Processing modules must exist (file-level, no PySpark runtime needed)."""
    import ast, os

    jobs = [
        os.path.join("src", "processing", "bronze", "bronze_streaming.py"),
        os.path.join("src", "processing", "silver", "silver_streaming.py"),
        os.path.join("src", "processing", "gold", "gold_streaming.py"),
        os.path.join("src", "processing", "weather", "bronze", "fetch_weather_bronze.py"),
        os.path.join("src", "processing", "weather", "silver", "transform_weather_silver.py"),
        os.path.join("src", "processing", "weather", "gold", "aggregate_weather_gold.py"),
    ]
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    for job in jobs:
        path = os.path.join(root, job)
        assert os.path.exists(path), f"Missing: {path}"
        with open(path, encoding="utf-8") as f:
            ast.parse(f.read())  # syntax check only
        print(f"  OK (syntax): {job}")


def test_ingestion_module_import():
    """Sensor simulator package must be importable."""
    import importlib.util
    for mod in [
        "src.ingestion.sensor_simulator.config",
        "src.ingestion.sensor_simulator.generator",
    ]:
        spec = importlib.util.find_spec(mod)
        assert spec is not None, f"Cannot find module: {mod}"
        print(f"  OK: {mod}")


if __name__ == "__main__":
    print("\n=== Smoke Tests ===")
    tests = [
        test_common_modules_import,
        test_config_values_load,
        test_logger_factory,
        test_processing_modules_import,
        test_ingestion_module_import,
    ]
    failures = []
    for t in tests:
        try:
            print(f"\n[RUN] {t.__name__}")
            t()
            print(f"[PASS] {t.__name__}")
        except Exception as e:
            print(f"[FAIL] {t.__name__}: {e}")
            failures.append(t.__name__)

    print(f"\n=== Results: {len(tests) - len(failures)} passed, {len(failures)} failed ===")
    if failures:
        print("FAILED:", failures)
        sys.exit(1)
    print("All smoke tests passed!")
