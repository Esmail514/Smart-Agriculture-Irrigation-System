from datetime import datetime, timedelta, timezone
import pytest
from src.processing.weather.contracts import flatten_record, weather_record_error


def _fixture():
    now = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
    farm = {"farm_id": 1, "farm_name": "Test farm", "latitude": 15.0, "longitude": 44.0}
    data = {"cod": 200, "dt": int(now.timestamp()), "main": {"temp": 28, "humidity": 70, "pressure": 1010},
            "wind": {"speed": 3}, "clouds": {"all": 25}, "weather": [{"main": "Clear", "description": "clear sky"}]}
    return now, farm, data


def test_weather_observation_has_metric_numeric_types_and_utc_datetimes():
    now, farm, data = _fixture()
    record = flatten_record(farm, data, now)
    assert record["temperature"] == 28.0
    assert isinstance(record["temperature"], float)
    assert record["observation_time_utc"] == now.replace(tzinfo=None)
    assert record["rain_1h_mm"] == 0.0
    assert record["schema_version"] == 2
    assert record["weather_reading_id"] == f"1-{data['dt']}"


@pytest.mark.parametrize("name,value", [("humidity", None), ("humidity", float("nan")), ("temp", 200), ("pressure", -1)])
def test_invalid_numeric_weather_never_enters_bronze(name, value):
    now, farm, data = _fixture()
    data["main"][name] = value
    with pytest.raises(ValueError):
        flatten_record(farm, data, now)


def test_api_error_payload_is_not_an_observation():
    now, farm, data = _fixture()
    with pytest.raises(ValueError):
        flatten_record(farm, {"cod": "401", "message": "invalid key"}, now)


def test_stale_weather_is_rejected():
    now, farm, data = _fixture()
    data["dt"] -= 7 * 3600
    with pytest.raises(ValueError, match="stale"):
        flatten_record(farm, data, now)


@pytest.mark.parametrize("name,value", [
    ("schema_version", 1), ("schema_version", None), ("schema_version", True),
    ("farm_id", 0), ("farm_id", True), ("weather_reading_id", ""),
    ("weather_reading_id", "2-1"), ("farm_name", "  "),
    ("temperature", float("nan")), ("humidity", float("inf")),
    ("rain_1h_mm", 10**400), ("feels_like", True),
    ("latitude", float("nan")), ("observation_time_utc", None),
])
def test_typed_silver_admission_rejects_broken_contract(name, value):
    now, farm, data = _fixture()
    record = flatten_record(farm, data, now)
    record[name] = value
    assert weather_record_error(record) is not None


@pytest.mark.parametrize("seconds", [601, -21601])
def test_silver_revalidation_rejects_future_and_stale_relative_to_fetch(seconds):
    now, farm, data = _fixture()
    record = flatten_record(farm, data, now)
    record["observation_time_utc"] += timedelta(seconds=seconds)
    assert weather_record_error(record) == "weather_observation_stale_or_future"


def test_valid_historical_weather_replay_does_not_depend_on_wall_clock():
    now, farm, data = _fixture()
    record = flatten_record(farm, data, now)
    assert weather_record_error(record) is None


@pytest.mark.parametrize("farm_id", [True, 1.5, 2**63])
def test_weather_farm_identifier_cannot_be_coerced_or_overflow(farm_id):
    now, farm, data = _fixture()
    farm["farm_id"] = farm_id
    with pytest.raises(ValueError):
        flatten_record(farm, data, now)


def test_boolean_feels_like_is_not_a_temperature():
    now, farm, data = _fixture()
    data["main"]["feels_like"] = True
    with pytest.raises(ValueError):
        flatten_record(farm, data, now)


def test_huge_epoch_raises_contract_value_error():
    now, farm, data = _fixture()
    data["dt"] = 1e100
    with pytest.raises(ValueError, match="epoch"):
        flatten_record(farm, data, now)


def test_slow_multi_farm_fetch_records_each_response_time(monkeypatch):
    import importlib
    import sys
    from types import ModuleType
    from pathlib import Path
    # Collection timing is a unit boundary: no JVM or Spark installation is
    # required to verify when each HTTP response is timestamped.
    session = ModuleType('common.session')
    f = ModuleType('pyspark.sql.functions')
    sql = ModuleType('pyspark.sql')
    sql.functions = f
    spark_module = ModuleType('pyspark')
    spark_module.sql = sql
    session.create_spark = lambda *args: None
    for name in ['year', 'month', 'dayofmonth']:
        setattr(f, name, lambda *args: None)
    monkeypatch.setitem(sys.modules, 'common.session', session)
    monkeypatch.setitem(sys.modules, 'pyspark', spark_module)
    monkeypatch.setitem(sys.modules, 'pyspark.sql', sql)
    monkeypatch.setitem(sys.modules, 'pyspark.sql.functions', f)
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "src" / "processing"))
    collector = importlib.import_module("weather.bronze.fetch_weather_bronze")
    monkeypatch.setattr(collector, 'bronze_schema', lambda: object())
    now, farm, data = _fixture()
    responses = [now + timedelta(minutes=1), now + timedelta(minutes=20)]
    instants = iter([now] + responses)
    class Clock:
        @staticmethod
        def now(tz):
            return next(instants)
    payloads = []
    for instant in responses:
        payloads.append(dict(data, dt=int(instant.timestamp())))
    replies = iter(payloads)
    monkeypatch.setattr(collector, "datetime", Clock)
    monkeypatch.setattr(collector, "load_farms", lambda: [farm, dict(farm, farm_id=2)])
    monkeypatch.setattr(collector, "fetch_weather", lambda *args: next(replies))
    written = []
    class Frame:
        write = property(lambda self: self)
        def withColumn(self, *args): return self
        def mode(self, *args): return self
        def partitionBy(self, *args): return self
        def parquet(self, path): written.append(path)
    class Spark:
        records = None
        stopped = False
        def createDataFrame(self, records, schema):
            self.records = records
            return Frame()
        def stop(self): self.stopped = True
    spark = Spark()
    monkeypatch.setattr(session, "create_spark", lambda *args: spark)
    for name in ["year", "month", "dayofmonth"]:
        monkeypatch.setattr(f, name, lambda *args: None)
    collector.main()
    assert len(spark.records) == 2
    assert [row["fetched_at"] for row in spark.records] == [instant.replace(tzinfo=None) for instant in responses]
    assert written == [collector.BRONZE_WEATHER_PATH]
    assert spark.stopped
