"""Failure/recovery tests model S3's commit-on-close object publication."""
import json
import ast
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from common import snapshots

ROOT = 's3a://lake/gold/sensor_latest'
OLD = 'a' * 32
NEW = 'b' * 32


class Iterator:
    def __init__(self, values):
        self.values = iter(values)
        self.next_value = next(self.values, None)

    def hasNext(self):
        return self.next_value is not None

    def next(self):
        value = self.next_value
        self.next_value = next(self.values, None)
        return SimpleNamespace(getPath=lambda: SimpleNamespace(toString=lambda: value))


class ObjectStream:
    def __init__(self, fs, path):
        self.fs, self.path, self.buffer = fs, path, ''

    def writeBytes(self, content):
        if self.fs.fail_write:
            raise OSError('Manifest buffer write failed')
        self.buffer = content

    def close(self):
        if self.fs.fail_commit:
            raise OSError('S3 PUT failed')
        self.fs.objects[self.path] = self.buffer


class Filesystem:
    def __init__(self):
        self.objects = {}
        self.fail_commit = False
        self.fail_write = False
        self.fail_access = False

    def exists(self, path):
        if self.fail_access:
            raise PermissionError('Access denied')
        return path in self.objects or any(k.startswith(path.rstrip('/') + '/') for k in self.objects)

    def listFiles(self, path, recursive):
        return Iterator([k for k in self.objects if k.startswith(path.rstrip('/') + '/')])

    def create(self, path, overwrite):
        assert overwrite is True
        return ObjectStream(self, path)


@pytest.fixture
def storage(monkeypatch):
    fs = Filesystem()
    spark = MagicMock()
    reader = spark.read
    reader.schema.return_value = reader
    reader.option.return_value = reader
    reader.parquet.side_effect = lambda *paths: paths
    monkeypatch.setattr(snapshots, '_filesystem', lambda spark, path: (fs, str))
    monkeypatch.setattr(snapshots, '_read_manifest', lambda spark, fs, path: json.loads(fs.objects[path]))
    monkeypatch.setattr(snapshots, 'uuid4', lambda: SimpleNamespace(hex=NEW))
    return fs, spark


def set_manifest(fs, version=OLD):
    fs.objects[f'{ROOT}/_current.json'] = json.dumps({'format_version': 1, 'version': version})
    fs.objects[f'{ROOT}/versions/{version}/part.parquet'] = 'data'


def test_unpublished_orphan_versions_are_not_a_first_snapshot(storage):
    fs, spark = storage
    fs.objects[f'{ROOT}/versions/{NEW}/part.parquet'] = 'orphan'
    assert snapshots.read_latest_snapshot(spark, ROOT, schema='explicit') is None
    spark.read.parquet.assert_not_called()


def test_reader_pins_only_manifest_selected_version_and_schema(storage):
    fs, spark = storage
    set_manifest(fs)
    fs.objects[f'{ROOT}/versions/{NEW}/part.parquet'] = 'orphan'
    assert snapshots.read_latest_snapshot(spark, ROOT, schema='explicit') == (f'{ROOT}/versions/{OLD}',)
    spark.read.schema.assert_called_once_with('explicit')


def test_legacy_recovery_excludes_orphan_and_temporary_files(storage):
    fs, spark = storage
    fs.objects[f'{ROOT}/part-legacy.parquet'] = 'old'
    fs.objects[f'{ROOT}/versions/{NEW}/part.parquet'] = 'orphan'
    fs.objects[f'{ROOT}/_temporary/part.parquet'] = 'partial'
    assert snapshots.read_latest_snapshot(spark, ROOT) == (f'{ROOT}/part-legacy.parquet',)


@pytest.mark.parametrize('manifest', ['not-json', '{}', '{"format_version":1,"version":"../../history"}'])
def test_corrupt_manifest_is_not_silently_replaced_by_legacy_data(storage, manifest):
    fs, spark = storage
    fs.objects[f'{ROOT}/_current.json'] = manifest
    fs.objects[f'{ROOT}/part-legacy.parquet'] = 'old'
    with pytest.raises(ValueError):
        snapshots.read_latest_snapshot(spark, ROOT)
    spark.read.parquet.assert_not_called()


def test_missing_manifest_target_is_a_failure(storage):
    fs, spark = storage
    fs.objects[f'{ROOT}/_current.json'] = json.dumps({'format_version': 1, 'version': OLD})
    with pytest.raises(FileNotFoundError):
        snapshots.read_latest_snapshot(spark, ROOT)


def test_access_failure_propagates(storage):
    fs, spark = storage
    fs.fail_access = True
    with pytest.raises(PermissionError):
        snapshots.read_latest_snapshot(spark, ROOT)


def test_parquet_failure_never_moves_manifest(storage):
    fs, spark = storage
    set_manifest(fs)
    old_manifest = fs.objects[f'{ROOT}/_current.json']
    df = MagicMock()
    df.write.mode.return_value = df.write
    df.write.parquet.side_effect = OSError('Parquet commit failed')
    with pytest.raises(OSError):
        snapshots.publish_snapshot(spark, df, ROOT)
    assert fs.objects[f'{ROOT}/_current.json'] == old_manifest
    assert snapshots.read_latest_snapshot(spark, ROOT) == (f'{ROOT}/versions/{OLD}',)


def test_manifest_commit_failure_leaves_previous_version_readable(storage):
    fs, spark = storage
    set_manifest(fs)
    fs.fail_commit = True
    df = MagicMock()
    df.write.mode.return_value = df.write
    df.write.parquet.side_effect = lambda path: fs.objects.update({f'{path}/part.parquet': 'new'})
    with pytest.raises(OSError):
        snapshots.publish_snapshot(spark, df, ROOT)
    assert snapshots.read_latest_snapshot(spark, ROOT) == (f'{ROOT}/versions/{OLD}',)
    assert f'{ROOT}/versions/{NEW}/part.parquet' in fs.objects


def test_manifest_buffer_failure_does_not_close_and_publish_partial_json(storage):
    fs, spark = storage
    set_manifest(fs)
    fs.fail_write = True
    df = MagicMock()
    df.write.mode.return_value = df.write
    df.write.parquet.side_effect = lambda path: fs.objects.update({f'{path}/part.parquet': 'new'})
    with pytest.raises(OSError, match='Manifest buffer write failed'):
        snapshots.publish_snapshot(spark, df, ROOT)
    assert snapshots.read_latest_snapshot(spark, ROOT) == (f'{ROOT}/versions/{OLD}',)


def test_successful_publication_preserves_previous_readers(storage):
    fs, spark = storage
    set_manifest(fs)
    old_read = snapshots.read_latest_snapshot(spark, ROOT)
    df = MagicMock()
    df.write.mode.return_value = df.write
    def write(path):
        assert snapshots.read_latest_snapshot(spark, ROOT) == old_read
        fs.objects[f'{path}/part.parquet'] = 'new'
    df.write.parquet.side_effect = write
    assert snapshots.publish_snapshot(spark, df, ROOT) == f'{ROOT}/versions/{NEW}'
    assert snapshots.read_latest_snapshot(spark, ROOT) == (f'{ROOT}/versions/{NEW}',)
    assert fs.objects[f'{old_read[0]}/part.parquet'] == 'data'


def test_corrupt_parquet_errors_propagate_without_fallback(storage):
    fs, spark = storage
    set_manifest(fs)
    fs.objects[f'{ROOT}/part-legacy.parquet'] = 'legacy'
    spark.read.parquet.side_effect = OSError('Corrupt snapshot')
    with pytest.raises(OSError, match='Corrupt snapshot'):
        snapshots.read_latest_snapshot(spark, ROOT)


def load_gold_batch_function(events, fail_farm_publication=False):
    """Execute the real batch function with Spark boundary objects stubbed.

    Isolating its AST avoids importing optional PySpark on the unit-test host.
    The function body itself is unchanged, so ordering regressions are caught.
    """
    source = Path(__file__).resolve().parents[2] / 'src/processing/gold/gold_streaming.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'write_gold_batch')
    def frame(name):
        result = MagicMock()
        result.rdd.isEmpty.return_value = False
        result.cache.return_value = result
        result.filter.return_value = result
        result.withColumn.return_value = result
        result.agg.return_value.first.return_value = (datetime(2026, 10, 7, 12),)
        result.write.mode.return_value = result.write
        result.write.parquet.side_effect = lambda path: events.append(('history', name, path))
        return result
    current, zone, farm, alerts = [frame(name) for name in ('sensor', 'zone', 'farm', 'alerts')]
    expression = MagicMock()
    expression.__ge__.return_value = 'recent-sensor-condition'
    expression.cast.return_value = expression
    def publish(spark, summary, path):
        events.append(('snapshot', path))
        if fail_farm_publication and path == 'farm-latest':
            raise OSError('Manifest commit failed')
    namespace = {'read_latest_snapshot': lambda *args, **kw: current,
        'upsert_latest': lambda *args: events.append(('snapshot', 'sensor-latest')),
        'publish_snapshot': publish,
        'create_zone_summary': lambda df: zone,
        'create_farm_summary': lambda df: farm,
        'create_irrigation_alerts': lambda df: alerts,
        'SENSOR_STATE_PATH': 'sensor-latest', 'GOLD_ZONE_LATEST_PATH': 'zone-latest',
        'GOLD_FARM_LATEST_PATH': 'farm-latest', 'GOLD_ZONE_HISTORY_PATH': 'zone-history',
        'GOLD_FARM_HISTORY_PATH': 'farm-history', 'GOLD_ALERTS_PATH': 'alerts-history',
        'silver_schema': object(), 'logger': MagicMock(),
        'spark_max': lambda name: expression, 'col': lambda name: expression,
        'lit': lambda value: expression, 'timedelta': timedelta}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), namespace)
    return namespace['write_gold_batch'], frame('incoming'), current


def test_gold_publishes_all_snapshots_before_history_is_discoverable():
    events = []
    write_batch, incoming, current = load_gold_batch_function(events)
    write_batch(MagicMock(), incoming, 7)
    assert events[:3] == [('snapshot', 'sensor-latest'), ('snapshot', 'zone-latest'), ('snapshot', 'farm-latest')]
    assert [item[2] for item in events[3:]] == [
        'zone-history/batch_00000000000000000007',
        'farm-history/batch_00000000000000000007',
        'alerts-history/batch_00000000000000000007']
    current.unpersist.assert_called_once()
    current.filter.assert_called_once_with('recent-sensor-condition')


def test_failed_snapshot_never_publishes_history_readiness():
    events = []
    write_batch, incoming, current = load_gold_batch_function(events, fail_farm_publication=True)
    with pytest.raises(OSError, match='Manifest commit failed'):
        write_batch(MagicMock(), incoming, 7)
    assert all(item[0] == 'snapshot' for item in events)
    current.unpersist.assert_called_once()
