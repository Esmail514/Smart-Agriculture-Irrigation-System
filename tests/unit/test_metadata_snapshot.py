from unittest.mock import Mock

import pytest

from common.metadata_snapshot import materialize_metadata_snapshot


def source(rows):
    frame = Mock()
    frame.schema = object()
    frame.limit.side_effect = lambda count: Mock(collect=lambda: list(rows[:count]))
    return frame


def test_snapshot_reuses_typed_driver_values_after_database_changes():
    rows = [{"sensor_id": 1, "farm_name": "Before rename"}]
    frame = source(rows)
    spark = Mock()
    result = materialize_metadata_snapshot(spark, frame, name="registry", max_rows=2)
    rows[:] = [{"sensor_id": 2, "farm_name": "Changed database"}]
    copied_rows = spark.createDataFrame.call_args.args[0]
    assert copied_rows == [{"sensor_id": 1, "farm_name": "Before rename"}]
    assert spark.createDataFrame.call_args.kwargs["schema"] is frame.schema
    assert result is spark.createDataFrame.return_value
    frame.limit.assert_called_once_with(3)


def test_snapshot_fails_before_creating_local_relation_when_limit_exceeded():
    frame = source(list(range(10)))
    spark = Mock()
    with pytest.raises(RuntimeError, match="registry.*limit.*2"):
        materialize_metadata_snapshot(spark, frame, name="registry", max_rows=2)
    frame.limit.assert_called_once_with(3)
    spark.createDataFrame.assert_not_called()


def test_empty_snapshot_keeps_explicit_schema():
    frame = source([])
    spark = Mock()
    materialize_metadata_snapshot(spark, frame, name="rules", max_rows=2)
    spark.createDataFrame.assert_called_once_with([], schema=frame.schema)


@pytest.mark.parametrize("limit", ["invalid", "0", "-1"])
def test_invalid_operator_limit_fails_closed(monkeypatch, limit):
    monkeypatch.setenv("METADATA_SNAPSHOT_MAX_ROWS", limit)
    frame = source([])
    with pytest.raises(ValueError, match="positive integer"):
        materialize_metadata_snapshot(Mock(), frame, name="registry")
    frame.limit.assert_not_called()


def test_configured_limit_is_applied(monkeypatch):
    monkeypatch.setenv("METADATA_SNAPSHOT_MAX_ROWS", "7")
    frame = source([])
    materialize_metadata_snapshot(Mock(), frame, name="registry")
    frame.limit.assert_called_once_with(8)
