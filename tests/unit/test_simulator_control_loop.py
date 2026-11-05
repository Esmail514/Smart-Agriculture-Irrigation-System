"""Run the actual simulator loop with virtual time and explicit fake boundaries."""
from types import SimpleNamespace

import pytest

from src.ingestion.sensor_simulator import simulator


def run_loop(monkeypatch, *, settings=None, stop_at=5, interval=2,
             sensor_count=1, sensor_delay=0, generate_hook=None, send_error=False):
    clock = SimpleNamespace(now=0.0, sleeps=[])
    output = SimpleNamespace(sent=[], flushes=[], generators=[], database_closed=False,
                             producer_closed=False, settings_reads=[], metadata_reads=[], signals=[])

    def sleep(seconds):
        assert 0 < seconds <= 0.5
        clock.sleeps.append(seconds)
        clock.now += seconds
        if clock.now >= stop_at:
            simulator._handle_shutdown(15, None)

    sensors = [{'sensor_id': index + 1, 'crop_id': 1} for index in range(sensor_count)]

    class Database:
        def load_sensors(self):
            output.metadata_reads.append(clock.now)
            return sensors

        def load_crop_config(self):
            return {1: {'configured': True}}

        def load_runtime_settings(self):
            output.settings_reads.append(clock.now)
            return settings(clock.now) if callable(settings) else settings or {}

        def close(self):
            output.database_closed = True

    class Generator:
        def __init__(self, crops):
            self.crop_config = crops
            self.anomaly_rate = simulator.ANOMALY_RATE
            output.generators.append(self)

        def generate(self, sensor):
            if generate_hook is not None:
                generate_hook(clock, output)
            return {'sensor_id': sensor['sensor_id'], 'anomaly_rate': self.anomaly_rate}

    class Producer:
        def send(self, reading):
            if send_error:
                raise OSError('Kafka unavailable')
            output.sent.append((clock.now, dict(reading)))

        def flush(self):
            output.flushes.append(clock.now)

        def close(self):
            output.producer_closed = True

    monkeypatch.setattr(simulator, 'running', True)
    monkeypatch.setattr(simulator, 'SEND_INTERVAL', interval)
    monkeypatch.setattr(simulator, 'ANOMALY_RATE', 0.03)
    monkeypatch.setattr(simulator, 'Database', Database)
    monkeypatch.setattr(simulator, 'DataGenerator', Generator)
    monkeypatch.setattr(simulator, 'SensorProducer', Producer)
    monkeypatch.setattr(simulator.signal, 'signal', lambda *args: output.signals.append(args[0]))
    monkeypatch.setattr(simulator.time, 'monotonic', lambda: clock.now)
    monkeypatch.setattr(simulator.time, 'sleep', sleep)
    monkeypatch.setattr(simulator.random, 'uniform', lambda *args: sensor_delay)
    output.run = simulator.main
    return clock, output


def test_missing_settings_use_environment_defaults_and_cleanup(monkeypatch):
    clock, output = run_loop(monkeypatch, settings={}, stop_at=5, interval=2)
    output.run()
    assert [time for time, reading in output.sent] == [0, 2, 4]
    assert all(reading['anomaly_rate'] == 0.03 for _, reading in output.sent)
    assert output.database_closed and output.producer_closed
    assert len(output.signals) == 2


def test_long_interval_polling_does_not_emit_extra_batches(monkeypatch):
    clock, output = run_loop(monkeypatch, settings={'simulator.interval_seconds': 120,
        'simulator.anomaly_rate': 0.9}, stop_at=241)
    output.run()
    assert [time for time, reading in output.sent] == [120, 240]
    assert output.settings_reads[:5] == [0, 30, 60, 90, 120]
    assert all(reading['anomaly_rate'] == 0.9 for _, reading in output.sent)


def test_minimum_interval_uses_subsecond_sleep_without_oversleep(monkeypatch):
    clock, output = run_loop(monkeypatch, settings={'simulator.interval_seconds': 0.1}, stop_at=0.41)
    output.run()
    assert [time for time, reading in output.sent] == pytest.approx([0.1, 0.2, 0.3, 0.4])
    assert clock.sleeps and max(clock.sleeps) <= 0.10000001


def test_disabled_configuration_never_generates_or_flushes_batches(monkeypatch):
    clock, output = run_loop(monkeypatch, settings={'simulator.enabled': False}, stop_at=61)
    output.run()
    assert output.sent == [] and output.flushes == []
    assert output.settings_reads == [0, 30, 60]
    assert output.database_closed and output.producer_closed


def test_disable_observed_mid_batch_prevents_remaining_sensor_sends(monkeypatch):
    settings = lambda time: {'simulator.enabled': time < 30}
    clock, output = run_loop(monkeypatch, settings=settings, stop_at=61,
                             sensor_count=6, sensor_delay=10)
    output.run()
    assert [time for time, reading in output.sent] == [0, 10, 20]
    assert output.settings_reads == [0, 30, 60]
    assert not any(time >= 30 for time, _ in output.sent)


def test_reenabled_and_updated_settings_are_applied(monkeypatch):
    def settings(time):
        return {'simulator.enabled': time >= 30, 'simulator.interval_seconds': 5,
                'simulator.anomaly_rate': 1}
    clock, output = run_loop(monkeypatch, settings=settings, stop_at=41)
    output.run()
    assert [time for time, reading in output.sent] == [30, 35, 40]
    assert all(reading['anomaly_rate'] == 1 for _, reading in output.sent)


def test_failed_settings_refresh_retains_last_valid_configuration(monkeypatch):
    def settings(time):
        if time >= 30:
            raise OSError('PostgreSQL temporarily unavailable')
        return {'simulator.interval_seconds': 10, 'simulator.anomaly_rate': 0.8}
    clock, output = run_loop(monkeypatch, settings=settings, stop_at=61)
    output.run()
    assert [time for time, reading in output.sent] == [10, 20, 30, 40, 50, 60]
    assert all(reading['anomaly_rate'] == 0.8 for _, reading in output.sent)


def test_send_failure_closes_both_dependencies(monkeypatch):
    clock, output = run_loop(monkeypatch, send_error=True)
    with pytest.raises(OSError, match='Kafka unavailable'):
        output.run()
    assert output.database_closed and output.producer_closed


def test_shutdown_during_sensor_pause_does_not_send_remaining_sensors(monkeypatch):
    clock, output = run_loop(monkeypatch, sensor_count=5, sensor_delay=5, stop_at=0.5)
    output.run()
    assert len(output.sent) == 1
    assert output.database_closed and output.producer_closed


def test_empty_catalog_aborts_before_producer_creation_but_closes_database(monkeypatch):
    clock, output = run_loop(monkeypatch, sensor_count=0)
    with pytest.raises(RuntimeError, match='No sensors registered'):
        output.run()
    assert output.database_closed and not output.producer_closed
    assert output.sent == []


def test_shutdown_during_generation_prevents_post_shutdown_send(monkeypatch):
    def interrupt_generation(clock, output):
        simulator._handle_shutdown(15, None)
    clock, output = run_loop(monkeypatch, generate_hook=interrupt_generation)
    output.run()
    assert output.sent == []
    assert output.database_closed and output.producer_closed
