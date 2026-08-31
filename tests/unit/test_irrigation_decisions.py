"""Tests for the irrigation decision text used by weather_irrigation_alerts (A4)."""

from src.common.irrigation_decisions import irrigation_decision


def test_rain_implies_postpone():
    decision = irrigation_decision("Dry", 3.5, 30.0, 18.0)
    assert "Postpone irrigation" in decision
    assert "3.5 mm" in decision


def test_dry_no_rain_irrigate_now():
    decision = irrigation_decision("Dry", 0.0, 25.0, 18.0)
    assert "Irrigate" in decision


def test_overwatered_reduce():
    decision = irrigation_decision("Overwatered", 0.0, 58.0, 18.0)
    assert "Reduce" in decision


def test_optimal_maintain():
    decision = irrigation_decision("Optimal", 0.0, 40.0, 18.0)
    assert decision == "Maintain irrigation schedule"


def test_rain_beats_status_even_when_dry():
    # Even a Dry zone postpones when rain is forecast.
    decision = irrigation_decision("Dry", 0.5, 30.0, 18.0)
    assert "Postpone" in decision
