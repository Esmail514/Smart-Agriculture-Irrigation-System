import json
import pandas as pd
import pytest
from src.ml.inference import build_predictions, chronological_split, latest_zone_rows


def test_chronological_split_keeps_same_timestamp_together():
    frame = pd.DataFrame({"last_update": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-03"]})
    training, test = chronological_split(frame, 0.3)
    assert training.last_update.max() < test.last_update.min()
    assert len(test) == 2


def test_latest_predictions_have_one_row_per_farm_zone():
    frame = pd.DataFrame({"farm_id": [1, 1, 2], "zone_id": [1, 1, 1],
                          "last_update": ["2026-01-01", "2026-01-02", "2026-01-01"]})
    result = latest_zone_rows(frame)
    assert len(result) == 2
    assert result.loc[result.farm_id == 1, "last_update"].iloc[0] == pd.Timestamp("2026-01-02", tz="UTC")


def test_prediction_uses_position_not_dataframe_index_and_has_no_fake_forecast():
    frame = pd.DataFrame({"farm_id": [1, 2], "zone_id": [10, 20], "avg_soil_moisture": [10.0, 40.0]}, index=[99, 2])
    predictions = build_predictions(frame, [0.8, 0.1], "test-model", ["avg_soil_moisture"])
    assert [p["needs_irrigation_today"] for p in predictions] == [1, 0]
    assert all(p["needs_irrigation_tomorrow"] is None for p in predictions)
    assert json.loads(predictions[0]["feature_snapshot"])["forecast_available"] is False
    assert predictions[1]["prediction_confidence"] == 0.9
    assert json.loads(predictions[1]["feature_snapshot"])["irrigation_probability"] == 0.1


def test_nonfinite_model_probability_fails_explicitly():
    with pytest.raises(ValueError):
        build_predictions(pd.DataFrame({"farm_id": [1], "zone_id": [2]}), [float("nan")], "test", [])


def test_prediction_identity_is_replay_stable():
    frame = pd.DataFrame({"farm_id": [1], "zone_id": [2], "last_update": ["2026-10-07"]})
    first = build_predictions(frame, [0.8], "test", [])
    second = build_predictions(frame, [0.8], "test", [])
    assert first[0]["prediction_id"] == second[0]["prediction_id"]
    assert first[0]["prediction_date"].isoformat() == "2026-10-07"


def test_rule_fallback_is_stable_per_zone_and_only_predicts_latest_rows():
    from src.ml.predict_spark import train_and_predict
    frame = pd.DataFrame({"farm_id": [1, 1, 1], "zone_id": [1, 1, 2],
                          "last_update": ["2026-10-01", "2026-10-02", "2026-10-02"],
                          "moisture_deficit": [5.0, 0.1, 100.0], "needs_irrigation": [1, 1, 1]})
    results = train_and_predict(frame)
    assert len(results) == 2
    assert all(row["needs_irrigation_today"] == 1 for row in results)
    assert all(row["model_version"] == "crop-threshold-rule-v2" for row in results)


def test_unknown_rule_zone_does_not_get_a_healthy_prediction():
    from src.ml.predict_spark import train_and_predict
    frame = pd.DataFrame({"farm_id": [1], "zone_id": [2], "last_update": ["2026-10-07"],
                          "needs_irrigation": [pd.NA], "moisture_deficit": [0.0]})
    assert train_and_predict(frame) == []


def test_unknown_latest_state_does_not_resurrect_older_valid_state():
    from src.ml.predict_spark import train_and_predict
    frame = pd.DataFrame({"farm_id": [1, 1], "zone_id": [2, 2],
                          "last_update": ["2026-10-06", "2026-10-07"],
                          "needs_irrigation": [0, pd.NA], "moisture_deficit": [-10.0, 0.0]})
    assert train_and_predict(frame) == []


def test_late_summary_update_wins_same_event_time_regardless_of_input_order():
    frame = pd.DataFrame({"farm_id": [1, 1], "zone_id": [2, 2],
                          "last_update": ["2026-10-07", "2026-10-07"],
                          "snapshot_id": [7, 8], "avg_soil_moisture": [40.0, 10.0]})
    for candidate in [frame, frame.iloc[::-1]]:
        assert latest_zone_rows(candidate).iloc[0]["avg_soil_moisture"] == 10.0


@pytest.mark.parametrize("probabilities", [[1.1], [-0.1], [[0.8]]])
def test_invalid_probability_values_are_not_silently_clipped(probabilities):
    with pytest.raises(ValueError, match="probabilities"):
        build_predictions(pd.DataFrame({"farm_id": [1], "zone_id": [2]}), probabilities, "test", [])


def test_missing_observation_time_cannot_supersede_valid_history_or_enter_training():
    frame = pd.DataFrame({"farm_id": [1, 1], "zone_id": [2, 2],
                          "last_update": ["2026-10-07", None]})
    with pytest.raises(ValueError, match="last_update"):
        latest_zone_rows(frame)
    with pytest.raises(ValueError, match="last_update"):
        chronological_split(frame)


def test_rule_evidence_is_retained_even_when_not_a_model_feature():
    frame = pd.DataFrame({"farm_id": [1], "zone_id": [2], "crop_id": [7],
                          "moisture_min": [30.0], "moisture_max": [45.0]})
    evidence = json.loads(build_predictions(frame, [0.8], "test", [])[0]["feature_snapshot"])
    assert evidence["crop_id"] == 7
    assert evidence["moisture_min"] == 30.0
    assert evidence["moisture_max"] == 45.0


def test_prediction_keeps_64_bit_identities_exact_beside_float_features():
    identity = 2**60 + 1
    frame = pd.DataFrame({"farm_id": [identity], "zone_id": [identity + 1],
                          "crop_id": [identity + 2], "moisture_min": [30.5],
                          "avg_soil_moisture": [10.5]})
    prediction = build_predictions(frame, [0.8], "test", ["avg_soil_moisture"])[0]
    assert prediction["farm_id"] == identity
    assert prediction["zone_id"] == identity + 1
    assert json.loads(prediction["feature_snapshot"])["crop_id"] == identity + 2
