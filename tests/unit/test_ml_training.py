"""Behavioural RF tests; skipped only when the optional ML runtime is absent."""
import pandas as pd
import pytest


def _history():
    count = 100
    return pd.DataFrame({"farm_id": [1] * count, "zone_id": [1] * (count - 1) + [2],
                         "last_update": pd.date_range("2026-01-01", periods=count, freq="h", tz="UTC"),
                         "moisture_deficit": [10.0 if i % 2 else -10.0 for i in range(count)],
                         "avg_soil_moisture": [15.0 if i % 2 else 35.0 for i in range(count)],
                         "needs_irrigation": [i % 2 for i in range(count)]})


def test_fitted_rf_predicts_only_latest_zone_states():
    pytest.importorskip("sklearn")
    from src.ml.predict_spark import train_and_predict
    results = train_and_predict(_history())
    assert len(results) == 2
    assert {row["model_version"] for row in results} == {"spark-rf-current-dryness-v2"}
    assert {row["zone_id"]: row["needs_irrigation_today"] for row in results} == {1: 0, 2: 1}
    assert all(row["needs_irrigation_tomorrow"] is None for row in results)


def test_trainer_saves_chronologically_evaluated_binary_model(tmp_path, monkeypatch):
    pytest.importorskip("sklearn")
    import joblib
    from src.ml.pipelines import trainer
    monkeypatch.setattr(trainer, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(trainer, "FEATURE_NAMES_PATH", tmp_path / "features.json")
    monkeypatch.setattr(trainer, "EVALUATION_REPORT_PATH", tmp_path / "report.json")
    result = trainer.train(_history())
    assert result["status"] == "success"
    assert result["evaluation"] == "chronological_holdout"
    assert result["best_roc_auc"] > 0.9
    assert result["forecast_available"] is False
    assert list(joblib.load(tmp_path / "model.joblib").classes_) == [0, 1]


def test_one_class_training_skips_cleanly():
    pytest.importorskip("sklearn")
    from src.ml.pipelines.trainer import train
    frame = _history()
    frame["needs_irrigation"] = 0
    assert train(frame)["reason"] == "insufficient_chronological_class_coverage"


def test_feature_engineering_integrates_soil_physics_and_agronomy():
    from src.ml.feature_engineering import engineer_features
    raw = pd.DataFrame({
        "farm_id": [1, 1],
        "zone_id": [10, 10],
        "crop_name": ["Tomato", "Tomato"],
        "soil_type": ["Clay", "Clay"],
        "last_update": ["2026-07-01 10:00:00", "2026-07-01 14:00:00"],
        "avg_soil_moisture": [30.0, 24.0],
        "avg_soil_temperature": [28.0, 31.0],
        "avg_air_temperature": [32.0, 36.0],
        "avg_air_humidity": [45.0, 38.0],
        "moisture_min": [32.0, 32.0],
        "moisture_max": [40.0, 40.0],
        "water_status": ["Dry", "Dry"],
    })
    feat = engineer_features(raw)
    assert "field_capacity" in feat.columns
    assert "wilting_point" in feat.columns
    assert "vpd_kpa" in feat.columns
    assert "et0_est" in feat.columns
    assert "etc_est" in feat.columns
    assert "nir_mm" in feat.columns
    assert feat["field_capacity"].iloc[0] == 38.0
    assert feat["wilting_point"].iloc[0] == 22.0
    assert feat["vpd_kpa"].iloc[0] > 0.0
    assert feat["et0_est"].iloc[0] > 0.0
    assert feat["nir_mm"].iloc[0] > 0.0
    # Second row at 14:00 has 4 hours delta from 10:00, moisture dropped by 6%
    # Depletion rate should be ~ 1.5 %/hour
    assert feat["moisture_depletion_rate_hourly"].iloc[1] > 1.0


def test_prediction_includes_nir_and_actuator_duration():
    import json
    from src.ml.inference import build_predictions
    frame = pd.DataFrame({
        "farm_id": [1, 2],
        "zone_id": [10, 20],
        "avg_soil_moisture": [15.0, 45.0],
        "moisture_min": [25.0, 30.0],
        "moisture_max": [35.0, 40.0],
        "moisture_deficit": [10.0, -15.0],
        "vpd_kpa": [2.1, 0.8],
        "last_update": ["2026-07-01 12:00:00", "2026-07-01 12:00:00"],
    })
    preds = build_predictions(frame, [0.85, 0.10], "test-model-v2", ["avg_soil_moisture", "moisture_deficit"])
    assert len(preds) == 2
    first_snap = json.loads(preds[0]["feature_snapshot"])
    second_snap = json.loads(preds[1]["feature_snapshot"])
    assert first_snap["net_irrigation_requirement_mm"] > 0.0
    assert first_snap["recommended_duration_seconds"] > 0
    assert len(first_snap["primary_drivers"]) > 0
    assert second_snap["net_irrigation_requirement_mm"] == 0.0
    assert second_snap["recommended_duration_seconds"] == 0


def test_champion_challenger_rejects_inferior_model(tmp_path, monkeypatch):
    pytest.importorskip("sklearn")
    import json
    import joblib
    from sklearn.dummy import DummyClassifier
    from src.ml.pipelines import trainer

    model_path = tmp_path / "model.joblib"
    features_path = tmp_path / "features.json"
    report_path = tmp_path / "report.json"

    # Pre-existing champion with superior score
    champion_model = DummyClassifier(strategy="constant", constant=1)
    champion_model.fit([[0]], [1])
    joblib.dump(champion_model, model_path)
    with open(features_path, "w") as f:
        json.dump(["avg_soil_moisture"], f)
    with open(report_path, "w") as f:
        json.dump({"status": "success", "best_roc_auc": 0.98, "champion_roc_auc": 0.98, "champion_promoted": True}, f)

    monkeypatch.setattr(trainer, "MODEL_PATH", model_path)
    monkeypatch.setattr(trainer, "FEATURE_NAMES_PATH", features_path)
    monkeypatch.setattr(trainer, "EVALUATION_REPORT_PATH", report_path)

    # Train on dataset with noisy labels where candidate will achieve ~0.9167 (less than 0.98)
    noisy_history = _history()
    noisy_history["needs_irrigation"] = [1 if (i % 2 and i % 5 != 0) else 0 for i in range(len(noisy_history))]
    result = trainer.train(noisy_history)
    assert result["status"] == "degraded"
    assert result["champion_promoted"] is False
    assert result["champion_roc_auc"] == 0.98
    assert result["best_roc_auc"] < 0.98

    # The champion model artifact on disk must NOT have been overwritten
    loaded_champion = joblib.load(model_path)
    assert isinstance(loaded_champion, DummyClassifier)


def test_champion_challenger_promotes_superior_model(tmp_path, monkeypatch):
    pytest.importorskip("sklearn")
    import json
    import joblib
    from sklearn.dummy import DummyClassifier
    from sklearn.ensemble import RandomForestClassifier
    from src.ml.pipelines import trainer

    model_path = tmp_path / "model.joblib"
    features_path = tmp_path / "features.json"
    report_path = tmp_path / "report.json"

    # Existing champion with mediocre score
    old_champion = DummyClassifier(strategy="constant", constant=0)
    old_champion.fit([[0]], [0])
    joblib.dump(old_champion, model_path)
    with open(report_path, "w") as f:
        json.dump({"status": "success", "best_roc_auc": 0.55, "champion_roc_auc": 0.55, "champion_promoted": True}, f)

    monkeypatch.setattr(trainer, "MODEL_PATH", model_path)
    monkeypatch.setattr(trainer, "FEATURE_NAMES_PATH", features_path)
    monkeypatch.setattr(trainer, "EVALUATION_REPORT_PATH", report_path)

    result = trainer.train(_history())
    assert result["status"] == "success"
    assert result["champion_promoted"] is True
    assert result["champion_roc_auc"] > 0.90
    assert model_path.exists()
    loaded = joblib.load(model_path)
    assert not isinstance(loaded, DummyClassifier)


def test_atomic_dump_persists_safely_and_cleans_temporary_files(tmp_path):
    pytest.importorskip("sklearn")
    import json
    from src.ml.pipelines.trainer import _atomic_dump
    target = tmp_path / "sub" / "artifact.json"
    payload = {"key": "value", "score": 0.95}
    _atomic_dump(payload, target)
    assert target.exists()
    with open(target) as f:
        assert json.load(f) == payload

    # Ensure no leftover temp files
    temp_files = list((tmp_path / "sub").glob("artifact_*.tmp"))
    assert len(temp_files) == 0

    # Ensure exception on unserializable payload cleans up temp files
    with pytest.raises(Exception):
        _atomic_dump({"bad": set([1, 2])}, target)
    temp_files = list((tmp_path / "sub").glob("artifact_*.tmp"))
    assert len(temp_files) == 0


def test_local_shap_explainability():
    pytest.importorskip("sklearn")
    from sklearn.ensemble import RandomForestClassifier
    from src.ml.pipelines.explainer import explain_local_instances, compute_shap_values

    X = [[10.0, 20.0], [20.0, 10.0], [12.0, 22.0], [25.0, 5.0]]
    y = [0, 1, 0, 1]
    features = ["moisture", "temperature"]
    rf = RandomForestClassifier(n_estimators=10, random_state=42).fit(X, y)

    # Empty batch returns empty list
    assert explain_local_instances(rf, [], features) == []

    # Valid batch returns top drivers
    drivers = explain_local_instances(rf, X[:2], features)
    assert len(drivers) == 2
    for inst_drivers in drivers:
        assert len(inst_drivers) <= 2
        for d in inst_drivers:
            assert "feature" in d
            assert "impact" in d
            assert d["feature"] in features
            assert isinstance(d["impact"], float)


def test_clickhouse_ml_predictions_schema_deduplication():
    import os
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    schema_path = os.path.join(root, "sql", "clickhouse", "init", "01-init-schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    assert "CREATE TABLE IF NOT EXISTS agri_gold.ml_predictions" in sql
    assert "ENGINE = ReplacingMergeTree(prediction_time)" in sql
    assert "ORDER BY (zone_id, prediction_date)" in sql
    assert "needs_irrigation_tomorrow Nullable(UInt8)" in sql


def test_candidate_tournament_evaluates_all_models(tmp_path, monkeypatch):
    pytest.importorskip("sklearn")
    from src.ml.pipelines import trainer
    monkeypatch.setattr(trainer, "MODEL_PATH", tmp_path / "model.joblib")
    monkeypatch.setattr(trainer, "FEATURE_NAMES_PATH", tmp_path / "features.json")
    monkeypatch.setattr(trainer, "EVALUATION_REPORT_PATH", tmp_path / "report.json")
    result = trainer.train(_history())
    assert "tournament" in result
    assert "RandomForest" in result["tournament"]
    assert "GradientBoosting" in result["tournament"]
    assert result["tournament"]["RandomForest"]["roc_auc"] > 0.8
    assert result["tournament"]["GradientBoosting"]["roc_auc"] > 0.8


def test_feature_engineering_handles_casing_and_spaces():
    from src.ml.feature_engineering import engineer_features
    raw = pd.DataFrame({
        "farm_id": [1, 2],
        "zone_id": [10, 20],
        "crop_name": [" Date Palm ", "tomato"],
        "soil_type": ["clay", " Sandy "],
        "last_update": ["2026-07-01 10:00:00", "2026-07-01 10:00:00"],
        "avg_soil_moisture": [30.0, 12.0],
        "avg_soil_temperature": [28.0, 26.0],
        "avg_air_temperature": [32.0, 30.0],
        "avg_air_humidity": [45.0, 50.0],
        "moisture_min": [32.0, 15.0],
        "moisture_max": [40.0, 25.0],
        "water_status": ["Dry", "Dry"],
    })
    feat = engineer_features(raw)
    assert feat["field_capacity"].iloc[0] == 38.0  # Clay fc
    assert feat["field_capacity"].iloc[1] == 15.0  # Sandy fc
    assert feat["wilting_point"].iloc[0] == 22.0  # Clay pwp
    assert feat["wilting_point"].iloc[1] == 6.0   # Sandy pwp
    assert feat["etc_est"].iloc[0] > 0.0
    assert feat["etc_est"].iloc[1] > 0.0


def test_prediction_handles_none_in_top_driver_features():
    from src.ml.inference import build_predictions
    frame = pd.DataFrame({
        "farm_id": [1],
        "zone_id": [10],
        "avg_soil_moisture": [15.0],
        "moisture_deficit": [10.0],
        "vpd_kpa": [None],
        "total_rain": [None],
        "last_update": ["2026-07-01 12:00:00"],
    })
    preds = build_predictions(frame, [0.85], "test-model-v2", ["avg_soil_moisture", "moisture_deficit"])
    assert len(preds) == 1
    assert preds[0]["needs_irrigation_today"] == 1


