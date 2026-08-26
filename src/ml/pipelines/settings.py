"""
ml/pipelines/settings.py
Single source of truth for all ML module configuration.
"""
import os
from pathlib import Path

from src.common.config.clickhouse import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
)

ML_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = ML_ROOT / "artifacts"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

MODEL_PATH = ARTIFACTS_DIR / "model.joblib"
FEATURE_NAMES_PATH = ARTIFACTS_DIR / "feature_names.json"
EVALUATION_REPORT_PATH = ARTIFACTS_DIR / "evaluation_report.json"
SHAP_VALUES_PATH = ARTIFACTS_DIR / "shap_values.json"

TABLE_ZONE_HISTORY = "zone_summary_history"
TABLE_FARM_WEATHER_DAILY = "farm_weather_daily"
TABLE_ML_PREDICTIONS = "ml_predictions"

LABEL_COLUMN = "needs_irrigation"
DRY_STATUS = "Dry"

NUMERIC_FEATURES = [
    "avg_soil_moisture", "avg_soil_temperature", "avg_air_temperature",
    "avg_air_humidity", "moisture_min", "moisture_max",
    "moisture_deficit", "moisture_surplus", "sensor_count",
    "avg_temperature", "avg_humidity", "total_rain", "avg_wind_speed",
    "moisture_rolling_mean_3", "moisture_trend", "month", "day_of_week", "season",
]

CATEGORICAL_FEATURES = ["crop_id", "zone_id", "farm_id"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

CLIP_BOUNDS = {
    "avg_soil_moisture": (0.0, 100.0),
    "avg_soil_temperature": (-10.0, 70.0),
    "avg_air_temperature": (-20.0, 60.0),
    "avg_air_humidity": (0.0, 100.0),
    "avg_temperature": (-20.0, 60.0),
    "avg_humidity": (0.0, 100.0),
    "total_rain": (0.0, 200.0),
    "avg_wind_speed": (0.0, 60.0),
}

RAIN_SKIP_THRESHOLD_MM = float(os.getenv("RAIN_SKIP_THRESHOLD_MM", "2.0"))

TEST_SIZE = 0.20
RANDOM_STATE = 42
CV_FOLDS = 5
PRIMARY_METRIC = "roc_auc"

MODEL_PARAMS = {
    "RandomForest": {
        "n_estimators": 200, "max_depth": 10, "min_samples_leaf": 5,
        "class_weight": "balanced", "random_state": RANDOM_STATE, "n_jobs": -1,
    },
    "XGBoost": {
        "n_estimators": 200, "max_depth": 6, "learning_rate": 0.05,
        "subsample": 0.8, "colsample_bytree": 0.8, "eval_metric": "logloss",
        "random_state": RANDOM_STATE, "n_jobs": -1,
    },
    "LightGBM": {
        "n_estimators": 200, "max_depth": 6, "learning_rate": 0.05,
        "num_leaves": 31, "class_weight": "balanced", "random_state": RANDOM_STATE,
        "n_jobs": -1, "verbose": -1,
    },
    "CatBoost": {
        "iterations": 200, "depth": 6, "learning_rate": 0.05,
        "auto_class_weights": "Balanced", "random_seed": RANDOM_STATE, "verbose": 0,
    },
}

MIN_TRAINING_ROWS = 50
IRRIGATION_THRESHOLD = 0.50
