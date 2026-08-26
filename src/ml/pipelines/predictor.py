import json
import uuid
from datetime import datetime, timezone

import joblib
import pandas as pd
from ml.pipelines.settings import (
    MODEL_PATH, FEATURE_NAMES_PATH, TABLE_ML_PREDICTIONS,
    IRRIGATION_THRESHOLD,
)
from ml.pipelines.clickhouse_client import get_client
from ml.pipelines.logger import get_logger

logger = get_logger("predictor")


def predict(df: pd.DataFrame) -> list[dict]:
    try:
        model = joblib.load(MODEL_PATH)
        with open(FEATURE_NAMES_PATH) as f:
            feature_names = json.load(f)
    except FileNotFoundError:
        logger.error("Model not found at %s", MODEL_PATH)
        return []

    available = [f for f in feature_names if f in df.columns]
    X = df[available].fillna(0)

    probabilities = model.predict_proba(X)[:, 1]
    predictions = []

    for i, row in df.iterrows():
        prob = float(probabilities[i])
        pred = {
            "prediction_id": str(uuid.uuid4()),
            "zone_id": int(row.get("zone_id", 0)),
            "zone_name": str(row.get("zone_name", "")),
            "farm_id": int(row.get("farm_id", 0)),
            "farm_name": str(row.get("farm_name", "")),
            "crop_name": str(row.get("crop_name", "")),
            "needs_irrigation_today": int(prob >= IRRIGATION_THRESHOLD),
            "needs_irrigation_tomorrow": int(prob >= IRRIGATION_THRESHOLD),
            "prediction_confidence": round(prob, 4),
            "model_version": "v1.0",
            "prediction_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "prediction_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "feature_snapshot": json.dumps({f: float(row.get(f, 0)) for f in available[:5]}),
        }
        predictions.append(pred)

    logger.info("Generated %d predictions", len(predictions))
    return predictions


def store_predictions(predictions: list[dict]):
    if not predictions:
        return
    client = get_client()
    column_names = [
        "prediction_id", "zone_id", "zone_name",
        "farm_id", "farm_name", "crop_name",
        "needs_irrigation_today", "needs_irrigation_tomorrow",
        "prediction_confidence", "model_version",
        "prediction_date", "prediction_time", "feature_snapshot",
    ]
    rows = [
        (
            pred["prediction_id"], pred["zone_id"], pred["zone_name"],
            pred["farm_id"], pred["farm_name"], pred["crop_name"],
            pred["needs_irrigation_today"], pred["needs_irrigation_tomorrow"],
            pred["prediction_confidence"], pred["model_version"],
            pred["prediction_date"], pred["prediction_time"], pred["feature_snapshot"],
        )
        for pred in predictions
    ]
    client.insert(TABLE_ML_PREDICTIONS, rows, column_names=column_names)
    logger.info("Stored %d predictions in ClickHouse", len(predictions))
