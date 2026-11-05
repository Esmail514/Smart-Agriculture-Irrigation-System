import json
import joblib
from ml.pipelines.settings import MODEL_PATH, FEATURE_NAMES_PATH, TABLE_ML_PREDICTIONS
from ml.pipelines.clickhouse_client import get_client
from ml.pipelines.logger import get_logger
from ml.inference import build_predictions, latest_zone_rows

logger = get_logger("predictor")


def predict(df):
    df = latest_zone_rows(df)
    if "water_status" in df:
        df = df.loc[df["water_status"].isin(["Dry", "Optimal", "Overwatered"])].copy()
    if df.empty:
        return []
    model = joblib.load(MODEL_PATH)
    with open(FEATURE_NAMES_PATH) as file:
        feature_names = json.load(file)
    missing = set(feature_names) - set(df.columns)
    if missing:
        raise ValueError(f"Prediction feature contract missing columns: {sorted(missing)}")
    latest = df
    probabilities = model.predict_proba(latest[feature_names].fillna(0))[:, list(model.classes_).index(1)]
    return build_predictions(latest, probabilities, "current-dryness-v2", feature_names)


def store_predictions(predictions):
    if not predictions:
        return
    client = get_client()
    try:
        columns = list(predictions[0])
        client.insert(TABLE_ML_PREDICTIONS, [tuple(p[c] for c in columns) for p in predictions], column_names=columns)
    finally:
        client.close()
