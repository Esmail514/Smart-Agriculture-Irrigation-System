"""Train a rule-surrogate dryness classifier with a chronological holdout."""
import json
import os
import tempfile
import joblib
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, f1_score, accuracy_score
from ml.inference import chronological_split
from ml.pipelines.settings import (
    ALL_FEATURES, LABEL_COLUMN, MODEL_PARAMS, TEST_SIZE,
    MODEL_PATH, FEATURE_NAMES_PATH, EVALUATION_REPORT_PATH, MIN_TRAINING_ROWS
)


def _atomic_dump(obj, target_path):
    """Atomically persist artifacts to avoid corruption on unexpected shutdown."""
    target_dir = os.path.dirname(os.path.abspath(target_path))
    os.makedirs(target_dir, exist_ok=True)
    temp_fd, temp_file = tempfile.mkstemp(dir=target_dir, prefix="artifact_", suffix=".tmp")
    os.close(temp_fd)
    try:
        if str(target_path).endswith(".json"):
            with open(temp_file, "w") as f:
                json.dump(obj, f, indent=2, allow_nan=False)
        else:
            joblib.dump(obj, temp_file)
        os.replace(temp_file, target_path)
    except Exception:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise


def train(df):
    df = df.loc[df[LABEL_COLUMN].notna()].copy()
    if len(df) < MIN_TRAINING_ROWS:
        return {"status": "skipped", "reason": "insufficient_data", "rows": len(df)}

    # Daily weather is a completed-day aggregate, with no point-in-time archive.
    # Exclude it from backtesting to avoid giving earlier rows future weather.
    excluded = {"avg_temperature", "avg_humidity", "total_rain", "avg_wind_speed"}
    features = [f for f in ALL_FEATURES if f in df and f not in excluded]
    training, testing = chronological_split(df, TEST_SIZE)

    if (
        len(training) < MIN_TRAINING_ROWS
        or training[LABEL_COLUMN].nunique() != 2
        or testing[LABEL_COLUMN].nunique() != 2
    ):
        return {"status": "skipped", "reason": "insufficient_chronological_class_coverage"}

    X_train = training[features].fillna(0)
    y_train = training[LABEL_COLUMN].astype(int)
    X_test = testing[features].fillna(0)
    y_test = testing[LABEL_COLUMN].astype(int)

    # Candidate models tournament: RandomForest & GradientBoosting
    candidates = {
        "RandomForest": RandomForestClassifier(**MODEL_PARAMS["RandomForest"]),
        "GradientBoosting": GradientBoostingClassifier(
            **MODEL_PARAMS.get(
                "GradientBoosting",
                {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.08, "random_state": 42},
            )
        ),
    }

    best_name = None
    best_model = None
    best_auc = -1.0
    best_f1 = 0.0
    tournament_metrics = {}

    for name, candidate in candidates.items():
        try:
            candidate.fit(X_train, y_train)
            probabilities = candidate.predict_proba(X_test)[:, list(candidate.classes_).index(1)]
            auc = float(roc_auc_score(y_test, probabilities))
            preds = (probabilities >= 0.5).astype(int)
            f1 = float(f1_score(y_test, preds, zero_division=0))
            tournament_metrics[name] = {"roc_auc": round(auc, 4), "f1": round(f1, 4)}
            if auc > best_auc:
                best_auc = auc
                best_name = name
                best_model = candidate
                best_f1 = f1
        except Exception as exc:
            tournament_metrics[name] = {"error": str(exc)}

    # Champion / Challenger Gate:
    # 1. Baseline quality gate: minimum acceptable ROC-AUC is 0.50
    # 2. Champion comparison: challenger must not regress against current champion
    champion_promoted = best_auc >= 0.50
    champion_auc = best_auc
    if os.path.exists(EVALUATION_REPORT_PATH) and os.path.exists(MODEL_PATH):
        try:
            with open(EVALUATION_REPORT_PATH) as f:
                prev_report = json.load(f)
                prev_auc = float(prev_report.get("champion_roc_auc", prev_report.get("best_roc_auc", 0.0)))
                if prev_auc > 0:
                    champion_auc = prev_auc
                if best_auc < 0.50 or (prev_auc > 0 and best_auc < prev_auc):
                    champion_promoted = False
                elif champion_promoted:
                    champion_auc = best_auc
        except Exception:
            pass

    if champion_promoted and best_model is not None:
        _atomic_dump(best_model, MODEL_PATH)
        _atomic_dump(features, FEATURE_NAMES_PATH)

    report = {
        "status": "success" if champion_promoted else "degraded",
        "best_model": best_name or "RandomForest",
        "best_roc_auc": round(best_auc, 4),
        "best_f1": round(best_f1, 4),
        "champion_roc_auc": round(champion_auc, 4),
        "tournament": tournament_metrics,
        "evaluation": "chronological_holdout",
        "target": "current_dryness",
        "label_source": "crop_moisture_threshold_rule",
        "forecast_available": False,
        "training_samples": len(training),
        "test_samples": len(testing),
        "features": features,
        "weather_backtest": "excluded_without_point_in_time_archive",
        "champion_promoted": champion_promoted,
    }

    _atomic_dump(report, EVALUATION_REPORT_PATH)
    return report
