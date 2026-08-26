import json
import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, classification_report
from ml.pipelines.settings import (
    ALL_FEATURES, LABEL_COLUMN, MODEL_PARAMS, TEST_SIZE,
    RANDOM_STATE, CV_FOLDS, PRIMARY_METRIC, MODEL_PATH,
    FEATURE_NAMES_PATH, EVALUATION_REPORT_PATH, MIN_TRAINING_ROWS,
)
from ml.pipelines.logger import get_logger

logger = get_logger("trainer")


def train(df: pd.DataFrame) -> dict:
    if len(df) < MIN_TRAINING_ROWS:
        logger.warning("Only %d rows (need %d). Skipping training.", len(df), MIN_TRAINING_ROWS)
        return {"status": "skipped", "reason": "insufficient_data", "rows": len(df)}

    available_features = [f for f in ALL_FEATURES if f in df.columns]
    X = df[available_features].fillna(0)
    y = df[LABEL_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )

    results = {}
    best_score = 0
    best_model = None

    for name, params in MODEL_PARAMS.items():
        logger.info("Training %s...", name)
        try:
            if name == "RandomForest":
                model = RandomForestClassifier(**params)
            elif name == "XGBoost":
                from xgboost import XGBClassifier
                model = XGBClassifier(**params)
            elif name == "LightGBM":
                from lightgbm import LGBMClassifier
                model = LGBMClassifier(**params)
            elif name == "CatBoost":
                from catboost import CatBoostClassifier
                model = CatBoostClassifier(**params)
            else:
                continue

            model.fit(X_train, y_train)
            y_prob = model.predict_proba(X_test)[:, 1]
            auc = roc_auc_score(y_test, y_prob)

            cv_scores = cross_val_score(model, X, y, cv=min(CV_FOLDS, len(X) // 2), scoring=PRIMARY_METRIC)

            results[name] = {
                "roc_auc": round(auc, 4),
                "cv_mean": round(float(cv_scores.mean()), 4),
                "cv_std": round(float(cv_scores.std()), 4),
            }
            logger.info("%s: ROC-AUC=%.4f, CV=%.4f+/-%.4f", name, auc, cv_scores.mean(), cv_scores.std())

            if auc > best_score:
                best_score = auc
                best_model = model
                results["best_model"] = name
        except Exception as e:
            logger.error("%s training failed: %s", name, e)
            results[name] = {"error": str(e)}

    if best_model is not None:
        joblib.dump(best_model, MODEL_PATH)
        with open(FEATURE_NAMES_PATH, "w") as f:
            json.dump(available_features, f)

        report = {
            "status": "success",
            "best_model": results.get("best_model"),
            "best_roc_auc": round(best_score, 4),
            "model_results": results,
            "training_samples": len(X_train),
            "test_samples": len(X_test),
            "features": available_features,
        }
        with open(EVALUATION_REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2)

        logger.info("Best model: %s (ROC-AUC=%.4f)", results.get("best_model"), best_score)
        return report

    return {"status": "failed", "results": results}
