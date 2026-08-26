import json
import joblib
import numpy as np
import shap
from ml.pipelines.settings import MODEL_PATH, FEATURE_NAMES_PATH, SHAP_VALUES_PATH
from ml.pipelines.logger import get_logger

logger = get_logger("explainer")


def compute_shap_values(X_sample) -> dict:
    try:
        model = joblib.load(MODEL_PATH)
        with open(FEATURE_NAMES_PATH) as f:
            feature_names = json.load(f)

        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)

        if isinstance(shap_values, list):
            shap_values = shap_values[1]

        mean_abs_shap = np.abs(shap_values).mean(axis=0)
        importance = dict(zip(feature_names, [round(float(v), 4) for v in mean_abs_shap]))
        importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))

        result = {
            "feature_importance": importance,
            "n_samples": len(X_sample),
        }
        with open(SHAP_VALUES_PATH, "w") as f:
            json.dump(result, f, indent=2)

        logger.info("SHAP values computed for %d features", len(feature_names))
        return result
    except Exception as e:
        logger.error("SHAP computation failed: %s", e)
        return {"error": str(e)}
