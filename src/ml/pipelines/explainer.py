import json
import os
import tempfile
import joblib
import numpy as np

try:
    import shap
except ImportError:
    shap = None

from ml.pipelines.settings import MODEL_PATH, FEATURE_NAMES_PATH, SHAP_VALUES_PATH
from ml.pipelines.logger import get_logger

logger = get_logger("explainer")


def _atomic_dump_json(obj, target_path):
    """Atomically persist JSON artifacts to prevent corruption on unexpected shutdown."""
    target_dir = os.path.dirname(os.path.abspath(target_path))
    os.makedirs(target_dir, exist_ok=True)
    temp_fd, temp_file = tempfile.mkstemp(dir=target_dir, prefix="artifact_", suffix=".tmp")
    os.close(temp_fd)
    try:
        with open(temp_file, "w") as f:
            json.dump(obj, f, indent=2, allow_nan=False)
        os.replace(temp_file, target_path)
    except Exception:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise


def compute_shap_values(X_sample) -> dict:
    if shap is None:
        logger.warning("SHAP library is not installed")
        return {"error": "shap_not_installed"}
    if len(X_sample) == 0:
        return {"error": "empty_sample"}
    try:
        model = joblib.load(MODEL_PATH)
        with open(FEATURE_NAMES_PATH) as f:
            feature_names = json.load(f)

        if hasattr(X_sample, "columns"):
            X_sample = X_sample[[c for c in feature_names if c in X_sample.columns]]

        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)

        if isinstance(shap_values, list):
            shap_values = shap_values[1]
        elif np.asarray(shap_values).ndim == 3:
            shap_values = shap_values[:, :, 1]

        mean_abs_shap = np.abs(shap_values).mean(axis=0)
        importance = dict(zip(feature_names, [round(float(v), 4) for v in mean_abs_shap]))
        importance = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))

        result = {
            "feature_importance": importance,
            "n_samples": len(X_sample),
        }
        _atomic_dump_json(result, SHAP_VALUES_PATH)

        logger.info("SHAP values computed for %d features", len(feature_names))
        return result
    except Exception as e:
        logger.error("SHAP computation failed: %s", e)
        return {"error": str(e)}


def explain_local_instances(model, X_batch, feature_names) -> list:
    """Compute local feature attributions for a batch of zone instances."""
    if len(X_batch) == 0:
        return []
    try:
        if shap is None:
            logger.warning("SHAP library is not installed; skipping local attributions")
            return [[] for _ in range(len(X_batch))]

        if hasattr(X_batch, "columns"):
            X_batch = X_batch[[c for c in feature_names if c in X_batch.columns]]

        explainer = shap.TreeExplainer(model)
        vals = explainer.shap_values(X_batch)
        if isinstance(vals, list):
            vals = vals[1]
        elif np.asarray(vals).ndim == 3:
            vals = vals[:, :, 1]

        batch_drivers = []
        for row_vals in vals:
            drivers = []
            for name, impact in zip(feature_names, row_vals):
                drivers.append({"feature": name, "impact": round(float(impact), 4)})
            drivers.sort(key=lambda x: abs(x["impact"]), reverse=True)
            batch_drivers.append(drivers[:3])
        return batch_drivers
    except Exception as exc:
        logger.warning("Local SHAP computation skipped: %s", exc)
        return [[] for _ in range(len(X_batch))]

