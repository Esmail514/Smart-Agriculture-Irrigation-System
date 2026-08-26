#!/usr/bin/env python3
"""Main training script for ML irrigation model."""
from ml.pipelines.loader import load_training_data
from ml.pipelines.engineer import engineer_features
from ml.pipelines.trainer import train
from ml.pipelines.explainer import compute_shap_values
from ml.pipelines.logger import get_logger
import pandas as pd

logger = get_logger("ml_train")


def main():
    logger.info("=== Starting ML Training Pipeline ===")

    raw_data = load_training_data()
    if raw_data.empty:
        logger.error("No data available for training")
        return

    features = engineer_features(raw_data)
    report = train(features)

    if report.get("status") == "success":
        logger.info("Training complete: %s", report.get("best_model"))
    else:
        logger.warning("Training skipped or failed: %s", report)

    logger.info("=== ML Training Pipeline Complete ===")


if __name__ == "__main__":
    main()
