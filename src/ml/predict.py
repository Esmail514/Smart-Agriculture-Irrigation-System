#!/usr/bin/env python3
"""Main prediction script for ML irrigation predictions."""
from ml.pipelines.loader import load_training_data
from ml.pipelines.engineer import engineer_features
from ml.pipelines.predictor import predict, store_predictions
from ml.pipelines.logger import get_logger

logger = get_logger("ml_predict")


def main():
    logger.info("=== Starting ML Prediction Pipeline ===")

    data = load_training_data()
    if data.empty:
        logger.error("No data available for predictions")
        return

    features = engineer_features(data)
    predictions = predict(features)
    store_predictions(predictions)

    logger.info("=== ML Prediction Pipeline Complete: %d predictions ===", len(predictions))


if __name__ == "__main__":
    main()
