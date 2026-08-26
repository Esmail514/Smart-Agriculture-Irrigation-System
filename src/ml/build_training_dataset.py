#!/usr/bin/env python3
"""Build training dataset from ClickHouse and export to CSV."""
import pandas as pd
from ml.pipelines.loader import load_training_data
from ml.pipelines.engineer import engineer_features
from ml.pipelines.settings import ML_ROOT
from ml.pipelines.logger import get_logger

logger = get_logger("build_dataset")


def main():
    logger.info("Building training dataset...")
    raw = load_training_data()
    if raw.empty:
        logger.error("No data available")
        return

    features = engineer_features(raw)
    output_path = ML_ROOT / "data" / "training_dataset.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    features.to_csv(output_path, index=False)
    logger.info("Saved %d rows to %s", len(features), output_path)


if __name__ == "__main__":
    main()
