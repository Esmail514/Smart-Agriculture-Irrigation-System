import numpy as np
import pandas as pd
from ml.pipelines.settings import NUMERIC_FEATURES, CLIP_BOUNDS, LABEL_COLUMN, DRY_STATUS
from ml.pipelines.logger import get_logger

logger = get_logger("feature_engineer")


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # Label
    df[LABEL_COLUMN] = (df["water_status"] == DRY_STATUS).astype(int)

    # Engineered features
    df["moisture_deficit"] = df["moisture_min"] - df["avg_soil_moisture"]
    df["moisture_surplus"] = df["avg_soil_moisture"] - df["moisture_max"]

    # Temporal features
    if "last_update" in df.columns:
        dt = pd.to_datetime(df["last_update"])
        df["month"] = dt.dt.month
        df["day_of_week"] = dt.dt.dayofweek
        df["season"] = dt.dt.month.map({
            12: 3, 1: 3, 2: 3,  # Winter
            3: 0, 4: 0, 5: 0,  # Spring
            6: 1, 7: 1, 8: 1,  # Summer
            9: 2, 10: 2, 11: 2,  # Autumn
        })
    else:
        df["month"] = 6
        df["day_of_week"] = 0
        df["season"] = 1

    # Rolling features (per zone)
    if len(df) > 1 and "zone_id" in df.columns:
        df = df.sort_values(["zone_id", "last_update"])
        df["moisture_rolling_mean_3"] = (
            df.groupby("zone_id")["avg_soil_moisture"]
            .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )
        df["moisture_trend"] = df["avg_soil_moisture"] - df["moisture_rolling_mean_3"]
    else:
        df["moisture_rolling_mean_3"] = df["avg_soil_moisture"]
        df["moisture_trend"] = 0.0

    # Fill NaN
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    # Clip outliers
    for col, (low, high) in CLIP_BOUNDS.items():
        if col in df.columns:
            df[col] = df[col].clip(low, high)

    logger.info("Engineered features: %d rows, %d columns", len(df), len(df.columns))
    return df
