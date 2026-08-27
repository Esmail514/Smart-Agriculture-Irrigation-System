#!/usr/bin/env python3
"""Rule-based irrigation prediction pipeline using Spark JDBC.

Reads zone_summary_history + farm_weather_daily from ClickHouse,
generates irrigation predictions, and stores them in ml_predictions.
No external ML libraries required beyond scikit-learn + pandas.
"""
import json
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from common.config import (
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_JDBC_DRIVER,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
)
from common.logger import setup_logging, get_logger
from common.session import create_spark

setup_logging()
logger = get_logger("ml_predictions")

TABLE_PREDICTIONS = "ml_predictions"
MIN_TRAINING_ROWS = 50
IRRIGATION_THRESHOLD = 0.50


def _jdbc_opts():
    return {
        "driver": CLICKHOUSE_JDBC_DRIVER,
        "url": CLICKHOUSE_JDBC_URL,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
    }


def load_data(spark):
    zone_query = """
    SELECT zone_id, zone_name, farm_id, farm_name, crop_id, crop_name,
           sensor_count, avg_soil_moisture, avg_soil_temperature,
           avg_air_temperature, avg_air_humidity, moisture_min, moisture_max,
           recommended_water_mm, last_update, water_status
    FROM zone_summary_history
    WHERE last_update >= today() - INTERVAL 90 DAY
    """
    weather_query = """
    SELECT farm_id, weather_date, avg_temperature, avg_humidity,
           total_rain, avg_wind_speed
    FROM farm_weather_daily
    WHERE weather_date >= today() - INTERVAL 90 DAY
    """

    logger.info("Loading zone history...")
    zone_df = spark.read.format("jdbc").options(**_jdbc_opts(), dbtable=f"({zone_query}) t").load()
    logger.info("Loaded %d zone records", zone_df.count())

    logger.info("Loading weather data...")
    try:
        weather_df = spark.read.format("jdbc").options(**_jdbc_opts(), dbtable=f"({weather_query}) w").load()
        logger.info("Loaded %d weather records", weather_df.count())
    except Exception:
        logger.warning("No weather data available, proceeding without")
        weather_df = None

    return zone_df, weather_df


def engineer_features(zone_pdf, weather_pdf=None):
    df = zone_pdf.copy()
    df["needs_irrigation"] = (df["water_status"] == "Dry").astype(int)

    df["moisture_deficit"] = df["moisture_min"] - df["avg_soil_moisture"]
    df["moisture_surplus"] = df["avg_soil_moisture"] - df["moisture_max"]

    dt = pd.to_datetime(df["last_update"])
    df["month"] = dt.dt.month
    df["day_of_week"] = dt.dt.dayofweek
    df["season"] = dt.dt.month.map({
        12: 3, 1: 3, 2: 3, 3: 0, 4: 0, 5: 0,
        6: 1, 7: 1, 8: 1, 9: 2, 10: 2, 11: 2,
    })

    if len(df) > 1:
        df = df.sort_values(["zone_id", "last_update"])
        df["moisture_rolling_mean_3"] = (
            df.groupby("zone_id")["avg_soil_moisture"]
            .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )
        df["moisture_trend"] = df["avg_soil_moisture"] - df["moisture_rolling_mean_3"]
    else:
        df["moisture_rolling_mean_3"] = df["avg_soil_moisture"]
        df["moisture_trend"] = 0.0

    df["avg_temperature"] = df["avg_air_temperature"]
    df["avg_humidity"] = df["avg_air_humidity"]
    df["total_rain"] = 0.0
    df["avg_wind_speed"] = 0.0

    for col in [
        "avg_soil_moisture", "avg_soil_temperature", "avg_air_temperature",
        "avg_air_humidity", "avg_temperature", "avg_humidity",
        "moisture_deficit", "moisture_surplus",
        "moisture_rolling_mean_3", "moisture_trend",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def train_and_predict(df):
    numeric_features = [
        "avg_soil_moisture", "avg_soil_temperature", "avg_air_temperature",
        "avg_air_humidity", "moisture_deficit", "moisture_surplus",
        "sensor_count", "avg_temperature", "avg_humidity",
        "total_rain", "avg_wind_speed",
        "moisture_rolling_mean_3", "moisture_trend",
        "month", "day_of_week", "season",
    ]
    available = [f for f in numeric_features if f in df.columns]

    X = df[available].fillna(0)
    y = df["needs_irrigation"]
    n_classes = len(y.unique())

    if len(df) >= MIN_TRAINING_ROWS and n_classes >= 2:
        from sklearn.model_selection import cross_val_score

        model = RandomForestClassifier(
            n_estimators=100, max_depth=8, min_samples_leaf=3,
            class_weight="balanced", random_state=42, n_jobs=-1,
        )
        cv = min(5, n_classes, len(y))
        scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
        logger.info("Training ROC-AUC: %.4f (mean)", scores.mean())

        model.fit(X, y)
        probs = model.predict_proba(X)[:, 1]
        model_version = "spark-rf-v1.0"
    else:
        logger.warning(
            "rows=%d (< %d) or classes=%d (< 2). Using rule-based predictions.",
            len(df), MIN_TRAINING_ROWS, n_classes,
        )
        max_deficit = df["moisture_deficit"].clip(lower=0).max()
        if max_deficit > 0:
            probs = (df["moisture_deficit"].clip(lower=0) / max_deficit).values
        else:
            probs = np.zeros(len(df))
        model_version = "rule-based-v1.0"

    now = datetime.now(timezone.utc)
    predictions = []
    for i, (_, row) in enumerate(df.iterrows()):
        prob = float(probs[i]) if not np.isnan(probs[i]) else 0.0
        predictions.append({
            "prediction_id": str(uuid.uuid4()),
            "zone_id": int(row.get("zone_id", 0)),
            "zone_name": str(row.get("zone_name", "")),
            "farm_id": int(row.get("farm_id", 0)),
            "farm_name": str(row.get("farm_name", "")),
            "crop_name": str(row.get("crop_name", "")),
            "needs_irrigation_today": int(prob >= IRRIGATION_THRESHOLD),
            "needs_irrigation_tomorrow": int(prob >= IRRIGATION_THRESHOLD),
            "prediction_confidence": round(prob, 4),
            "model_version": model_version,
            "prediction_date": now.strftime("%Y-%m-%d"),
            "prediction_time": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "feature_snapshot": json.dumps({f: float(row.get(f, 0)) for f in available[:5]}),
        })

    logger.info("Generated %d predictions", len(predictions))
    return predictions


def store_predictions(spark, predictions):
    if not predictions:
        return

    columns = [
        "prediction_id", "zone_id", "zone_name",
        "farm_id", "farm_name", "crop_name",
        "needs_irrigation_today", "needs_irrigation_tomorrow",
        "prediction_confidence", "model_version",
        "prediction_date", "prediction_time", "feature_snapshot",
    ]
    rows = [
        tuple(p[c] for c in columns)
        for p in predictions
    ]
    pdf = pd.DataFrame(rows, columns=columns)

    sdf = spark.createDataFrame(pdf)
    sdf.write.format("jdbc").mode("append").options(**_jdbc_opts(), dbtable=f"agri_gold.{TABLE_PREDICTIONS}").save()
    logger.info("Stored %d predictions in ClickHouse", len(rows))


def main():
    spark = create_spark("ML Predictions")
    spark.sparkContext.setLogLevel("WARN")

    try:
        zone_df, weather_df = load_data(spark)
        zone_pdf = zone_df.toPandas()
        logger.info("Zone data: %d rows", len(zone_pdf))

        if zone_pdf.empty:
            logger.error("No zone data available for predictions")
            return

        features = engineer_features(zone_pdf)
        predictions = train_and_predict(features)
        store_predictions(spark, predictions)

        logger.info("ML predictions pipeline complete: %d predictions", len(predictions))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
