#!/usr/bin/env python3
"""Current dryness classifier, with chronological evaluation and latest-zone inference.

Training labels come from the crop moisture rule. This is a rule surrogate;
it is not an independently validated irrigation forecast.
"""
import pandas as pd

from common.config import CLICKHOUSE_JDBC_DRIVER, CLICKHOUSE_JDBC_URL, CLICKHOUSE_PASSWORD, CLICKHOUSE_USER
from common.logger import get_logger, setup_logging
from ml.feature_engineering import engineer_features
from ml.inference import build_predictions, chronological_split, latest_zone_rows
from ml.pipelines.settings import NUMERIC_FEATURES, MIN_TRAINING_ROWS

setup_logging()
logger = get_logger("ml_predictions")
TABLE_PREDICTIONS = "ml_predictions"


def _jdbc_opts():
    return {"driver": CLICKHOUSE_JDBC_DRIVER, "url": CLICKHOUSE_JDBC_URL,
            "user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD}


def load_data(spark):
    zone_query = """SELECT zone_id, zone_name, farm_id, farm_name, crop_id, crop_name,
       sensor_count, avg_soil_moisture, avg_soil_temperature, avg_air_temperature,
       avg_air_humidity, moisture_min, moisture_max, recommended_water_mm,
       last_update, water_status, snapshot_id FROM zone_summary_history FINAL
       WHERE last_update >= today() - INTERVAL 90 DAY"""
    weather_query = """SELECT farm_id, weather_date, avg_temperature, avg_humidity,
       total_rain, avg_wind_speed, last_update FROM farm_weather_daily
       WHERE weather_date >= today() - INTERVAL 90 DAY"""
    zone_df = spark.read.format("jdbc").options(**_jdbc_opts(), dbtable=f"({zone_query}) t").load()
    weather_df = spark.read.format("jdbc").options(**_jdbc_opts(), dbtable=f"({weather_query}) w").load()
    return zone_df, weather_df


def train_and_predict(df):
    if df.empty:
        return []
    latest = latest_zone_rows(df)
    latest = latest.loc[latest["needs_irrigation"].notna()].copy()
    df = df.loc[df["needs_irrigation"].notna()].copy()
    if latest.empty:
        return []
    # Completed-day aggregates cannot be used in point-in-time backtests.
    excluded = {"avg_temperature", "avg_humidity", "total_rain", "avg_wind_speed"}
    features = [f for f in NUMERIC_FEATURES if f in df and f not in excluded]
    train_rows, test_rows = chronological_split(df)
    model_version = "crop-threshold-rule-v2"
    # Stable per-zone rule: a small deficit remains dry regardless of other zones.
    probabilities = (latest["moisture_deficit"] > 0).astype(float).to_numpy()
    if len(train_rows) >= MIN_TRAINING_ROWS and train_rows["needs_irrigation"].nunique() == 2 and test_rows["needs_irrigation"].nunique() == 2:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import roc_auc_score
        model = RandomForestClassifier(n_estimators=100, max_depth=8,
            min_samples_leaf=3, class_weight="balanced", random_state=42, n_jobs=-1)
        model.fit(train_rows[features].fillna(0), train_rows["needs_irrigation"].astype(int))
        holdout_prob = model.predict_proba(test_rows[features].fillna(0))[:, list(model.classes_).index(1)]
        auc = roc_auc_score(test_rows["needs_irrigation"].astype(int), holdout_prob)
        logger.info("Chronological holdout ROC-AUC %.4f; target is current dryness, labels are rule-derived", auc)
        # Production fit uses only observations strictly older than latest inference.
        cutoff = latest["last_update"].min()
        historical = df.loc[pd.to_datetime(df["last_update"], utc=True) < cutoff]
        if len(historical) >= MIN_TRAINING_ROWS and historical["needs_irrigation"].nunique() == 2:
            model.fit(historical[features].fillna(0), historical["needs_irrigation"].astype(int))
            probabilities = model.predict_proba(latest[features].fillna(0))[:, list(model.classes_).index(1)]
            model_version = "spark-rf-current-dryness-v2"
    else:
        logger.warning("Insufficient chronological binary-label coverage; using crop moisture rule")
    return build_predictions(latest, probabilities, model_version, features)


def store_predictions(spark, predictions):
    if not predictions:
        return
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType, TimestampType
    columns = list(predictions[0])
    types = {"zone_id": LongType(), "farm_id": LongType(), "needs_irrigation_today": LongType(),
             "needs_irrigation_tomorrow": LongType(), "prediction_confidence": DoubleType(),
             "prediction_date": DateType(), "prediction_time": TimestampType()}
    schema = StructType([StructField(c, types.get(c, StringType()), c == "needs_irrigation_tomorrow") for c in columns])
    sdf = spark.createDataFrame([tuple(p[c] for c in columns) for p in predictions], schema)
    sdf.write.format("jdbc").mode("append").options(**_jdbc_opts(), dbtable=TABLE_PREDICTIONS).save()
    logger.info("Stored %d latest-zone predictions", len(predictions))


def main():
    from common.session import create_spark
    spark = create_spark("ML Predictions")
    spark.sparkContext.setLogLevel("WARN")
    try:
        zones, weather = load_data(spark)
        features = engineer_features(zones.toPandas(), weather.toPandas())
        store_predictions(spark, train_and_predict(features))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
