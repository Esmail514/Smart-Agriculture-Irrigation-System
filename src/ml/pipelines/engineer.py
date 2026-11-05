"""Use the same feature contract for batch training and Spark inference."""
from ml.feature_engineering import engineer_features as _engineer


def engineer_features(df):
    result = _engineer(df)
    for column in ["total_rain", "avg_wind_speed", "avg_temperature", "avg_humidity", "weather_available"]:
        if column in df:
            result[column] = df[column].reindex(result.index)
    return result
