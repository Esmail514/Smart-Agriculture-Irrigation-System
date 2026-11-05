from pyspark.sql.functions import broadcast, col, coalesce, lit
from common.config import POSTGRES_JDBC_URL, POSTGRES_JDBC_PROPERTIES
from common.metadata_snapshot import materialize_metadata_snapshot


def load_sensor_registry(spark):
    """Operational identities, refreshed when the admission stream starts."""
    sql = """SELECT s.sensor_id AS registry_sensor_id, s.sensor_code AS registry_sensor_code,
        z.zone_id AS registry_zone_id, z.zone_name AS registry_zone_name,
        f.farm_id AS registry_farm_id, f.farm_name AS registry_farm_name,
        c.crop_id AS registry_crop_id, c.crop_name AS registry_crop_name
        FROM sensors s JOIN zones z USING(zone_id) JOIN farms f USING(farm_id)
        JOIN crops c USING(crop_id)"""
    registry = spark.read.jdbc(POSTGRES_JDBC_URL, f"({sql}) AS sensor_registry", properties=POSTGRES_JDBC_PROPERTIES)
    return materialize_metadata_snapshot(spark, registry, name="sensor_registry")


def validate_sensor_registry(df, registry):
    joined = df.join(broadcast(registry), col("sensor_id") == col("registry_sensor_id"), "left")
    condition = col("registry_sensor_id").isNotNull()
    for key in ("zone_id", "farm_id", "crop_id", "sensor_code", "zone_name", "farm_name", "crop_name"):
        condition = condition & (col(key) == col(f"registry_{key}"))
    return joined.withColumn("registry_valid", coalesce(condition, lit(False))).drop(*registry.columns)
