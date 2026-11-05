from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

from common.config import POSTGRES_JDBC_PROPERTIES, POSTGRES_JDBC_URL
from common.metadata_snapshot import materialize_metadata_snapshot


def load_irrigation_rules(spark):
    rules_df = (
        spark.read
        .jdbc(
            url=POSTGRES_JDBC_URL,
            table="(SELECT crop_id, moisture_min, moisture_max, recommended_water_mm "
                  "FROM irrigation_rules) AS irrigation_rules",
            properties=POSTGRES_JDBC_PROPERTIES,
        )
    )
    typed = (
        rules_df
        .withColumn("moisture_min", col("moisture_min").cast(DoubleType()))
        .withColumn("moisture_max", col("moisture_max").cast(DoubleType()))
        .withColumn("recommended_water_mm", col("recommended_water_mm").cast(DoubleType()))
    )
    # Invalid rules must never create a numerical irrigation recommendation.
    # A missing rule remains explicit Unknown_NoRule after the left join.
    valid = typed.filter(
        col("moisture_min").between(0, 100)
        & col("moisture_max").between(0, 100)
        & (col("moisture_min") < col("moisture_max"))
        & col("recommended_water_mm").between(0, 200)
    )
    return materialize_metadata_snapshot(spark, valid, name="irrigation_rules")
