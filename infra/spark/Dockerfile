# Match Spark 3.5 and the model serialization version used by training.
FROM jupyter/pyspark-notebook:spark-3.5.0
USER root
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt && python -c "import psycopg2, requests, pandas, sklearn, clickhouse_connect"
