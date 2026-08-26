import clickhouse_connect
from ml.pipelines.settings import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
)
from ml.pipelines.logger import get_logger

logger = get_logger("clickhouse_client")


def get_client():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    logger.info("Connected to ClickHouse at %s:%s", CLICKHOUSE_HOST, CLICKHOUSE_PORT)
    return client


def query_to_dataframe(client, query: str):
    import pandas as pd
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)
