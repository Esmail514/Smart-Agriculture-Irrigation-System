import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_PORT = int(CLICKHOUSE_HTTP_PORT)
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "agri_analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DB", "agri_gold")
CLICKHOUSE_JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
# NOTE: protocol segment ("http") is REQUIRED - without it clickhouse-jdbc
# >=0.6 defaults to the native TCP protocol and fails against the HTTP port.
# NOTE: compress/decompress disabled - ClickHouse servers >=25.x stopped
# honouring the legacy HTTP block-compression flag, so a compressed read
# fails with "Magic is not correct - expect [-126]" and silently breaks
# Spark's table-exists probe (leading to spurious CREATE TABLE / Code 57).
CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse:http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}"
    f"/{CLICKHOUSE_DATABASE}?compress=0&decompress=0"
)
