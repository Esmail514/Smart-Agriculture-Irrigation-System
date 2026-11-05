"""Bounded immutable metadata for independently evaluated stream plans."""
import logging
import os

logger = logging.getLogger(__name__)
DEFAULT_MAX_ROWS = 100_000


def materialize_metadata_snapshot(spark, dataframe, *, name, max_rows=None):
    """Copy small JDBC metadata into driver-owned rows before starting streams.

    A cached JDBC plan can be re-read after eviction. Rebuilding a DataFrame
    from collected, typed rows instead keeps both accepted/quarantine plans on
    the same catalog version, without changing file sinks or checkpoints.
    Restart the consumer to apply metadata changes. Collect at most limit+1
    rows and fail before stream startup rather than risking unbounded memory.
    """
    if max_rows is None:
        try:
            max_rows = int(os.getenv("METADATA_SNAPSHOT_MAX_ROWS", str(DEFAULT_MAX_ROWS)))
        except ValueError as error:
            raise ValueError("METADATA_SNAPSHOT_MAX_ROWS must be a positive integer") from error
    if type(max_rows) is not int or max_rows <= 0:
        raise ValueError("Metadata snapshot row limit must be a positive integer")
    rows = dataframe.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise RuntimeError(f"{name} metadata exceeds startup snapshot limit ({max_rows} rows)")
    result = spark.createDataFrame(rows, schema=dataframe.schema)
    logger.info("metadata_snapshot_loaded name=%s rows=%d refresh=consumer_restart", name, len(rows))
    return result
