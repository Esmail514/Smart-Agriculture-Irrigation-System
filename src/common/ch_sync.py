#!/usr/bin/env python3
"""Atomic-ish replacement of ClickHouse snapshot (full-refresh) tables.

A naive `TRUNCATE TABLE x` followed by INSERT leaves `x` empty for the
dashboards if the process dies in between.  Instead we replace the table
contents using ClickHouse's atomic `EXCHANGE TABLES`:

    1. create a scratch table whose structure mirrors the target,
    2. insert the new snapshot into the scratch table (Spark JDBC append),
    3. atomically swap the two tables,
    4. drop the scratch table, which now holds the old rows.

If any step fails the target keeps its previous contents.
"""

import logging

logger = logging.getLogger("ch_sync")


def atomic_replace_statements(table):
    """DDL labels for the swap (drop scratch, clone, exchange, drop scratch)."""
    tmp = f"{table}_snapshot_tmp"
    return [
        f"DROP TABLE IF EXISTS {tmp}",
        f"CREATE TABLE {tmp} AS {table}",
        f"EXCHANGE TABLES {table} AND {tmp}",
        f"DROP TABLE IF EXISTS {tmp}",
    ]


def _run_ddl(jdbc_opts, sql):
    jvm = jdbc_opts.get("_jvm")
    if jvm is None:
        raise ValueError("jdbc_opts must carry the Spark JVM gateway via '_jvm'")
    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_opts["url"],
        jdbc_opts["user"],
        jdbc_opts["password"],
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def atomic_replace(spark, batch_df, table, jdbc_opts):
    """Replace the full contents of ClickHouse `table` with `batch_df`.

    spark: SparkSession (used only for the JDBC write + JVM gateway).
    table: fully-qualified ClickHouse table e.g. "agri_gold.zone_summary_latest".
    jdbc_opts: {"driver","url","user","password"} Spark JDBC options.
    """
    jdbc_opts = dict(jdbc_opts)
    jdbc_opts["_jvm"] = spark.sparkContext._jvm

    tmp = f"{table}_snapshot_tmp"
    statements = atomic_replace_statements(table)

    _run_ddl(jdbc_opts, statements[0])
    _run_ddl(jdbc_opts, statements[1])

    try:
        (
            batch_df.write.format("jdbc")
            .mode("append")
            .option("driver", jdbc_opts["driver"])
            .option("url", jdbc_opts["url"])
            .option("user", jdbc_opts["user"])
            .option("password", jdbc_opts["password"])
            .option("dbtable", tmp)
            .save()
        )
    except Exception:
        # Leave the target untouched; just drop the scratch table and re-raise.
        try:
            _run_ddl(jdbc_opts, statements[3])
        except Exception:
            logger.exception("failed to drop scratch table %s", tmp)
        raise

    _run_ddl(jdbc_opts, statements[2])
    _run_ddl(jdbc_opts, statements[3])
    logger.info("ch_snapshot_replaced table=%s rows=%d", table, batch_df.count())
