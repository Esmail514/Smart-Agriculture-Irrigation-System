"""Tests for the atomic ClickHouse snapshot-replacement helper (A3/A6)."""

from src.common.ch_sync import atomic_replace_statements


def test_statements_build_exchange_plan():
    stmts = atomic_replace_statements("agri_gold.zone_summary_latest")
    tmp = "agri_gold.zone_summary_latest_snapshot_tmp"
    assert stmts[0] == f"DROP TABLE IF EXISTS {tmp}"
    assert stmts[1] == f"CREATE TABLE {tmp} AS agri_gold.zone_summary_latest"
    assert stmts[2] == f"EXCHANGE TABLES agri_gold.zone_summary_latest AND {tmp}"
    assert stmts[3] == f"DROP TABLE IF EXISTS {tmp}"


def test_scratch_table_is_cleaned_up():
    stmts = atomic_replace_statements("agri_gold.farm_weather_daily")
    assert stmts[0].startswith("DROP TABLE IF EXISTS")
    assert stmts[3].startswith("DROP TABLE IF EXISTS")
    assert "agri_gold.farm_weather_daily" in stmts[1]
    assert "EXCHANGE TABLES" in stmts[2]
