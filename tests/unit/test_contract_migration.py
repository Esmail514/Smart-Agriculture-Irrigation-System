import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("migration", ROOT / "scripts/migrate_contract_v2.py")
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


class Client:
    def __init__(self, tables):
        self.tables, self.statements = tables, []

    def rows(self, sql):
        return [{"name": key, "engine": engine} for key, engine in self.tables.items()]

    def run(self, sql):
        self.statements.append(sql)


def test_existing_history_is_copied_exchanged_and_retained():
    client = Client({"zone_summary_history": "MergeTree"})
    migration.migrate(client)
    operations = client.statements
    copy = next(x for x in operations if x.startswith("INSERT INTO agri_gold.zone_summary_history_contract_v2"))
    swap = "EXCHANGE TABLES agri_gold.zone_summary_history AND agri_gold.zone_summary_history_contract_v2"
    keep = "RENAME TABLE agri_gold.zone_summary_history_contract_v2 TO agri_gold.zone_summary_history_legacy_v1"
    assert operations.index(copy) < operations.index(swap) < operations.index(keep)
    assert not any(x.startswith("DROP TABLE agri_gold.zone_summary_history") for x in operations)
    assert any("CREATE TABLE" in x and "ReplacingMergeTree(snapshot_id)" in x for x in operations)
    assert not any(x.startswith("ALTER TABLE agri_gold.zone_summary_history") for x in operations)
    assert any("DateTime64(6, 'UTC')" in x for x in operations if x.startswith("CREATE TABLE agri_gold.zone_summary_history_contract_v2"))


def test_retry_after_exchange_recovers_backup_without_replacing_good_table():
    client = Client({"zone_summary_history": "ReplacingMergeTree", "zone_summary_history_contract_v2": "MergeTree"})
    migration.migrate(client)
    assert client.statements[0] == "RENAME TABLE agri_gold.zone_summary_history_contract_v2 TO agri_gold.zone_summary_history_legacy_v1"
    assert not any(x.startswith("EXCHANGE TABLES") for x in client.statements)


def test_current_engines_are_not_rebuilt_again():
    client = Client({"ml_predictions": "ReplacingMergeTree", "ml_predictions_legacy_v1": "MergeTree"})
    migration.migrate(client)
    assert not any(x.startswith("EXCHANGE TABLES") or x.startswith("INSERT INTO") for x in client.statements)


def test_unknown_history_engine_is_not_overwritten():
    import pytest
    client = Client({"zone_summary_history": "Log"})
    with pytest.raises(RuntimeError, match="Unexpected engine"):
        migration.migrate(client)
    assert not any(x.startswith("DROP TABLE") or x.startswith("EXCHANGE") for x in client.statements)


def test_api_and_pipeline_pins_can_be_installed_together():
    def pins(filename):
        return dict(line.strip().split("==", 1) for line in (ROOT / filename).read_text().splitlines()
                    if "==" in line and not line.lstrip().startswith("#"))
    api_pins, pipeline_pins = pins("requirements-api.txt"), pins("requirements.txt")
    for name in api_pins.keys() & pipeline_pins.keys():
        assert api_pins[name] == pipeline_pins[name], f"Conflicting dependency {name}"


def test_migration_refuses_redirects_and_bypasses_external_proxies(monkeypatch):
    from types import SimpleNamespace
    import pytest

    options = {}

    def redirect(*args, **kwargs):
        options.update(kwargs)
        return SimpleNamespace(status_code=302)

    monkeypatch.setattr(migration.requests, 'post', redirect)
    with pytest.raises(RuntimeError, match='redirected'):
        migration.ClickHouse('http://localhost:8123').run('SELECT 1')
    assert options['allow_redirects'] is False
    assert options['proxies'] == {'http': '', 'https': '', 'all': ''}
