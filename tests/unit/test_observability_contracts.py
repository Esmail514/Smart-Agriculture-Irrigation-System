"""Validate monitoring deployment and dashboard query contracts without Docker."""
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def read_yaml(path):
    return yaml.safe_load((ROOT / path).read_text(encoding='utf-8'))


def test_every_scrape_and_probe_target_is_a_compose_service():
    compose = read_yaml('docker-compose.yml')
    services = compose['services']
    config = read_yaml('infra/prometheus/prometheus.yml')
    for job in config['scrape_configs']:
        for group in job['static_configs']:
            for target in group['targets']:
                host = target.split('://')[-1].split(':')[0]
                assert host in services, f'{job["job_name"]} targets missing service {host}'
                assert set(services['prometheus']['networks']) & set(services[host]['networks'])
        if job.get('metrics_path') == '/probe':
            assert job['relabel_configs'][-1]['replacement'] == 'blackbox-exporter:9115'
            assert job['params']['module'][0] in read_yaml('infra/prometheus/blackbox.yml')['modules']


def test_optional_profiles_do_not_disable_pipeline_or_break_dependencies():
    services = read_yaml('docker-compose.yml')['services']
    for name in ('kafka', 'minio', 'postgres', 'clickhouse', 'spark-master', 'spark-worker', 'airflow-scheduler', 'api'):
        assert not services[name].get('profiles'), f'{name} must run in the app deployment'
    for service in services.values():
        for dependency in service.get('depends_on', {}):
            profiles = services[dependency].get('profiles', [])
            assert not profiles or set(profiles) & set(service.get('profiles', []))


def test_dashboards_have_unique_panels_datasources_and_valid_history_filters():
    datasources = {item['uid'] for item in read_yaml('infra/grafana/provisioning/datasources/datasources.yml')['datasources']}
    schema = '\n'.join(path.read_text(encoding='utf-8') for path in (ROOT / 'sql/clickhouse/init').glob('*.sql'))
    tables = set(re.findall(r'CREATE (?:TABLE|VIEW) IF NOT EXISTS agri_gold\.(\w+)', schema))
    for path in (ROOT / 'infra/grafana/dashboards').glob('*.json'):
        dashboard = json.loads(path.read_text(encoding='utf-8'))
        assert len({p['id'] for p in dashboard['panels']}) == len(dashboard['panels'])
        for panel in dashboard['panels']:
            if panel.get('datasource'):
                assert panel['datasource']['uid'] in datasources
            for target in panel.get('targets', []):
                sql = target.get('rawSql', '')
                for table in re.findall(r'FROM agri_gold\.(\w+)', sql):
                    assert table in tables, f'{path.name} refers to missing table {table}'
                    if table not in ('zone_summary_latest', 'farm_summary_latest') and path.name != '05_platform_operations.json':
                        assert '$__timeFilter(' in sql, f'{path.name} ignores date range: {sql}'
                assert "LIKE '$farm_name'" not in sql
                assert "LIKE '$zone_name'" not in sql


def test_platform_health_uses_real_telemetry_not_alert_generation():
    path = ROOT / 'infra/grafana/dashboards/05_platform_operations.json'
    dashboard = json.loads(path.read_text(encoding='utf-8'))
    panels = {panel['id']: panel for panel in dashboard['panels']}
    for number in (1, 4, 5, 6):
        assert panels[number]['datasource']['uid'] == 'prometheus'
        assert 'expr' in panels[number]['targets'][0]
    assert not any('FROM agri_gold.irrigation_alerts' in t.get('rawSql', '') for p in panels.values() for t in p.get('targets', []))


def test_alert_rules_refer_to_enabled_jobs():
    jobs = {job['job_name'] for job in read_yaml('infra/prometheus/prometheus.yml')['scrape_configs']}
    rules = read_yaml('infra/prometheus/rules/agri-alerts.yml')
    for group in rules['groups']:
        for rule in group['rules']:
            for job in re.findall(r'job="([^"]+)"', rule['expr']):
                assert job in jobs


def test_dashboards_fit_the_grid_without_overlapping_panels():
    for path in (ROOT / 'infra/grafana/dashboards').glob('*.json'):
        panels = json.loads(path.read_text(encoding='utf-8'))['panels']
        occupied = {}
        for panel in panels:
            pos = panel['gridPos']
            assert 0 <= pos['x'] < 24 and 0 < pos['w'] <= 24 - pos['x']
            assert pos['y'] >= 0 and pos['h'] > 0
            for x in range(pos['x'], pos['x'] + pos['w']):
                for y in range(pos['y'], pos['y'] + pos['h']):
                    assert (x, y) not in occupied, f'{path.name}: {panel["id"]} overlaps {occupied.get((x, y))}'
                    occupied[x, y] = panel['id']


def test_monitoring_cannot_expose_the_tsdb_admin_api():
    services = read_yaml('docker-compose.yml')['services']
    assert '--web.enable-admin-api' not in services['prometheus']['command']
    for name in ('prometheus', 'grafana'):
        assert all(port.startswith('127.0.0.1:') for port in services[name]['ports'])


def test_platform_queries_preserve_unknown_data_and_metric_types():
    dashboard = json.loads((ROOT / 'infra/grafana/dashboards/05_platform_operations.json').read_text(encoding='utf-8'))
    panels = {panel['id']: panel for panel in dashboard['panels']}
    # Kafka exporter exposes offsets as gauges: counter reset handling can invent traffic.
    traffic = panels[201]['targets'][0]['expr']
    assert 'deriv(kafka_topic_partition_current_offset' in traffic
    assert 'rate(kafka_topic_partition_current_offset' not in traffic
    # Zero errors is meaningful only when the matching route has actual traffic telemetry.
    errors = panels[210]['targets'][0]['expr']
    assert 'or on (route)' in errors and '0 * sum by (route)' in errors
    assert 'vector(0)' not in errors
    percentage = panels[212]['targets'][0]['expr']
    assert '> 0' in percentage and panels[212]['fieldConfig']['defaults']['unit'] == 'percent'
    for panel in panels.values():
        for target in panel.get('targets', []):
            expression = target.get('expr', '')
            if 'agri_api_' in expression:
                assert 'job="application-api"' in expression
                assert '${route:regex}' in expression
    assert {v['name'] for v in dashboard['templating']['list']} == {'topic', 'route', 'service'}


def test_application_alerts_are_scoped_and_latency_has_a_traffic_guard():
    groups = read_yaml('infra/prometheus/rules/agri-alerts.yml')['groups']
    rules = {rule['alert']: rule for group in groups for rule in group['rules']}
    for name in ('ApplicationAPIErrorRateHigh', 'ApplicationAPILatencyHigh'):
        assert 'job="application-api"' in rules[name]['expr']
        assert '> 20' in rules[name]['expr']
    assert 'prometheus_config_last_reload_successful' in rules['PrometheusConfigurationReloadFailed']['expr']
