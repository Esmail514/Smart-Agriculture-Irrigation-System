"""Exercise deployment failures without touching a live Docker engine."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def module_at(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def test_airflow_database_failure_cannot_report_success():
    init = module_at('airflow_init', 'infra/airflow/init_airflow.py')
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=1, stdout='', stderr='password=do-not-log')

    with pytest.raises(RuntimeError, match='initialization failed') as failure:
        init.initialize(run=run, environ={})
    assert len(calls) == 1
    assert 'do-not-log' not in str(failure.value)


def test_airflow_existing_admin_is_preserved():
    init = module_at('airflow_init', 'infra/airflow/init_airflow.py')
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout=json.dumps([{'username': 'farmer'}]))

    init.initialize(run=run, environ={'_AIRFLOW_WWW_USER_USERNAME': 'farmer',
                                     '_AIRFLOW_WWW_USER_PASSWORD': 'private-password'})
    assert len(calls) == 2
    assert not any('create' in call for call in calls)


def test_docker_execution_checks_exit_status_and_closes_client(monkeypatch):
    helper = module_at('docker_helper', 'orchestration/airflow/dags/utils/docker_exec.py')
    closed = []
    client = SimpleNamespace(containers=SimpleNamespace(get=lambda name: SimpleNamespace(
        exec_run=lambda **kwargs: SimpleNamespace(exit_code=17, output=b'job failed'))),
        close=lambda: closed.append(True))
    monkeypatch.setattr(helper.docker, 'from_env', lambda **kwargs: client)
    with pytest.raises(RuntimeError, match='exit=17'):
        helper.docker_exec('agri-spark-master', ['bash', '/job.sh'])
    assert closed == [True]


def bash_executable():
    candidate = Path('C:/Program Files/Git/bin/bash.exe')
    if candidate.is_file():
        return str(candidate)
    found = shutil.which('bash')
    if not found:
        pytest.skip('Bash unavailable')
    return found


@pytest.mark.parametrize('spark_exit', [0, 17])
def test_batch_submission_is_bounded_and_propagates_failure(tmp_path, spark_exit):
    apps = tmp_path / 'apps'
    job = apps / 'ml/predict_spark.py'
    job.parent.mkdir(parents=True)
    job.write_text('')
    (apps / 'common').mkdir()
    (apps / 'common/__init__.py').write_text('')
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    python = sys.executable.replace('\\', '/')
    (binaries / 'python3').write_text(f'#!/usr/bin/env bash\nexec "{python}" "$@"\n')
    (binaries / 'spark-submit').write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$ARGS_FILE"\nexit "$SPARK_EXIT"\n')
    for path in binaries.iterdir():
        path.chmod(0o755)
    args_file = tmp_path / 'arguments.txt'
    env = {**os.environ, 'PATH': str(binaries) + os.pathsep + os.environ['PATH'],
           'SPARK_APPS_DIR': str(apps), 'SPARK_EXIT': str(spark_exit), 'ARGS_FILE': str(args_file)}
    completed = subprocess.run([bash_executable(), str(ROOT / 'scripts/run_spark_batch_job.sh'),
                                'ml/predict_spark.py'], env=env, capture_output=True, text=True, timeout=30)
    assert completed.returncode == spark_exit, completed.stdout + completed.stderr
    args = args_file.read_text()
    assert 'local[1]' in args and 'spark.cores.max=1' in args
    assert 'spark://spark-master' not in args


def test_failed_init_container_is_not_ready():
    powershell = shutil.which('powershell') or shutil.which('pwsh')
    if not powershell:
        pytest.skip('PowerShell unavailable')
    runner = str(ROOT / 'scripts/run-pipeline.ps1').replace("'", "''")
    program = f"""
    . '{runner}' -NoSimulator
    function docker {{
        $global:LASTEXITCODE = 0
        if ($args[2] -eq '{{{{.State.Status}}}}') {{ return 'exited' }}
        return '17'
    }}
    if (Test-ServiceReady -Name 'test-initializer' -Expect 'exited') {{ exit 1 }}
    exit 0
    """
    result = subprocess.run([powershell, '-NoProfile', '-Command', program],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_default_deployment_has_real_runtime_dependencies_and_no_periodic_killer():
    compose = yaml.safe_load((ROOT / 'docker-compose.yml').read_text(encoding='utf-8'))
    services = compose['services']
    for name in ('airflow-init', 'airflow-webserver', 'airflow-scheduler'):
        assert services[name]['build']['dockerfile'] == 'infra/airflow/Dockerfile'
        assert services[name]['environment']['AIRFLOW__CORE__EXECUTOR'] == 'LocalExecutor'
    for name in ('spark-master', 'spark-worker', 'spark-history'):
        assert services[name]['build']['dockerfile'] == 'infra/spark/Dockerfile'
    assert 'auto_kill_streaming_jobs.sh' not in services['spark-master']['command']
    assert services['airflow-init']['command'] == ['python', '/opt/airflow/init_airflow.py']
    assert services['kafka']['environment']['KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE'] == 'LogAppendTime'
    assert '--config message.timestamp.type=LogAppendTime' in services['kafka-init']['command']
    assert 'LAKEHOUSE_BUCKET' in services['minio-init']['environment']
