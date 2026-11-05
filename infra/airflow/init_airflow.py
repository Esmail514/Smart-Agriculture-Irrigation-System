"""Initialize Airflow once, retaining an existing administrator and failing closed."""
import json
import os
import subprocess


def initialize(run=subprocess.run, environ=None):
    env = os.environ if environ is None else environ
    # Do not print command arguments: user creation includes its password.
    def command(args):
        result = run(['airflow', *args], capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(f'Airflow {args[0]} initialization failed (exit={result.returncode})')
        return result.stdout

    command(['db', 'migrate'])
    existing = json.loads(command(['users', 'list', '--output', 'json']))
    username = env.get('_AIRFLOW_WWW_USER_USERNAME', '')
    password = env.get('_AIRFLOW_WWW_USER_PASSWORD', '')
    if not username or not password:
        raise ValueError('Explicit Airflow administrator username and password are required')
    if not any(user.get('username') == username for user in existing):
        command(['users', 'create', '--username', username, '--password', password,
                 '--firstname', env.get('_AIRFLOW_WWW_USER_FIRSTNAME', 'Farm'),
                 '--lastname', env.get('_AIRFLOW_WWW_USER_LASTNAME', 'Administrator'),
                 '--email', env.get('_AIRFLOW_WWW_USER_EMAIL', 'admin@localhost'), '--role', 'Admin'])
    print('Airflow schema and administrator verified')


if __name__ == '__main__':
    initialize()
