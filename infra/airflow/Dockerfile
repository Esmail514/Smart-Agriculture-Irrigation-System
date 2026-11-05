# Runtime required by all three Airflow services; no startup-time pip installs.
FROM apache/airflow:2.9.1
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.9.1" "docker==7.1.0" "requests==2.32.5"
COPY infra/airflow/init_airflow.py /opt/airflow/init_airflow.py

