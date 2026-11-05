"""Narrow HTTP client for the private system-control agent."""
import os
from urllib.parse import urlsplit
import requests
from fastapi import HTTPException

SERVICES = frozenset({"kafka", "minio", "spark-master", "spark-worker", "airflow-scheduler",
    "airflow-webserver", "clickhouse", "postgres", "sensor-simulator", "grafana", "prometheus",
    "kafka-ui", "spark-history"})
DAGS = frozenset({"sensor_pipeline", "weather_api_pipeline", "ml_pipeline"})
ACTIONS = frozenset({"start", "stop", "restart"})


def validate_service_action(service, action):
    if service not in SERVICES or action not in ACTIONS:
        raise HTTPException(422, "Unsupported service or action")
    if service in {"postgres", "clickhouse", "minio"} and action != "restart":
        raise HTTPException(422, "This core database supports restart only")


def validate_dag(dag):
    if dag not in DAGS:
        raise HTTPException(422, "Unsupported pipeline")


def agent_configuration():
    url = os.getenv("CONTROL_AGENT_URL", "").rstrip("/")
    token = os.getenv("CONTROL_AGENT_TOKEN", "")
    parsed = urlsplit(url)
    if not token or parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.query or parsed.fragment or parsed.username is not None or parsed.password is not None:
        raise HTTPException(503, "System control agent is not configured")
    return url, token


def call_agent(method, path, payload=None):
    url, token = agent_configuration()
    try:
        response = requests.request(method, url + path, json=payload,
            headers={"Authorization": "Bearer " + token}, timeout=(3, 30), allow_redirects=False,
            proxies={"http": "", "https": "", "all": ""})
        errors = {404: "Requested control target is unavailable", 409: "Control operation conflicts with the current system state",
                  422: "Unsupported control operation"}
        if response.status_code in errors:
            raise HTTPException(response.status_code, errors[response.status_code])
        if response.status_code >= 400:
            raise HTTPException(503, "System control agent rejected the request")
        if response.status_code >= 300:
            raise HTTPException(503, "System control agent redirect refused")
        result = response.json()
        if (method == "GET" and not isinstance(result, list)) or (method != "GET" and not isinstance(result, dict)):
            raise HTTPException(503, "System control agent returned an invalid response")
        return result
    except (requests.RequestException, ValueError):
        raise HTTPException(503, "System control agent unavailable") from None
