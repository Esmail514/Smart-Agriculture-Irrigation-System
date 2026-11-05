import hmac
import os
from datetime import datetime, timezone
from uuid import uuid4

import docker
import requests
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, StrictBool

app = FastAPI(title="Irrigation internal control agent", docs_url=None, redoc_url=None)
bearer = HTTPBearer(auto_error=False)
SERVICES = {
    name: f"agri-{name}" for name in (
        "postgres", "clickhouse", "kafka", "minio", "spark-master", "spark-worker",
        "airflow-scheduler", "airflow-webserver", "sensor-simulator", "grafana",
        "prometheus", "kafka-ui", "spark-history")
}
DAGS = {"sensor_pipeline", "weather_api_pipeline", "ml_pipeline"}


def authenticated(credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    configured = os.getenv("CONTROL_AGENT_TOKEN", "")
    if not configured:
        raise HTTPException(503, "Control agent is not configured")
    if credentials is None or credentials.scheme.lower() != "bearer" or not hmac.compare_digest(
            credentials.credentials.encode(), configured.encode()):
        raise HTTPException(401, "Internal authentication required")


def allowed_actions(name):
    # These storage services underpin authentication and retain project data.
    return ["restart"] if name in {"postgres", "clickhouse", "minio"} else ["start", "stop", "restart"]


def client():
    try:
        return docker.from_env(timeout=15)
    except (docker.errors.DockerException, requests.RequestException):
        raise HTTPException(503, "Docker engine unavailable") from None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/services", dependencies=[Depends(authenticated)])
def services():
    engine = client()
    try:
        result = []
        for name, container_name in SERVICES.items():
            try:
                container = engine.containers.get(container_name)
                result.append({"name": name, "status": container.status, "available": True,
                               "health": container.attrs.get("State", {}).get("Health", {}).get("Status", "unknown"),
                               "allowed_actions": allowed_actions(name)})
            except docker.errors.NotFound:
                result.append({"name": name, "status": "not_created", "available": False,
                               "health": "unknown", "allowed_actions": []})
        return result
    except (docker.errors.DockerException, requests.RequestException):
        raise HTTPException(503, "Docker engine unavailable") from None
    finally:
        engine.close()


@app.post("/services/{name}/{action}", dependencies=[Depends(authenticated)])
def service_action(name: str, action: str):
    if name not in SERVICES:
        raise HTTPException(404, "Unknown service")
    if action not in allowed_actions(name):
        raise HTTPException(422, "Unsupported service action")
    engine = client()
    try:
        container = engine.containers.get(SERVICES[name])
        changed = True
        if action == "start":
            if container.status == "running":
                changed = False
            else:
                container.start()
        elif action == "stop":
            if container.status in {"exited", "created"}:
                changed = False
            else:
                container.stop(timeout=10)
        else:
            container.restart(timeout=10)
        container.reload()
        return {"name": name, "action": action, "status": container.status,
                "changed": changed, "accepted": True}
    except docker.errors.NotFound:
        raise HTTPException(409, "Service is not provisioned; create it with Docker Compose first") from None
    except (docker.errors.DockerException, requests.RequestException):
        raise HTTPException(503, "Service action failed; check system status before retrying") from None
    finally:
        engine.close()


def airflow(method, path, payload=None):
    username, password = os.getenv("AIRFLOW_ADMIN_USER", ""), os.getenv("AIRFLOW_ADMIN_PASSWORD", "")
    if not username or not password:
        raise HTTPException(503, "Airflow control is not configured")
    try:
        response = requests.request(method,
            os.getenv("AIRFLOW_CONTROL_URL", "http://airflow-webserver:8080").rstrip("/") + "/api/v1/" + path,
            auth=(username, password), json=payload, timeout=(3, 5), allow_redirects=False,
            proxies={"http": "", "https": "", "all": ""})
        if response.status_code == 404:
            raise HTTPException(404, "Pipeline is not available in Airflow")
        if response.status_code == 409:
            raise HTTPException(409, "Pipeline operation conflicts with its current state")
        if response.status_code == 422:
            raise HTTPException(422, "Airflow rejected the pipeline parameters")
        if 300 <= response.status_code < 400:
            raise HTTPException(503, "Airflow control redirect refused")
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise HTTPException(503, "Airflow returned an invalid control response")
        dag = path.split("/")[1]
        if method in {"GET", "PATCH"}:
            if result.get("dag_id") != dag or type(result.get("is_paused")) is not bool or ("is_active" in result and type(result["is_active"]) is not bool):
                raise HTTPException(503, "Airflow returned invalid pipeline state")
        elif method == "POST":
            if not isinstance(result.get("dag_run_id"), str) or not result["dag_run_id"].strip() or not isinstance(result.get("state"), str) or result["state"] not in {"queued", "running", "success", "failed"}:
                raise HTTPException(503, "Airflow returned invalid pipeline run state")
        return result
    except (requests.RequestException, ValueError):
        raise HTTPException(503, "Airflow control unavailable") from None


@app.get("/pipelines", dependencies=[Depends(authenticated)])
def pipelines():
    result = []
    for dag in sorted(DAGS):
        try:
            row = airflow("GET", f"dags/{dag}")
            result.append({"dag_id": dag, "is_paused": bool(row["is_paused"]),
                           "is_active": bool(row.get("is_active", True)), "available": True})
        except HTTPException as error:
            if error.status_code != 404:
                raise
            result.append({"dag_id": dag, "is_paused": None, "is_active": False, "available": False})
    return result


@app.post("/pipelines/{dag}/trigger", dependencies=[Depends(authenticated)])
def trigger(dag: str):
    if dag not in DAGS:
        raise HTTPException(404, "Unknown pipeline")
    run = airflow("POST", f"dags/{dag}/dagRuns", {
        "dag_run_id": f"app__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}__{uuid4().hex}", "conf": {}})
    return {"dag_run_id": run["dag_run_id"], "state": run.get("state", "queued"), "accepted": True}


class PipelineState(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_paused: StrictBool


@app.patch("/pipelines/{dag}/state", dependencies=[Depends(authenticated)])
def pipeline_state(dag: str, payload: PipelineState):
    if dag not in DAGS:
        raise HTTPException(404, "Unknown pipeline")
    row = airflow("PATCH", f"dags/{dag}", {"is_paused": payload.is_paused})
    return {"dag_id": row["dag_id"], "is_paused": row["is_paused"]}
