from __future__ import annotations
import base64
import json
import time
from dataclasses import dataclass, field
import requests

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_PIPELINE_NAME = "pip_daily_ai_ingestion"
_JOB_POLL_INTERVAL = 30
_JOB_TIMEOUT = 1800  # 30 minutes


class PipelineJobError(Exception):
    pass


class TableVerificationError(Exception):
    pass


@dataclass
class PipelineResult:
    pipeline_id: str
    status: str


@dataclass
class ScheduleResult:
    schedule_id: str
    status: str
    next_run: str = ""


@dataclass
class RunResult:
    job_instance_id: str
    status: str
    duration_seconds: float
    tables_verified: list = field(default_factory=list)


def provision_pipeline(
    workspace_id: str,
    nb_cloud_discovery_id: str,
    nb_copilot_usage_id: str,
    headers: dict,
) -> PipelineResult:
    """Create or reuse the daily AI ingestion Data Pipeline."""
    resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/dataPipelines",
        headers=headers,
    )
    resp.raise_for_status()
    existing = [p for p in resp.json().get("value", []) if p["displayName"] == _PIPELINE_NAME]
    if existing:
        return PipelineResult(pipeline_id=existing[0]["id"], status="existing_reused")

    # Create empty pipeline
    create_resp = requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items",
        headers=headers,
        json={
            "displayName": _PIPELINE_NAME,
            "type": "DataPipeline",
            "description": "Pipeline quotidien d'ingestion IA (Cloud Discovery + Copilot Usage)",
        },
    )
    create_resp.raise_for_status()
    pipeline_id = create_resp.json()["id"]

    # Update pipeline definition with both Notebook Activities
    definition = {
        "properties": {
            "activities": [
                {
                    "name": "Ingest_Cloud_Discovery",
                    "type": "TridentNotebook",
                    "typeProperties": {
                        "notebookId": nb_cloud_discovery_id,
                        "workspaceId": workspace_id,
                    },
                },
                {
                    "name": "Ingest_Copilot_Usage",
                    "type": "TridentNotebook",
                    "typeProperties": {
                        "notebookId": nb_copilot_usage_id,
                        "workspaceId": workspace_id,
                    },
                },
            ]
        }
    }
    def_b64 = base64.b64encode(json.dumps(definition).encode()).decode()
    requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
        headers=headers,
        json={
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": def_b64, "payloadType": "InlineBase64"}
                ]
            }
        },
    ).raise_for_status()

    return PipelineResult(pipeline_id=pipeline_id, status="created")


def provision_schedule(
    workspace_id: str,
    pipeline_id: str,
    headers: dict,
    start_date: str = "2026-04-01T06:00:00",
    end_date: str = "2027-03-31T23:59:00",
) -> ScheduleResult:
    """Create or reuse the daily 6h00 schedule (DefaultJob, Romance Standard Time)."""
    sched_url = (
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/DefaultJob/schedules"
    )
    resp = requests.get(sched_url, headers=headers)
    resp.raise_for_status()
    active = [s for s in resp.json().get("value", []) if s.get("enabled")]
    if active:
        return ScheduleResult(schedule_id=active[0]["id"], status="existing_reused")

    create_resp = requests.post(
        sched_url,
        headers=headers,
        json={
            "enabled": True,
            "configuration": {
                "startDateTime": start_date,
                "endDateTime": end_date,
                "localTimeZoneId": "Romance Standard Time",
                "type": "Cron",
                "interval": 1440,
            },
        },
    )
    create_resp.raise_for_status()
    sched_id = create_resp.json().get("id", "")
    return ScheduleResult(schedule_id=sched_id, status="created")


def run_and_monitor(
    workspace_id: str,
    pipeline_id: str,
    lakehouse_id: str,
    headers: dict,
) -> RunResult:
    """Trigger on-demand pipeline run, poll to completion, verify Bronze tables."""
    t_start = time.time()

    # Trigger on-demand run
    run_resp = requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        headers=headers,
    )
    run_resp.raise_for_status()
    job_location = run_resp.headers["Location"]
    job_instance_id = job_location.rstrip("/").split("/")[-1]

    # Poll job status
    deadline = t_start + _JOB_TIMEOUT
    while True:
        if time.time() > deadline:
            raise TimeoutError(f"Pipeline job timed out after {_JOB_TIMEOUT}s")
        status_resp = requests.get(job_location, headers=headers)
        status_resp.raise_for_status()
        body = status_resp.json()
        status = body.get("status", "Unknown")
        if status == "Completed":
            break
        if status in ("Failed", "Cancelled"):
            raise PipelineJobError(
                f"Pipeline job {status}: {body.get('failureReason', 'unknown reason')}"
            )
        time.sleep(_JOB_POLL_INTERVAL)

    duration = time.time() - t_start

    # Verify Bronze tables
    _EXPECTED_TABLES = [
        "bronze.cloud_discovery_apps",
        "bronze.cloud_discovery_users",
        "bronze.copilot_usage_detail",
        "bronze.copilot_usage_trend",
    ]
    tables_resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        headers=headers,
    )
    tables_resp.raise_for_status()
    available = {t["name"] for t in tables_resp.json().get("data", [])}
    missing = [t for t in _EXPECTED_TABLES if t not in available]
    if missing:
        raise TableVerificationError(f"Tables manquantes après le run : {missing}")

    return RunResult(
        job_instance_id=job_instance_id,
        status="completed",
        duration_seconds=round(duration, 1),
        tables_verified=_EXPECTED_TABLES,
    )
