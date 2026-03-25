import pytest
from unittest.mock import patch, Mock
from deploy.fabric_pipelines import (
    provision_pipeline, provision_schedule, run_and_monitor,
    PipelineResult, ScheduleResult, RunResult,
)

WS_ID = "ws-guid-222"
NB_CD_ID = "nb-cd-001"
NB_CU_ID = "nb-cu-002"
PIP_ID = "pip-guid-444"
JOB_INSTANCE_ID = "job-inst-555"
LKH_ID = "lkh-guid-333"


def _ok(json_body):
    m = Mock(); m.status_code = 200; m.json.return_value = json_body
    m.headers = {}; m.raise_for_status = Mock(); return m


def _created(json_body):
    m = Mock(); m.status_code = 201; m.json.return_value = json_body
    m.headers = {}; m.raise_for_status = Mock(); return m


def _accepted(loc):
    m = Mock(); m.status_code = 202
    m.headers = {"Location": loc}; m.raise_for_status = Mock(); return m


# --- provision_pipeline ---

def test_provision_pipeline_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [
            _created({"id": PIP_ID}),  # create pipeline
            _ok({}),                   # update definition
        ]
        result = provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    assert result.pipeline_id == PIP_ID
    assert result.status == "created"


def test_provision_pipeline_skips_when_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": PIP_ID, "displayName": "pip_daily_ai_ingestion"}]})
        result = provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"


def test_provision_pipeline_includes_both_notebooks(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [_created({"id": PIP_ID}), _ok({})]
        provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    update_call = mock_post.call_args_list[1]
    import base64, json
    parts = update_call[1]["json"]["definition"]["parts"]
    content_b64 = next(p for p in parts if p["path"] == "pipeline-content.json")["payload"]
    definition = json.loads(base64.b64decode(content_b64).decode())
    activity_ids = [a["typeProperties"]["notebookId"] for a in definition["properties"]["activities"]]
    assert NB_CD_ID in activity_ids
    assert NB_CU_ID in activity_ids


# --- provision_schedule ---

def test_provision_schedule_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        result = provision_schedule(WS_ID, PIP_ID, fabric_headers)

    assert result.status == "created"


def test_provision_schedule_uses_default_job_type(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        provision_schedule(WS_ID, PIP_ID, fabric_headers)

    url = mock_post.call_args[0][0]
    assert "DefaultJob" in url


def test_provision_schedule_uses_romance_standard_time(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        provision_schedule(WS_ID, PIP_ID, fabric_headers)

    body = mock_post.call_args[1]["json"]
    assert body["configuration"]["localTimeZoneId"] == "Romance Standard Time"


def test_provision_schedule_skips_when_active(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": "sched-001", "enabled": True}]})
        result = provision_schedule(WS_ID, PIP_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"


# --- run_and_monitor ---

def test_run_and_monitor_success(fabric_headers):
    job_location = f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items/{PIP_ID}/jobs/instances/{JOB_INSTANCE_ID}"
    with patch("deploy.fabric_pipelines.requests.post") as mock_post, \
         patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.time.sleep"):
        mock_post.return_value = _accepted(job_location)
        mock_get.side_effect = [
            _ok({"status": "InProgress"}),
            _ok({"status": "Completed"}),
            _ok({"data": [
                {"name": "bronze.cloud_discovery_apps"},
                {"name": "bronze.cloud_discovery_users"},
                {"name": "bronze.copilot_usage_detail"},
                {"name": "bronze.copilot_usage_trend"},
            ]}),  # list tables
        ]
        result = run_and_monitor(WS_ID, PIP_ID, LKH_ID, fabric_headers)

    assert result.status == "completed"
    assert len(result.tables_verified) == 4


def test_run_and_monitor_raises_on_failure(fabric_headers):
    from deploy.fabric_pipelines import PipelineJobError
    job_location = "https://api.fabric.microsoft.com/v1/.../instances/job-1"
    with patch("deploy.fabric_pipelines.requests.post") as mock_post, \
         patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.time.sleep"):
        mock_post.return_value = _accepted(job_location)
        mock_get.return_value = _ok({"status": "Failed", "failureReason": "Out of memory"})
        with pytest.raises(PipelineJobError) as exc_info:
            run_and_monitor(WS_ID, PIP_ID, LKH_ID, fabric_headers)
    assert "Out of memory" in str(exc_info.value)
