import pytest
from unittest.mock import patch, Mock
from deploy.lro import poll_lro, FabricLROError

OPERATION_URL = "https://api.fabric.microsoft.com/v1/operations/op-123"
HEADERS = {"Authorization": "Bearer token"}


def _make_response(status_code, json_body):
    m = Mock()
    m.status_code = status_code
    m.json.return_value = json_body
    m.headers = {}
    return m


def test_poll_lro_success_returns_resource(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})
    phase1_succeeded = _make_response(200, {"status": "Succeeded"})
    phase2_result = _make_response(200, {"id": "lakehouse-123", "displayName": "lkh_ai_analytics"})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"):
        mock_get.side_effect = [phase1_running, phase1_succeeded, phase2_result]
        result = poll_lro(OPERATION_URL, fabric_headers, timeout_s=120, poll_interval=1)

    assert result == {"id": "lakehouse-123", "displayName": "lkh_ai_analytics"}
    assert mock_get.call_count == 3
    # Phase 2 must call /result
    last_call_url = mock_get.call_args_list[2][0][0]
    assert last_call_url == OPERATION_URL + "/result"


def test_poll_lro_failed_raises(fabric_headers):
    phase1_failed = _make_response(200, {"status": "Failed", "error": {"message": "Quota exceeded"}})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"):
        mock_get.return_value = phase1_failed
        with pytest.raises(FabricLROError) as exc_info:
            poll_lro(OPERATION_URL, fabric_headers, timeout_s=60, poll_interval=1)

    assert "Quota exceeded" in str(exc_info.value)


def test_poll_lro_timeout_raises(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"), \
         patch("deploy.lro.time.monotonic") as mock_time:
        mock_get.return_value = phase1_running
        mock_time.side_effect = [0, 0, 400]  # start, first check, timeout exceeded
        with pytest.raises(TimeoutError):
            poll_lro(OPERATION_URL, fabric_headers, timeout_s=300, poll_interval=1)


def test_poll_lro_uses_retry_after_header(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})
    phase1_running.headers = {"Retry-After": "45"}
    phase1_succeeded = _make_response(200, {"status": "Succeeded"})
    phase2_result = _make_response(200, {"id": "res-1"})

    sleep_calls = []
    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        mock_get.side_effect = [phase1_running, phase1_succeeded, phase2_result]
        poll_lro(OPERATION_URL, fabric_headers, timeout_s=300, poll_interval=30)

    assert 45 in sleep_calls
