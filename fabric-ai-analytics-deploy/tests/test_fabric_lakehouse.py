import pytest
from unittest.mock import patch, Mock
from deploy.fabric_lakehouse import provision_lakehouse, LakehouseResult

WS_ID = "ws-guid-222"
LKH_ID = "lkh-guid-333"
LKH_NAME = "lkh_ai_analytics"
OP_URL = "https://api.fabric.microsoft.com/v1/operations/op-444"


def _ok(json_body):
    m = Mock()
    m.status_code = 200
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def _accepted(location_url):
    m = Mock()
    m.status_code = 202
    m.headers = {"Location": location_url}
    m.raise_for_status = Mock()
    return m


def _created(json_body):
    m = Mock()
    m.status_code = 201
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def test_provision_lakehouse_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.return_value = {"id": LKH_ID}

        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    assert result.lakehouse_id == LKH_ID
    assert result.status == "created"
    mock_lro.assert_called_once_with(OP_URL, fabric_headers)


def test_provision_lakehouse_handles_201_sync(fabric_headers):
    """Fabric may return 201 directly (no LRO)."""
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": LKH_ID})

        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    mock_lro.assert_not_called()
    assert result.lakehouse_id == LKH_ID
    assert result.status == "created"


def test_provision_lakehouse_skips_when_exists(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": LKH_ID, "displayName": LKH_NAME}]})
        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"
    assert result.lakehouse_id == LKH_ID


def test_provision_lakehouse_enables_schemas(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.return_value = {"id": LKH_ID}
        provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    call_body = mock_post.call_args[1]["json"]
    assert call_body["creationPayload"]["enableSchemas"] is True
