import pytest
import base64
from unittest.mock import patch, Mock
from deploy.fabric_notebooks import provision_notebooks, NotebooksResult

WS_ID = "ws-guid-222"
NB_CD_ID = "nb-cd-guid-001"
NB_CU_ID = "nb-cu-guid-002"
OP_URL = "https://api.fabric.microsoft.com/v1/operations/op-nb"


def _ok(json_body):
    m = Mock()
    m.status_code = 200
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def _accepted(loc):
    m = Mock()
    m.status_code = 202
    m.headers = {"Location": loc}
    m.raise_for_status = Mock()
    return m


def test_provision_notebooks_creates_both_when_not_exist(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.side_effect = [{"id": NB_CD_ID}, {"id": NB_CU_ID}]

        result = provision_notebooks(WS_ID, fabric_headers)

    assert result.ids["nb_cloud_discovery"] == NB_CD_ID
    assert result.ids["nb_copilot_usage"] == NB_CU_ID
    assert result.status == "created"
    assert mock_post.call_count == 2


def test_provision_notebooks_skips_existing(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": [
            {"id": NB_CD_ID, "displayName": "nb_ingest_cloud_discovery"},
            {"id": NB_CU_ID, "displayName": "nb_ingest_copilot_usage"},
        ]})
        result = provision_notebooks(WS_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"
    assert result.ids["nb_cloud_discovery"] == NB_CD_ID


def test_provision_notebooks_encodes_content_as_base64(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.side_effect = [{"id": NB_CD_ID}, {"id": NB_CU_ID}]

        provision_notebooks(WS_ID, fabric_headers)

    first_call_body = mock_post.call_args_list[0][1]["json"]
    parts = first_call_body["definition"]["parts"]
    content_part = next(p for p in parts if p["path"] == "notebook-content.py")
    # Verify it's valid base64
    decoded = base64.b64decode(content_part["payload"]).decode("utf-8")
    assert len(decoded) > 0
