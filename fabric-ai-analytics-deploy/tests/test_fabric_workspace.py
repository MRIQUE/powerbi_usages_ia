import pytest
from unittest.mock import patch, Mock
from deploy.fabric_workspace import provision_workspace, WorkspaceResult

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
WS_NAME = "WS_AI_Analytics_ADENES"
CAPACITY_ID = "cap-guid-111"
SPN_ID = "spn-guid-789"
WS_ID = "ws-guid-222"


def _ok(json_body):
    m = Mock()
    m.status_code = 200
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def _created(json_body):
    m = Mock()
    m.status_code = 201
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def test_provision_workspace_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_workspace.requests.get") as mock_get, \
         patch("deploy.fabric_workspace.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [
            _created({"id": WS_ID}),  # create workspace
            _created({}),             # role assignment
        ]
        result = provision_workspace(WS_NAME, CAPACITY_ID, SPN_ID, fabric_headers)

    assert result.workspace_id == WS_ID
    assert result.status == "created"


def test_provision_workspace_skips_when_exists(fabric_headers):
    with patch("deploy.fabric_workspace.requests.get") as mock_get, \
         patch("deploy.fabric_workspace.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": WS_ID, "displayName": WS_NAME}]})
        result = provision_workspace(WS_NAME, CAPACITY_ID, SPN_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.workspace_id == WS_ID
    assert result.status == "existing_reused"


def test_provision_workspace_assigns_contributor_role(fabric_headers):
    with patch("deploy.fabric_workspace.requests.get") as mock_get, \
         patch("deploy.fabric_workspace.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [_created({"id": WS_ID}), _created({})]
        provision_workspace(WS_NAME, CAPACITY_ID, SPN_ID, fabric_headers)

    role_call = mock_post.call_args_list[1]
    body = role_call[1]["json"]
    assert body["role"] == "Contributor"
    assert body["principal"]["id"] == SPN_ID
    assert body["principal"]["type"] == "ServicePrincipal"
