import pytest
from unittest.mock import patch, Mock, call
from deploy.entra import provision_entra, EntraResult, GRAPH_BASE

APP_NAME = "Fabric-Pipeline-AI-Analytics"
ADMIN_HEADERS = {"Authorization": "Bearer admin-token"}

SAMPLE_APP_ID = "app-guid-123"
SAMPLE_OBJECT_ID = "obj-guid-456"
SAMPLE_SPN_ID = "spn-guid-789"
SAMPLE_SECRET = "generated-secret-value"
GRAPH_SPN_ID = "graph-spn-guid-000"
ROLE_CD_ID = "e4c9e354-4dc5-45b8-9e7c-e1393b0b1a20"
ROLE_REPORTS_ID = "230c1aed-a721-4c5d-9cb4-a90514e508ef"


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


def test_provision_entra_creates_app_when_not_exists():
    with patch("deploy.entra.requests.get") as mock_get, \
         patch("deploy.entra.requests.post") as mock_post:
        # GET existing apps → not found
        mock_get.side_effect = [
            _ok({"value": []}),  # check app existence
            _ok({"value": [{"id": GRAPH_SPN_ID, "appRoles": [
                {"id": ROLE_CD_ID, "value": "CloudApp-Discovery.Read.All"},
                {"id": ROLE_REPORTS_ID, "value": "Reports.Read.All"},
            ]}]}),  # resolve Microsoft Graph SPN
        ]
        mock_post.side_effect = [
            _created({"appId": SAMPLE_APP_ID, "id": SAMPLE_OBJECT_ID}),  # create app
            _created({"id": SAMPLE_SPN_ID}),  # create SPN
            _created({"secretText": SAMPLE_SECRET}),  # create secret
            _created({}),  # appRoleAssignment CloudApp-Discovery
            _created({}),  # appRoleAssignment Reports
        ]

        result = provision_entra(APP_NAME, ADMIN_HEADERS)

    assert isinstance(result, EntraResult)
    assert result.app_id == SAMPLE_APP_ID
    assert result.object_id == SAMPLE_OBJECT_ID
    assert result.spn_object_id == SAMPLE_SPN_ID
    assert result.client_secret == SAMPLE_SECRET
    assert result.status == "created"


def test_provision_entra_skips_when_exists():
    with patch("deploy.entra.requests.get") as mock_get, \
         patch("deploy.entra.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{
            "appId": SAMPLE_APP_ID,
            "id": SAMPLE_OBJECT_ID,
        }]})

        result = provision_entra(APP_NAME, ADMIN_HEADERS)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"
    assert result.app_id == SAMPLE_APP_ID


def test_provision_entra_resolves_role_ids_dynamically():
    """appRoleAssignments must use IDs resolved from Graph SPN, not hardcoded."""
    custom_role_id = "custom-role-id-999"
    with patch("deploy.entra.requests.get") as mock_get, \
         patch("deploy.entra.requests.post") as mock_post:
        mock_get.side_effect = [
            _ok({"value": []}),
            _ok({"value": [{"id": GRAPH_SPN_ID, "appRoles": [
                {"id": custom_role_id, "value": "CloudApp-Discovery.Read.All"},
                {"id": ROLE_REPORTS_ID, "value": "Reports.Read.All"},
            ]}]}),
        ]
        mock_post.side_effect = [
            _created({"appId": SAMPLE_APP_ID, "id": SAMPLE_OBJECT_ID}),
            _created({"id": SAMPLE_SPN_ID}),
            _created({"secretText": SAMPLE_SECRET}),
            _created({}),
            _created({}),
        ]
        provision_entra(APP_NAME, ADMIN_HEADERS)

    # Find the appRoleAssignment call for CloudApp-Discovery
    role_assignment_calls = [
        c for c in mock_post.call_args_list
        if "appRoleAssignments" in str(c)
    ]
    # The first role assignment must use the dynamically resolved custom_role_id
    first_call_body = role_assignment_calls[0][1]["json"]
    assert first_call_body["appRoleId"] == custom_role_id
