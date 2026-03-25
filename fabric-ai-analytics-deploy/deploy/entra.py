from __future__ import annotations
from dataclasses import dataclass
import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
_GRAPH_APP_ID = "00000003-0000-0000-c000-000000000000"  # well-known Microsoft Graph ID
_PERMISSION_NAMES = ["CloudApp-Discovery.Read.All", "Reports.Read.All"]


@dataclass
class EntraResult:
    app_id: str
    object_id: str
    spn_object_id: str
    client_secret: str
    status: str  # "created" or "existing_reused"


def provision_entra(app_display_name: str, admin_headers: dict) -> EntraResult:
    """Create or reuse App Registration, SPN, secret, and appRoleAssignments."""
    # Check existence
    check_resp = requests.get(
        f"{GRAPH_BASE}/applications?$filter=displayName eq '{app_display_name}'",
        headers=admin_headers,
    )
    check_resp.raise_for_status()
    existing = check_resp.json().get("value", [])
    if existing:
        app = existing[0]
        return EntraResult(
            app_id=app["appId"],
            object_id=app["id"],
            spn_object_id="",
            client_secret="",
            status="existing_reused",
        )

    # Create App Registration
    app_resp = requests.post(
        f"{GRAPH_BASE}/applications",
        headers=admin_headers,
        json={
            "displayName": app_display_name,
            "signInAudience": "AzureADMyOrg",
        },
    )
    app_resp.raise_for_status()
    app_data = app_resp.json()
    app_id = app_data["appId"]
    object_id = app_data["id"]

    # Create Service Principal
    spn_resp = requests.post(
        f"{GRAPH_BASE}/servicePrincipals",
        headers=admin_headers,
        json={"appId": app_id},
    )
    spn_resp.raise_for_status()
    spn_id = spn_resp.json()["id"]

    # Create Client Secret
    secret_resp = requests.post(
        f"{GRAPH_BASE}/applications/{object_id}/addPassword",
        headers=admin_headers,
        json={"passwordCredential": {"displayName": "FabricPipeline"}},
    )
    secret_resp.raise_for_status()
    client_secret = secret_resp.json()["secretText"]

    # Resolve Microsoft Graph SPN and role IDs dynamically
    graph_spn_resp = requests.get(
        f"{GRAPH_BASE}/servicePrincipals?$filter=appId eq '{_GRAPH_APP_ID}'",
        headers=admin_headers,
    )
    graph_spn_resp.raise_for_status()
    graph_spn = graph_spn_resp.json()["value"][0]
    graph_spn_id = graph_spn["id"]
    role_map = {r["value"]: r["id"] for r in graph_spn.get("appRoles", [])}

    # Assign application roles
    for permission_name in _PERMISSION_NAMES:
        role_id = role_map[permission_name]
        requests.post(
            f"{GRAPH_BASE}/servicePrincipals/{spn_id}/appRoleAssignments",
            headers=admin_headers,
            json={
                "principalId": spn_id,
                "resourceId": graph_spn_id,
                "appRoleId": role_id,
            },
        ).raise_for_status()

    return EntraResult(
        app_id=app_id,
        object_id=object_id,
        spn_object_id=spn_id,
        client_secret=client_secret,
        status="created",
    )
