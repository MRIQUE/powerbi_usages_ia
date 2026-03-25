from __future__ import annotations
from dataclasses import dataclass
import requests

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"


@dataclass
class WorkspaceResult:
    workspace_id: str
    status: str


def provision_workspace(
    display_name: str,
    capacity_id: str,
    spn_object_id: str,
    headers: dict,
) -> WorkspaceResult:
    """Create or reuse a Fabric workspace and assign SPN as Contributor."""
    # Check existence
    resp = requests.get(f"{FABRIC_BASE}/workspaces", headers=headers)
    resp.raise_for_status()
    existing = [w for w in resp.json().get("value", []) if w["displayName"] == display_name]
    if existing:
        return WorkspaceResult(workspace_id=existing[0]["id"], status="existing_reused")

    # Create workspace
    create_resp = requests.post(
        f"{FABRIC_BASE}/workspaces",
        headers=headers,
        json={"displayName": display_name, "capacityId": capacity_id},
    )
    create_resp.raise_for_status()
    workspace_id = create_resp.json()["id"]

    # Assign SPN as Contributor
    requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/roleAssignments",
        headers=headers,
        json={
            "principal": {"id": spn_object_id, "type": "ServicePrincipal"},
            "role": "Contributor",
        },
    ).raise_for_status()

    return WorkspaceResult(workspace_id=workspace_id, status="created")
