from __future__ import annotations
from dataclasses import dataclass
import requests
from deploy.lro import poll_lro

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"


@dataclass
class LakehouseResult:
    lakehouse_id: str
    status: str


def provision_lakehouse(
    display_name: str,
    workspace_id: str,
    headers: dict,
) -> LakehouseResult:
    """Create or reuse a Fabric Lakehouse with schema support enabled."""
    # Check existence
    resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/lakehouses",
        headers=headers,
    )
    resp.raise_for_status()
    existing = [l for l in resp.json().get("value", []) if l["displayName"] == display_name]
    if existing:
        return LakehouseResult(lakehouse_id=existing[0]["id"], status="existing_reused")

    # Create lakehouse
    create_resp = requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/lakehouses",
        headers=headers,
        json={
            "displayName": display_name,
            "creationPayload": {"enableSchemas": True},
        },
    )
    create_resp.raise_for_status()

    if create_resp.status_code == 202:
        location = create_resp.headers["Location"]
        resource = poll_lro(location, headers)
        lakehouse_id = resource["id"]
    else:
        lakehouse_id = create_resp.json()["id"]

    return LakehouseResult(lakehouse_id=lakehouse_id, status="created")
