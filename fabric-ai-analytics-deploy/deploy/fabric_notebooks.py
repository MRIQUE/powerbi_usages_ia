from __future__ import annotations
import base64
import json
from dataclasses import dataclass, field
from pathlib import Path
import requests
from deploy.lro import poll_lro

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"

_NOTEBOOKS = [
    {
        "key": "nb_cloud_discovery",
        "display_name": "nb_ingest_cloud_discovery",
        "description": "Ingestion quotidienne Cloud Discovery via Graph API",
        "source_file": "nb_cloud_discovery.py",
    },
    {
        "key": "nb_copilot_usage",
        "display_name": "nb_ingest_copilot_usage",
        "description": "Ingestion quotidienne Copilot Usage via Graph API v1.0",
        "source_file": "nb_copilot_usage.py",
    },
]

_CONTENT_DIR = Path(__file__).parent / "notebook_content"


@dataclass
class NotebooksResult:
    ids: dict = field(default_factory=dict)
    status: str = "created"


def _make_platform_metadata(display_name: str, logical_id: str) -> str:
    meta = {
        "$schema": (
            "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
            "platformProperties/2.0.0/schema.json"
        ),
        "metadata": {"type": "Notebook", "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id},
    }
    return base64.b64encode(json.dumps(meta).encode()).decode()


def _encode_content(source_file: str) -> str:
    content = (_CONTENT_DIR / source_file).read_text(encoding="utf-8")
    return base64.b64encode(content.encode()).decode()


def provision_notebooks(workspace_id: str, headers: dict) -> NotebooksResult:
    """Create or reuse Fabric Notebooks for Cloud Discovery and Copilot Usage ingestion."""
    existing_resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/notebooks",
        headers=headers,
    )
    existing_resp.raise_for_status()
    existing = {nb["displayName"]: nb["id"] for nb in existing_resp.json().get("value", [])}

    result = NotebooksResult()
    all_existed = True

    for i, nb_def in enumerate(_NOTEBOOKS):
        if nb_def["display_name"] in existing:
            result.ids[nb_def["key"]] = existing[nb_def["display_name"]]
            continue

        all_existed = False
        logical_id = f"00000000-0000-0000-0000-{i + 1:012d}"
        payload = {
            "displayName": nb_def["display_name"],
            "description": nb_def["description"],
            "definition": {
                "format": "fabricGitSource",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": _encode_content(nb_def["source_file"]),
                        "payloadType": "InlineBase64",
                    },
                    {
                        "path": ".platform",
                        "payload": _make_platform_metadata(nb_def["display_name"], logical_id),
                        "payloadType": "InlineBase64",
                    },
                ],
            },
        }
        create_resp = requests.post(
            f"{FABRIC_BASE}/workspaces/{workspace_id}/notebooks",
            headers=headers,
            json=payload,
        )
        create_resp.raise_for_status()

        if create_resp.status_code == 202:
            location = create_resp.headers["Location"]
            resource = poll_lro(location, headers)
            result.ids[nb_def["key"]] = resource["id"]
        else:
            result.ids[nb_def["key"]] = create_resp.json()["id"]

    result.status = "existing_reused" if all_existed else "created"
    return result
