from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path

_OUTPUT_FILE = "deploy-output.json"

_PORTAL_URLS = {
    "entra_app": "https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/{app_id}",
    "workspace": "https://app.fabric.microsoft.com/groups/{workspace_id}",
    "lakehouse": "https://app.fabric.microsoft.com/groups/{workspace_id}/lakehouses/{lakehouse_id}",
    "pipeline": "https://app.fabric.microsoft.com/groups/{workspace_id}/datapipelines/{pipeline_id}",
}


class OutputError(Exception):
    pass


def build_portal_url(resource_type: str, **kwargs) -> str:
    template = _PORTAL_URLS.get(resource_type, "")
    return template.format(**kwargs)


def write_output(steps: dict) -> None:
    data = {
        "deployedAt": datetime.now(timezone.utc).isoformat(),
        "steps": steps,
    }
    Path(_OUTPUT_FILE).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def read_output() -> dict:
    path = Path(_OUTPUT_FILE)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("steps", {})


def get_id(output: dict, step_key: str, id_field: str) -> str:
    step = output.get(step_key, {})
    value = step.get(id_field)
    if not value:
        raise OutputError(
            f"ID '{id_field}' manquant pour l'étape '{step_key}' dans {_OUTPUT_FILE}. "
            f"Relancez depuis l'étape concernée ou exécutez le déploiement complet."
        )
    return value
