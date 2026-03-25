from __future__ import annotations
import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class ConfigError(Exception):
    pass


_REQUIRED_VARS = [
    "TENANT_ID",
    "CLIENT_ID",
    "CLIENT_SECRET",
    "CAPACITY_ID",
    "KEY_VAULT_URL",
    "KV_SECRET_NAME_CLIENT_ID",
    "KV_SECRET_NAME_CLIENT_SECRET",
]


@dataclass
class DeployConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    capacity_id: str
    key_vault_url: str
    kv_secret_name_client_id: str
    kv_secret_name_client_secret: str
    admin_token: str = ""


def load_config() -> DeployConfig:
    missing = [v for v in _REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise ConfigError(f"Missing required environment variable(s): {', '.join(missing)}")
    return DeployConfig(
        tenant_id=os.environ["TENANT_ID"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
        capacity_id=os.environ["CAPACITY_ID"],
        key_vault_url=os.environ["KEY_VAULT_URL"],
        kv_secret_name_client_id=os.environ["KV_SECRET_NAME_CLIENT_ID"],
        kv_secret_name_client_secret=os.environ["KV_SECRET_NAME_CLIENT_SECRET"],
        admin_token=os.getenv("ADMIN_TOKEN", ""),
    )
