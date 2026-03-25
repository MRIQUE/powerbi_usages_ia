import os
import pytest
from deploy.config import load_config, ConfigError, DeployConfig


def test_load_config_from_env_vars(base_env_vars):
    cfg = load_config()
    assert cfg.tenant_id == "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    assert cfg.client_id == "bbbbbbbb-0000-0000-0000-bbbbbbbbbbbb"
    assert cfg.client_secret == "test-secret-value"
    assert cfg.capacity_id == "cccccccc-0000-0000-0000-cccccccccccc"
    assert cfg.key_vault_url == "https://kv-test.vault.azure.net/"
    assert cfg.kv_secret_name_client_id == "pipeline-client-id"
    assert cfg.kv_secret_name_client_secret == "pipeline-client-secret"


def test_admin_token_defaults_to_empty(base_env_vars):
    cfg = load_config()
    assert cfg.admin_token == ""


def test_admin_token_loaded_when_set(base_env_vars, monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "my-admin-token")
    cfg = load_config()
    assert cfg.admin_token == "my-admin-token"


def test_missing_required_var_raises(monkeypatch):
    for var in ["TENANT_ID", "CLIENT_ID", "CLIENT_SECRET", "CAPACITY_ID",
                "KEY_VAULT_URL", "KV_SECRET_NAME_CLIENT_ID", "KV_SECRET_NAME_CLIENT_SECRET"]:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ConfigError) as exc_info:
        load_config()
    assert "TENANT_ID" in str(exc_info.value)


def test_missing_single_var_names_it(base_env_vars, monkeypatch):
    monkeypatch.delenv("CAPACITY_ID")
    with pytest.raises(ConfigError) as exc_info:
        load_config()
    assert "CAPACITY_ID" in str(exc_info.value)
