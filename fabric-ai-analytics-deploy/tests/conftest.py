import pytest

SAMPLE_TENANT_ID = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
SAMPLE_CLIENT_ID = "bbbbbbbb-0000-0000-0000-bbbbbbbbbbbb"
SAMPLE_CLIENT_SECRET = "test-secret-value"
SAMPLE_CAPACITY_ID = "cccccccc-0000-0000-0000-cccccccccccc"
SAMPLE_KEY_VAULT_URL = "https://kv-test.vault.azure.net/"
SAMPLE_WORKSPACE_ID = "dddddddd-0000-0000-0000-dddddddddddd"
SAMPLE_LAKEHOUSE_ID = "eeeeeeee-0000-0000-0000-eeeeeeeeeeee"
SAMPLE_APP_ID = "ffffffff-0000-0000-0000-ffffffffffff"
SAMPLE_OPERATION_ID = "gggggggg-0000-0000-0000-gggggggggggg"


@pytest.fixture
def fabric_headers():
    return {"Authorization": "Bearer test-fabric-token", "Content-Type": "application/json"}


@pytest.fixture
def graph_headers():
    return {"Authorization": "Bearer test-graph-token", "Content-Type": "application/json"}


@pytest.fixture
def base_env_vars(monkeypatch):
    """Set all required environment variables."""
    monkeypatch.setenv("TENANT_ID", SAMPLE_TENANT_ID)
    monkeypatch.setenv("CLIENT_ID", SAMPLE_CLIENT_ID)
    monkeypatch.setenv("CLIENT_SECRET", SAMPLE_CLIENT_SECRET)
    monkeypatch.setenv("CAPACITY_ID", SAMPLE_CAPACITY_ID)
    monkeypatch.setenv("KEY_VAULT_URL", SAMPLE_KEY_VAULT_URL)
    monkeypatch.setenv("KV_SECRET_NAME_CLIENT_ID", "pipeline-client-id")
    monkeypatch.setenv("KV_SECRET_NAME_CLIENT_SECRET", "pipeline-client-secret")
