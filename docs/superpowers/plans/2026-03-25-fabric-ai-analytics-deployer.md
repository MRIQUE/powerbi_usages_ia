# Fabric AI Analytics Deployer — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI tool that provisions the full Microsoft Fabric pipeline infrastructure for ADENES's AI analytics project in a single idempotent execution.

**Architecture:** Modular Python package (`deploy/`) with one module per infrastructure domain, orchestrated by `__main__.py`. Each module checks for resource existence (idempotence) before creation, handles LRO polling for async Fabric operations, and uses exponential backoff for API throttling. Deployment state is persisted to `deploy-output.json` for step resume.

**Tech Stack:** Python 3.10+, pytest + unittest.mock, msal>=1.28, requests>=2.32, python-dotenv>=1.0

**Spec:** `docs/superpowers/specs/2026-03-25-fabric-ai-analytics-deploy-design.md`

---

## File Map

| File | Responsibility |
|---|---|
| `fabric-ai-analytics-deploy/deploy/__init__.py` | Package marker |
| `fabric-ai-analytics-deploy/deploy/__main__.py` | CLI: argument parsing, step orchestration, `--from-step N`, `--list-steps` |
| `fabric-ai-analytics-deploy/deploy/config.py` | `DeployConfig` dataclass + `load_config()` (.env → env vars → `ConfigError`) |
| `fabric-ai-analytics-deploy/deploy/logger.py` | `setup_logger()`: console INFO + file DEBUG in `logs/deploy_YYYYMMDD_HHMMSS.log` |
| `fabric-ai-analytics-deploy/deploy/lro.py` | `poll_lro()`: two-phase LRO polling for Fabric async ops |
| `fabric-ai-analytics-deploy/deploy/retry.py` | `@retryable` decorator: exponential backoff on 429/5xx, max 5 attempts |
| `fabric-ai-analytics-deploy/deploy/entra.py` | Step 1: App Registration + Service Principal + appRoleAssignments |
| `fabric-ai-analytics-deploy/deploy/fabric_workspace.py` | Step 2: Workspace creation + SPN Contributor role assignment |
| `fabric-ai-analytics-deploy/deploy/fabric_lakehouse.py` | Step 3: Lakehouse creation via LRO |
| `fabric-ai-analytics-deploy/deploy/fabric_notebooks.py` | Step 4: Notebook creation with Base64-encoded content via LRO |
| `fabric-ai-analytics-deploy/deploy/fabric_pipelines.py` | Steps 5–7: Pipeline, schedule (`DefaultJob`), on-demand run + table verification |
| `fabric-ai-analytics-deploy/deploy/output.py` | Read/write `deploy-output.json`; ID lookup for `--from-step` |
| `fabric-ai-analytics-deploy/deploy/notebook_content/__init__.py` | Package marker |
| `fabric-ai-analytics-deploy/deploy/notebook_content/nb_cloud_discovery.py` | Notebook source: Graph Cloud Discovery ingestion (Bronze tables) |
| `fabric-ai-analytics-deploy/deploy/notebook_content/nb_copilot_usage.py` | Notebook source: Graph Copilot Usage ingestion `/v1.0/` (Bronze tables) |
| `fabric-ai-analytics-deploy/tests/conftest.py` | Shared pytest fixtures: sample IDs, mock headers, mock HTTP responses |
| `fabric-ai-analytics-deploy/tests/test_config.py` | Tests for config.py |
| `fabric-ai-analytics-deploy/tests/test_logger.py` | Tests for logger.py |
| `fabric-ai-analytics-deploy/tests/test_lro.py` | Tests for lro.py |
| `fabric-ai-analytics-deploy/tests/test_retry.py` | Tests for retry.py |
| `fabric-ai-analytics-deploy/tests/test_entra.py` | Tests for entra.py |
| `fabric-ai-analytics-deploy/tests/test_fabric_workspace.py` | Tests for fabric_workspace.py |
| `fabric-ai-analytics-deploy/tests/test_fabric_lakehouse.py` | Tests for fabric_lakehouse.py |
| `fabric-ai-analytics-deploy/tests/test_fabric_notebooks.py` | Tests for fabric_notebooks.py |
| `fabric-ai-analytics-deploy/tests/test_fabric_pipelines.py` | Tests for fabric_pipelines.py |
| `fabric-ai-analytics-deploy/tests/test_output.py` | Tests for output.py |

> All paths below are relative to `C:\temp\powerbi_usages_ia\fabric-ai-analytics-deploy\`.

---

## Chunk 1: Project Scaffolding + Helpers (config, logger, lro, retry)

### Task 1: Project scaffolding

**Files:**
- Create: `deploy/__init__.py`
- Create: `deploy/notebook_content/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `.env.example`

- [ ] **Step 1.1: Create directory structure and init git**

```bash
cd C:\temp\powerbi_usages_ia
mkdir -p fabric-ai-analytics-deploy/deploy/notebook_content
mkdir -p fabric-ai-analytics-deploy/tests
mkdir -p fabric-ai-analytics-deploy/logs
cd fabric-ai-analytics-deploy
git init
```

- [ ] **Step 1.2: Create package markers, requirements files, and .gitignore**

`deploy/__init__.py` — empty file.

`deploy/notebook_content/__init__.py` — empty file.

`tests/__init__.py` — empty file.

`.gitignore`:
```
.env
logs/
deploy-output.json
__pycache__/
*.pyc
*.pyo
.venv/
venv/
*.egg-info/
dist/
build/
.vscode/
.idea/
```

`requirements.txt`:
```
msal>=1.28.0
requests>=2.32.0
python-dotenv>=1.0.0
```

`requirements-dev.txt`:
```
pytest>=8.0.0
pytest-mock>=3.12.0
```

`.env.example`:
```
TENANT_ID=your-tenant-id
CLIENT_ID=your-client-id
CLIENT_SECRET=your-client-secret
CAPACITY_ID=your-fabric-capacity-id
KEY_VAULT_URL=https://kv-adenes.vault.azure.net/
KV_SECRET_NAME_CLIENT_ID=fabric-pipeline-client-id
KV_SECRET_NAME_CLIENT_SECRET=fabric-pipeline-client-secret
# ADMIN_TOKEN is required only for step 1 (Entra provisioning)
# Obtain via: az account get-access-token --resource https://graph.microsoft.com --query accessToken -o tsv
ADMIN_TOKEN=
```

- [ ] **Step 1.3: Create conftest.py with shared fixtures**

`tests/conftest.py`:
```python
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
```

- [ ] **Step 1.4: Install dependencies**

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

Expected: all packages installed without errors.

- [ ] **Step 1.5: Verify pytest works**

```bash
pytest tests/ -v
```

Expected: `no tests ran` (0 collected).

- [ ] **Step 1.6: Commit scaffold**

```bash
git add fabric-ai-analytics-deploy/
git commit -m "chore: scaffold fabric-ai-analytics-deploy project structure"
```

---

### Task 2: config.py

**Files:**
- Create: `deploy/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 2.1: Write failing tests**

`tests/test_config.py`:
```python
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
```

- [ ] **Step 2.2: Run tests — verify they fail**

```bash
pytest tests/test_config.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `deploy.config` does not exist yet.

- [ ] **Step 2.3: Implement config.py**

`deploy/config.py`:
```python
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
```

- [ ] **Step 2.4: Run tests — verify they pass**

```bash
pytest tests/test_config.py -v
```

Expected: 5 tests PASSED.

- [ ] **Step 2.5: Commit**

```bash
git add deploy/config.py tests/test_config.py
git commit -m "feat: add config loading with env var validation"
```

---

### Task 3: logger.py

**Files:**
- Create: `deploy/logger.py`
- Create: `tests/test_logger.py`

- [ ] **Step 3.1: Write failing tests**

`tests/test_logger.py`:
```python
import logging
import os
import pytest
from pathlib import Path
from deploy.logger import setup_logger


def test_setup_logger_returns_logger(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    assert isinstance(logger, logging.Logger)


def test_setup_logger_creates_logs_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    setup_logger()
    assert (tmp_path / "logs").is_dir()


def test_setup_logger_creates_log_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    setup_logger()
    log_files = list((tmp_path / "logs").glob("deploy_*.log"))
    assert len(log_files) == 1


def test_logger_has_two_handlers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    assert len(logger.handlers) == 2


def test_logger_file_handler_is_debug(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = setup_logger()
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    assert file_handlers[0].level == logging.DEBUG
```

- [ ] **Step 3.2: Run tests — verify they fail**

```bash
pytest tests/test_logger.py -v
```

Expected: `ImportError` — `deploy.logger` does not exist yet.

- [ ] **Step 3.3: Implement logger.py**

`deploy/logger.py`:
```python
from __future__ import annotations
import logging
import os
from datetime import datetime
from pathlib import Path

_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logger(name: str = "deploy") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(logging.DEBUG)

    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    logger.addHandler(console)

    # File handler — DEBUG and above
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"deploy_{timestamp}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    logger.addHandler(file_handler)

    return logger
```

- [ ] **Step 3.4: Run tests — verify they pass**

```bash
pytest tests/test_logger.py -v
```

Expected: 5 tests PASSED.

- [ ] **Step 3.5: Commit**

```bash
git add deploy/logger.py tests/test_logger.py
git commit -m "feat: add dual-output logger (console INFO + file DEBUG)"
```

---

### Task 4: lro.py

**Files:**
- Create: `deploy/lro.py`
- Create: `tests/test_lro.py`

- [ ] **Step 4.1: Write failing tests**

`tests/test_lro.py`:
```python
import pytest
from unittest.mock import patch, Mock
from deploy.lro import poll_lro, FabricLROError

OPERATION_URL = "https://api.fabric.microsoft.com/v1/operations/op-123"
HEADERS = {"Authorization": "Bearer token"}


def _make_response(status_code, json_body):
    m = Mock()
    m.status_code = status_code
    m.json.return_value = json_body
    m.headers = {}
    return m


def test_poll_lro_success_returns_resource(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})
    phase1_succeeded = _make_response(200, {"status": "Succeeded"})
    phase2_result = _make_response(200, {"id": "lakehouse-123", "displayName": "lkh_ai_analytics"})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"):
        mock_get.side_effect = [phase1_running, phase1_succeeded, phase2_result]
        result = poll_lro(OPERATION_URL, fabric_headers, timeout_s=120, poll_interval=1)

    assert result == {"id": "lakehouse-123", "displayName": "lkh_ai_analytics"}
    assert mock_get.call_count == 3
    # Phase 2 must call /result
    last_call_url = mock_get.call_args_list[2][0][0]
    assert last_call_url == OPERATION_URL + "/result"


def test_poll_lro_failed_raises(fabric_headers):
    phase1_failed = _make_response(200, {"status": "Failed", "error": {"message": "Quota exceeded"}})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"):
        mock_get.return_value = phase1_failed
        with pytest.raises(FabricLROError) as exc_info:
            poll_lro(OPERATION_URL, fabric_headers, timeout_s=60, poll_interval=1)

    assert "Quota exceeded" in str(exc_info.value)


def test_poll_lro_timeout_raises(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})

    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep"), \
         patch("deploy.lro.time.monotonic") as mock_time:
        mock_get.return_value = phase1_running
        mock_time.side_effect = [0, 0, 400]  # start, first check, timeout exceeded
        with pytest.raises(TimeoutError):
            poll_lro(OPERATION_URL, fabric_headers, timeout_s=300, poll_interval=1)


def test_poll_lro_uses_retry_after_header(fabric_headers):
    phase1_running = _make_response(200, {"status": "Running"})
    phase1_running.headers = {"Retry-After": "45"}
    phase1_succeeded = _make_response(200, {"status": "Succeeded"})
    phase2_result = _make_response(200, {"id": "res-1"})

    sleep_calls = []
    with patch("deploy.lro.requests.get") as mock_get, \
         patch("deploy.lro.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        mock_get.side_effect = [phase1_running, phase1_succeeded, phase2_result]
        poll_lro(OPERATION_URL, fabric_headers, timeout_s=300, poll_interval=30)

    assert 45 in sleep_calls
```

- [ ] **Step 4.2: Run tests — verify they fail**

```bash
pytest tests/test_lro.py -v
```

Expected: `ImportError` — `deploy.lro` does not exist yet.

- [ ] **Step 4.3: Implement lro.py**

`deploy/lro.py`:
```python
from __future__ import annotations
import time
import requests


class FabricLROError(Exception):
    pass


def poll_lro(
    location_url: str,
    headers: dict,
    timeout_s: int = 300,
    poll_interval: int = 30,
) -> dict:
    """Poll a Fabric Long Running Operation until completion.

    Phase 1: GET location_url until status is Succeeded or Failed.
    Phase 2: GET location_url/result to retrieve the created resource.

    Returns the resource dict from phase 2.
    Raises FabricLROError on failure, TimeoutError on timeout.
    """
    start = time.monotonic()

    while True:
        if time.monotonic() - start > timeout_s:
            raise TimeoutError(
                f"LRO timed out after {timeout_s}s: {location_url}"
            )

        resp = requests.get(location_url, headers=headers)
        resp.raise_for_status()
        body = resp.json()
        status = body.get("status", "Unknown")

        if status == "Succeeded":
            break
        if status == "Failed":
            error_msg = body.get("error", {}).get("message", "Unknown error")
            raise FabricLROError(f"LRO failed: {error_msg}")

        # Still running — respect Retry-After if present, else use poll_interval
        delay = int(resp.headers.get("Retry-After", poll_interval))
        time.sleep(delay)

    # Phase 2: retrieve the created resource
    result_resp = requests.get(f"{location_url}/result", headers=headers)
    result_resp.raise_for_status()
    return result_resp.json()
```

- [ ] **Step 4.4: Run tests — verify they pass**

```bash
pytest tests/test_lro.py -v
```

Expected: 4 tests PASSED.

- [ ] **Step 4.5: Commit**

```bash
git add deploy/lro.py tests/test_lro.py
git commit -m "feat: add two-phase LRO polling helper for Fabric async operations"
```

---

### Task 5: retry.py

**Files:**
- Create: `deploy/retry.py`
- Create: `tests/test_retry.py`

- [ ] **Step 5.1: Write failing tests**

`tests/test_retry.py`:
```python
import pytest
import requests
from unittest.mock import Mock, patch
from deploy.retry import retryable


def _mock_response(status_code, retry_after=None):
    m = Mock(spec=requests.Response)
    m.status_code = status_code
    m.headers = {"Retry-After": str(retry_after)} if retry_after else {}
    m.raise_for_status = Mock()
    if status_code >= 400:
        m.raise_for_status.side_effect = requests.HTTPError(response=m)
    return m


def test_retryable_passes_through_on_success():
    success_resp = _mock_response(200)
    func = Mock(return_value=success_resp)
    wrapped = retryable(func)
    result = wrapped("arg1", key="val")
    assert result == success_resp
    func.assert_called_once_with("arg1", key="val")


def test_retryable_retries_on_429():
    resp_429 = _mock_response(429, retry_after=1)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_429, resp_200])
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        result = wrapped()

    assert result == resp_200
    assert func.call_count == 2


def test_retryable_retries_on_500():
    resp_500 = _mock_response(500)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_500, resp_200])
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        result = wrapped()

    assert result == resp_200


def test_retryable_raises_after_max_attempts():
    resp_429 = _mock_response(429)
    func = Mock(return_value=resp_429)
    wrapped = retryable(func, max_attempts=3)

    with patch("deploy.retry.time.sleep"):
        with pytest.raises(requests.HTTPError):
            wrapped()

    assert func.call_count == 3


def test_retryable_does_not_retry_400():
    resp_400 = _mock_response(400)
    func = Mock(return_value=resp_400)
    wrapped = retryable(func, max_attempts=5)

    with patch("deploy.retry.time.sleep"):
        with pytest.raises(requests.HTTPError):
            wrapped()

    assert func.call_count == 1


def test_retryable_uses_retry_after_header():
    resp_429 = _mock_response(429, retry_after=42)
    resp_200 = _mock_response(200)
    func = Mock(side_effect=[resp_429, resp_200])
    wrapped = retryable(func, max_attempts=3)

    sleep_calls = []
    with patch("deploy.retry.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        wrapped()

    assert sleep_calls[0] == 42
```

- [ ] **Step 5.2: Run tests — verify they fail**

```bash
pytest tests/test_retry.py -v
```

Expected: `ImportError` — `deploy.retry` does not exist yet.

- [ ] **Step 5.3: Implement retry.py**

`deploy/retry.py`:
```python
from __future__ import annotations
import functools
import time
import requests

_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
_DEFAULT_DELAY = 30


def retryable(func=None, *, max_attempts: int = 5):
    """Wrap an HTTP-calling function with exponential backoff retry.

    Retries on 429 and 5xx responses. Respects Retry-After header.
    Raises on 4xx (except 429) immediately without retry.
    """
    if func is None:
        return functools.partial(retryable, max_attempts=max_attempts)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        delay = _DEFAULT_DELAY
        last_response = None
        for attempt in range(1, max_attempts + 1):
            response = func(*args, **kwargs)
            if response.status_code not in _RETRYABLE_STATUSES:
                response.raise_for_status()
                return response
            last_response = response
            if attempt == max_attempts:
                break
            wait = int(response.headers.get("Retry-After", delay))
            time.sleep(wait)
            delay = min(delay * 2, 300)
        last_response.raise_for_status()

    return wrapper
```

- [ ] **Step 5.4: Run tests — verify they pass**

```bash
pytest tests/test_retry.py -v
```

Expected: 6 tests PASSED.

- [ ] **Step 5.5: Run all tests**

```bash
pytest tests/ -v
```

Expected: 20 tests PASSED (config: 5, logger: 5, lro: 4, retry: 6).

- [ ] **Step 5.6: Commit**

```bash
git add deploy/retry.py tests/test_retry.py
git commit -m "feat: add retryable decorator with exponential backoff for 429/5xx"
```

---

## Chunk 2: Step 1 — Entra App Registration (`entra.py`)

### Task 6: entra.py

**Files:**
- Create: `deploy/entra.py`
- Create: `tests/test_entra.py`

**What this module does:**
1. Check if App Registration with `displayName` already exists → skip if yes
2. Create App Registration with required resource access declarations
3. Create Service Principal for the app
4. Add Client Secret
5. Resolve Microsoft Graph SPN object ID (for appRoleAssignments)
6. Assign `CloudApp-Discovery.Read.All` and `Reports.Read.All` application roles
7. Return `EntraResult(app_id, object_id, spn_object_id, client_secret)`

- [ ] **Step 6.1: Write failing tests**

`tests/test_entra.py`:
```python
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
```

- [ ] **Step 6.2: Run tests — verify they fail**

```bash
pytest tests/test_entra.py -v
```

Expected: `ImportError` — `deploy.entra` does not exist.

- [ ] **Step 6.3: Implement entra.py**

`deploy/entra.py`:
```python
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
```

- [ ] **Step 6.4: Run tests — verify they pass**

```bash
pytest tests/test_entra.py -v
```

Expected: 3 tests PASSED.

- [ ] **Step 6.5: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASSED.

- [ ] **Step 6.6: Commit**

```bash
git add deploy/entra.py tests/test_entra.py
git commit -m "feat: add Entra App Registration provisioning with idempotence"
```

---

## Chunk 3: Steps 2–3 — Workspace + Lakehouse

### Task 7: fabric_workspace.py

**Files:**
- Create: `deploy/fabric_workspace.py`
- Create: `tests/test_fabric_workspace.py`

**What this module does:**
1. List workspaces, check if `displayName` exists → skip + return existing ID
2. Create workspace with `capacityId`
3. Assign SPN as Contributor via `roleAssignments`
4. Return `WorkspaceResult(workspace_id, status)`

- [ ] **Step 7.1: Write failing tests**

`tests/test_fabric_workspace.py`:
```python
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
```

- [ ] **Step 7.2: Run tests — verify they fail**

```bash
pytest tests/test_fabric_workspace.py -v
```

Expected: `ImportError`.

- [ ] **Step 7.3: Implement fabric_workspace.py**

`deploy/fabric_workspace.py`:
```python
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
```

- [ ] **Step 7.4: Run tests — verify they pass**

```bash
pytest tests/test_fabric_workspace.py -v
```

Expected: 3 tests PASSED.

- [ ] **Step 7.5: Commit**

```bash
git add deploy/fabric_workspace.py tests/test_fabric_workspace.py
git commit -m "feat: add Fabric workspace provisioning with SPN role assignment"
```

---

### Task 8: fabric_lakehouse.py

**Files:**
- Create: `deploy/fabric_lakehouse.py`
- Create: `tests/test_fabric_lakehouse.py`

**What this module does:**
1. List lakehouses in workspace, check `displayName` → skip if exists
2. POST create lakehouse (with `enableSchemas: true`)
3. Handle 202 LRO via `poll_lro()`
4. Return `LakehouseResult(lakehouse_id, status)`

- [ ] **Step 8.1: Write failing tests**

`tests/test_fabric_lakehouse.py`:
```python
import pytest
from unittest.mock import patch, Mock
from deploy.fabric_lakehouse import provision_lakehouse, LakehouseResult

WS_ID = "ws-guid-222"
LKH_ID = "lkh-guid-333"
LKH_NAME = "lkh_ai_analytics"
OP_URL = "https://api.fabric.microsoft.com/v1/operations/op-444"


def _ok(json_body):
    m = Mock()
    m.status_code = 200
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def _accepted(location_url):
    m = Mock()
    m.status_code = 202
    m.headers = {"Location": location_url}
    m.raise_for_status = Mock()
    return m


def _created(json_body):
    m = Mock()
    m.status_code = 201
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def test_provision_lakehouse_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.return_value = {"id": LKH_ID}

        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    assert result.lakehouse_id == LKH_ID
    assert result.status == "created"
    mock_lro.assert_called_once_with(OP_URL, fabric_headers)


def test_provision_lakehouse_handles_201_sync(fabric_headers):
    """Fabric may return 201 directly (no LRO)."""
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": LKH_ID})

        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    mock_lro.assert_not_called()
    assert result.lakehouse_id == LKH_ID
    assert result.status == "created"


def test_provision_lakehouse_skips_when_exists(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": LKH_ID, "displayName": LKH_NAME}]})
        result = provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"
    assert result.lakehouse_id == LKH_ID


def test_provision_lakehouse_enables_schemas(fabric_headers):
    with patch("deploy.fabric_lakehouse.requests.get") as mock_get, \
         patch("deploy.fabric_lakehouse.requests.post") as mock_post, \
         patch("deploy.fabric_lakehouse.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.return_value = {"id": LKH_ID}
        provision_lakehouse(LKH_NAME, WS_ID, fabric_headers)

    call_body = mock_post.call_args[1]["json"]
    assert call_body["creationPayload"]["enableSchemas"] is True
```

- [ ] **Step 8.2: Run tests — verify they fail**

```bash
pytest tests/test_fabric_lakehouse.py -v
```

Expected: `ImportError`.

- [ ] **Step 8.3: Implement fabric_lakehouse.py**

`deploy/fabric_lakehouse.py`:
```python
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
```

- [ ] **Step 8.4: Run tests — verify they pass**

```bash
pytest tests/test_fabric_lakehouse.py -v
```

Expected: 4 tests PASSED.

- [ ] **Step 8.5: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASSED.

- [ ] **Step 8.6: Commit**

```bash
git add deploy/fabric_lakehouse.py tests/test_fabric_lakehouse.py
git commit -m "feat: add Fabric lakehouse provisioning with two-phase LRO support"
```

---

## Chunk 4: Notebook Content (Cloud Discovery + Copilot Usage)

### Task 9: nb_cloud_discovery.py

**Files:**
- Create: `deploy/notebook_content/nb_cloud_discovery.py`
- Create: `tests/test_notebook_content.py`

**What this notebook does (runs inside Fabric Spark):**
1. Retrieve `CLIENT_ID` and `CLIENT_SECRET` from Key Vault via `mssparkutils`
2. Authenticate with MSAL (`client_credentials`)
3. Fetch stream ID from Cloud Discovery uploadedStreams
4. Fetch aggregated apps (`P90D`) filtered on Generative AI / AI Model Provider categories
5. Paginate `/users` for each app
6. Write `bronze.cloud_discovery_apps` and `bronze.cloud_discovery_users` Delta tables (append + dedup)
7. Log execution summary

- [ ] **Step 9.1: Write tests for notebook content validation**

`tests/test_notebook_content.py`:
```python
"""
Validate notebook content files: syntax correctness, required function presence,
and correct API endpoint usage. Notebooks run in Fabric Spark; these tests
validate the source code without executing it.
"""
import ast
import importlib.util
from pathlib import Path


def _load_source(filename):
    path = Path("deploy/notebook_content") / filename
    return path.read_text(encoding="utf-8")


def test_nb_cloud_discovery_valid_python_syntax():
    source = _load_source("nb_cloud_discovery.py")
    ast.parse(source)  # raises SyntaxError if invalid


def test_nb_cloud_discovery_uses_correct_api_endpoint():
    source = _load_source("nb_cloud_discovery.py")
    assert "dataDiscovery/cloudAppDiscovery/uploadedStreams" in source
    assert "aggregatedAppsDetails" in source
    assert "P90D" in source or "P30D" in source


def test_nb_cloud_discovery_uses_keyvault():
    source = _load_source("nb_cloud_discovery.py")
    assert "mssparkutils" in source
    assert "getSecret" in source
    assert "KV_SECRET_NAME_CLIENT_ID" in source
    assert "KV_SECRET_NAME_CLIENT_SECRET" in source


def test_nb_cloud_discovery_writes_bronze_tables():
    source = _load_source("nb_cloud_discovery.py")
    assert "bronze.cloud_discovery_apps" in source
    assert "bronze.cloud_discovery_users" in source


def test_nb_copilot_usage_valid_python_syntax():
    source = _load_source("nb_copilot_usage.py")
    ast.parse(source)


def test_nb_copilot_usage_uses_v1_endpoint():
    source = _load_source("nb_copilot_usage.py")
    assert "/v1.0/copilot/reports/" in source
    # Must NOT use the deprecated /beta/reports/ path
    assert "/beta/reports/" not in source


def test_nb_copilot_usage_uses_keyvault():
    source = _load_source("nb_copilot_usage.py")
    assert "mssparkutils" in source
    assert "getSecret" in source


def test_nb_copilot_usage_writes_bronze_tables():
    source = _load_source("nb_copilot_usage.py")
    assert "bronze.copilot_usage_detail" in source
    assert "bronze.copilot_usage_trend" in source


def test_nb_copilot_usage_handles_nested_adoption_by_date():
    source = _load_source("nb_copilot_usage.py")
    assert "adoptionByDate" in source
    assert "explode" in source
```

- [ ] **Step 9.2: Run tests — verify they fail**

```bash
pytest tests/test_notebook_content.py -v
```

Expected: `FileNotFoundError` — notebook files do not exist yet.

- [ ] **Step 9.3: Write nb_cloud_discovery.py**

`deploy/notebook_content/nb_cloud_discovery.py`:
```python
# Notebook: Ingestion Cloud Discovery via Microsoft Graph Beta API
# Exécuté quotidiennement par le pipeline pip_daily_ai_ingestion
# Prérequis : Key Vault référencé dans le Lakehouse, permission CloudApp-Discovery.Read.All

import time
import requests
import msal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, FloatType
)

# ---------------------------------------------------------------------------
# 1. Configuration (secrets depuis Key Vault via mssparkutils)
# ---------------------------------------------------------------------------
TENANT_ID = mssparkutils.env.getTenantId()
KEY_VAULT_URL = mssparkutils.env.getJobTags().get("keyVaultUrl", "")
KV_SECRET_NAME_CLIENT_ID = "fabric-pipeline-client-id"
KV_SECRET_NAME_CLIENT_SECRET = "fabric-pipeline-client-secret"

CLIENT_ID = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_ID)
CLIENT_SECRET = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_SECRET)

GRAPH_BASE = "https://graph.microsoft.com"
DISCOVERY_BASE = f"{GRAPH_BASE}/beta/security/dataDiscovery/cloudAppDiscovery"
AI_CATEGORIES_FILTER = "category eq 'Generative AI' or category eq 'AI Model Provider'"
PERIOD = "P90D"

# ---------------------------------------------------------------------------
# 2. Authentification MSAL avec renouvellement automatique
# ---------------------------------------------------------------------------
_msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)
_token_cache = {"token": None, "expires_at": 0}


def get_headers():
    if time.time() > _token_cache["expires_at"] - 300:
        result = _msal_app.acquire_token_for_client(scopes=[f"{GRAPH_BASE}/.default"])
        _token_cache["token"] = result["access_token"]
        _token_cache["expires_at"] = time.time() + result.get("expires_in", 3600)
    return {"Authorization": f"Bearer {_token_cache['token']}"}


# ---------------------------------------------------------------------------
# 3. Retry avec backoff exponentiel
# ---------------------------------------------------------------------------
_RETRYABLE = {429, 500, 502, 503, 504}


def get_with_retry(url, max_attempts=5, base_delay=30):
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=get_headers())
        if resp.status_code not in _RETRYABLE:
            resp.raise_for_status()
            return resp
        if attempt == max_attempts:
            resp.raise_for_status()
        wait = int(resp.headers.get("Retry-After", delay))
        print(f"[RETRY] {resp.status_code} — attente {wait}s (tentative {attempt}/{max_attempts})")
        time.sleep(wait)
        delay = min(delay * 2, 300)


# ---------------------------------------------------------------------------
# 4. Pagination automatique
# ---------------------------------------------------------------------------
def paginate(url):
    results = []
    while url:
        resp = get_with_retry(url).json()
        results.extend(resp.get("value", []))
        url = resp.get("@odata.nextLink")
    return results


# ---------------------------------------------------------------------------
# 5. Récupération du streamId MDE
# ---------------------------------------------------------------------------
t_start = time.time()
streams = get_with_retry(f"{DISCOVERY_BASE}/uploadedStreams").json()
stream_id = next(s["id"] for s in streams["value"])
print(f"[INFO] Stream MDE : {stream_id}")

# ---------------------------------------------------------------------------
# 6. Apps découvertes (filtrage sur catégories IA)
# ---------------------------------------------------------------------------
apps_url = (
    f"{DISCOVERY_BASE}/uploadedStreams/{stream_id}/"
    f"microsoft.graph.security.aggregatedAppsDetails(period=duration'{PERIOD}')"
    f"?$filter={AI_CATEGORIES_FILTER}"
    f"&$select=displayName,category,riskScore,userCount,transactionCount,"
    f"uploadNetworkTrafficInBytes,downloadNetworkTrafficInBytes,lastSeenDateTime,deviceCount"
)
apps = paginate(apps_url)
print(f"[INFO] Apps découvertes : {len(apps)}")

# ---------------------------------------------------------------------------
# 7. Utilisateurs par app (pagination)
# ---------------------------------------------------------------------------
users_rows = []
for app in apps:
    app_id = app.get("id", app.get("appId", ""))
    users_url = (
        f"{DISCOVERY_BASE}/uploadedStreams/{stream_id}/"
        f"microsoft.graph.security.aggregatedAppsDetails(period=duration'{PERIOD}')/{app_id}/users"
    )
    try:
        users = paginate(users_url)
        for u in users:
            users_rows.append({
                "appId": app_id,
                "userIdentifier": u.get("userIdentifier", ""),
                "transactionCount": u.get("transactionCount", 0),
                "uploadBytes": u.get("uploadNetworkTrafficInBytes", 0),
                "downloadBytes": u.get("downloadNetworkTrafficInBytes", 0),
                "reportDate": app.get("lastSeenDateTime", "")[:10],
            })
    except Exception as e:
        print(f"[WARN] Erreur users pour app {app_id}: {e}")

# ---------------------------------------------------------------------------
# 8. Écriture Delta — bronze.cloud_discovery_apps
# ---------------------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

apps_schema = StructType([
    StructField("appId", StringType()),
    StructField("displayName", StringType()),
    StructField("category", StringType()),
    StructField("riskScore", FloatType()),
    StructField("userCount", IntegerType()),
    StructField("transactionCount", LongType()),
    StructField("uploadNetworkTrafficInBytes", LongType()),
    StructField("downloadNetworkTrafficInBytes", LongType()),
    StructField("lastSeenDateTime", StringType()),
    StructField("deviceCount", IntegerType()),
])

apps_rows = [{
    "appId": a.get("id", a.get("appId", "")),
    "displayName": a.get("displayName", ""),
    "category": a.get("category", ""),
    "riskScore": float(a.get("riskScore", 0) or 0),
    "userCount": int(a.get("userCount", 0) or 0),
    "transactionCount": int(a.get("transactionCount", 0) or 0),
    "uploadNetworkTrafficInBytes": int(a.get("uploadNetworkTrafficInBytes", 0) or 0),
    "downloadNetworkTrafficInBytes": int(a.get("downloadNetworkTrafficInBytes", 0) or 0),
    "lastSeenDateTime": str(a.get("lastSeenDateTime", "") or ""),
    "deviceCount": int(a.get("deviceCount", 0) or 0),
} for a in apps]

df_apps = spark.createDataFrame(apps_rows, schema=apps_schema)
df_apps.dropDuplicates(["appId", "lastSeenDateTime"]) \
       .write.mode("append").format("delta").saveAsTable("bronze.cloud_discovery_apps")
print(f"[INFO] bronze.cloud_discovery_apps : {df_apps.count()} lignes écrites")

# ---------------------------------------------------------------------------
# 9. Écriture Delta — bronze.cloud_discovery_users
# ---------------------------------------------------------------------------
users_schema = StructType([
    StructField("appId", StringType()),
    StructField("userIdentifier", StringType()),
    StructField("transactionCount", LongType()),
    StructField("uploadBytes", LongType()),
    StructField("downloadBytes", LongType()),
    StructField("reportDate", StringType()),
])

df_users = spark.createDataFrame(users_rows, schema=users_schema)
df_users.dropDuplicates(["appId", "userIdentifier", "reportDate"]) \
        .write.mode("append").format("delta").saveAsTable("bronze.cloud_discovery_users")
print(f"[INFO] bronze.cloud_discovery_users : {df_users.count()} lignes écrites")

print(f"[INFO] Durée totale : {time.time() - t_start:.1f}s")
```

- [ ] **Step 9.4: Write nb_copilot_usage.py**

`deploy/notebook_content/nb_copilot_usage.py`:
```python
# Notebook: Ingestion Copilot Usage via Microsoft Graph Reports API v1.0
# Exécuté quotidiennement par le pipeline pip_daily_ai_ingestion
# Prérequis : Key Vault référencé dans le Lakehouse, permission Reports.Read.All
# Note: utilise /v1.0/copilot/reports/ (le chemin /beta/reports/ est déprécié)

import time
import requests
import msal
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# ---------------------------------------------------------------------------
# 1. Configuration (secrets depuis Key Vault via mssparkutils)
# ---------------------------------------------------------------------------
TENANT_ID = mssparkutils.env.getTenantId()
KEY_VAULT_URL = mssparkutils.env.getJobTags().get("keyVaultUrl", "")
KV_SECRET_NAME_CLIENT_ID = "fabric-pipeline-client-id"
KV_SECRET_NAME_CLIENT_SECRET = "fabric-pipeline-client-secret"

CLIENT_ID = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_ID)
CLIENT_SECRET = mssparkutils.credentials.getSecret(KEY_VAULT_URL, KV_SECRET_NAME_CLIENT_SECRET)

GRAPH_BASE = "https://graph.microsoft.com"
COPILOT_BASE = f"{GRAPH_BASE}/v1.0/copilot/reports"
PERIOD = "D30"

# ---------------------------------------------------------------------------
# 2. Authentification MSAL avec renouvellement
# ---------------------------------------------------------------------------
_msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)
_token_cache = {"token": None, "expires_at": 0}


def get_headers():
    if time.time() > _token_cache["expires_at"] - 300:
        result = _msal_app.acquire_token_for_client(scopes=[f"{GRAPH_BASE}/.default"])
        _token_cache["token"] = result["access_token"]
        _token_cache["expires_at"] = time.time() + result.get("expires_in", 3600)
    return {"Authorization": f"Bearer {_token_cache['token']}"}


# ---------------------------------------------------------------------------
# 3. Retry avec backoff exponentiel
# ---------------------------------------------------------------------------
_RETRYABLE = {429, 500, 502, 503, 504}


def get_with_retry(url, max_attempts=5, base_delay=30):
    delay = base_delay
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=get_headers())
        if resp.status_code not in _RETRYABLE:
            resp.raise_for_status()
            return resp
        if attempt == max_attempts:
            resp.raise_for_status()
        wait = int(resp.headers.get("Retry-After", delay))
        print(f"[RETRY] {resp.status_code} — attente {wait}s (tentative {attempt}/{max_attempts})")
        time.sleep(wait)
        delay = min(delay * 2, 300)


t_start = time.time()
spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 4. Copilot Usage User Detail
# ---------------------------------------------------------------------------
user_detail_url = (
    f"{COPILOT_BASE}/getMicrosoft365CopilotUsageUserDetail(period='{PERIOD}')"
    "?$format=application/json"
)
user_detail_data = get_with_retry(user_detail_url).json()
user_rows = user_detail_data.get("value", [])
print(f"[INFO] Copilot Usage User Detail : {len(user_rows)} enregistrements")

user_detail_schema = StructType([
    StructField("reportRefreshDate", StringType()),
    StructField("userPrincipalName", StringType()),
    StructField("displayName", StringType()),
    StructField("lastActivityDate", StringType()),
    StructField("copilotChatLastActivityDate", StringType()),
    StructField("microsoftTeamsCopilotLastActivityDate", StringType()),
    StructField("wordCopilotLastActivityDate", StringType()),
    StructField("excelCopilotLastActivityDate", StringType()),
    StructField("powerPointCopilotLastActivityDate", StringType()),
    StructField("outlookCopilotLastActivityDate", StringType()),
    StructField("oneNoteCopilotLastActivityDate", StringType()),
    StructField("loopCopilotLastActivityDate", StringType()),
])

df_detail = spark.createDataFrame(
    [{k: str(r.get(k, "") or "") for k in user_detail_schema.fieldNames()} for r in user_rows],
    schema=user_detail_schema,
)
df_detail.dropDuplicates(["userPrincipalName", "reportRefreshDate"]) \
         .write.mode("append").format("delta").saveAsTable("bronze.copilot_usage_detail")
print(f"[INFO] bronze.copilot_usage_detail : {df_detail.count()} lignes écrites")

# ---------------------------------------------------------------------------
# 5. Copilot User Count Trend (avec explosion de adoptionByDate)
# ---------------------------------------------------------------------------
trend_url = (
    f"{COPILOT_BASE}/getMicrosoft365CopilotUserCountTrend(period='{PERIOD}')"
    "?$format=application/json"
)
trend_data = get_with_retry(trend_url).json()
trend_rows = trend_data.get("value", [])
print(f"[INFO] Copilot User Count Trend : {len(trend_rows)} apps")

df_trend_raw = spark.createDataFrame(trend_rows)
df_trend = (
    df_trend_raw
    .select(explode("adoptionByDate").alias("day"))
    .select("day.*")
)
df_trend.dropDuplicates(["reportDate"]) \
        .write.mode("append").format("delta").saveAsTable("bronze.copilot_usage_trend")
print(f"[INFO] bronze.copilot_usage_trend : {df_trend.count()} lignes écrites")

print(f"[INFO] Durée totale : {time.time() - t_start:.1f}s")
```

- [ ] **Step 9.5: Run tests — verify they pass**

```bash
pytest tests/test_notebook_content.py -v
```

Expected: 9 tests PASSED.

- [ ] **Step 9.6: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASSED.

- [ ] **Step 9.7: Commit**

```bash
git add deploy/notebook_content/nb_cloud_discovery.py \
        deploy/notebook_content/nb_copilot_usage.py \
        tests/test_notebook_content.py
git commit -m "feat: add production-ready notebook content for Cloud Discovery and Copilot Usage ingestion"
```

---

## Chunk 5: Steps 4–7 — Notebooks + Pipeline + Schedule + Test Run

### Task 10: fabric_notebooks.py

**Files:**
- Create: `deploy/fabric_notebooks.py`
- Create: `tests/test_fabric_notebooks.py`

**What this module does:**
1. For each notebook (Cloud Discovery, Copilot Usage):
   a. Check if notebook with `displayName` exists → skip
   b. Read notebook source from `notebook_content/`
   c. Encode content + `.platform` metadata as Base64
   d. POST to Fabric notebooks API
   e. Handle 202 LRO via `poll_lro()`
2. Return `NotebooksResult(ids: dict[str, str], status: str)`

- [ ] **Step 10.1: Write failing tests**

`tests/test_fabric_notebooks.py`:
```python
import pytest
import base64
from unittest.mock import patch, Mock
from deploy.fabric_notebooks import provision_notebooks, NotebooksResult

WS_ID = "ws-guid-222"
NB_CD_ID = "nb-cd-guid-001"
NB_CU_ID = "nb-cu-guid-002"
OP_URL = "https://api.fabric.microsoft.com/v1/operations/op-nb"


def _ok(json_body):
    m = Mock()
    m.status_code = 200
    m.json.return_value = json_body
    m.headers = {}
    m.raise_for_status = Mock()
    return m


def _accepted(loc):
    m = Mock()
    m.status_code = 202
    m.headers = {"Location": loc}
    m.raise_for_status = Mock()
    return m


def test_provision_notebooks_creates_both_when_not_exist(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.side_effect = [{"id": NB_CD_ID}, {"id": NB_CU_ID}]

        result = provision_notebooks(WS_ID, fabric_headers)

    assert result.ids["nb_cloud_discovery"] == NB_CD_ID
    assert result.ids["nb_copilot_usage"] == NB_CU_ID
    assert result.status == "created"
    assert mock_post.call_count == 2


def test_provision_notebooks_skips_existing(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": [
            {"id": NB_CD_ID, "displayName": "nb_ingest_cloud_discovery"},
            {"id": NB_CU_ID, "displayName": "nb_ingest_copilot_usage"},
        ]})
        result = provision_notebooks(WS_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"
    assert result.ids["nb_cloud_discovery"] == NB_CD_ID


def test_provision_notebooks_encodes_content_as_base64(fabric_headers):
    with patch("deploy.fabric_notebooks.requests.get") as mock_get, \
         patch("deploy.fabric_notebooks.requests.post") as mock_post, \
         patch("deploy.fabric_notebooks.poll_lro") as mock_lro:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _accepted(OP_URL)
        mock_lro.side_effect = [{"id": NB_CD_ID}, {"id": NB_CU_ID}]

        provision_notebooks(WS_ID, fabric_headers)

    first_call_body = mock_post.call_args_list[0][1]["json"]
    parts = first_call_body["definition"]["parts"]
    content_part = next(p for p in parts if p["path"] == "notebook-content.py")
    # Verify it's valid base64
    decoded = base64.b64decode(content_part["payload"]).decode("utf-8")
    assert len(decoded) > 0
```

- [ ] **Step 10.2: Run tests — verify they fail**

```bash
pytest tests/test_fabric_notebooks.py -v
```

Expected: `ImportError`.

- [ ] **Step 10.3: Implement fabric_notebooks.py**

`deploy/fabric_notebooks.py`:
```python
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
```

- [ ] **Step 10.4: Run tests — verify they pass**

```bash
pytest tests/test_fabric_notebooks.py -v
```

Expected: 3 tests PASSED.

- [ ] **Step 10.5: Commit**

```bash
git add deploy/fabric_notebooks.py tests/test_fabric_notebooks.py
git commit -m "feat: add Fabric notebook provisioning with Base64-encoded content"
```

---

### Task 11: fabric_pipelines.py

**Files:**
- Create: `deploy/fabric_pipelines.py`
- Create: `tests/test_fabric_pipelines.py`

**What this module does:**
1. `provision_pipeline()` — create/reuse Data Pipeline with 2 Notebook Activities
2. `provision_schedule()` — create/reuse daily schedule (`DefaultJob`, 6h00, Romance Standard Time)
3. `run_and_monitor()` — trigger on-demand run, poll to completion, verify Bronze tables exist

- [ ] **Step 11.1: Write failing tests**

`tests/test_fabric_pipelines.py`:
```python
import pytest
from unittest.mock import patch, Mock
from deploy.fabric_pipelines import (
    provision_pipeline, provision_schedule, run_and_monitor,
    PipelineResult, ScheduleResult, RunResult,
)

WS_ID = "ws-guid-222"
NB_CD_ID = "nb-cd-001"
NB_CU_ID = "nb-cu-002"
PIP_ID = "pip-guid-444"
JOB_INSTANCE_ID = "job-inst-555"
LKH_ID = "lkh-guid-333"


def _ok(json_body):
    m = Mock(); m.status_code = 200; m.json.return_value = json_body
    m.headers = {}; m.raise_for_status = Mock(); return m


def _created(json_body):
    m = Mock(); m.status_code = 201; m.json.return_value = json_body
    m.headers = {}; m.raise_for_status = Mock(); return m


def _accepted(loc):
    m = Mock(); m.status_code = 202
    m.headers = {"Location": loc}; m.raise_for_status = Mock(); return m


# --- provision_pipeline ---

def test_provision_pipeline_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [
            _created({"id": PIP_ID}),  # create pipeline
            _ok({}),                   # update definition
        ]
        result = provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    assert result.pipeline_id == PIP_ID
    assert result.status == "created"


def test_provision_pipeline_skips_when_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": PIP_ID, "displayName": "pip_daily_ai_ingestion"}]})
        result = provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"


def test_provision_pipeline_includes_both_notebooks(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.side_effect = [_created({"id": PIP_ID}), _ok({})]
        provision_pipeline(WS_ID, NB_CD_ID, NB_CU_ID, fabric_headers)

    update_call = mock_post.call_args_list[1]
    import base64, json
    parts = update_call[1]["json"]["definition"]["parts"]
    content_b64 = next(p for p in parts if p["path"] == "pipeline-content.json")["payload"]
    definition = json.loads(base64.b64decode(content_b64).decode())
    activity_ids = [a["typeProperties"]["notebookId"] for a in definition["properties"]["activities"]]
    assert NB_CD_ID in activity_ids
    assert NB_CU_ID in activity_ids


# --- provision_schedule ---

def test_provision_schedule_creates_when_not_exists(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        result = provision_schedule(WS_ID, PIP_ID, fabric_headers)

    assert result.status == "created"


def test_provision_schedule_uses_default_job_type(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        provision_schedule(WS_ID, PIP_ID, fabric_headers)

    url = mock_post.call_args[0][0]
    assert "DefaultJob" in url


def test_provision_schedule_uses_romance_standard_time(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": []})
        mock_post.return_value = _created({"id": "sched-001"})
        provision_schedule(WS_ID, PIP_ID, fabric_headers)

    body = mock_post.call_args[1]["json"]
    assert body["configuration"]["localTimeZoneId"] == "Romance Standard Time"


def test_provision_schedule_skips_when_active(fabric_headers):
    with patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.requests.post") as mock_post:
        mock_get.return_value = _ok({"value": [{"id": "sched-001", "enabled": True}]})
        result = provision_schedule(WS_ID, PIP_ID, fabric_headers)

    mock_post.assert_not_called()
    assert result.status == "existing_reused"


# --- run_and_monitor ---

def test_run_and_monitor_success(fabric_headers):
    job_location = f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items/{PIP_ID}/jobs/instances/{JOB_INSTANCE_ID}"
    with patch("deploy.fabric_pipelines.requests.post") as mock_post, \
         patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.time.sleep"):
        mock_post.return_value = _accepted(job_location)
        mock_get.side_effect = [
            _ok({"status": "InProgress"}),
            _ok({"status": "Completed"}),
            _ok({"data": [
                {"name": "bronze.cloud_discovery_apps"},
                {"name": "bronze.cloud_discovery_users"},
                {"name": "bronze.copilot_usage_detail"},
                {"name": "bronze.copilot_usage_trend"},
            ]}),  # list tables
        ]
        result = run_and_monitor(WS_ID, PIP_ID, LKH_ID, fabric_headers)

    assert result.status == "completed"
    assert len(result.tables_verified) == 4


def test_run_and_monitor_raises_on_failure(fabric_headers):
    from deploy.fabric_pipelines import PipelineJobError
    job_location = "https://api.fabric.microsoft.com/v1/.../instances/job-1"
    with patch("deploy.fabric_pipelines.requests.post") as mock_post, \
         patch("deploy.fabric_pipelines.requests.get") as mock_get, \
         patch("deploy.fabric_pipelines.time.sleep"):
        mock_post.return_value = _accepted(job_location)
        mock_get.return_value = _ok({"status": "Failed", "failureReason": "Out of memory"})
        with pytest.raises(PipelineJobError) as exc_info:
            run_and_monitor(WS_ID, PIP_ID, LKH_ID, fabric_headers)
    assert "Out of memory" in str(exc_info.value)
```

- [ ] **Step 11.2: Run tests — verify they fail**

```bash
pytest tests/test_fabric_pipelines.py -v
```

Expected: `ImportError`.

- [ ] **Step 11.3: Implement fabric_pipelines.py**

`deploy/fabric_pipelines.py`:
```python
from __future__ import annotations
import base64
import json
import time
from dataclasses import dataclass, field
import requests

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
_PIPELINE_NAME = "pip_daily_ai_ingestion"
_JOB_POLL_INTERVAL = 30
_JOB_TIMEOUT = 1800  # 30 minutes


class PipelineJobError(Exception):
    pass


class TableVerificationError(Exception):
    pass


@dataclass
class PipelineResult:
    pipeline_id: str
    status: str


@dataclass
class ScheduleResult:
    schedule_id: str
    status: str
    next_run: str = ""


@dataclass
class RunResult:
    job_instance_id: str
    status: str
    duration_seconds: float
    tables_verified: list = field(default_factory=list)


def provision_pipeline(
    workspace_id: str,
    nb_cloud_discovery_id: str,
    nb_copilot_usage_id: str,
    headers: dict,
) -> PipelineResult:
    """Create or reuse the daily AI ingestion Data Pipeline."""
    resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/dataPipelines",
        headers=headers,
    )
    resp.raise_for_status()
    existing = [p for p in resp.json().get("value", []) if p["displayName"] == _PIPELINE_NAME]
    if existing:
        return PipelineResult(pipeline_id=existing[0]["id"], status="existing_reused")

    # Create empty pipeline
    create_resp = requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items",
        headers=headers,
        json={
            "displayName": _PIPELINE_NAME,
            "type": "DataPipeline",
            "description": "Pipeline quotidien d'ingestion IA (Cloud Discovery + Copilot Usage)",
        },
    )
    create_resp.raise_for_status()
    pipeline_id = create_resp.json()["id"]

    # Update pipeline definition with both Notebook Activities
    definition = {
        "properties": {
            "activities": [
                {
                    "name": "Ingest_Cloud_Discovery",
                    "type": "TridentNotebook",
                    "typeProperties": {
                        "notebookId": nb_cloud_discovery_id,
                        "workspaceId": workspace_id,
                    },
                },
                {
                    "name": "Ingest_Copilot_Usage",
                    "type": "TridentNotebook",
                    "typeProperties": {
                        "notebookId": nb_copilot_usage_id,
                        "workspaceId": workspace_id,
                    },
                },
            ]
        }
    }
    def_b64 = base64.b64encode(json.dumps(definition).encode()).decode()
    requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
        headers=headers,
        json={
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": def_b64, "payloadType": "InlineBase64"}
                ]
            }
        },
    ).raise_for_status()

    return PipelineResult(pipeline_id=pipeline_id, status="created")


def provision_schedule(
    workspace_id: str,
    pipeline_id: str,
    headers: dict,
    start_date: str = "2026-04-01T06:00:00",
    end_date: str = "2027-03-31T23:59:00",
) -> ScheduleResult:
    """Create or reuse the daily 6h00 schedule (DefaultJob, Romance Standard Time)."""
    sched_url = (
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/DefaultJob/schedules"
    )
    resp = requests.get(sched_url, headers=headers)
    resp.raise_for_status()
    active = [s for s in resp.json().get("value", []) if s.get("enabled")]
    if active:
        return ScheduleResult(schedule_id=active[0]["id"], status="existing_reused")

    create_resp = requests.post(
        sched_url,
        headers=headers,
        json={
            "enabled": True,
            "configuration": {
                "startDateTime": start_date,
                "endDateTime": end_date,
                "localTimeZoneId": "Romance Standard Time",
                "type": "Cron",
                "interval": 1440,
            },
        },
    )
    create_resp.raise_for_status()
    sched_id = create_resp.json().get("id", "")
    return ScheduleResult(schedule_id=sched_id, status="created")


def run_and_monitor(
    workspace_id: str,
    pipeline_id: str,
    lakehouse_id: str,
    headers: dict,
) -> RunResult:
    """Trigger on-demand pipeline run, poll to completion, verify Bronze tables."""
    t_start = time.time()

    # Trigger on-demand run
    run_resp = requests.post(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        headers=headers,
    )
    run_resp.raise_for_status()
    job_location = run_resp.headers["Location"]
    job_instance_id = job_location.rstrip("/").split("/")[-1]

    # Poll job status
    deadline = t_start + _JOB_TIMEOUT
    while True:
        if time.time() > deadline:
            raise TimeoutError(f"Pipeline job timed out after {_JOB_TIMEOUT}s")
        status_resp = requests.get(job_location, headers=headers)
        status_resp.raise_for_status()
        body = status_resp.json()
        status = body.get("status", "Unknown")
        if status == "Completed":
            break
        if status in ("Failed", "Cancelled"):
            raise PipelineJobError(
                f"Pipeline job {status}: {body.get('failureReason', 'unknown reason')}"
            )
        time.sleep(_JOB_POLL_INTERVAL)

    duration = time.time() - t_start

    # Verify Bronze tables
    _EXPECTED_TABLES = [
        "bronze.cloud_discovery_apps",
        "bronze.cloud_discovery_users",
        "bronze.copilot_usage_detail",
        "bronze.copilot_usage_trend",
    ]
    tables_resp = requests.get(
        f"{FABRIC_BASE}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables",
        headers=headers,
    )
    tables_resp.raise_for_status()
    available = {t["name"] for t in tables_resp.json().get("data", [])}
    missing = [t for t in _EXPECTED_TABLES if t not in available]
    if missing:
        raise TableVerificationError(f"Tables manquantes après le run : {missing}")

    return RunResult(
        job_instance_id=job_instance_id,
        status="completed",
        duration_seconds=round(duration, 1),
        tables_verified=_EXPECTED_TABLES,
    )
```

- [ ] **Step 11.4: Run tests — verify they pass**

```bash
pytest tests/test_fabric_pipelines.py -v
```

Expected: 9 tests PASSED.

- [ ] **Step 11.5: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASSED.

- [ ] **Step 11.6: Commit**

```bash
git add deploy/fabric_pipelines.py tests/test_fabric_pipelines.py
git commit -m "feat: add full pipeline lifecycle (create, schedule, on-demand run, table verification)"
```

---

## Chunk 6: Output + CLI Orchestration + Docs

### Task 12: output.py

**Files:**
- Create: `deploy/output.py`
- Create: `tests/test_output.py`

**What this module does:**
1. `write_output(steps: dict)` — write `deploy-output.json` with timestamp
2. `read_output()` — load existing `deploy-output.json`, return dict or `{}`
3. `get_id(output, step_key, id_field)` — extract an ID from a prior step, raise if missing
4. `build_portal_url(resource_type, **kwargs)` — build Fabric/Entra portal URLs

- [ ] **Step 12.1: Write failing tests**

`tests/test_output.py`:
```python
import json
import pytest
from pathlib import Path
from deploy.output import write_output, read_output, get_id, OutputError


def test_write_output_creates_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    steps = {"1_entra": {"status": "created", "appId": "app-123"}}
    write_output(steps)
    assert (tmp_path / "deploy-output.json").exists()


def test_write_output_includes_deployed_at(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_output({"1_entra": {"status": "created"}})
    data = json.loads((tmp_path / "deploy-output.json").read_text())
    assert "deployedAt" in data
    assert "steps" in data


def test_read_output_returns_empty_when_no_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = read_output()
    assert result == {}


def test_read_output_returns_steps(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_output({"2_workspace": {"status": "created", "workspaceId": "ws-001"}})
    result = read_output()
    assert result["2_workspace"]["workspaceId"] == "ws-001"


def test_get_id_returns_value(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    write_output({"3_lakehouse": {"status": "created", "lakehouseId": "lkh-001"}})
    output = read_output()
    assert get_id(output, "3_lakehouse", "lakehouseId") == "lkh-001"


def test_get_id_raises_when_missing():
    with pytest.raises(OutputError) as exc_info:
        get_id({}, "3_lakehouse", "lakehouseId")
    assert "3_lakehouse" in str(exc_info.value)
    assert "lakehouseId" in str(exc_info.value)
```

- [ ] **Step 12.2: Run tests — verify they fail**

```bash
pytest tests/test_output.py -v
```

Expected: `ImportError`.

- [ ] **Step 12.3: Implement output.py**

`deploy/output.py`:
```python
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
```

- [ ] **Step 12.4: Run tests — verify they pass**

```bash
pytest tests/test_output.py -v
```

Expected: 6 tests PASSED.

- [ ] **Step 12.5: Commit**

```bash
git add deploy/output.py tests/test_output.py
git commit -m "feat: add deploy-output.json read/write with ID lookup for step resume"
```

---

### Task 13: __main__.py — CLI Orchestrator

**Files:**
- Create: `deploy/__main__.py`
- Create: `tests/test_main.py`

**What this module does:**
1. Parse CLI args (`--from-step N`, `--list-steps`)
2. Load config + setup logger
3. Acquire tokens (Fabric via MSAL, admin from env var)
4. For each step ≥ from_step: call the relevant module, update output, write output
5. On error: log, write partial output, exit with non-zero code

- [ ] **Step 13.1: Write failing tests**

`tests/test_main.py`:
```python
import pytest
import sys
from unittest.mock import patch, Mock, MagicMock
from deploy import __main__ as main_module


def test_list_steps_prints_and_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["deploy", "--list-steps"]):
            main_module.main()
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Step 1" in captured.out
    assert "Step 7" in captured.out


def test_from_step_invalid_raises():
    with pytest.raises(SystemExit):
        with patch("sys.argv", ["deploy", "--from-step", "99"]):
            main_module.main()


def test_full_deployment_calls_all_steps(base_env_vars):
    mock_results = {
        "entra": Mock(app_id="app-1", object_id="obj-1", spn_object_id="spn-1",
                      client_secret="sec-1", status="created"),
        "workspace": Mock(workspace_id="ws-1", status="created"),
        "lakehouse": Mock(lakehouse_id="lkh-1", status="created"),
        "notebooks": Mock(ids={"nb_cloud_discovery": "nb-1", "nb_copilot_usage": "nb-2"},
                          status="created"),
        "pipeline": Mock(pipeline_id="pip-1", status="created"),
        "schedule": Mock(schedule_id="sched-1", status="created", next_run=""),
        "run": Mock(job_instance_id="job-1", status="completed",
                    duration_seconds=30.0, tables_verified=["bronze.cloud_discovery_apps"]),
    }

    with patch("sys.argv", ["deploy"]), \
         patch("deploy.__main__.provision_entra", return_value=mock_results["entra"]), \
         patch("deploy.__main__.provision_workspace", return_value=mock_results["workspace"]), \
         patch("deploy.__main__.provision_lakehouse", return_value=mock_results["lakehouse"]), \
         patch("deploy.__main__.provision_notebooks", return_value=mock_results["notebooks"]), \
         patch("deploy.__main__.provision_pipeline", return_value=mock_results["pipeline"]), \
         patch("deploy.__main__.provision_schedule", return_value=mock_results["schedule"]), \
         patch("deploy.__main__.run_and_monitor", return_value=mock_results["run"]), \
         patch("deploy.__main__.load_config"), \
         patch("deploy.__main__.setup_logger", return_value=Mock()), \
         patch("deploy.__main__._get_fabric_token", return_value="fab-token"), \
         patch("deploy.__main__.write_output"), \
         patch("deploy.__main__.read_output", return_value={}):
        main_module.main()

    # Verify all 7 steps were executed
    deploy.__main__.provision_entra.assert_called_once()
    deploy.__main__.provision_workspace.assert_called_once()
    deploy.__main__.provision_lakehouse.assert_called_once()
    deploy.__main__.provision_notebooks.assert_called_once()
    deploy.__main__.provision_pipeline.assert_called_once()
    deploy.__main__.provision_schedule.assert_called_once()
    deploy.__main__.run_and_monitor.assert_called_once()
```

- [ ] **Step 13.2: Run tests — verify they fail**

```bash
pytest tests/test_main.py -v
```

Expected: `ImportError`.

- [ ] **Step 13.3: Implement __main__.py**

`deploy/__main__.py`:
```python
from __future__ import annotations
import argparse
import sys
import logging
import msal

from deploy.config import load_config, ConfigError
from deploy.logger import setup_logger
from deploy.output import read_output, write_output, get_id, build_portal_url, OutputError
from deploy.entra import provision_entra
from deploy.fabric_workspace import provision_workspace
from deploy.fabric_lakehouse import provision_lakehouse
from deploy.fabric_notebooks import provision_notebooks
from deploy.fabric_pipelines import provision_pipeline, provision_schedule, run_and_monitor

FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

_STEPS = [
    (1, "App Registration Entra + Service Principal + appRoleAssignments"),
    (2, "Workspace Fabric + SPN Contributor role"),
    (3, "Lakehouse lkh_ai_analytics"),
    (4, "Notebooks d'ingestion (Cloud Discovery + Copilot Usage)"),
    (5, "Data Pipeline pip_daily_ai_ingestion"),
    (6, "Planification Job Scheduler (DefaultJob, quotidien 6h00)"),
    (7, "Exécution on-demand + monitoring + vérification tables Delta"),
]


def _get_fabric_token(cfg) -> str:
    app = msal.ConfidentialClientApplication(
        cfg.client_id,
        authority=f"https://login.microsoftonline.com/{cfg.tenant_id}",
        client_credential=cfg.client_secret,
    )
    result = app.acquire_token_for_client(scopes=[FABRIC_SCOPE])
    return result["access_token"]


def _make_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fabric AI Analytics Deployer")
    parser.add_argument("--from-step", type=int, default=1, metavar="N",
                        help="Reprendre depuis l'étape N (1-7)")
    parser.add_argument("--list-steps", action="store_true",
                        help="Afficher les étapes et quitter")
    args = parser.parse_args()

    if args.list_steps:
        for num, desc in _STEPS:
            print(f"  Step {num}: {desc}")
        sys.exit(0)

    if not 1 <= args.from_step <= 7:
        print(f"Erreur : --from-step doit être entre 1 et 7 (reçu: {args.from_step})")
        sys.exit(1)

    logger = setup_logger()
    logger.info("=== Fabric AI Analytics Deployer ===")

    try:
        cfg = load_config()
    except ConfigError as e:
        logger.error(f"Configuration manquante : {e}")
        sys.exit(1)

    fabric_token = _get_fabric_token(cfg)
    fabric_headers = _make_headers(fabric_token)
    admin_headers = _make_headers(cfg.admin_token) if cfg.admin_token else {}

    output = read_output() if args.from_step > 1 else {}
    steps_result = dict(output)

    from_step = args.from_step

    try:
        # Step 1: Entra
        if from_step <= 1:
            logger.info("[STEP 1] Provisionnement App Registration Entra...")
            result = provision_entra("Fabric-Pipeline-AI-Analytics", admin_headers)
            steps_result["1_entra"] = {
                "status": result.status,
                "appId": result.app_id,
                "objectId": result.object_id,
                "spnObjectId": result.spn_object_id,
                "portalUrl": build_portal_url("entra_app", app_id=result.app_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 1] {result.status.upper()} — appId: {result.app_id}")

        # Step 2: Workspace
        if from_step <= 2:
            logger.info("[STEP 2] Provisionnement Workspace Fabric...")
            spn_id = get_id(steps_result, "1_entra", "spnObjectId")
            result = provision_workspace(
                "WS_AI_Analytics_ADENES", cfg.capacity_id, spn_id, fabric_headers
            )
            steps_result["2_workspace"] = {
                "status": result.status,
                "workspaceId": result.workspace_id,
                "portalUrl": build_portal_url("workspace", workspace_id=result.workspace_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 2] {result.status.upper()} — workspaceId: {result.workspace_id}")

        ws_id = get_id(steps_result, "2_workspace", "workspaceId")

        # Step 3: Lakehouse
        if from_step <= 3:
            logger.info("[STEP 3] Provisionnement Lakehouse...")
            result = provision_lakehouse("lkh_ai_analytics", ws_id, fabric_headers)
            steps_result["3_lakehouse"] = {
                "status": result.status,
                "lakehouseId": result.lakehouse_id,
                "portalUrl": build_portal_url("lakehouse", workspace_id=ws_id,
                                               lakehouse_id=result.lakehouse_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 3] {result.status.upper()} — lakehouseId: {result.lakehouse_id}")

        lkh_id = get_id(steps_result, "3_lakehouse", "lakehouseId")

        # Step 4: Notebooks
        if from_step <= 4:
            logger.info("[STEP 4] Provisionnement Notebooks...")
            result = provision_notebooks(ws_id, fabric_headers)
            steps_result["4_notebooks"] = {
                "status": result.status,
                "ids": result.ids,
            }
            write_output(steps_result)
            logger.info(f"[STEP 4] {result.status.upper()} — ids: {result.ids}")

        nb_ids = get_id(steps_result, "4_notebooks", "ids")

        # Step 5: Pipeline
        if from_step <= 5:
            logger.info("[STEP 5] Provisionnement Data Pipeline...")
            result = provision_pipeline(
                ws_id, nb_ids["nb_cloud_discovery"], nb_ids["nb_copilot_usage"], fabric_headers
            )
            steps_result["5_pipeline"] = {
                "status": result.status,
                "pipelineId": result.pipeline_id,
                "portalUrl": build_portal_url("pipeline", workspace_id=ws_id,
                                               pipeline_id=result.pipeline_id),
            }
            write_output(steps_result)
            logger.info(f"[STEP 5] {result.status.upper()} — pipelineId: {result.pipeline_id}")

        pip_id = get_id(steps_result, "5_pipeline", "pipelineId")

        # Step 6: Schedule
        if from_step <= 6:
            logger.info("[STEP 6] Provisionnement planification Job Scheduler...")
            result = provision_schedule(ws_id, pip_id, fabric_headers)
            steps_result["6_schedule"] = {
                "status": result.status,
                "scheduleId": result.schedule_id,
                "nextRun": result.next_run,
            }
            write_output(steps_result)
            logger.info(f"[STEP 6] {result.status.upper()} — scheduleId: {result.schedule_id}")

        # Step 7: Test run
        if from_step <= 7:
            logger.info("[STEP 7] Exécution on-demand et vérification tables...")
            result = run_and_monitor(ws_id, pip_id, lkh_id, fabric_headers)
            steps_result["7_test_run"] = {
                "status": result.status,
                "jobInstanceId": result.job_instance_id,
                "durationSeconds": result.duration_seconds,
                "tablesVerified": result.tables_verified,
            }
            write_output(steps_result)
            logger.info(
                f"[STEP 7] COMPLETED — durée: {result.duration_seconds}s, "
                f"tables: {result.tables_verified}"
            )

        logger.info("=== Déploiement terminé avec succès ===")
        logger.info(f"Résultat écrit dans deploy-output.json")

    except (OutputError, ConfigError) as e:
        logger.error(f"Erreur de configuration ou d'état : {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Erreur inattendue : {e}")
        write_output(steps_result)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 13.4: Run tests — verify they pass**

```bash
pytest tests/test_main.py -v
```

Expected: 3 tests PASSED.

- [ ] **Step 13.5: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASSED (≥35 tests across all modules).

- [ ] **Step 13.6: Commit**

```bash
git add deploy/__main__.py tests/test_main.py
git commit -m "feat: add CLI orchestrator with step-by-step deployment and --from-step resume"
```

---

### Task 14: README.md

**Files:**
- Create: `README.md`

- [ ] **Step 14.1: Write README.md**

`README.md`:
```markdown
# Fabric AI Analytics Deployer

Script Python d'orchestration pour provisionner l'infrastructure Microsoft Fabric
du pipeline d'analytics IA d'ADENES en une seule exécution.

## Prérequis

1. Python 3.10+
2. Activer dans le portail admin Fabric :
   - « Service principals can use Fabric APIs »
   - « Service principals can create workspaces, connections, and deployment pipelines »
3. Pour l'étape 1 (Entra), un compte avec `Application.ReadWrite.All` + `AppRoleAssignment.ReadWrite.All`

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Copier `.env.example` en `.env` et renseigner les valeurs :

```bash
cp .env.example .env
```

Pour l'étape 1, obtenir un token admin :
```bash
az account get-access-token --resource https://graph.microsoft.com --query accessToken -o tsv
```

## Utilisation

```bash
# Déploiement complet
python -m deploy

# Reprendre depuis l'étape 3 (après un échec)
python -m deploy --from-step 3

# Lister les étapes
python -m deploy --list-steps
```

## Étapes

| # | Description |
|---|---|
| 1 | App Registration Entra + Service Principal + permissions |
| 2 | Workspace Fabric + rôle SPN Contributor |
| 3 | Lakehouse `lkh_ai_analytics` |
| 4 | Notebooks d'ingestion (Cloud Discovery + Copilot Usage) |
| 5 | Data Pipeline `pip_daily_ai_ingestion` |
| 6 | Planification quotidienne 6h00 (Romance Standard Time) |
| 7 | Exécution test on-demand + vérification tables Bronze |

## Configuration Viva Insights (manuelle)

Le Dataflow Gen2 Viva Insights n'est pas provisionné par ce script
(API Fabric REST non disponible). Configuration manuelle :
1. Créer une Custom Person Query dans Viva Insights Advanced Analysis
2. Créer un Dataflow Gen2 dans Fabric avec le connecteur Viva Insights
3. Destination : Lakehouse `lkh_ai_analytics`, table `bronze.viva_copilot_metrics`
4. Planifier en refresh hebdomadaire

## Sortie

Le fichier `deploy-output.json` est généré avec les IDs et URLs de toutes les ressources.
```

- [ ] **Step 14.2: Run final test suite**

```bash
pytest tests/ -v --tb=short
```

Expected: all tests PASSED.

- [ ] **Step 14.3: Final commit**

```bash
git add README.md
git commit -m "docs: add README with prerequisites, usage, and manual Viva Insights steps"
```

---
