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
