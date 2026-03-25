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
