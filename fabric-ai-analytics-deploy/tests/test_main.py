import pytest
import sys
from unittest.mock import patch, Mock, MagicMock
from deploy import __main__ as main_module
import deploy.__main__


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
         patch("deploy.__main__.provision_entra", return_value=mock_results["entra"]) as mock_entra, \
         patch("deploy.__main__.provision_workspace", return_value=mock_results["workspace"]) as mock_ws, \
         patch("deploy.__main__.provision_lakehouse", return_value=mock_results["lakehouse"]) as mock_lkh, \
         patch("deploy.__main__.provision_notebooks", return_value=mock_results["notebooks"]) as mock_nb, \
         patch("deploy.__main__.provision_pipeline", return_value=mock_results["pipeline"]) as mock_pip, \
         patch("deploy.__main__.provision_schedule", return_value=mock_results["schedule"]) as mock_sched, \
         patch("deploy.__main__.run_and_monitor", return_value=mock_results["run"]) as mock_run, \
         patch("deploy.__main__.load_config"), \
         patch("deploy.__main__.setup_logger", return_value=Mock()), \
         patch("deploy.__main__._get_fabric_token", return_value="fab-token"), \
         patch("deploy.__main__.write_output"), \
         patch("deploy.__main__.read_output", return_value={}):
        main_module.main()

    # Verify all 7 steps were executed
    mock_entra.assert_called_once()
    mock_ws.assert_called_once()
    mock_lkh.assert_called_once()
    mock_nb.assert_called_once()
    mock_pip.assert_called_once()
    mock_sched.assert_called_once()
    mock_run.assert_called_once()
