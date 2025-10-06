from unittest.mock import patch, create_autospec
from datetime import timedelta, datetime, timezone
from databricks.labs.dqx.installer.workflow_installer import DeployedWorkflows
from databricks.sdk.service.jobs import Run, RunState, RunResultState
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installer import InstallState


def test_run_workflow():
    mock_ws = create_autospec(WorkspaceClient)
    mock_install_state = create_autospec(InstallState)
    mock_install_state.jobs = {'test_workflow': '123'}

    mock_run = create_autospec(Run)
    mock_run.run_id = 456
    mock_run.state = RunState(result_state=RunResultState.SUCCESS, state_message="Completed successfully")
    mock_run.start_time = datetime.now(tz=timezone.utc).timestamp() * 1000
    mock_run.end_time = datetime.now(tz=timezone.utc).timestamp() * 1000
    mock_run.run_duration = 1000

    with (
        patch.object(mock_ws.jobs, 'run_now', return_value=mock_run),
        patch.object(mock_ws.jobs, 'wait_get_run_job_terminated_or_skipped', return_value=mock_run),
    ):
        deployed_workflows = DeployedWorkflows(mock_ws, mock_install_state)
        run_id = deployed_workflows.run_workflow('test_workflow', 'test_run_config')

        assert run_id == 456
        mock_ws.jobs.run_now.assert_called_once_with(
            123,
            job_parameters={
                "run_config_name": "test_run_config",
                "patterns": "",
                "exclude_patterns": "",
                "output_table_suffix": "_dq_output",
                "quarantine_table_suffix": "_dq_quarantine",
            },
        )
        mock_ws.jobs.wait_get_run_job_terminated_or_skipped.assert_called_once_with(
            run_id=456, timeout=timedelta(minutes=60)
        )

        assert mock_run.state.result_state == RunResultState.SUCCESS
        assert mock_run.state.state_message == "Completed successfully"
        assert mock_run.start_time is not None
        assert mock_run.end_time is not None
        assert mock_run.run_duration == 1000
        assert mock_ws.jobs.run_now.called
        assert mock_ws.jobs.wait_get_run_job_terminated_or_skipped.called
