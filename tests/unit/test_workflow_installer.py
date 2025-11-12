from unittest.mock import patch, create_autospec, MagicMock
from datetime import timedelta, datetime, timezone
from importlib.metadata import PackageNotFoundError

from databricks.labs.dqx.installer.workflow_installer import (
    DeployedWorkflows,
    WorkflowDeployment,
    get_dependency_prefixes,
)
from databricks.sdk.service.jobs import Run, RunState, RunResultState
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.wheels import WheelsV2, ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.installer.workflow_task import Task


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


def test_upload_wheel_includes_dependencies():
    """Test that upload_wheel uploads both the main wheel and its dependencies."""
    mock_ws = create_autospec(WorkspaceClient)
    config = WorkspaceConfig([])
    mock_installation = MockInstallation()
    install_state = InstallState.from_installation(mock_installation)

    # Mock the WheelsV2 context manager
    mock_wheels = MagicMock(spec=WheelsV2)
    mock_wheels.__enter__ = MagicMock(return_value=mock_wheels)
    mock_wheels.__exit__ = MagicMock(return_value=False)

    # Mock the dependency upload and main wheel upload
    mock_wheels.upload_wheel_dependencies.return_value = [
        '/wheels/databricks_sdk-0.71.0-py3-none-any.whl',
        '/wheels/databricks_labs_blueprint-0.9.1-py3-none-any.whl',
        '/wheels/databricks_labs_lsql-0.5.0-py3-none-any.whl',
        '/wheels/SQLAlchemy-2.0.0-py3-none-any.whl',
    ]
    mock_wheels.upload_to_wsfs.return_value = '/wheels/databricks_labs_dqx-0.8.0-py3-none-any.whl'

    product_info = ProductInfo.for_testing(WorkspaceConfig)
    tasks = [Task("workflow", "task", "docs", lambda *_: None)]

    workflow_deployment = WorkflowDeployment(
        config,
        mock_installation,
        install_state,
        mock_ws,
        mock_wheels,
        product_info,
        tasks=tasks,
    )

    # Test through create_jobs which calls _upload_wheel internally
    with patch.object(workflow_deployment, '_deploy_workflow'):
        workflow_deployment.create_jobs()

    # Verify dependencies were uploaded (should be called with dynamically determined list)
    assert mock_wheels.upload_wheel_dependencies.called
    call_args = mock_wheels.upload_wheel_dependencies.call_args[0][0]

    # Verify that common dependencies are included
    assert 'databricks_sdk' in call_args
    assert 'databricks_labs_blueprint' in call_args
    assert 'databricks_labs_lsql' in call_args

    # Verify main wheel was uploaded
    mock_wheels.upload_to_wsfs.assert_called_once()


def test_get_dependency_prefixes():
    """Test that get_dependency_prefixes correctly parses package metadata."""

    # Create a mock requirements function
    def mock_requires(_package_name):
        return [
            'databricks-labs-blueprint>=0.9.1,<0.10',
            'databricks-sdk~=0.71',
            'databricks-labs-lsql>=0.5,<=0.16',
            'sqlalchemy>=1.4,<3.0',
            'numpy<2.0,>=1.20; extra == "pii"',  # This should be excluded
            'dspy~=3.0.3; extra == "llm"',  # This should be excluded
        ]

    prefixes = get_dependency_prefixes(requirements_func=mock_requires)

    # Verify correct dependencies are included
    assert 'databricks_labs_blueprint' in prefixes
    assert 'databricks_sdk' in prefixes
    assert 'databricks_labs_lsql' in prefixes
    assert 'sqlalchemy' in prefixes

    # Verify optional dependencies are excluded
    assert 'numpy' not in prefixes
    assert 'dspy' not in prefixes

    # Verify count
    assert len(prefixes) == 4


def test_get_dependency_prefixes_fallback():
    """Test that get_dependency_prefixes falls back to defaults when metadata is not available."""

    def mock_requires_error(_package_name):
        raise PackageNotFoundError()

    prefixes = get_dependency_prefixes(requirements_func=mock_requires_error)

    # Should return fallback list
    assert 'databricks_sdk' in prefixes
    assert 'databricks_labs_blueprint' in prefixes
    assert 'databricks_labs_lsql' in prefixes
    assert 'SQLAlchemy' in prefixes
