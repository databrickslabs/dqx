import logging
import importlib.metadata
from datetime import timedelta
from unittest.mock import patch, create_autospec
import pytest

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig, InputConfig, OutputConfig, ProfilerConfig
from databricks.labs.dqx.installer.install import WorkspaceInstaller, InstallationService
from databricks.labs.dqx.installer.workflow_installer import WorkflowDeployment, DeployedWorkflows
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.sdk.service.jobs import RunResultState

logger = logging.getLogger(__name__)


@pytest.fixture
def installation_with_upload_deps(ws, make_random, env_or_skip):
    """Create an installation with upload_dependencies=True."""
    cleanup = []

    def factory():
        product_info = ProductInfo.for_testing(WorkspaceConfig)

        prompts = MockPrompts(
            {
                r'Provide location for the input data .*': 'main.dqx_test.input_table',
                r'Provide output table .*': 'main.dqx_test.output_table',
                r'Do you want to uninstall DQX .*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Does the given workspace block Internet access\?": "yes",  # Enable upload_dependencies
                r".*": "",
            }
        )

        installation = Installation(ws, product_info.product_name())
        installer = WorkspaceInstaller(ws).replace(
            installation=installation,
            product_info=product_info,
            prompts=prompts,
        )

        workspace_config = installer.configure()

        # Verify upload_dependencies is set to True
        assert workspace_config.upload_dependencies is True, "upload_dependencies should be True"

        installation.save(workspace_config)
        cleanup.append(installation)

        return installation, workspace_config

    yield factory

    for pending in cleanup:
        try:
            pending.remove()
        except Exception as e:
            logger.warning(f"Failed to cleanup installation: {e}")


def test_installation_with_upload_dependencies(ws, installation_with_upload_deps):
    """Test that installation correctly sets upload_dependencies flag."""
    installation, config = installation_with_upload_deps()

    # Verify configuration
    assert config.upload_dependencies is True

    # Verify configuration is saved correctly
    loaded_config = installation.load(WorkspaceConfig)
    assert loaded_config.upload_dependencies is True


def test_workflow_deployment_uploads_dependencies(ws, installation_with_upload_deps, make_random):
    """Test that workflow deployment uploads dependencies when upload_dependencies is True."""
    installation, config = installation_with_upload_deps()

    product_info = ProductInfo.for_testing(WorkspaceConfig)
    # Mock install state since we're just testing wheel upload behavior
    install_state = create_autospec(InstallState)
    install_state.jobs = {}
    install_state.save.return_value = None  # Mock save to avoid NotInstalled error
    wheels = product_info.wheels(ws)

    # Create a simple task for testing
    tasks = [
        Task(
            workflow="test_workflow",
            name="test_task",
            job_cluster="default",
            doc="Test task",
            fn=lambda: None,
        )
    ]

    deployment = WorkflowDeployment(
        config=config,
        installation=installation,
        install_state=install_state,
        ws=ws,
        wheels=wheels,
        product_info=product_info,
        tasks=tasks,
    )

    # Mock the wheels methods and job creation to test upload behavior
    with (
        patch.object(wheels, 'upload_wheel_dependencies') as mock_upload_deps,
        patch.object(wheels, 'upload_to_wsfs') as mock_upload_main,
        patch.object(ws.jobs, 'create') as mock_create_job,
    ):
        mock_upload_deps.return_value = [
            "/foo/bar/databricks_sdk-0.71.0-py3-none-any.whl",
            "/foo/bar/databricks_labs_lsql-0.5.0-py3-none-any.whl",
        ]
        mock_upload_main.return_value = "/foo/bar/databricks_labs_dqx-0.1.0-py3-none-any.whl"
        mock_create_job.return_value = type('obj', (object,), {'job_id': 123})()

        # Call public method create_jobs() which internally calls _upload_wheel()
        deployment.create_jobs()

        # Verify upload_wheel_dependencies was called
        mock_upload_deps.assert_called_once()

        # Verify the dependency prefixes passed to upload_wheel_dependencies
        call_args = mock_upload_deps.call_args[0][0]
        assert isinstance(call_args, list)
        assert len(call_args) > 0

        # Verify core dependencies are included
        assert any("databricks_sdk" in dep for dep in call_args)
        assert any("databricks_labs_lsql" in dep for dep in call_args)

        # Verify job was created with the uploaded wheels
        mock_create_job.assert_called_once()
        job_settings = mock_create_job.call_args[1]
        assert 'tasks' in job_settings


def test_dependency_discovery_includes_extras(ws, installation_with_upload_deps):
    """Test that dependency discovery includes extras dependencies when creating jobs."""
    installation, config = installation_with_upload_deps()

    product_info = ProductInfo.for_testing(WorkspaceConfig)
    # Mock install state since we're just testing wheel upload behavior
    install_state = create_autospec(InstallState)
    install_state.jobs = {}
    install_state.save.return_value = None  # Mock save to avoid NotInstalled error
    wheels = product_info.wheels(ws)

    tasks = [
        Task(
            workflow="test_workflow",
            name="test_task",
            job_cluster="default",
            doc="Test task",
            fn=lambda: None,
        )
    ]

    deployment = WorkflowDeployment(
        config=config,
        installation=installation,
        install_state=install_state,
        ws=ws,
        wheels=wheels,
        product_info=product_info,
        tasks=tasks,
    )

    # Mock package metadata to include extras dependencies
    mock_requires = [
        "databricks-labs-blueprint>=0.9.1,<0.10",
        "databricks-sdk~=0.71",
        "databricks-labs-lsql>=0.5,<=0.16",
        "sqlalchemy>=1.4,<3.0",
        "pydantic>=2.0; extra == 'pii'",  # extras dependency
        "openai>=1.0; extra == 'llm'",  # extras dependency
    ]

    with (
        patch.object(importlib.metadata, 'requires', return_value=mock_requires),
        patch.object(wheels, 'upload_wheel_dependencies') as mock_upload_deps,
        patch.object(wheels, 'upload_to_wsfs') as mock_upload_main,
        patch.object(ws.jobs, 'create') as mock_create_job,
    ):
        mock_upload_deps.return_value = ["/foo/bar/dep.whl"]
        mock_upload_main.return_value = "/foo/bar/dqx.whl"
        mock_create_job.return_value = type('obj', (object,), {'job_id': 123})()

        # Call public method that triggers dependency discovery
        deployment.create_jobs()

        # Verify upload_wheel_dependencies was called with all dependencies including extras
        mock_upload_deps.assert_called_once()
        uploaded_prefixes = mock_upload_deps.call_args[0][0]

        # Verify all dependencies are included (core + extras)
        assert len(uploaded_prefixes) == 6
        assert "databricks_labs_blueprint" in uploaded_prefixes
        assert "databricks_sdk" in uploaded_prefixes
        assert "databricks_labs_lsql" in uploaded_prefixes
        assert "sqlalchemy" in uploaded_prefixes
        assert "pydantic" in uploaded_prefixes  # extras should be included
        assert "openai" in uploaded_prefixes  # extras should be included


def test_config_upload_dependencies_persists(ws, installation_with_upload_deps):
    """Test that upload_dependencies configuration persists through save/load cycle."""
    installation, original_config = installation_with_upload_deps()

    # Verify original config
    assert original_config.upload_dependencies is True

    # Load config from installation
    loaded_config = installation.load(WorkspaceConfig)

    # Verify loaded config has the same value
    assert loaded_config.upload_dependencies is True

    # Verify it's properly serialized in as_dict()
    config_dict = loaded_config.as_dict()
    assert "upload_dependencies" in config_dict
    assert config_dict["upload_dependencies"] is True


def _configure_test_workspace(installer, input_table, catalog_name, schema_name, make_random):
    """Helper to configure workspace with test data."""
    workspace_config = installer.configure()
    assert workspace_config.upload_dependencies is True, "upload_dependencies should be True"

    run_config = workspace_config.get_run_config()
    run_config.input_config = InputConfig(location=input_table.full_name, options={"versionAsOf": "0"})
    output_table = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    run_config.output_config = OutputConfig(location=output_table)
    run_config.profiler_config = ProfilerConfig(sample_fraction=1.0, sample_seed=100)

    return workspace_config


def _run_and_verify_workflow(ws, installation):
    """Helper to run and verify profiler workflow execution."""
    install_state = InstallState.from_installation(installation)
    assert len(install_state.jobs) > 0, "Jobs should be created"

    deployed_workflows = DeployedWorkflows(ws, install_state)
    run_id = deployed_workflows.run_workflow("profiler", run_config_name="default", max_wait=timedelta(minutes=15))

    assert run_id is not None, "Workflow should return a run_id"

    job_runs = list(ws.jobs.list_runs(run_id=run_id, limit=1))
    assert len(job_runs) > 0, "Job run should exist"

    run_state = job_runs[0].state
    assert run_state is not None, "Job run should have a state"

    assert run_state.result_state in [
        RunResultState.SUCCESS,
        RunResultState.CANCELED,
    ], f"Job should complete successfully, got: {run_state.result_state}"

    return run_id


def test_end_to_end_installation_and_workflow_with_upload_dependencies(
    ws, make_schema, make_table, make_random, env_or_skip
):
    """
    End-to-end integration test: Install DQX with upload_dependencies=True and run a workflow.
    This verifies that dependencies are properly uploaded and workflows can execute successfully.
    """
    product_info = ProductInfo.for_testing(WorkspaceConfig)

    # Prepare test data
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, NULL) AS data(id, name)",
    )

    # Create prompts with upload_dependencies enabled
    prompts = MockPrompts(
        {
            r'Provide location for the input data .*': input_table.full_name,
            r'Provide output table .*': f'{catalog_name}.{schema.name}.{make_random(10).lower()}',
            r'Do you want to uninstall DQX .*': 'yes',
            r".*PRO or SERVERLESS SQL warehouse.*": "1",
            r"Does the given workspace block Internet access\?": "yes",  # Enable upload_dependencies
            r".*": "",
        }
    )

    installation = Installation(ws, product_info.product_name())
    installer = WorkspaceInstaller(ws).replace(
        installation=installation,
        product_info=product_info,
        prompts=prompts,
    )

    try:
        # Configure workspace
        workspace_config = _configure_test_workspace(installer, input_table, catalog_name, schema.name, make_random)
        installation.save(workspace_config)
    except Exception:
        installation.remove()
        raise

    try:
        # Run installation (creates jobs and uploads dependencies)
        installation_service = InstallationService.current(ws)
        installation_service.run()

        # Run and verify profiler workflow
        run_id = _run_and_verify_workflow(ws, installation)
        logger.info(f"✅ End-to-end test passed: Workflow {run_id} completed with upload_dependencies=True")

    finally:
        try:
            installation.remove()
        except Exception as e:
            logger.warning(f"Failed to cleanup installation: {e}")
