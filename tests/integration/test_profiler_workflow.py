from datetime import timedelta

import pytest

from databricks.labs.dqx.engine import DQEngine


def test_profiler_workflow_e2e_when_missing_input_location_in_config(ws, setup_workflows):
    installation_ctx, run_config = setup_workflows

    config = installation_ctx.config
    run_config = config.get_run_config()
    input_config = run_config.input_config
    input_config.location = "invalid"
    installation_ctx.installation.save(installation_ctx.config)

    with pytest.raises(ValueError) as failure:
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    assert "Invalid input location." in str(failure.value)

    install_folder = installation_ctx.installation.install_folder()
    workflow_run_logs = list(ws.workspace.list(f"{install_folder}/logs"))
    assert len(workflow_run_logs) == 1


def test_profiler_workflow_e2e_when_timeout(ws, setup_workflows):
    installation_ctx, run_config = setup_workflows

    with pytest.raises(TimeoutError) as failure:
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name, max_wait=timedelta(seconds=0))

    assert "timed out" in str(failure.value)


def test_profiler_workflow_e2e(ws, setup_workflows):
    installation_ctx, run_config = setup_workflows

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    checks = DQEngine(ws).load_checks_from_installation(
        run_config_name=run_config.name, assume_user=True, product_name=installation_ctx.installation.product()
    )
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."
