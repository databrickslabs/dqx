import pytest

from databricks.labs.dqx.engine import DQEngine


def test_profiler_workflow_when_missing_input_location_in_config(ws, installation_ctx):
    installation_ctx.workspace_installation.run()

    with pytest.raises(ValueError) as failure:
        installation_ctx.deployed_workflows.run_workflow("profiler", "default")

    assert "Invalid input location." in str(failure.value)

    install_folder = installation_ctx.installation.install_folder()
    workflow_run_logs = list(ws.workspace.list(f"{install_folder}/logs"))
    assert len(workflow_run_logs) == 1


def test_profiler_workflow(ws, installation_ctx, make_schema, make_table, make_random):
    installation_ctx.workspace_installation.run()

    # prepare test data
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, NULL)  AS data(id, name)",
    )

    # update input location
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.input_location = table.full_name
    installation_ctx.installation.save(installation_ctx.config)
    run_config_name = "default"

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config_name)

    dqx_engine = DQEngine(ws)
    checks = dqx_engine.load_checks_from_installation(
        run_config_name=run_config_name, assume_user=True, product_name=installation_ctx.installation.product()
    )
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profile_summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profile_summary_stats_file} does not exist."
