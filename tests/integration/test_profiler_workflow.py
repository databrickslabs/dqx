import copy
from datetime import timedelta

import pytest
from databricks.labs.blueprint.parallel import ManyError

from databricks.labs.dqx.config import (
    InstallationChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine


def test_profiler_workflow_when_missing_input_location_in_config(ws, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    config = installation_ctx.config
    run_config = config.get_run_config()
    input_config = run_config.input_config
    input_config.location = "invalid"
    installation_ctx.installation.save(installation_ctx.config)

    with pytest.raises(ManyError) as failure:
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    assert "Invalid input location." in str(failure.value)

    install_folder = installation_ctx.installation.install_folder()
    workflow_run_logs = list(ws.workspace.list(f"{install_folder}/logs"))
    assert len(workflow_run_logs) == 1


def test_profiler_workflow_when_timeout(ws, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    with pytest.raises(TimeoutError) as failure:
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name, max_wait=timedelta(seconds=0))

    assert "timed out" in str(failure.value)


def test_profiler_workflow(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows()

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."


def test_profiler_workflow_serverless(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()
    dq_engine = DQEngine(ws, spark)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )

    # save fake checks to make sure they are overwritten by the profiler
    fake_checks = [
        {
            "check": {"function": "fake_func", "arguments": {"column": "fake_col"}},
        },
    ]
    dq_engine.save_checks(fake_checks, config=config)

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    checks = dq_engine.load_checks(config=config)
    assert checks, "Checks were not loaded correctly"
    assert checks != fake_checks, "Checks were not updated"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."


def test_profiler_workflow_with_custom_install_folder(ws, spark, setup_workflows_with_custom_folder):
    installation_ctx, run_config = setup_workflows_with_custom_folder()

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
        install_folder=installation_ctx.installation.install_folder(),
    )

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."


def test_profiler_workflow_for_multiple_run_configs(ws, spark, setup_workflows):
    installation_ctx, run_config = setup_workflows()

    second_run_config = copy.deepcopy(run_config)
    second_run_config.name = "second"
    second_run_config.checks_location = "second_checks.yml"
    second_run_config.profiler_config.summary_stats_file = "second_profile_summary_stats.yml"
    installation_ctx.config.run_configs.append(second_run_config)

    # overwrite config in the installation folder
    installation_ctx.installation.save(installation_ctx.config)

    # run workflow
    installation_ctx.deployed_workflows.run_workflow("profiler", run_config_name="")

    dq_engine = DQEngine(ws, spark)

    # assert first run config results
    storage_config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )

    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks from the {run_config.name} run config were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."

    # assert second run config results
    storage_config = InstallationChecksStorageConfig(
        run_config_name=second_run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )

    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks from the {second_run_config.name} run config were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{second_run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {second_run_config.profiler_config.summary_stats_file} does not exist."


def test_profiler_workflow_for_patterns(ws, spark, setup_workflows, make_table, make_random):
    installation_ctx, run_config = setup_workflows()

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')
    second_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    # run profiler for all tables in the schema
    installation_ctx.deployed_workflows.run_workflow(
        "profiler", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    dq_engine = DQEngine(ws, spark)

    # assert checks for first table
    workspace_file_storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml",
    )
    checks = dq_engine.load_checks(config=workspace_file_storage_config)
    assert checks, f"Checks for {first_table} were not generated"

    # assert checks for second table
    workspace_file_storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{second_table}.yml",
    )
    checks = dq_engine.load_checks(config=workspace_file_storage_config)
    assert checks, f"Checks for {second_table} were not generated"


def test_profiler_workflow_for_patterns_table_checks_storage(ws, spark, setup_workflows, make_table, make_random):
    installation_ctx, run_config = setup_workflows()

    first_table_full_name = run_config.input_config.location
    catalog_name, schema_name, _ = first_table_full_name.split('.')

    # update run config to use table storage for checks
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = f"{catalog_name}.{schema_name}.checks"
    installation_ctx.installation.save(config)

    second_table_full_name = make_second_input_table(
        spark, catalog_name, schema_name, first_table_full_name, make_random
    )

    # run profiler for all tables in the schema
    installation_ctx.deployed_workflows.run_workflow(
        "profiler", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    dq_engine = DQEngine(ws, spark)

    # assert checks for first table
    table_storage_config = TableChecksStorageConfig(
        location=run_config.checks_location,
        run_config_name=first_table_full_name,
    )
    checks = dq_engine.load_checks(config=table_storage_config)
    assert checks, f"Checks for {first_table_full_name} were not generated"

    # assert checks for second table
    workspace_file_storage_config = TableChecksStorageConfig(
        location=run_config.checks_location,
        run_config_name=second_table_full_name,
    )
    checks = dq_engine.load_checks(config=workspace_file_storage_config)
    assert checks, f"Checks for {second_table_full_name} were not generated"


def make_second_input_table(spark, catalog_name, schema_name, first_table, make_random):
    second_table = f"{catalog_name}.{schema_name}.dummy_t{make_random(4).lower()}"
    spark.table(first_table).write.format("delta").mode("overwrite").saveAsTable(second_table)
    return second_table
