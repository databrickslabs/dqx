import copy

import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import (
    InstallationChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine


def test_e2e_workflow(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows()

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "Output table is empty"
    assert checked_df.count() == input_df.count(), "Output table is empty"


def test_e2e_workflow_for_multiple_run_configs(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows()

    second_run_config = copy.deepcopy(run_config)
    second_run_config.name = "second"
    second_run_config.checks_location = "second_checks.yml"
    second_run_config.profiler_config.summary_stats_file = "second_profile_summary_stats.yml"
    second_run_config.output_config.location = run_config.output_config.location + "_second"
    installation_ctx.config.run_configs.append(second_run_config)

    # overwrite config in the installation folder
    installation_ctx.installation.save(installation_ctx.config)

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config_name="")

    dq_engine = DQEngine(ws, spark)

    # assert first run config results
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = dq_engine.load_checks(config=config)
    assert checks, f"Checks from the {run_config.name} run config were not loaded correctly"

    input_df = spark.table(run_config.input_config.location)
    checked_df = spark.table(run_config.output_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, f"Output table from the {run_config.name} run config is empty"
    assert checked_df.count() == input_df.count(), f"Output table from the {run_config.name} run config is empty"

    # assert second run config results
    config = InstallationChecksStorageConfig(
        run_config_name=second_run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = dq_engine.load_checks(config=config)
    assert checks, f"Checks from the {second_run_config.name} run config were not loaded correctly"

    checked_df = spark.table(second_run_config.output_config.location)

    assert checked_df.count() > 0, f"Output table from the {second_run_config.name} run config is empty"
    assert checked_df.count() == input_df.count(), f"Output table from the {second_run_config.name} run config is empty"


def test_e2e_workflow_serverless(ws, spark, setup_serverless_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(quarantine=True)

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    output_df = spark.table(run_config.output_config.location)
    assert output_df.count() > 0, "Output table is empty"

    quarantine_df = spark.table(run_config.quarantine_config.location)
    assert quarantine_df.count() > 0, "Quarantine table is empty"


def test_e2e_workflow_with_custom_install_folder(
    ws, spark, setup_workflows_with_custom_folder, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows_with_custom_folder()

    installation_ctx.deployed_workflows.run_workflow("e2e", run_config.name)

    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name,
        assume_user=True,
        product_name=installation_ctx.installation.product(),
        install_folder=installation_ctx.installation.install_folder(),
    )
    checks = DQEngine(ws, spark).load_checks(config=config)
    assert checks, "Checks were not loaded correctly"

    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "Output table is empty"
    assert checked_df.count() == input_df.count(), "Output table is empty"


def test_e2e_workflow_for_patterns(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows()

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')
    second_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    installation_ctx.deployed_workflows.run_workflow(
        "e2e", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    dq_engine = DQEngine(ws, spark)

    # assert checks for first table
    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml",
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {first_table} were not generated"

    # assert checks for second table
    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{second_table}.yml",
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {second_table} were not generated"

    checked_df = spark.table(first_table + "_dq_output")
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "First output table is empty"
    assert checked_df.count() == input_df.count(), "First output table is empty"

    checked_df = spark.table(second_table + "_dq_output")

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "Second output table is empty"
    assert checked_df.count() == input_df.count(), "Second output table is empty"


def test_e2e_workflow_for_patterns_exclude_patterns(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows()

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')
    exclude_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    installation_ctx.deployed_workflows.run_workflow(
        workflow="e2e",
        run_config_name=run_config.name,
        patterns=f"{catalog_name}.{schema_name}.*",
        exclude_patterns=exclude_table,
    )

    dq_engine = DQEngine(ws, spark)

    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml",
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {first_table} were not generated"

    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{exclude_table}.yml",
    )
    with pytest.raises(NotFound):
        engine = DQEngine(ws, spark)
        engine.load_checks(config=storage_config)

    checked_df = spark.table(first_table + "_dq_output")
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "First output table is empty"
    assert checked_df.count() == input_df.count(), "First output table is empty"

    # 1 input + 1 output + 1 existing
    tables = ws.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema_name)
    assert len(list(tables)) == 3, "Tables count mismatch"


def test_e2e_workflow_for_patterns_exclude_output(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(quarantine=True)

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')

    output_table_suffix = "_output"
    quarantine_table_suffix = "_quarantine"

    existing_output_table = make_second_input_table(
        spark, catalog_name, schema_name, first_table, make_random, output_table_suffix
    )
    existing_quarantine_table = make_second_input_table(
        spark, catalog_name, schema_name, first_table, make_random, quarantine_table_suffix
    )

    installation_ctx.deployed_workflows.run_workflow(
        workflow="e2e",
        run_config_name=run_config.name,
        patterns=f"{catalog_name}.{schema_name}.*",
        # existing output and quarantine tables are excluded by default based on suffixes
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    dq_engine = DQEngine(ws, spark)

    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml",
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {first_table} were not generated"

    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{existing_output_table}.yml",
    )
    with pytest.raises(NotFound):
        engine = DQEngine(ws, spark)
        engine.load_checks(config=storage_config)

    storage_config = WorkspaceFileChecksStorageConfig(
        location=f"{installation_ctx.installation.install_folder()}/checks/{existing_quarantine_table}.yml",
    )
    with pytest.raises(NotFound):
        engine = DQEngine(ws, spark)
        engine.load_checks(config=storage_config)

    output_df = spark.table(first_table + output_table_suffix)
    assert output_df.count() > 0, "Output table is empty"

    quarantine_df = spark.table(first_table + quarantine_table_suffix)
    assert quarantine_df.count() > 0, "Quarantine table is empty"

    # 1 input + 2 outputs (output, quarantine) + 2 existing
    tables = ws.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema_name)
    assert len(list(tables)) == 5, "Tables count mismatch"


def test_e2e_workflow_for_patterns_table_checks_storage(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows()

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')

    # update run config to use table storage for checks
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = f"{catalog_name}.{schema_name}.checks"
    installation_ctx.installation.save(config)

    second_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    installation_ctx.deployed_workflows.run_workflow(
        "e2e", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    dq_engine = DQEngine(ws, spark)

    # assert checks for first table
    storage_config = TableChecksStorageConfig(
        location=run_config.checks_location,
        run_config_name=first_table,
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {first_table} were not generated"

    # assert checks for second table
    storage_config = TableChecksStorageConfig(
        location=run_config.checks_location,
        run_config_name=second_table,
    )
    checks = dq_engine.load_checks(config=storage_config)
    assert checks, f"Checks for {second_table} were not generated"

    checked_df = spark.table(first_table + "_dq_output")
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "First output table is empty"
    assert checked_df.count() == input_df.count(), "First output table is empty"

    checked_df = spark.table(second_table + "_dq_output")

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() > 0, "Second output table is empty"
    assert checked_df.count() == input_df.count(), "Second output table is empty"


def make_second_input_table(spark, catalog_name, schema_name, first_table, make_random, suffix=""):
    second_table = f"{catalog_name}.{schema_name}.dummy_t{make_random(4).lower()}{suffix}"
    spark.table(first_table).write.format("delta").mode("overwrite").saveAsTable(second_table)
    return second_table
