import copy
from io import BytesIO

import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.blueprint.parallel import ManyError
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.dqx.checks_storage import InstallationChecksStorageHandler
from databricks.labs.dqx.config import (
    InstallationChecksStorageConfig,
    InputConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from tests.integration.conftest import RUN_TIME, REPORTING_COLUMNS


def test_quality_checker_workflow(ws, spark, setup_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows(checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_for_multiple_run_configs(
    ws, spark, setup_workflows, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows(checks=True)

    second_run_config = copy.deepcopy(run_config)
    second_run_config.name = "second"
    # use the same checks but different output location
    second_run_config.output_config.location = run_config.output_config.location + "_second"
    installation_ctx.config.run_configs.append(second_run_config)

    # overwrite config in the installation folder
    installation_ctx.installation.save(installation_ctx.config)

    # run workflow
    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config_name="")

    # assert results
    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    checked_df = spark.table(second_run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_for_multiple_run_configs_table_checks_storage(
    ws, spark, setup_workflows, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows(checks=True)

    input_table = run_config.input_config.location
    catalog_name, schema_name, _ = input_table.split('.')

    # update run config to use table storage for checks
    checks_table = f"{catalog_name}.{schema_name}.checks"
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = checks_table

    second_run_config = copy.deepcopy(run_config)
    second_run_config.name = "second"
    # use the same checks but different output location
    second_run_config.output_config.location = run_config.output_config.location + "_second"
    installation_ctx.config.run_configs.append(second_run_config)

    # overwrite config in the installation folder
    installation_ctx.installation.save(installation_ctx.config)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=run_config.name), checks=checks
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    # run workflow
    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config_name="")

    # assert results
    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    checked_df = spark.table(second_run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_serverless(ws, spark, setup_serverless_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_table_checks_storage(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(checks=True)

    input_table = run_config.input_config.location
    catalog_name, schema_name, _ = input_table.split('.')

    # update run config to use table storage for checks
    checks_table = f"{catalog_name}.{schema_name}.checks"
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = checks_table
    installation_ctx.installation.save(config)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=run_config.name), checks=checks
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_with_custom_install_folder(
    ws, spark, setup_workflows_with_custom_folder, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows_with_custom_folder(checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_streaming(ws, spark, setup_serverless_workflows, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(checks=True, is_streaming=True)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_with_quarantine(
    ws, spark, setup_serverless_workflows, expected_quality_checking_output
):
    installation_ctx, run_config = setup_serverless_workflows(quarantine=True, checks=True)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    dq_engine = DQEngine(ws, spark)
    expected_output_df = dq_engine.get_valid(expected_quality_checking_output)
    expected_quarantine_df = dq_engine.get_invalid(expected_quality_checking_output)

    output_df = spark.table(run_config.output_config.location)
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

    quarantine_df = spark.table(run_config.quarantine_config.location)
    assert_df_equality(quarantine_df, expected_quarantine_df, ignore_nullable=True)


def test_quality_checker_workflow_when_missing_checks_file(ws, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    with pytest.raises(ManyError) as failure:
        installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    assert f"Checks file {checks_location} missing" in str(failure.value)


def test_quality_checker_workflow_with_custom_check_func(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    installation_dir = installation_ctx.installation.install_folder()
    custom_checks_funcs_location = f"/Workspace{installation_dir}/custom_check_funcs.py"

    _test_quality_checker_with_custom_check_func(ws, spark, installation_ctx, run_config, custom_checks_funcs_location)


def test_quality_checker_workflow_with_custom_check_func_in_volume(
    ws, spark, setup_serverless_workflows, make_schema, make_volume
):
    installation_ctx, run_config = setup_serverless_workflows()

    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    custom_checks_funcs_location = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/custom_check_funcs.py"

    _test_quality_checker_with_custom_check_func(ws, spark, installation_ctx, run_config, custom_checks_funcs_location)


def test_quality_checker_workflow_with_custom_check_func_rel_path(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()
    custom_checks_funcs_location = "custom_check_funcs.py"  # path relative to the installation folder
    _test_quality_checker_with_custom_check_func(ws, spark, installation_ctx, run_config, custom_checks_funcs_location)


def _test_quality_checker_with_custom_check_func(ws, spark, installation_ctx, run_config, custom_checks_funcs_location):
    installation_dir = installation_ctx.installation.install_folder()
    checks_location = f"{installation_dir}/{run_config.checks_location}"

    _setup_custom_checks(ws, spark, checks_location, installation_ctx.product_info.product_name())
    _setup_custom_check_func(ws, installation_ctx, custom_checks_funcs_location)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked = spark.table(run_config.output_config.location)

    expected = spark.createDataFrame(
        [
            [1, "a", None, None],
            [2, "b", None, None],
            [3, None, None, None],
            [
                None,
                "c",
                [
                    {
                        "name": "id_is_not_null_built_in",
                        "message": "Column 'id' value is null",
                        "columns": ["id"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "id_is_not_null_custom",
                        "message": "Column 'id' value is null",
                        "columns": ["id"],
                        "filter": None,
                        "function": "is_not_null_custom_func",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [3, None, None, None],
            [1, "a", None, None],
            [6, "a", None, None],
            [2, "c", None, None],
            [4, "a", None, None],
            [5, "d", None, None],
        ],
        f"id int, name string {REPORTING_COLUMNS}",
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_quality_checker_workflow_with_ref(
    ws, spark, setup_serverless_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_serverless_workflows()

    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    _setup_checks_with_ref(ws, spark, checks_location, installation_ctx.product_info.product_name())
    _setup_ref_table(spark, installation_ctx, make_random, run_config)

    installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config.name)

    checked = spark.table(run_config.output_config.location)
    expected = spark.createDataFrame(
        [
            [1, "a", None, None],
            [2, "b", None, None],
            [3, None, None, None],
            [None, "c", None, None],
            [3, None, None, None],
            [1, "a", None, None],
            [
                6,
                "a",
                [
                    {
                        "name": "id_missing_foreign_key",
                        "message": "Value '6' in column 'id' not found in reference column 'id'",
                        "columns": ["id"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [2, "c", None, None],
            [4, "a", None, None],
            [
                5,
                "d",
                [
                    {
                        "name": "id_missing_foreign_key",
                        "message": "Value '5' in column 'id' not found in reference column 'id'",
                        "columns": ["id"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        f"id int, name string {REPORTING_COLUMNS}",
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_quality_checker_workflow_for_patterns(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(checks=True)

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')
    second_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        checks=checks,
        config=WorkspaceFileChecksStorageConfig(
            location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml"
        ),
    )
    dq_engine.save_checks(
        checks=checks,
        config=WorkspaceFileChecksStorageConfig(
            location=f"{installation_ctx.installation.install_folder()}/checks/{second_table}.yml"
        ),
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    # run workflow
    installation_ctx.deployed_workflows.run_workflow(
        "quality-checker", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    # assert first table
    checked_df = spark.table(first_table + "_dq_output")
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    # assert second table
    checked_df = spark.table(second_table + "_dq_output")
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def test_quality_checker_workflow_for_patterns_exclude_patterns(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(checks=True)

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')
    exclude_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        checks=checks,
        config=WorkspaceFileChecksStorageConfig(
            location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml"
        ),
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    # run workflow
    installation_ctx.deployed_workflows.run_workflow(
        workflow="quality-checker",
        run_config_name=run_config.name,
        patterns=f"{catalog_name}.{schema_name}.*",
        exclude_patterns=exclude_table,
    )

    checked_df = spark.table(first_table + "_dq_output")
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    # 1 input + 1 output + 1 existing
    tables = ws.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema_name)
    assert len(list(tables)) == 3, "Tables count mismatch"


def test_quality_checker_workflow_for_patterns_exclude_output(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(quarantine=True, checks=True)

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        checks=checks,
        config=WorkspaceFileChecksStorageConfig(
            location=f"{installation_ctx.installation.install_folder()}/checks/{first_table}.yml"
        ),
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    output_table_suffix = "_output"
    quarantine_table_suffix = "_quarantine"

    # run workflow
    installation_ctx.deployed_workflows.run_workflow(
        workflow="quality-checker",
        run_config_name=run_config.name,
        patterns=f"{catalog_name}.{schema_name}.*",
        # existing output and quarantine tables are excluded by default based on suffixes
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    # run workflow again to verify that existing output and quarantine tables are excluded
    installation_ctx.deployed_workflows.run_workflow(
        workflow="quality-checker",
        run_config_name=run_config.name,
        patterns=f"{catalog_name}.{schema_name}.*",
        # existing output and quarantine tables are excluded by default based on suffixes
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    expected_output_df = dq_engine.get_valid(expected_quality_checking_output)
    expected_quarantine_df = dq_engine.get_invalid(expected_quality_checking_output)

    output_df = spark.table(first_table + output_table_suffix)
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

    quarantine_df = spark.table(first_table + quarantine_table_suffix)
    assert_df_equality(quarantine_df, expected_quarantine_df, ignore_nullable=True)

    # 1 input + 2 outputs (output, quarantine)
    tables = ws.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema_name)
    assert len(list(tables)) == 3, "Tables count mismatch"


def test_quality_checker_workflow_for_patterns_table_checks_storage(
    ws, spark, make_table, setup_workflows, expected_quality_checking_output, make_random
):
    installation_ctx, run_config = setup_workflows(checks=True)

    first_table = run_config.input_config.location
    catalog_name, schema_name, _ = first_table.split('.')

    # update run config to use table storage for checks
    checks_table = f"{catalog_name}.{schema_name}.checks"
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = checks_table
    installation_ctx.installation.save(config)

    second_table = make_second_input_table(spark, catalog_name, schema_name, first_table, make_random)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=WorkspaceFileChecksStorageConfig(location=f"{installation_ctx.installation.install_folder()}/checks.yml")
    )
    dq_engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=first_table), checks=checks
    )
    dq_engine.save_checks(
        config=TableChecksStorageConfig(location=checks_table, run_config_name=second_table), checks=checks
    )
    ws.workspace.delete(f"{installation_ctx.installation.install_folder()}/checks.yml")

    # run workflow
    installation_ctx.deployed_workflows.run_workflow(
        "quality-checker", run_config_name=run_config.name, patterns=f"{catalog_name}.{schema_name}.*"
    )

    # assert first table
    checked_df = spark.table(first_table + "_dq_output")
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    # assert second table
    checked_df = spark.table(second_table + "_dq_output")
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)


def make_second_input_table(spark, catalog_name, schema_name, first_table, make_random, suffix=""):
    second_table = f"{catalog_name}.{schema_name}.dummy_t{make_random(4).lower()}{suffix}"
    spark.table(first_table).write.format("delta").mode("overwrite").saveAsTable(second_table)
    return second_table


def _setup_custom_check_func(ws, installation_ctx, custom_checks_funcs_location):
    content = '''from databricks.labs.dqx.check_funcs import make_condition, register_rule
from pyspark.sql import functions as F

@register_rule("row")
def is_not_null_custom_func(column: str):
    return make_condition(F.col(column).isNull(), f"Column '{column}' value is null", f"{column}_is_null")
'''
    if custom_checks_funcs_location.startswith("/Workspace/"):
        ws.workspace.upload(
            path=custom_checks_funcs_location, format=ImportFormat.AUTO, content=content.encode(), overwrite=True
        )
    elif custom_checks_funcs_location.startswith("/Volumes/"):
        binary_data = BytesIO(content.encode("utf-8"))
        ws.files.upload(custom_checks_funcs_location, binary_data, overwrite=True)
    else:  # relative workspace path
        installation_dir = installation_ctx.installation.install_folder()
        ws.workspace.upload(
            path=f"{installation_dir}/{custom_checks_funcs_location}",
            format=ImportFormat.AUTO,
            content=content.encode(),
            overwrite=True,
        )

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.custom_check_functions = {"is_not_null_custom_func": custom_checks_funcs_location}
    installation_ctx.installation.save(config)


def _setup_ref_table(spark, installation_ctx, make_random, run_config):
    schema_and_catalog = run_config.input_config.location.split(".")
    catalog, schema = schema_and_catalog[0], schema_and_catalog[1]
    ref_table = f"{catalog}.{schema}.{make_random(10).lower()}"

    spark.createDataFrame(
        [
            [1],
            [2],
            [3],
            [4],
        ],
        schema="id: int",
    ).write.format(
        "delta"
    ).saveAsTable(ref_table)

    # update run config with reference table
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.reference_tables = {"ref_df": InputConfig(location=ref_table)}
    installation_ctx.installation.save(config)


def _setup_checks_with_ref(ws, spark, checks_location, product):
    checks = [
        {
            "name": "id_missing_foreign_key",
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["id"], "ref_columns": ["id"], "ref_df_name": "ref_df"},
            },
        },
    ]

    config = InstallationChecksStorageConfig(
        location=checks_location,
        product_name=product,
    )

    InstallationChecksStorageHandler(ws, spark).save(checks=checks, config=config)


def _setup_custom_checks(ws, spark, checks_location, product):
    checks = [
        {
            "name": "id_is_not_null_built_in",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "id_is_not_null_custom",
            "criticality": "error",
            "check": {"function": "is_not_null_custom_func", "arguments": {"column": "id"}},
        },
    ]

    config = InstallationChecksStorageConfig(
        location=checks_location,
        product_name=product,
    )

    InstallationChecksStorageHandler(ws, spark).save(checks=checks, config=config)
