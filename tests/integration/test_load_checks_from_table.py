import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound


TEST_CHECKS = [
    {
        "name": "column_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col_1"}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_1", "limit": "0"}},
    },
]


def test_load_checks_when_checks_table_does_not_exist(installation_ctx, make_schema, make_random, spark):
    client = installation_ctx.workspace_client
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    with pytest.raises(NotFound, match=f"Table {table_name} does not exist in the workspace"):
        engine = DQEngine(client)
        engine.load_checks_from_table(table_name, spark)


def test_load_checks_from_table(installation_ctx, make_schema, make_random, spark):
    client = installation_ctx.workspace_client
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(client)
    DQEngine.save_checks_in_table(TEST_CHECKS, table_name)
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == TEST_CHECKS, "Checks were not loaded correctly."


def test_load_checks_from_table_with_run_config(installation_ctx, make_schema, make_random, spark):
    client = installation_ctx.workspace_client
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(client)
    run_config_name = "workflow_001"
    DQEngine.save_checks_in_table(TEST_CHECKS[:1], table_name, run_config_name=run_config_name)
    checks = engine.load_checks_from_table(table_name, run_config_name=run_config_name, spark=spark)
    assert checks == TEST_CHECKS[:1], "Checks were not loaded correctly for workflow run config."

    DQEngine.save_checks_in_table(TEST_CHECKS[1:], table_name)
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == TEST_CHECKS[1:], "Checks were not loaded correctly for default run config."


def test_save_checks_to_table_output_modes(installation_ctx, make_schema, make_random, spark):
    client = installation_ctx.workspace_client
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(client)
    engine.save_checks_in_table(TEST_CHECKS[:1], table_name, mode="append")
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == TEST_CHECKS[:1], "Checks were not loaded correctly after appending."

    engine.save_checks_in_table(TEST_CHECKS[1:], table_name, mode="overwrite")
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == TEST_CHECKS[1:], "Checks were not loaded correctly after overwriting."
