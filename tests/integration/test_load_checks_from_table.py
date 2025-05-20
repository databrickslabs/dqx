import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound


TEST_CHECKS = [
    {
        "name": "column_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"col_name": "col_1"}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"col_name": "col_1", "min_limit": "0"}},
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
    engine.save_checks_in_table(TEST_CHECKS, table_name)
    checks = engine.load_checks_from_table(table_name, spark)
    assert checks == TEST_CHECKS, "Checks were not loaded correctly"
