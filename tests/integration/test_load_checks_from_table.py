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
        "criticality": "warning",
        "check": {"function": "is_not_less_than", "arguments": {"col_name": "col_1", "min_limit": "0"}},
    },
]


def test_load_checks_when_checks_table_does_not_exist_in_workspace(ws, installation_ctx):
    client = installation_ctx.workspace_client
    table_name = installation_ctx.check_table

    with pytest.raises(NotFound, match=f"Table {table_name} does not exist in the workspace"):
        engine = DQEngine(client)
        engine.load_checks_from_table(table_name)


def test_load_checks_from_table(installation_ctx):
    client = installation_ctx.workspace_client
    table_name = installation_ctx.check_table
    engine = DQEngine(client)
    engine.save_checks_in_table(TEST_CHECKS, table_name)
    checks = engine.load_checks_from_table(table_name)
    assert checks == TEST_CHECKS, "Checks were not loaded correctly"


def test_load_checks_from_table_with_query(installation_ctx):
    client = installation_ctx.workspace_client
    table_name = installation_ctx.check_table
    engine = DQEngine(client)
    engine.save_checks_in_table(TEST_CHECKS, table_name)
    checks = engine.load_checks_from_table(table_name, query="criticality <> 'warning'")
    assert checks == [c for c in TEST_CHECKS if c["criticality"] != "warning"], "Checks were not loaded correctly"
