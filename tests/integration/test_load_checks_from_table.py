import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound


TEST_CHECKS = [
    {
        "name": "column_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"col_names": ["col1", "col2"]}},
    },
    {
        "name": "column_is_not_null_or_empty",
        "criticality": "warning",
        "check": {"function": "is_not_null_or_empty", "arguments": {"col_names": ["col_1"]}},
    },
]


def test_load_checks_when_checks_table_does_not_exist_in_workspace(ws, installation_ctx):
    client = installation_ctx.workspace_client
    client.catalogs.create(name="labs")
    client.schemas.create(name="dqx", catalog_name="labs")

    checks_table = installation_ctx.check_table
    with pytest.raises(NotFound, match=f"Table {checks_table} does not exist in the workspace"):
        engine = DQEngine(ws)
        engine.load_checks_from_table(checks_table)


def test_load_checks_from_table(ws, installation_ctx):
    client = installation_ctx.workspace_client
    client.catalogs.create(name="labs")
    client.schemas.create(name="dqx", catalog_name="labs")

    checks_table = installation_ctx.check_table
    engine = DQEngine(ws)
    engine.save_checks_in_table(TEST_CHECKS, checks_table)
    checks = engine.load_checks_from_table(checks_table)
    assert checks == TEST_CHECKS, "Checks were not loaded correctly"


def test_load_checks_from_table_with_query(ws, installation_ctx):
    client = installation_ctx.workspace_client
    client.catalogs.create(name="labs")
    client.schemas.create(name="dqx", catalog_name="labs")

    checks_table = installation_ctx.check_table
    engine = DQEngine(ws)
    engine.save_checks_in_table(TEST_CHECKS, checks_table)
    checks = engine.load_checks_from_table(checks_table, "criticality <> 'warning'")
    assert checks == [c for c in TEST_CHECKS if c["criticality"] != "warning"], "Checks were not loaded correctly"
