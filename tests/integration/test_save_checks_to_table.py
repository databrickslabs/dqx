from databricks.labs.dqx.engine import DQEngine


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


def test_save_checks_in_table(ws, installation_ctx):
    client = installation_ctx.workspace_client
    client.catalogs.create(name="labs")
    client.schemas.create(name="dqx", catalog_name="labs")

    dq_engine = DQEngine(ws)
    checks_table = installation_ctx.check_table
    dq_engine.save_checks_in_table(TEST_CHECKS, checks_table)
    checks = dq_engine.load_checks_from_table(checks_table)
    assert TEST_CHECKS == checks, "Checks were not saved correctly"
