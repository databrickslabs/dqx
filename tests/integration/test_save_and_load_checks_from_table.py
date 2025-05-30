import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.sdk.errors import NotFound


INPUT_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1}},
    },
]

EXPECTED_CHECKS = [
    {
        "name": "col_col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
    },
    {
        "name": "col_col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1}},
    },
]


def test_load_checks_when_checks_table_does_not_exist(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    with pytest.raises(NotFound, match=f"Table {table_name} does not exist in the workspace"):
        engine = DQEngine(ws)
        engine.load_checks_from_table(table_name, spark)


def test_save_and_load_checks_from_table(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws)
    DQEngine.save_checks_in_table(INPUT_CHECKS, table_name)
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."


def test_save_checks_to_table_with_unresolved_for_each_column(make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    DQEngine.save_checks_in_table(INPUT_CHECKS, table_name)
    checks_df = spark.read.table(table_name)

    expected_checks = [
        {
            "name": "col_col1_is_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "column": "\"col1\"",
                },
            },
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "col_col2_is_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "column": "\"col2\"",
                },
            },
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"limit": "1", "column": "\"col_2\""}},
            "filter": None,
            "run_config_name": "default",
        },
    ]

    expected_checks_df = spark.createDataFrame(expected_checks, DQEngineCore.CHECKS_TABLE_SCHEMA)

    assert_df_equality(checks_df, expected_checks_df, ignore_nullable=True)


def test_load_checks_to_table_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_checks = [
        {
            "name": "col_col1_is_null",
            "criticality": "error",
            "check": {
                "for_each_column": ["col1", "col2"],
                "function": "is_not_null",
            },
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"limit": "1", "column": "\"col_2\""}},
            "filter": None,
            "run_config_name": "default",
        },
    ]
    checks_df = spark.createDataFrame(input_checks, DQEngineCore.CHECKS_TABLE_SCHEMA)
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws)
    loaded_checks = engine.load_checks_from_table(table_name, spark=spark)

    expected_checks = [
        {
            'name': 'col_col1_is_null',
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "for_each_column": None,
                "arguments": {"column": "col_2", "limit": 1},
            },
        },
    ]

    assert loaded_checks == expected_checks, "Checks were not loaded correctly"


def test_save_and_load_checks_from_table_with_run_config(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws)
    run_config_name = "workflow_001"
    DQEngine.save_checks_in_table(INPUT_CHECKS[:1], table_name, run_config_name=run_config_name)
    checks = engine.load_checks_from_table(table_name, run_config_name=run_config_name, spark=spark)
    assert checks == EXPECTED_CHECKS[:2], f"Checks were not loaded correctly for {run_config_name} run config."

    # verify overwrite works for specific run config only
    run_config_name2 = "workflow_002"
    DQEngine.save_checks_in_table(INPUT_CHECKS[1:], table_name, run_config_name=run_config_name2, mode="overwrite")
    checks = engine.load_checks_from_table(table_name, run_config_name=run_config_name, spark=spark)
    assert checks == EXPECTED_CHECKS[:2], f"Checks were not loaded correctly for {run_config_name} run config."

    # use default run_config_name
    DQEngine.save_checks_in_table(INPUT_CHECKS[1:], table_name)
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == EXPECTED_CHECKS[2:], "Checks were not loaded correctly for default run config."


def test_save_and_load_checks_to_table_output_modes(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws)
    engine.save_checks_in_table(INPUT_CHECKS[:1], table_name, mode="append")
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == EXPECTED_CHECKS[:2], "Checks were not loaded correctly after appending."

    engine.save_checks_in_table(INPUT_CHECKS[1:], table_name, mode="overwrite")
    checks = engine.load_checks_from_table(table_name, spark=spark)
    assert checks == EXPECTED_CHECKS[2:], "Checks were not loaded correctly after overwriting."


def test_save_load_checks_from_table_in_user_installation(ws, installation_ctx, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_table = table_name
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws)
    dq_engine.save_checks_in_installation(
        INPUT_CHECKS, method="table", run_config_name=run_config.name, assume_user=True, product_name=product_name
    )

    checks = dq_engine.load_checks_from_installation(
        method="table", run_config_name=run_config.name, assume_user=True, product_name=product_name, spark=spark
    )

    assert EXPECTED_CHECKS == checks, "Checks were not saved correctly"


def test_save_and_load_checks_from_table_in_user_installation_missing_configuration(ws, installation_ctx):
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_table = None
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws)
    match_condition = "Table name must be provided either as a parameter or through run configuration."

    with pytest.raises(ValueError, match=match_condition):
        dq_engine.save_checks_in_installation(
            INPUT_CHECKS, method="table", run_config_name=run_config.name, assume_user=True, product_name=product_name
        )

    with pytest.raises(ValueError, match=match_condition):
        dq_engine.load_checks_from_installation(
            method="table", run_config_name=run_config.name, assume_user=True, product_name=product_name
        )
