import json
from dataclasses import dataclass

import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx.config import (
    TableChecksStorageConfig,
    InstallationChecksStorageConfig,
    BaseChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_serializer import CHECKS_TABLE_SCHEMA


INPUT_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1}},
        "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
    },
    {
        "criticality": "warn",
        "name": "column_in_list",
        "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
    },
]

EXPECTED_CHECKS = [
    {
        "name": "col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {
            "function": "is_not_less_than",
            "arguments": {"column": "col_2", "limit": 1},
        },
        "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
    },
    {
        "name": "column_in_list",
        "criticality": "warn",
        "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
    },
]


def test_load_checks_when_checks_table_does_not_exist(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    with pytest.raises(NotFound, match=f"Table {table_name} does not exist in the workspace"):
        engine = DQEngine(ws, spark)
        config = TableChecksStorageConfig(location=table_name)
        engine.load_checks(config=config)


def test_save_and_load_checks_from_table(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, run_config_name="default")
    engine.save_checks(checks=INPUT_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."


def test_save_checks_to_table_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, run_config_name="default")
    engine.save_checks(INPUT_CHECKS, config=config)
    checks_df = spark.read.table(table_name)

    expected_raw_checks = [
        {
            "name": "col1_is_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "column": "\"col1\"",
                },
            },
            "filter": None,
            "run_config_name": "default",
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        },
        {
            "name": "col2_is_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {
                    "column": "\"col2\"",
                },
            },
            "filter": None,
            "run_config_name": "default",
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"limit": "1", "column": "\"col_2\""}},
            "filter": None,
            "run_config_name": "default",
            "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": '"col_2"', "allowed": '[1, 2]'}},
            "filter": None,
            "run_config_name": "default",
            "user_metadata": None,
        },
    ]

    expected_checks_df = spark.createDataFrame(expected_raw_checks, CHECKS_TABLE_SCHEMA)

    assert_df_equality(checks_df, expected_checks_df, ignore_nullable=True)


def test_load_checks_from_table_saved_from_dict_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_checks = [
        {
            "name": "col1_is_null",
            "criticality": "error",
            "check": {
                "for_each_column": ["col1", "col2"],
                "function": "is_not_null",
            },
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "column_not_less_than_escaped",
            "criticality": "warn",
            # use json.dumps to escape string arguments (columns)
            "check": {"function": "is_not_less_than", "arguments": {"limit": "1", "column": json.dumps("col_2")}},
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {"function": "is_not_less_than", "arguments": {"limit": 2, "column": "col_2"}},
            "filter": "col1 > 0",
            "run_config_name": "default",
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "column_in_list_escaped",
            "criticality": "warn",
            # escape string arguments (columns and allowed)
            "check": {"function": "is_in_list", "arguments": {"column": "\"col_2\"", "allowed": "[1, 2]"}},
            "filter": None,
            "run_config_name": "default",
        },
        {
            "name": "check_to_skip",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "\"col_2\"", "allowed": [1, 2]}},
            "filter": None,
            "run_config_name": "non_default",
        },
    ]
    checks_df = spark.createDataFrame(input_checks, CHECKS_TABLE_SCHEMA)
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)  # only loading run_config_name = "default"
    loaded_checks = engine.load_checks(config=config)

    expected_checks = [
        {
            'name': 'col1_is_null',
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "name": "column_not_less_than_escaped",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "col_2", "limit": 1},
            },
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "col_2", "limit": 2},
            },
            "filter": "col1 > 0",
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "col_2", "allowed": [1, 2]},
            },
        },
        {
            "name": "column_in_list_escaped",
            "criticality": "warn",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "col_2", "allowed": [1, 2]},
            },
        },
    ]

    assert not engine.validate_checks(loaded_checks).has_errors
    assert loaded_checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_table_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    input_checks = [
        [
            "col1_is_null",
            "error",
            {"for_each_column": ["col1", "col2"], "function": "is_not_null"},
            None,
            "default",
            None,
        ],
        [
            "column_not_less_than_escaped",
            "warn",
            # use json.dumps to escape string arguments (columns)
            {"function": "is_not_less_than", "arguments": {"limit": "1", "column": "\"col_2\""}},
            None,
            "default",
            None,
        ],
        [
            "column_not_less_than",
            "warn",
            {"function": "is_not_less_than", "arguments": {"limit": 2, "column": "col_2"}},
            "col1 > 0",
            "default",
            None,
        ],
        [
            "column_in_list",
            "warn",
            {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
            None,
            "default",
            None,
        ],
        [
            "column_in_list_escaped",
            "warn",
            # escape string arguments (columns and allowed)
            {"function": "is_in_list", "arguments": {"column": "\"col_2\"", "allowed": "[1, 2]"}},
            None,
            "default",
            None,
        ],
        [
            "check_to_skip",
            "warn",
            {"function": "is_in_list", "arguments": {"column": "\"col_2\"", "allowed": [1, 2]}},
            None,
            "non_default",
            None,
        ],
    ]

    checks_df = spark.createDataFrame(input_checks, CHECKS_TABLE_SCHEMA)
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)  # only loading run_config_name = "default"
    loaded_checks = engine.load_checks(config=config)

    expected_checks = [
        {
            'name': 'col1_is_null',
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "name": "column_not_less_than_escaped",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "col_2", "limit": 1},
            },
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "col_2", "limit": 2},
            },
            "filter": "col1 > 0",
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "col_2", "allowed": [1, 2]},
            },
        },
        {
            "name": "column_in_list_escaped",
            "criticality": "warn",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "col_2", "allowed": [1, 2]},
            },
        },
    ]

    assert not engine.validate_checks(loaded_checks).has_errors
    assert loaded_checks == expected_checks, "Checks were not loaded correctly"


def test_save_and_load_checks_from_table_with_run_config(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws, spark)
    run_config_name = "workflow_001"
    config_save = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    engine.save_checks(INPUT_CHECKS[:1], config=config_save)
    config_load = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    checks = engine.load_checks(config=config_load)
    assert checks == EXPECTED_CHECKS[:2], f"Checks were not loaded correctly for {run_config_name} run config."

    # verify overwrite works for specific run config only
    run_config_name2 = "workflow_002"
    config_save2 = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name2, mode="overwrite")
    engine.save_checks(INPUT_CHECKS[1:], config=config_save2)
    config_load2 = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    checks = engine.load_checks(config=config_load2)
    assert checks == EXPECTED_CHECKS[:2], f"Checks were not loaded correctly for {run_config_name} run config."

    # use default run_config_name
    engine.save_checks(INPUT_CHECKS[1:], config=TableChecksStorageConfig(location=table_name))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS[2:], "Checks were not loaded correctly for default run config."


def test_save_and_load_checks_to_table_output_modes(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

    engine = DQEngine(ws, spark)
    engine.save_checks(INPUT_CHECKS[:1], config=TableChecksStorageConfig(location=table_name, mode="append"))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS[:2], "Checks were not loaded correctly after appending."

    engine.save_checks(INPUT_CHECKS[1:], config=TableChecksStorageConfig(location=table_name, mode="overwrite"))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
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

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name, assume_user=True, product_name=product_name
    )
    dq_engine.save_checks(INPUT_CHECKS, config=config)

    checks = dq_engine.load_checks(config=config)
    assert EXPECTED_CHECKS == checks, "Checks were not saved correctly"


@dataclass
class ChecksDummyStorageConfig(BaseChecksStorageConfig):
    """Dummy storage config for testing unsupported storage type."""


def test_load_checks_invalid_storage_config(ws, spark):
    engine = DQEngine(ws, spark)
    config = ChecksDummyStorageConfig()

    with pytest.raises(ValueError, match="Unsupported storage config type"):
        engine.load_checks(config=config)


def test_save_checks_invalid_storage_config(ws, spark):
    engine = DQEngine(ws, spark)
    config = ChecksDummyStorageConfig()

    with pytest.raises(ValueError, match="Unsupported storage config type"):
        engine.save_checks([{}], config=config)
