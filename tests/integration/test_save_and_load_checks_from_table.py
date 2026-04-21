import json
import time
from dataclasses import dataclass
from decimal import Decimal

import pytest
from pyspark.testing.utils import assertDataFrameEqual

from databricks.labs.dqx.checks_storage import DataFrameConverter
from databricks.labs.dqx.rule_fingerprint import compute_rule_set_fingerprint_by_metadata
from databricks.labs.dqx.config import (
    TableChecksStorageConfig,
    InstallationChecksStorageConfig,
    BaseChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError, UnsafeSqlQueryError
from databricks.sdk.errors import NotFound


from tests.constants import TEST_CATALOG

# Legacy schema: no versioning columns (created_at, rule_fingerprint, rule_set_fingerprint)
TEST_CHECKS_TABLE_SCHEMA = (
    "name STRING, criticality STRING, check STRUCT<function STRING, for_each_column ARRAY<STRING>,"
    " arguments MAP<STRING, STRING>>, filter STRING, run_config_name STRING, user_metadata MAP<STRING, STRING>"
)


INPUT_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {"function": "is_not_less_than", "arguments": {"column": "col_2", "limit": 1.01}},
        "filter": "Col_3 >1",
        "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
    },
    {
        "criticality": "warn",
        "name": "column_in_list",
        "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
    },
    {
        "criticality": "warn",
        "name": "col_3_is_in_range",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "col_3", "min_limit": Decimal("0.01"), "max_limit": Decimal("999.99")},
        },
    },
]


# When loading from a table, missing name becomes None; all other fields are preserved.
EXPECTED_CHECKS_FROM_TABLE_LOAD = [{**check, "name": check.get("name")} for check in INPUT_CHECKS]


def test_load_checks_when_checks_table_does_not_exist(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    with pytest.raises(NotFound, match=f"Checks table '{table_name}' does not exist in the workspace"):
        engine = DQEngine(ws, spark)
        config = TableChecksStorageConfig(location=table_name)
        engine.load_checks(config=config)


def test_load_checks_with_special_characters_in_table_name(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"`{catalog_name}`.`{schema_name}`.`table-with-special_chars$#@{make_random(5)}`"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, run_config_name="default")
    engine.save_checks(INPUT_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD, "Checks were not loaded correctly."


def test_save_and_load_checks_from_table(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, run_config_name="default")
    engine.save_checks(checks=INPUT_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD, "Checks were not loaded correctly."


def test_save_checks_to_table_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, run_config_name="default")
    engine.save_checks(INPUT_CHECKS, config=config)
    checks_df = spark.read.table(table_name).select(
        "name", "criticality", "check", "filter", "run_config_name", "user_metadata"
    )

    expected_raw_checks = [
        {
            "name": None,
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "for_each_column": ["col1", "col2"],
                "arguments": {},
            },
            "filter": None,
            "run_config_name": "default",
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        },
        {
            "name": "column_not_less_than",
            "criticality": "warn",
            "check": {
                "function": "is_not_less_than",
                "for_each_column": [],
                "arguments": {"limit": "1.01", "column": "\"col_2\""},
            },
            "filter": "Col_3 >1",
            "run_config_name": "default",
            "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
        },
        {
            "name": "column_in_list",
            "criticality": "warn",
            "check": {
                "function": "is_in_list",
                "for_each_column": [],
                "arguments": {"column": '"col_2"', "allowed": '[1, 2]'},
            },
            "filter": None,
            "run_config_name": "default",
            "user_metadata": None,
        },
        {
            "name": "col_3_is_in_range",
            "criticality": "warn",
            "check": {
                "function": "is_in_range",
                "for_each_column": [],
                "arguments": {
                    "column": '"col_3"',
                    "min_limit": '{"__decimal__": "0.01"}',
                    "max_limit": '{"__decimal__": "999.99"}',
                },
            },
            "filter": None,
            "run_config_name": "default",
            "user_metadata": None,
        },
    ]

    expected_checks_df = spark.createDataFrame(expected_raw_checks, TEST_CHECKS_TABLE_SCHEMA)
    assertDataFrameEqual(checks_df.sort("name"), expected_checks_df.sort("name"))


def test_load_checks_from_table_saved_from_dict_with_unresolved_for_each_column(ws, make_schema, make_random, spark):
    """Load from table created with dict rows. Supports compact for_each_column format."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

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
    checks_df = spark.createDataFrame(input_checks, TEST_CHECKS_TABLE_SCHEMA)
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)  # only loading run_config_name = "default"
    loaded_checks = engine.load_checks(config=config)

    expected_checks = [
        {
            "name": "col1_is_null",
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
    """Load from table created with row format. Supports compact for_each_column format."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    # Row format: [name, criticality, check, filter, run_config_name, user_metadata]
    # check struct: {function, for_each_column, arguments} — compact format supported
    input_checks = [
        [
            "col1_is_null",
            "error",
            {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
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

    checks_df = spark.createDataFrame(input_checks, TEST_CHECKS_TABLE_SCHEMA)
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)  # only loading run_config_name = "default"
    loaded_checks = engine.load_checks(config=config)

    expected_checks = [
        {
            "name": "col1_is_null",
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
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    run_config_name = "workflow_001"
    config_save = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    engine.save_checks(INPUT_CHECKS[:1], config=config_save)
    config_load = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    checks = engine.load_checks(config=config_load)
    assert (
        checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[:1]
    ), f"Checks were not loaded correctly for {run_config_name} run config."

    # verify overwrite works for specific run config only
    run_config_name2 = "workflow_002"
    config_save2 = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name2, mode="overwrite")
    engine.save_checks(INPUT_CHECKS[1:], config=config_save2)
    config_load2 = TableChecksStorageConfig(location=table_name, run_config_name=run_config_name)
    checks = engine.load_checks(config=config_load2)
    assert (
        checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[:1]
    ), f"Checks were not loaded correctly for {run_config_name} run config."

    # use default run_config_name
    engine.save_checks(INPUT_CHECKS[1:], config=TableChecksStorageConfig(location=table_name))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:], "Checks were not loaded correctly for default run config."


def test_save_and_load_checks_to_table_output_modes(ws, make_schema, make_random, spark):
    """Append adds rows; overwrite replaces all rows for run_config when fingerprint differs."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    engine.save_checks(INPUT_CHECKS[:1], config=TableChecksStorageConfig(location=table_name, mode="append"))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[:1], "Checks were not loaded correctly after appending."

    engine.save_checks(INPUT_CHECKS[1:], config=TableChecksStorageConfig(location=table_name, mode="overwrite"))
    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:], "Checks were not loaded correctly after overwriting."


def test_save_append_accumulates_multiple_versions(ws, make_schema, make_random, spark):
    """Append twice with different fingerprints; load returns latest version."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, mode="append")

    engine.save_checks(INPUT_CHECKS[:1], config=config)
    time.sleep(1)  # ensures second append has later created_at
    engine.save_checks(INPUT_CHECKS[1:], config=config)

    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:], "Load must return latest version after append accumulates"


def test_save_checks_raises_unsafe_sql_query_error_when_run_config_name_contains_forbidden_sql(
    ws, make_schema, make_random, spark
):
    """Save with mode=overwrite rejects run_config_name that would produce unsafe SQL (e.g. DML/DDL)."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(
        location=table_name,
        run_config_name="default'; DROP TABLE x; --",
        mode="overwrite",
    )

    with pytest.raises(UnsafeSqlQueryError, match="run_config_name must not contain unsafe SQL"):
        engine.save_checks(INPUT_CHECKS[:1], config=config)


def test_save_load_checks_from_table_in_user_installation(ws, installation_ctx, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = table_name
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name, assume_user=True, product_name=product_name
    )
    dq_engine.save_checks(INPUT_CHECKS, config=config)

    checks = dq_engine.load_checks(config=config)
    assert EXPECTED_CHECKS_FROM_TABLE_LOAD == checks, "Checks were not saved correctly"


@dataclass
class ChecksDummyStorageConfig(BaseChecksStorageConfig):
    """Dummy storage config for testing unsupported storage type."""

    location: str = "test_location"


def test_load_checks_invalid_storage_config(ws, spark):
    engine = DQEngine(ws, spark)
    config = ChecksDummyStorageConfig()

    with pytest.raises(InvalidConfigError, match="Unsupported storage config type"):
        engine.load_checks(config=config)


def test_save_checks_invalid_storage_config(ws, spark):
    engine = DQEngine(ws, spark)
    config = ChecksDummyStorageConfig()

    with pytest.raises(InvalidConfigError, match="Unsupported storage config type"):
        engine.save_checks([{}], config=config)


def test_load_checks_from_table_with_new_schema(ws, make_schema, make_random, spark):
    """Load from table created with new schema (versioning columns populated)."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    checks_df = DataFrameConverter.to_dataframe(spark, INPUT_CHECKS, run_config_name="default")
    checks_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)
    loaded_checks = engine.load_checks(config=config)

    assert (
        loaded_checks == EXPECTED_CHECKS_FROM_TABLE_LOAD
    ), "Checks were not loaded correctly from table with new schema."


def test_load_checks_from_table_with_new_schema_and_rule_set_fingerprint(ws, make_schema, make_random, spark):
    """Load from table with new schema, filtering by rule_set_fingerprint."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    df_batch1 = DataFrameConverter.to_dataframe(spark, INPUT_CHECKS[:1], run_config_name="default")
    df_batch2 = DataFrameConverter.to_dataframe(spark, INPUT_CHECKS[1:], run_config_name="default")
    df_batch1.write.saveAsTable(table_name)
    df_batch2.write.mode("append").saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(
        location=table_name,
        rule_set_fingerprint=compute_rule_set_fingerprint_by_metadata(INPUT_CHECKS[:1]),
    )
    loaded_checks = engine.load_checks(config=config)

    assert (
        loaded_checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[:1]
    ), "Checks were not loaded correctly when filtering by rule_set_fingerprint."


def test_save_and_load_checks_from_delta_table_without_rule_set_fingerprint(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)
    engine.save_checks(INPUT_CHECKS[:1], config=config)

    engine.save_checks(INPUT_CHECKS[1:], config=config)

    checks = engine.load_checks(config=config)
    assert (
        checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:]
    ), f"Checks were not loaded correctly for {config.run_config_name} run config."


def test_save_and_load_checks_from_delta_table_with_rule_set_fingerprint(ws, make_schema, make_random, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config_save = TableChecksStorageConfig(location=table_name)
    engine.save_checks(INPUT_CHECKS[:1], config=config_save)

    engine.save_checks(INPUT_CHECKS[1:], config=config_save)

    config_load = TableChecksStorageConfig(
        location=table_name,
        rule_set_fingerprint=compute_rule_set_fingerprint_by_metadata(INPUT_CHECKS[:1]),
    )
    checks = engine.load_checks(config=config_load)

    assert (
        checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[0:1]
    ), f"Checks were not loaded correctly for {config_load.run_config_name} run config."


def test_save_and_load_checks_from_delta_table_rule_set_fingerprint_already_exists(ws, make_schema, make_random, spark):

    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name)
    engine.save_checks(INPUT_CHECKS[1:], config=config)
    engine.save_checks(INPUT_CHECKS[1:], config=config)
    checks = engine.load_checks(config=config)
    assert (
        checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:]
    ), f"Checks were not loaded correctly for {config.run_config_name} run config."


def test_save_checks_for_each_column_produces_deterministic_rule_set_fingerprint(ws, make_schema, make_random, spark):
    """A for_each_column check produces a deterministic rule_set_fingerprint (same when saved twice)."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    compact = [{"criticality": "error", "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"]}}]

    engine = DQEngine(ws, spark)
    engine.save_checks(compact, config=TableChecksStorageConfig(location=table_name))
    engine.save_checks(compact, config=TableChecksStorageConfig(location=table_name))  # idempotency: same fingerprint

    rule_set_fingerprint = spark.read.table(table_name).select("rule_set_fingerprint").first()["rule_set_fingerprint"]
    assert rule_set_fingerprint is not None
    assert spark.read.table(table_name).count() == 1  # one compact row, second save skipped


def test_save_to_legacy_delta_table_adds_versioning_columns(ws, make_schema, make_random, spark):
    """Saving to an existing legacy table (no versioning columns) triggers ALTER TABLE and succeeds."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    # Create table with legacy schema (no versioning columns)
    legacy_df = spark.createDataFrame(
        [
            [
                "a_is_null",
                "error",
                {"function": "is_not_null", "for_each_column": [], "arguments": {}},
                None,
                "default",
                None,
            ]
        ],
        TEST_CHECKS_TABLE_SCHEMA,
    )
    legacy_df.write.saveAsTable(table_name)

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, mode="append")
    engine.save_checks(INPUT_CHECKS[:1], config=config)

    # Verify versioning columns were added and data was written
    saved_df = spark.read.table(table_name)
    assert "rule_fingerprint" in saved_df.columns
    assert "rule_set_fingerprint" in saved_df.columns
    assert "created_at" in saved_df.columns
    # New rows (from save_checks) have fingerprints; legacy row has null fingerprints
    new_rows = saved_df.filter("rule_set_fingerprint IS NOT NULL")
    assert new_rows.count() == 1  # INPUT_CHECKS[:1] stored as 1 compact row (for_each_column preserved)


def test_save_idempotency_overwrite_mode(ws, make_schema, make_random, spark):
    """Same fingerprint: mode=overwrite is skipped (idempotency); no duplicate write."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    engine = DQEngine(ws, spark)
    config = TableChecksStorageConfig(location=table_name, mode="overwrite")
    engine.save_checks(INPUT_CHECKS[1:], config=config)
    # Second save with same checks and overwrite mode — idempotency guard must skip it
    engine.save_checks(INPUT_CHECKS[1:], config=config)

    checks = engine.load_checks(config=TableChecksStorageConfig(location=table_name))
    assert checks == EXPECTED_CHECKS_FROM_TABLE_LOAD[1:], "Idempotency guard must prevent duplicate overwrite"
