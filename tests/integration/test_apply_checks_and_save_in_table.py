import tempfile
import pytest
from pyspark.sql.functions import col, lit, when
from pyspark.sql import Column
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.sdk.errors import NotFound
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import (
    InputConfig,
    OutputConfig,
    RunConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from tests.integration.conftest import EXTRA_PARAMS, RUN_TIME, RUN_ID, REPORTING_COLUMNS

from tests.conftest import TEST_CATALOG


def test_apply_checks_and_save_in_single_table(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "error"], [4, None, "warn"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
        DQRowRule(
            name="b_is_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
        ),
    ]

    # Apply checks and write to table (no quarantine table)
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    # Verify the table was created and contains the expected data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, "valid", None, None],
            [
                None,
                3,
                "error",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                4,
                None,
                "warn",
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_single_table_with_quarantine(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, 5, "good"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    # Apply checks, split, and write to tables (with quarantine table)
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite", options={"overwriteSchema": "true"}),
        quarantine_config=OutputConfig(
            location=quarantine_table, mode="overwrite", options={"overwriteSchema": "true"}
        ),
    )

    # Verify the tables were created and contain the expected data
    actual_validated_df = spark.table(output_table)
    actual_quarantine_df = spark.table(quarantine_table)
    expected_validated_df = spark.createDataFrame([[1, 2, "valid"], [4, 5, "good"]], schema=test_schema)
    quarantine_schema = test_schema + REPORTING_COLUMNS
    expected_quarantine_df = spark.createDataFrame(
        [
            [
                None,
                3,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ]
        ],
        schema=quarantine_schema,
    )

    assert_df_equality(actual_validated_df, expected_validated_df, ignore_nullable=True)
    assert_df_equality(actual_quarantine_df, expected_quarantine_df, ignore_nullable=True)


def test_apply_checks_by_metadata_and_save_in_single_table(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "error"], [4, None, "warn"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create metadata checks
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        },
    ]

    # Apply checks and write to table (no quarantine table)
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_by_metadata_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    # Verify the table was created and contains the expected data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, "valid", None, None],
            [
                None,
                3,
                "error",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                4,
                None,
                "warn",
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_by_metadata_and_save_in_single_table_with_quarantine(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, 5, "good"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create metadata checks
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
    ]

    # Apply checks, split, and write to tables (with quarantine table)
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_by_metadata_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite", options={"overwriteSchema": "true"}),
        quarantine_config=OutputConfig(
            location=quarantine_table, mode="overwrite", options={"overwriteSchema": "true"}
        ),
    )

    # Verify the tables were created  and contain the expected data
    actual_validated_df = spark.table(output_table)
    actual_quarantine_df = spark.table(quarantine_table)
    expected_validated_df = spark.createDataFrame([[1, 2, "valid"], [4, 5, "good"]], schema=test_schema)
    quarantine_schema = test_schema + REPORTING_COLUMNS
    expected_quarantine_df = spark.createDataFrame(
        [
            [
                None,
                3,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ]
        ],
        schema=quarantine_schema,
    )

    assert_df_equality(actual_validated_df, expected_validated_df, ignore_nullable=True)
    assert_df_equality(actual_quarantine_df, expected_quarantine_df, ignore_nullable=True)


def test_apply_checks_and_save_in_table_with_options(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [3, 4]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_positive",
            criticality="warn",
            check_func=check_funcs.is_not_less_than,
            column="a",
            check_func_kwargs={"limit": 0},
        ),
    ]

    # Apply checks and write to table with custom options
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite", options={"overwriteSchema": "true"}),
    )

    # Verify the table was created and contains the expected data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, None, None],
            [3, 4, None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    # Add more data with different schema to test schema evolution
    new_test_schema = "a: int, b: int, d: string"
    new_test_df = spark.createDataFrame([[5, 6, "new"]], new_test_schema)
    new_input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    new_test_df.write.format("delta").mode("overwrite").saveAsTable(new_input_table)

    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=new_input_table),
        output_config=OutputConfig(location=output_table, mode="append", options={"mergeSchema": "true"}),
    )

    # Verify schema was merged
    actual_df = spark.table(output_table).orderBy(["a"])
    expected_schema = new_test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, None, None, None],
            [3, 4, None, None, None],
            [5, 6, "new", None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_apply_checks_and_save_in_table_with_different_modes(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [None, 4]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    # First write with overwrite mode
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    # Verify initial data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, None, None],
            [
                None,
                4,
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    # Second write with append mode
    new_test_df = spark.createDataFrame([[None, 4], [5, 6]], test_schema)
    new_input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    new_test_df.write.format("delta").mode("overwrite").saveAsTable(new_input_table)

    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=new_input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    # Verify appended data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [
                None,
                4,
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [5, 6, None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_by_metadata_and_save_in_table_with_custom_functions(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Define custom check function
    def custom_string_check(column: str) -> Column:
        """Custom check function for testing."""
        return when(col(column).contains("custom"), lit("Contains custom text")).otherwise(lit(None))

    # Create metadata checks with custom function
    checks = [
        {
            "name": "custom_string_check",
            "criticality": "warn",
            "check": {"function": "custom_string_check", "arguments": {"column": "b"}},
        },
    ]

    # Apply checks with custom functions
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_by_metadata_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        custom_check_functions={"custom_string_check": custom_string_check},
    )

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [
                2,
                "custom",
                None,
                [
                    {
                        "name": "custom_string_check",
                        "message": "Contains custom text",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_string_check",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_table_with_custom_functions(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Define custom check function
    def custom_string_check(column: str) -> Column:
        """Custom check function for testing."""
        return when(col(column).contains("custom"), lit("Contains custom text")).otherwise(lit(None))

    # Create metadata checks with custom function
    checks = [
        DQRowRule(
            name="custom_string_check",
            criticality="warn",
            check_func=custom_string_check,
            column="b",
        ),
    ]

    # Apply checks with custom functions
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
    )

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [
                2,
                "custom",
                None,
                [
                    {
                        "name": "custom_string_check",
                        "message": "Contains custom text",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_string_check",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_table_with_ref_df(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    ref_dfs = {"ref_df_key": test_df}

    # Create checks
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df_key",
            },
        ),
    ]

    # Apply checks
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        ref_dfs=ref_dfs,
    )

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [2, "custom", None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_by_metadata_and_save_in_table_with_ref_df(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    ref_dfs = {"ref_df_key": test_df}

    # Create checks
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["a"], "ref_columns": ["a"], "ref_df_name": "ref_df_key"},
            },
        },
    ]

    # Apply checks
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_by_metadata_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table),
        output_config=OutputConfig(location=output_table, mode="overwrite"),
        ref_dfs=ref_dfs,
    )

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [2, "custom", None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_table_streaming_write(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)
    checkpoint_location = f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(8).lower()}"

    # Create source table for streaming
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [None, 4]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    # Apply checks and write streaming DataFrame
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_table(
        checks=checks,
        input_config=InputConfig(location=input_table, is_streaming=True),
        output_config=OutputConfig(
            location=output_table, options={"checkPointLocation": checkpoint_location}, trigger={"availableNow": True}
        ),
    )

    # Verify the table was created with streaming data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, None, None],
            [
                None,
                4,
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "error"], [4, None, "warn"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        },
    ]

    # Save the checks to a workspace file:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    # should work for any storage type, using workspace path as an example
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    # Configure table checks
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
            checks_location=checks_location,
        )
    ]

    # Apply checks and write to table
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=1)

    # Verify the table was created and contains the expected data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, "valid", None, None],
            [
                None,
                3,
                "error",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                4,
                None,
                "warn",
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_streaming_write(
    ws, spark, make_schema, make_random, make_directory, make_volume
):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)
    checkpoint_location = f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(8).lower()}"

    # Create test data and save to source table
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "error"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        },
    ]

    # Save the checks to a workspace file:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    # should work for any storage type, using workspace path as an example
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    # Configure table checks
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_table, is_streaming=True),
            output_config=OutputConfig(
                location=output_table,
                options={"checkPointLocation": checkpoint_location},
                trigger={"availableNow": True},
            ),
            checks_location=checks_location,
        )
    ]

    # Apply checks and write to table
    engine.apply_checks_and_save_in_tables(run_configs=run_configs)

    # Verify the table was created and contains the expected data
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, 2, "valid", None, None],
            [
                None,
                3,
                "error",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_multiple_tables(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create multiple input and output tables
    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]

    # Create test data for both tables
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, None]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create different checks for each table
    table1_checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]
    table2_checks = [
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location1 = f"{workspace_folder}/{input_tables[0]}.yml"
    checks_location2 = f"{workspace_folder}/{input_tables[1]}.yml"
    engine.save_checks(table1_checks, config=WorkspaceFileChecksStorageConfig(location=checks_location1))
    engine.save_checks(table2_checks, config=WorkspaceFileChecksStorageConfig(location=checks_location2))

    # Configure multiple table checks
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_tables[0]),
            output_config=OutputConfig(location=output_tables[0], mode="overwrite"),
            checks_location=checks_location1,
        ),
        RunConfig(
            input_config=InputConfig(location=input_tables[1]),
            output_config=OutputConfig(location=output_tables[1], mode="overwrite"),
            checks_location=checks_location2,
        ),
    ]

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=2)

    # Verify both tables were created and contain the expected data
    actual_df1 = spark.table(output_tables[0])
    actual_df2 = spark.table(output_tables[1])

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df1 = spark.createDataFrame(
        [
            [1, "valid", None, None],
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    expected_df2 = spark.createDataFrame(
        [
            [100, "test", None, None],
            [
                200,
                None,
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(actual_df1, expected_df1, ignore_nullable=True)
    assert_df_equality(actual_df2, expected_df2, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_with_quarantine(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    quarantine_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]

    # Create test data
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"], [3, "good"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, "check"]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create checks
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    for input_table in input_tables:
        engine.save_checks(
            checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_table}.yml")
        )

    # Configure mixed table setups (one with quarantine, one without)
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_tables[0]),
            output_config=OutputConfig(location=output_tables[0], mode="overwrite"),
            quarantine_config=OutputConfig(location=quarantine_tables[0], mode="overwrite"),
            checks_location=f"{workspace_folder}/{input_tables[0]}.yml",
        ),
        RunConfig(
            input_config=InputConfig(location=input_tables[1]),
            output_config=OutputConfig(location=output_tables[1], mode="overwrite"),
            # Skip the `quarantine_config` for this input table
            checks_location=f"{workspace_folder}/{input_tables[1]}.yml",
        ),
    ]

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=2)

    # Verify first table was split correctly
    actual_validated_dfs = [spark.table(output_table) for output_table in output_tables]
    actual_quarantine_df1 = spark.table(quarantine_tables[0])
    expected_validated_df1 = spark.createDataFrame([[1, "valid"], [3, "good"]], schema=test_schema)
    expected_quarantine_df1 = spark.createDataFrame(
        [
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ]
        ],
        schema=test_schema + REPORTING_COLUMNS,
    )

    # Verify second table includes all records with reporting columns
    expected_df2 = spark.createDataFrame(
        [
            [100, "test", None, None],
            [200, "check", None, None],
        ],
        schema=test_schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual_validated_dfs[0], expected_validated_df1, ignore_nullable=True)
    assert_df_equality(actual_validated_dfs[1], expected_df2, ignore_nullable=True)
    assert_df_equality(actual_quarantine_df1, expected_quarantine_df1, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_custom_parallelism(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    # Create 4 tables to test parallelism
    run_configs = []
    input_tables = []
    output_tables = []

    for i in range(4):
        input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
        output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

        input_tables.append(input_table)
        output_tables.append(output_table)

        # Create test data
        test_schema = "a: int, b: string"
        test_df = spark.createDataFrame([[i, f"test_{i}"], [i + 10, f"data_{i}"]], test_schema)
        test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

        # Create simple checks
        checks = [
            {
                "name": "a_is_positive",
                "criticality": "warn",
                "check": {"function": "is_not_less_than", "arguments": {"column": "a", "limit": 0}},
            },
        ]

        # Save the checks to workspace files:
        workspace_folder = str(make_directory().absolute())
        for input_table in input_tables:
            engine.save_checks(
                checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_table}.yml")
            )

        run_configs.append(
            RunConfig(
                input_config=InputConfig(location=input_table),
                output_config=OutputConfig(location=output_table, mode="overwrite"),
                checks_location=f"{workspace_folder}/{input_table}.yml",
            )
        )

    # Apply checks with limited parallelism
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=2)

    # Verify all tables were processed correctly
    for i, output_table in enumerate(output_tables):
        actual_df = spark.table(output_table)
        test_schema = "a: int, b: string"
        expected_schema = test_schema + REPORTING_COLUMNS
        expected_df = spark.createDataFrame(
            [
                [i, f"test_{i}", None, None],
                [i + 10, f"data_{i}", None, None],
            ],
            schema=expected_schema,
        )
        assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_empty_configs(ws, spark, make_schema, make_random):
    # Test with empty list of table configs
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    # This should not raise an error
    engine.apply_checks_and_save_in_tables(run_configs=[])


def test_apply_checks_and_save_in_tables_missing_input_config(ws, spark):
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    run_config = RunConfig()

    with pytest.raises(InvalidConfigError, match="Input configuration not provided"):
        engine.apply_checks_and_save_in_tables(run_configs=[run_config])


def test_apply_checks_and_save_in_tables_missing_output_config(ws, spark):
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    run_config = RunConfig(input_config=InputConfig(location="some_table"))

    with pytest.raises(InvalidConfigError, match="Output configuration not provided"):
        engine.apply_checks_and_save_in_tables(run_configs=[run_config])


def test_apply_checks_and_save_in_tables_with_custom_functions(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create metadata checks with custom function
    checks = [
        {
            "name": "custom_string_check",
            "criticality": "warn",
            "check": {"function": "custom_string_check", "arguments": {"column": "b"}},
        },
    ]

    # Save the checks to a workspace file:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    custom_function_content = '''
from databricks.labs.dqx.check_funcs import make_condition, register_rule
from pyspark.sql import functions as F
from pyspark.sql import Column

@register_rule("row")
def custom_string_check(column: str) -> Column:
    return F.when(F.col(column).contains("custom"), F.lit("Contains custom text")).otherwise(F.lit(None))
    '''

    # Save the custom function content to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as temp_file:
        temp_file.write(custom_function_content.encode())
        custom_check_function_location = temp_file.name

    # Configure table checks
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
            checks_location=checks_location,
            custom_check_functions={"custom_string_check": custom_check_function_location},
        )
    ]

    # Apply checks and write to table
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=1)

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [
                2,
                "custom",
                None,
                [
                    {
                        "name": "custom_string_check",
                        "message": "Contains custom text",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_string_check",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_with_ref_df(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"

    # Create test data
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create checks
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["a"], "ref_columns": ["a"], "ref_df_name": "ref_df_key"},
            },
        },
    ]

    # Save the checks to a workspace file:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    # Configure table checks
    run_configs = [
        RunConfig(
            input_config=InputConfig(location=input_table),
            output_config=OutputConfig(location=output_table, mode="overwrite"),
            checks_location=checks_location,
            reference_tables={"ref_df_key": InputConfig(location=input_table)},
        )
    ]

    # Apply checks and write to table
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    engine.apply_checks_and_save_in_tables(run_configs=run_configs, max_parallelism=1)

    # Verify the table was created
    actual_df = spark.table(output_table)
    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [2, "custom", None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_for_patterns(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create multiple input and output tables
    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{table}_dq_output" for table in input_tables]

    # Create test data for both tables
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, None]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create different checks for each table
    table1_checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]
    table2_checks = [
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location1 = f"{workspace_folder}/{input_tables[0]}.yml"
    checks_location2 = f"{workspace_folder}/{input_tables[1]}.yml"
    engine.save_checks(table1_checks, config=WorkspaceFileChecksStorageConfig(location=checks_location1))
    engine.save_checks(table2_checks, config=WorkspaceFileChecksStorageConfig(location=checks_location2))

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        checks_location=workspace_folder,
    )

    # Verify both tables were created and contain the expected data
    actual_df1 = spark.table(output_tables[0])
    actual_df2 = spark.table(output_tables[1])

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df1 = spark.createDataFrame(
        [
            [1, "valid", None, None],
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    expected_df2 = spark.createDataFrame(
        [
            [100, "test", None, None],
            [
                200,
                None,
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(actual_df1, expected_df1, ignore_nullable=True)
    assert_df_equality(actual_df2, expected_df2, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_for_patterns_checks_in_table(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create multiple input and output tables
    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{table}_dq_output" for table in input_tables]

    # Create test data for both tables
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, None]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create different checks for each table
    table1_checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]
    table2_checks = [
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    checks_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    engine.save_checks(
        table1_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=input_tables[0])
    )
    engine.save_checks(
        table2_checks, config=TableChecksStorageConfig(location=checks_table, run_config_name=input_tables[1])
    )

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[input_tables[0], input_tables[1]],
        checks_location=checks_table,
    )

    # Verify both tables were created and contain the expected data
    actual_df1 = spark.table(output_tables[0])
    actual_df2 = spark.table(output_tables[1])

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df1 = spark.createDataFrame(
        [
            [1, "valid", None, None],
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    expected_df2 = spark.createDataFrame(
        [
            [100, "test", None, None],
            [
                200,
                None,
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(actual_df1, expected_df1, ignore_nullable=True)
    assert_df_equality(actual_df2, expected_df2, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_for_patterns_with_quarantine(
    ws, spark, make_schema, make_random, make_directory
):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create multiple input and output tables
    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{table}_dq_output" for table in input_tables]
    quarantine_tables = [f"{table}_dq_quarantine" for table in input_tables]

    # Create test data for both tables
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, None]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create different checks for each table
    table1_checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]
    table2_checks = [
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    engine.save_checks(
        table1_checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_tables[0]}.yml")
    )
    engine.save_checks(
        table2_checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_tables[1]}.yml")
    )

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        checks_location=workspace_folder,
        max_parallelism=2,
        run_config_template=RunConfig(quarantine_config=OutputConfig(location="")),
    )

    # Verify both tables were created and contain the expected data
    expected_valid_df1 = spark.createDataFrame(
        [
            [1, "valid"],
        ],
        schema=test_schema,
    )

    expected_valid_df2 = spark.createDataFrame(
        [
            [100, "test"],
            [200, None],
        ],
        schema=test_schema,
    )

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_quarantine_df1 = spark.createDataFrame(
        [
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    expected_quarantine_df2 = spark.createDataFrame(
        [
            [
                200,
                None,
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(spark.table(output_tables[0]), expected_valid_df1, ignore_nullable=True)
    assert_df_equality(spark.table(output_tables[1]), expected_valid_df2, ignore_nullable=True)
    assert_df_equality(spark.table(quarantine_tables[0]), expected_quarantine_df1, ignore_nullable=True)
    assert_df_equality(spark.table(quarantine_tables[1]), expected_quarantine_df2, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_for_patterns_with_exclude_patterns(
    ws, spark, make_schema, make_random, make_directory
):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    output_table_suffix = "_output"
    output_table = f"{input_table}{output_table_suffix}"
    # should be excluded from applying checks
    existing_output_table = f"{input_table}_existing{output_table_suffix}"
    test_df.write.format("delta").saveAsTable(existing_output_table)

    quarantine_table_suffix = "_quarantine"
    quarantine_table = f"{input_table}{quarantine_table_suffix}"
    # should be excluded from applying checks
    existing_quarantine_table = f"{input_table}_existing{quarantine_table_suffix}"
    test_df.write.format("delta").saveAsTable(existing_quarantine_table)

    # Create different checks for each table
    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    engine.save_checks(
        checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_table}.yml")
    )
    engine.save_checks(
        checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{existing_output_table}.yml")
    )
    engine.save_checks(
        checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{existing_quarantine_table}.yml")
    )

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        exclude_patterns=[f"*{output_table_suffix}", f"*{quarantine_table_suffix}"],
        checks_location=workspace_folder + "/checks.yml",  # should strip the file name automatically,
        max_parallelism=2,
        run_config_template=RunConfig(quarantine_config=OutputConfig(location="")),
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    expected_valid_df = spark.createDataFrame(
        [
            [1, "valid"],
        ],
        schema=test_schema,
    )

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_quarantine_df = spark.createDataFrame(
        [
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(spark.table(output_table), expected_valid_df, ignore_nullable=True)
    assert_df_equality(spark.table(quarantine_table), expected_quarantine_df, ignore_nullable=True)
    # 1 input + 1 output + 1 quarantine + 2 existing
    tables = ws.tables.list_summaries(catalog_name=catalog_name, schema_name_pattern=schema.name)
    assert len(list(tables)) == 5, "Tables count mismatch"


def test_apply_checks_and_save_in_tables_for_patterns_no_tables_matching(ws, spark):
    # Test with empty list of table configs
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    # This should not raise an error
    with pytest.raises(NotFound, match="No tables found matching include or exclude criteria"):
        engine.apply_checks_and_save_in_tables_for_patterns(
            patterns=["main.non_existent_schema.*"], checks_location="some/location"
        )


def test_apply_checks_and_save_in_tables_for_patterns_exclude_no_tables_matching(ws, spark, make_schema, make_table):
    # Create an isolated schema with a known table, then exclude it using exclude_patterns
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create a table in the isolated schema
    make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT 1 as id",
    )

    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)

    # Include the schema pattern, but exclude all tables with wildcard - should result in no tables
    # Using exclude_patterns avoids the full catalog scan that exclude_matched=True triggers
    with pytest.raises(NotFound, match="No tables found matching include or exclude criteria"):
        engine.apply_checks_and_save_in_tables_for_patterns(
            patterns=[f"{catalog_name}.{schema.name}.*"],
            exclude_patterns=[f"{catalog_name}.{schema.name}.*"],
            checks_location="some/location",
        )


def test_apply_checks_and_save_in_tables_for_patterns_with_custom_suffix(
    ws, spark, make_schema, make_random, make_directory
):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    # Create multiple input and output tables
    output_table_suffix = "_dq_output_custom"
    quarantine_table_suffix = "_dq_quarantine_custom"
    input_tables = [f"{catalog_name}.{schema.name}.{make_random(8).lower()}" for _ in range(2)]
    output_tables = [f"{table}{output_table_suffix}" for table in input_tables]
    quarantine_tables = [f"{table}{quarantine_table_suffix}" for table in input_tables]

    # Create test data for both tables
    test_schema = "a: int, b: string"
    test_df1 = spark.createDataFrame([[1, "valid"], [None, "invalid"]], test_schema)
    test_df2 = spark.createDataFrame([[100, "test"], [200, None]], test_schema)

    test_df1.write.format("delta").mode("overwrite").saveAsTable(input_tables[0])
    test_df2.write.format("delta").mode("overwrite").saveAsTable(input_tables[1])

    # Create different checks for each table
    table1_checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "a"}},
        }
    ]
    table2_checks = [
        {
            "name": "b_is_null",
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "b"}},
        }
    ]

    # Save the checks to workspace files:
    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    engine.save_checks(
        table1_checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_tables[0]}.yml")
    )
    engine.save_checks(
        table2_checks, config=WorkspaceFileChecksStorageConfig(location=f"{workspace_folder}/{input_tables[1]}.yml")
    )

    # Apply checks and write to tables
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        checks_location=workspace_folder,
        run_config_template=RunConfig(quarantine_config=OutputConfig(location="")),
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    # Overwrite results to test that options are properly passed
    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[input_tables[0], input_tables[1]],
        checks_location=workspace_folder,
        run_config_template=RunConfig(
            input_config=InputConfig(location="", format="delta"),
            output_config=OutputConfig(location="", mode="overwrite"),
            quarantine_config=OutputConfig(location="", mode="overwrite"),
        ),
        output_table_suffix=output_table_suffix,
        quarantine_table_suffix=quarantine_table_suffix,
    )

    # Verify both tables were created and contain the expected data
    expected_valid_df1 = spark.createDataFrame(
        [
            [1, "valid"],
        ],
        schema=test_schema,
    )

    expected_valid_df2 = spark.createDataFrame(
        [
            [100, "test"],
            [200, None],
        ],
        schema=test_schema,
    )

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_quarantine_df1 = spark.createDataFrame(
        [
            [
                None,
                "invalid",
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )

    expected_quarantine_df2 = spark.createDataFrame(
        [
            [
                200,
                None,
                None,
                [
                    {
                        "name": "b_is_null",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )

    assert_df_equality(spark.table(output_tables[0]), expected_valid_df1, ignore_nullable=True)
    assert_df_equality(spark.table(output_tables[1]), expected_valid_df2, ignore_nullable=True)
    assert_df_equality(spark.table(quarantine_tables[0]), expected_quarantine_df1, ignore_nullable=True)
    assert_df_equality(spark.table(quarantine_tables[1]), expected_quarantine_df2, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_with_patterns_and_custom_functions(
    ws, spark, make_schema, make_random, make_directory
):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{input_table}_dq_output"

    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [
        {
            "name": "custom_string_check",
            "criticality": "warn",
            "check": {"function": "custom_string_check", "arguments": {"column": "b"}},
        },
    ]

    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    custom_function_content = '''
from databricks.labs.dqx.check_funcs import make_condition, register_rule
from pyspark.sql import functions as F
from pyspark.sql import Column

@register_rule("row")
def custom_string_check(column: str) -> Column:
    return F.when(F.col(column).contains("custom"), F.lit("Contains custom text")).otherwise(F.lit(None))
        '''

    # Save the custom function content to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".py") as temp_file:
        temp_file.write(custom_function_content.encode())
        custom_check_function_location = temp_file.name

    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        checks_location=workspace_folder,
        run_config_template=RunConfig(custom_check_functions={"custom_string_check": custom_check_function_location}),
    )

    actual_df = spark.table(output_table)

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [
                2,
                "custom",
                None,
                [
                    {
                        "name": "custom_string_check",
                        "message": "Contains custom text",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_string_check",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_with_patterns_and_ref_df(ws, spark, make_schema, make_random, make_directory):
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table = f"{input_table}_dq_output"

    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "foreign_key",
                "arguments": {"columns": ["a"], "ref_columns": ["a"], "ref_df_name": "ref_df_key"},
            },
        },
    ]

    engine = DQEngine(ws, spark=spark, extra_params=EXTRA_PARAMS)
    workspace_folder = str(make_directory().absolute())
    checks_location = f"{workspace_folder}/{input_table}.yml"
    engine.save_checks(checks, config=WorkspaceFileChecksStorageConfig(location=checks_location))

    run_config_template = RunConfig(reference_tables={"ref_df_key": InputConfig(location=input_table)})

    engine.apply_checks_and_save_in_tables_for_patterns(
        patterns=[f"{catalog_name}.{schema.name}.*"],
        checks_location=workspace_folder,
        run_config_template=run_config_template,
    )

    actual_df = spark.table(output_table)

    expected_schema = test_schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            [1, "test", None, None],
            [2, "custom", None, None],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
