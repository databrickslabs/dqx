import pyspark.errors.exceptions.connect
import pytest
from pyspark.sql.functions import col, lit, when
from pyspark.sql import Column
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from tests.integration.conftest import EXTRA_PARAMS, RUN_TIME, REPORTING_COLUMNS


def test_save_results_in_table(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table_mode = "overwrite"
    output_config = OutputConfig(location=output_table, mode=output_table_mode)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table_mode = "overwrite"
    quarantine_config = OutputConfig(location=quarantine_table, mode=quarantine_table_mode)

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)

    output_config.mode = "append"
    output_config.options = {"overwriteSchema": "true"}
    quarantine_config.mode = "append"
    quarantine_config.options = {"overwriteSchema": "true"}

    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=output_config,
        quarantine_config=quarantine_config,
    )

    assert_df_equality(output_df.union(output_df), output_df_loaded)
    assert_df_equality(quarantine_df.union(quarantine_df), quarantine_df_loaded)


def test_save_results_in_table_only_output(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    output_table_mode = "overwrite"
    output_config = OutputConfig(location=output_table, mode=output_table_mode)

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        output_config=output_config,
    )

    output_df_loaded = spark.table(output_table)

    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_only_quarantine(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}"
    quarantine_table_mode = "overwrite"
    quarantine_config = OutputConfig(location=quarantine_table, mode=quarantine_table_mode)

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(quarantine_df=quarantine_df, quarantine_config=quarantine_config)

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_only_output(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_in_user_installation_only_quarantine(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        quarantine_df=quarantine_df,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation_output_table_provided(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.quarantine_config = OutputConfig(location=quarantine_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_config=OutputConfig(location=output_table),
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_quarantine_table_provided(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_config = OutputConfig(location=output_table)
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        quarantine_config=OutputConfig(location=quarantine_table),
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)


def test_save_results_in_table_in_user_installation_missing_output_and_quarantine_table(
    ws, spark, installation_ctx, make_schema, make_random
):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    data_schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], data_schema)
    quarantine_df = spark.createDataFrame([[3, 4]], data_schema)

    engine = DQEngine(ws, spark)
    with pytest.raises(
        pyspark.errors.exceptions.connect.AnalysisException, match="The schema `main.dqx_test` cannot be found"
    ):
        engine.save_results_in_table(
            output_df=output_df,
            quarantine_df=quarantine_df,
            run_config_name=run_config.name,
            product_name=product_name,
            assume_user=True,
        )

    assert (
        spark.sql(f"SHOW TABLES FROM {catalog_name}.{schema.name} LIKE '{output_table}'").count() == 0
    ), "Output table should not have been saved"
    assert (
        spark.sql(f"SHOW TABLES FROM {catalog_name}.{schema.name} LIKE '{quarantine_table}'").count() == 0
    ), "Quarantine table should not have been saved"


def test_save_streaming_results_in_table(ws, spark, make_schema, make_random, make_volume):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    random_name = make_random(6).lower()
    output_table = f"{catalog_name}.{schema.name}.{random_name}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").mode("append").saveAsTable(input_table)
    streaming_input_df = spark.readStream.table(input_table)

    output_table_mode = "append"
    output_table_options = {
        "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{random_name}"
    }
    output_table_trigger = {"availableNow": True}
    output_config = OutputConfig(
        location=output_table, mode=output_table_mode, options=output_table_options, trigger=output_table_trigger
    )

    engine = DQEngine(ws, spark)
    engine.save_results_in_table(
        output_df=streaming_input_df,
        output_config=output_config,
    )

    output_df_loaded = spark.table(output_table)
    assert_df_equality(input_df, output_df_loaded)


def test_apply_checks_and_save_in_table_single_table(ws, spark, make_schema, make_random):
    """Test apply_checks_and_save_in_table method with single table."""
    catalog_name = "main"
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
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_and_save_in_table_split_tables(ws, spark, make_schema, make_random):
    """Test apply_checks_and_save_in_table method with split tables."""
    catalog_name = "main"
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


def test_apply_checks_by_metadata_and_save_in_table_single_table(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_save_in_table method with single table."""
    catalog_name = "main"
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
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_apply_checks_by_metadata_and_save_in_table_split_tables(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_save_in_table method with split tables."""
    catalog_name = "main"
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
    """Test apply_checks_and_save_in_table with custom options."""
    catalog_name = "main"
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
    """Test apply_checks_and_save_in_table with different write modes."""
    catalog_name = "main"
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


def test_apply_checks_by_metadata_with_custom_functions(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_save_in_table with custom check functions."""
    catalog_name = "main"
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
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_streaming_write(ws, spark, make_schema, make_random, make_volume):
    """Test writing streaming DataFrames to Delta tables."""
    catalog_name = "main"
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
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema=expected_schema,
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
