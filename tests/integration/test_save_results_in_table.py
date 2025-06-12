from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs


def test_save_results_in_table(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(output_table)
    quarantine_df_loaded = spark.table(quarantine_table)

    assert_df_equality(output_df, output_df_loaded)
    assert_df_equality(quarantine_df, quarantine_df_loaded)

    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="append",
        quarantine_table_mode="append",
        output_table_options={"overwriteSchema": "true"},
        quarantine_table_options={"overwriteSchema": "true"},
    )

    assert_df_equality(output_df.union(output_df), output_df_loaded)
    assert_df_equality(quarantine_df.union(quarantine_df), quarantine_df_loaded)


def test_save_results_in_table_only_output(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        output_table=output_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(output_table)

    assert_df_equality(output_df, output_df_loaded)


def test_save_results_in_table_only_quarantine(ws, spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        quarantine_df=quarantine_df,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite",
    )

    output_df_loaded = spark.table(quarantine_table)
    assert_df_equality(quarantine_df, output_df_loaded)


def test_save_results_in_table_in_user_installation(ws, spark, installation_ctx, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.output_table = output_table
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
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
    run_config.output_table = output_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)

    engine = DQEngine(ws)
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
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
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
    run_config.quarantine_table = quarantine_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        output_table=output_table,
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
    run_config.output_table = output_table
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    schema = "a: int, b: int"
    output_df = spark.createDataFrame([[1, 2]], schema)
    quarantine_df = spark.createDataFrame([[3, 4]], schema)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=output_df,
        quarantine_df=quarantine_df,
        quarantine_table=quarantine_table,
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

    engine = DQEngine(ws)
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
    input_df.write.format("delta").mode("overwrite").saveAsTable(input_table)
    streaming_input_df = spark.readStream.table(input_table)

    engine = DQEngine(ws)
    engine.save_results_in_table(
        output_df=streaming_input_df,
        output_table=output_table,
        output_table_mode="overwrite",
        output_table_options={
            "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{random_name}"
        },
        trigger={"availableNow": True},
    )

    output_df_loaded = spark.table(output_table)
    assert_df_equality(input_df, output_df_loaded)


def test_apply_checks_and_write_to_table_single_table(ws, spark, make_schema, make_random):
    """Test apply_checks_and_write_to_table method with single table."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, None, "mixed"]], test_schema)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
        DQRowRule(
            name="b_is_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="b",
        ),
    ]

    # Apply checks and write to table (no quarantine table)
    engine = DQEngine(ws)
    result_df = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        output_table_mode="overwrite"
    )

    # Verify the table was created and contains the expected data
    saved_df = spark.table(output_table)
    assert_df_equality(result_df, saved_df, ignore_nullable=True)

    # Verify the result has the expected structure (original columns + error/warning columns)
    expected_columns = ["a", "b", "c", "_error", "_warning"]
    assert set(result_df.columns) == set(expected_columns)

    # Verify error and warning data
    error_rows = result_df.filter("_error IS NOT NULL").count()
    warning_rows = result_df.filter("_warning IS NOT NULL").count()
    assert error_rows == 1  # One row with null 'a'
    assert warning_rows == 1  # One row with null 'b'


def test_apply_checks_and_write_to_table_split_tables(ws, spark, make_schema, make_random):
    """Test apply_checks_and_write_to_table method with split tables."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, 5, "good"]], test_schema)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    # Apply checks, split, and write to tables (with quarantine table)
    engine = DQEngine(ws)
    good_df, bad_df = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite"
    )

    # Verify the tables were created
    saved_good_df = spark.table(output_table)
    saved_bad_df = spark.table(quarantine_table)

    assert_df_equality(good_df, saved_good_df, ignore_nullable=True)
    assert_df_equality(bad_df, saved_bad_df, ignore_nullable=True)

    # Verify data distribution
    assert good_df.count() == 2  # Two rows without errors
    assert bad_df.count() == 1   # One row with error

    # Verify good data doesn't have error/warning columns
    assert "_error" not in good_df.columns
    assert "_warning" not in good_df.columns

    # Verify bad data has error/warning columns
    assert "_error" in bad_df.columns
    assert "_warning" in bad_df.columns


def test_apply_checks_by_metadata_and_write_to_table_single_table(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_write_to_table method with single table."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, None, "mixed"]], test_schema)

    # Create metadata checks
    checks = [
        {
            "name": "a_is_not_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "a"}
            }
        },
        {
            "name": "b_is_not_null",
            "criticality": "warn",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "b"}
            }
        },
    ]

    # Apply checks and write to table (no quarantine table)
    engine = DQEngine(ws)
    result_df = engine.apply_checks_by_metadata_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        output_table_mode="overwrite"
    )

    # Verify the table was created and contains the expected data
    saved_df = spark.table(output_table)
    assert_df_equality(result_df, saved_df, ignore_nullable=True)

    # Verify the result has the expected structure
    expected_columns = ["a", "b", "c", "_error", "_warning"]
    assert set(result_df.columns) == set(expected_columns)

    # Verify error and warning data
    error_rows = result_df.filter("_error IS NOT NULL").count()
    warning_rows = result_df.filter("_warning IS NOT NULL").count()
    assert error_rows == 1  # One row with null 'a'
    assert warning_rows == 1  # One row with null 'b'


def test_apply_checks_by_metadata_and_write_to_table_split_tables(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_write_to_table method with split tables."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int, c: string"
    test_df = spark.createDataFrame([[1, 2, "valid"], [None, 3, "invalid"], [4, 5, "good"]], test_schema)

    # Create metadata checks
    checks = [
        {
            "name": "a_is_not_null",
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "a"}
            }
        },
    ]

    # Apply checks, split, and write to tables (with quarantine table)
    engine = DQEngine(ws)
    good_df, bad_df = engine.apply_checks_by_metadata_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite"
    )

    # Verify the tables were created
    saved_good_df = spark.table(output_table)
    saved_bad_df = spark.table(quarantine_table)

    assert_df_equality(good_df, saved_good_df, ignore_nullable=True)
    assert_df_equality(bad_df, saved_bad_df, ignore_nullable=True)

    # Verify data distribution
    assert good_df.count() == 2  # Two rows without errors
    assert bad_df.count() == 1   # One row with error

    # Verify good data doesn't have error/warning columns
    assert "_error" not in good_df.columns
    assert "_warning" not in good_df.columns

    # Verify bad data has error/warning columns
    assert "_error" in bad_df.columns
    assert "_warning" in bad_df.columns


def test_apply_checks_and_write_to_table_with_options(ws, spark, make_schema, make_random):
    """Test apply_checks_and_write_to_table with custom options."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [3, 4]], test_schema)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_positive",
            criticality="warn",
            check_func=check_funcs.is_greater_than,
            column="a",
            limit=0,
        ),
    ]

    # Apply checks and write to table with custom options
    engine = DQEngine(ws)
    result_df = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        output_table_mode="overwrite",
        output_table_options={"overwriteSchema": "true"}
    )

    # Verify the table was created
    saved_df = spark.table(output_table)
    assert_df_equality(result_df, saved_df, ignore_nullable=True)

    # Add more data with different schema to test schema evolution
    extended_schema = "a: int, b: int, d: string"
    extended_df = spark.createDataFrame([[5, 6, "new"]], extended_schema)
    
    extended_result_df = engine.apply_checks_and_write_to_table(
        df=extended_df,
        checks=checks,
        output_table=output_table,
        output_table_mode="append",
        output_table_options={"mergeSchema": "true"}
    )

    # Verify schema was merged
    final_df = spark.table(output_table)
    assert "d" in final_df.columns
    assert final_df.count() == 3  # Original 2 + new 1


def test_apply_checks_and_write_to_table_with_different_modes(ws, spark, make_schema, make_random):
    """Test apply_checks_and_write_to_table with different write modes."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [None, 4]], test_schema)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    # First write with overwrite mode
    engine = DQEngine(ws)
    good_df1, bad_df1 = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite"
    )

    # Verify initial data
    assert spark.table(output_table).count() == 1
    assert spark.table(quarantine_table).count() == 1

    # Second write with append mode
    test_df2 = spark.createDataFrame([[3, 4], [None, 6]], test_schema)
    good_df2, bad_df2 = engine.apply_checks_and_write_to_table(
        df=test_df2,
        checks=checks,
        output_table=output_table,
        quarantine_table=quarantine_table,
        output_table_mode="append",
        quarantine_table_mode="append"
    )

    # Verify appended data
    assert spark.table(output_table).count() == 2  # 1 + 1 good records
    assert spark.table(quarantine_table).count() == 2  # 1 + 1 bad records


def test_apply_checks_by_metadata_with_custom_functions(ws, spark, make_schema, make_random):
    """Test apply_checks_by_metadata_and_write_to_table with custom check functions."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: string"
    test_df = spark.createDataFrame([[1, "test"], [2, "custom"]], test_schema)

    # Define custom check function
    def custom_string_check(column: str) -> str:
        """Custom check function for testing."""
        from pyspark.sql import functions as F
        return F.when(F.col(column).contains("custom"), F.lit("Contains custom text")).otherwise(F.lit(None))

    # Create metadata checks with custom function
    checks = [
        {
            "name": "custom_string_check",
            "criticality": "warn",
            "check": {
                "function": "custom_string_check",
                "arguments": {"column": "b"}
            }
        },
    ]

    # Apply checks with custom functions
    engine = DQEngine(ws)
    result_df = engine.apply_checks_by_metadata_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=output_table,
        custom_check_functions={"custom_string_check": custom_string_check},
        output_table_mode="overwrite"
    )

    # Verify the table was created
    saved_df = spark.table(output_table)
    assert_df_equality(result_df, saved_df, ignore_nullable=True)

    # Verify custom check was applied
    warning_rows = result_df.filter("_warning IS NOT NULL").count()
    assert warning_rows == 1  # One row with "custom" text


def test_streaming_dataframe_write(ws, spark, make_schema, make_random, make_volume):
    """Test writing streaming DataFrames to Delta tables."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    input_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)
    checkpoint_location = f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(6).lower()}"

    # Create source table for streaming
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [3, 4]], test_schema)
    test_df.write.format("delta").mode("overwrite").saveAsTable(input_table)

    # Create streaming DataFrame
    streaming_df = spark.readStream.table(input_table)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_positive",
            criticality="warn",
            check_func=check_funcs.is_greater_than,
            column="a",
            limit=0,
        ),
    ]

    # Apply checks and write streaming DataFrame
    engine = DQEngine(ws)
    result_df = engine.apply_checks_and_write_to_table(
        df=streaming_df,
        checks=checks,
        output_table=output_table,
        output_table_options={"checkpointLocation": checkpoint_location},
        trigger={"availableNow": True}
    )

    # Verify the table was created with streaming data
    saved_df = spark.table(output_table)
    assert saved_df.count() == 2

    # Verify the result has the expected structure
    expected_columns = ["a", "b", "_error", "_warning"]
    assert set(saved_df.columns) == set(expected_columns)


def test_return_type_consistency(ws, spark, make_schema, make_random):
    """Test that return types are consistent based on quarantine_table parameter."""
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"

    # Create test data
    test_schema = "a: int, b: int"
    test_df = spark.createDataFrame([[1, 2], [None, 4]], test_schema)

    # Create checks
    checks = [
        DQRowRule(
            name="a_is_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
        ),
    ]

    engine = DQEngine(ws)

    # Test single table return (DataFrame)
    result_single = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=f"{output_table}_single",
        output_table_mode="overwrite"
    )
    
    # Should return a single DataFrame
    assert hasattr(result_single, 'columns')  # DataFrame has columns attribute
    assert not isinstance(result_single, tuple)

    # Test split tables return (tuple of DataFrames)
    result_split = engine.apply_checks_and_write_to_table(
        df=test_df,
        checks=checks,
        output_table=f"{output_table}_split",
        quarantine_table=quarantine_table,
        output_table_mode="overwrite",
        quarantine_table_mode="overwrite"
    )
    
    # Should return a tuple of two DataFrames
    assert isinstance(result_split, tuple)
    assert len(result_split) == 2
    good_df, bad_df = result_split
    assert hasattr(good_df, 'columns')
    assert hasattr(bad_df, 'columns')
