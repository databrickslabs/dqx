import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.utils import read_input_data, save_dataframe_as_table


def test_read_input_data_no_input_format(spark):
    input_location = "s3://bucket/path"
    input_config = InputConfig(location=input_location)

    with pytest.raises(Exception, match="Incompatible format detected"):
        df = read_input_data(spark, input_config)
        df.count()


def test_read_invalid_input_location(spark):
    input_location = "invalid/location"
    input_config = InputConfig(location=input_location)

    with pytest.raises(
        ValueError,
        match="Invalid input location. It must be a 2 or 3-level table namespace or storage path, given invalid/location",
    ):
        read_input_data(spark, input_config)


def test_read_invalid_input_table(spark):
    input_location = "table"  # not a valid 2 or 3-level namespace
    input_config = InputConfig(location=input_location)

    with pytest.raises(
        ValueError,
        match="Invalid input location. It must be a 2 or 3-level table namespace or storage path, given table",
    ):
        read_input_data(spark, input_config)


def test_read_input_data_from_table(spark, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_location = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    input_config = InputConfig(location=input_location)

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").saveAsTable(input_location)

    result_df = read_input_data(spark, input_config)
    assert_df_equality(input_df, result_df)


def test_read_input_data_from_table_with_schema_and_spark_options(spark, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_location = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    input_format = None
    input_read_options = {"versionAsOf": "0"}
    input_schema = "a int, b int"
    input_config = InputConfig(
        location=input_location, format=input_format, schema=input_schema, options=input_read_options
    )

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.format("delta").saveAsTable(input_location)
    input_df_ver1 = spark.createDataFrame([[0, 0]], schema)
    input_df_ver1.write.format("delta").insertInto(input_location)

    result_df = read_input_data(spark, input_config)
    assert_df_equality(input_df_ver0, result_df)


def test_read_input_data_from_workspace_file(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = info.full_name
    input_format = "delta"
    input_config = InputConfig(location=input_location, format=input_format)

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").saveAsTable(input_location)

    result_df = read_input_data(spark, input_config)
    assert_df_equality(input_df, result_df)


def test_read_input_data_from_workspace_file_with_spark_options(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = info.full_name
    input_format = "delta"
    input_read_options = {"versionAsOf": "0"}
    input_config = InputConfig(location=input_location, format=input_format, options=input_read_options)

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.format("delta").saveAsTable(input_location)
    input_df_ver1 = spark.createDataFrame([[0, 0]], schema)
    input_df_ver1.write.format("delta").insertInto(input_location)

    result_df = read_input_data(spark, input_config)
    assert_df_equality(input_df_ver0, result_df)


def test_read_input_data_from_workspace_file_in_csv_format(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = f"/Volumes/{info.catalog_name}/{info.schema_name}/{info.name}"
    input_format = "csv"

    input_read_options = {"header": "true"}
    input_schema = "a int, b int"
    input_config = InputConfig(
        location=input_location, format=input_format, schema=input_schema, options=input_read_options
    )

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.options(**input_read_options).format("csv").mode("overwrite").save(input_location)

    result_df = read_input_data(spark, input_config)

    assert_df_equality(input_df_ver0, result_df)


def test_save_dataframe_as_table(spark, make_schema, make_random):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table_name = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    mode = "overwrite"
    output_config = OutputConfig(location=table_name, mode=mode)

    data_schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], data_schema)
    save_dataframe_as_table(input_df, output_config)

    result_df = spark.table(table_name)
    assert_df_equality(input_df, result_df)

    output_config.mode = "append"
    output_config.options = {"mergeSchema": "true"}
    changed_df = input_df.selectExpr("*", "1 AS c")
    save_dataframe_as_table(changed_df, output_config)

    result_df = spark.table(table_name)
    expected_df = changed_df.union(input_df.selectExpr("*", "NULL AS c"))
    assert_df_equality(expected_df, result_df)


def test_save_streaming_dataframe_in_table(spark, make_schema, make_random, make_volume):
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table_name = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    random_name = make_random(6).lower()
    result_table_name = f"{catalog_name}.{schema.name}.{random_name}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema.name)
    options = {"checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{random_name}"}
    trigger = {"availableNow": True}
    output_config = OutputConfig(location=result_table_name, options=options, trigger=trigger)

    data_schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], data_schema)
    input_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    streaming_input_df = spark.readStream.table(table_name)

    save_dataframe_as_table(
        streaming_input_df,
        output_config,
    )

    result_df = spark.table(result_table_name)
    assert_df_equality(input_df, result_df)

    save_dataframe_as_table(
        streaming_input_df,
        output_config,
    )

    result_df = spark.table(result_table_name)
    assert_df_equality(input_df, result_df)  # no new records
