from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.utils import read_input_data


def test_read_input_data_from_table(spark, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_location = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    input_format = None

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").saveAsTable(input_location)

    result_df = read_input_data(spark, input_location, input_format)
    assert_df_equality(input_df, result_df)


def test_read_input_data_from_table_with_schema_and_spark_options(spark, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_location = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    input_format = None
    input_read_options = {"versionAsOf": "0"}
    input_schema = "a int, b int"

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.format("delta").saveAsTable(input_location)
    input_df_ver1 = spark.createDataFrame([[0, 0]], schema)
    input_df_ver1.write.format("delta").insertInto(input_location)

    result_df = read_input_data(spark, input_location, input_format, input_schema, input_read_options)
    assert_df_equality(input_df_ver0, result_df)


def test_read_input_data_from_workspace_file(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = info.full_name
    input_format = "delta"

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").saveAsTable(input_location)

    result_df = read_input_data(spark, input_location, input_format)
    assert_df_equality(input_df, result_df)


def test_read_input_data_from_workspace_file_with_spark_options(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = info.full_name
    input_format = "delta"
    input_read_options = {"versionAsOf": "0"}

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.format("delta").saveAsTable(input_location)
    input_df_ver1 = spark.createDataFrame([[0, 0]], schema)
    input_df_ver1.write.format("delta").insertInto(input_location)

    result_df = read_input_data(spark, input_location, input_format, input_read_options=input_read_options)
    assert_df_equality(input_df_ver0, result_df)


def test_read_input_data_from_workspace_file_in_csv_format(spark, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    info = make_volume(catalog_name=catalog_name, schema_name=schema_name)
    input_location = f"/Volumes/{info.catalog_name}/{info.schema_name}/{info.name}"
    input_format = "csv"

    input_read_options = {"header": "true"}
    input_schema = "a int, b int"

    schema = "a: int, b: int"
    input_df_ver0 = spark.createDataFrame([[1, 2]], schema)
    input_df_ver0.write.options(**input_read_options).format("csv").mode("overwrite").save(input_location)

    result_df = read_input_data(spark, input_location, input_format, input_schema, input_read_options)

    assert_df_equality(input_df_ver0, result_df)
