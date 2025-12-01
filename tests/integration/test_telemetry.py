import pyspark.sql.types as T

from databricks.labs.dqx.telemetry import count_tables_in_spark_plan
from tests.conftest import TEST_CATALOG


def test_count_tables_from_file_based_dataframe(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").save(volume_path)
    test_df = spark.read.format("delta").load(volume_path)

    count = count_tables_in_spark_plan(test_df)

    assert count == 0, f"Expected 0 tables, but found {count}"


def test_count_tables_from_code_base_dataframe(ws, spark, make_schema, make_random, make_volume):
    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)

    count = count_tables_in_spark_plan(df)

    assert count == 0, f"Expected 0 tables, but found {count}"


def test_count_tables_from_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.table(table_name)

    count = count_tables_in_spark_plan(test_df)

    assert count == 1, f"Expected 1 table, but found {count}"


def test_count_tables_from_aggregated_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema).groupBy("id").count()
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.table(table_name)

    count = count_tables_in_spark_plan(test_df)

    assert count == 1, f"Expected 1 table, but found {count}"


def test_count_multiple_joined_tables_from_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name1 = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    table_name2 = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name1)
    df.write.format("delta").saveAsTable(table_name2)
    test_df1 = spark.table(table_name1)
    test_df2 = spark.table(table_name2)
    test_df = test_df1.join(test_df2, on="id", how="inner")

    count = count_tables_in_spark_plan(test_df)

    assert count == 2, f"Expected 2 tables, but found {count}"


def test_count_multiple_unioned_tables_from_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name1 = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    table_name2 = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name1)
    df.write.format("delta").saveAsTable(table_name2)
    test_df1 = spark.table(table_name1)
    test_df2 = spark.table(table_name2)
    test_df = test_df1.union(test_df2)

    count = count_tables_in_spark_plan(test_df)

    assert count == 2, f"Expected 2 tables, but found {count}"


def test_count_path_and_table_based_dataframe(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)

    df.write.format("delta").saveAsTable(table_name)
    df.write.format("delta").save(volume_path)
    test_df1 = spark.table(table_name)
    test_df2 = spark.read.format("delta").load(volume_path)
    test_df = test_df1.join(test_df2, on="id", how="inner")

    count = count_tables_in_spark_plan(test_df)

    assert count == 1, f"Expected 1 table, but found {count}"


def test_count_tables_from_streaming_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.readStream.table(table_name)

    count = count_tables_in_spark_plan(test_df)

    assert count == 1, f"Expected 1 table, but found {count}"
