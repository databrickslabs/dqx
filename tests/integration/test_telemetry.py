import pyspark.sql.types as T

from databricks.labs.dqx.telemetry import (
    get_tables_from_spark_plan,
    get_paths_from_spark_plan,
    get_spark_plan_as_string,
)
from tests.conftest import TEST_CATALOG


def test_get_tables_and_paths_from_file_delta_dataframe(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").save(volume_path)
    test_df = spark.read.format("delta").load(volume_path)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 0, f"Expected 0 tables, but found {len(tables)}"
    assert len(paths) == 1, f"Expected 1 path, but found {len(paths)}"


def test_get_tables_and_paths_from_file_parquet_dataframe(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.mode('overwrite').format("parquet").save(volume_path)
    test_df = spark.read.format("parquet").load(volume_path)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 0, f"Expected 0 tables, but found {len(tables)}"
    assert len(paths) == 1, f"Expected 1 path, but found {len(paths)}"


def test_get_tables_and_paths_from_code_dataframe(ws, spark, make_schema, make_random, make_volume):
    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    test_df = spark.createDataFrame([[1]], schema=input_schema)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 0, f"Expected 0 tables, but found {len(tables)}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"


def test_get_tables_and_paths_from_table_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.table(table_name)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 1, f"Expected 1 table, but found {len(tables)}"
    assert table_name in tables, f"Expected table with name {table_name}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"


def test_get_tables_and_paths_from_aggregated_table_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema).groupBy("id").count()
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.table(table_name)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 1, f"Expected 1 table, but found {len(tables)}"
    assert table_name in tables, f"Expected table with name {table_name}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"


def test_get_tables_and_paths_from_joined_tables_dataframe(ws, spark, make_schema, make_random):
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

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 2, f"Expected 2 tables, but found {len(tables)}"
    assert table_name1 in tables, f"Expected table with name {table_name1}"
    assert table_name2 in tables, f"Expected table with name {table_name2}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"


def test_get_tables_and_paths_from_unioned_tables_dataframe(ws, spark, make_schema, make_random):
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

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 2, f"Expected 2 tables, but found {len(tables)}"
    assert table_name1 in tables, f"Expected table with name {table_name1}"
    assert table_name2 in tables, f"Expected table with name {table_name2}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"


def test_get_tables_and_paths_from_mixed_dataframe(ws, spark, make_schema, make_random, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)

    df.write.format("delta").saveAsTable(table_name)
    df.write.format("delta").save(volume_path)
    test_df1 = spark.table(table_name)
    test_df2 = spark.read.format("delta").load(volume_path)
    test_df = test_df1.join(test_df2, on="id", how="inner")

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 1, f"Expected 1 table, but found {len(tables)}"
    assert table_name in tables, f"Expected table with name {table_name}"
    assert len(paths) == 1, f"Expected 1 path, but found {len(paths)}"
    # Volume paths in Spark plans are shown with dbfs: prefix
    assert any(volume_path in path for path in paths), f"Expected path containing {volume_path}, got {paths}"


def test_get_tables_and_paths_from_streaming_table_based_dataframe(ws, spark, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = T.StructType([T.StructField("id", T.IntegerType())])
    df = spark.createDataFrame([[1]], schema=input_schema)
    df.write.format("delta").saveAsTable(table_name)
    test_df = spark.readStream.table(table_name)

    plan = get_spark_plan_as_string(test_df)
    tables = get_tables_from_spark_plan(plan)
    paths = get_paths_from_spark_plan(plan, tables)

    assert len(tables) == 1, f"Expected 1 table, but found {len(tables) == 1}"
    assert table_name in tables, f"Expected table with name {table_name}"
    assert len(paths) == 0, f"Expected 0 paths, but found {len(paths)}"
