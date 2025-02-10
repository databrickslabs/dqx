from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.utils import read_input_data


def test_read_input_data_unity_catalog_table(spark, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_location = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    input_format = None

    schema = "a: int, b: int"
    input_df = spark.createDataFrame([[1, 2]], schema)
    input_df.write.format("delta").saveAsTable(input_location)

    result_df = read_input_data(spark, input_location, input_format)
    assert_df_equality(input_df, result_df)


def test_read_input_data_workspace_file(spark, make_schema, make_volume):
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
