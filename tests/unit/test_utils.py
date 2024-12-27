import pyspark.sql.functions as F
import pytest
from databricks.labs.dqx.utils import read_input_data
from databricks.labs.dqx.utils import get_column_name


def test_get_column_name():
    col = F.col("a")
    actual = get_column_name(col)
    assert actual == "a"


def test_get_col_name_alias():
    col = F.col("a").alias("b")
    actual = get_column_name(col)
    assert actual == "b"


def test_get_col_name_multiple_alias():
    col = F.col("a").alias("b").alias("c")
    actual = get_column_name(col)
    assert actual == "c"


def test_get_col_name_longer():
    col = F.col("local")
    actual = get_column_name(col)
    assert actual == "local"


def test_read_input_data_unity_catalog_table(spark):
    input_location = "catalog.schema.table"
    input_format = None
    spark.read.table.return_value = "dataframe"

    result = read_input_data(spark, input_location, input_format)

    spark.read.table.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_storage_path(spark):
    input_location = "s3://bucket/path"
    input_format = "delta"
    spark.read.format.return_value.load.return_value = "dataframe"

    result = read_input_data(spark, input_location, input_format)

    spark.read.format.assert_called_once_with(input_format)
    spark.read.format.return_value.load.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_workspace_file(spark):
    input_location = "/folder/path"
    input_format = "delta"
    spark.read.format.return_value.load.return_value = "dataframe"

    result = read_input_data(spark, input_location, input_format)

    spark.read.format.assert_called_once_with(input_format)
    spark.read.format.return_value.load.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_no_input_location(spark):
    with pytest.raises(ValueError, match="Input location not configured"):
        read_input_data(spark, None, None)


def test_read_input_data_no_input_format(spark):
    input_location = "s3://bucket/path"
    input_format = None

    with pytest.raises(ValueError, match="Input format not configured"):
        read_input_data(spark, input_location, input_format)


def test_read_invalid_input_location(spark):
    input_location = "invalid/location"
    input_format = None

    with pytest.raises(ValueError, match="Invalid input location."):
        read_input_data(spark, input_location, input_format)
