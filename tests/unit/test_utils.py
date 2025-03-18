import pyspark.sql.functions as F
import pytest
from databricks.labs.dqx.utils import read_input_data, get_str_from_col


def test_get_column_name():
    col = F.col("a")
    actual = get_str_from_col(col)
    assert actual == "a"


def test_get_col_name_alias():
    col = F.col("a").alias("b")
    actual = get_str_from_col(col)
    assert actual == "b"


def test_get_col_name_multiple_alias():
    col = F.col("a").alias("b").alias("c")
    actual = get_str_from_col(col)
    assert actual == "c"


def test_get_col_name_longer():
    col = F.col("local")
    actual = get_str_from_col(col)
    assert actual == "local"


def test_get_col_name_and_truncate():
    long_col_name = "a" * 300
    col = F.col(long_col_name)
    actual = get_str_from_col(col)
    max_chars = 255
    assert len(actual) == max_chars


def test_get_col_name_as_str():
    col = "a"
    actual = get_str_from_col(col)
    assert actual == col


def test_read_input_data_no_input_location(spark_local):
    with pytest.raises(ValueError, match="Input location not configured"):
        read_input_data(spark_local, None, None)


def test_read_input_data_no_input_format(spark_local):
    input_location = "s3://bucket/path"
    input_format = None

    with pytest.raises(ValueError, match="Input format not configured"):
        read_input_data(spark_local, input_location, input_format)


def test_read_invalid_input_location(spark_local):
    input_location = "invalid/location"
    input_format = None

    with pytest.raises(ValueError, match="Invalid input location."):
        read_input_data(spark_local, input_location, input_format)
