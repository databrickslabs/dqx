import tempfile
import os
import pyspark.sql.functions as F
from pyspark.sql.types import Row
import pytest
from databricks.labs.dqx.utils import read_input_data, get_column_name


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


def test_read_input_data_storage_path(spark_local):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"val1,val2\n")
        temp_file_path = temp_file.name

    try:
        input_location = temp_file_path
        result = read_input_data(spark_local, input_location, "csv")
        assert result.collect() == [Row(_c0='val1', _c1='val2')]

    finally:
        os.remove(temp_file_path)


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
