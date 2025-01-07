import pyspark.sql.functions as F
import pytest
from databricks.labs.dqx.utils import read_input_data, get_column_name, remove_extra_indentation, extract_major_minor


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


def test_read_input_data_unity_catalog_table(spark_mock):
    input_location = "catalog.schema.table"
    input_format = None
    spark_mock.read.table.return_value = "dataframe"

    result = read_input_data(spark_mock, input_location, input_format)

    spark_mock.read.table.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_storage_path(spark_mock):
    input_location = "s3://bucket/path"
    input_format = "delta"
    spark_mock.read.format.return_value.load.return_value = "dataframe"

    result = read_input_data(spark_mock, input_location, input_format)

    spark_mock.read.format.assert_called_once_with(input_format)
    spark_mock.read.format.return_value.load.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_workspace_file(spark_mock):
    input_location = "/folder/path"
    input_format = "delta"
    spark_mock.read.format.return_value.load.return_value = "dataframe"

    result = read_input_data(spark_mock, input_location, input_format)

    spark_mock.read.format.assert_called_once_with(input_format)
    spark_mock.read.format.return_value.load.assert_called_once_with(input_location)
    assert result == "dataframe"


def test_read_input_data_no_input_location(spark_mock):
    with pytest.raises(ValueError, match="Input location not configured"):
        read_input_data(spark_mock, None, None)


def test_read_input_data_no_input_format(spark_mock):
    input_location = "s3://bucket/path"
    input_format = None

    with pytest.raises(ValueError, match="Input format not configured"):
        read_input_data(spark_mock, input_location, input_format)


def test_read_invalid_input_location(spark_mock):
    input_location = "invalid/location"
    input_format = None

    with pytest.raises(ValueError, match="Invalid input location."):
        read_input_data(spark_mock, input_location, input_format)


def test_remove_extra_indentation_no_indentation():
    doc = "This is a test docstring."
    expected = "This is a test docstring."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_with_indentation():
    doc = "    This is a test docstring with indentation."
    expected = "This is a test docstring with indentation."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_mixed_indentation():
    doc = "    This is a test docstring with indentation.\nThis line has no indentation."
    expected = "This is a test docstring with indentation.\nThis line has no indentation."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_multiple_lines():
    doc = "    Line one.\n    Line two.\n    Line three."
    expected = "Line one.\nLine two.\nLine three."
    assert remove_extra_indentation(doc) == expected


def test_remove_extra_indentation_empty_string():
    doc = ""
    expected = ""
    assert remove_extra_indentation(doc) == expected


def test_extract_major_minor():
    assert extract_major_minor("1.2.3") == "1.2"
    assert extract_major_minor("10.20.30") == "10.20"
    assert extract_major_minor("v1.2.3") == "1.2"
    assert extract_major_minor("version 1.2.3") == "1.2"
    assert extract_major_minor("1.2") == "1.2"
    assert extract_major_minor("1.2.3.4") == "1.2"
    assert extract_major_minor("no version") is None
    assert extract_major_minor("") is None
    assert extract_major_minor("1") is None
