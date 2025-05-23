from unittest.mock import Mock
import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.utils import read_input_data, get_column_as_string, extract_struct_fields


def test_get_column_name():
    col = F.col("a")
    actual = get_column_as_string(col)
    assert actual == "a"


def test_get_col_name_alias():
    col = F.col("a").alias("b")
    actual = get_column_as_string(col)
    assert actual == "b"


def test_get_col_name_multiple_alias():
    col = F.col("a").alias("b").alias("c")
    actual = get_column_as_string(col)
    assert actual == "c"


def test_get_col_name_longer():
    col = F.col("local")
    actual = get_column_as_string(col)
    assert actual == "local"


def test_get_col_name_and_truncate():
    long_col_name = "a" * 300
    col = F.col(long_col_name)
    actual = get_column_as_string(col, normalize=True)
    max_chars = 255
    assert len(actual) == max_chars


def test_get_col_name_normalize():
    col = F.col("a").alias("b")
    actual = get_column_as_string(col, normalize=True)
    assert actual == "b"


def test_get_col_name_capital_normalize():
    col = F.col("A")
    actual = get_column_as_string(col, normalize=True)
    assert actual == "a"


def test_get_col_name_alias_not_normalize():
    col = F.col("A").alias("B")
    actual = get_column_as_string(col, normalize=True)
    assert actual == "B"


def test_get_col_name_capital():
    col = F.col("A").alias("B")
    actual = get_column_as_string(col)
    assert actual == "B"


def test_get_col_name_normalize_complex_expr():
    col = F.try_element_at("col7", F.lit("key1").cast("string"))
    actual = get_column_as_string(col, normalize=True)
    assert actual == "try_element_at_col7_cast_key1_as_string"


def test_get_col_name_alias_from_complex_expr():
    col = F.try_element_at("col7", F.lit("key1").cast("string")).alias("alias1")
    actual = get_column_as_string(col)
    assert actual == "alias1"


def test_get_col_name_as_str():
    col = "a"
    actual = get_column_as_string(col)
    assert actual == col


def test_get_col_name_expr_not_found(spark_local):
    with pytest.raises(ValueError, match="Invalid column expression"):
        get_column_as_string(Mock())


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


def test_read_invalid_input_table(spark_local):
    input_location = "table"  # not a valid 2 or 3-level namespace
    input_format = None

    with pytest.raises(ValueError, match="Invalid input location."):
        read_input_data(spark_local, input_location, input_format)


def test_valid_2_level_table_namespace():
    input_location = "db.table"
    input_format = None
    assert read_input_data(Mock(), input_location, input_format)


def test_valid_3_level_table_namespace():
    input_location = "catalog.schema.table"
    input_format = None
    assert read_input_data(Mock(), input_location, input_format)


def test_extract_struct_fields():
    col_string = "struct(col1, col2)"
    expected = ["col1", "col2"]
    actual = extract_struct_fields(col_string)
    assert actual == expected


def test_extract_nested_struct_fields():
    col_string = "struct(col1, col1.col2, col1.col3)"
    expected = ["col1", "col1.col2", "col1.col3"]
    actual = extract_struct_fields(col_string)
    print(actual)
    assert actual == expected


def test_extract_struct_fields_with_expr():
    col_string = "2 * struct(col1, col2)"
    expected = ["col1", "col2"]
    actual = extract_struct_fields(col_string)
    assert actual == expected


def test_extract_fields_when_not_given_struct():
    col_string = "col1, col2"
    expected = []
    actual = extract_struct_fields(col_string)
    assert actual == expected
