from unittest.mock import Mock
import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.utils import read_input_data, get_column_as_string


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


def test_get_col_name_expr_not_found():
    with pytest.raises(ValueError, match="Invalid column expression"):
        get_column_as_string(Mock())


def test_valid_2_level_table_namespace():
    input_location = "db.table"
    input_format = None
    assert read_input_data(Mock(), input_location, input_format)


def test_valid_3_level_table_namespace():
    input_location = "catalog.schema.table"
    input_format = None
    assert read_input_data(Mock(), input_location, input_format)


def test_streaming_source():
    input_location = "catalog.schema.table"
    df = read_input_data(Mock(), input_location, with_streaming=True)
    assert df.isStreaming


def test_invalid_streaming_source_format():
    input_location = "/Volumes/catalog/schema/volume/"
    input_format = "json"
    with pytest.raises(ValueError, match="Input format is not a valid streaming source format"):
        read_input_data(Mock(), input_location, input_format, with_streaming=True)
