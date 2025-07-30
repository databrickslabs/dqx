from datetime import date, datetime
from typing import Any
from unittest.mock import Mock
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column

from databricks.labs.dqx.utils import (
    read_input_data,
    get_column_name_or_alias,
    is_sql_query_safe,
    normalize_col_str,
    safe_json_load,
    get_columns_as_strings,
    is_valid_column_name,
    normalize_bound_args,
)
from databricks.labs.dqx.config import InputConfig


def test_get_column_name():
    col = F.col("a")
    actual = get_column_name_or_alias(col)
    assert actual == "a"


def test_get_col_name_alias():
    col = F.col("a").alias("b")
    actual = get_column_name_or_alias(col)
    assert actual == "b"


def test_get_col_name_multiple_alias():
    col = F.col("a").alias("b").alias("c")
    actual = get_column_name_or_alias(col)
    assert actual == "c"


def test_get_col_name_longer():
    col = F.col("local")
    actual = get_column_name_or_alias(col)
    assert actual == "local"


def test_get_col_name_and_truncate():
    long_col_name = "a" * 300
    col = F.col(long_col_name)
    actual = get_column_name_or_alias(col, normalize=True)
    max_chars = 255
    assert len(actual) == max_chars


def test_normalize_col_str():
    long_str = "a" * 300
    actual = normalize_col_str(long_str)
    max_chars = 255
    assert len(actual) == max_chars


def test_get_col_name_normalize():
    col = F.col("a").alias("b")
    actual = get_column_name_or_alias(col, normalize=True)
    assert actual == "b"


def test_get_col_name_capital_normalize():
    col = F.col("A")
    actual = get_column_name_or_alias(col, normalize=True)
    assert actual == "a"


def test_get_col_name_alias_not_normalize():
    col = F.col("A").alias("B")
    actual = get_column_name_or_alias(col, normalize=True)
    assert actual == "B"


def test_get_col_name_capital():
    col = F.col("A").alias("B")
    actual = get_column_name_or_alias(col)
    assert actual == "B"


def test_get_col_name_normalize_complex_expr():
    col = F.try_element_at("col7", F.lit("key1").cast("string"))
    actual = get_column_name_or_alias(col, normalize=True)
    assert actual == "try_element_at_col7_cast_key1_as_string"


def test_get_col_name_alias_from_complex_expr():
    col = F.try_element_at("col7", F.lit("key1").cast("string")).alias("alias1")
    actual = get_column_name_or_alias(col)
    assert actual == "alias1"


def test_get_col_name_as_str():
    col = "a"
    actual = get_column_name_or_alias(col)
    assert actual == col


def test_get_col_name_expr_not_found():
    with pytest.raises(ValueError, match="Invalid column expression"):
        get_column_name_or_alias(Mock())


def test_get_col_name_not_simple_expression() -> None:
    with pytest.raises(ValueError, match="Unable to interpret column expression. Only simple references are allowed"):
        get_column_name_or_alias(F.col("a") + F.col("b"), allow_simple_expressions_only=True)


def test_get_col_name_from_string_not_simple_expression() -> None:
    with pytest.raises(ValueError, match="Unable to interpret column expression. Only simple references are allowed"):
        get_column_name_or_alias("a + b", allow_simple_expressions_only=True)


@pytest.mark.parametrize(
    "columns, expected_columns",
    [
        (["a", "b"], ["a", "b"]),
        ([F.col("a"), F.col("b")], ["a", "b"]),
        (["a", F.col("b")], ["a", "b"]),
    ],
)
def test_get_columns_as_strings(columns: list[str | Column], expected_columns: list[str | Column]):
    assert get_columns_as_strings(columns, allow_simple_expressions_only=False) == expected_columns


@pytest.mark.parametrize(
    "columns",
    [
        [F.col("a"), F.col("b") + F.lit(1)],
        [F.col("a") + F.col("b"), F.col("b")],
    ],
)
def test_get_columns_as_strings_allow_simple_expression_only(columns: list[str | Column]):
    with pytest.raises(ValueError, match="Unable to interpret column expression. Only simple references are allowed"):
        get_columns_as_strings(columns, allow_simple_expressions_only=True)


def test_valid_2_level_table_namespace():
    input_location = "db.table"
    input_format = None
    input_config = InputConfig(location=input_location, format=input_format)
    assert read_input_data(Mock(), input_config)


def test_valid_3_level_table_namespace():
    input_location = "catalog.schema.table"
    input_format = None
    input_config = InputConfig(location=input_location, format=input_format)
    assert read_input_data(Mock(), input_config)


def test_streaming_source():
    input_location = "catalog.schema.table"
    input_config = InputConfig(location=input_location, is_streaming=True)
    df = read_input_data(Mock(), input_config)
    assert df.isStreaming


def test_invalid_streaming_source_format():
    input_location = "/Volumes/catalog/schema/volume/"
    input_format = "json"
    input_config = InputConfig(location=input_location, format=input_format, is_streaming=True)
    with pytest.raises(ValueError, match="Streaming reads from file sources must use 'cloudFiles' format"):
        read_input_data(Mock(), input_config)


def test_input_location_missing_when_reading_input_data():
    input_config = InputConfig(location="")
    with pytest.raises(ValueError, match="Input location not configured"):
        read_input_data(Mock(), input_config)


def test_safe_query_with_similar_names():
    # Should be safe: keywords appear only as part of column or table names
    assert is_sql_query_safe("SELECT insert_, _delete, update_, * FROM insert_users_delete_update")


@pytest.mark.parametrize(
    "forbidden_statements",
    [
        "delete",
        "insert",
        "update",
        "drop",
        "truncate",
        "alter",
        "create",
        "replace",
        "grant",
        "revoke",
        "merge",
        "use",
        "refresh",
        "analyze",
        "optimize",
        "zorder",
    ],
)
def test_query_with_forbidden_commands(forbidden_statements):
    query = f"{forbidden_statements.upper()} something FROM table"
    assert not is_sql_query_safe(query), f"Query with '{forbidden_statements}' should be unsafe"


def test_case_insensitivity():
    assert not is_sql_query_safe("dElEtE FROM users")
    assert not is_sql_query_safe("InSeRT INTO users (id) VALUES (1)")
    assert not is_sql_query_safe("uPdAtE users SET name = 'Charlie'")


def test_query_with_comments_containing_keywords():
    # Should still be safe because we're not removing SQL comments
    # but in practice this would be flagged — optional to allow or disallow
    assert not is_sql_query_safe("SELECT * FROM users -- delete everything later")


def test_keyword_as_substring():
    assert is_sql_query_safe("SELECT * FROM deleted_users_log WHERE update_flag = true")


def test_multiple_statements_in_one_query():
    assert not is_sql_query_safe("SELECT * FROM users; DELETE FROM users")


def test_blank_query():
    assert is_sql_query_safe("  ")


def test_mixed_content():
    assert not is_sql_query_safe("WITH cte AS (UPDATE users SET x=1) SELECT * FROM cte")


def test_safe_json_load_dict():
    value = '{"key": "value"}'
    result = safe_json_load(value)
    assert result == {"key": "value"}


def test_safe_json_load_list():
    value = "[1, 2]"
    result = safe_json_load(value)
    assert result == [1, 2]


def test_safe_json_load_string():
    value = "col1"
    result = safe_json_load(value)
    assert result == value


def test_safe_json_load_int_as_string():
    value = "1"
    result = safe_json_load(value)
    assert result == 1


def test_safe_json_load_escaped_string():
    value = '"col1"'
    result = safe_json_load(value)
    assert result == "col1"


def test_safe_json_load_invalid_json():
    value = "{key: value}"  # treat it as string
    result = safe_json_load(value)
    assert result == value


def test_safe_json_load_empty_string():
    value = ""
    result = safe_json_load(value)
    assert result == value


def test_safe_json_load_non_string_arg():
    with pytest.raises(TypeError):
        safe_json_load(123)


@pytest.mark.parametrize(
    "column, expected",
    [
        ("valid_column", True),  # Valid column name
        ("validColumn123", True),  # Valid column name with numbers
        ("_valid_column", True),  # Valid column name starting with an underscore
        ("invalid column", False),  # Contains space
        ("invalid,column", False),  # Contains comma
        ("invalid;column", False),  # Contains semicolon
        ("invalid{column}", False),  # Contains curly braces
        ("invalid(column)", False),  # Contains parentheses
        ("invalid\ncolumn", False),  # Contains newline
        ("invalid\tcolumn", False),  # Contains tab
        ("invalid=column", False),  # Contains equals sign
    ],
)
def test_is_valid_column_name(column: str, expected: bool):
    assert is_valid_column_name(column) == expected


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Primitives
        ("string", "string"),
        (123, 123),
        (45.67, 45.67),
        (True, True),
        # Dates
        (date(2023, 1, 1), "2023-01-01"),
        (datetime(2023, 1, 1, 12, 0, 0), "2023-01-01 12:00:00"),
        # Collections
        ([1, 2, 3], [1, 2, 3]),
        ((4, 5, 6), [4, 5, 6]),
        # PySpark Column
        (F.col("col_name"), "col_name"),
        (F.col("col_name"), "col_name"),
    ],
)
def test_normalize_bound_args(input_value: Any, expected_output: Any):
    assert normalize_bound_args(input_value) == expected_output


def test_normalize_bound_args_unsupported_type():
    with pytest.raises(TypeError, match="Unsupported type for normalization"):
        normalize_bound_args({"a": 1})
