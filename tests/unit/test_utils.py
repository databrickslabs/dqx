from unittest.mock import Mock
import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.utils import read_input_data, get_column_as_string, is_sql_query_safe, normalize_col_str
from databricks.labs.dqx.config import InputConfig


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


def test_normalize_col_str():
    long_str = "a" * 300
    actual = normalize_col_str(long_str)
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
