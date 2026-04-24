import logging
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from pathlib import Path
from unittest.mock import Mock
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from databricks.labs.dqx.io import read_input_data, get_reference_dataframes
from databricks.labs.dqx.utils import (
    get_column_name_or_alias,
    is_sql_query_safe,
    normalize_col_str,
    safe_json_load,
    get_columns_as_strings,
    is_simple_column_expression,
    safe_strip_file_from_path,
    missing_required_packages,
    get_file_extension,
    resolve_variables,
)
from databricks.labs.dqx.rule import normalize_bound_args
from databricks.labs.dqx.errors import InvalidParameterError, InvalidConfigError
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.pii.nlp_engine_config import NLPEngineConfig


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
    with pytest.raises(InvalidParameterError, match="Invalid column expression"):
        get_column_name_or_alias(Mock())


def test_get_col_name_with_trailing_crlf():
    """Serverless v5 may append CRLF to the column string representation,
    which would prevent the end-of-string anchor in COLUMN_PATTERN from matching."""
    col = Mock()
    col.__str__ = Mock(return_value="Column<'!(EXISTS (SELECT 1 FROM t WHERE x = a))'>\r\n")
    actual = get_column_name_or_alias(col, normalize=True)
    assert actual == "_exists_select_1_from_t_where_x_a"


def test_get_col_name_not_simple_expression() -> None:
    with pytest.raises(
        InvalidParameterError, match="Unable to interpret column expression. Only simple references are allowed"
    ):
        get_column_name_or_alias(F.col("a") + F.col("b"), allow_simple_expressions_only=True)


def test_get_col_name_from_string_not_simple_expression() -> None:
    with pytest.raises(
        InvalidParameterError, match="Unable to interpret column expression. Only simple references are allowed"
    ):
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
    with pytest.raises(
        InvalidParameterError, match="Unable to interpret column expression. Only simple references are allowed"
    ):
        get_columns_as_strings(columns, allow_simple_expressions_only=True)


def test_valid_2_level_table_namespace(mock_spark):
    input_location = "db.table"
    input_format = None
    input_config = InputConfig(location=input_location, format=input_format)
    assert read_input_data(mock_spark, input_config)


def test_valid_3_level_table_namespace(mock_spark):
    input_location = "catalog.schema.table"
    input_format = None
    input_config = InputConfig(location=input_location, format=input_format)
    assert read_input_data(mock_spark, input_config)


def test_streaming_source(mock_spark):
    input_location = "catalog.schema.table"
    input_config = InputConfig(location=input_location, is_streaming=True)
    df = read_input_data(mock_spark, input_config)
    assert df.isStreaming


def test_invalid_streaming_source_format(mock_spark):
    input_location = "/Volumes/catalog/schema/volume/"
    input_format = "json"
    input_config = InputConfig(location=input_location, format=input_format, is_streaming=True)
    with pytest.raises(InvalidConfigError, match="Streaming reads from file sources must use 'cloudFiles' format"):
        read_input_data(mock_spark, input_config)


def test_input_location_missing_when_reading_input_data(mock_spark):
    input_config = InputConfig(location="")
    with pytest.raises(InvalidConfigError, match="Input location not configured"):
        read_input_data(mock_spark, input_config)


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
def test_is_simple_column_expression(column: str, expected: bool):
    assert is_simple_column_expression(column) == expected


@pytest.mark.parametrize(
    "input_value, expected_output",
    [
        # Primitives
        ("string", "string"),
        (123, 123),
        (45.67, 45.67),
        (True, True),
        (False, False),
        # Dates
        (date(2023, 1, 1), "2023-01-01"),
        (datetime(2023, 1, 1, 12, 0, 0), "2023-01-01 12:00:00"),
        # Collections
        ([1, 2, 3], [1, 2, 3]),
        ((4, 5, 6), [4, 5, 6]),
        # PySpark Column
        (F.col("col_name"), "col_name"),
        (F.col("a").alias("b"), "b"),
    ],
)
def test_normalize_bound_args(input_value: Any, expected_output: Any):
    assert normalize_bound_args(input_value) == expected_output


def test_normalize_bound_args_unsupported_type():
    with pytest.raises(TypeError, match="Unsupported type for normalization"):
        normalize_bound_args(object())


def test_normalize_bound_args_handle_none():
    assert normalize_bound_args(None) is None


def test_normalize_bound_args_complex_column_default_rejects():
    """Default allow_simple_expressions_only=True rejects complex Column expressions."""
    with pytest.raises(InvalidParameterError, match="Only simple references are allowed"):
        normalize_bound_args(F.try_element_at("arr_col", F.lit(1)))


def test_normalize_bound_args_complex_column_allowed_when_opted_in():
    """allow_simple_expressions_only=False accepts complex Column expressions for serialization."""
    result = normalize_bound_args(F.try_element_at("arr_col", F.lit(1)), allow_simple_expressions_only=False)
    assert "try_element_at" in result


def test_complex_column_serialized_for_fingerprinting_is_not_round_trip_safe():
    """Serializing a complex Column with allow_simple_expressions_only=False is a one-way operation.

    The resulting string contains the expression details but is NOT a valid simple column name.
    Round-tripping it through normalize_bound_args (default allow_simple_expressions_only=True)
    returns it as a plain string — it is never reconstructed as the original Column expression.
    This confirms that dicts produced by DQRule.to_dict() (which uses allow_simple_expressions_only=False)
    cannot be fed back into apply_checks_by_metadata to recover the original Column behaviour.
    """
    complex_col = F.try_element_at("arr_col", F.lit(1))

    serialized = normalize_bound_args(complex_col, allow_simple_expressions_only=False)

    # Result is a string containing expression details, not a Column object
    assert isinstance(serialized, str)
    assert "try_element_at" in serialized

    # The string is NOT a valid simple column reference (contains special chars like parentheses)
    assert not is_simple_column_expression(serialized)

    # Round-trip: normalize_bound_args treats the string as a plain column-name string,
    # it does NOT reconstruct the original Column expression
    round_tripped = normalize_bound_args(serialized)
    assert isinstance(round_tripped, str)
    assert round_tripped == serialized  # unchanged — no reconstruction happened


def test_normalize_bound_args_struct_type():
    """StructType is normalized to simpleString for fingerprinting (e.g. has_valid_schema with df.schema)."""
    schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
    result = normalize_bound_args(schema)
    assert result == "struct<id:int,name:string>"


def test_normalize_bound_args_dict():
    """Dict is recursively normalized for fingerprinting (e.g. custom checks with dict args)."""
    result = normalize_bound_args({"min": 1, "max": 10})
    assert result == {"min": 1, "max": 10}

    result = normalize_bound_args({"nested": {"a": 1, "b": "x"}})
    assert result == {"nested": {"a": 1, "b": "x"}}


def test_normalize_bound_args_set():
    """Set is recursively normalized to list for fingerprinting."""
    result = normalize_bound_args({1, 2, 3})
    assert sorted(result) == [1, 2, 3]


def test_normalize_bound_args_decimal():
    """Decimal is normalized to __decimal__ format for round-trip serialization."""
    result = normalize_bound_args(Decimal("123.45"))
    assert result == {"__decimal__": "123.45"}


def test_normalize_bound_args_nested_collections():
    """Nested dict/list/set with Decimal and Enum are recursively normalized."""

    class TestEnum(Enum):
        X = "x"

    result = normalize_bound_args({"a": [Decimal("1.0"), TestEnum.X], "b": {1, 2}})
    assert result["a"] == [{"__decimal__": "1.0"}, "x"]
    assert sorted(result["b"]) == [1, 2]


def test_normalize_bound_args_frozenset():
    """Frozenset is recursively normalized for fingerprinting."""
    result = normalize_bound_args(frozenset({1, 2, 3}))
    assert sorted(result) == [1, 2, 3]


def test_normalize_bound_args_enum():
    """Enum (e.g. NLPEngineConfig, Criticality) is normalized to its value for fingerprinting."""

    class TestEnum(Enum):
        FOO = "foo_value"
        BAR = {"key": "val"}

    assert normalize_bound_args(TestEnum.FOO) == "foo_value"
    assert normalize_bound_args(TestEnum.BAR) == {"key": "val"}


def test_normalize_bound_args_nlp_engine_config():
    """NLPEngineConfig enum from PII module is normalized (used by does_not_contain_pii)."""
    result = normalize_bound_args(NLPEngineConfig.SPACY_SMALL)
    assert result == {"nlp_engine_name": "spacy", "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}]}


def test_get_reference_dataframes_with_missing_ref_tables(mock_spark) -> None:
    assert get_reference_dataframes(mock_spark, reference_tables={}) is None
    assert get_reference_dataframes(mock_spark, reference_tables=None) is None


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/path/to/file.txt", "/path/to"),  # File with extension
        ("/path/to/dir", "/path/to/dir"),  # Directory path
        ("folder", "folder"),  # Single folder
        ("folder/", "folder"),  # Folder with trailing slash
        ("folder/dir/", "folder/dir"),  # Nested folder with trailing slash
        ("folder/dir", "folder/dir"),  # Nested folder
        ("", ""),  # Empty path
        ("file.txt", ""),  # File in current dir
        ("/file.with.dots.ext", "/"),  # File in root
        ("folder/file.with.dots.ext", "folder"),  # File inside folder
        ("folder/.hiddenfile.yml", "folder"),  # Hidden file in folder
        (
            "/Users/marcin.wojtyczka@databricks.com/.corespondency-predeterminer/",
            "/Users/marcin.wojtyczka@databricks.com/.corespondency-predeterminer",
        ),
        (
            "/Volume/catalog/schema/dir/checks.json",
            "/Volume/catalog/schema/dir",
        ),
    ],
)
def test_safe_strip_file_from_path(path: str, expected: str):
    assert safe_strip_file_from_path(path) == expected


@pytest.mark.parametrize(
    "packages,expected",
    [
        (["os", "json"], False),
        (["os", "definitely_not_a_real_package"], True),
        (["not_a_real_package1", "not_a_real_package2"], True),
        ([], False),
    ],
)
def test_missing_required_packages(packages, expected):
    assert missing_required_packages(packages) == expected


@pytest.mark.parametrize(
    "file_path, expected_extension",
    [
        ("/path/to/file.json", ".json"),
        ("/path/to/file.yaml", ".yaml"),
        ("/path/to/file.yml", ".yml"),
        ("file.json", ".json"),
        ("file.yaml", ".yaml"),
        ("file.yml", ".yml"),
        ("/path/to/file", ""),
        ("file", ""),
        ("/path/to/file.JSON", ".JSON"),  # Case preserved, will be lowercased by serializer
        ("/path/to/file.YAML", ".YAML"),
        ("/path/to/file.with.multiple.dots.json", ".json"),
        ("", ""),
    ],
)
def test_get_file_extension(file_path: str, expected_extension: str):
    """Test get_file_extension function with various file paths."""
    assert get_file_extension(file_path) == expected_extension


def test_get_file_extension_with_path_object():
    """Test get_file_extension function with Path object."""
    file_path = Path("/path/to/file.json")
    assert get_file_extension(file_path) == ".json"


def test_resolve_variables_replaces_all_string_fields():
    checks = [
        {
            "criticality": "error",
            "name": "{{ col }}_not_null",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "{{ col }}"},
            },
            "filter": "{{ filter_col }} = 'active'",
        }
    ]
    variables = {"col": "email", "filter_col": "status"}
    result = resolve_variables(checks, variables)

    assert result[0]["name"] == "email_not_null"
    assert result[0]["check"]["arguments"]["column"] == "email"
    assert result[0]["filter"] == "status = 'active'"


def test_resolve_variables_empty_variables():
    checks = [{"name": "{{ x }}"}]
    result = resolve_variables(checks, {})
    assert result is checks  # same object, no copy
    assert result[0]["name"] == "{{ x }}"


def test_resolve_variables_non_string_values_converted():
    checks = [
        {
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "{{ col }} > {{ threshold }}"},
            },
        }
    ]
    variables = {"col": "age", "threshold": 18}
    result = resolve_variables(checks, variables)
    assert result[0]["check"]["arguments"]["expression"] == "age > 18"


def test_resolve_variables_does_not_mutate_original():
    checks = [
        {
            "name": "{{ col }}_check",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "{{ col }}"},
            },
        }
    ]
    variables = {"col": "name"}
    resolve_variables(checks, variables)

    # Original must be unchanged
    assert checks[0]["name"] == "{{ col }}_check"
    assert checks[0]["check"]["arguments"]["column"] == "{{ col }}"


def test_resolve_variables_nested_dicts():
    checks = [
        {
            "check": {
                "function": "sql_expression",
                "arguments": {
                    "expression": "{{ col }} IS NOT NULL",
                },
            },
            "user_metadata": {"owner": "{{ team }}"},
        }
    ]
    variables = {"col": "id", "team": "data-eng"}
    result = resolve_variables(checks, variables)

    assert result[0]["check"]["arguments"]["expression"] == "id IS NOT NULL"
    assert result[0]["user_metadata"]["owner"] == "data-eng"


def test_resolve_variables_partial_replacement():
    checks = [{"name": "{{ p1 }}_greater_than_{{ threshold }}"}]
    variables = {"p1": "column1", "threshold": 10}
    result = resolve_variables(checks, variables)
    assert result[0]["name"] == "column1_greater_than_10"


def test_resolve_variables_unresolved_placeholder_warning(caplog):
    checks = [{"name": "{{ resolved }}_{{ unresolved }}"}]
    variables = {"resolved": "ok"}
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.utils"):
        result = resolve_variables(checks, variables)

    assert result[0]["name"] == "ok_{{ unresolved }}"
    assert any("Unresolved placeholder" in msg for msg in caplog.messages)


def test_resolve_variables_whitespace_tolerance():
    checks = [
        {"a": "{{x}}", "b": "{{ x }}", "c": "{{  x  }}"},
    ]
    variables = {"x": "val"}
    result = resolve_variables(checks, variables)
    assert result[0]["a"] == "val"
    assert result[0]["b"] == "val"
    assert result[0]["c"] == "val"


def test_resolve_variables_non_string_dict_values_untouched():
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "{{ col }}", "allowed": [1, 2, 3]},
            },
        }
    ]
    variables = {"col": "status"}
    result = resolve_variables(checks, variables)
    assert result[0]["check"]["arguments"]["column"] == "status"
    assert result[0]["check"]["arguments"]["allowed"] == [1, 2, 3]
    assert result[0]["criticality"] == "error"


def test_resolve_variables_for_each_column():
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_not_null",
                "for_each_column": ["{{ col1 }}", "{{ col2 }}"],
            },
        }
    ]
    variables = {"col1": "first_name", "col2": "last_name"}
    result = resolve_variables(checks, variables)
    assert result[0]["check"]["for_each_column"] == ["first_name", "last_name"]


def test_resolve_variables_multiple_checks():
    checks = [
        {
            "name": "{{ col }}_not_null",
            "check": {"function": "is_not_null", "arguments": {"column": "{{ col }}"}},
        },
        {
            "name": "{{ col2 }}_not_empty",
            "check": {"function": "is_not_empty", "arguments": {"column": "{{ col2 }}"}},
        },
    ]
    variables = {"col": "a", "col2": "b"}
    result = resolve_variables(checks, variables)
    assert result[0]["name"] == "a_not_null"
    assert result[0]["check"]["arguments"]["column"] == "a"
    assert result[1]["name"] == "b_not_empty"
    assert result[1]["check"]["arguments"]["column"] == "b"


def test_resolve_variables_empty_checks_list():
    result = resolve_variables([], {"col": "x"})
    assert result == []


def test_resolve_variables_empty_string_value():
    checks = [{"name": "prefix_{{ col }}_suffix"}]
    result = resolve_variables(checks, {"col": ""})
    assert result[0]["name"] == "prefix__suffix"


def test_resolve_variables_value_contains_braces():
    """Variable value itself contains {{ }} — should NOT be re-expanded."""
    checks = [{"expr": "{{ col }}"}]
    result = resolve_variables(checks, {"col": "{{ other }}"})
    assert result[0]["expr"] == "{{ other }}"


def test_resolve_variables_key_with_regex_special_chars():
    """Variable keys with regex metacharacters must be escaped properly."""
    checks = [{"name": "{{ col.name }}_check", "filter": "{{ col+1 }} > 0"}]
    variables = {"col.name": "revenue", "col+1": "amount"}
    result = resolve_variables(checks, variables)
    assert result[0]["name"] == "revenue_check"
    assert result[0]["filter"] == "amount > 0"


def test_resolve_variables_same_placeholder_repeated_in_string():
    checks = [{"expr": "{{ x }} + {{ x }}"}]
    result = resolve_variables(checks, {"x": "col"})
    assert result[0]["expr"] == "col + col"


def test_resolve_variables_deeply_nested():
    checks = [{"a": {"b": {"c": {"d": "{{ v }}"}}}}]
    result = resolve_variables(checks, {"v": "deep"})
    assert result[0]["a"]["b"]["c"]["d"] == "deep"


def test_resolve_variables_value_with_backslash():
    """Backslashes in values should be treated literally (no regex group refs)."""
    checks = [{"path": "{{ p }}"}]
    result = resolve_variables(checks, {"p": r"C:\Users\test"})
    assert result[0]["path"] == r"C:\Users\test"


def test_resolve_variables_rejects_list_value():
    checks = [{"check": {"arguments": {"column": "{{ col }}"}}}]
    with pytest.raises(InvalidParameterError, match="unsupported type 'list'"):
        resolve_variables(checks, {"col": ["a", "b"]})


def test_resolve_variables_rejects_dict_value():
    checks = [{"check": {"arguments": {"column": "{{ col }}"}}}]
    with pytest.raises(InvalidParameterError, match="unsupported type 'dict'"):
        resolve_variables(checks, {"col": {"nested": "value"}})


def test_resolve_variables_accepts_decimal_value():
    checks = [{"expr": "col > {{ threshold }}"}]
    result = resolve_variables(checks, {"threshold": Decimal("3.14")})
    assert result[0]["expr"] == "col > 3.14"


def test_resolve_variables_accepts_bool_value():
    checks = [{"expr": "{{ flag }}"}]
    result = resolve_variables(checks, {"flag": True})
    assert result[0]["expr"] == "True"


def test_resolve_variables_false_bool():
    checks = [{"expr": "{{ flag }}"}]
    result = resolve_variables(checks, {"flag": False})
    assert result[0]["expr"] == "False"


def test_resolve_variables_rejects_none_value():
    checks = [{"col": "{{ col }}"}]
    with pytest.raises(InvalidParameterError, match="unsupported type 'NoneType'"):
        resolve_variables(checks, {"col": None})


def test_resolve_variables_rejects_set_value():
    checks = [{"col": "{{ col }}"}]
    with pytest.raises(InvalidParameterError, match="unsupported type 'set'"):
        resolve_variables(checks, {"col": {1, 2}})


def test_resolve_variables_rejects_tuple_value():
    checks = [{"col": "{{ col }}"}]
    with pytest.raises(InvalidParameterError, match="unsupported type 'tuple'"):
        resolve_variables(checks, {"col": (1, 2)})


def test_resolve_variables_dict_keys_not_substituted():
    checks = [{"{{ col }}": "value", "other": "{{ col }}"}]
    result = resolve_variables(checks, {"col": "replaced"})
    assert "{{ col }}" in result[0]
    assert result[0]["{{ col }}"] == "value"
    assert result[0]["other"] == "replaced"


def test_resolve_variables_nan():
    checks = [{"expr": "{{ val }}"}]
    result = resolve_variables(checks, {"val": float("nan")})
    assert result[0]["expr"] == "nan"


def test_resolve_variables_inf():
    checks = [{"expr": "{{ val }}"}]
    result = resolve_variables(checks, {"val": float("inf")})
    assert result[0]["expr"] == "inf"


def test_resolve_variables_multiple_unresolved_warns(caplog):
    checks = [{"expr": "{{ a }} and {{ b }}"}]
    with caplog.at_level(logging.WARNING):
        result = resolve_variables(checks, {"a": "x"})
    assert result[0]["expr"] == "x and {{ b }}"
    assert any("Unresolved placeholder" in msg for msg in caplog.messages)


def test_resolve_variables_none_vars_no_warning(caplog):
    checks = [{"col": "{{ x }}"}]
    with caplog.at_level(logging.WARNING):
        result = resolve_variables(checks, None)
    assert result[0]["col"] == "{{ x }}"
    assert not any("Unresolved placeholder" in msg for msg in caplog.messages)

    with caplog.at_level(logging.WARNING):
        result = resolve_variables(checks, {})
    assert result[0]["col"] == "{{ x }}"
    assert not any("Unresolved placeholder" in msg for msg in caplog.messages)


def test_resolve_variables_unicode_values():
    checks = [{"col": "{{ col }}"}]
    result = resolve_variables(checks, {"col": "prénom"})
    assert result[0]["col"] == "prénom"


def test_resolve_variables_accepts_date():
    checks = [{"expr": "date > '{{ d }}'"}]
    result = resolve_variables(checks, {"d": date(2024, 1, 15)})
    assert result[0]["expr"] == "date > '2024-01-15'"


def test_resolve_variables_accepts_datetime():
    checks = [{"expr": "ts > '{{ ts }}'"}]
    result = resolve_variables(checks, {"ts": datetime(2024, 1, 15, 10, 30)})
    assert "2024-01-15" in result[0]["expr"]


def test_resolve_variables_accepts_time():
    checks = [{"expr": "t > '{{ t }}'"}]
    result = resolve_variables(checks, {"t": time(10, 30)})
    assert result[0]["expr"] == "t > '10:30:00'"
