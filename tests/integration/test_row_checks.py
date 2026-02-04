from datetime import datetime
from decimal import Decimal
import pytest
import pyspark.sql.functions as F
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from pyspark.errors import AnalysisException

from databricks.labs.dqx.check_funcs import (
    is_equal_to,
    is_not_equal_to,
    is_in_range,
    is_not_empty,
    is_not_in_range,
    is_not_null,
    is_not_null_and_not_empty,
    is_older_than_col2_for_n_days,
    is_older_than_n_days,
    is_not_in_future,
    is_not_in_near_future,
    is_not_less_than,
    is_not_greater_than,
    regex_match,
    sql_expression,
    is_in_list,
    is_not_in_list,
    is_not_null_and_is_in_list,
    is_not_null_and_not_empty_array,
    is_valid_date,
    is_valid_json,
    has_json_keys,
    has_valid_json_schema,
    is_valid_timestamp,
    is_valid_ipv4_address,
    is_ipv4_address_in_cidr,
    is_valid_ipv6_address,
    is_ipv6_address_in_cidr,
    is_data_fresh,
    is_null,
    is_empty,
    is_null_or_empty,
)
from databricks.labs.dqx.pii import pii_detection_funcs
from databricks.labs.dqx.errors import InvalidParameterError

SCHEMA = "a: string, b: int"


def test_col_is_not_null_and_not_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>, e: string"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"], "str1"],
            ["", None, {"val": ""}, [None, "a"], ""],
            [" ", 3, {"val": None}, ["", "a"], " "],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null_and_not_empty("a"),
        is_not_null_and_not_empty("b", True),
        is_not_null_and_not_empty(F.col("c").getItem("val")),
        is_not_null_and_not_empty(F.try_element_at("d", F.lit(1))),
        is_not_null_and_not_empty("e", trim_strings=True),
    )

    checked_schema = (
        "a_is_null_or_empty: string, "
        + "b_is_null_or_empty: string, "
        + "unresolvedextractvalue_c_val_is_null_or_empty: string, "
        + "try_element_at_d_1_is_null_or_empty: string, "
        + "e_is_null_or_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None],
            [
                "Column 'a' value is null or empty",
                "Column 'b' value is null or empty",
                "Column 'UnresolvedExtractValue(c, val)' value is null or empty",
                "Column 'try_element_at(d, 1)' value is null or empty",
                "Column 'e' value is null or empty",
            ],
            [
                None,
                None,
                "Column 'UnresolvedExtractValue(c, val)' value is null or empty",
                "Column 'try_element_at(d, 1)' value is null or empty",
                "Column 'e' value is null or empty",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>, e: string"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"], "str1"],
            ["", None, {"val": ""}, [None, "a"], ""],
            [" ", 3, {"val": None}, ["", "a"], " "],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_empty("a"),
        is_not_empty("b"),
        is_not_empty(F.col("c").getItem("val")),
        is_not_empty(F.try_element_at("d", F.lit(1))),
        is_not_empty("e", trim_strings=True),
    )

    checked_schema = (
        "a_is_empty: string, "
        + "b_is_empty: string, "
        + "unresolvedextractvalue_c_val_is_empty: string, "
        + "try_element_at_d_1_is_empty: string, "
        + "e_is_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "Column 'a' value is empty",
                None,
                "Column 'UnresolvedExtractValue(c, val)' value is empty",
                None,
                "Column 'e' value is empty",
            ],
            [
                None,
                None,
                None,
                "Column 'try_element_at(d, 1)' value is empty",
                "Column 'e' value is empty",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["", None, {"val": ""}, [None, "a"]],
            [" ", 3, {"val": None}, ["", "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null("a"),
        is_not_null("b"),
        is_not_null(F.col("c").getItem("val")),
        is_not_null(F.try_element_at("d", F.lit(1))),
    )

    checked_schema = (
        "a_is_null: string, "
        + "b_is_null: string, "
        + "unresolvedextractvalue_c_val_is_null: string, "
        + "try_element_at_d_1_is_null: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None],
            [None, "Column 'b' value is null", None, "Column 'try_element_at(d, 1)' value is null"],
            [None, None, "Column 'UnresolvedExtractValue(c, val)' value is null", None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_null(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["", None, {"val": ""}, [None, "a"]],
            [" ", 3, {"val": None}, ["", "a"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_null("a"),
        is_null("b"),
        is_null(F.col("c").getItem("val")),
        is_null(F.try_element_at("d", F.lit(1))),
    )

    checked_schema = (
        "a_is_not_null: string, "
        + "b_is_not_null: string, "
        + "unresolvedextractvalue_c_val_is_not_null: string, "
        + "try_element_at_d_1_is_not_null: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Column 'a' value is not null",
                "Column 'b' value is not null",
                "Column 'UnresolvedExtractValue(c, val)' value is not null",
                "Column 'try_element_at(d, 1)' value is not null",
            ],
            ["Column 'a' value is not null", None, "Column 'UnresolvedExtractValue(c, val)' value is not null", None],
            [
                "Column 'a' value is not null",
                "Column 'b' value is not null",
                None,
                "Column 'try_element_at(d, 1)' value is not null",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>, e: string"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"], "str1"],
            ["", None, {"val": ""}, [None, "a"], ""],
            [" ", 3, {"val": None}, ["", "a"], " "],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_empty("a"),
        is_empty("b"),
        is_empty(F.col("c").getItem("val")),
        is_empty(F.try_element_at("d", F.lit(1))),
        is_empty("e", trim_strings=True),
    )

    checked_schema = (
        "a_is_not_empty: string, "
        + "b_is_not_empty: string, "
        + "unresolvedextractvalue_c_val_is_not_empty: string, "
        + "try_element_at_d_1_is_not_empty: string, "
        + "e_is_not_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Column 'a' value is not empty",
                "Column 'b' value is not empty",
                "Column 'UnresolvedExtractValue(c, val)' value is not empty",
                "Column 'try_element_at(d, 1)' value is not empty",
                "Column 'e' value is not empty",
            ],
            [
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "Column 'a' value is not empty",
                "Column 'b' value is not empty",
                None,
                None,
                None,
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_null_or_empty(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>, e: string"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"], "str1"],
            ["", None, {"val": ""}, [None, "a"], ""],
            [" ", 3, {"val": None}, ["", "a"], " "],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_null_or_empty("a"),
        is_null_or_empty("b"),
        is_null_or_empty(F.col("c").getItem("val")),
        is_null_or_empty(F.try_element_at("d", F.lit(1))),
        is_null_or_empty("e", trim_strings=True),
    )

    checked_schema = (
        "a_is_not_null_and_not_empty: string, "
        + "b_is_not_null_and_not_empty: string, "
        + "unresolvedextractvalue_c_val_is_not_null_and_not_empty: string, "
        + "try_element_at_d_1_is_not_null_and_not_empty: string, "
        + "e_is_not_null_and_not_empty: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Column 'a' value is not null and not empty",
                "Column 'b' value is not null and not empty",
                "Column 'UnresolvedExtractValue(c, val)' value is not null and not empty",
                "Column 'try_element_at(d, 1)' value is not null and not empty",
                "Column 'e' value is not null and not empty",
            ],
            [
                None,
                None,
                None,
                None,
                None,
            ],
            [
                "Column 'a' value is not null and not empty",
                "Column 'b' value is not null and not empty",
                None,
                None,
                None,
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null_and_is_in_list(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["str2", None, {"val": "str2"}, [None, "a"]],
            [" ", 3, {"val": " "}, [None, " "]],
            ["STR1", 4, {"val": "A"}, ["B", "c"]],
            ["str5", 5, {"val": "e"}, ["b", "C"]],
        ],
        input_schema,
    )

    actual = test_df.select(
        is_not_null_and_is_in_list("a", ["str1"]),
        is_not_null_and_is_in_list("b", [F.lit(3)]),
        is_not_null_and_is_in_list(F.col("c").getItem("val"), [F.lit("a")]),
        is_not_null_and_is_in_list(F.try_element_at("d", F.lit(2)), ["b"]),
        is_not_null_and_is_in_list("a", ["str1"], case_sensitive=False),
        is_not_null_and_is_in_list(F.col("c").getItem("val"), [F.lit("a")], case_sensitive=False),
        is_not_null_and_is_in_list(F.try_element_at("d", F.lit(2)), ["b"], case_sensitive=False),
        is_not_null_and_is_in_list("d", [["a", "b"], ["B", "c"]]),
        is_not_null_and_is_in_list("d", [["a", "b"], ["B", "c"]], case_sensitive=False),
    )

    checked_schema = (
        "a_is_null_or_is_not_in_the_list: string, "
        + "b_is_null_or_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_null_or_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_null_or_is_not_in_the_list: string, "
        + "a_is_null_or_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_null_or_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_null_or_is_not_in_the_list: string, "
        + "d_is_null_or_is_not_in_the_list: string, "
        + "d_is_null_or_is_not_in_the_list: string"
    )
    col_d_null = None
    expected = spark.createDataFrame(
        [
            [
                None,
                "Value '1' in Column 'b' is null or not in the allowed list: [3]",
                None,
                None,
                None,
                None,
                None,
                None,
                col_d_null,
            ],
            [
                "Value 'str2' in Column 'a' is null or not in the allowed list: [str1]",
                "Value 'null' in Column 'b' is null or not in the allowed list: [3]",
                "Value 'str2' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value 'a' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value 'str2' in Column 'a' is null or not in the allowed list: [str1]",
                "Value 'str2' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value 'a' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value '[null, a]' in Column 'd' is null or not in the allowed list: [[a, b], [B, c]]",
                "Value '[null, a]' in Column 'd' is null or not in the allowed list: [[a, b], [B, c]]",
            ],
            [
                "Value ' ' in Column 'a' is null or not in the allowed list: [str1]",
                None,
                "Value ' ' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value ' ' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value ' ' in Column 'a' is null or not in the allowed list: [str1]",
                "Value ' ' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value ' ' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value '[null,  ]' in Column 'd' is null or not in the allowed list: [[a, b], [B, c]]",
                "Value '[null,  ]' in Column 'd' is null or not in the allowed list: [[a, b], [B, c]]",
            ],
            [
                "Value 'STR1' in Column 'a' is null or not in the allowed list: [str1]",
                "Value '4' in Column 'b' is null or not in the allowed list: [3]",
                "Value 'A' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value 'c' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                None,
                None,
                "Value 'c' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                None,
                None,
            ],
            [
                "Value 'str5' in Column 'a' is null or not in the allowed list: [str1]",
                "Value '5' in Column 'b' is null or not in the allowed list: [3]",
                "Value 'e' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value 'C' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value 'str5' in Column 'a' is null or not in the allowed list: [str1]",
                "Value 'e' in Column 'UnresolvedExtractValue(c, val)' is null or not in the allowed list: [a]",
                "Value 'C' in Column 'try_element_at(d, 2)' is null or not in the allowed list: [b]",
                "Value '[b, C]' in Column 'd' is null or not in the allowed list: [[a, b], [B, c]]",
                None,
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_list(spark):
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", 1, {"val": "a"}, ["a", "b"]],
            ["str2", None, {"val": "str2"}, [None, "a"]],
            [" ", 3, {"val": None}, [None, "a"]],
            ["STR1", 4, {"val": "A"}, ["B", "c"]],
            ["str5", 5, {"val": "e"}, ["b", "C"]],
        ],
        input_schema,
    )
    actual = test_df.select(
        is_in_list("a", ["str1"]),
        is_in_list("b", [F.lit(3)]),
        is_in_list(F.col("c").getItem("val"), [F.lit("a")]),
        is_in_list(F.try_element_at("d", F.lit(2)), ["b"]),
        is_in_list("a", ["str1"], case_sensitive=False),
        is_in_list(F.col("c").getItem("val"), [F.lit("a")], case_sensitive=False),
        is_in_list(F.try_element_at("d", F.lit(2)), ["b"], case_sensitive=False),
        is_in_list("d", [["a", "b"], ["B", "c"]]),
        is_in_list("d", [["a", "b"], ["B", "c"]], case_sensitive=False),
    )
    checked_schema = (
        "a_is_not_in_the_list: string, "
        + "b_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_not_in_the_list: string, "
        + "a_is_not_in_the_list: string, "
        + "unresolvedextractvalue_c_val_is_not_in_the_list: string, "
        + "try_element_at_d_2_is_not_in_the_list: string, "
        + "d_is_not_in_the_list: string, "
        + "d_is_not_in_the_list: string"
    )
    expected = spark.createDataFrame(
        [
            [None, "Value '1' in Column 'b' is not in the allowed list: [3]", None, None, None, None, None, None, None],
            [
                "Value 'str2' in Column 'a' is not in the allowed list: [str1]",
                None,
                "Value 'str2' in Column 'UnresolvedExtractValue(c, val)' is not in the allowed list: [a]",
                "Value 'a' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value 'str2' in Column 'a' is not in the allowed list: [str1]",
                "Value 'str2' in Column 'UnresolvedExtractValue(c, val)' is not in the allowed list: [a]",
                "Value 'a' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value '[null, a]' in Column 'd' is not in the allowed list: [[a, b], [B, c]]",
                "Value '[null, a]' in Column 'd' is not in the allowed list: [[a, b], [B, c]]",
            ],
            [
                "Value ' ' in Column 'a' is not in the allowed list: [str1]",
                None,
                None,
                "Value 'a' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value ' ' in Column 'a' is not in the allowed list: [str1]",
                None,
                "Value 'a' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value '[null, a]' in Column 'd' is not in the allowed list: [[a, b], [B, c]]",
                "Value '[null, a]' in Column 'd' is not in the allowed list: [[a, b], [B, c]]",
            ],
            [
                "Value 'STR1' in Column 'a' is not in the allowed list: [str1]",
                "Value '4' in Column 'b' is not in the allowed list: [3]",
                "Value 'A' in Column 'UnresolvedExtractValue(c, val)' is not in the allowed list: [a]",
                "Value 'c' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                None,
                None,
                "Value 'c' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                None,
                None,
            ],
            [
                "Value 'str5' in Column 'a' is not in the allowed list: [str1]",
                "Value '5' in Column 'b' is not in the allowed list: [3]",
                "Value 'e' in Column 'UnresolvedExtractValue(c, val)' is not in the allowed list: [a]",
                "Value 'C' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value 'str5' in Column 'a' is not in the allowed list: [str1]",
                "Value 'e' in Column 'UnresolvedExtractValue(c, val)' is not in the allowed list: [a]",
                "Value 'C' in Column 'try_element_at(d, 2)' is not in the allowed list: [b]",
                "Value '[b, C]' in Column 'd' is not in the allowed list: [[a, b], [B, c]]",
                None,
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_list_mismatch_datatype(spark):
    input_schema = "a: string, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", ["a", "b"]],
        ],
        input_schema,
    )
    actual = test_df.select(
        is_in_list("d", ["a", "b"]),  # wrong data type
    )
    with pytest.raises(AnalysisException, match="[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"):
        actual.count()


def test_col_is_not_null_and_is_in_list_mismatch_datatype(spark):
    input_schema = "a: string, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["str1", ["a", "b"]],
        ],
        input_schema,
    )
    actual = test_df.select(
        is_not_null_and_is_in_list("d", ["a", "b"]),  # wrong data type
    )
    with pytest.raises(AnalysisException, match="[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"):
        actual.count()


def test_is_not_in_list(spark):
    """Test is_not_in_list check function - blacklist functionality."""
    input_schema = "a: string, b: int, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["active", 1, {"status": "ok"}, ["read", "write"]],
            ["banned", None, {"status": "banned"}, [None, "admin"]],
            ["suspended", 3, {"status": None}, [None, "read"]],
            ["BANNED", 4, {"status": "OK"}, ["ADMIN", "write"]],
            ["normal", 5, {"status": "error"}, ["read", "DELETE"]],
        ],
        input_schema,
    )

    # Test with forbidden values - should fail when value IS in the forbidden list
    actual = test_df.select(
        is_not_in_list("a", ["banned", "suspended", "deleted"]),
        is_not_in_list("b", [F.lit(99), F.lit(100)]),
        is_not_in_list(F.col("c").getItem("status"), [F.lit("banned"), F.lit("error")]),
        is_not_in_list(F.try_element_at("d", F.lit(1)), ["admin", "root"]),
        is_not_in_list("a", ["banned", "suspended"], case_sensitive=False),
        is_not_in_list(F.col("c").getItem("status"), [F.lit("ok")], case_sensitive=False),
        is_not_in_list(F.try_element_at("d", F.lit(1)), ["admin"], case_sensitive=False),
        is_not_in_list("d", [["admin", "root"], ["DELETE", "write"]]),
        is_not_in_list("d", [["admin", "root"], ["DELETE", "write"]], case_sensitive=False),
    )

    checked_schema = (
        "a_is_in_the_forbidden_list: string, "
        + "b_is_in_the_forbidden_list: string, "
        + "unresolvedextractvalue_c_status_is_in_the_forbidden_list: string, "
        + "try_element_at_d_1_is_in_the_forbidden_list: string, "
        + "a_is_in_the_forbidden_list: string, "
        + "unresolvedextractvalue_c_status_is_in_the_forbidden_list: string, "
        + "try_element_at_d_1_is_in_the_forbidden_list: string, "
        + "d_is_in_the_forbidden_list: string, "
        + "d_is_in_the_forbidden_list: string"
    )

    expected = spark.createDataFrame(
        [
            # Row 1: "active" - all pass (not in forbidden lists)
            # d = ["read", "write"], try_element_at(d, 1) = "read" (1-based: first element)
            [None, None, None, None, None, None, None, None, None],
            # Row 2: "banned" - fails where "banned" is forbidden
            # d = [None, "admin"], try_element_at(d, 1) = None (1-based: first element)
            [
                "Value 'banned' in Column 'a' is in the forbidden list: [banned, suspended, deleted]",
                None,
                "Value 'banned' in Column 'UnresolvedExtractValue(c, status)' is in the forbidden list: [banned, error]",
                None,  # try_element_at returns None, which passes
                "Value 'banned' in Column 'a' is in the forbidden list: [banned, suspended]",
                None,
                None,  # try_element_at returns None, which passes
                None,
                None,
            ],
            # Row 3: "suspended" - fails where "suspended" is forbidden
            # d = [None, "read"], try_element_at(d, 1) = None (1-based: first element)
            [
                "Value 'suspended' in Column 'a' is in the forbidden list: [banned, suspended, deleted]",
                None,
                None,
                None,  # try_element_at returns None, which passes
                "Value 'suspended' in Column 'a' is in the forbidden list: [banned, suspended]",
                None,
                None,  # try_element_at returns None, which passes
                None,
                None,
            ],
            # Row 4: "BANNED" - case sensitive tests
            # d = ["ADMIN", "write"], try_element_at(d, 1) = "ADMIN" (1-based: first element)
            [
                None,  # case sensitive: "BANNED" != "banned"
                None,
                None,
                None,  # case sensitive: "ADMIN" not in ["admin", "root"]
                "Value 'BANNED' in Column 'a' is in the forbidden list: [banned, suspended]",  # case insensitive
                "Value 'OK' in Column 'UnresolvedExtractValue(c, status)' is in the forbidden list: [ok]",  # case insensitive
                "Value 'ADMIN' in Column 'try_element_at(d, 1)' is in the forbidden list: [admin]",  # case insensitive: "ADMIN" in ["admin"]
                None,
                None,
            ],
            # Row 5: "normal", but "error" status and "DELETE" in array
            # d = ["read", "DELETE"], try_element_at(d, 1) = "read" (1-based: first element)
            [
                None,
                None,
                "Value 'error' in Column 'UnresolvedExtractValue(c, status)' is in the forbidden list: [banned, error]",
                None,  # case sensitive: "read" not in ["admin", "root"]
                None,
                None,
                None,  # case insensitive: "read" not in ["admin"]
                None,  # ["read", "DELETE"] != ["admin", "root"] and != ["DELETE", "write"] (different order)
                None,  # ["read", "DELETE"] != ["admin", "root"] and != ["DELETE", "write"] (case insensitive, still different order)
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_sql_expression(spark):
    test_df = spark.createDataFrame([["str1", 1, 1], ["str2", None, None], ["", 2, 3]], SCHEMA + ", c: string")

    actual = test_df.select(
        sql_expression("a = 'str2'"),
        sql_expression("b is null", name="test", negate=True),
        sql_expression("c is null", msg="failed validation", negate=True),
        sql_expression("b < c", msg="b is greater or equal c", negate=False),
        # since sql expressions are evaluated at runtime, any illegal arguments must be handled explicitly
        sql_expression(
            """
            CASE
                WHEN TRY_CAST(a AS BIGINT) IS NOT NULL AND TRY_CAST(b AS BIGINT) IS NOT NULL
                    THEN SUBSTRING(a, 1, LENGTH(b)) = b
                ELSE FALSE
            END""",
            name="illegal_args",
            msg="Illegal Arguments",
        ),
    )

    checked_schema = "not_a_str2: string, test: string, c_is_null: string, not_b_c: string, illegal_args: string"
    expected = spark.createDataFrame(
        [
            [
                "Value is not matching expression: a = 'str2'",
                None,
                None,
                "b is greater or equal c",
                "Illegal Arguments",
            ],
            [
                None,
                "Value is matching expression: ~(b is null)",
                "failed validation",
                None,
                "Illegal Arguments",
            ],
            ["Value is not matching expression: a = 'str2'", None, None, None, "Illegal Arguments"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_sql_expression_long_name(spark):
    long_col_name = "a" * 300
    normalized_col_name = "a" * 255

    test_df = spark.createDataFrame([["str1"]], long_col_name + ": string")

    actual = test_df.select(
        sql_expression(long_col_name + " = 'str2'"),
        sql_expression(long_col_name + " = 'str2'", columns=[long_col_name]),
    )

    checked_schema = f"not_{normalized_col_name[:-4]}: string, {normalized_col_name}: string"
    expected = spark.createDataFrame(
        [
            [
                f"Value is not matching expression: {long_col_name} = 'str2'",
                f"Value is not matching expression: {long_col_name} = 'str2'",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_col2_for_n_days(spark):
    schema_dates = "a: string, b: string, c: map<string, string>, d: array<string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10", "2023-01-12", {"val": "2023-01-10"}, ["2023-01-12"]],
            ["2023-01-10", "2023-01-13", {"val": "2023-01-10"}, ["2023-01-13"]],
            ["2023-01-10", "2023-01-05", {"val": "2023-01-10"}, ["2023-01-05"]],
            ["2023-01-10", None, {"val": "2023-01-10"}, [None]],
            [None, None, {"val": None}, [None]],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_older_than_col2_for_n_days("a", "b", 2),
        is_older_than_col2_for_n_days(F.col("c").getItem("val"), F.try_element_at("d", F.lit(1)), 2),
        is_older_than_col2_for_n_days("a", "b", 2, negate=True),
        is_older_than_col2_for_n_days(F.col("c").getItem("val"), F.try_element_at("d", F.lit(1)), 2, negate=True),
    )

    checked_schema = (
        "is_col_a_older_than_b_for_n_days: string, "
        + "is_col_unresolvedextractvalue_c_val_older_than_try_element_at_d_1_for_n_days: string, "
        + "is_col_a_not_older_than_b_for_n_days: string, "
        + "is_col_unresolvedextractvalue_c_val_not_older_than_try_element_at_d_1_for_n_days: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value '2023-01-10' in Column 'a' is not less than Value '2023-01-12' in Column 'b' "
                + "for more than 2 days",
                "Value '2023-01-10' in Column 'UnresolvedExtractValue(c, val)' is not less than Value "
                + "'2023-01-12' in Column 'try_element_at(d, 1)' for more than 2 days",
                None,
                None,
            ],
            [
                None,
                None,
                "Value '2023-01-10' in Column 'a' is less than Value '2023-01-13' in Column 'b' "
                + "for 2 or more days",
                "Value '2023-01-10' in Column 'UnresolvedExtractValue(c, val)' is less than Value "
                + "'2023-01-13' in Column 'try_element_at(d, 1)' for 2 or more days",
            ],
            [
                "Value '2023-01-10' in Column 'a' is not less than Value '2023-01-05' in Column 'b' "
                + "for more than 2 days",
                "Value '2023-01-10' in Column 'UnresolvedExtractValue(c, val)' is not less than Value "
                + "'2023-01-05' in Column 'try_element_at(d, 1)' for more than 2 days",
                None,
                None,
            ],
            [None, None, None, None],
            [None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days(spark):
    schema_dates = "a: string, b: map<string, string>, c: array<string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10", {"val": "2023-01-11"}, ["2023-01-12"]],
            ["2023-01-05", {"val": "2023-01-05"}, ["2023-01-05"]],
            [None, None, None],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_older_than_n_days("a", 2, F.lit("2023-01-12")),
        is_older_than_n_days(F.col("b").getItem("val"), 2, F.lit("2023-01-12")),
        is_older_than_n_days(F.try_element_at("c", F.lit(1)), 2, F.lit("2023-01-12")),
        is_older_than_n_days("a", 2, F.lit("2023-01-12"), negate=True),
        is_older_than_n_days(F.col("b").getItem("val"), 2, F.lit("2023-01-12"), negate=True),
        is_older_than_n_days(F.try_element_at("c", F.lit(1)), 2, F.lit("2023-01-12"), negate=True),
    )

    checked_schema = (
        "is_col_a_older_than_n_days: string, "
        + "is_col_unresolvedextractvalue_b_val_older_than_n_days: string, "
        + "is_col_try_element_at_c_1_older_than_n_days: string, "
        + "is_col_a_not_older_than_n_days: string, "
        + "is_col_unresolvedextractvalue_b_val_not_older_than_n_days: string, "
        + "is_col_try_element_at_c_1_not_older_than_n_days: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value '2023-01-10' in Column 'a' is not less than current date '2023-01-12' for more than 2 days",
                "Value '2023-01-11' in Column 'UnresolvedExtractValue(b, val)' is not less than "
                + "current date '2023-01-12' for more than 2 days",
                "Value '2023-01-12' in Column 'try_element_at(c, 1)' is not less than "
                + "current date '2023-01-12' for more than 2 days",
                None,
                None,
                None,
            ],
            [
                None,
                None,
                None,
                "Value '2023-01-05' in Column 'a' is less than current date '2023-01-12' for 2 or more days",
                "Value '2023-01-05' in Column 'UnresolvedExtractValue(b, val)' is less than "
                + "current date '2023-01-12' for 2 or more days",
                "Value '2023-01-05' in Column 'try_element_at(c, 1)' is less than "
                + "current date '2023-01-12' for 2 or more days",
            ],
            [None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_future(spark):
    schema_dates = "a: string, b: map<string, string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10 11:08:37", {"dt": "2023-01-10 11:01:10"}],
            ["2023-01-10 11:08:43", {"dt": "2024-01-02 02:41:20"}],
            [None, {"dt": None}],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_not_in_future("a", 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_future(F.col("b").getItem("dt"), 2, F.lit("2023-01-10 11:08:40")),
    )

    checked_schema = "a_in_future: string, unresolvedextractvalue_b_dt_in_future: string"
    expected = spark.createDataFrame(
        [
            [None, None],
            [
                "Value '2023-01-10 11:08:43' in Column 'a' is greater than time '2023-01-10 11:08:42'",
                "Value '2024-01-02 02:41:20' in Column 'UnresolvedExtractValue(b, dt)' is greater than time '2023-01-10 11:08:42'",
            ],
            [None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_near_future(spark):
    schema_dates = "a: string, b: long, c: map<string, string>"
    test_df = spark.createDataFrame(
        [
            ["2023-01-10 11:08:40", 1673348920, {"dt": "2023-01-10 11:08:40"}],
            ["2023-01-10 11:08:41", 1673348921, {"dt": "2023-01-10 11:08:41"}],
            ["2023-01-10 11:08:42", 1673348922, {"dt": "2023-01-10 11:08:42"}],
            [None, None, {"dt": None}],
        ],
        schema_dates,
    )

    actual = test_df.select(
        is_not_in_near_future("a", 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_near_future(F.col("b").cast("timestamp"), 2, F.lit("2023-01-10 11:08:40")),
        is_not_in_near_future(F.col("c").getItem("dt"), 2, F.lit("2023-01-10 11:08:40")),
    )

    checked_schema = "a_in_near_future: string, cast_b_as_timestamp_in_near_future: string, unresolvedextractvalue_c_dt_in_near_future: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            [
                "Value '2023-01-10 11:08:41' in Column 'a' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
                "Value '2023-01-10 11:08:41' in Column 'CAST(b AS TIMESTAMP)' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
                "Value '2023-01-10 11:08:41' in Column 'UnresolvedExtractValue(c, dt)' is greater than '2023-01-10 11:08:40 and smaller than '2023-01-10 11:08:42'",
            ],
            [None, None, None],
            [None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_col_older_than_n_days_cur(spark):
    schema_dates = "a: string, b: map<string, string>"
    cur_date = spark.sql("SELECT current_date() AS current_date").collect()[0]['current_date'].strftime("%Y-%m-%d")

    test_df = spark.createDataFrame(
        [["2023-01-10", {"dt": "2023-01-10"}], [None, {"dt": None}], [cur_date, {"dt": cur_date}]], schema_dates
    )

    actual = test_df.select(
        is_older_than_n_days("a", 2, None),
        is_older_than_n_days(F.col("b").getItem("dt"), 2, None),
        is_older_than_n_days(F.col("a"), 2, None, True),
        is_older_than_n_days(F.col("b").getItem("dt"), 2, None, True),
    )

    checked_schema = (
        "is_col_a_older_than_n_days: string, is_col_unresolvedextractvalue_b_dt_older_than_n_days: string,"
        + "is_col_a_not_older_than_n_days: string, is_col_unresolvedextractvalue_b_dt_not_older_than_n_days: string"
    )

    expected = spark.createDataFrame(
        [
            [
                None,
                None,
                f"Value '2023-01-10' in Column 'a' is less than current date '{cur_date}' for 2 or more days",
                f"Value '2023-01-10' in Column 'UnresolvedExtractValue(b, dt)' is less than current date "
                f"'{cur_date}' for 2 or more days",
            ],
            [None, None, None, None],
            [
                f"Value '{cur_date}' in Column 'a' is not less than current date '{cur_date}' for more than 2 days",
                f"Value '{cur_date}' in Column 'UnresolvedExtractValue(b, dt)' is not less than current date "
                f"'{cur_date}' for more than 2 days",
                None,
                None,
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_less_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>, g: map<string, int>, h: float"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1], {"val": 1}, 1.2],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.99"), [2], {"val": 2}, 3.6],
            [4, 3, None, None, Decimal("2.01"), [4], {"val": 4}, 4.8],
            [None, None, None, None, None, [None], {"val": None}, None],
        ],
        schema_num,
    )

    actual = test_df.select(
        is_not_less_than("a", 2),
        is_not_less_than("a", "2"),
        is_not_less_than("a", F.col("b") * 2),
        is_not_less_than("b", "a"),
        is_not_less_than("c", datetime(2025, 2, 1).date()),
        is_not_less_than("c", "2025-02-01"),
        is_not_less_than("d", datetime(2025, 2, 1)),
        is_not_less_than("d", "2025-02-01"),
        is_not_less_than("e", 2),
        is_not_less_than(F.try_element_at("f", F.lit(1)), 2),
        is_not_less_than(F.col("g").getItem("val"), 2),
        is_not_less_than("h", 2.4),
    )

    checked_schema = (
        "a_less_than_limit: string, a_less_than_limit: string, a_less_than_limit: string, "
        "b_less_than_limit: string, c_less_than_limit: string, c_less_than_limit: string, "
        "d_less_than_limit: string, d_less_than_limit: string, "
        "e_less_than_limit: string, try_element_at_f_1_less_than_limit: string, "
        "unresolvedextractvalue_g_val_less_than_limit: string, "
        "h_less_than_limit: string"
    )

    expected = spark.createDataFrame(
        [
            [
                "Value '1' in Column 'a' is less than limit: 2",
                "Value '1' in Column 'a' is less than limit: 2",
                None,
                None,
                "Value '2025-01-01' in Column 'c' is less than limit: 2025-02-01",
                "Value '2025-01-01' in Column 'c' is less than limit: 2025-02-01",
                "Value '2025-01-01 00:00:00' in Column 'd' is less than limit: 2025-02-01 00:00:00",
                "Value '2025-01-01 00:00:00' in Column 'd' is less than limit: 2025-02-01 00:00:00",
                "Value '1.00' in Column 'e' is less than limit: 2",
                "Value '1' in Column 'try_element_at(f, 1)' is less than limit: 2",
                "Value '1' in Column 'UnresolvedExtractValue(g, val)' is less than limit: 2",
                "Value '1.2' in Column 'h' is less than limit: 2.4",
            ],
            [
                None,
                None,
                "Value '2' in Column 'a' is less than limit: 8",
                None,
                None,
                None,
                None,
                None,
                "Value '1.99' in Column 'e' is less than limit: 2",
                None,
                None,
                None,
            ],
            [
                None,
                None,
                "Value '4' in Column 'a' is less than limit: 6",
                "Value '3' in Column 'b' is less than limit: 4",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [None, None, None, None, None, None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_greater_than(spark, set_utc_timezone):
    schema_num = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>, g: float"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1], 1.2],
            [2, 4, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.01"), [2], 3.6],
            [8, 3, None, None, Decimal("0.99"), [8], 4.8],
            [None, None, None, None, None, [None], None],
        ],
        schema_num,
    )

    actual = test_df.select(
        is_not_greater_than("a", 1),
        is_not_greater_than("a", "1"),
        is_not_greater_than("a", F.col("b") * 2),
        is_not_greater_than("b", "a"),
        is_not_greater_than("c", datetime(2025, 1, 1).date()),
        is_not_greater_than("c", "2025-01-01"),
        is_not_greater_than("d", datetime(2025, 1, 1)),
        is_not_greater_than("d", "2025-01-01"),
        is_not_greater_than("e", 1),
        is_not_greater_than(F.try_element_at("f", F.lit(1)), 1),
        is_not_greater_than("g", 2.4),
    )

    checked_schema = (
        "a_greater_than_limit: string, a_greater_than_limit: string, a_greater_than_limit: string, "
        "b_greater_than_limit: string, c_greater_than_limit: string, c_greater_than_limit: string, "
        "d_greater_than_limit: string, d_greater_than_limit: string, e_greater_than_limit: string, "
        "try_element_at_f_1_greater_than_limit: string, g_greater_than_limit: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None, None, None, None, None, None, None],
            [
                "Value '2' in Column 'a' is greater than limit: 1",
                "Value '2' in Column 'a' is greater than limit: 1",
                None,
                "Value '4' in Column 'b' is greater than limit: 2",
                "Value '2025-02-01' in Column 'c' is greater than limit: 2025-01-01",
                "Value '2025-02-01' in Column 'c' is greater than limit: 2025-01-01",
                "Value '2025-02-01 00:00:00' in Column 'd' is greater than limit: 2025-01-01 00:00:00",
                "Value '2025-02-01 00:00:00' in Column 'd' is greater than limit: 2025-01-01 00:00:00",
                "Value '1.01' in Column 'e' is greater than limit: 1",
                "Value '2' in Column 'try_element_at(f, 1)' is greater than limit: 1",
                "Value '3.6' in Column 'g' is greater than limit: 2.4",
            ],
            [
                "Value '8' in Column 'a' is greater than limit: 1",
                "Value '8' in Column 'a' is greater than limit: 1",
                "Value '8' in Column 'a' is greater than limit: 6",
                None,
                None,
                None,
                None,
                None,
                None,
                "Value '8' in Column 'try_element_at(f, 1)' is greater than limit: 1",
                "Value '4.8' in Column 'g' is greater than limit: 2.4",
            ],
            [None, None, None, None, None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: int, e: int, f: int, g: decimal(10,2), h: map<string, int>, i:float"
    test_df = spark.createDataFrame(
        [
            [0, datetime(2024, 12, 1).date(), datetime(2024, 12, 1), -1, 5, 6, Decimal("2.00"), {"val": 0}, 0.0],
            [1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), 2, 6, 3, Decimal("1.00"), {"val": 1}, 0.2],
            [2, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), 2, 7, 3, Decimal("3.00"), {"val": 2}, 0.4],
            [3, datetime(2025, 3, 1).date(), datetime(2025, 3, 1), 3, 8, 3, Decimal("1.01"), {"val": 3}, 0.6],
            [4, datetime(2025, 4, 1).date(), datetime(2025, 4, 1), 2, 9, 3, Decimal("3.01"), {"val": 4}, 0.8],
            [None, None, None, None, None, None, None, {"val": None}, None],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 3, 1)
    actual = test_df.select(
        is_in_range("a", 1, 3),
        is_in_range("a", "1", "3"),
        is_in_range("b", start_date.date(), end_date.date()),
        is_in_range("b", "2025-01-01", "2025-03-01"),
        is_in_range("c", start_date, end_date),
        is_in_range("c", "2025-01-01", "2025-03-01"),
        is_in_range("c", "2025-01-01 00:00:00", "2025-03-01 00:00:00"),
        is_in_range("d", F.col("a"), F.expr("e - 1")),
        is_in_range("f", "a", 5),
        is_in_range("g", 1, 3),
        is_in_range(F.col("h").getItem("val"), 1, 3),
        is_in_range("i", 0.1, 0.7),
    )

    checked_schema = (
        "a_not_in_range: string, a_not_in_range: string, "
        "b_not_in_range: string, b_not_in_range: string, "
        "c_not_in_range: string, c_not_in_range: string, c_not_in_range: string, "
        "d_not_in_range: string, f_not_in_range: string, g_not_in_range: string, "
        "unresolvedextractvalue_h_val_not_in_range: string, i_not_in_range: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value '0' in Column 'a' not in range: [1, 3]",
                "Value '0' in Column 'a' not in range: [1, 3]",
                "Value '2024-12-01' in Column 'b' not in range: [2025-01-01, 2025-03-01]",
                "Value '2024-12-01' in Column 'b' not in range: [2025-01-01, 2025-03-01]",
                "Value '2024-12-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '2024-12-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '2024-12-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '-1' in Column 'd' not in range: [0, 4]",
                "Value '6' in Column 'f' not in range: [0, 5]",
                None,
                "Value '0' in Column 'UnresolvedExtractValue(h, val)' not in range: [1, 3]",
                "Value '0.0' in Column 'i' not in range: [0.1, 0.7]",
            ],
            [None, None, None, None, None, None, None, None, None, None, None, None],
            [None, None, None, None, None, None, None, None, None, None, None, None],
            [None, None, None, None, None, None, None, None, None, None, None, None],
            [
                "Value '4' in Column 'a' not in range: [1, 3]",
                "Value '4' in Column 'a' not in range: [1, 3]",
                "Value '2025-04-01' in Column 'b' not in range: [2025-01-01, 2025-03-01]",
                "Value '2025-04-01' in Column 'b' not in range: [2025-01-01, 2025-03-01]",
                "Value '2025-04-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '2025-04-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '2025-04-01 00:00:00' in Column 'c' not in range: [2025-01-01 00:00:00, 2025-03-01 00:00:00]",
                "Value '2' in Column 'd' not in range: [4, 8]",
                "Value '3' in Column 'f' not in range: [4, 5]",
                "Value '3.01' in Column 'g' not in range: [1, 3]",
                "Value '4' in Column 'UnresolvedExtractValue(h, val)' not in range: [1, 3]",
                "Value '0.8' in Column 'i' not in range: [0.1, 0.7]",
            ],
            [None, None, None, None, None, None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_range(spark, set_utc_timezone):
    schema_num = "a: int, b: date, c: timestamp, d: timestamp, e: decimal(10,2), f: array<int>, g: float"
    test_df = spark.createDataFrame(
        [
            [
                0,
                datetime(2024, 12, 31).date(),
                datetime(2025, 1, 4),
                datetime(2025, 1, 7),
                Decimal("0.99"),
                [0, 1],
                0.0,
            ],
            [1, datetime(2025, 1, 1).date(), datetime(2025, 1, 3), datetime(2025, 1, 1), Decimal("1.00"), [1, 2], 0.3],
            [3, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), datetime(2025, 2, 3), Decimal("3.00"), [3, 4], 0.6],
            [None, None, None, None, None, [None, 1], None],
        ],
        schema_num,
    )

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 1, 3)
    actual = test_df.select(
        is_not_in_range("a", 1, 3),
        is_not_in_range("a", "1", "3"),
        is_not_in_range("b", start_date.date(), end_date.date()),
        is_not_in_range("b", "2025-01-01", "2025-01-03"),
        is_not_in_range("c", start_date, end_date),
        is_not_in_range("c", "2025-01-01", "2025-01-03"),
        is_not_in_range("c", "2025-01-01 00:00:00", "2025-01-03 00:00:00"),
        is_not_in_range("d", "c", F.expr("cast(b as timestamp) + INTERVAL 2 DAY")),
        is_not_in_range("e", 1, 3),
        is_not_in_range(F.try_element_at("f", F.lit(1)), 1, 3),
        is_not_in_range("g", 0.2, 0.5),
    )

    checked_schema = (
        "a_in_range: string, a_in_range: string, b_in_range: string, b_in_range: string, "
        "c_in_range: string, c_in_range: string, c_in_range: string, d_in_range: string, e_in_range: string, "
        "try_element_at_f_1_in_range: string, g_in_range: string"
    )
    expected = spark.createDataFrame(
        [
            [None, None, None, None, None, None, None, None, None, None, None],
            [
                "Value '1' in Column 'a' in range: [1, 3]",
                "Value '1' in Column 'a' in range: [1, 3]",
                "Value '2025-01-01' in Column 'b' in range: [2025-01-01, 2025-01-03]",
                "Value '2025-01-01' in Column 'b' in range: [2025-01-01, 2025-01-03]",
                "Value '2025-01-03 00:00:00' in Column 'c' in range: [2025-01-01 00:00:00, 2025-01-03 00:00:00]",
                "Value '2025-01-03 00:00:00' in Column 'c' in range: [2025-01-01 00:00:00, 2025-01-03 00:00:00]",
                "Value '2025-01-03 00:00:00' in Column 'c' in range: [2025-01-01 00:00:00, 2025-01-03 00:00:00]",
                None,
                "Value '1.00' in Column 'e' in range: [1, 3]",
                "Value '1' in Column 'try_element_at(f, 1)' in range: [1, 3]",
                "Value '0.3' in Column 'g' in range: [0.2, 0.5]",
            ],
            [
                "Value '3' in Column 'a' in range: [1, 3]",
                "Value '3' in Column 'a' in range: [1, 3]",
                None,
                None,
                None,
                "Value '2025-02-03 00:00:00' in Column 'd' in range: [2025-02-01 00:00:00, 2025-02-03 00:00:00]",
                "Value '2025-02-03 00:00:00' in Column 'd' in range: [2025-02-01 00:00:00, 2025-02-03 00:00:00]",
                "Value '2025-02-03 00:00:00' in Column 'd' in range: [2025-02-01 00:00:00, 2025-02-03 00:00:00]",
                "Value '3.00' in Column 'e' in range: [1, 3]",
                "Value '3' in Column 'try_element_at(f, 1)' in range: [1, 3]",
                None,
            ],
            [None, None, None, None, None, None, None, None, None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_matching_regex(spark):
    schema_str = "a: string, b: map<string, string>"
    test_df = spark.createDataFrame(
        [["2023-01-02", {"s": "2023-01-02"}], ["2023/01/02", {"s": "2023/01/02"}], [None, {"s": None}]],
        schema_str,
    )

    # matching ISO date: yyyy-MM-dd format
    date_re = "^\\d{4}-([0]\\d|1[0-2])-([0-2]\\d|3[01])$"

    actual = test_df.select(
        regex_match("a", date_re), regex_match("a", date_re, negate=True), regex_match(F.col("b").getItem("s"), date_re)
    )

    checked_schema = (
        "a_not_matching_regex: string, a_matching_regex: string, unresolvedextractvalue_b_s_not_matching_regex: string"
    )
    expected = spark.createDataFrame(
        [
            [None, "Column 'a' is matching regex", None],
            ["Column 'a' is not matching regex", None, "Column 'UnresolvedExtractValue(b, s)' is not matching regex"],
            [None, None, None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_struct(spark):
    test_df = spark.createDataFrame([[("str1",)]], "data: struct<x:string>")

    actual = test_df.select(is_not_empty("data.x"))

    checked_schema = "data_x_is_empty: string"
    expected = spark.createDataFrame([[None]], checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_in_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["9999-12-31 23:59:59"]], schema_dates)

    actual = test_df.select(is_not_in_future("a", 0, None))

    checked_schema = "a_in_future: string"

    expected = spark.createDataFrame([[None]], checked_schema)

    assert actual.select("a_in_future") != expected.select("a_in_future")


def test_col_is_not_in_near_future_cur(spark):
    schema_dates = "a: string"

    test_df = spark.createDataFrame([["1900-01-01 23:59:59"], ["9999-12-31 23:59:59"], [None]], schema_dates)

    actual = test_df.select(is_not_in_near_future("a", 2, None))

    checked_schema = "a_in_near_future: string"
    expected = spark.createDataFrame(
        [[None], [None], [None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_null_and_not_empty_array(spark):
    schema_array = (
        "str_col: array<string>, int_col: array<int> , timestamp_col: array<timestamp>, "
        "date_col: array<string>, struct_col: array<struct<a: string, b: int>>, "
        "nested_array_col: map<string, array<string>>"
    )
    data = [
        (
            ["a", "b", None],
            [1, 2, None],
            [None, datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d"), None],
            [{"a": "x", "b": 1}, None],
            {"arr": ["a", "b", None]},
        ),
        ([], [], [], [], [], {"arr": []}),
        (None, None, None, None, None, {"arr": None}),
        (
            ["non-empty"],
            [10],
            [datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")],
            [datetime.strptime("2025-01-01", "%Y-%m-%d")],
            [{"a": "y", "b": 2}],
            {"arr": ["non-empty"]},
        ),
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_not_null_and_not_empty_array("str_col"),
        is_not_null_and_not_empty_array("int_col"),
        is_not_null_and_not_empty_array("timestamp_col"),
        is_not_null_and_not_empty_array("date_col"),
        is_not_null_and_not_empty_array("struct_col"),
        is_not_null_and_not_empty_array(F.col("nested_array_col").getItem("arr")),
    )

    checked_schema = (
        "str_col_is_null_or_empty_array: string, int_col_is_null_or_empty_array: string, "
        "timestamp_col_is_null_or_empty_array: string, date_col_is_null_or_empty_array: string, "
        "struct_col_is_null_or_empty_array: string, "
        "unresolvedextractvalue_nested_array_col_arr_is_null_or_empty_array: string"
    )
    # Create the data
    checked_data = [
        (None, None, None, None, None, None),
        (
            "Column 'str_col' is null or empty array",
            "Column 'int_col' is null or empty array",
            "Column 'timestamp_col' is null or empty array",
            "Column 'date_col' is null or empty array",
            "Column 'struct_col' is null or empty array",
            "Column 'UnresolvedExtractValue(nested_array_col, arr)' is null or empty array",
        ),
        (
            "Column 'str_col' is null or empty array",
            "Column 'int_col' is null or empty array",
            "Column 'timestamp_col' is null or empty array",
            "Column 'date_col' is null or empty array",
            "Column 'struct_col' is null or empty array",
            "Column 'UnresolvedExtractValue(nested_array_col, arr)' is null or empty array",
        ),
        (None, None, None, None, None, None),
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_date(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string, e: map<string, string>"
    data = [
        ["2024-01-01", "12/31/2025", "invalid_date", None, {"dt": "2024-01-01"}],
        ["12/31/2025", "2024-01-01", "invalid_date", None, {"dt": "12/31/2025"}],
        ["12/31/2025", "invalid_date", "2024-01-01", None, {"dt": "12/31/2025"}],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_date("a"),
        is_valid_date("b", "MM/dd/yyyy"),
        is_valid_date("c", "yyyy-MM-dd"),
        is_valid_date("d"),
        is_valid_date(F.col("e").getItem("dt")),
    )

    checked_schema = """
        a_is_not_valid_date: string,
        b_is_not_valid_date: string,
        c_is_not_valid_date: string,
        d_is_not_valid_date: string,
        unresolvedextractvalue_e_dt_is_not_valid_date: string
        """
    checked_data = [
        [None, None, "Value 'invalid_date' in Column 'c' is not a valid date with format 'yyyy-MM-dd'", None, None],
        [
            "Value '12/31/2025' in Column 'a' is not a valid date",
            "Value '2024-01-01' in Column 'b' is not a valid date with format 'MM/dd/yyyy'",
            "Value 'invalid_date' in Column 'c' is not a valid date with format 'yyyy-MM-dd'",
            None,
            "Value '12/31/2025' in Column 'UnresolvedExtractValue(e, dt)' is not a valid date",
        ],
        [
            "Value '12/31/2025' in Column 'a' is not a valid date",
            "Value 'invalid_date' in Column 'b' is not a valid date with format 'MM/dd/yyyy'",
            None,
            None,
            "Value '12/31/2025' in Column 'UnresolvedExtractValue(e, dt)' is not a valid date",
        ],
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_timestamp(spark, set_utc_timezone):
    schema_array = "a: string, b: string, c: string, d: string, e: string, f: map<string, string>"
    data = [
        [
            "2024-01-01 00:00:00",
            "12/31/2025 00:00:00",
            "invalid_timestamp",
            None,
            "2025-01-31T00:00:00",
            {"dt": "2024-01-01 00:00:00"},
        ],
        [
            "12/31/2025 00:00:00",
            "2024-01-01 00:00:00",
            "invalid_timestamp",
            None,
            "2025-01-31 00:00:00",
            {"dt": "12/31/2025 00:00:00"},
        ],
        [
            "2024-01-01T00:00:00",
            "invalid_timestamp",
            "2024-01-01 00:00:00",
            None,
            "1/31/2025 00:00:00",
            {"dt": "2024-01-01 00:00:00"},
        ],
    ]

    test_df = spark.createDataFrame(data, schema_array)

    actual = test_df.select(
        is_valid_timestamp("a"),
        is_valid_timestamp("b", "MM/dd/yyyy HH:mm:ss"),
        is_valid_timestamp("c", "yyyy-MM-dd HH:mm:ss"),
        is_valid_timestamp("d"),
        is_valid_timestamp("e", "yyyy-MM-dd'T'HH:mm:ss"),
        is_valid_timestamp(F.col("f").getItem("dt")),
    )

    checked_schema = """
        a_is_not_valid_timestamp: string,
        b_is_not_valid_timestamp: string,
        c_is_not_valid_timestamp: string,
        d_is_not_valid_timestamp: string,
        e_is_not_valid_timestamp: string,
        unresolvedextractvalue_f_dt_is_not_valid_timestamp: string
        """
    checked_data = [
        [
            None,
            None,
            "Value 'invalid_timestamp' in Column 'c' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            None,
            None,
        ],
        [
            "Value '12/31/2025 00:00:00' in Column 'a' is not a valid timestamp",
            "Value '2024-01-01 00:00:00' in Column 'b' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            "Value 'invalid_timestamp' in Column 'c' is not a valid timestamp with format 'yyyy-MM-dd HH:mm:ss'",
            None,
            "Value '2025-01-31 00:00:00' in Column 'e' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
            "Value '12/31/2025 00:00:00' in Column 'UnresolvedExtractValue(f, dt)' is not a valid timestamp",
        ],
        [
            None,
            "Value 'invalid_timestamp' in Column 'b' is not a valid timestamp with format 'MM/dd/yyyy HH:mm:ss'",
            None,
            None,
            "Value '1/31/2025 00:00:00' in Column 'e' is not a valid timestamp with format 'yyyy-MM-dd'T'HH:mm:ss'",
            None,
        ],
    ]
    expected = spark.createDataFrame(checked_data, checked_schema)

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_valid_ipv4_address(spark):
    schema_ipv4 = "a: string"

    test_df = spark.createDataFrame(
        [
            # Valid IPv4 addresses - boundary and common cases
            ["255.255.255.255"],  # Maximum address
            ["0.0.0.0"],  # Minimum address
            ["127.0.0.1"],  # Loopback
            ["192.168.1.1"],  # Private Class C
            ["10.0.0.1"],  # Private Class A
            ["172.16.0.1"],  # Private Class B
            ["224.0.0.1"],  # Multicast start
            ["240.0.0.1"],  # Reserved range
            ["1.1.1.1"],  # Public DNS (Cloudflare)
            ["8.8.8.8"],  # Public DNS (Google)
            ["169.254.1.1"],  # Link-local
            ["203.0.113.1"],  # TEST-NET-3 (RFC 5737)
            ["198.51.100.1"],  # TEST-NET-2 (RFC 5737)
            ["192.0.2.1"],  # TEST-NET-1 (RFC 5737)
            # Additional boundary testing
            ["0.0.0.1"],  # Just above minimum
            ["1.0.0.0"],  # First class A
            ["254.255.255.255"],  # Just below maximum
            ["255.255.255.254"],  # Just below broadcast
            ["127.255.255.255"],  # Loopback broadcast
            ["10.255.255.255"],  # Private Class A broadcast
            ["172.31.255.255"],  # Private Class B broadcast
            ["192.168.255.255"],  # Private Class C broadcast
            ["239.255.255.255"],  # Multicast end
            ["223.255.255.255"],  # Last Class C
            # Invalid formats - leading zeros (RFC 5321 compliance)
            ["192.168.01.1"],  # Leading zero in third octet
            ["001.002.003.004"],  # Leading zeros in all octets
            ["192.168.001.1"],  # Leading zeros in third octet variant
            ["01.168.1.1"],  # Leading zero in first octet
            ["192.01.1.1"],  # Leading zero in second octet
            ["192.168.1.01"],  # Leading zero in fourth octet
            ["00.0.0.0"],  # Double leading zero
            ["0.00.0.0"],  # Leading zero in second octet
            ["192.168.1.000"],  # Triple leading zero
            # Invalid formats - out of range octets
            ["256.168.1.1"],  # First octet > 255
            ["192.256.1.1"],  # Second octet > 255
            ["192.168.256.1"],  # Third octet > 255
            ["1.2.3.256"],  # Fourth octet > 255
            ["300.1.1.1"],  # Way out of range
            ["1.2.3.999"],  # Way out of range
            ["1000.1.1.1"],  # Four digit octet
            ["1.1000.1.1"],  # Four digit second octet
            ["1.1.1000.1"],  # Four digit third octet
            ["1.1.1.1000"],  # Four digit fourth octet
            # Invalid formats - negative numbers
            ["-1.0.0.0"],  # Negative first octet
            ["0.-1.0.0"],  # Negative second octet
            ["0.0.-1.0"],  # Negative third octet
            ["0.0.0.-1"],  # Negative fourth octet
            ["-192.168.1.1"],  # Negative with valid range
            ["192.-168.1.1"],  # Negative in middle
            # Invalid formats - structural issues
            ["192.168.1"],  # Missing octet
            ["192.168"],  # Missing two octets
            ["192"],  # Missing three octets
            ["192.168.1.1.1"],  # Extra octet
            ["192.168.1.1.1.1"],  # Multiple extra octets
            ["1.1.1.1.1.1.1.1"],  # IPv6-like format
            # Invalid formats - empty and whitespace
            [""],  # Empty string
            [" "],  # Space only
            ["\t"],  # Tab
            ["\n"],  # Newline
            # Invalid formats - dots and separators
            ["192.168..1"],  # Double dot
            ["192...168.1.1"],  # Triple dot
            ["192....168.1.1"],  # Quad dot
            ["192.168.1."],  # Trailing dot
            [".192.168.1.1"],  # Leading dot
            ["..192.168.1.1"],  # Double leading dot
            ["192.168.1.1."],  # Double trailing dot
            ["192.168.1.1.."],  # Double trailing dots
            ["19..2.168.1.1"],  # Double dot in first octet
            ["192..168.1.1"],  # Double dot between octets
            ["192.168..1.1"],  # Double dot before last
            ["192.168.1..1"],  # Double dot in last
            # Invalid formats - whitespace in addresses
            ["192. 168.1.1"],  # Space after dot
            [" 192.168.1.1"],  # Leading space
            ["192.168.1.1 "],  # Trailing space
            ["192.168.1. 1"],  # Space before last octet
            ["192 .168.1.1"],  # Space before dot
            ["192.168 .1.1"],  # Space before dot (middle)
            ["192.168. 1.1"],  # Space after dot (middle)
            [" 192.168.1.1 "],  # Leading and trailing spaces
            ["192.168.\t1.1"],  # Tab in address
            ["192.168.1\n.1"],  # Newline in address
            # Invalid formats - non-numeric characters
            ["abc.def.ghi.jkl"],  # All alphabetic
            ["192.abc.1.1"],  # Alphabetic in second octet
            ["192.168.def.1"],  # Alphabetic in third octet
            ["192.168.1.xyz"],  # Alphabetic in fourth octet
            ["a.b.c.d"],  # Single chars
            ["192.168.1.!"],  # Special character
            ["192.168.@.1"],  # Special character in middle
            ["#192.168.1.1"],  # Special character at start
            ["192.168.1.1$"],  # Special character at end
            ["192.16$.1.1"],  # Special character in octet
            ["192.168.1.1a"],  # Alphanumeric in last octet
            ["1a.168.1.1"],  # Alphanumeric in first octet
            # Invalid formats - alternative separators
            ["192:168:1:1"],  # Colons instead of dots
            ["192-168-1-1"],  # Hyphens instead of dots
            ["192_168_1_1"],  # Underscores instead of dots
            ["192,168,1,1"],  # Commas instead of dots
            ["192 168 1 1"],  # Spaces instead of dots
            ["192/168/1/1"],  # Slashes instead of dots
            ["192\\168\\1\\1"],  # Backslashes instead of dots
            ["192|168|1|1"],  # Pipes instead of dots
            # Invalid formats - CIDR notation (should be invalid for pure IPv4)
            ["192.168.1.0/24"],  # Valid CIDR but invalid for IPv4 address
            ["192.168.1.0/0"],  # /0 CIDR
            ["192.168.1.0/32"],  # /32 CIDR
            ["192.168.1.0/33"],  # Invalid CIDR (> 32)
            ["192.168.1.0/abc"],  # Non-numeric CIDR
            ["192.168.1.0/"],  # Missing CIDR value
            ["/24"],  # CIDR without IP
            ["192.168.1.1/"],  # Trailing slash without CIDR
            # Invalid formats - too few/many characters
            ["1"],  # Single digit
            ["12345"],  # Random number
            ["255255255255"],  # No dots
            ["255255155255"],  # No dots variant
            ["192168001001"],  # No dots, leading zeros
            # Additional edge cases
            [None],  # NULL value
        ],
        schema_ipv4,
    )

    actual = test_df.select(is_valid_ipv4_address("a"))

    checked_schema = "a_does_not_match_pattern_ipv4_address: string"

    expected = spark.createDataFrame(
        [
            # Valid IPv4 addresses - boundary and common cases
            [None],  # 255.255.255.255 - Maximum address
            [None],  # 0.0.0.0 - Minimum address
            [None],  # 127.0.0.1 - Loopback
            [None],  # 192.168.1.1 - Private Class C
            [None],  # 10.0.0.1 - Private Class A
            [None],  # 172.16.0.1 - Private Class B
            [None],  # 224.0.0.1 - Multicast start
            [None],  # 240.0.0.1 - Reserved range
            [None],  # 1.1.1.1 - Public DNS (Cloudflare)
            [None],  # 8.8.8.8 - Public DNS (Google)
            [None],  # 169.254.1.1 - Link-local
            [None],  # 203.0.113.1 - TEST-NET-3 (RFC 5737)
            [None],  # 198.51.100.1 - TEST-NET-2 (RFC 5737)
            [None],  # 192.0.2.1 - TEST-NET-1 (RFC 5737)
            # Additional boundary testing
            [None],  # 0.0.0.1 - Just above minimum
            [None],  # 1.0.0.0 - First class A
            [None],  # 254.255.255.255 - Just below maximum
            [None],  # 255.255.255.254 - Just below broadcast
            [None],  # 127.255.255.255 - Loopback broadcast
            [None],  # 10.255.255.255 - Private Class A broadcast
            [None],  # 172.31.255.255 - Private Class B broadcast
            [None],  # 192.168.255.255 - Private Class C broadcast
            [None],  # 239.255.255.255 - Multicast end
            [None],  # 223.255.255.255 - Last Class C
            # Invalid formats - leading zeros (RFC 5321 compliance)
            ["Value '192.168.01.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '001.002.003.004' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.001.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '01.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.01.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.01' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '00.0.0.0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '0.00.0.0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.000' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - out of range octets
            ["Value '256.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.256.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.256.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.2.3.256' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '300.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.2.3.999' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1000.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.1000.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.1.1000.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.1.1.1000' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - negative numbers
            ["Value '-1.0.0.0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '0.-1.0.0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '0.0.-1.0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '0.0.0.-1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '-192.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.-168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - structural issues
            ["Value '192.168.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1.1.1.1.1.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - empty and whitespace
            ["Value '' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value ' ' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '\t' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '\n' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - dots and separators
            ["Value '192.168..1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192...168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192....168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '.192.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '..192.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1.' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1..' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '19..2.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192..168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168..1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1..1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - whitespace in addresses
            ["Value '192. 168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value ' 192.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1 ' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1. 1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192 .168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168 .1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168. 1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value ' 192.168.1.1 ' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.\t1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1\n.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - non-numeric characters
            ["Value 'abc.def.ghi.jkl' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.abc.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.def.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.xyz' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value 'a.b.c.d' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.!' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.@.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '#192.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1$' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.16$.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1a' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '1a.168.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - alternative separators
            ["Value '192:168:1:1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192-168-1-1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192_168_1_1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192,168,1,1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192 168 1 1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192/168/1/1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192\\168\\1\\1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192|168|1|1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - CIDR notation (should be invalid for pure IPv4)
            ["Value '192.168.1.0/24' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.0/0' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.0/32' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.0/33' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.0/abc' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.0/' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '/24' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192.168.1.1/' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Invalid formats - too few/many characters
            ["Value '1' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '12345' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '255255255255' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '255255155255' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            ["Value '192168001001' in Column 'a' does not match pattern 'IPV4_ADDRESS'"],
            # Additional edge cases
            [None],  # NULL value
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_ipv4_address_in_cidr(spark):
    schema_ipv4 = "a: string, b: string"

    test_df = spark.createDataFrame(
        [
            ["255.255.255.255", "192.168.01.1"],
            ["0.0.0.0", "192.168.1"],
            ["abc.def.ghi.jkl", None],
            ["255255155255", "127.0.0.1"],
            ["192.168.1.1", "10.0.0.1"],
            ["172.16.0.1", "224.0.0.1"],
            ["240.0.0.1", "192.168.1"],
            ["192.168.1.1.1", ""],
            [" ", "abc.def.ghi.jkl"],
            ["1.178.7.255", "1.178.4.0"],
            ["1.178.4.1", "1.178.4.255"],
            # COMPREHENSIVE CIDR EDGE CASES
            # Boundary testing for /12 range (172.16.0.0/12 = 172.16.0.0 - 172.31.255.255)
            ["172.16.0.0", "1.178.4.1"],  # First address in /12 range
            ["172.31.255.255", "1.178.4.2"],  # Last address in /12 range
            ["172.15.255.255", "1.178.4.3"],  # Just before /12 range
            ["172.32.0.0", "1.178.4.4"],  # Just after /12 range
            # Boundary testing for /24 range (1.178.4.0/24 = 1.178.4.0 - 1.178.4.255)
            ["172.16.1.1", "1.178.4.0"],  # First address in /24 range
            ["172.16.2.1", "1.178.4.255"],  # Last address in /24 range
            ["172.16.3.1", "1.178.3.255"],  # Just before /24 range
            ["172.16.4.1", "1.178.5.0"],  # Just after /24 range
            # Different prefix lengths testing
            ["10.0.0.1", "192.168.0.1"],  # Class A private vs Class C private
            ["172.16.0.1", "192.168.1.1"],  # Class B private vs Class C private
            ["192.168.0.1", "10.0.0.1"],  # Class C private vs Class A private
            # Special address ranges
            ["127.0.0.1", "169.254.1.1"],  # Loopback vs Link-local
            ["169.254.1.1", "224.0.0.1"],  # Link-local vs Multicast
            ["224.0.0.1", "240.0.0.1"],  # Multicast vs Reserved
            # Public address ranges
            ["8.8.8.8", "1.1.1.1"],  # Google DNS vs Cloudflare DNS
            ["203.0.113.1", "198.51.100.1"],  # TEST-NET-3 vs TEST-NET-2
            ["192.0.2.1", "203.0.113.2"],  # TEST-NET-1 vs TEST-NET-3
            # Network and broadcast address testing
            ["172.16.0.0", "1.178.4.0"],  # Network address
            ["172.16.0.255", "1.178.4.255"],  # Subnet broadcast (for /24)
            ["172.31.255.255", "1.178.4.128"],  # Range broadcast
            # Edge case addresses
            ["0.0.0.0", "255.255.255.255"],  # Minimum vs Maximum
            ["255.255.255.254", "0.0.0.1"],  # Near maximum vs Near minimum
            # Invalid addresses for error testing
            ["256.1.1.1", "1.178.4.5"],  # Out of range first octet
            ["172.16.1.256", "1.300.4.5"],  # Out of range last octet
            ["172.16.01.1", "1.178.04.5"],  # Leading zeros
            ["172.16", "1.178.4"],  # Incomplete addresses
            ["172.16.1.1.1", "1.178.4.5.6"],  # Too many octets
            ["invalid.ip", "also.invalid"],  # Non-IP strings
            ["192.168.-1.1", "1.178.4.-1"],  # Negative octets
            ["172.16.1.", "1.178.4."],  # Trailing dots
            [".172.16.1.1", ".1.178.4.1"],  # Leading dots
            ["172..16.1.1", "1..178.4.1"],  # Double dots
            ["172.16. 1.1", "1.178.4. 1"],  # Spaces in address
            [" 172.16.1.1", " 1.178.4.1"],  # Leading spaces
            ["172.16.1.1 ", "1.178.4.1 "],  # Trailing spaces
        ],
        schema_ipv4,
    )
    actual = test_df.select(
        is_ipv4_address_in_cidr("a", "172.16.0.0/12"),
        is_ipv4_address_in_cidr("b", "1.178.4.0/24"),
    )
    checked_schema = "a_is_not_ipv4_in_cidr: string, b_is_not_ipv4_in_cidr: string"
    expected = spark.createDataFrame(
        [
            [
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '192.168.01.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '192.168.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            ["Value 'abc.def.ghi.jkl' in Column 'a' does not match pattern 'IPV4_ADDRESS'", None],
            [
                "Value '255255155255' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '127.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '192.168.1.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '10.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [None, "Value '224.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'"],
            [
                "Value '240.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '192.168.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '192.168.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value ' ' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value 'abc.def.ghi.jkl' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            ["Value '1.178.7.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'", None],
            ["Value '1.178.4.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'", None],
            # COMPREHENSIVE CIDR EDGE CASES RESULTS
            # Boundary testing for /12 range (172.16.0.0/12 = 172.16.0.0 - 172.31.255.255)
            [None, None],  # First in range
            [None, None],  # Last in range
            [
                "Value '172.15.255.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
            ],  # Just before range
            ["Value '172.32.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'", None],  # Just after range
            # Boundary testing for /24 range (1.178.4.0/24 = 1.178.4.0 - 1.178.4.255)
            [None, None],  # Both in range
            [None, None],  # Both in range
            [None, "Value '1.178.3.255' in Column 'b' is not in the CIDR block '1.178.4.0/24'"],  # Just before ranges
            [None, "Value '1.178.5.0' in Column 'b' is not in the CIDR block '1.178.4.0/24'"],  # Just after ranges
            # Different prefix lengths testing
            [
                "Value '10.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '192.168.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [None, "Value '192.168.1.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'"],
            [
                "Value '192.168.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '10.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            # Special address ranges
            [
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '169.254.1.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '224.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '224.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '240.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            # Public address ranges
            [
                "Value '8.8.8.8' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '1.1.1.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '198.51.100.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '192.0.2.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '203.0.113.2' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            # Network and broadcast address testing
            [None, None],  # Network addresses in range
            [None, None],  # Addresses in range
            [None, None],
            # Edge case addresses
            [
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '255.255.255.255' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            [
                "Value '255.255.255.254' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '0.0.0.1' in Column 'b' is not in the CIDR block '1.178.4.0/24'",
            ],
            # Invalid addresses for error testing
            ["Value '256.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'", None],
            [
                "Value '172.16.1.256' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.300.4.5' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16.01.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.04.5' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16.1.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4.5.6' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value 'invalid.ip' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value 'also.invalid' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '192.168.-1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4.-1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16.1.' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4.' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '.172.16.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '.1.178.4.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172..16.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1..178.4.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16. 1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4. 1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value ' 172.16.1.1' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value ' 1.178.4.1' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
            [
                "Value '172.16.1.1 ' in Column 'a' does not match pattern 'IPV4_ADDRESS'",
                "Value '1.178.4.1 ' in Column 'b' does not match pattern 'IPV4_ADDRESS'",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_ipv4_address_cidr_edge_cases(spark):
    """Test comprehensive IPv4 CIDR edge cases including different prefix lengths."""
    schema_ipv4 = "a: string"

    test_df = spark.createDataFrame(
        [
            # Test different prefix lengths boundary cases
            ["192.168.1.1"],  # Single host testing
            ["192.168.1.2"],  # Different host
            ["0.0.0.0"],  # Universal range start
            ["255.255.255.255"],  # Universal range end
            ["127.0.0.1"],  # Loopback
            ["10.0.0.1"],  # Class A private
            ["172.16.0.1"],  # Class B private
            ["192.168.0.1"],  # Class C private
            ["224.0.0.1"],  # Multicast
            ["169.254.1.1"],  # Link-local
            ["203.0.113.1"],  # TEST-NET
            # Boundary testing
            ["10.0.0.0"],  # Network address
            ["10.255.255.255"],  # Broadcast address
            ["192.168.0.0"],  # Class C network
            ["192.168.255.255"],  # Class C broadcast
            ["172.16.0.0"],  # Class B network
            ["172.31.255.255"],  # Class B broadcast
        ],
        schema_ipv4,
    )

    actual = test_df.select(
        is_ipv4_address_in_cidr("a", "192.168.1.1/32").alias("a_is_not_ipv4_in_cidr_192_168_1_1_32"),  # Single host
        is_ipv4_address_in_cidr("a", "0.0.0.0/0").alias("a_is_not_ipv4_in_cidr_0_0_0_0_0"),  # Universal range
        is_ipv4_address_in_cidr("a", "10.0.0.0/8").alias("a_is_not_ipv4_in_cidr_10_0_0_0_8"),  # Class A
        is_ipv4_address_in_cidr("a", "172.16.0.0/12").alias("a_is_not_ipv4_in_cidr_172_16_0_0_12"),  # Class B
        is_ipv4_address_in_cidr("a", "192.168.0.0/16").alias("a_is_not_ipv4_in_cidr_192_168_0_0_16"),  # Class C range
        is_ipv4_address_in_cidr("a", "224.0.0.0/4").alias("a_is_not_ipv4_in_cidr_224_0_0_0_4"),  # Multicast
    )

    checked_schema = (
        "a_is_not_ipv4_in_cidr_192_168_1_1_32: string, "
        "a_is_not_ipv4_in_cidr_0_0_0_0_0: string, "
        "a_is_not_ipv4_in_cidr_10_0_0_0_8: string, "
        "a_is_not_ipv4_in_cidr_172_16_0_0_12: string, "
        "a_is_not_ipv4_in_cidr_192_168_0_0_16: string, "
        "a_is_not_ipv4_in_cidr_224_0_0_0_4: string"
    )

    expected = spark.createDataFrame(
        [
            # Test /32 (single host), /0 (all), /8 (Class A), /12 (Class B), /16 (Class C), /4 (multicast)
            [
                None,
                None,
                "Value '192.168.1.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '192.168.1.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
                "Value '192.168.1.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '192.168.1.2' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '192.168.1.2' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '192.168.1.2' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
                "Value '192.168.1.2' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '0.0.0.0' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '255.255.255.255' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '127.0.0.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '10.0.0.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                None,
                "Value '10.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '10.0.0.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '10.0.0.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '172.16.0.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '172.16.0.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                None,
                "Value '172.16.0.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '172.16.0.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '192.168.0.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '192.168.0.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '192.168.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
                "Value '192.168.0.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '224.0.0.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '224.0.0.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '224.0.0.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '224.0.0.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                None,
            ],
            [
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '169.254.1.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '203.0.113.1' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            # Boundary testing
            [
                "Value '10.0.0.0' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                None,
                "Value '10.0.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '10.0.0.0' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '10.0.0.0' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '10.255.255.255' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                None,
                "Value '10.255.255.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                "Value '10.255.255.255' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '10.255.255.255' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '192.168.0.0' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '192.168.0.0' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '192.168.0.0' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
                "Value '192.168.0.0' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '192.168.255.255' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '192.168.255.255' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                "Value '192.168.255.255' in Column 'a' is not in the CIDR block '172.16.0.0/12'",
                None,
                "Value '192.168.255.255' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '172.16.0.0' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '172.16.0.0' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                None,
                "Value '172.16.0.0' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '172.16.0.0' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
            [
                "Value '172.31.255.255' in Column 'a' is not in the CIDR block '192.168.1.1/32'",
                None,
                "Value '172.31.255.255' in Column 'a' is not in the CIDR block '10.0.0.0/8'",
                None,
                "Value '172.31.255.255' in Column 'a' is not in the CIDR block '192.168.0.0/16'",
                "Value '172.31.255.255' in Column 'a' is not in the CIDR block '224.0.0.0/4'",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_ipv4_cidr_invalid_blocks_raise_error(spark):
    schema_ipv4 = "a: string"
    test_df = spark.createDataFrame([["192.168.1.1"]], schema_ipv4)

    # Test various invalid CIDR block formats
    invalid_cidr_blocks = [
        "",  # Empty CIDR block
        "192.168.1.0/33",  # Prefix > 32
        "192.168.1.0/-1",  # Negative prefix
        "192.168.1.0/abc",  # Non-numeric prefix
        "256.168.1.0/24",  # Invalid IP address in CIDR
        "192.168.1.0",  # Missing prefix length
        "/24",  # Missing IP address
        "192.168.1.0/",  # Missing prefix value
        "192.168.1.g/24",  # Invalid character in IP
        "192.168..1/24",  # Double dots in IP
        "192.168.01.0/24",  # Leading zeros in IP
    ]

    for invalid_cidr in invalid_cidr_blocks:
        with pytest.raises(
            InvalidParameterError,
            match=r"CIDR block .* is not a valid IPv4 CIDR block|'cidr_block' must be a non-empty string",
        ):
            test_df.select(is_ipv4_address_in_cidr("a", invalid_cidr))


def test_col_is_valid_ipv6_address(spark):
    schema_ipv6 = "a: string"

    test_df = spark.createDataFrame(
        [
            # Invalid formats - IPv4 and malformed addresses
            ["192.170.01.1"],
            ["0.0.0.0"],
            ["abc.def.ghi.jkl"],
            [None],
            ["255255155255"],
            ["192.168.1.1.1"],
            [""],
            [" "],
            ["192.168.1.0/"],
            # Valid IPv6 addresses - basic formats
            ["::1"],  # Loopback
            ["12345"],  # Invalid - hextet too long
            ["::"],  # Unspecified
            ["2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64"],  # malformed IPv4-embedded suffix
            ["001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Leading zeros (valid)
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # All F's
            ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Full form
            ["fe80:0000:0000:0000:0202:b3ff:fe1e:8329"],  # Link-local full
            ["2001:0db8:0000:0000:0000:ff00:0042:8329"],  # Valid full
            ["2606:4700:4700:0000:0000:0000:0000:1111"],  # Cloudflare DNS
            ["2a03:2880:f12f:83:FACE:b00c:0000:25DE"],  # Mixed case
            ["2001:4860:4860:0000:0000:0000:0000:8888"],  # Google DNS
            ["2002:c0a8:0101:0000:0000:0000:c0a8:0101"],  # 6to4
            # Invalid formats - various malformed cases
            ["zFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:ZZZFFF"],  # Invalid hex chars
            ["2001:0018:0194:0c02:0001:02ff:fe03:0405"],  # Valid
            ["2000:0018:0194:0c02:0001:02ff:fe03:0405"],  # Valid
            ["FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"],  # All uppercase F's
            ["f:f:a:d:g:1:2:3"],  # Invalid hex char 'g'
            ["f:: "],  # Trailing space
            [" :: "],  # Leading/trailing spaces
            [" ::"],  # Leading space
            ["FF FF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF"],  # Space in address
            ["2000:00 8:0194:0c02:0001:02ff:fe03:0405"],  # Space in hextet
            [":::"],  # Too many colons
            ["0:0::d::"],  # Multiple compressions
            ["aaaa:"],  # Incomplete address
            ["aaaa"],  # Single hextet
            [":abcd"],  # Leading colon without compression
            ["::abcd"],  # Valid compression
            ["::abcg"],  # Invalid hex in compression
            ["::1bcg"],  # Invalid hex in compression
            ["::1bcf"],  # Valid compression
            ["1b::cf"],  # Valid compression
            ["1b::cf_"],  # Invalid character
            ["2000:0:0194:0c02:00+1:02ff:fe03:_10"],  # Invalid characters
            [".::"],  # Invalid format
            ["0::"],  # Valid compression
            ["0::0"],  # Valid compression
            ["0::z"],  # Invalid hex char
            ["::0:0194:0c02:1:02ff:fe03:10"],  # Valid compression
            ["1:2::3:4"],  # Valid compression
            ["1234::5678::abcd"],  # Multiple compressions (invalid)
            [":1234:5678:9abc:def0:1234:5678:9abc"],  # Leading colon
            ["1234:5678:9abc:def0:1234:5678:9abc:"],  # Trailing colon
            ["::1"],  # Loopback (duplicate for completeness)
            ["::"],  # Unspecified (duplicate for completeness)
            ["1::"],  # Valid compression
            ["::1:2:3:4:5:6"],  # Valid compression
            ["2001:db8::1"],  # Valid compression
            ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],  # Full form (duplicate)
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # All F's (duplicate)
            ["2001:DB8::1"],  # Mixed case
            ["fe80::1%eth0"],  # Zone ID (invalid in pure IPv6)
            ["fe80::%12"],  # Zone ID (invalid in pure IPv6)
            ["1:2:3:4:5:6:7:8"],  # Valid full form
            ["2001:db8::192.168.0.1"],  # IPv4-embedded
            ["::ffff:192.0.2.128"],  # IPv4-mapped
            ["2001:db8:abcd:0012::"],  # Valid compression
            # ADDITIONAL EDGE CASES - Link-local addresses
            ["fe80::1"],  # Valid link-local
            ["fe80::0000:0000:0000:1"],  # Link-local full form
            ["fe81::1"],  # Valid link-local variant
            ["fec0::1"],  # Site-local (deprecated but valid format)
            # Unique Local Addresses (ULA)
            ["fc00::1"],  # Valid ULA
            ["fd00::1"],  # Valid ULA (locally assigned)
            ["fb00::1"],  # Valid ULA
            ["fe00::1"],  # Outside ULA range
            # Multicast addresses
            ["ff00::1"],  # Valid multicast
            ["ff02::1"],  # All-nodes multicast
            ["ff05::1:3"],  # Valid multicast with scope
            ["fe02::1"],  # Just outside multicast range
            # Documentation addresses (RFC 3849)
            ["2001:db8::"],  # Valid documentation range
            ["2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # Max in doc range
            ["2001:db7::1"],  # Just outside documentation range
            ["2001:db9::1"],  # Just outside documentation range
            # Extreme compression edge cases
            ["1::"],  # Minimal compression at end (duplicate)
            ["::1:0:0:0:0:0:0"],  # Compression at start with trailing
            ["1:0:0:0::1"],  # Compression in middle
            ["::0:0:0:0:0:0:1"],  # Compression with zeros
            # Zero padding edge cases
            ["2001:0db8:0000:0000:0000:0000:0000:0001"],  # Excessive zeros
            ["2001:db8:0:0:0:0:0:1"],  # Minimal zeros
            ["0001:0002:0003:0004:0005:0006:0007:0008"],  # All with leading zeros
            # IPv4-mapped additional cases
            ["::ffff:0:0"],  # IPv4-mapped all zeros
            ["::ffff:255.255.255.255"],  # IPv4-mapped max values
            ["::ffff:127.0.0.1"],  # IPv4-mapped loopback
            ["::ffff:192.168.1.1"],  # IPv4-mapped private
            ["2001:db8::10.0.0.1"],  # IPv4-embedded in documentation range
            # Invalid edge cases
            ["12345::1"],  # Hextet too long (5 digits)
            ["1:2:3:4:5:6:7:8:9"],  # Too many hextets
            ["1::2::3"],  # Multiple compressions
            ["::1::"],  # Compression with extra colon
            ["1:2:3:4:5:6:7:"],  # Trailing colon without compression
            ["1:2:3:4:5:6:7"],  # Too few hextets without compression
            [":1:2:3:4:5:6:7:8"],  # Leading colon without compression
            ["g::1"],  # Invalid hexadecimal character
            ["1:2:3:4:5:6:7:8:"],  # Trailing colon with full address
            ["1:2:3:4:5:6:7::8"],  # Invalid compression position
            ["::g"],  # Invalid hex in compression
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff"],  # Hextet too long
            ["1:2:3:4:5:6:7:8:9:a"],  # Way too many hextets
            # Boundary cases
            ["0:0:0:0:0:0:0:0"],  # All zeros (should be ::)
            ["0000:0000:0000:0000:0000:0000:0000:0000"],  # All zeros with padding
            # Case sensitivity tests
            ["ABCD:EFAB:CDEF:1234:5678:9ABC:DEF0:1234"],  # All uppercase
            ["abcd:efab:cdef:1234:5678:9abc:def0:1234"],  # All lowercase
            ["AbCd:EfAb:CdEf:1234:5678:9aBc:DeF0:1234"],  # Mixed case
        ],
        schema_ipv6,
    )

    actual = test_df.select(is_valid_ipv6_address("a"))

    checked_schema = "a_does_not_match_pattern_ipv6_address: string"

    expected = spark.createDataFrame(
        [
            # Invalid formats - IPv4 and malformed addresses
            ["Value '192.170.01.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0.0.0.0' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'abc.def.ghi.jkl' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # NULL values should pass (return None)
            ["Value '255255155255' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.1.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '192.168.1.0/' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            # Valid IPv6 addresses - basic formats
            [None],  # ::1 - Valid loopback
            ["Value '12345' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # :: - Valid unspecified
            [
                "Value '2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64' in Column 'a' does not match pattern 'IPV6_ADDRESS'"
            ],  # malformed IPv4-embedded suffix
            [None],  # Leading zeros valid
            [None],  # All F's valid
            [None],  # Full form valid
            [None],  # Link-local full valid
            [None],  # Valid full
            [None],  # Cloudflare DNS valid
            [None],  # Mixed case valid
            [None],  # Google DNS valid
            [None],  # 6to4 valid
            # Invalid formats - various malformed cases
            ["Value 'zFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:ZZZFFF' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid
            [None],  # Valid
            [None],  # All uppercase F's valid
            ["Value 'f:f:a:d:g:1:2:3' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'f:: ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' :: ' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ' ::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'FF FF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:00 8:0194:0c02:0001:02ff:fe03:0405' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '0:0::d::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'aaaa' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':abcd' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::abcd valid
            ["Value '::abcg' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1bcg' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1bcf valid
            [None],  # 1b::cf valid
            ["Value '1b::cf_' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '2000:0:0194:0c02:00+1:02ff:fe03:_10' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '.::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # 0:: valid
            [None],  # 0::0 valid
            ["Value '0::z' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # Valid compression
            [None],  # Valid compression
            ["Value '1234::5678::abcd' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1234:5678:9abc:def0:1234:5678:9abc' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1234:5678:9abc:def0:1234:5678:9abc:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            [None],  # ::1 valid (duplicate)
            [None],  # :: valid (duplicate)
            [None],  # 1:: valid
            [None],  # Valid compression
            [None],  # Valid compression
            [None],  # Full form valid (duplicate)
            [None],  # All F's valid (duplicate)
            [None],  # Mixed case valid
            [None],  # Zone ID (invalid in pure IPv6)
            [None],  # Valid full form
            [None],  # Valid full form
            [None],  # IPv4-embedded valid
            [None],  # IPv4-mapped valid
            [None],  # Valid compression
            # ADDITIONAL EDGE CASES - Link-local addresses
            [None],  # fe80::1 valid
            [None],  # Link-local full form valid
            [None],  # fe81::1 valid
            [None],  # Site-local valid format
            # Unique Local Addresses (ULA)
            [None],  # fc00::1 valid
            [None],  # fd00::1 valid
            [None],  # Valid ULA
            [None],  # fe00::1 valid format (though not in ULA range)
            # Multicast addresses
            [None],  # ff00::1 valid multicast
            [None],  # ff02::1 valid multicast
            [None],  # ff05::1:3 valid multicast
            [None],  # fe02::1 valid format
            # Documentation addresses
            [None],  # 2001:db8:: valid
            [None],  # Max in doc range valid
            [None],  # 2001:db7::1 valid
            [None],  # 2001:db9::1 valid
            # Extreme compression edge cases
            [None],  # 1:: valid (duplicate)
            [None],  # Compression with trailing valid
            [None],  # Compression in middle valid
            [None],  # Compression with zeros valid
            # Zero padding edge cases
            [None],  # Excessive zeros valid
            [None],  # Minimal zeros valid
            [None],  # All with leading zeros valid
            # IPv4-mapped additional cases
            [None],  # IPv4-mapped zeros valid
            [None],  # IPv4-mapped max valid
            [None],  # IPv4-mapped loopback valid
            [None],  # IPv4-mapped private valid
            [None],  # IPv4-embedded in doc range valid
            # Invalid edge cases
            ["Value '12345::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1::2::3' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::1::' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value ':1:2:3:4:5:6:7:8' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'g::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7::8' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '::g' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffff' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            ["Value '1:2:3:4:5:6:7:8:9:a' in Column 'a' does not match pattern 'IPV6_ADDRESS'"],
            # Boundary cases
            [None],  # All zeros valid (0:0:0:0:0:0:0:0)
            [None],  # All zeros with padding valid
            # Case sensitivity tests
            [None],  # All uppercase valid
            [None],  # All lowercase valid
            [None],  # Mixed case valid
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_ipv6_address_in_cidr(spark):
    schema_ipv6 = "a: string, b: string"

    test_df = spark.createDataFrame(
        [
            ["2001:db8:abcd:0012", None],
            ["::1", "2002:c0a8:0101::1"],
            ["192.1", "1.01"],
            ["2001:db8:abcd:0012:0000:0000:0000:0001", "2001:db8:1234:5600::1"],
            ["2001:db8:abcd:0012::1", "2001:db8:1234:56ff::1"],
            ["2001:db8:ffff:0012::1", "2001:db9::1"],
            [None, None],
            ["", ""],
            ["::ffff:192.168.1.1", "2001:db8::192.168.1.1"],
            ["2001:db8:abcd:12::", "2001:db8:1234:56aa::1"],
            ["2001:DB8:ABCD:0012::FFFF", "2001:db8:1234:5700::1"],
            ["2001:db8:abcd:0013::1", "::ffff:192.0.2.128"],
            ["[2001:db8:abcd:0012::1]", "fe80::1%eth0"],
            ["2001:db8:abcd:0012:0:0:0:0", "2001:db8:1234:5600::192.0.2.128"],
            ["::", "2001:db8::192.168.1.1"],
            ["2001:db8:abcd:0012:ffff:ffff:ffff:ffff", "2001:db8:1234:56ff::ffff"],
            ["2001:db8:abcd:0012::dead", "2001:db8:1234:56ab::"],
            ["2001:DB8:ABCD:0012:0:0:BeEf:1", "2001:db8:1234:56ab::192.168.10.20"],
            ["2001:db8:abcd:0011::", "2001:db8:1234:55ff::1"],
            ["2001:db8:abgd::1", "2001:db8:1234:5800::"],
            ["2001:db8:abcd:0012::1", "2001:db8:1234:5600::"],
            ["2001:db8:abcd:12:0::1", "2001:db8:1234:56ff:ffff:ffff::"],
            ["::1", "::ffff:203.0.113.10"],
            ["::", "2001:db8:1234:5700::192.0.2.128"],
            ["2001:db8:abcd:0012:FFFF:ffff:FFFF:ffff", "2001:DB8:1234:56AA::"],
            ["2002::1", "2001:db8:1234:56:0:0:0:0:1"],
            ["2001:db8:abcd:0012::", "2001:db8:1234:56aa::10.0.0.1"],
            # ADDITIONAL EDGE CASES FOR CIDR TESTING
            # Boundary testing - first/last addresses in ranges
            ["2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # First and last in /32
            ["2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", "2001:db9::"],  # Just before/after range
            # Different prefix lengths
            ["2001:db8::1", "2001:db8::2"],  # Single host testing
            ["::1", "::2"],  # Loopback testing
            ["2001:db8::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Max address testing
            # Zero prefix length edge case (/0 matches everything)
            ["::", "2001:db8::"],  # All addresses should match /0
            # Link-local ranges
            ["fe80::1", "fec0::1"],  # Link-local testing
            # Multicast ranges
            ["ff02::1", "fe02::1"],  # Multicast testing
            # ULA ranges
            ["fc00::1", "fd00::1"],  # ULA testing
            ["fe00::1", "fb00::1"],  # Outside ULA testing
            # IPv4-embedded in CIDR
            ["::ffff:192.168.1.1", "::ffff:192.168.2.1"],  # IPv4-mapped testing
            # Case sensitivity in addresses
            ["2001:DB8::1", "2001:db8::1"],  # Mixed case testing
            # Compression variations
            ["2001:0:0:0:0:0:0:1", "2001::2"],  # Different compression styles
            # Invalid addresses for error testing
            ["invalid::address", "2001:db8::invalid"],  # Invalid formats
            ["12345::1", "g::1"],  # Invalid hex chars
        ],
        schema_ipv6,
    )

    # Test with multiple different CIDR blocks to cover various edge cases
    actual = test_df.select(
        is_ipv6_address_in_cidr("a", "2001:db8:abcd:0012::/64"),
        is_ipv6_address_in_cidr("b", "2001:db8:1234:5600::192.0.2.128/56"),
    )

    checked_schema = "a_is_not_ipv6_in_cidr: string, b_is_not_ipv6_in_cidr: string"
    expected = spark.createDataFrame(
        [
            ["Value '2001:db8:abcd:0012' in Column 'a' does not match pattern 'IPV6_ADDRESS'", None],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2002:c0a8:0101::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '192.1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '1.01' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [None, None],
            [None, None],
            [
                "Value '2001:db8:ffff:0012::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db9::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                "Value '' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [
                "Value '::ffff:192.168.1.1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::192.168.1.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                None,
                "Value '2001:db8:1234:5700::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8:abcd:0013::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:192.0.2.128' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '[2001:db8:abcd:0012::1]' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value 'fe80::1%eth0' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::192.168.1.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [None, None],
            [None, None],
            [
                "Value '2001:db8:abcd:0011::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:55ff::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8:abgd::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '2001:db8:1234:5800::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [None, None],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:203.0.113.10' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:5700::192.0.2.128' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [None, None],
            [
                "Value '2002::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:1234:56:0:0:0:0:1' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [None, None],
            # ADDITIONAL EDGE CASE RESULTS
            # Boundary testing - first/last addresses in ranges
            [
                "Value '2001:db8::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db9::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Different prefix lengths
            [
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Zero prefix length edge case
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Link-local ranges
            [
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fec0::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Multicast ranges
            [
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fe02::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # ULA ranges
            [
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fd00::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            [
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value 'fb00::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # IPv4-embedded in CIDR
            [
                "Value '::ffff:192.168.1.1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '::ffff:192.168.2.1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Case sensitivity in addresses (should still work)
            [
                "Value '2001:DB8::1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001:db8::1' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Compression variations
            [
                "Value '2001:0:0:0:0:0:0:1' in Column 'a' is not in the CIDR block '2001:db8:abcd:0012::/64'",
                "Value '2001::2' in Column 'b' is not in the CIDR block '2001:db8:1234:5600::192.0.2.128/56'",
            ],
            # Invalid addresses for error testing
            [
                "Value 'invalid::address' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value '2001:db8::invalid' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
            [
                "Value '12345::1' in Column 'a' does not match pattern 'IPV6_ADDRESS'",
                "Value 'g::1' in Column 'b' does not match pattern 'IPV6_ADDRESS'",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_ipv6_address_cidr_edge_cases(spark):
    """Test comprehensive IPv6 CIDR edge cases including different prefix lengths."""
    schema_ipv6 = "a: string"

    test_df = spark.createDataFrame(
        [
            # Boundary testing for different prefix lengths
            ["2001:db8::"],  # First in /32
            ["2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"],  # Last in /32
            ["2001:db7:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before /32
            ["2001:db9::"],  # Just after /32
            # Single host testing (/128)
            ["2001:db8::1"],  # Exact match for /128
            ["2001:db8::2"],  # Different host
            # Loopback testing
            ["::1"],  # Exact loopback
            ["::2"],  # Different loopback
            # Zero prefix testing (/0 - should match everything)
            ["::"],  # Unspecified
            ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Maximum address
            ["2001:db8::1"],  # Any address
            # Link-local prefix testing
            ["fe80::1"],  # Valid link-local
            ["fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before link-local
            ["fec0::1"],  # Just after link-local range
            # Multicast prefix testing
            ["ff00::1"],  # First multicast
            ["ff02::1"],  # All-nodes multicast
            ["feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Just before multicast
            # ULA prefix testing
            ["fc00::1"],  # First ULA
            ["fd00::1"],  # Local ULA
            ["fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"],  # Last ULA
            ["fe00::1"],  # Just after ULA
        ],
        schema_ipv6,
    )

    actual = test_df.select(
        # Test various prefix lengths
        is_ipv6_address_in_cidr("a", "2001:db8::/32"),  # /32 - 96 bits of network
        is_ipv6_address_in_cidr("a", "2001:db8::1/128"),  # /128 - single host
        is_ipv6_address_in_cidr("a", "::/0"),  # /0 - match everything
        is_ipv6_address_in_cidr("a", "fe80::/10"),  # /10 - link-local
        is_ipv6_address_in_cidr("a", "ff00::/8"),  # /8 - multicast
        is_ipv6_address_in_cidr("a", "fc00::/7"),  # /7 - ULA range
    )

    checked_schema = (
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string, "
        "a_is_not_ipv6_in_cidr: string"
    )

    expected = spark.createDataFrame(
        [
            # Test /32 range (2001:db8::/32)
            [
                None,
                "Value '2001:db8::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db7:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '2001:db9::' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db9::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Single host testing (/128)
            [
                None,
                None,
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::2' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Loopback testing
            [
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value '::2' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::2' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::2' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::2' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::2' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Zero prefix testing (/0 - should match everything)
            [
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value '::' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '::' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '::' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '::' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value '2001:db8::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            # Link-local prefix testing (/10)
            [
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe80::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                None,
                "Value 'fe80::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fec0::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fec0::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                None,
                "Value 'fec0::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            # Multicast prefix testing (/8)
            [
                "Value 'ff00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ff00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ff00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ff00::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'ff02::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'ff02::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                None,
                "Value 'ff02::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
            [
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            # ULA prefix testing (/7)
            [
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fc00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fc00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fc00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fd00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fd00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fd00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fd00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff' in Column 'a' is not in the CIDR block 'ff00::/8'",
                None,
            ],
            [
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8::/32'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block '2001:db8::1/128'",
                None,
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'fe80::/10'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'ff00::/8'",
                "Value 'fe00::1' in Column 'a' is not in the CIDR block 'fc00::/7'",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_ipv6_cidr_invalid_blocks_raise_error(spark):
    schema_ipv6 = "a: string"
    test_df = spark.createDataFrame([["2001:db8::1"]], schema_ipv6)

    # Test various invalid CIDR block formats
    invalid_cidr_blocks = [
        "",  # Empty CIDR block
        "2001:db8::/129",  # Prefix > 128
        "2001:db8::/-1",  # Negative prefix
        "2001:db8::/abc",  # Non-numeric prefix
        "invalid::/32",  # Invalid address in CIDR
        "2001:db8::",  # Missing prefix length
        "/32",  # Missing address
        "2001:db8::1/",  # Missing prefix value
        "2001:db8::g/64",  # Invalid hex in address
    ]

    for invalid_cidr in invalid_cidr_blocks:
        with pytest.raises(
            InvalidParameterError,
            match=r"CIDR block .* is not a valid IPv6 CIDR block|'cidr_block' must be a non-empty string",
        ):
            test_df.select(is_ipv6_address_in_cidr("a", invalid_cidr))


def test_contains_pii_fails_session_validation(spark):
    """
    We restrict running `does_not_contain_pii` using Databricks Connect due to limitations
    on the size of UDF dependencies. Because tests are run from a Databricks Connect session,
    we can only validate that the correct error is raised.

    Complete testing of `does_not_contain_pii` and its options has been added to e2e tests.
    We run many scenarios in `test_pii_detection_checks` to validate `contains_pii` from
    a Databricks workspace.
    """
    schema_pii = "col1: string"
    test_df = spark.createDataFrame([["Contact us at info@company.com"]], schema_pii)

    with pytest.raises(
        ImportError, match="'does_not_contain_pii' is not supported when running checks with Databricks Connect"
    ):
        test_df.select(pii_detection_funcs.does_not_contain_pii("col1"))


def test_is_data_fresh(spark, set_utc_timezone):
    input_schema = "a: string, b: timestamp, c: date, d: timestamp"
    test_df = spark.createDataFrame(
        [
            ["row1", datetime(2023, 1, 2, 0, 0, 0), datetime(2023, 1, 1).date(), datetime(2023, 1, 2, 0, 0, 0)],
            ["row2", datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1).date(), datetime(2025, 1, 1, 0, 0, 0)],
            ["row3", None, None, None],
            [
                "row4",
                datetime(2023, 12, 31, 23, 59, 59),
                datetime(2022, 12, 31).date(),
                datetime(2023, 12, 31, 0, 0, 0),
            ],
        ],
        input_schema,
    )

    reference_date = datetime(2024, 1, 1)
    mins_threshold_b = 120
    mins_threshold_c = 3600

    actual = test_df.select(
        is_data_fresh("b", mins_threshold_b, F.lit(reference_date)),
        is_data_fresh("b", mins_threshold_b, "2024-01-01"),
        is_data_fresh("c", mins_threshold_c, reference_date),
        is_data_fresh("d", mins_threshold_b, "b"),
    )

    checked_schema = (
        "b_is_data_fresh: string, b_is_data_fresh: string, c_is_data_fresh: string, d_is_data_fresh: string"
    )
    expected = spark.createDataFrame(
        [
            [
                "Value '2023-01-02 00:00:00' in Column 'b' is older than 120 minutes from base timestamp '2024-01-01 00:00:00'",
                "Value '2023-01-02 00:00:00' in Column 'b' is older than 120 minutes from base timestamp '2024-01-01 00:00:00'",
                "Value '2023-01-01' in Column 'c' is older than 3600 minutes from base timestamp '2024-01-01 00:00:00'",
                None,
            ],
            [None, None, None, None],
            [None, None, None, None],
            [
                None,
                None,
                "Value '2022-12-31' in Column 'c' is older than 3600 minutes from base timestamp '2024-01-01 00:00:00'",
                "Value '2023-12-31 00:00:00' in Column 'd' is older than 120 minutes from base timestamp '2023-12-31 23:59:59'",
            ],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_data_fresh_cur(spark, set_utc_timezone):
    input_schema = "a: timestamp, b: timestamp, c: timestamp"

    test_df = spark.createDataFrame(
        [[datetime.now(), datetime.now(), datetime.now()], [None, None, None]], input_schema
    )

    actual = test_df.select(is_data_fresh("a", 2), is_data_fresh("b", 2, "c"))

    checked_schema = "a_is_data_fresh: string, b_is_data_fresh: string"
    expected = spark.createDataFrame(
        [[None, None], [None, None]],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_not_equal_to(spark, set_utc_timezone):
    schema = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1]],
            [2, 1, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.01"), [2]],
            [1, 2, None, None, Decimal("0.99"), [1]],
            [None, None, None, None, None, [None]],
        ],
        schema,
    )

    actual = test_df.select(
        is_not_equal_to("a", 1).alias("a_equal_to_literal"),
        is_not_equal_to("a", "1").alias("a_equal_to_str_literal"),
        is_not_equal_to("a", F.col("b")).alias("a_equal_to_column"),
        is_not_equal_to("c", datetime(2025, 1, 1).date()),
        is_not_equal_to("c", "2025-01-01"),
        is_not_equal_to("d", datetime(2025, 1, 1)),
        is_not_equal_to("d", "2025-01-01 00:00:00"),
        is_not_equal_to("e", Decimal("1.00")),
        is_not_equal_to(F.try_element_at("f", F.lit(1)), 1),
    )

    expected_schema = (
        "a_equal_to_literal: string, a_equal_to_str_literal: string, a_equal_to_column: string, "
        "c_equal_to_value: string, c_equal_to_value: string, d_equal_to_value: string, d_equal_to_value: string, "
        "e_equal_to_value: string, try_element_at_f_1_equal_to_value: string"
    )

    expected = spark.createDataFrame(
        [
            [
                "Value '1' in Column 'a' is equal to value: 1",
                "Value '1' in Column 'a' is equal to value: 1",
                "Value '1' in Column 'a' is equal to value: 1",
                "Value '2025-01-01' in Column 'c' is equal to value: 2025-01-01",
                "Value '2025-01-01' in Column 'c' is equal to value: 2025-01-01",
                "Value '2025-01-01 00:00:00' in Column 'd' is equal to value: 2025-01-01 00:00:00",
                "Value '2025-01-01 00:00:00' in Column 'd' is equal to value: 2025-01-01 00:00:00",
                "Value '1.00' in Column 'e' is equal to value: 1.00",
                "Value '1' in Column 'try_element_at(f, 1)' is equal to value: 1",
            ],
            [None, None, None, None, None, None, None, None, None],
            [
                "Value '1' in Column 'a' is equal to value: 1",
                "Value '1' in Column 'a' is equal to value: 1",
                None,
                None,
                None,
                None,
                None,
                None,
                "Value '1' in Column 'try_element_at(f, 1)' is equal to value: 1",
            ],
            [None, None, None, None, None, None, None, None, None],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_col_is_equal_to(spark, set_utc_timezone):
    schema = "a: int, b: int, c: date, d: timestamp, e: decimal(10,2), f: array<int>"
    test_df = spark.createDataFrame(
        [
            [1, 1, datetime(2025, 1, 1).date(), datetime(2025, 1, 1), Decimal("1.00"), [1]],
            [2, 1, datetime(2025, 2, 1).date(), datetime(2025, 2, 1), Decimal("1.01"), [2]],
            [1, 2, None, None, Decimal("0.99"), [1]],
            [None, None, None, None, None, [None]],
        ],
        schema,
    )

    actual = test_df.select(
        is_equal_to("a", 1).alias("a_not_equal_to_value"),
        is_equal_to("a", "1").alias("a_not_equal_to_str_value"),
        is_equal_to("a", F.col("b")).alias("a_not_equal_to_value_col"),
        is_equal_to("c", datetime(2025, 1, 1).date()),
        is_equal_to("c", "2025-01-01"),
        is_equal_to("d", datetime(2025, 1, 1)),
        is_equal_to("d", "2025-01-01 00:00:00"),
        is_equal_to("e", Decimal("1.00")),
        is_equal_to(F.try_element_at("f", F.lit(1)), 1),
    )

    expected_schema = (
        "a_not_equal_to_value: string, a_not_equal_to_str_value: string, a_not_equal_to_value_col: string, "
        "c_not_equal_to_value: string, c_not_equal_to_value: string, "
        "d_not_equal_to_value: string, d_not_equal_to_value: string, "
        "e_not_equal_to_value: string, try_element_at_f_1_not_equal_to_value: string"
    )

    expected = spark.createDataFrame(
        [
            [None, None, None, None, None, None, None, None, None],
            [
                "Value '2' in Column 'a' is not equal to value: 1",
                "Value '2' in Column 'a' is not equal to value: 1",
                "Value '2' in Column 'a' is not equal to value: 1",
                "Value '2025-02-01' in Column 'c' is not equal to value: 2025-01-01",
                "Value '2025-02-01' in Column 'c' is not equal to value: 2025-01-01",
                "Value '2025-02-01 00:00:00' in Column 'd' is not equal to value: 2025-01-01 00:00:00",
                "Value '2025-02-01 00:00:00' in Column 'd' is not equal to value: 2025-01-01 00:00:00",
                "Value '1.01' in Column 'e' is not equal to value: 1.00",
                "Value '2' in Column 'try_element_at(f, 1)' is not equal to value: 1",
            ],
            [
                None,
                None,
                "Value '1' in Column 'a' is not equal to value: 2",
                None,
                None,
                None,
                None,
                "Value '0.99' in Column 'e' is not equal to value: 1.00",
                None,
            ],
            [None, None, None, None, None, None, None, None, None],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_valid_json(spark):
    schema = "a: string, b: string"
    test_df = spark.createDataFrame(
        [
            ['{"key": "value"}', '{"key": value}'],
            ['{"number": 123}', '{"number": 123}'],
            ['{"array": [1, 2, 3]}', '{"array": [1, 2, 3}'],
            ['Not a JSON string', 'Also not JSON'],
            [None, None],
            ['123', '"a string"'],
            ['true', 'null'],
            ['[]', '{}'],
            ['{"a": 1,}', '{key: "value"}'],
            ['[1, 2,', '{"a": "b"'],
            ["{'a': 'b'}", ''],
            [' {"a": 1} ', '{"b": 2}\n'],
        ],
        schema,
    )

    actual = test_df.select(is_valid_json("a"), is_valid_json("b"))

    expected_schema = "a_is_not_valid_json: string, b_is_not_valid_json: string"

    expected = spark.createDataFrame(
        [
            [None, "Value '{\"key\": value}' in Column 'b' is not a valid JSON string"],
            [None, None],
            [None, "Value '{\"array\": [1, 2, 3}' in Column 'b' is not a valid JSON string"],
            [
                "Value 'Not a JSON string' in Column 'a' is not a valid JSON string",
                "Value 'Also not JSON' in Column 'b' is not a valid JSON string",
            ],
            [None, None],
            [None, None],
            [None, None],
            [None, None],
            [
                "Value '{\"a\": 1,}' in Column 'a' is not a valid JSON string",
                "Value '{key: \"value\"}' in Column 'b' is not a valid JSON string",
            ],
            [
                "Value '[1, 2,' in Column 'a' is not a valid JSON string",
                "Value '{\"a\": \"b\"' in Column 'b' is not a valid JSON string",
            ],
            [
                "Value '{'a': 'b'}' in Column 'a' is not a valid JSON string",
                "Value '' in Column 'b' is not a valid JSON string",
            ],
            [None, None],
        ],
        expected_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_json_keys_require_all_true(spark):
    schema = "a: string, b: string"
    test_df = spark.createDataFrame(
        [
            ['{"key": "value", "another_key": 123}', '{"key": "value"}'],
            ['{"number": 123}', '{"number": 123, "extra": true}'],
            ['{"array": [1, 2, 3]}', '{"array": {1, 2, 3}]'],
            ['{"key": "value"}', '{"missing_key": "value"}'],
            [None, None],
            ['Not a JSON string', '{"key": "value"}'],
            ['{"key": "value"}', 'Not a JSON string'],
            ['{"key": "value"}', None],
            [None, '{"key": "value"}'],
            ['{"nested": {"inner_key": "inner_value"}}', '{"nested": {"inner_key": "inner_value"}}'],
            ['{"key": null, "another_key": null}', '{"nested": {"key": null}}'],
        ],
        schema,
    )

    actual = test_df.select(
        has_json_keys("a", ["key", "another_key"]),
        has_json_keys("b", ["key"]),
    )

    expected_schema = "a_does_not_have_json_keys: string, b_does_not_have_json_keys: string"

    expected = spark.createDataFrame(
        [
            [None, None],
            [
                "Value '{\"number\": 123}' in Column 'a' is missing keys in the list: [key, another_key]",
                "Value '{\"number\": 123, \"extra\": true}' in Column 'b' is missing keys in the list: [key]",
            ],
            [
                "Value '{\"array\": [1, 2, 3]}' in Column 'a' is missing keys in the list: [key, another_key]",
                "Value '{\"array\": {1, 2, 3}]' in Column 'b' is not a valid JSON string",
            ],
            [
                "Value '{\"key\": \"value\"}' in Column 'a' is missing keys in the list: [key, another_key]",
                "Value '{\"missing_key\": \"value\"}' in Column 'b' is missing keys in the list: [key]",
            ],
            [None, None],
            ["Value 'Not a JSON string' in Column 'a' is not a valid JSON string", None],
            [
                "Value '{\"key\": \"value\"}' in Column 'a' is missing keys in the list: [key, another_key]",
                "Value 'Not a JSON string' in Column 'b' is not a valid JSON string",
            ],
            ["Value '{\"key\": \"value\"}' in Column 'a' is missing keys in the list: [key, another_key]", None],
            [None, None],
            [
                "Value '{\"nested\": {\"inner_key\": \"inner_value\"}}' in Column 'a' is missing keys in the list: [key, another_key]",
                "Value '{\"nested\": {\"inner_key\": \"inner_value\"}}' in Column 'b' is missing keys in the list: [key]",
            ],
            [
                None,
                "Value '{\"nested\": {\"key\": null}}' in Column 'b' is missing keys in the list: [key]",
            ],
        ],
        expected_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_json_keys_require_at_least_one(spark):
    schema = "a: string, b: string"
    required_keys = ["key", "another_key", "extra_key"]

    test_df = spark.createDataFrame(
        [
            ['{"key": 1, "another_key": 2, "extra_key": 3}', '{"key": 1, "another_key": 2, "extra_key": 3}'],
            ['{"key": 1}', '{"key": 1}'],
            ['{"number": 123}', '{"random_sample": 1523}'],
            ['{}', '{}'],
            ['{"key": "value"', '{"key": "value"'],
            [None, 'Not a JSON string'],
            [None, None],
            ['{"key": null}', '{"nested": {"key": null}}'],
        ],
        schema,
    )

    actual = test_df.select(
        has_json_keys("a", required_keys, require_all=False),
        has_json_keys("b", required_keys, require_all=False),
    )

    expected_schema = "a_does_not_have_json_keys: string, b_does_not_have_json_keys: string"

    expected = spark.createDataFrame(
        [
            [None, None],
            [None, None],
            [
                "Value '{\"number\": 123}' in Column 'a' is missing keys in the list: [key, another_key, extra_key]",
                "Value '{\"random_sample\": 1523}' in Column 'b' is missing keys in the list: [key, another_key, extra_key]",
            ],
            [
                "Value '{}' in Column 'a' is missing keys in the list: [key, another_key, extra_key]",
                "Value '{}' in Column 'b' is missing keys in the list: [key, another_key, extra_key]",
            ],
            [
                "Value '{\"key\": \"value\"' in Column 'a' is not a valid JSON string",
                "Value '{\"key\": \"value\"' in Column 'b' is not a valid JSON string",
            ],
            [None, "Value 'Not a JSON string' in Column 'b' is not a valid JSON string"],
            [None, None],
            [
                None,
                "Value '{\"nested\": {\"key\": null}}' in Column 'b' is missing keys in the list: [key, another_key, extra_key]",
            ],
        ],
        expected_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_valid_json_schema(spark):
    schema = "a: string, b: string"
    test_df = spark.createDataFrame(
        [
            ['{"a": 1, "b": 2}', '{"a": 3, "b": 4}'],
            ['{"key": "value", "another_key": 123}', '{"key": "value"}'],
            ['{"number": 123}', '{"number": 123, "extra": true}'],
            ['{"array": [1, 2, 3]}', '{"array": {1, 2, 3}]'],
            ['{"key": "value"}', '{"missing_key": "value"}'],
            [None, None],
            ['Not a JSON string', '{"key": "value"}'],
            ['{"key": "value"}', 'Not a JSON string'],
            ['{"key": "value"}', None],
        ],
        schema,
    )

    json_schema = "STRUCT<a: BIGINT NOT NULL, b: BIGINT NOT NULL>"
    expected_schema = "a_has_invalid_json_schema: string, b_has_invalid_json_schema: string"
    expected = spark.createDataFrame(
        [
            [None, None],
            [
                "Value '{\"key\": \"value\", \"another_key\": 123}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                "Value '{\"key\": \"value\"}' in Column 'b' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
            ],
            [
                "Value '{\"number\": 123}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                "Value '{\"number\": 123, \"extra\": true}' in Column 'b' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
            ],
            [
                "Value '{\"array\": [1, 2, 3]}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                "Value '{\"array\": {1, 2, 3}]' in Column 'b' is not a valid JSON string",
            ],
            [
                "Value '{\"key\": \"value\"}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                "Value '{\"missing_key\": \"value\"}' in Column 'b' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
            ],
            [None, None],
            [
                "Value 'Not a JSON string' in Column 'a' is not a valid JSON string",
                "Value '{\"key\": \"value\"}' in Column 'b' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
            ],
            [
                "Value '{\"key\": \"value\"}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                "Value 'Not a JSON string' in Column 'b' is not a valid JSON string",
            ],
            [
                "Value '{\"key\": \"value\"}' in Column 'a' does not conform to expected JSON schema: struct<a:bigint,b:bigint>",
                None,
            ],
        ],
        expected_schema,
    )
    actual = test_df.select(
        has_valid_json_schema("a", json_schema),
        has_valid_json_schema("b", json_schema),
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_valid_json_schema_with_nested_depth_5(spark):
    """Test has_valid_json_schema with nested fields of depth 5."""
    schema = "json_data: string"
    test_data = [
        ['{"level1": {"level2": {"level3": {"level4": {"level5": "value"}}}}}'],
        ['{"level1": {"level2": {"level3": {"level4": {"level5": 0.12}}}}}'],
        ['{"level1": {"level2": {"level3": {"level4": {"level5": null}}}}}'],
        ['{"level1": {"level2": {"level3": {"level4": {"level5": "0.123"}}}}}'],
        ['{"level1": {"level2": {"level3": {"level4": null}}}}'],
        ['{"level1": {"level2": {"level3": {"level4": {"level6": "sample"}}}}}'],
        [None],
        ['{"level1": null}'],
        ['Not a JSON string'],
    ]

    test_df = spark.createDataFrame(test_data, schema)

    json_schema = "struct<level1:struct<level2:struct<level3:struct<level4:struct<level5:string NOT NULL>>>>>"
    expected_schema = "json_data_has_invalid_json_schema: string"
    expected = spark.createDataFrame(
        [
            [None],
            [None],
            [
                "Value '{\"level1\": {\"level2\": {\"level3\": {\"level4\": {\"level5\": null}}}}}' in Column 'json_data' does not conform to expected JSON schema: struct<level1:struct<level2:struct<level3:struct<level4:struct<level5:string>>>>>"
            ],
            [None],
            [None],
            [
                "Value '{\"level1\": {\"level2\": {\"level3\": {\"level4\": {\"level6\": \"sample\"}}}}}' in Column 'json_data' does not conform to expected JSON schema: struct<level1:struct<level2:struct<level3:struct<level4:struct<level5:string>>>>>",
            ],
            [None],
            [None],
            ["Value 'Not a JSON string' in Column 'json_data' is not a valid JSON string"],
        ],
        expected_schema,
    )
    actual = test_df.select(
        has_valid_json_schema("json_data", json_schema),
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_valid_json_schema_nullability(spark):
    schema = "json_data: string"
    json_schema = "id int, name string not null"

    test_df = spark.createDataFrame(
        [['{"id": 1, "name": "valid"}'], ['{"id": 1, "name": null}'], ['{"id": 1}'], [None], ["json_data string"]],
        schema,
    )

    expected_schema = "json_data_has_invalid_json_schema: string"
    expected = spark.createDataFrame(
        [
            [None],
            [
                "Value '{\"id\": 1, \"name\": null}' in Column 'json_data' does not conform to expected JSON schema: struct<id:int,name:string>"
            ],
            [
                "Value '{\"id\": 1}' in Column 'json_data' does not conform to expected JSON schema: struct<id:int,name:string>"
            ],
            [None],
            ["Value 'json_data string' in Column 'json_data' is not a valid JSON string"],
        ],
        expected_schema,
    )

    actual = test_df.select(has_valid_json_schema("json_data", json_schema))
    assert_df_equality(actual, expected)


def test_has_valid_json_schema_with_decimal_fields(spark):
    schema = "json_data: string"
    test_data = [
        ['{"price": 19.99, "discount": 0.15}'],
        ['{"price": 99.99, "discount": 0.5}'],
        ['{"price": 0.01, "discount": 0.0}'],
        ['{"price": 0.01, "discount": null}'],
        ['{"price": true, "discount": false}'],
        [None],
    ]
    test_df = spark.createDataFrame(test_data, schema)

    json_schema = "STRUCT<price: DOUBLE, discount: DOUBLE>"
    expected_schema = "json_data_has_invalid_json_schema: string"
    expected = spark.createDataFrame(
        [
            [None],
            [None],
            [None],
            [None],
            [
                "Value '{\"price\": true, \"discount\": false}' in Column 'json_data' does not conform to expected JSON schema: struct<price:double,discount:double>"
            ],
            [None],
        ],
        expected_schema,
    )
    actual = test_df.select(
        has_valid_json_schema("json_data", json_schema),
    )
    assert_df_equality(actual, expected)


def test_has_valid_json_schema_with_complex_nested_structure(spark):
    """Test has_valid_json_schema with complex nested structure - VALID case."""
    schema = "json_data: string"
    test_df = spark.createDataFrame(
        [
            ['{"user": {"id": 1, "profile": {"name": "John", "age": 30}}, "tags": ["admin", "user"]}'],
            ['{"user": {"id": 2, "profile": {"name": "Jane", "age": 25}}, "tags": []}'],
            [None],
            ['{"user": {"id": "invalid", "profile": {"name": "John", "age": 30}}, "tags": ["admin"]}'],
            ['{"user": {"id": 1, "profile": {"name": 123, "age": "thirty"}}, "tags": ["admin"]}'],
            ['{"user": {"id": 1, "profile": null}, "tags": ["admin"]}'],
        ],
        schema,
    )

    json_schema = "struct<user:struct<id:bigint,profile:struct<name:string,age:bigint>>,tags:array<string>>"
    expected_schema = "json_data_has_invalid_json_schema: string"
    expected = spark.createDataFrame(
        [
            [None],
            [None],
            [None],
            [
                "Value '{\"user\": {\"id\": \"invalid\", \"profile\": {\"name\": \"John\", \"age\": 30}}, \"tags\": [\"admin\"]}' in Column 'json_data' does not conform to expected JSON schema: struct<user:struct<id:bigint,profile:struct<name:string,age:bigint>>,tags:array<string>>"
            ],
            [
                "Value '{\"user\": {\"id\": 1, \"profile\": {\"name\": 123, \"age\": \"thirty\"}}, \"tags\": [\"admin\"]}' in Column 'json_data' does not conform to expected JSON schema: struct<user:struct<id:bigint,profile:struct<name:string,age:bigint>>,tags:array<string>>"
            ],
            [None],
        ],
        expected_schema,
    )
    actual = test_df.select(
        has_valid_json_schema("json_data", json_schema),
    )
    assert_df_equality(actual, expected, ignore_nullable=True)
