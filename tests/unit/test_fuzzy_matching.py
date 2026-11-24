from unittest.mock import create_autospec

import pytest
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.fuzzy_matching import (
    NEAREST_BY_CANDIDATES,
    FuzzyMatcher,
    FuzzyMatchMethod,
    build_fuzzy_matcher,
    build_nearest_by_query,
    match_best_reference_rows,
    uses_vector_matching,
)


def test_build_fuzzy_matcher_returns_none_for_exact_matching():
    assert build_fuzzy_matcher(None, 1) is None


def test_build_fuzzy_matcher_defaults_to_levenshtein():
    assert build_fuzzy_matcher({"max_distance": 2}, 2) == FuzzyMatcher(
        method=FuzzyMatchMethod.LEVENSHTEIN, max_distance=2.0
    )


def test_build_fuzzy_matcher_reads_all_options():
    matcher = build_fuzzy_matcher(
        {"method": "levenshtein", "min_similarity": 0.8, "case_sensitive": False, "trim_strings": True}, 1
    )
    assert matcher == FuzzyMatcher(min_similarity=0.8, case_sensitive=False, trim_strings=True)


@pytest.mark.parametrize(
    "fuzzy_matching_options, num_columns, message",
    [
        (["levenshtein"], 1, "'fuzzy_match' must be a dictionary"),
        ({"max_distance": 1, "distance": 1}, 1, "Unknown 'fuzzy_match' options: distance"),
        (
            {"method": "jaro_winkler", "max_distance": 1},
            1,
            "Invalid fuzzy match method: 'jaro_winkler'. Use one of: levenshtein, vector_cosine, vector_l2.",
        ),
        ({"method": 1, "max_distance": 1}, 1, "option 'method' must be a string"),
        ({}, 1, "exactly one of 'max_distance' or 'min_similarity'"),
        ({"max_distance": 1, "min_similarity": 0.5}, 1, "exactly one of 'max_distance' or 'min_similarity'"),
        ({"max_distance": -1}, 1, "'max_distance' must be non-negative"),
        ({"max_distance": "1"}, 1, "option 'max_distance' must be a number"),
        ({"max_distance": True}, 1, "option 'max_distance' must be a number"),
        ({"min_similarity": 1.5}, 1, "'min_similarity' must be between 0 and 1"),
        ({"max_distance": float("nan")}, 1, "must be finite numbers"),
        ({"max_distance": float("inf")}, 1, "must be finite numbers"),
        ({"min_similarity": float("nan")}, 1, "must be finite numbers"),
        ({"max_distance": 1}, 0, "requires at least one key column"),
        ({"max_distance": 1, "case_sensitive": "no"}, 1, "option 'case_sensitive' must be a boolean"),
        ({"method": "vector_l2", "min_similarity": 0.9}, 1, "'min_similarity' is not supported for 'vector_l2'"),
        ({"method": "vector_cosine", "max_distance": 0.1, "trim_strings": True}, 1, "apply to 'levenshtein'"),
        ({"method": "vector_cosine", "max_distance": 0.1}, 2, "requires exactly one embedding column"),
    ],
)
def test_build_fuzzy_matcher_rejects_invalid_options(fuzzy_matching_options, num_columns, message):
    with pytest.raises(InvalidParameterError, match=message):
        build_fuzzy_matcher(fuzzy_matching_options, num_columns)


def test_build_nearest_by_query_for_cosine_similarity():
    query = build_nearest_by_query("k1", "r1", FuzzyMatchMethod.VECTOR_COSINE)
    assert query == (
        "SELECT k.*, r.* FROM {keys} AS k "
        f"INNER JOIN {{reference}} AS r EXACT NEAREST {NEAREST_BY_CANDIDATES} "
        "BY SIMILARITY vector_cosine_similarity(k.`k1`, r.`r1`)"
    )


def test_build_nearest_by_query_for_l2_distance():
    query = build_nearest_by_query("k1", "r1", FuzzyMatchMethod.VECTOR_L2)
    assert query == (
        "SELECT k.*, r.* FROM {keys} AS k "
        f"INNER JOIN {{reference}} AS r EXACT NEAREST {NEAREST_BY_CANDIDATES} "
        "BY DISTANCE vector_l2_distance(k.`k1`, r.`r1`)"
    )


def test_build_nearest_by_query_rejects_string_methods():
    with pytest.raises(InvalidParameterError, match="'levenshtein' is not a vector match method"):
        build_nearest_by_query("k1", "r1", FuzzyMatchMethod.LEVENSHTEIN)


def test_build_nearest_by_query_escapes_backticks_in_column_names():
    query = build_nearest_by_query("emb`x", "ref``y", FuzzyMatchMethod.VECTOR_L2)
    assert query.endswith("BY DISTANCE vector_l2_distance(k.`emb``x`, r.`ref````y`)")


def test_build_nearest_by_query_keeps_an_injection_attempt_inside_the_identifier():
    query = build_nearest_by_query("k1`) AS d FROM x; --", "r1", FuzzyMatchMethod.VECTOR_L2)
    assert "k.`k1``) AS d FROM x; --`" in query


def test_build_nearest_by_query_escapes_braces_for_the_query_formatter():
    query = build_nearest_by_query("emb{x}", "r1", FuzzyMatchMethod.VECTOR_L2)
    assert "k.`emb{{x}}`" in query
    assert "k.`emb{x}`" in query.format(keys="keys", reference="reference")


@pytest.mark.parametrize(
    "method, expected",
    [
        (FuzzyMatchMethod.LEVENSHTEIN, "CAST(levenshtein(a, b) AS DOUBLE)"),
        (FuzzyMatchMethod.VECTOR_L2, "CAST(CallFunction('vector_l2_distance', a, b) AS DOUBLE)"),
        (FuzzyMatchMethod.VECTOR_COSINE, "-(1.0, CAST(CallFunction('vector_cosine_similarity', a, b) AS DOUBLE))"),
    ],
)
def test_fuzzy_match_method_distance_uses_the_member_sql_function(method, expected):
    assert str(method.distance(F.col("a"), F.col("b"))) == f"Column<'{expected}'>"


@pytest.mark.parametrize(
    "arguments, expected",
    [
        ({"fuzzy_matching_options": {"method": "vector_cosine", "min_similarity": 0.9}}, True),
        ({"fuzzy_matching_options": {"method": "vector_l2", "max_distance": 1}}, True),
        ({"fuzzy_matching_options": {"method": "levenshtein", "max_distance": 1}}, False),
        ({"fuzzy_matching_options": {"max_distance": 1}}, False),
        ({"fuzzy_matching_options": None}, False),
        ({}, False),
    ],
)
def test_uses_vector_matching(arguments, expected):
    assert uses_vector_matching(arguments) is expected


@pytest.mark.parametrize(
    "matcher, expected",
    [
        (FuzzyMatcher(max_distance=2.0), "<=(d, 2.0)"),
        (FuzzyMatcher(min_similarity=0.75), ">=(-(1.0, /(d, greatest(n, 1))), 0.75)"),
        (FuzzyMatcher(method=FuzzyMatchMethod.VECTOR_COSINE, min_similarity=0.9), ">=(-(1.0, d), 0.9)"),
    ],
)
def test_fuzzy_matcher_within_threshold(matcher, expected):
    assert str(matcher.within_threshold(F.col("d"), F.col("n"))) == f"Column<'{expected}'>"


@pytest.mark.parametrize(
    "value, method",
    [
        ("levenshtein", FuzzyMatchMethod.LEVENSHTEIN),
        ("vector_cosine", FuzzyMatchMethod.VECTOR_COSINE),
        ("vector_l2", FuzzyMatchMethod.VECTOR_L2),
    ],
)
def test_build_fuzzy_matcher_parses_method_names(value, method):
    options = {"method": value, "max_distance": 1}
    matcher = build_fuzzy_matcher(options, 1)
    assert matcher is not None
    assert matcher.method is method


@pytest.mark.parametrize(
    "method, is_vector, supports_min_similarity",
    [
        (FuzzyMatchMethod.LEVENSHTEIN, False, True),
        (FuzzyMatchMethod.VECTOR_COSINE, True, True),
        (FuzzyMatchMethod.VECTOR_L2, True, False),
    ],
)
def test_fuzzy_match_method_capabilities(method, is_vector, supports_min_similarity):
    assert method.uses_nearest_by_search is is_vector
    assert method.supports_min_similarity is supports_min_similarity


@pytest.mark.parametrize("streaming_side", ["source", "reference"])
def test_match_best_reference_rows_rejects_streaming_dataframes(streaming_side):
    df = create_autospec(DataFrame, instance=True)
    df.isStreaming = streaming_side == "source"
    ref_df = create_autospec(DataFrame, instance=True)
    ref_df.isStreaming = streaming_side == "reference"
    spark = create_autospec(SparkSession, instance=True)
    with pytest.raises(InvalidParameterError, match="does not support streaming inputs"):
        match_best_reference_rows(df, ref_df, ["a"], ["b"], FuzzyMatcher(max_distance=1), "dist", spark)


def _batch_dataframe() -> DataFrame:
    df = create_autospec(DataFrame, instance=True)
    df.isStreaming = False
    df.columns = ["a"]
    return df


def test_vector_matching_runs_nearest_by_query():
    spark = create_autospec(SparkSession, instance=True)
    match_best_reference_rows(
        _batch_dataframe(),
        _batch_dataframe(),
        ["a"],
        ["b"],
        FuzzyMatcher(method=FuzzyMatchMethod.VECTOR_L2, max_distance=0.5),
        "dist",
        spark,
    )
    query = spark.sql.call_args.args[0]
    assert f"EXACT NEAREST {NEAREST_BY_CANDIDATES} BY DISTANCE vector_l2_distance" in query
    assert set(spark.sql.call_args.kwargs) == {"keys", "reference"}


def test_vector_matching_leaves_no_session_views():
    df = _batch_dataframe()
    ref_df = _batch_dataframe()
    spark = create_autospec(SparkSession, instance=True)
    match_best_reference_rows(
        df,
        ref_df,
        ["a"],
        ["b"],
        FuzzyMatcher(method=FuzzyMatchMethod.VECTOR_COSINE, max_distance=0.1),
        "dist",
        spark,
    )
    df.createOrReplaceTempView.assert_not_called()
    ref_df.createOrReplaceTempView.assert_not_called()


@pytest.mark.parametrize("columns, ref_columns", [([], []), (["a"], []), (["a", "b"], ["a"])])
def test_match_best_reference_rows_rejects_empty_or_unequal_columns(columns, ref_columns):
    spark = create_autospec(SparkSession, instance=True)
    with pytest.raises(InvalidParameterError, match="must be non-empty and have the same length"):
        match_best_reference_rows(
            _batch_dataframe(),
            _batch_dataframe(),
            columns,
            ref_columns,
            FuzzyMatcher(max_distance=1),
            "dist",
            spark,
        )
