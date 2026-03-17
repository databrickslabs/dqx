from unittest.mock import create_autospec

import pytest
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from databricks.labs.dqx.profiler.profile import DQProfile
from databricks.labs.dqx.profiler.profile_builder import (
    PROFILE_BUILDER_REGISTRY,
    make_is_in_profile,
    make_min_max_profile,
    make_null_or_empty_profile,
)


@pytest.fixture
def mock_df():
    df = create_autospec(DataFrame)
    return df


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_profile_builder_registry_contains_expected_builders():
    assert "null_or_empty" in PROFILE_BUILDER_REGISTRY
    assert "is_in" in PROFILE_BUILDER_REGISTRY
    assert "min_max" in PROFILE_BUILDER_REGISTRY


# ---------------------------------------------------------------------------
# make_null_or_empty_profile — text types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("column_type", [T.StringType(), T.CharType(10), T.VarcharType(50)])
def test_null_or_empty_text_no_nulls_no_empties_returns_not_null_or_empty(mock_df, column_type):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        column_type,
        {"null_count": 0, "empty_count": 0, "total_count": 10},
        {"max_null_ratio": 0.0, "max_empty_ratio": 0.0},
    )
    assert profile == DQProfile(
        name="is_not_null_or_empty", column="col", description=None, parameters={"trim_strings": True}, filter=None
    )


def test_null_or_empty_text_nulls_and_empties_within_threshold_has_description(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 1, "empty_count": 1, "total_count": 10},
        {"max_null_ratio": 0.2, "max_empty_ratio": 0.2},
    )
    assert profile is not None
    assert profile.name == "is_not_null_or_empty"
    assert "10.0% of null values" in profile.description
    assert "10.0% of empty values" in profile.description


def test_null_or_empty_text_nulls_exceed_threshold_empty_ok_returns_is_not_empty(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 5, "empty_count": 0, "total_count": 10},
        {"max_null_ratio": 0.3, "max_empty_ratio": 0.0},
    )
    assert profile is not None
    assert profile.name == "is_not_empty"
    assert profile.parameters == {"trim_strings": True}


def test_null_or_empty_text_empties_exceed_threshold_null_ok_returns_is_not_null(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 0, "empty_count": 5, "total_count": 10},
        {"max_null_ratio": 0.0, "max_empty_ratio": 0.3},
    )
    assert profile is not None
    assert profile.name == "is_not_null"
    assert profile.parameters is None


def test_null_or_empty_text_both_exceed_threshold_returns_none(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 5, "empty_count": 4, "total_count": 10},
        {"max_null_ratio": 0.3, "max_empty_ratio": 0.3},
    )
    assert profile is None


def test_null_or_empty_text_trim_strings_false_propagated(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 0, "empty_count": 0, "total_count": 5},
        {"max_null_ratio": 0.0, "max_empty_ratio": 0.0, "trim_strings": False},
    )
    assert profile is not None
    assert profile.parameters == {"trim_strings": False}


def test_null_or_empty_text_filter_propagated(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 0, "empty_count": 0, "total_count": 5},
        {"max_null_ratio": 0.0, "max_empty_ratio": 0.0, "filter": "x > 0"},
    )
    assert profile is not None
    assert profile.filter == "x > 0"


def test_null_or_empty_text_empty_dataframe_returns_none(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        T.StringType(),
        {"null_count": 0, "empty_count": 0, "total_count": 0},
        {},
    )
    assert profile is None


# ---------------------------------------------------------------------------
# make_null_or_empty_profile — non-text types
# ---------------------------------------------------------------------------


def test_null_or_empty_non_text_no_nulls_returns_is_not_null(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "age",
        T.IntegerType(),
        {"null_count": 0, "total_count": 10},
        {"max_null_ratio": 0.0},
    )
    assert profile == DQProfile(name="is_not_null", column="age", description=None, parameters=None, filter=None)


def test_null_or_empty_non_text_nulls_within_threshold_has_description(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "age",
        T.IntegerType(),
        {"null_count": 1, "total_count": 10},
        {"max_null_ratio": 0.2},
    )
    assert profile is not None
    assert profile.name == "is_not_null"
    assert "10.0%" in profile.description
    assert "20.0%" in profile.description


def test_null_or_empty_non_text_nulls_exceed_threshold_returns_none(mock_df):
    profile = make_null_or_empty_profile(
        mock_df,
        "age",
        T.IntegerType(),
        {"null_count": 5, "total_count": 10},
        {"max_null_ratio": 0.3},
    )
    assert profile is None


@pytest.mark.parametrize("column_type", [T.LongType(), T.DoubleType(), T.DateType(), T.BooleanType()])
def test_null_or_empty_non_text_types_no_nulls_return_is_not_null(mock_df, column_type):
    profile = make_null_or_empty_profile(
        mock_df,
        "col",
        column_type,
        {"null_count": 0, "total_count": 5},
        {"max_null_ratio": 0.0},
    )
    assert profile is not None
    assert profile.name == "is_not_null"


# ---------------------------------------------------------------------------
# make_is_in_profile
# ---------------------------------------------------------------------------


def _make_mock_df(columns: list, distinct_values: list) -> DataFrame:
    """Helper to create a spec'd mock DataFrame for is_in profile tests."""
    mock_distinct_df = create_autospec(DataFrame)
    mock_distinct_df.count.return_value = len(distinct_values)
    mock_distinct_df.collect.return_value = [[v] for v in distinct_values]
    mock_distinct_df.distinct.return_value = mock_distinct_df

    df = create_autospec(DataFrame)
    df.columns = columns
    df.select.return_value = mock_distinct_df
    return df


@pytest.mark.parametrize("column_type", [T.DoubleType(), T.FloatType(), T.BooleanType(), T.DateType()])
def test_is_in_unsupported_type_returns_none(mock_df, column_type):
    assert make_is_in_profile(mock_df, "col", column_type, {"total_count": 10}, {}) is None


def test_is_in_total_count_zero_returns_none(mock_df):
    assert make_is_in_profile(mock_df, "col", T.IntegerType(), {"total_count": 0}, {}) is None


def test_is_in_no_distinct_values_returns_none():
    df = _make_mock_df(["col"], [])
    assert (
        make_is_in_profile(df, "col", T.StringType(), {"total_count": 3}, {"max_in_count": 10, "distinct_ratio": 1.0})
        is None
    )


def test_is_in_conditions_met_returns_profile():
    df = _make_mock_df(["col"], [1, 2, 3])
    profile = make_is_in_profile(
        df,
        "status",
        T.IntegerType(),
        {"total_count": 5},
        {"max_in_count": 10, "distinct_ratio": 1.0},
    )
    assert profile is not None
    assert profile.name == "is_in"
    assert profile.column == "status"
    assert set(profile.parameters["in"]) == {1, 2, 3}


def test_is_in_distinct_count_exceeds_max_in_count_returns_none():
    # 11 distinct values, max_in_count=10 → distinct_count > max_in_count → None
    df = _make_mock_df(["col"], list(range(11)))
    profile = make_is_in_profile(
        df,
        "col",
        T.IntegerType(),
        {"total_count": 100},
        {"max_in_count": 10, "distinct_ratio": 1.0},
    )
    assert profile is None


def test_is_in_distinct_ratio_exceeds_threshold_returns_none():
    # 10 distinct values in 10 total → ratio=1.0, threshold=0.5
    df = _make_mock_df(["col"], list(range(10)))
    profile = make_is_in_profile(
        df,
        "col",
        T.StringType(),
        {"total_count": 10},
        {"max_in_count": 20, "distinct_ratio": 0.5},
    )
    assert profile is None


def test_is_in_filter_propagated():
    df = _make_mock_df(["col"], ["a", "b"])
    profile = make_is_in_profile(
        df,
        "col",
        T.StringType(),
        {"total_count": 5},
        {"max_in_count": 10, "distinct_ratio": 1.0, "filter": "x > 0"},
    )
    assert profile is not None
    assert profile.filter == "x > 0"


# ---------------------------------------------------------------------------
# make_min_max_profile
# ---------------------------------------------------------------------------


def test_min_max_count_non_null_zero_returns_none(mock_df):
    assert make_min_max_profile(mock_df, "col", T.IntegerType(), {"count_non_null": 0}, {}) is None


@pytest.mark.parametrize("column_type", [T.StringType(), T.BooleanType(), T.ByteType()])
def test_min_max_unsupported_type_returns_none(mock_df, column_type):
    assert make_min_max_profile(mock_df, "col", column_type, {"count_non_null": 5}, {"remove_outliers": False}) is None


def test_min_max_without_outlier_removal_uses_metrics(mock_df):
    profile = make_min_max_profile(
        mock_df,
        "amount",
        T.IntegerType(),
        {"count_non_null": 5, "min": 1, "max": 100},
        {"remove_outliers": False},
    )
    assert profile is not None
    assert profile.name == "min_max"
    assert profile.column == "amount"
    assert profile.parameters == {"min": 1, "max": 100}
    assert profile.description == "Real min/max values were used"


def test_min_max_without_outlier_removal_double_type(mock_df):
    profile = make_min_max_profile(
        mock_df,
        "score",
        T.DoubleType(),
        {"count_non_null": 10, "min": 0.5, "max": 9.9},
        {"remove_outliers": False},
    )
    assert profile is not None
    assert profile.parameters == {"min": 0.5, "max": 9.9}


def test_min_max_filter_propagated(mock_df):
    profile = make_min_max_profile(
        mock_df,
        "col",
        T.IntegerType(),
        {"count_non_null": 5, "min": 1, "max": 10},
        {"remove_outliers": False, "filter": "x > 0"},
    )
    assert profile is not None
    assert profile.filter == "x > 0"


@pytest.mark.parametrize(
    "column_type",
    [T.IntegerType(), T.LongType(), T.DoubleType(), T.FloatType(), T.DecimalType(10, 2), T.ShortType()],
)
def test_min_max_supported_numeric_types_return_profile(mock_df, column_type):
    profile = make_min_max_profile(
        mock_df,
        "col",
        column_type,
        {"count_non_null": 5, "min": 1, "max": 10},
        {"remove_outliers": False},
    )
    assert profile is not None
    assert profile.name == "min_max"


def test_min_max_with_outlier_removal_stddev_zero_returns_real_min_max(mock_df):
    # stddev=0 means all values are identical; sigma bounds collapse to mean.
    # None of the sigma-capping branches fire, so real min/max are used.
    profile = make_min_max_profile(
        mock_df,
        "amount",
        T.IntegerType(),
        {"count_non_null": 10, "min": 5, "max": 5, "mean": 5.0, "stddev": 0.0},
        {"remove_outliers": True, "outlier_columns": ["amount"]},
    )
    assert profile is not None
    assert profile.name == "min_max"
    assert profile.parameters == {"min": 5, "max": 5}
    assert profile.description == "Real min/max values were used"
