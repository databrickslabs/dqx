# pylint: disable=import-private-name
import datetime
import decimal
import math

import pyspark.sql.types as T
import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.profiler.profile_builder import (
    PROFILE_BUILDER_REGISTRY,
    _is_text,
    _make_null_or_empty_profile,
    _make_null_profile,
    _supports_distinct,
    _supports_min_max,
    _remove_outliers,
    _get_min_max_limits,
    _adjust_min_max_limits,
    _round_value,
    _round_datetime,
    _round_float,
    _round_decimal,
)


# --- Registry tests ---


def test_registry_has_expected_builders():
    assert "null_or_empty" in PROFILE_BUILDER_REGISTRY
    assert "is_in" in PROFILE_BUILDER_REGISTRY
    assert "min_max" in PROFILE_BUILDER_REGISTRY


# --- Type-check helpers ---


@pytest.mark.parametrize(
    "col_type,expected",
    [
        (T.StringType(), True),
        (T.CharType(10), True),
        (T.VarcharType(50), True),
        (T.IntegerType(), False),
        (T.DateType(), False),
    ],
)
def test_is_text(col_type, expected):
    assert _is_text(col_type) is expected


@pytest.mark.parametrize(
    "col_type,expected",
    [
        (T.IntegerType(), True),
        (T.LongType(), True),
        (T.StringType(), True),
        (T.DoubleType(), False),
        (T.DateType(), False),
    ],
)
def test_supports_distinct(col_type, expected):
    assert _supports_distinct(col_type) is expected


@pytest.mark.parametrize(
    "col_type,expected",
    [
        (T.DateType(), True),
        (T.TimestampType(), True),
        (T.TimestampNTZType(), True),
        (T.IntegerType(), True),
        (T.DoubleType(), True),
        (T.StringType(), False),
    ],
)
def test_supports_min_max(col_type, expected):
    assert _supports_min_max(col_type) is expected


# --- Outlier removal option ---


def test_remove_outliers_disabled():
    assert _remove_outliers("col", {"remove_outliers": False}) is False


def test_remove_outliers_enabled_no_columns():
    assert _remove_outliers("col", {"remove_outliers": True, "outlier_columns": []}) is True


def test_remove_outliers_enabled_column_in_list():
    assert _remove_outliers("col", {"remove_outliers": True, "outlier_columns": ["col"]}) is True


def test_remove_outliers_enabled_column_not_in_list():
    assert _remove_outliers("col", {"remove_outliers": True, "outlier_columns": ["other"]}) is False


# --- Null or empty profile ---


def test_make_null_or_empty_profile_zero_total():
    result = _make_null_or_empty_profile("c", {"total_count": 0}, {})
    assert result is None


def test_make_null_or_empty_profile_not_null_and_not_empty():
    metrics = {"total_count": 100, "null_count": 0, "empty_count": 0}
    opts = {"max_null_ratio": 0.01, "max_empty_ratio": 0.01, "trim_strings": True}
    result = _make_null_or_empty_profile("c", metrics, opts)
    assert result is not None
    assert result.name == "is_not_null_or_empty"
    assert result.description is None  # no nulls or empties -> no description


def test_make_null_or_empty_profile_with_nulls_below_threshold():
    metrics = {"total_count": 1000, "null_count": 5, "empty_count": 0}
    opts = {"max_null_ratio": 0.01, "max_empty_ratio": 0.01, "trim_strings": True}
    result = _make_null_or_empty_profile("c", metrics, opts)
    assert result is not None
    assert result.name == "is_not_null_or_empty"
    assert result.description is not None


def test_make_null_or_empty_profile_null_only():
    metrics = {"total_count": 100, "null_count": 0, "empty_count": 50}
    opts = {"max_null_ratio": 0.01, "max_empty_ratio": 0.0}
    result = _make_null_or_empty_profile("c", metrics, opts)
    assert result is not None
    assert result.name == "is_not_null"


def test_make_null_or_empty_profile_empty_only():
    metrics = {"total_count": 100, "null_count": 50, "empty_count": 0}
    opts = {"max_null_ratio": 0.0, "max_empty_ratio": 0.01}
    result = _make_null_or_empty_profile("c", metrics, opts)
    assert result is not None
    assert result.name == "is_not_empty"


def test_make_null_or_empty_profile_both_exceed():
    metrics = {"total_count": 100, "null_count": 50, "empty_count": 50}
    opts = {"max_null_ratio": 0.0, "max_empty_ratio": 0.0}
    result = _make_null_or_empty_profile("c", metrics, opts)
    assert result is None


# --- Null profile ---


def test_make_null_profile_zero_total():
    result = _make_null_profile("c", {"total_count": 0}, {})
    assert result is None


def test_make_null_profile_below_threshold():
    metrics = {"total_count": 100, "null_count": 0}
    opts = {"max_null_ratio": 0.01}
    result = _make_null_profile("c", metrics, opts)
    assert result is not None
    assert result.name == "is_not_null"
    assert result.description is None  # null_count == 0


def test_make_null_profile_with_some_nulls():
    metrics = {"total_count": 1000, "null_count": 5}
    opts = {"max_null_ratio": 0.01}
    result = _make_null_profile("c", metrics, opts)
    assert result is not None
    assert result.description is not None


def test_make_null_profile_above_threshold():
    metrics = {"total_count": 100, "null_count": 50}
    opts = {"max_null_ratio": 0.01}
    result = _make_null_profile("c", metrics, opts)
    assert result is None


# --- Min/max limits ---


def test_get_min_max_limits_no_stddev():
    aggregates = {"min_value": 0, "max_value": 100, "mean_value": None, "stddev_value": None}
    min_l, max_l, desc = _get_min_max_limits(T.IntegerType(), {}, aggregates)
    assert min_l == 0
    assert max_l == 100
    assert desc == "Real min/max values were used"


def test_get_min_max_limits_sigma_within_range():
    aggregates = {"min_value": 0, "max_value": 100, "mean_value": 50, "stddev_value": 100}
    min_l, max_l, desc = _get_min_max_limits(T.IntegerType(), {"num_sigmas": 3}, aggregates)
    assert min_l == 0
    assert max_l == 100
    assert desc == "Real min/max values were used"


def test_get_min_max_limits_sigma_caps_both():
    aggregates = {"min_value": 0, "max_value": 100, "mean_value": 50, "stddev_value": 10}
    min_l, max_l, desc = _get_min_max_limits(T.IntegerType(), {"num_sigmas": 3}, aggregates)
    assert min_l == 20
    assert max_l == 80
    assert "outliers" in desc


# --- Adjust min/max ---


def test_adjust_min_max_limits_date():
    min_v, max_v = _adjust_min_max_limits(T.DateType(), 0, 86400, {})
    assert isinstance(min_v, datetime.date)
    assert isinstance(max_v, datetime.date)


def test_adjust_min_max_limits_timestamp():
    min_v, max_v = _adjust_min_max_limits(T.TimestampType(), 0, 86400, {"round": False})
    assert isinstance(min_v, datetime.datetime)
    assert isinstance(max_v, datetime.datetime)


def test_adjust_min_max_limits_integer():
    min_v, max_v = _adjust_min_max_limits(T.IntegerType(), 1.5, 9.5, {"round": True})
    assert isinstance(min_v, int)
    assert isinstance(max_v, int)


# --- Rounding ---


def test_round_value_disabled():
    assert _round_value(3.7, "up", {"round": False}) == 3.7


def test_round_value_none():
    assert _round_value(None, "up", {"round": True}) is None


def test_round_float_up():
    assert _round_float(3.1, "up") == math.ceil(3.1)


def test_round_float_down():
    assert _round_float(3.9, "down") == math.floor(3.9)


def test_round_float_invalid_direction():
    assert _round_float(3.5, "sideways") == 3.5


def test_round_decimal_up():
    result = _round_decimal(decimal.Decimal("3.1"), "up")
    assert result == decimal.Decimal("4")


def test_round_decimal_down():
    result = _round_decimal(decimal.Decimal("3.9"), "down")
    assert result == decimal.Decimal("3")


def test_round_datetime_down():
    date_time = datetime.datetime(2023, 6, 15, 14, 30, 0)
    result = _round_datetime(date_time, "down")
    assert result == datetime.datetime(2023, 6, 15, 0, 0, 0)


def test_round_datetime_up():
    date_time = datetime.datetime(2023, 6, 15, 14, 30, 0)
    result = _round_datetime(date_time, "up")
    assert result == datetime.datetime(2023, 6, 16, 0, 0, 0)


def test_round_datetime_up_already_midnight():
    date_time = datetime.datetime(2023, 6, 15, 0, 0, 0)
    result = _round_datetime(date_time, "up")
    assert result == date_time


def test_round_datetime_invalid_direction():
    date_time = datetime.datetime(2023, 6, 15, 14, 30, 0)
    with pytest.raises(InvalidParameterError, match="Invalid rounding direction"):
        _round_datetime(date_time, "sideways")
