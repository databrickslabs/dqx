import pytest
import pyspark.sql.types as T

from databricks.labs.dqx.profiler.profiler_column_metrics import (
    PROFILE_COLUMN_METRIC_REGISTRY,
    count_distinct,
    count_non_null,
    empty_count,
    register_profile_column_metric,
)


def test_register_profile_column_metric_registers_under_explicit_type():
    @register_profile_column_metric("custom_metric_key")
    def _test_metric(_field, _column_label):
        return None

    try:
        assert "custom_metric_key" in PROFILE_COLUMN_METRIC_REGISTRY
        assert PROFILE_COLUMN_METRIC_REGISTRY["custom_metric_key"] is _test_metric
    finally:
        PROFILE_COLUMN_METRIC_REGISTRY.pop("custom_metric_key", None)


@pytest.mark.parametrize("column_type", [T.StringType(), T.CharType(10), T.VarcharType(50)])
def test_empty_count_returns_column_for_text_types(column_type):
    field = T.StructField("col", column_type)
    assert empty_count(field, "col") is not None


@pytest.mark.parametrize("column_type", [T.IntegerType(), T.DoubleType(), T.LongType(), T.DateType()])
def test_empty_count_returns_literal_zero_for_non_text_types(column_type):
    field = T.StructField("col", column_type)
    assert empty_count(field, "col") is not None


@pytest.mark.parametrize("column_type", [T.StringType(), T.IntegerType(), T.DoubleType(), T.DateType()])
def test_count_distinct_returns_column_for_any_type(column_type):
    field = T.StructField("col", column_type)
    assert count_distinct(field, "col") is not None


@pytest.mark.parametrize("column_type", [T.StringType(), T.IntegerType(), T.DoubleType(), T.DateType()])
def test_count_non_null_returns_column_for_any_type(column_type):
    field = T.StructField("col", column_type)
    assert count_non_null(field, "col") is not None
