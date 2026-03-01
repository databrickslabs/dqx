import types
from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.explainability import add_top_contributors_to_message
from databricks.labs.dqx.errors import InvalidParameterError


def test_add_top_contributors_requires_severity_column():
    """Missing severity_percentile/_dq_info should raise InvalidParameterError."""
    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(types.SimpleNamespace(columns=[]), threshold=60.0)


def test_add_top_contributors_uses_severity_percentile_column_path():
    """When severity_percentile is in df.columns, use it and call withColumn with _top_contributors."""
    df = create_autospec(DataFrame, instance=True)
    df.columns = ["severity_percentile", "anomaly_contributions"]
    result_df = create_autospec(DataFrame, instance=True)
    df.withColumn.return_value = result_df

    result = add_top_contributors_to_message(df, threshold=60.0, top_n=2)

    assert result is result_df
    df.withColumn.assert_called_once()
    _, call_expr = df.withColumn.call_args[0]
    assert call_expr is not None


def test_add_top_contributors_uses_dq_info_path_when_no_severity_column():
    """When _dq_info is in df.columns and severity_percentile is not, use _dq_info and call withColumn."""
    df = create_autospec(DataFrame, instance=True)
    df.columns = ["_dq_info", "anomaly_contributions"]
    result_df = create_autospec(DataFrame, instance=True)
    df.withColumn.return_value = result_df

    result = add_top_contributors_to_message(df, threshold=60.0, top_n=2)

    assert result is result_df
    df.withColumn.assert_called_once()
    _, call_expr = df.withColumn.call_args[0]
    assert call_expr is not None


def test_add_top_contributors_prefers_severity_percentile_over_dq_info():
    """When both severity_percentile and _dq_info exist, first branch (severity_percentile) is used."""
    df = create_autospec(DataFrame, instance=True)
    df.columns = ["severity_percentile", "_dq_info", "anomaly_contributions"]
    result_df = create_autospec(DataFrame, instance=True)
    df.withColumn.return_value = result_df

    result = add_top_contributors_to_message(df, threshold=60.0, top_n=2)

    assert result is result_df
    df.withColumn.assert_called_once()
    _, call_expr = df.withColumn.call_args[0]
    assert call_expr is not None


def test_add_top_contributors_raises_when_only_other_columns():
    """Having other columns but neither severity_percentile nor _dq_info raises."""
    df = create_autospec(DataFrame, instance=True)
    df.columns = ["anomaly_contributions", "other"]

    with pytest.raises(InvalidParameterError, match="severity_percentile is required"):
        add_top_contributors_to_message(df, threshold=60.0)
