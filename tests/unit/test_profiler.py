from unittest.mock import MagicMock, create_autospec, patch

import pyspark.sql.types as T
import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.errors import MissingParameterError, InvalidConfigError
from databricks.labs.dqx.profiler.profiler import DQProfiler
import databricks.labs.dqx.profiler.profiler as profiler_module
from databricks.labs.dqx.profiler.profile_builder import PROFILE_BUILDER_REGISTRY


def test_get_columns_or_fields():
    inp = T.StructType(
        [
            T.StructField("ts1", T.IntegerType()),
            T.StructField(
                "ss1",
                T.StructType(
                    [
                        T.StructField("ns1", T.TimestampType()),
                        T.StructField(
                            "s2",
                            T.StructType([T.StructField("ns2", T.StringType()), T.StructField("ns3", T.DateType())]),
                        ),
                    ]
                ),
            ),
        ]
    )
    fields = DQProfiler.get_columns_or_fields(inp.fields)
    expected = [
        T.StructField("ts1", T.IntegerType()),
        T.StructField("ss1.ns1", T.TimestampType()),
        T.StructField("ss1.s2.ns2", T.StringType()),
        T.StructField("ss1.s2.ns3", T.DateType()),
    ]
    assert fields == expected


@pytest.mark.parametrize(
    "input_config,expected_error",
    [
        (None, "Input config with location is required"),
        (InputConfig(""), "Input config with location is required"),
    ],
)
def test_profiler_profile_input_config_missing(input_config, expected_error, profiler):
    with pytest.raises(MissingParameterError, match=expected_error):
        profiler.profile_table(input_config=input_config)


def test_profiler_llm_disabled(profiler, monkeypatch):
    """Test error when llm is not installed."""
    monkeypatch.setattr(profiler_module, "LLM_ENABLED", False)

    # Attempt to generate rules should raise ImportError
    with pytest.raises(MissingParameterError, match="LLM engine not available"):
        profiler.detect_primary_keys_with_llm(input_config=InputConfig(location="fake"))


def test_input_location_missing_when_detecting_primary_keys(mock_workspace_client, mock_spark):
    input_config = InputConfig(location="")
    with pytest.raises(InvalidConfigError, match="Input location not configured"):
        profiler = DQProfiler(workspace_client=mock_workspace_client, spark=mock_spark)
        profiler.detect_primary_keys_with_llm(input_config=input_config)


def test_profile_builder_registry_has_expected_builders():
    assert "null_or_empty" in PROFILE_BUILDER_REGISTRY
    assert "is_in" in PROFILE_BUILDER_REGISTRY
    assert "min_max" in PROFILE_BUILDER_REGISTRY



def _mock_agg_df(agg_dict: dict) -> MagicMock:
    mock_row = MagicMock()
    mock_row.asDict.return_value = agg_dict
    mock_df = create_autospec(DataFrame)
    mock_df.agg.return_value.first.return_value = mock_row
    return mock_df


def test_build_column_metrics_aliases_are_applied(profiler):
    mock_df = _mock_agg_df({"count_non_null": 7, "count_distinct": 3, "empty_count": 0})
    field = T.StructField("col1", T.StringType())

    metrics = profiler._build_column_metrics(mock_df, "col1", field, {}, total_count=10)

    assert "count_non_null" in metrics
    assert "count_distinct" in metrics
    assert "empty_count" in metrics


def test_build_column_metrics_computes_count_null(profiler):
    mock_df = _mock_agg_df({"count_non_null": 7, "count_distinct": 3, "empty_count": 0})
    field = T.StructField("col1", T.StringType())

    metrics = profiler._build_column_metrics(mock_df, "col1", field, {}, total_count=10)

    assert metrics["count_null"] == 3


def test_build_column_metrics_merges_summary_stats(profiler):
    mock_df = _mock_agg_df({"count_non_null": 8})
    field = T.StructField("col1", T.IntegerType())
    summary_stats = {"col1": {"min": 0, "max": 100, "mean": 50.0}}

    metrics = profiler._build_column_metrics(mock_df, "col1", field, summary_stats, total_count=10)

    assert metrics["min"] == 0
    assert metrics["max"] == 100
    assert metrics["count_non_null"] == 8


def test_build_column_metrics_aggregation_overrides_summary_stats_on_conflict(profiler):
    mock_df = _mock_agg_df({"count_non_null": 9})
    field = T.StructField("col1", T.IntegerType())
    summary_stats = {"col1": {"count_non_null": 5}}

    metrics = profiler._build_column_metrics(mock_df, "col1", field, summary_stats, total_count=10)

    assert metrics["count_non_null"] == 9


def test_build_column_metrics_handles_empty_dataframe(profiler):
    mock_df = create_autospec(DataFrame)
    mock_df.agg.return_value.first.return_value = None
    field = T.StructField("col1", T.IntegerType())
    summary_stats = {"col1": {"count_non_null": 0}}

    metrics = profiler._build_column_metrics(mock_df, "col1", field, summary_stats, total_count=5)

    assert metrics["count_null"] == 5


def test_build_column_metrics_skips_none_returning_metric_functions(profiler):
    mock_df = create_autospec(DataFrame)
    mock_row = MagicMock()
    mock_row.asDict.return_value = {"count_non_null": 5}
    mock_df.agg.return_value.first.return_value = mock_row

    field = T.StructField("col1", T.IntegerType())
    registry = {
        "count_non_null": lambda f, c: MagicMock(),
        "numeric_only": lambda f, c: None,
    }

    with patch.dict(profiler_module.PROFILE_COLUMN_METRIC_REGISTRY, registry, clear=True):
        profiler._build_column_metrics(mock_df, "col1", field, {}, total_count=10)

    agg_args = mock_df.agg.call_args[0]
    assert len(agg_args) == 1
