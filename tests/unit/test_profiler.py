import pyspark.sql.types as T
import pytest

from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.errors import MissingParameterError
from databricks.labs.dqx.profiler.profiler import DQProfiler
import databricks.labs.dqx.profiler.profiler as profiler_module


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
        profiler.detect_primary_keys_with_llm(table="dummy")
