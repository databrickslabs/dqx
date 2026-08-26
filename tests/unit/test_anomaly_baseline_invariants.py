"""Unit pins for two invariants group conditioning rests on (no Spark).

- The group-column *type* contract: only types whose string rendering agrees between Spark and
  Python may be a baseline column, because the group key is built in both places and a divergence
  silently misses every lookup (see test_anomaly_group_key for the key itself, and
  test_anomaly_group_relative_features for the live Python/Spark parity).
- The feature-engineering contract: every scorer must run features through the shared feature_prep
  entry points before scoring, or a model would score on a different feature list than it trained on.
"""

import inspect
from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly import ensemble_scorer, single_model_scorer
from databricks.labs.dqx.anomaly.validation import validate_baseline_columns
from databricks.labs.dqx.errors import InvalidParameterError


def _fake_df(schema: dict[str, T.DataType]) -> DataFrame:
    """A DataFrame stand-in exposing only the schema/columns validate_baseline_columns reads.

    Uses create_autospec rather than a real session: the function under test only inspects
    ``df.schema.fields`` and ``df.columns``, so no Spark is needed to exercise the type contract.
    """
    df = create_autospec(DataFrame, instance=True)
    df.schema = T.StructType([T.StructField(name, dtype, True) for name, dtype in schema.items()])
    df.columns = list(schema)
    return df


@pytest.mark.parametrize(
    "dtype",
    [
        T.StringType(),
        T.ByteType(),
        T.ShortType(),
        T.IntegerType(),
        T.LongType(),
        T.BooleanType(),
        T.DateType(),
    ],
)
def test_validate_baseline_columns_accepts_types_that_render_identically(dtype: T.DataType):
    """String, integral, boolean and date render the same in Spark and Python, so they are allowed."""
    validate_baseline_columns(_fake_df({"g": dtype}), ["g"], [])  # must not raise


@pytest.mark.parametrize("dtype", [T.FloatType(), T.DoubleType(), T.DecimalType(10, 2)])
def test_validate_baseline_columns_rejects_types_spark_and_python_format_differently(dtype: T.DataType):
    """Floating-point and decimal render differently between Spark and Python, so a group key built
    at training would not match the one built at scoring. They must be rejected up front."""
    with pytest.raises(InvalidParameterError, match="unsupported types"):
        validate_baseline_columns(_fake_df({"g": dtype}), ["g"], [])


def test_every_scorer_applies_feature_engineering_before_scoring():
    """Structural guard: every scoring module must route features through the shared feature_prep
    entry points (which call apply_feature_engineering_from_metadata) before scoring.

    A scorer that fed raw columns to the model would score on a different feature list than the
    model trained on — the shapes can still line up, so it is a silent correctness bug, not a crash.
    If you add a scoring module, add it to this list and make it apply feature engineering.
    """
    fe_entry_points = (
        "apply_feature_engineering_for_scoring",
        "apply_feature_engineering_with_row_passthrough",
        "apply_feature_engineering_from_metadata",
    )
    for module in (single_model_scorer, ensemble_scorer):
        source = inspect.getsource(module)
        assert any(
            entry in source for entry in fe_entry_points
        ), f"{module.__name__} must apply feature engineering before scoring"
