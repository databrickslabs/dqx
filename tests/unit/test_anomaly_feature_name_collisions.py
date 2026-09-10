"""Unit pins for the refusal of a generated feature name that lands on a real column (no Spark).

Feature engineering builds derived columns by appending a suffix and writing the result with
``withColumn``, which *replaces* any column already carrying that name. A table with both ``amount`` and
``amount_rel_baseline`` therefore lost the second one silently: the derived value overwrote it, the name
was appended to the positional feature list a second time, and the model was fitted on two copies of the
derived value with the real metric gone. Attribution then named the wrong source column.

Nothing downstream can notice that, which is why the refusal is up front and why it is worth pinning per
transform family. The suffixes live in ``transformers`` as constants for the same reason these tests
import them rather than spelling them out: a suffix that drifted between the transform and the validator
would reopen the hole while every test still passed.
"""

from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import (
    BASELINE_RELATIVE_SUFFIX,
    BOOLEAN_FEATURE_SUFFIX,
    CALENDAR_FEATURE_SUFFIXES,
    FREQUENCY_FEATURE_SUFFIX,
    NULL_INDICATOR_SUFFIX,
    TEMPORAL_RELATIVE_SUFFIX,
)
from databricks.labs.dqx.anomaly.validation import generated_feature_names, validate_generated_feature_names
from databricks.labs.dqx.errors import InvalidParameterError


def _fake_df(schema: dict[str, T.DataType]) -> DataFrame:
    """A DataFrame stand-in exposing only the schema and columns the validator reads."""
    df = create_autospec(DataFrame, instance=True)
    df.schema = T.StructType([T.StructField(name, dtype, True) for name, dtype in schema.items()])
    df.columns = list(schema)
    return df


def test_an_ordinary_schema_passes():
    df = _fake_df({"amount": T.DoubleType(), "region": T.StringType(), "created": T.TimestampType()})

    validate_generated_feature_names(df, ["amount", "region"], ["region"], "created")  # must not raise


def test_a_column_named_after_the_group_relative_feature_is_refused():
    """The review's own example, and the one that loses a real metric."""
    df = _fake_df(
        {
            "amount": T.DoubleType(),
            f"amount{BASELINE_RELATIVE_SUFFIX}": T.DoubleType(),
            "region": T.StringType(),
        }
    )

    with pytest.raises(InvalidParameterError) as raised:
        validate_generated_feature_names(df, ["amount", f"amount{BASELINE_RELATIVE_SUFFIX}"], ["region"], None)

    message = str(raised.value)
    assert f"amount{BASELINE_RELATIVE_SUFFIX}" in message
    assert "overwritten" in message


def test_the_same_column_is_accepted_when_no_grouping_makes_the_feature():
    """The refusal is about what *this* configuration builds, not about the name in the abstract.

    Without ``baseline_by`` there is no group-relative transform, so ``amount_rel_baseline`` is just an
    ordinary numeric column and rejecting it would be a false alarm.
    """
    df = _fake_df({"amount": T.DoubleType(), f"amount{BASELINE_RELATIVE_SUFFIX}": T.DoubleType()})

    validate_generated_feature_names(df, ["amount", f"amount{BASELINE_RELATIVE_SUFFIX}"], None, None)


def test_a_column_named_after_the_time_relative_feature_is_refused():
    df = _fake_df(
        {
            "latency": T.DoubleType(),
            f"latency{TEMPORAL_RELATIVE_SUFFIX}": T.DoubleType(),
            "reading_ts": T.TimestampType(),
        }
    )

    with pytest.raises(InvalidParameterError):
        validate_generated_feature_names(df, ["latency", f"latency{TEMPORAL_RELATIVE_SUFFIX}"], None, "reading_ts")


@pytest.mark.parametrize("suffix", CALENDAR_FEATURE_SUFFIXES)
def test_a_column_named_after_any_calendar_feature_is_refused(suffix: str):
    df = _fake_df({"created": T.TimestampType(), f"created{suffix}": T.DoubleType()})

    with pytest.raises(InvalidParameterError) as raised:
        validate_generated_feature_names(df, ["created", f"created{suffix}"], None, None)

    assert f"created{suffix}" in str(raised.value)


def test_a_column_named_after_the_boolean_feature_is_refused():
    df = _fake_df({"is_vip": T.BooleanType(), f"is_vip{BOOLEAN_FEATURE_SUFFIX}": T.DoubleType()})

    with pytest.raises(InvalidParameterError):
        validate_generated_feature_names(df, ["is_vip", f"is_vip{BOOLEAN_FEATURE_SUFFIX}"], None, None)


def test_a_column_named_after_the_frequency_feature_is_refused():
    df = _fake_df({"sku": T.StringType(), f"sku{FREQUENCY_FEATURE_SUFFIX}": T.DoubleType()})

    with pytest.raises(InvalidParameterError):
        validate_generated_feature_names(df, ["sku", f"sku{FREQUENCY_FEATURE_SUFFIX}"], None, None)


def test_a_null_indicator_collision_is_refused_even_with_no_nulls_present():
    """Claimed unconditionally, and deliberately so.

    Whether an indicator is built depends on the training frame having nulls, which is a property of
    today's data rather than of the schema. A schema whose indicator name collides is a bug waiting for
    the first null to arrive, and it would then appear as a silent overwrite on a later run rather than
    as an error on this one.
    """
    df = _fake_df({"amount": T.DoubleType(), f"amount{NULL_INDICATOR_SUFFIX}": T.DoubleType()})

    with pytest.raises(InvalidParameterError):
        validate_generated_feature_names(df, ["amount", f"amount{NULL_INDICATOR_SUFFIX}"], None, None)


def test_the_collector_keeps_every_source_rather_than_one_per_name():
    """Why the collector returns a list rather than a name-keyed mapping.

    A dict keeps one entry per name, so the duplicate check below could never see a clash at all. This
    is the shape that makes it representable.
    """
    generated = generated_feature_names({"x": T.DoubleType()}, ["x", "x"], None, None)

    assert [name for name, _, _ in generated] == [f"x{NULL_INDICATOR_SUFFIX}", f"x{NULL_INDICATOR_SUFFIX}"]


def test_a_column_listed_twice_is_refused():
    """Reachable: nothing upstream deduplicates *columns*, so a caller repeating one would otherwise get
    that column's features appended twice into a positional list."""
    df = _fake_df({"x": T.DoubleType()})

    with pytest.raises(InvalidParameterError) as raised:
        validate_generated_feature_names(df, ["x", "x"], None, None)

    assert "same feature name" in str(raised.value)


def test_a_feature_column_missing_from_the_schema_is_left_to_the_column_validator():
    """Not this validator's complaint. Reporting it here would give two errors for one mistake, and the
    less useful one first."""
    df = _fake_df({"amount": T.DoubleType()})

    validate_generated_feature_names(df, ["amount", "nonexistent"], None, None)
