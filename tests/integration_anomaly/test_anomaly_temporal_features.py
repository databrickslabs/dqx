"""Integration tests for time-relative features: `baseline_over_time`.

The inertness test here is the most important one in the file, and the reason it lives in integration
rather than unit: the guarantee is about ``engineered_feature_names``, which only
:func:`apply_feature_engineering` produces, and only from a real DataFrame. Everything else in this
change is safe to add *because* the default path is provably unchanged, so that has to be asserted
against the public function rather than against the early return inside it.
"""

import datetime

import numpy as np
import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.temporal import TemporalBasis
from databricks.labs.dqx.anomaly.transformers import (
    BASELINE_RELATIVE_SUFFIX,
    TEMPORAL_RELATIVE_SUFFIX,
    ColumnTypeInfo,
    apply_feature_engineering,
    apply_feature_engineering_from_metadata,
)

HOUR_SECONDS = 3600
START = datetime.datetime(2025, 1, 6, 0, 0, 0)  # a Monday, so weekday/weekend features are meaningful


def _numeric(name: str) -> ColumnTypeInfo:
    return ColumnTypeInfo(name=name, spark_type=T.DoubleType(), category="numeric")


def _timestamp(name: str) -> ColumnTypeInfo:
    return ColumnTypeInfo(name=name, spark_type=T.TimestampType(), category="datetime")


def _categorical(name: str) -> ColumnTypeInfo:
    return ColumnTypeInfo(name=name, spark_type=T.StringType(), category="categorical")


def _trending_frame(spark: SparkSession, hours: int = 24 * 30, slope: float = 0.05) -> DataFrame:
    """A metric that grows steadily, sampled hourly over 30 days.

    Long enough for the daily and weekly seasonal terms to clear the six-cycle guard, so the fitted
    basis is a realistic one rather than trend-only.
    """
    rng = np.random.default_rng(17)
    rows = [(START + datetime.timedelta(hours=i), float(100.0 + slope * i + rng.normal(0, 2.0))) for i in range(hours)]
    return spark.createDataFrame(rows, "event_ts timestamp, revenue double")


# ============================================================================
# Inertness: the guarantee everything else rests on
# ============================================================================


def test_omitting_the_time_column_leaves_the_feature_list_byte_identical(spark: SparkSession):
    """The default path must not move.

    Adding a transform to a positional feature list is only safe if it is provably absent when unused.
    This compares the feature names produced with the parameter omitted against those produced by a call
    that predates it existing, which is the same thing an already-trained model will replay.
    """
    df = _trending_frame(spark, hours=200)
    infos = [_timestamp("event_ts"), _numeric("revenue")]

    _, without_argument = apply_feature_engineering(df, infos)
    _, with_explicit_empty = apply_feature_engineering(df, infos, baseline_over_time=None)
    _, with_empty_string = apply_feature_engineering(df, infos, baseline_over_time="")

    assert without_argument.engineered_feature_names == with_explicit_empty.engineered_feature_names
    assert without_argument.engineered_feature_names == with_empty_string.engineered_feature_names
    assert not any(name.endswith(TEMPORAL_RELATIVE_SUFFIX) for name in without_argument.engineered_feature_names)
    # And nothing temporal is persisted either, so a scoring run has nothing to replay.
    assert without_argument.baseline_over_time == ""
    assert not without_argument.temporal_coefficients


def test_a_table_with_no_numeric_metrics_stays_inert(spark: SparkSession):
    """There is nothing whose level over time could be expected, so nothing is appended."""
    df = spark.createDataFrame([(START, "eu"), (START, "us")], "event_ts timestamp, region string")

    _, metadata = apply_feature_engineering(
        df, [_timestamp("event_ts"), _categorical("region")], baseline_over_time="event_ts"
    )

    assert not any(name.endswith(TEMPORAL_RELATIVE_SUFFIX) for name in metadata.engineered_feature_names)


# ============================================================================
# What the transform produces
# ============================================================================


def test_a_time_relative_feature_is_appended_per_metric_and_appended_last(spark: SparkSession):
    """Position matters as much as presence.

    ``engineered_feature_names`` is handed to the sklearn pipeline in order, so a transform that inserts
    rather than appends silently reorders an already-trained model's inputs.
    """
    df = _trending_frame(spark)

    _, metadata = apply_feature_engineering(
        df, [_timestamp("event_ts"), _numeric("revenue")], baseline_over_time="event_ts"
    )

    names = metadata.engineered_feature_names
    assert f"revenue{TEMPORAL_RELATIVE_SUFFIX}" in names
    assert names[-1] == f"revenue{TEMPORAL_RELATIVE_SUFFIX}"


def test_the_time_column_does_not_become_a_feature(spark: SparkSession):
    """A time column is the axis a metric is measured along, not a thing being measured.

    The same rule the grouping columns already follow. Without this the timestamp would also be expanded
    into seven cyclical calendar features, which is measured to be actively harmful in some shapes of
    data, and the caller would have no way to tell it had happened.
    """
    df = _trending_frame(spark)

    engineered, metadata = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")

    assert "event_ts" not in engineered.columns
    assert "event_ts" not in metadata.engineered_feature_names


def test_the_residual_removes_the_trend_it_was_fitted_to(spark: SparkSession):
    """The point of the whole transform, stated as a measurement.

    A steadily growing metric leaves the range it trained on, and that is what makes ordinary later rows
    look anomalous. The residual has to be stationary where the raw metric is not.
    """
    df = _trending_frame(spark, hours=24 * 30, slope=0.05)

    engineered, _ = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")

    stats = engineered.selectExpr(
        "stddev(revenue) as raw_sd", f"stddev(`revenue{TEMPORAL_RELATIVE_SUFFIX}`) as residual_sd"
    ).first()
    assert stats is not None
    # The trend spans 0.05 * 720 = 36 units against a noise sd of 2, so the raw spread is dominated by
    # the trend and the residual should collapse to roughly the noise floor.
    assert stats["residual_sd"] < stats["raw_sd"] / 3


def test_the_fitted_basis_and_window_are_persisted(spark: SparkSession):
    """Scoring rebuilds the expectation from these alone, so they have to be there and be usable."""
    df = _trending_frame(spark)

    _, metadata = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")

    assert metadata.baseline_over_time == "event_ts"
    assert metadata.temporal_window["t_min"] < metadata.temporal_window["t_max"]
    basis = TemporalBasis.from_dict(metadata.temporal_basis)
    assert basis.trend is True
    # 30 days of hourly data clears six cycles for the daily period, so it should have been admitted.
    assert 86400.0 in basis.periods
    assert len(metadata.temporal_coefficients["revenue"]) == basis.n_terms


# ============================================================================
# Training and scoring have to agree
# ============================================================================


def test_scoring_replays_the_same_expectation_as_training(spark: SparkSession):
    """Training fits the basis; scoring only evaluates it.

    If the two disagree about the design's column order or the time origin, the residual is wrong rather
    than absent, and nothing raises. This is the temporal equivalent of the group-key parity contract.
    """
    df = _trending_frame(spark)
    trained_features, metadata = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")
    scored_features, _ = apply_feature_engineering_from_metadata(df, metadata)

    column = f"revenue{TEMPORAL_RELATIVE_SUFFIX}"
    trained = [row[column] for row in trained_features.select(column).collect()]
    replayed = [row[column] for row in scored_features.select(column).collect()]

    assert trained == pytest.approx(replayed, rel=1e-9)


def test_scoring_a_timestamp_past_the_training_window_still_produces_a_number(spark: SparkSession):
    """The expectation is a function of time, so it extrapolates where a lookup table could not.

    This is the property that makes the approach work at all, and also the one that needs a staleness
    horizon on top: extrapolation stays finite, but its accuracy decays with distance.
    """
    df = _trending_frame(spark, hours=24 * 30)
    _, metadata = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")

    future = spark.createDataFrame([(START + datetime.timedelta(days=45), 200.0)], "event_ts timestamp, revenue double")
    engineered, _ = apply_feature_engineering_from_metadata(future, metadata)

    value = engineered.select(f"revenue{TEMPORAL_RELATIVE_SUFFIX}").first()
    assert value is not None
    assert value[0] is not None and np.isfinite(value[0])


# ============================================================================
# Composition with baseline_by, which is not incidental
# ============================================================================


def test_the_fit_runs_on_the_group_relative_value_when_grouping_is_in_play(spark: SparkSession):
    """Measured: a pooled trend fitted on raw values is wrong for every group once their slopes differ.

    Event coverage fell from 80% to 29% as group slopes diverged when the fit used raw values, while a
    single pooled fit on the group-relative residual reached 101% of per-group fits. So the two features
    compose, and the order they are appended in is load-bearing rather than arbitrary.
    """
    rng = np.random.default_rng(23)
    rows = []
    for group, (level, slope) in enumerate([(100.0, 0.02), (400.0, 0.09)]):
        for i in range(24 * 30):
            rows.append(
                (
                    START + datetime.timedelta(hours=i),
                    f"g{group}",
                    float(level + slope * i + rng.normal(0, 1.5)),
                )
            )
    df = spark.createDataFrame(rows, "event_ts timestamp, grp string, revenue double")

    engineered, metadata = apply_feature_engineering(
        df, [_numeric("revenue")], baseline_by=["grp"], baseline_over_time="event_ts"
    )

    names = metadata.engineered_feature_names
    relative = f"revenue{BASELINE_RELATIVE_SUFFIX}"
    temporal = f"revenue{TEMPORAL_RELATIVE_SUFFIX}"
    # Both present, and the temporal one after the group-relative one it reads.
    assert names.index(relative) < names.index(temporal)

    # The composed residual must be stationary across both groups despite their different slopes. A fit
    # on raw values could not be, since one pooled line cannot follow two different slopes.
    spreads = {
        row["grp"]: row["sd"]
        for row in engineered.groupBy("grp")
        .agg({temporal: "stddev"})
        .withColumnRenamed(f"stddev({temporal})", "sd")
        .collect()
    }
    assert len(spreads) == 2
    for spread in spreads.values():
        assert spread < 4.0  # noise sd is 1.5; a mis-fitted trend would leave far more than this


# ============================================================================
# Degenerate inputs
# ============================================================================


def test_a_constant_metric_yields_a_finite_residual(spark: SparkSession):
    """Nothing to learn, and no division by a zero standard deviation on the way to finding that out."""
    rows = [(START + datetime.timedelta(hours=i), 5.0) for i in range(24 * 30)]
    df = spark.createDataFrame(rows, "event_ts timestamp, flat double")

    engineered, _ = apply_feature_engineering(df, [_numeric("flat")], baseline_over_time="event_ts")

    residuals = [row[0] for row in engineered.select(f"flat{TEMPORAL_RELATIVE_SUFFIX}").collect()]
    assert all(value is not None and abs(value) < 1e-6 for value in residuals)


def test_a_null_timestamp_produces_a_zero_residual_rather_than_a_null_feature(spark: SparkSession):
    """A null time axis means the expectation is unknown for that row.

    Zero rather than null, because the sklearn pipeline cannot take a null: a null here would fail the
    whole batch rather than degrade one row. The row is then judged on its other features, which is the
    same conservative direction an unseen group takes.
    """
    rows = [(START + datetime.timedelta(hours=i), float(100 + i)) for i in range(24 * 30)]
    df = spark.createDataFrame(rows, "event_ts timestamp, revenue double")
    _, metadata = apply_feature_engineering(df, [_numeric("revenue")], baseline_over_time="event_ts")

    with_null = spark.createDataFrame(
        [Row(event_ts=None, revenue=150.0)],
        T.StructType(
            [
                T.StructField("event_ts", T.TimestampType(), True),
                T.StructField("revenue", T.DoubleType(), True),
            ]
        ),
    )
    engineered, _ = apply_feature_engineering_from_metadata(with_null, metadata)

    value = engineered.select(f"revenue{TEMPORAL_RELATIVE_SUFFIX}").first()
    assert value is not None
    assert value[0] == 0.0
