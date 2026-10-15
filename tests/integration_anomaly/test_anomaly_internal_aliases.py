"""Private working columns must not replace or hide literal input fields."""

from collections import Counter
from datetime import datetime, timedelta

import numpy as np
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    apply_feature_engineering,
    apply_feature_engineering_from_metadata,
)


@pytest.fixture(name="alias_collision_frame")
def fixture_alias_collision_frame(spark: SparkSession) -> DataFrame:
    start = datetime(2025, 1, 1)
    rows = [
        (
            index,
            start + timedelta(hours=index),
            100.0 + float(np.sin(index * 0.31)),
            800.0 + float(index % 7),
            "north" if index % 2 else "south",
            *[float(index + offset) for offset in range(6)],
        )
        for index in range(120)
    ]
    rows.append(rows[10])
    # Occupy both first-choice and case-variant aliases to require real allocation,
    # rather than moving the collision to a different fixed working name.
    occupied = [
        "__dqx_time_bucket_0",
        "__DQX_TIME_BUCKET_1",
        "__dqx_bucket_seconds_0",
        "__DQX_BUCKET_SECONDS_1",
        "__dqx_baseline_0",
        "__DQX_BASELINE_1",
    ]
    schema = T.StructType(
        [
            T.StructField("row_id", T.IntegerType(), False),
            T.StructField("event_time", T.TimestampType(), False),
            T.StructField("amount", T.DoubleType(), False),
            T.StructField("secondary", T.DoubleType(), False),
            T.StructField("region", T.StringType(), False),
        ]
        + [T.StructField(name, T.DoubleType(), False) for name in occupied]
    )
    return spark.createDataFrame(rows, schema)


def _engineer(
    frame: DataFrame, features: list[str], *, groups: list[str] | None = None, time: str | None = None
) -> tuple[DataFrame, SparkFeatureMetadata]:
    infos = [ColumnTypeInfo(name=name, spark_type=T.DoubleType(), category="numeric") for name in features]
    return apply_feature_engineering(frame, infos, baseline_by=groups, baseline_over_time=time)


def _assert_equivalent(
    control: DataFrame,
    candidate: DataFrame,
    control_names: list[str],
    candidate_names: list[str],
) -> None:
    assert len(control_names) == len(candidate_names)
    rename = dict(zip(control_names, candidate_names, strict=True))
    assert candidate.columns == [rename.get(name, name) for name in control.columns]
    left_rows = control.collect()
    right_rows = candidate.collect()
    assert len(left_rows) == len(right_rows) == 121
    assert Counter(row.row_id for row in left_rows) == Counter(row.row_id for row in right_rows)
    left = sorted((row.row_id, *(row[name] for name in control_names)) for row in left_rows)
    right = sorted((row.row_id, *(row[name] for name in candidate_names)) for row in right_rows)
    np.testing.assert_allclose(left, right, rtol=1e-10, atol=1e-8)
    extra_names = [name for name in control.columns if name not in control_names]
    assert Counter(tuple(row[name] for name in extra_names) for row in left_rows) == Counter(
        tuple(row[name] for name in extra_names) for row in right_rows
    )


def _assert_replay(frame: DataFrame, trained: DataFrame, metadata: SparkFeatureMetadata) -> None:
    restored = SparkFeatureMetadata.from_json(metadata.to_json())
    replayed, replayed_metadata = apply_feature_engineering_from_metadata(frame.repartition(2), restored)
    assert replayed_metadata == restored
    _assert_equivalent(trained, replayed, metadata.engineered_feature_names, metadata.engineered_feature_names)


@pytest.mark.parametrize("metric", ["__dqx_time_bucket", "__dqx_bucket_seconds", "__DQX_BUCKET_SECONDS"])
def test_temporal_fit_preserves_metrics_named_like_working_columns(
    alias_collision_frame: DataFrame, metric: str
) -> None:
    control, control_metadata = _engineer(alias_collision_frame, ["amount"], time="event_time")
    frame = alias_collision_frame.withColumnRenamed("amount", metric)
    candidate, metadata = _engineer(frame, [metric], time="event_time")

    assert metadata.temporal_basis == control_metadata.temporal_basis
    assert metadata.temporal_window == control_metadata.temporal_window
    assert metadata.temporal_coefficients[metric] == pytest.approx(
        control_metadata.temporal_coefficients["amount"], rel=1e-10, abs=1e-8
    )
    _assert_equivalent(control, candidate, ["amount", "amount_rel_time"], [metric, metric + "_rel_time"])
    _assert_replay(frame, candidate, metadata)


@pytest.mark.parametrize("time_name", ["__dqx_time_bucket", "__DQX_TIME_BUCKET", "__dqx_bucket_seconds"])
def test_temporal_fit_preserves_time_axis_named_like_working_columns(
    alias_collision_frame: DataFrame, time_name: str
) -> None:
    control, control_metadata = _engineer(alias_collision_frame, ["amount"], time="event_time")
    frame = alias_collision_frame.withColumnRenamed("event_time", time_name)
    candidate, metadata = _engineer(frame, ["amount"], time=time_name)

    assert metadata.baseline_over_time == time_name
    assert metadata.temporal_basis == control_metadata.temporal_basis
    assert metadata.temporal_window == control_metadata.temporal_window
    assert metadata.temporal_coefficients["amount"] == pytest.approx(
        control_metadata.temporal_coefficients["amount"], rel=1e-10, abs=1e-8
    )
    _assert_equivalent(control, candidate, ["amount", "amount_rel_time"], ["amount", "amount_rel_time"])
    _assert_replay(frame, candidate, metadata)


@pytest.mark.parametrize("secondary_name", ["__dqx_amount_baseline", "__DQX_AMOUNT_BASELINE"])
def test_group_lookup_preserves_selected_metric_named_like_a_private_median(
    alias_collision_frame: DataFrame, secondary_name: str
) -> None:
    control, control_metadata = _engineer(alias_collision_frame, ["amount", "secondary"], groups=["region"])
    frame = alias_collision_frame.withColumnRenamed("secondary", secondary_name)
    candidate, metadata = _engineer(frame, ["amount", secondary_name], groups=["region"])

    assert metadata.baseline_medians["amount"] == control_metadata.baseline_medians["amount"]
    assert metadata.baseline_medians[secondary_name] == control_metadata.baseline_medians["secondary"]
    _assert_equivalent(
        control,
        candidate,
        ["amount", "amount_rel_baseline", "secondary", "secondary_rel_baseline"],
        ["amount", "amount_rel_baseline", secondary_name, secondary_name + "_rel_baseline"],
    )
    _assert_replay(frame, candidate, metadata)


def test_combined_baselines_preserve_metrics_and_time_named_like_working_columns(
    alias_collision_frame: DataFrame,
) -> None:
    control, control_metadata = _engineer(
        alias_collision_frame, ["amount", "secondary"], groups=["region"], time="event_time"
    )
    frame = alias_collision_frame.withColumnRenamed("secondary", "__dqx_amount_baseline").withColumnRenamed(
        "event_time", "__dqx_time_bucket"
    )
    candidate, metadata = _engineer(
        frame, ["amount", "__dqx_amount_baseline"], groups=["region"], time="__dqx_time_bucket"
    )

    assert metadata.temporal_basis == control_metadata.temporal_basis
    assert metadata.temporal_window == control_metadata.temporal_window
    for before, after in (("amount", "amount"), ("secondary", "__dqx_amount_baseline")):
        assert metadata.baseline_medians[after] == control_metadata.baseline_medians[before]
        assert metadata.temporal_coefficients[after] == pytest.approx(
            control_metadata.temporal_coefficients[before], rel=1e-10, abs=1e-8
        )
    _assert_equivalent(control, candidate, control_metadata.engineered_feature_names, metadata.engineered_feature_names)
    _assert_replay(frame, candidate, metadata)
