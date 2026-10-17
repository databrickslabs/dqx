"""Collected Spark contracts for the anomaly detection data generators."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from tests.integration_anomaly.synthetic_generators import (
    apply_distribution_shift,
    generate_overlapping_gaussian_data,
    generate_segment_conditional_data,
    inject_missingness_spike,
)


def test_missingness_spike_and_shift_generators(spark: SparkSession) -> None:
    """Missingness and shift helpers should deterministically modify data characteristics."""
    _feature_cols, _train_df, test_df = generate_overlapping_gaussian_data(
        spark,
        seed=321,
        n_samples=1200,
        n_features=4,
        anomaly_frac=0.02,
    )

    base_nulls = test_df.filter(F.col("feature_0").isNull()).count()
    spiked = inject_missingness_spike(test_df, columns=["feature_0"], missing_fraction=0.35, seed=11)
    spiked_nulls = spiked.filter(F.col("feature_0").isNull()).count()
    assert spiked_nulls > base_nulls

    shifted = apply_distribution_shift(test_df, columns=["feature_1"], shift_factor=1.5)
    base_statistics = test_df.select(F.mean("feature_1")).first()
    shifted_statistics = shifted.select(F.mean("feature_1")).first()
    assert base_statistics is not None
    assert shifted_statistics is not None
    assert shifted_statistics[0] == pytest.approx(base_statistics[0] * 1.5)


def test_segment_conditional_generator_has_region_column(spark: SparkSession) -> None:
    """Segment-conditioned synthetic generator should preserve region segmentation column."""
    feature_cols, train_df, _test_df = generate_segment_conditional_data(spark, seed=77, n_per_segment=300)

    assert feature_cols == ["amount", "quantity"]
    assert "region" in train_df.columns
    regions = {row[0] for row in train_df.select("region").distinct().collect()}
    assert regions == {"US", "EU", "APAC"}
