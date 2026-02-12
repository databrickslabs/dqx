"""Integration tests for synthetic anomaly data generators."""

import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly import has_no_anomalies
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.synthetic_generators import (
    apply_distribution_shift,
    generate_overlapping_gaussian_data,
    generate_segment_conditional_data,
    inject_missingness_spike,
)


def test_synthetic_generator_threshold_monotonicity(anomaly_engine, spark, make_schema, make_random):
    """Lower threshold should flag at least as many anomalies as higher threshold."""
    feature_cols, train_df, test_df = generate_overlapping_gaussian_data(
        spark,
        seed=123,
        n_samples=2500,
        n_features=8,
        anomaly_frac=0.03,
        anomaly_shift=2.1,
    )

    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.syn_model_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.syn_reg_{make_random(4).lower()}"

    anomaly_engine.train(
        df=train_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=feature_cols,
    )

    _, apply_low = has_no_anomalies(model=model_name, registry_table=registry_table, threshold=30.0)
    _, apply_high = has_no_anomalies(model=model_name, registry_table=registry_table, threshold=90.0)

    low_count = apply_low(test_df).filter(F.col("_dq_info.anomaly.is_anomaly") == F.lit(True)).count()
    high_count = apply_high(test_df).filter(F.col("_dq_info.anomaly.is_anomaly") == F.lit(True)).count()

    assert low_count >= high_count


def test_missingness_spike_and_shift_generators(spark):
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
    base_mean = test_df.select(F.mean("feature_1")).first()[0]
    shifted_mean = shifted.select(F.mean("feature_1")).first()[0]
    assert shifted_mean > base_mean


def test_segment_conditional_generator_has_region_column(spark):
    """Segment-conditioned synthetic generator should preserve region segmentation column."""
    feature_cols, train_df, _test_df = generate_segment_conditional_data(spark, seed=77, n_per_segment=300)

    assert feature_cols == ["amount", "quantity"]
    assert "region" in train_df.columns
    regions = {row[0] for row in train_df.select("region").distinct().collect()}
    assert regions == {"US", "EU", "APAC"}
