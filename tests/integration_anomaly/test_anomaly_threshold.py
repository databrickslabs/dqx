"""Integration tests for anomaly threshold selection."""

from collections import Counter
from collections.abc import Callable
import math

import numpy as np
import pytest
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Row, SparkSession

from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.explainability import severity_from_scores
from databricks.labs.dqx.anomaly.scoring_utils import add_severity_percentile_column
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
)
from tests.integration_anomaly.conftest import (
    apply_anomaly_check_direct,
    create_anomaly_check_rule,
    create_anomaly_dataset_rule,
    train_simple_2d_model,
)
from tests.integration_anomaly.synthetic_generators import generate_overlapping_gaussian_data


def _scored_snapshot(source: DataFrame, result: DataFrame, anomaly: Column, threshold: float) -> list[Row]:
    """Validate all evaluated rows before comparing flags or calculating a quality metric."""
    expected = source.collect()
    assert expected, "threshold fixtures must contain evaluation rows"
    assert [result.schema[name] for name in source.columns] == list(source.schema)
    fields = [
        anomaly.getField("score").alias("observed_score"),
        anomaly.getField("severity_percentile").alias("observed_severity"),
        anomaly.getField("is_anomaly").alias("observed_flag"),
    ]
    has_errors = "_errors" in result.columns
    if has_errors:
        fields.append((F.size(F.coalesce(F.col("_errors"), F.array())) > 0).alias("observed_error"))
    observed = result.select(*source.columns, *fields).collect()
    expected_rows = Counter(tuple(row) for row in expected)
    observed_rows = Counter(tuple(row[name] for name in source.columns) for row in observed)
    assert observed_rows == expected_rows, "scoring changed evaluation rows or duplicate counts"
    for row in observed:
        assert row.observed_score is not None and math.isfinite(row.observed_score)
        assert row.observed_severity is not None and math.isfinite(row.observed_severity)
        assert 0.0 <= row.observed_severity <= 100.0
        assert isinstance(row.observed_flag, bool), "known rows must have a defined anomaly verdict"
        assert row.observed_flag is (row.observed_severity >= threshold)
        if has_errors:
            assert row.observed_error is row.observed_flag
    return observed


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

    # This test only asserts on is_anomaly counts; disable SHAP contributions + AI explanations
    # so it doesn't pay the SHAP cost or make ai_query (LLM) calls.
    _, apply_low, info_col_low = has_no_row_anomalies(
        model_name=model_name,
        registry_table=registry_table,
        threshold=30.0,
        enable_contributions=False,
        enable_ai_explanation=False,
    )
    _, apply_high, info_col_high = has_no_row_anomalies(
        model_name=model_name,
        registry_table=registry_table,
        threshold=90.0,
        enable_contributions=False,
        enable_ai_explanation=False,
    )

    low_rows = _scored_snapshot(test_df, apply_low(test_df), F.col(info_col_low).getField("anomaly"), 30.0)
    high_rows = _scored_snapshot(test_df, apply_high(test_df), F.col(info_col_high).getField("anomaly"), 90.0)
    low_ids = {tuple(row[column] for column in feature_cols) for row in low_rows if row.observed_flag}
    high_ids = {tuple(row[column] for column in feature_cols) for row in high_rows if row.observed_flag}
    assert high_ids, "the fixture must include a positive control above the high threshold"
    assert low_ids - high_ids, "the fixture must exercise the interval between thresholds"
    assert high_ids <= low_ids


def test_threshold_affects_flagging(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that different thresholds flag different numbers of anomalies."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_threshold_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train model using helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Test data with varying degrees of anomalousness
    test_df = spark.createDataFrame(
        [
            (1, 100.0, 2.0),  # Normal
            (2, 150.0, 1.8),  # Slightly unusual
            (3, 500.0, 1.0),  # Moderately unusual
            (4, OUTLIER_AMOUNT, 0.5),  # Highly unusual
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)

    # Aggressive threshold (0.3) - flags more
    rule_aggressive = create_anomaly_dataset_rule(model_name, registry_table, threshold=30.0)

    result_aggressive = dq_engine.apply_checks(test_df, [rule_aggressive])
    aggressive = _scored_snapshot(test_df, result_aggressive, F.col("_dq_info")[0].getField("anomaly"), 30.0)
    aggressive_ids = {row.transaction_id for row in aggressive if row.observed_flag}

    # Conservative threshold (0.9) - flags less
    rule_conservative = create_anomaly_dataset_rule(model_name, registry_table, threshold=90.0)

    result_conservative = dq_engine.apply_checks(test_df, [rule_conservative])
    conservative = _scored_snapshot(test_df, result_conservative, F.col("_dq_info")[0].getField("anomaly"), 90.0)
    conservative_ids = {row.transaction_id for row in conservative if row.observed_flag}

    # More aggressive threshold should flag more rows
    assert conservative_ids <= aggressive_ids


def test_precision_recall_tradeoff(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that lower threshold increases recall (catches more anomalies)."""
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_precision_recall_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train model using helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table)

    # Test data with clear anomalies
    test_df = spark.createDataFrame(
        [
            (1, 100.0, 2.0),  # Normal
            (2, OUTLIER_AMOUNT, 0.1),  # Extreme anomaly
            (3, 8888.0, 0.2),  # Extreme anomaly
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)

    # High threshold (low recall) - use helper
    rule_low_recall = create_anomaly_dataset_rule(model_name, registry_table, threshold=95.0)

    result_low_recall = dq_engine.apply_checks(test_df, [rule_low_recall])
    low_recall = _scored_snapshot(test_df, result_low_recall, F.col("_dq_info")[0].getField("anomaly"), 95.0)
    low_recall_ids = {row.transaction_id for row in low_recall if row.observed_flag}

    # Low threshold (high recall) - use helper
    rule_high_recall = create_anomaly_dataset_rule(model_name, registry_table, threshold=30.0)

    result_high_recall = dq_engine.apply_checks(test_df, [rule_high_recall])
    high_recall = _scored_snapshot(test_df, result_high_recall, F.col("_dq_info")[0].getField("anomaly"), 30.0)
    high_recall_ids = {row.transaction_id for row in high_recall if row.observed_flag}

    # Lower threshold should catch more anomalies
    assert low_recall_ids <= high_recall_ids
    anomaly_ids = {2, 3}
    low_recall_rate = len(low_recall_ids & anomaly_ids) / len(anomaly_ids)
    high_recall_rate = len(high_recall_ids & anomaly_ids) / len(anomaly_ids)
    assert high_recall_rate >= low_recall_rate


def test_threshold_edge_cases(ws, spark, test_df_factory, quick_model_factory):
    """Test the inclusive zero cutoff and rejected upper endpoint."""
    model_name, registry_table, _columns = quick_model_factory(spark)

    test_df = test_df_factory(spark)

    dq_engine = DQEngine(ws, spark)

    # Threshold 0.0 - flags everything - use helper
    rule_zero = create_anomaly_dataset_rule(model_name, registry_table, threshold=0.0)

    result_zero = dq_engine.apply_checks(test_df, [rule_zero])
    zero = _scored_snapshot(test_df, result_zero, F.col("_dq_info")[0].getField("anomaly"), 0.0)
    zero_ids = {row.transaction_id for row in zero if row.observed_flag}

    # Every known, finite row meets the inclusive zero-percentile threshold.
    assert zero_ids == {row.transaction_id for row in test_df.collect()}

    with pytest.raises(InvalidParameterError, match="threshold"):
        create_anomaly_dataset_rule(model_name, registry_table, threshold=100.0)


def test_threshold_consistency(spark, test_df_factory, quick_model_factory):
    """Test that same threshold produces consistent results."""
    # Use sample_fraction=1.0 for consistency
    params = AnomalyParams(sample_fraction=1.0, max_rows=50)

    model_name, registry_table, _columns = quick_model_factory(spark, params=params)

    test_df = test_df_factory(spark)

    # Call directly to get anomaly_score column
    # Run twice with same threshold
    result1 = apply_anomaly_check_direct(
        test_df,
        model_name,
        registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result2 = apply_anomaly_check_direct(
        test_df,
        model_name,
        registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )

    expected_ids = sorted(row.transaction_id for row in test_df.select("transaction_id").collect())
    rows1 = result1.orderBy("transaction_id").collect()
    rows2 = result2.orderBy("transaction_id").collect()
    assert [row.transaction_id for row in rows1] == expected_ids
    assert [row.transaction_id for row in rows2] == expected_ids
    for row1, row2 in zip(rows1, rows2, strict=True):
        assert row1.anomaly_score is not None and row2.anomaly_score is not None
        assert np.isfinite(row1.anomaly_score) and np.isfinite(row2.anomaly_score)
        assert row1.anomaly_score == pytest.approx(row2.anomaly_score, abs=0.001)
        assert row1.severity_percentile == row2.severity_percentile


def test_validation_metrics_in_registry(spark: SparkSession, quick_model_factory):
    """Test that validation metrics are stored in registry."""
    # Use sample_fraction=1.0 to ensure validation set has enough data
    params = AnomalyParams(sample_fraction=1.0, max_rows=50)

    model_name, registry_table, _columns = quick_model_factory(spark, params=params)

    # Check metrics in registry (model_name is already full three-level name)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None, f"Model {model_name} not found in registry"

    metrics = record["training"]["metrics"]
    assert metrics is not None

    # Recommended threshold should be present

    # May also have precision, recall, f1_score (depending on implementation)
    # These are optional but good to have: precision, recall, f1_score, val_anomaly_rate
    # At least some metrics should be present
    assert len(metrics) >= 1


def test_the_spark_and_numpy_severity_maps_agree_across_the_tail(spark: SparkSession):
    """The two implementations must not diverge, and only a session can check the Spark one.

    ``severity_from_scores`` decides which rows get SHAP inside the scoring UDF while
    ``add_severity_percentile_column`` produces the severity everything downstream reads. A disagreement
    would drop contributions from precisely the rows that were flagged, which is the one place a reader
    looks. The probe grid deliberately spans past the last knot, because that whole range used to be a
    single clamped value and is now where the two could most easily part company.
    """
    points = [
        (0.0, 0.0),
        (1.0, 0.4),
        (5.0, 0.8),
        (10.0, 1.0),
        (25.0, 1.5),
        (50.0, 2.0),
        (75.0, 3.0),
        (90.0, 4.0),
        (95.0, 5.0),
        (99.0, 8.0),
        (100.0, 12.0),
    ]
    probes = [float(value) for value in np.linspace(-1.0, 60.0, 400)]

    scored = add_severity_percentile_column(
        spark.createDataFrame([(value,) for value in probes], "score double"),
        score_col="score",
        severity_col="severity",
        quantile_points=points,
    )
    from_spark = np.array([row["severity"] for row in scored.orderBy("score").collect()])
    from_numpy = severity_from_scores(np.array(sorted(probes)), points)

    assert np.allclose(
        from_spark, from_numpy, atol=1e-9
    ), f"largest disagreement {np.max(np.abs(from_spark - from_numpy))}"


def test_a_threshold_between_the_knots_flags_close_to_its_budget_through_real_scoring(
    ws, spark: SparkSession, quick_model_factory
):
    """End to end, through a trained model and the public check, not just the mapping in isolation.

    ``threshold=98`` used to flag well under the 2% it promises, because the persisted grid has no knot
    between 95 and 99 and the interpolation ran in score space. Scoring data drawn from the same
    distribution the model trained on is what isolates that: no drift, no planted anomalies, so the only
    thing the realised rate can be measuring is the mapping.

    This test also guards ``SEVERITY_QUANTILE_RELATIVE_ERROR``, which is easy to read as a tuning detail and
    is not. The tail takes its decay rate from ``q99 - q95``, so slackening that error loosens where the cut
    for 98 lands: measured on an Isolation Forest ensemble with every other knot exact, reporting the true
    p100 as ``q99`` -- which a relative error of 0.01 permits -- moved the realised rate from 2.25% to 0.65%.
    An earlier version of this assertion failed at 0.750% for exactly that reason.
    """
    rng = np.random.default_rng(29)
    train_rows = [(float(a), float(b)) for a, b in rng.normal(100.0, 10.0, size=(4000, 2))]
    model_name, registry_table, _ = quick_model_factory(
        spark, train_data=train_rows, params=AnomalyParams(sample_fraction=1.0)
    )

    test_rows = [(float(a), float(b)) for a, b in rng.normal(100.0, 10.0, size=(4000, 2))]
    test_df = spark.createDataFrame(test_rows, "amount double, quantity double")

    result_df = DQEngine(ws, spark).apply_checks(
        test_df,
        [create_anomaly_check_rule(model_name=model_name, registry_table=registry_table, threshold=98.0)],
    )
    observed = _scored_snapshot(test_df, result_df, F.col("_dq_info")[0].getField("anomaly"), 98.0)
    realised = sum(row.observed_flag for row in observed) / len(observed)

    # Measured expectation is about 2.25%. The band is wide because a 4,000-row sample of a 2% tail carries
    # binomial noise of roughly 0.22pp on its own, the quantile grid is still an approximation, and the
    # exponential tail is the right shape for these scores without being exact for any of them. It stays a
    # real guard: score-space interpolation gave 0.65% to 1.6% on this fixture depending on the grid.
    assert 0.012 < realised < 0.035, f"threshold 98 flagged {realised:.3%} of an unremarkable batch"
