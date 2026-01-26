"""Integration tests for anomaly threshold selection."""

from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine

from tests.integration_anomaly.test_anomaly_constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
)
from tests.integration_anomaly.test_anomaly_utils import (
    apply_anomaly_check_direct,
    create_anomaly_dataset_rule,
    train_simple_2d_model,
)


def test_threshold_affects_flagging(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that different thresholds flag different numbers of anomalies."""
    unique_id = make_random(8).lower()
    model_name = f"test_threshold_{make_random(4).lower()}"
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
    rule_aggressive = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=0.3)

    result_aggressive = dq_engine.apply_checks(test_df, [rule_aggressive])
    errors_aggressive = result_aggressive.filter(F.size("_errors") > 0).count()

    # Conservative threshold (0.9) - flags less
    rule_conservative = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=0.9)

    result_conservative = dq_engine.apply_checks(test_df, [rule_conservative])
    errors_conservative = result_conservative.filter(F.size("_errors") > 0).count()

    # More aggressive threshold should flag more rows
    assert errors_aggressive >= errors_conservative


def test_recommended_threshold_stored(spark: SparkSession, quick_model_factory):
    """Test that recommended_threshold is stored in registry metrics."""
    # Use sample_fraction=1.0 to ensure validation set has enough data
    params = AnomalyParams(sample_fraction=1.0, max_rows=50)

    model_name, registry_table, _columns = quick_model_factory(spark, params=params)

    # Query recommended threshold from registry (model_name is already full three-level name)
    record = spark.table(registry_table).filter(f"identity.model_name = '{model_name}'").first()
    assert record is not None, f"Model {model_name} not found in registry"

    # Verify recommended_threshold exists in metrics
    assert record["training"]["metrics"] is not None
    assert "recommended_threshold" in record["training"]["metrics"]

    # Verify it's a reasonable value (between 0 and 1)
    recommended = record["training"]["metrics"]["recommended_threshold"]
    assert 0.0 <= recommended <= 1.0


def test_using_recommended_threshold(spark: SparkSession, test_df_factory, quick_model_factory):
    """Test using recommended_threshold from registry in checks."""
    # Use sample_fraction=1.0 to ensure validation set has enough data
    params = AnomalyParams(sample_fraction=1.0, max_rows=50)

    model_name, registry_table, _columns = quick_model_factory(spark, params=params)

    recommended_result = spark.sql(
        f"""
        SELECT training.metrics['recommended_threshold'] as threshold
        FROM {registry_table}
        WHERE identity.model_name = '{model_name}' AND identity.status = 'active'
    """
    )
    threshold_row = recommended_result.first()
    assert threshold_row is not None, "Failed to retrieve threshold from registry"
    recommended_threshold = threshold_row["threshold"]

    # Use recommended threshold in check
    test_df = test_df_factory(spark)

    # Call directly to get anomaly_score column
    result_df = apply_anomaly_check_direct(test_df, model_name, registry_table, score_threshold=recommended_threshold)

    # Should work correctly
    assert "anomaly_score" in result_df.columns


def test_precision_recall_tradeoff(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that lower threshold increases recall (catches more anomalies)."""
    unique_id = make_random(8).lower()
    model_name = f"test_precision_recall_{make_random(4).lower()}"
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
    rule_low_recall = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=0.95)

    result_low_recall = dq_engine.apply_checks(test_df, [rule_low_recall])
    flagged_low_recall = result_low_recall.filter(F.size("_errors") > 0).count()

    # Low threshold (high recall) - use helper
    rule_high_recall = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=0.3)

    result_high_recall = dq_engine.apply_checks(test_df, [rule_high_recall])
    flagged_high_recall = result_high_recall.filter(F.size("_errors") > 0).count()

    # Lower threshold should catch more anomalies
    assert flagged_high_recall >= flagged_low_recall


def test_threshold_edge_cases(ws, spark, test_df_factory, quick_model_factory):
    """Test edge case thresholds (0.0 and 1.0)."""
    model_name, registry_table, _columns = quick_model_factory(spark)

    test_df = test_df_factory(spark)

    dq_engine = DQEngine(ws, spark)

    # Threshold 0.0 - flags everything - use helper
    rule_zero = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=0.0)

    result_zero = dq_engine.apply_checks(test_df, [rule_zero])
    flagged_zero = result_zero.filter(F.size("_errors") > 0).count()

    # Should flag most/all rows
    assert flagged_zero >= 1

    # Threshold 1.0 - flags nothing (almost impossible to exceed) - use helper
    rule_one = create_anomaly_dataset_rule(model_name, registry_table, score_threshold=1.0)

    result_one = dq_engine.apply_checks(test_df, [rule_one])
    flagged_one = result_one.filter(F.size("_errors") > 0).count()

    # Should flag few/no rows
    assert flagged_one <= flagged_zero


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
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result2 = apply_anomaly_check_direct(
        test_df,
        model_name,
        registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
    )

    # Should produce same results
    scores1 = [row["anomaly_score"] for row in result1.orderBy("transaction_id").collect()]
    scores2 = [row["anomaly_score"] for row in result2.orderBy("transaction_id").collect()]

    # Scores should be identical (deterministic)
    for score1, score2 in zip(scores1, scores2):
        if score1 is not None and score2 is not None:
            assert abs(score1 - score2) < 0.001  # Allow small floating point error


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
    assert "recommended_threshold" in metrics

    # May also have precision, recall, f1_score (depending on implementation)
    # These are optional but good to have: precision, recall, f1_score, val_anomaly_rate
    # At least some metrics should be present
    assert len(metrics) >= 1
