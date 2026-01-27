"""Integration tests for drift detection.
"""

import warnings
from collections.abc import Callable

from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.drift_detector import compute_drift_score
from databricks.labs.dqx.engine import DQEngine

from tests.integration_anomaly.test_anomaly_constants import (
    DEFAULT_SCORE_THRESHOLD,
    DRIFT_THRESHOLD,
    DRIFT_TRAIN_SIZE,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)
from tests.integration_anomaly.test_anomaly_utils import (
    create_anomaly_check_rule,
    train_simple_2d_model,
)


def test_drift_detection_warns_on_distribution_shift(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that drift warning is issued when data distribution shifts."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_drift_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on distribution centered at 100 - use helper (need >= 1000 rows for drift check)
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=DRIFT_TRAIN_SIZE)

    # Test on distribution centered at 2000 (significant shift) - need >= 1000 rows
    test_df = spark.createDataFrame(
        [(i, 2000.0 + i, 10.0) for i in range(1200)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            score_threshold=DEFAULT_SCORE_THRESHOLD,
            drift_threshold=DRIFT_THRESHOLD,
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()  # Force evaluation

        # Check that drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) > 0

        # Check that warning mentions drifted columns and sample size
        warning_message = str(drift_warnings[0].message)
        assert "amount" in warning_message.lower() or "quantity" in warning_message.lower()
        assert "sample size" in warning_message.lower()
        assert "1200" in warning_message or "1,200" in warning_message


def test_no_drift_warning_on_similar_distribution(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that no drift warning is issued when distributions are similar."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_no_drift_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train on distribution - use helper (need >= 1000 rows)
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=DRIFT_TRAIN_SIZE)

    # Test on similar distribution (need >= 1000 rows for drift check to run)
    test_df = spark.createDataFrame(
        [(i, 100.0 + i, 2.0) for i in range(1200)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            score_threshold=DEFAULT_SCORE_THRESHOLD,
            drift_threshold=DRIFT_THRESHOLD,
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()

        # Check that NO drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) == 0


def test_drift_detection_disabled_when_threshold_none(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that drift detection is disabled when drift_threshold=None."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_drift_disabled_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=DRIFT_TRAIN_SIZE)

    # Test on very different distribution (large enough batch)
    test_df = spark.createDataFrame(
        [(i, OUTLIER_AMOUNT, OUTLIER_QUANTITY) for i in range(1200)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            score_threshold=DEFAULT_SCORE_THRESHOLD,
            drift_threshold=None,  # Disable drift detection
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()

        # Check that NO drift warning was issued (case-insensitive check)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) == 0


def test_drift_detection_skipped_on_small_batch(
    ws,
    spark: SparkSession,
    make_random: Callable[[int], str],
    anomaly_engine,
    anomaly_registry_prefix,
):
    """Test that drift detection is skipped when batch size < 1000 rows."""
    # Create unique table names for test isolation
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_small_batch_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    # Train model - use helper
    train_simple_2d_model(spark, anomaly_engine, model_name, registry_table, train_size=DRIFT_TRAIN_SIZE)

    # Test on very different distribution BUT with small batch (< 1000 rows)
    test_df = spark.createDataFrame(
        [(i, OUTLIER_AMOUNT, OUTLIER_QUANTITY) for i in range(500)],  # Only 500 rows
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            score_threshold=DEFAULT_SCORE_THRESHOLD,
            drift_threshold=DRIFT_THRESHOLD,  # Drift detection enabled, but batch too small
        )
    ]

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result_df = dq_engine.apply_checks(test_df, checks)
        result_df.collect()

        # Check that NO drift warning was issued (skipped due to small batch)
        drift_warnings = [warning for warning in w if "drift detected" in str(warning.message).lower()]
        assert len(drift_warnings) == 0


# ============================================================================
# Direct Drift Detector Module Tests
# ============================================================================


def test_drift_detector_no_drift_when_distributions_match(spark):
    """Test that drift is not detected when distributions are identical."""
    # Need >= 1000 rows for drift check to run
    df = spark.createDataFrame(
        [(100.0 + (i % 10) * 0.1,) for i in range(1200)],
        "value double",
    )

    baseline_stats = {
        "value": {
            "mean": 100.5,
            "std": 1.12,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.5,
            "p75": 101.5,
        }
    }

    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)

    assert not result.drift_detected
    assert result.drift_score < 3.0
    assert len(result.drifted_columns) == 0
    assert result.sample_size == 1200


def test_drift_detector_drift_detected_when_mean_shifts(spark):
    """Test that drift is detected when mean shifts significantly."""
    # Need >= 1000 rows for drift check to run
    df = spark.createDataFrame(
        [(500.0 + (i % 10) * 0.1,) for i in range(1200)],
        "value double",
    )

    baseline_stats = {
        "value": {
            "mean": 100.0,
            "std": 1.0,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.0,
            "p75": 101.0,
        }
    }

    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)

    assert result.drift_detected
    assert result.drift_score > 3.0
    assert "value" in result.drifted_columns
    assert result.recommendation == "retrain"
    assert result.sample_size == 1200


def test_drift_detector_with_multiple_columns(spark):
    """Test drift detection with multiple columns."""
    # Need >= 1000 rows for drift check to run
    df = spark.createDataFrame(
        [(100.0 + (i % 10) * 0.1, 500.0 + (i % 10) * 0.1) for i in range(1200)],
        "col1 double, col2 double",
    )

    baseline_stats = {
        "col1": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
        "col2": {"mean": 100.0, "std": 1.0, "min": 99.0, "max": 102.0, "p25": 99.5, "p50": 100.0, "p75": 101.0},
    }

    result = compute_drift_score(df, ["col1", "col2"], baseline_stats, threshold=3.0)

    assert result.drift_detected
    assert "col2" in result.drifted_columns
    assert "col1" not in result.drifted_columns
    assert result.sample_size == 1200


def test_drift_detector_small_batch_skipped(spark):
    """Test that drift check is skipped for batches < 1000 rows."""
    df = spark.createDataFrame(
        [(500.0 + i,) for i in range(500)],  # Only 500 rows
        "value double",
    )

    baseline_stats = {
        "value": {
            "mean": 100.0,
            "std": 1.0,
            "min": 99.0,
            "max": 102.0,
            "p25": 99.5,
            "p50": 100.0,
            "p75": 101.0,
        }
    }

    result = compute_drift_score(df, ["value"], baseline_stats, threshold=3.0)

    # Even with massive drift, should be skipped due to small batch
    assert not result.drift_detected
    assert result.recommendation == "skipped_small_batch"
    assert result.sample_size == 500
    assert result.drift_score == 0.0
    assert len(result.drifted_columns) == 0
