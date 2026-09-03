"""Integration tests for anomaly threshold selection."""

from collections.abc import Callable

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.explainability import severity_from_scores
from databricks.labs.dqx.anomaly.scoring_utils import add_severity_percentile_column
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine
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

    low_count = apply_low(test_df).filter(F.col(f"{info_col_low}.anomaly.is_anomaly") == F.lit(True)).count()
    high_count = apply_high(test_df).filter(F.col(f"{info_col_high}.anomaly.is_anomaly") == F.lit(True)).count()

    assert low_count >= high_count


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
    errors_aggressive = result_aggressive.filter(F.size("_errors") > 0).count()

    # Conservative threshold (0.9) - flags less
    rule_conservative = create_anomaly_dataset_rule(model_name, registry_table, threshold=90.0)

    result_conservative = dq_engine.apply_checks(test_df, [rule_conservative])
    errors_conservative = result_conservative.filter(F.size("_errors") > 0).count()

    # More aggressive threshold should flag more rows
    assert errors_aggressive >= errors_conservative


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
    flagged_low_recall = result_low_recall.filter(F.size("_errors") > 0).count()

    # Low threshold (high recall) - use helper
    rule_high_recall = create_anomaly_dataset_rule(model_name, registry_table, threshold=30.0)

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
    rule_zero = create_anomaly_dataset_rule(model_name, registry_table, threshold=0.0)

    result_zero = dq_engine.apply_checks(test_df, [rule_zero])
    flagged_zero = result_zero.filter(F.size("_errors") > 0).count()

    # Should flag most/all rows
    assert flagged_zero >= 1

    # Threshold 100.0 - flags nothing (almost impossible to exceed) - use helper
    rule_one = create_anomaly_dataset_rule(model_name, registry_table, threshold=100.0)

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
        threshold=DEFAULT_SCORE_THRESHOLD,
    )
    result2 = apply_anomaly_check_direct(
        test_df,
        model_name,
        registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
    )

    # Should produce same results
    scores1 = [row["anomaly_score"] for row in result1.orderBy("transaction_id").collect()]
    scores2 = [row["anomaly_score"] for row in result2.orderBy("transaction_id").collect()]

    # Scores should be identical (deterministic)
    for score1, score2 in zip(scores1, scores2, strict=False):
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
    severity = F.col("_dq_info")[0].getField("anomaly").getField("severity_percentile")
    realised = result_df.filter(severity >= 98.0).count() / result_df.count()

    # Generous, because a 4,000-row sample of a 2% tail carries real binomial noise (sd about 0.2pp) on top
    # of the grid's own approximation. The behaviour being guarded against realised 1.4% or less.
    assert 0.014 < realised < 0.030, f"threshold 98 flagged {realised:.3%} of an unremarkable batch"
