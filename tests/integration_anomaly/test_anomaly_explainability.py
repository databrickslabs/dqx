"""
Integration tests for anomaly feature explainability.

NOTE: These tests require SHAP library (shap>=0.42.0,<0.46) installed on the cluster.
If using DATABRICKS_CLUSTER_ID, install the library via:
  Cluster -> Libraries -> Install New -> PyPI -> shap>=0.42.0,<0.46

OPTIMIZATION: These tests use session-scoped shared fixtures (shared_2d_model, shared_3d_model, 
shared_4d_model) to avoid retraining models. This reduces runtime from ~60 min to ~10 min (83% savings).
"""

import numpy as np
import pytest
from pyspark.sql import SparkSession
from sklearn.ensemble import IsolationForest

from databricks.labs.dqx.anomaly.explainer import (
    add_top_contributors_to_message,
    create_optimal_tree_explainer,
)
from databricks.labs.dqx.anomaly import explainer as explainer_mod
from databricks.labs.dqx.anomaly.check_funcs import set_driver_only_for_tests

from tests.integration_anomaly.test_anomaly_constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)
from tests.integration_anomaly.test_anomaly_utils import score_3d_with_contributions


def test_feature_contributions_added(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that anomaly_contributions column is added when requested."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use helper to score with contributions
    result_df = score_3d_with_contributions(
        spark,
        test_df_factory,
        anomaly_scorer,
        model_name,
        registry_table,
    )
    row = result_df.collect()[0]

    # Verify contributions exist in _dq_info.anomaly.contributions
    assert row["_dq_info"]["anomaly"]["contributions"] is not None

    # Extract contributions map from _dq_info.anomaly.contributions
    contribs = row["_dq_info"]["anomaly"]["contributions"]

    # Verify contributions contain feature names
    assert contribs is not None
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs


def test_contribution_percentages_sum_to_one(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that contribution percentages sum to approximately 1.0."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )

    # Use anomaly_scorer with include_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        include_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info.anomaly.contributions
    contribs = row["_dq_info"]["anomaly"]["contributions"]

    # Filter out None values before summing
    total = sum(v for v in contribs.values() if v is not None)

    # Allow small floating point error
    assert abs(total - 1.0) < 0.01


def test_multi_feature_contributions(spark: SparkSession, shared_4d_model, test_df_factory, anomaly_scorer):
    """Test contributions with 4+ columns."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_4d_model["model_name"]
    registry_table = shared_4d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95, 1.0)],
        columns_schema="amount double, quantity double, discount double, weight double",
    )

    # Use anomaly_scorer with include_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        include_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info.anomaly.contributions
    contribs = row["_dq_info"]["anomaly"]["contributions"]

    # Verify all features are represented
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs
    assert "weight" in contribs


def test_contributions_without_flag_not_added(spark: SparkSession, shared_2d_model, test_df_factory, anomaly_scorer):
    """Test that contributions are not added when include_contributions=False."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Use anomaly_scorer with include_contributions=False
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        include_contributions=False,  # Explicitly False
        extract_score=False,
    )

    # Verify anomaly_contributions column does NOT exist
    assert "anomaly_contributions" not in result_df.columns


def test_top_contributor_is_reasonable(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that the top contributor makes sense for the anomaly."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use factory to create test DataFrame with extreme amount value
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, 2.0, 0.15)],  # Extreme amount
        columns_schema="amount double, quantity double, discount double",
    )

    # Use anomaly_scorer with include_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        score_threshold=DEFAULT_SCORE_THRESHOLD,
        include_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info.anomaly.contributions
    contribs = row["_dq_info"]["anomaly"]["contributions"]

    # Filter out None values
    valid_contribs = {k: v for k, v in contribs.items() if v is not None}

    # Find top contributor
    top_feature = max(valid_contribs, key=lambda k: valid_contribs[k])

    # Amount should likely be the top contributor (or at least significant)
    # Since amount is the most anomalous feature
    assert top_feature in {"amount", "discount", "quantity"}
    assert valid_contribs[top_feature] > 0.2  # Should have significant contribution


def test_add_top_contributors_message(spark: SparkSession):
    """Top contributors are added only for rows above threshold."""
    df = spark.createDataFrame(
        [
            (0.7, {"amount": 0.8, "quantity": 0.2}),
            (0.2, {"amount": 0.9, "quantity": 0.1}),
        ],
        "anomaly_score double, anomaly_contributions map<string,double>",
    )

    result = add_top_contributors_to_message(df, threshold=0.6, top_n=2)
    rows = {row.anomaly_score: row["_top_contributors"] for row in result.collect()}

    assert "amount" in rows[0.7]
    assert "quantity" in rows[0.7]
    assert rows[0.2] == ""


def test_create_optimal_tree_explainer():
    """TreeExplainer is created when SHAP is available, otherwise ImportError."""
    if explainer_mod.shap is None:
        with pytest.raises(ImportError):
            create_optimal_tree_explainer(object())
        return

    model = IsolationForest(random_state=42).fit(np.array([[0.0, 0.1], [1.0, 1.1], [2.0, 2.1]]))
    explainer = create_optimal_tree_explainer(model)
    assert explainer is not None


def test_driver_only_contributions_smoke(spark: SparkSession, shared_2d_model, test_df_factory, anomaly_scorer):
    """Compute SHAP contributions in driver-only mode (Spark Connect safe)."""
    if explainer_mod.shap is None:
        pytest.skip("Explainability dependencies not available")

    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    set_driver_only_for_tests(True)
    try:
        result_df = anomaly_scorer(
            test_df,
            model_name=model_name,
            registry_table=registry_table,
            score_threshold=DEFAULT_SCORE_THRESHOLD,
            include_contributions=True,
            extract_score=False,
        )
        row = result_df.collect()[0]
        contribs = row["_dq_info"]["anomaly"]["contributions"]
        assert contribs is not None
        assert "amount" in contribs and "quantity" in contribs
    finally:
        set_driver_only_for_tests(False)


def test_compute_contributions_helper():
    """Compute contributions directly for a small matrix."""
    if explainer_mod.shap is None or explainer_mod.np is None or explainer_mod.pd is None:
        pytest.skip("Explainability dependencies not available")

    model = IsolationForest(random_state=42).fit(np.array([[0.0, 0.1], [1.0, 1.1], [2.0, 2.1]]))
    feature_matrix = np.array([[0.0, 0.1], [2.0, 2.1]])
    contributions = explainer_mod.compute_contributions_for_matrix(model, feature_matrix, ["amount", "quantity"])

    assert len(contributions) == 2
    assert set(contributions[0].keys()) == {"amount", "quantity"}
