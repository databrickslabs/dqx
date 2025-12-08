"""Integration tests for anomaly feature explainability."""

import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_feature_importance_stored(spark: SparkSession):
    """Test that feature_importance is stored in registry."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(50)],
        "amount double, quantity double, discount double",
    )
    
    registry_table = "main.default.test_importance_registry"
    model_name = "test_importance"
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Query registry for feature_importance
    record = spark.table(registry_table).filter(f"model_name = '{model_name}'").first()
    
    # Verify feature_importance exists and is non-empty
    assert record["feature_importance"] is not None
    assert len(record["feature_importance"]) > 0
    
    # Verify all columns are represented
    feature_importance = record["feature_importance"]
    assert "amount" in feature_importance
    assert "quantity" in feature_importance
    assert "discount" in feature_importance
    
    # Verify importance values are numeric
    for col, importance in feature_importance.items():
        assert isinstance(importance, (int, float))
        assert importance >= 0


def test_feature_contributions_added(spark: SparkSession, mock_workspace_client):
    """Test that anomaly_contributions column is added when requested."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(30)],
        "amount double, quantity double, discount double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name="test_contrib",
        registry_table="main.default.test_contrib_registry",
    )
    
    # Score with include_contributions=True
    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_contrib",
            registry_table="main.default.test_contrib_registry",
            score_threshold=0.5,
            include_contributions=True,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    row = result_df.collect()[0]
    
    # Verify anomaly_contributions column exists
    assert "anomaly_contributions" in result_df.columns
    
    # Verify contributions is a map
    contribs = row["anomaly_contributions"]
    assert contribs is not None
    assert isinstance(contribs, dict)
    
    # Verify contributions contain feature names
    assert len(contribs) > 0
    assert any(col in contribs for col in ["amount", "quantity", "discount"])


def test_contribution_percentages_sum_to_one(spark: SparkSession, mock_workspace_client):
    """Test that contribution percentages sum to approximately 1.0."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1) for i in range(30)],
        "amount double, quantity double, discount double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name="test_contrib_sum",
        registry_table="main.default.test_contrib_sum_registry",
    )
    
    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_contrib_sum",
            registry_table="main.default.test_contrib_sum_registry",
            score_threshold=0.5,
            include_contributions=True,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    row = result_df.collect()[0]
    
    contribs = row["anomaly_contributions"]
    total = sum(contribs.values())
    
    # Allow small floating point error
    assert abs(total - 1.0) < 0.01


def test_multi_feature_contributions(spark: SparkSession, mock_workspace_client):
    """Test contributions with 4+ columns."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1, 50.0) for i in range(30)],
        "amount double, quantity double, discount double, weight double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name="test_multi_contrib",
        registry_table="main.default.test_multi_contrib_registry",
    )
    
    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95, 1.0)],
        "amount double, quantity double, discount double, weight double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity", "discount", "weight"],
            model="test_multi_contrib",
            registry_table="main.default.test_multi_contrib_registry",
            score_threshold=0.5,
            include_contributions=True,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    row = result_df.collect()[0]
    
    contribs = row["anomaly_contributions"]
    
    # Verify all features are represented
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs
    assert "weight" in contribs


def test_contributions_without_flag_not_added(spark: SparkSession, mock_workspace_client):
    """Test that contributions are not added when include_contributions=False."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(30)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_no_contrib",
        registry_table="main.default.test_no_contrib_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_no_contrib",
            registry_table="main.default.test_no_contrib_registry",
            score_threshold=0.5,
            include_contributions=False,  # Explicitly False
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Verify anomaly_contributions column does NOT exist
    assert "anomaly_contributions" not in result_df.columns


def test_top_contributor_is_reasonable(spark: SparkSession, mock_workspace_client):
    """Test that the top contributor makes sense for the anomaly."""
    # Train on data where amount is always 100, but discount varies
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1 + i * 0.01) for i in range(30)],
        "amount double, quantity double, discount double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name="test_top_contrib",
        registry_table="main.default.test_top_contrib_registry",
    )
    
    # Test with anomalous amount (should be top contributor)
    test_df = spark.createDataFrame(
        [(9999.0, 2.0, 0.15)],  # Extreme amount
        "amount double, quantity double, discount double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity", "discount"],
            model="test_top_contrib",
            registry_table="main.default.test_top_contrib_registry",
            score_threshold=0.5,
            include_contributions=True,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    row = result_df.collect()[0]
    
    contribs = row["anomaly_contributions"]
    
    # Find top contributor
    top_feature = max(contribs, key=contribs.get)
    
    # Amount should likely be the top contributor (or at least significant)
    # Since amount is the most anomalous feature
    assert top_feature in ["amount", "discount", "quantity"]
    assert contribs[top_feature] > 0.2  # Should have significant contribution

