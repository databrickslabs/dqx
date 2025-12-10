"""Integration tests for anomaly detection end-to-end flow."""

import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.sdk import WorkspaceClient
from tests.conftest import TEST_CATALOG


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_basic_train_and_score(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test basic training and scoring workflow."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_basic_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"
    
    # Train on normal data (more data for stable model)
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.1, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    model_uri = train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Verify model URI is returned
    assert model_uri is not None
    assert "models:/" in model_uri or "runs:/" in model_uri
    
    # Score normal + anomalous data
    test_df = spark.createDataFrame(
        [(110.0, 2.5), (9999.0, 100.0)],  # First is clearly in range, second is clearly out
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": 0.7,  # Higher threshold to reduce false positives
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    errors = result_df.select("_errors").collect()
    
    # First row (normal) should pass, second row (anomaly) should fail
    assert errors[0]["_errors"] == [] or errors[0]["_errors"] is None
    assert len(errors[1]["_errors"]) > 0


def test_anomaly_scores_are_added(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that anomaly scores are added to the DataFrame."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_scores_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (500.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": 0.5,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    
    # Verify anomaly_score column exists
    assert "anomaly_score" in result_df.columns
    
    # Verify scores are present
    rows = result_df.select("anomaly_score").collect()
    assert all(row["anomaly_score"] is not None for row in rows)


def test_auto_derivation_of_names(spark: SparkSession, mock_workspace_client):
    """Test that model_name and registry_table are auto-derived when omitted."""
    # Train with varied data
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(100)],
        "amount double, quantity double",
    )
    
    # Train without explicit names
    model_uri = train(
        df=train_df,
        columns=["amount", "quantity"],
    )
    
    # Model URI should still be returned
    assert model_uri is not None
    
    # Score with normal data (within training distribution) without explicit names
    test_df = spark.createDataFrame(
        [(125.0, 2.5)],  # Mid-range value from training data
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "score_threshold": 0.7,  # Higher threshold to avoid false positives
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    
    # Should succeed without errors - the auto-derived names allow scoring to work
    errors = result_df.select("_errors").first()["_errors"]
    assert errors is None or len(errors) == 0, f"Expected no errors, got: {errors}"


def test_threshold_flagging(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that anomalous rows are flagged based on score_threshold."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_threshold_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Create test data with clear normal and anomalous rows
    test_df = spark.createDataFrame(
        [
            (100.0, 2.0),  # Normal
            (101.0, 2.0),  # Normal
            (9999.0, 0.1),  # Anomaly
        ],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": 0.5,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    result_df = result_df.collect()
    
    # Normal rows should have no errors
    assert result_df[0]["_errors"] == [] or result_df[0]["_errors"] is None
    assert result_df[1]["_errors"] == [] or result_df[1]["_errors"] is None
    
    # Anomalous row should have errors
    assert result_df[2]["_errors"] is not None
    assert len(result_df[2]["_errors"]) > 0


def test_registry_table_auto_creation(spark: SparkSession, make_schema, make_random):
    """Test that registry table is auto-created if missing."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_auto_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {registry_table}")
    
    # Verify table doesn't exist
    assert not spark.catalog.tableExists(registry_table)
    
    # Train (should auto-create registry)
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    # Verify table now exists
    assert spark.catalog.tableExists(registry_table)
    
    # Verify table has expected schema
    registry_df = spark.table(registry_table)
    expected_columns = [
        "model_name",
        "model_uri",
        "columns",
        "algorithm",
        "training_time",
        "status",
        "baseline_stats",
        "feature_importance",
    ]
    
    for col in expected_columns:
        assert col in registry_df.columns


def test_multiple_columns(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test training and scoring with multiple columns."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_multi_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1, 50.0) for i in range(50)],
        "amount double, quantity double, discount double, weight double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0, 0.1, 50.0), (9999.0, 1.0, 0.95, 1.0)],
        "amount double, quantity double, discount double, weight double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity", "discount", "weight"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": 0.5,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    
    assert "anomaly_score" in result_df.columns
    errors = result_df.select("_errors").collect()
    
    # First row normal, second row anomalous
    assert errors[0]["_errors"] == [] or errors[0]["_errors"] is None
    assert len(errors[1]["_errors"]) > 0

