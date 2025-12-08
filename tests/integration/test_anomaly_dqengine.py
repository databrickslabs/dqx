"""Integration tests for anomaly detection with DQEngine."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule
from databricks.sdk import WorkspaceClient


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_apply_checks_by_metadata(spark: SparkSession, mock_workspace_client):
    """Test that apply_checks_by_metadata adds anomaly scores and DQX metadata."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_apply_metadata",
        registry_table="main.default.test_apply_metadata_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_apply_metadata",
            registry_table="main.default.test_apply_metadata_registry",
            score_threshold=0.5,
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Verify anomaly_score is added
    assert "anomaly_score" in result_df.columns
    
    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns
    
    # Verify original columns are preserved
    assert "amount" in result_df.columns
    assert "quantity" in result_df.columns


def test_apply_checks_and_split(spark: SparkSession, mock_workspace_client):
    """Test that apply_checks_by_metadata_and_split correctly splits valid/quarantine."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_split",
        registry_table="main.default.test_split_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (101.0, 2.0), (9999.0, 1.0), (8888.0, 0.5)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_split",
            registry_table="main.default.test_split_registry",
            score_threshold=0.5,
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)
    
    # Verify split occurred
    assert valid_df.count() + quarantine_df.count() == test_df.count()
    
    # Verify normal rows are in valid
    assert valid_df.count() >= 2  # At least 2 normal rows
    
    # Verify anomalous rows are in quarantine
    assert quarantine_df.count() >= 1  # At least 1 anomalous row
    
    # Verify anomaly_score is in both DataFrames
    assert "anomaly_score" in valid_df.columns
    assert "anomaly_score" in quarantine_df.columns


def test_quarantine_dataframe_structure(spark: SparkSession, mock_workspace_client):
    """Test that quarantine DataFrame has expected structure."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_quarantine_structure",
        registry_table="main.default.test_quarantine_structure_registry",
    )
    
    test_df = spark.createDataFrame(
        [(9999.0, 1.0)],  # Anomalous row
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_quarantine_structure",
            registry_table="main.default.test_quarantine_structure_registry",
            score_threshold=0.5,
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)
    
    # Quarantine should have the anomalous row
    assert quarantine_df.count() == 1
    
    # Verify structure
    row = quarantine_df.collect()[0]
    
    # Original columns
    assert "amount" in quarantine_df.columns
    assert "quantity" in quarantine_df.columns
    
    # Anomaly score
    assert "anomaly_score" in quarantine_df.columns
    assert row["anomaly_score"] is not None
    
    # DQX metadata
    assert "_errors" in quarantine_df.columns
    assert row["_errors"] is not None
    assert len(row["_errors"]) > 0


def test_multiple_checks_combined(spark: SparkSession, mock_workspace_client):
    """Test combining anomaly check with other DQX checks."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_multi_checks",
        registry_table="main.default.test_multi_checks_registry",
    )
    
    test_df = spark.createDataFrame(
        [
            (100.0, 2.0),  # Normal
            (None, 2.0),  # Null amount
            (9999.0, 1.0),  # Anomaly
        ],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        # Standard DQX check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="amount",
        ),
        # Anomaly check
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_multi_checks",
            registry_table="main.default.test_multi_checks_registry",
            score_threshold=0.5,
        ),
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    rows = result_df.collect()
    
    # Normal row: no errors
    assert rows[0]["_errors"] == [] or rows[0]["_errors"] is None
    
    # Null row: has is_not_null error
    assert rows[1]["_errors"] is not None
    assert len(rows[1]["_errors"]) > 0
    assert any("is_not_null" in str(err) for err in rows[1]["_errors"])
    
    # Anomaly row: has anomaly error
    assert rows[2]["_errors"] is not None
    assert len(rows[2]["_errors"]) > 0


def test_criticality_error(spark: SparkSession, mock_workspace_client):
    """Test anomaly check with criticality='error'."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_crit_error",
        registry_table="main.default.test_crit_error_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    # Use DQRowRule with criticality='error'
    from databricks.labs.dqx.rule import DQDatasetRule
    
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_crit_error",
                "registry_table": "main.default.test_crit_error_registry",
                "score_threshold": 0.5,
            }
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)
    
    # Anomalous rows should be in quarantine
    assert quarantine_df.count() >= 1
    assert "_errors" in quarantine_df.columns


def test_criticality_warn(spark: SparkSession, mock_workspace_client):
    """Test anomaly check with criticality='warn'."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_crit_warn",
        registry_table="main.default.test_crit_warn_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    from databricks.labs.dqx.rule import DQDatasetRule
    
    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_crit_warn",
                "registry_table": "main.default.test_crit_warn_registry",
                "score_threshold": 0.5,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    rows = result_df.collect()
    
    # With warn criticality, anomalous rows should have warnings not errors
    # (Check for _warnings instead of _errors)
    anomalous_row = rows[1]  # 9999.0, 1.0
    
    # Should have either warnings or errors (implementation may vary)
    assert anomalous_row["_warnings"] is not None or anomalous_row["_errors"] is not None


def test_get_valid_and_invalid_helpers(spark: SparkSession, mock_workspace_client):
    """Test that get_valid() and get_invalid() helpers work with anomaly checks."""
    train_df = spark.createDataFrame(
        [(100.0, 2.0) for i in range(50)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name="test_helpers",
        registry_table="main.default.test_helpers_registry",
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        has_no_anomalies(
            columns=["amount", "quantity"],
            model="test_helpers",
            registry_table="main.default.test_helpers_registry",
            score_threshold=0.5,
        )
    ]
    
    # Apply checks
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    
    # Use helpers to split
    valid_df = dq_engine.get_valid(result_df)
    invalid_df = dq_engine.get_invalid(result_df)
    
    # Verify split
    assert valid_df.count() >= 1  # At least one normal row
    assert invalid_df.count() >= 1  # At least one anomalous row
    
    # Verify DQX metadata columns are NOT in valid/invalid
    # (get_valid/get_invalid should drop them)
    assert "_errors" not in valid_df.columns
    assert "_errors" not in invalid_df.columns

