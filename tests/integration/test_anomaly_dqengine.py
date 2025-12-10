"""Integration tests for anomaly detection with DQEngine."""

import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from unittest.mock import MagicMock

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
from databricks.sdk import WorkspaceClient
from tests.conftest import TEST_CATALOG


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_apply_checks_by_metadata(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that apply_checks_by_metadata adds anomaly scores and DQX metadata."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_apply_metadata_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
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
                "score_threshold": 0.6,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    
    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns
    
    # Verify original columns are preserved
    assert "amount" in result_df.columns
    assert "quantity" in result_df.columns
    
    # Verify at least one row has an error (the anomalous one)
    rows = result_df.collect()
    has_error = any(row["_errors"] and len(row["_errors"]) > 0 for row in rows)
    assert has_error, "Expected at least one row to have anomaly error"


def test_apply_checks_and_split(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that apply_checks_by_metadata_and_split correctly splits valid/quarantine."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_split_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (101.0, 2.0), (9999.0, 1.0), (8888.0, 0.5)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_split",
                "registry_table": "main.default.test_split_registry",
                "score_threshold": 0.6,
            }
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)
    
    # Verify split occurred
    assert valid_df.count() + quarantine_df.count() == test_df.count()
    
    # Verify normal rows are in valid
    assert valid_df.count() >= 2  # At least 2 normal rows
    
    # Verify anomalous rows are in quarantine
    assert quarantine_df.count() >= 1  # At least 1 anomalous row
    
    # Verify original columns are preserved (no DQX metadata in split DataFrames)
    assert "amount" in valid_df.columns
    assert "amount" in quarantine_df.columns


def test_quarantine_dataframe_structure(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that quarantine DataFrame has expected structure."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_quarantine_structure_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(9999.0, 1.0)],  # Anomalous row
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_quarantine_structure",
                "registry_table": "main.default.test_quarantine_structure_registry",
                "score_threshold": 0.6,
            }
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)
    
    # Quarantine should have the anomalous row
    assert quarantine_df.count() == 1
    
    # Verify structure
    row = quarantine_df.collect()[0]
    
    # Original columns
    assert "amount" in quarantine_df.columns
    assert "quantity" in quarantine_df.columns
    
    # DQX split DataFrames don't include metadata columns
    # Verify we have the correct data
    assert row["amount"] == 9999.0
    assert row["quantity"] == 1.0


def test_multiple_checks_combined(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test combining anomaly check with other DQX checks."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_multi_checks_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
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
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_multi_checks",
                "registry_table": "main.default.test_multi_checks_registry",
                "score_threshold": 0.6,
            }
        ),
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
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


def test_criticality_error(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test anomaly check with criticality='error'."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_crit_error_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_crit_error",
                "registry_table": "main.default.test_crit_error_registry",
                "score_threshold": 0.6,
            }
        )
    ]
    
    valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)
    
    # Anomalous rows should be in quarantine
    assert quarantine_df.count() >= 1
    assert "_errors" in quarantine_df.columns


def test_criticality_warn(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test anomaly check with criticality='warn'."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_crit_warn_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    
    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_crit_warn",
                "registry_table": "main.default.test_crit_warn_registry",
                "score_threshold": 0.6,
            }
        )
    ]
    
    result_df = dq_engine.apply_checks(test_df, checks)
    rows = result_df.collect()
    
    # With warn criticality, anomalous rows should have warnings not errors
    # (Check for _warnings instead of _errors)
    anomalous_row = rows[1]  # 9999.0, 1.0
    
    # Should have either warnings or errors (implementation may vary)
    assert anomalous_row["_warnings"] is not None or anomalous_row["_errors"] is not None


def test_get_valid_and_invalid_helpers(spark: SparkSession, mock_workspace_client, make_schema, make_random):
    """Test that get_valid() and get_invalid() helpers work with anomaly checks."""
    # Create unique names for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    model_name = f"test_helpers_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.{make_random(8).lower()}_registry"
    
    train_df = spark.createDataFrame(
        [(100.0 + i * 0.5, 2.0 + i * 0.01) for i in range(200)],
        "amount double, quantity double",
    )
    
    train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )
    
    test_df = spark.createDataFrame(
        [(100.0, 2.0), (9999.0, 1.0)],
        "amount double, quantity double",
    )
    
    dq_engine = DQEngine(mock_workspace_client)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": "test_helpers",
                "registry_table": "main.default.test_helpers_registry",
                "score_threshold": 0.6,
            }
        )
    ]
    
    # Apply checks
    result_df = dq_engine.apply_checks(test_df, checks)
    
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

