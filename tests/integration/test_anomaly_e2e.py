"""Integration tests for anomaly detection end-to-end flow."""

from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.sdk import WorkspaceClient
from tests.conftest import TEST_CATALOG
from tests.integration.test_anomaly_utils import (
    get_standard_2d_training_data,
    get_standard_4d_training_data,
    get_recommended_threshold,
)


@pytest.fixture
def mock_workspace_client():
    """Create a mock WorkspaceClient for testing."""
    return MagicMock(spec=WorkspaceClient)


def test_basic_train_and_score(spark: SparkSession, mock_workspace_client, make_schema, make_random, anomaly_engine):
    """Test basic training and scoring workflow."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_basic_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data for consistent results
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    model_uri = anomaly_engine.train(
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
        [
            (150.0, 15.0),  # Normal - in dense part of training range
            (9999.0, 100.0),  # Clear anomaly - far outside
        ],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    threshold = get_recommended_threshold("standard")
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": threshold,
            },
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)
    errors = result_df.select("_errors").collect()

    # First row (normal) should pass, second row (anomaly) should fail
    # Handle both [] and None for empty errors
    first_errors = errors[0]["_errors"]
    assert first_errors == [] or first_errors is None, f"Normal row has errors: {first_errors}"

    second_errors = errors[1]["_errors"]
    assert second_errors is not None and len(second_errors) > 0, "Anomalous row should have errors"


def test_anomaly_scores_are_added(spark: SparkSession, mock_workspace_client, make_schema, make_random, anomaly_engine):
    """Test that anomaly scores are added to the DataFrame."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_scores_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Test with normal and anomalous data
    test_df = spark.createDataFrame(
        [
            (150.0, 15.0),  # Normal - in dense part of training range
            (9999.0, 1.0),  # Anomaly - far outside
        ],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    threshold = get_recommended_threshold("standard")
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": threshold,
            },
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify anomaly_score column exists
    assert "anomaly_score" in result_df.columns

    # Verify scores are present
    rows = result_df.select("anomaly_score").collect()
    assert all(row["anomaly_score"] is not None for row in rows)


def test_auto_derivation_of_names(spark: SparkSession, mock_workspace_client, anomaly_engine):
    """Test that registry_table is auto-derived when omitted."""
    # Use standard 2D training data
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    # Train with model_name but without registry_table (auto-derived)
    model_uri = anomaly_engine.train(
        df=train_df,
        model_name="test_auto_model",
        columns=["amount", "quantity"],
    )

    # Model URI should still be returned
    assert model_uri is not None

    # Score with normal data (within training distribution) without explicit names
    test_df = spark.createDataFrame(
        [(200.0, 30.0)],  # Center of training range (100-300, 10-50)
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    threshold = get_recommended_threshold("permissive")  # Higher threshold since using auto-derived names
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "score_threshold": threshold,
            },
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Should succeed without errors - the auto-derived names allow scoring to work
    errors_row = result_df.select("_errors").first()
    assert errors_row is not None, "Failed to get errors column"
    errors = errors_row["_errors"]
    assert errors is None or len(errors) == 0, f"Expected no errors, got: {errors}"


def test_threshold_flagging(spark: SparkSession, mock_workspace_client, make_schema, make_random, anomaly_engine):
    """Test that anomalous rows are flagged based on score_threshold."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_threshold_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Create test data with clear normal and anomalous rows
    test_df = spark.createDataFrame(
        [
            (200.0, 30.0),  # Normal - center of training range (100-300, 10-50)
            (210.0, 32.0),  # Normal - center of training range
            (9999.0, 0.1),  # Anomaly - far out
        ],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    threshold = get_recommended_threshold("standard")
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": threshold,
            },
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)
    result_df = result_df.collect()

    # Normal rows should have no errors (handle both [] and None)
    first_errors = result_df[0]["_errors"]
    assert first_errors == [] or first_errors is None, f"First normal row has errors: {first_errors}"

    second_errors = result_df[1]["_errors"]
    assert second_errors == [] or second_errors is None, f"Second normal row has errors: {second_errors}"

    # Anomalous row should have errors
    anomaly_errors = result_df[2]["_errors"]
    assert anomaly_errors is not None and len(anomaly_errors) > 0, "Anomalous row should have errors"


def test_registry_table_auto_creation(spark: SparkSession, make_schema, make_random, anomaly_engine):
    """Test that registry table is auto-created if missing."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_auto_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 2D training data
    train_df = spark.createDataFrame(
        get_standard_2d_training_data(),
        "amount double, quantity double",
    )

    # Drop table if exists
    spark.sql(f"DROP TABLE IF EXISTS {registry_table}")

    # Verify table doesn't exist (Unity Catalog compatible)
    table_exists = False
    try:
        spark.table(registry_table).limit(0).count()
        table_exists = True
    except Exception:  # noqa: BLE001
        pass
    assert not table_exists

    # Train (should auto-create registry)
    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Verify table now exists (Unity Catalog compatible)
    table_exists = False
    try:
        spark.table(registry_table).limit(0).count()
        table_exists = True
    except Exception:  # noqa: BLE001
        pass
    assert table_exists

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


def test_multiple_columns(spark: SparkSession, mock_workspace_client, make_schema, make_random, anomaly_engine):
    """Test training and scoring with multiple columns."""
    # Create unique schema and table names for test isolation
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)
    model_name = f"test_multi_{make_random(4).lower()}"
    registry_table = f"{catalog_name}.{schema.name}.{make_random(8).lower()}_registry"

    # Use standard 4D training data
    train_df = spark.createDataFrame(
        get_standard_4d_training_data(),
        "amount double, quantity double, discount double, weight double",
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name=model_name,
        registry_table=registry_table,
    )

    test_df = spark.createDataFrame(
        [(200.0, 30.0, 0.25, 90.0), (9999.0, 1.0, 0.95, 1.0)],  # First in center, second far out
        "amount double, quantity double, discount double, weight double",
    )

    dq_engine = DQEngine(mock_workspace_client)
    threshold = get_recommended_threshold("standard")
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_anomalies,
            check_func_kwargs={
                "columns": ["amount", "quantity", "discount", "weight"],
                "model": model_name,
                "registry_table": registry_table,
                "score_threshold": threshold,
            },
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    assert "anomaly_score" in result_df.columns
    errors = result_df.select("_errors").collect()

    # First row normal, second row anomalous (handle both [] and None)
    first_errors = errors[0]["_errors"]
    assert first_errors == [] or first_errors is None, f"Normal row has errors: {first_errors}"

    second_errors = errors[1]["_errors"]
    assert second_errors is not None and len(second_errors) > 0, "Anomalous row should have errors"
