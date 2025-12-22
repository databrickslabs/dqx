"""
Shared session-scoped fixtures for anomaly detection integration tests.

These fixtures train models ONCE per test session and share them across multiple tests.

Usage:
    def test_my_feature(spark, shared_2d_model):
        model_name = shared_2d_model["model_name"]
        registry_table = shared_2d_model["registry_table"]
        # Use the pre-trained model...
"""

import logging
import os
from uuid import uuid4

import pytest

from tests.integration.test_anomaly_utils import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    get_standard_4d_training_data,
)


@pytest.fixture(scope="session")
def spark_session():
    """
    Session-scoped Spark fixture for shared model training.

    Creates a Databricks Connect session once per test session for use by
    shared model fixtures (shared_2d_model, etc.) without scope mismatch.

    Directly creates a DatabricksSession from environment variables without
    depending on the function-scoped ws fixture.
    """
    from databricks.connect import DatabricksSession  # pylint: disable=import-outside-toplevel

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    serverless_cluster_id = os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID")

    if serverless_cluster_id:
        logging.debug(f"Using serverless cluster id '{serverless_cluster_id}'")
        return DatabricksSession.builder.serverless(True).getOrCreate()

    logging.debug(f"Using cluster id '{cluster_id}'")
    # Build session from environment variables directly
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def shared_2d_model(spark_session, anomaly_engine):
    """
    Shared 2D anomaly model trained once per session.

    Used by: test_anomaly_dqengine.py, test_anomaly_e2e.py (some tests)

    Training data: 400 rows of (amount, quantity) with realistic variance
    Columns: ["amount", "quantity"]

    Returns:
        dict: {
            "model_name": str,
            "registry_table": str,
            "columns": list[str],
            "training_data": list[tuple],
        }
    """
    suffix = uuid4().hex[:8]
    model_name = f"shared_2d_model_{suffix}"
    registry_table = f"main.default.shared_2d_reg_{suffix}"

    train_df = spark_session.createDataFrame(get_standard_2d_training_data(), "amount double, quantity double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": model_name,
        "registry_table": registry_table,
        "columns": ["amount", "quantity"],
        "training_data": get_standard_2d_training_data(),
    }


@pytest.fixture(scope="session")
def shared_3d_model(spark_session, anomaly_engine):
    """
    Shared 3D anomaly model with contributions support.

    Used by: test_anomaly_explainability.py (4 tests)

    Training data: 400 rows of (amount, quantity, discount)
    Columns: ["amount", "quantity", "discount"]

    This model supports SHAP explainability features for computing
    feature contributions to anomaly scores.

    Returns:
        dict: {
            "model_name": str,
            "registry_table": str,
            "columns": list[str],
            "training_data": list[tuple],
        }
    """
    suffix = uuid4().hex[:8]
    model_name = f"shared_3d_model_{suffix}"
    registry_table = f"main.default.shared_3d_reg_{suffix}"

    train_df = spark_session.createDataFrame(
        get_standard_3d_training_data(), "amount double, quantity double, discount double"
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": model_name,
        "registry_table": registry_table,
        "columns": ["amount", "quantity", "discount"],
        "training_data": get_standard_3d_training_data(),
    }


@pytest.fixture(scope="session")
def shared_4d_model(spark_session, anomaly_engine):
    """
    Shared 4D anomaly model for multi-feature tests.

    Used by: test_anomaly_explainability.py (test_multi_feature_contributions)

    Training data: 400 rows of (amount, quantity, discount, weight)
    Columns: ["amount", "quantity", "discount", "weight"]

    Returns:
        dict: {
            "model_name": str,
            "registry_table": str,
            "columns": list[str],
            "training_data": list[tuple],
        }
    """
    suffix = uuid4().hex[:8]
    model_name = f"shared_4d_model_{suffix}"
    registry_table = f"main.default.shared_4d_reg_{suffix}"

    train_df = spark_session.createDataFrame(
        get_standard_4d_training_data(), "amount double, quantity double, discount double, weight double"
    )

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": model_name,
        "registry_table": registry_table,
        "columns": ["amount", "quantity", "discount", "weight"],
        "training_data": get_standard_4d_training_data(),
    }


@pytest.fixture(scope="session")
def shared_segmented_model(spark_session, anomaly_engine):
    """
    Shared segmented anomaly model for segment-based tests.

    Used by: test_anomaly_segments.py (multiple tests)

    Training data: 600 rows across 3 segments (US, EU, APAC)
    Segments: region in ["US", "EU", "APAC"]
    Columns: ["amount", "discount"]

    Creates 3 segment-specific models, one for each region.

    Returns:
        dict: {
            "base_model_name": str,
            "registry_table": str,
            "table_name": str,
            "segments": list[str],
            "columns": list[str],
        }
    """
    suffix = uuid4().hex[:8]

    # Generate multi-region data
    data = []
    for region in ("US", "EU", "APAC"):
        base = 100 if region == "US" else (200 if region == "EU" else 150)
        for i in range(200):
            data.append((region, base + i * 0.5, base * 0.8 + i * 0.3))

    df = spark_session.createDataFrame(data, "region string, amount double, discount double")
    table_name = f"main.default.shared_segment_data_{suffix}"
    df.write.mode("overwrite").saveAsTable(table_name)

    model_name = f"shared_segmented_{suffix}"
    registry_table = f"main.default.shared_segment_reg_{suffix}"

    # Train with segments
    anomaly_engine.train(
        df=spark_session.table(table_name),
        columns=["amount", "discount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "base_model_name": model_name,
        "registry_table": registry_table,
        "table_name": table_name,
        "segments": ["US", "EU", "APAC"],
        "columns": ["amount", "discount"],
    }
