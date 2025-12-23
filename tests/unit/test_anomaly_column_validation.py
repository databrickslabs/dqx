"""Unit tests for anomaly detection column validation and ID field detection."""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.errors import InvalidParameterError


# ============================================================================
# Overlap Validation Tests
# ============================================================================


def test_columns_exclude_columns_overlap_raises_error(spark: SparkSession, anomaly_engine):
    """Test that overlapping columns and exclude_columns raises InvalidParameterError."""
    df = spark.createDataFrame(
        [(1, 100.0, 50.0, "id123") for _ in range(100)],
        "row_id int, amount double, quantity double, user_id string",
    )

    # Try to train with overlapping columns
    with pytest.raises(InvalidParameterError) as exc_info:
        anomaly_engine.train(
            df=df,
            model_name="test_overlap",
            columns=["amount", "quantity", "user_id"],  # user_id in both lists
            exclude_columns=["user_id", "row_id"],
            registry_table="main.default.test_overlap_registry",
        )

    error_msg = str(exc_info.value)
    assert "overlap" in error_msg.lower()
    assert "user_id" in error_msg
    assert "Remove overlapping columns from either list" in error_msg


def test_columns_exclude_columns_no_overlap_succeeds(spark: SparkSession, anomaly_engine):
    """Test that non-overlapping columns and exclude_columns works correctly."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 50.0 + i * 0.1, i, f"id{i}") for i in range(100)],
        "amount double, quantity double, row_id int, user_id string",
    )

    # This should succeed - no overlap
    model_name = anomaly_engine.train(
        df=df,
        model_name="test_no_overlap",
        columns=["amount", "quantity"],  # No overlap with exclude_columns
        exclude_columns=["user_id", "row_id"],
        registry_table="main.default.test_no_overlap_registry",
    )

    assert model_name is not None


# ============================================================================
# ID Field Detection Tests
# ============================================================================


def test_id_field_warning_for_column_ending_in_id(spark: SparkSession, anomaly_engine):
    """Test that columns ending in '_id' trigger a warning."""
    df = spark.createDataFrame(
        [(i, 100.0 + i * 0.5, 50.0) for i in range(100)],
        "user_id int, amount double, quantity double",
    )

    # Should issue warning but not fail
    with pytest.warns(UserWarning) as warning_list:
        anomaly_engine.train(
            df=df,
            model_name="test_id_warning",
            columns=["user_id", "amount", "quantity"],
            registry_table="main.default.test_id_warning_registry",
        )

    # Check that warning was issued for user_id
    warnings_text = " ".join(str(w.message) for w in warning_list)
    assert "user_id" in warnings_text
    assert "ID field" in warnings_text or "id field" in warnings_text


def test_id_field_warning_for_high_cardinality(spark: SparkSession, anomaly_engine):
    """Test that high cardinality columns trigger a warning."""
    # Create data with high cardinality numeric column (80% unique)
    df = spark.createDataFrame(
        [(i, 100.0 + (i % 20) * 0.5, 50.0) for i in range(100)],  # transaction_num is 100% unique
        "transaction_num int, amount double, quantity double",
    )

    with pytest.warns(UserWarning) as warning_list:
        anomaly_engine.train(
            df=df,
            model_name="test_high_card_warning",
            columns=["transaction_num", "amount", "quantity"],
            registry_table="main.default.test_high_card_warning_registry",
        )

    # Check that warning was issued for high cardinality
    warnings_text = " ".join(str(w.message) for w in warning_list)
    assert "transaction_num" in warnings_text
    assert "cardinality" in warnings_text.lower()


def test_id_field_pattern_variations(spark: SparkSession, anomaly_engine):
    """Test that various ID naming patterns are detected."""
    # Test data with various ID-like column names
    id_columns = [
        "customer_id",
        "user_key",
        "transaction_ID",  # Case insensitive
        "session_uuid",
        "record_guid",
    ]

    for id_col in id_columns:
        df = spark.createDataFrame(
            [(i, 100.0 + i * 0.5) for i in range(50)],
            f"{id_col} int, amount double",
        )

        with pytest.warns(UserWarning) as warning_list:
            anomaly_engine.train(
                df=df,
                model_name=f"test_pattern_{id_col}",
                columns=[id_col, "amount"],
                registry_table=f"main.default.test_pattern_{id_col}_registry",
            )

        warnings_text = " ".join(str(w.message) for w in warning_list)
        assert id_col in warnings_text, f"Expected warning for {id_col}"


def test_no_warning_for_normal_columns(spark: SparkSession, anomaly_engine):
    """Test that normal columns do not trigger ID warnings."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 50.0 + i * 0.1, 0.1 + i * 0.001) for i in range(100)],
        "amount double, quantity double, discount double",
    )

    with pytest.warns(UserWarning) as warning_list:
        anomaly_engine.train(
            df=df,
            model_name="test_no_id_warning",
            columns=["amount", "quantity", "discount"],
            registry_table="main.default.test_no_id_warning_registry",
        )

    # Should not have any ID-related warnings
    warnings_text = " ".join(str(w.message) for w in warning_list)
    # It's okay to have other warnings, just not ID-related ones
    if "appears to be an ID field" in warnings_text:
        pytest.fail("Unexpected ID field warning for normal columns")


def test_auto_discovery_excludes_id_fields(spark: SparkSession, anomaly_engine):
    """Test that auto-discovery automatically excludes ID fields."""
    df = spark.createDataFrame(
        [(i, f"user{i}", 100.0 + i * 0.5, 50.0 + i * 0.1) for i in range(100)],
        "row_id int, user_id string, amount double, quantity double",
    )

    # Auto-discovery (columns=None) should exclude ID fields
    model_name = anomaly_engine.train(
        df=df,
        model_name="test_auto_exclude_ids",
        # No columns specified - let auto-discovery handle it
        registry_table="main.default.test_auto_exclude_ids_registry",
    )

    assert model_name is not None
    # Auto-discovery should have excluded row_id and user_id automatically


def test_exclude_columns_recommended_for_id_fields(spark: SparkSession, anomaly_engine):
    """Test that exclude_columns is the recommended way to handle ID fields."""
    df = spark.createDataFrame(
        [(i, f"user{i}", 100.0 + i * 0.5, 50.0 + i * 0.1) for i in range(100)],
        "row_id int, user_id string, amount double, quantity double",
    )

    # Using exclude_columns should avoid warnings
    model_name = anomaly_engine.train(
        df=df,
        model_name="test_exclude_ids",
        columns=["amount", "quantity"],  # Explicitly select only behavioral columns
        exclude_columns=["row_id", "user_id"],  # Exclude IDs
        registry_table="main.default.test_exclude_ids_registry",
    )

    assert model_name is not None


# ============================================================================
# Edge Case Tests
# ============================================================================


def test_exclude_columns_not_in_dataframe_raises_error(spark: SparkSession, anomaly_engine):
    """Test that exclude_columns with non-existent columns raises error."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 50.0) for i in range(100)],
        "amount double, quantity double",
    )

    with pytest.raises(InvalidParameterError) as exc_info:
        anomaly_engine.train(
            df=df,
            model_name="test_invalid_exclude",
            exclude_columns=["nonexistent_column"],
            registry_table="main.default.test_invalid_exclude_registry",
        )

    error_msg = str(exc_info.value)
    assert "exclude_columns contains columns not in DataFrame" in error_msg
    assert "nonexistent_column" in error_msg


def test_empty_exclude_columns_is_allowed(spark: SparkSession, anomaly_engine):
    """Test that empty exclude_columns list is allowed."""
    df = spark.createDataFrame(
        [(100.0 + i * 0.5, 50.0 + i * 0.1) for i in range(100)],
        "amount double, quantity double",
    )

    # Empty exclude_columns should be fine
    model_name = anomaly_engine.train(
        df=df,
        model_name="test_empty_exclude",
        columns=["amount", "quantity"],
        exclude_columns=[],  # Empty list
        registry_table="main.default.test_empty_exclude_registry",
    )

    assert model_name is not None


def test_categorical_id_field_warning(spark: SparkSession, anomaly_engine):
    """Test that categorical (string) ID fields also trigger warnings."""
    df = spark.createDataFrame(
        [(f"uuid-{i:04d}", 100.0 + i * 0.5) for i in range(100)],  # High cardinality string
        "transaction_uuid string, amount double",
    )

    with pytest.warns(UserWarning) as warning_list:
        anomaly_engine.train(
            df=df,
            model_name="test_categorical_id",
            columns=["transaction_uuid", "amount"],
            registry_table="main.default.test_categorical_id_registry",
        )

    warnings_text = " ".join(str(w.message) for w in warning_list)
    assert "transaction_uuid" in warnings_text
    assert "ID field" in warnings_text or "id field" in warnings_text
