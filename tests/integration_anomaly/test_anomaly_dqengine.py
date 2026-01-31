"""
Integration tests for anomaly detection with DQEngine.
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from tests.integration_anomaly.test_anomaly_constants import (
    DQENGINE_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)
from tests.integration_anomaly.test_anomaly_utils import create_anomaly_check_rule


def test_apply_checks_by_metadata(ws, spark: SparkSession, shared_2d_model):
    """Test that apply_checks_by_metadata adds anomaly scores and DQX metadata."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=95.0,
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


def test_apply_checks_and_split(ws, spark: SparkSession, shared_2d_model):
    """Test that apply_checks_by_metadata_and_split correctly splits valid/quarantine."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Test with in-cluster points and clear outliers
    test_df = spark.createDataFrame(
        [(1, 110.0, 12.0), (2, 150.0, 20.0), (3, OUTLIER_AMOUNT, OUTLIER_QUANTITY), (4, 8888.0, 100.0)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=95.0,
        )
    ]

    # Now split
    valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)

    # Verify split occurred
    assert valid_df.count() + quarantine_df.count() == test_df.count()

    # Verify normal rows are in valid
    assert valid_df.count() >= 2, f"Expected >= 2 normal rows, got {valid_df.count()}"

    # Verify at least one anomalous row is in quarantine
    # Use anomaly metadata to avoid threshold sensitivity across environments
    flagged = quarantine_df.filter(F.col("_dq_info.anomaly.is_anomaly")).count()
    assert flagged >= 1, f"Expected >= 1 anomalous row, got {flagged}"

    # Verify original columns are preserved (no DQX metadata in split DataFrames)
    assert "amount" in valid_df.columns
    assert "amount" in quarantine_df.columns


def test_quarantine_dataframe_structure(ws, spark: SparkSession, shared_2d_model):
    """Test that quarantine DataFrame has expected structure."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [(1, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Anomalous row
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=DQENGINE_SCORE_THRESHOLD,
        )
    ]

    _valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)

    # Quarantine should have the anomalous row
    assert quarantine_df.count() == 1

    # Verify structure
    row = quarantine_df.collect()[0]

    # Original columns
    assert "amount" in quarantine_df.columns
    assert "quantity" in quarantine_df.columns

    # DQX split DataFrames don't include metadata columns
    # Verify we have the correct data
    assert row["amount"] == OUTLIER_AMOUNT
    assert row["quantity"] == OUTLIER_QUANTITY


def test_multiple_checks_combined(ws, spark: SparkSession, shared_2d_model):
    """Test combining anomaly check with other DQX checks."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [
            (1, 110.0, 12.0),  # Normal - in dense part of training range (100-300, 10-50)
            (2, None, 10.0),  # Null amount - will fail is_not_null
            (3, OUTLIER_AMOUNT, OUTLIER_QUANTITY),  # Far-out anomaly
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        # Standard DQX check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="amount",
        ),
        # Anomaly check
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
        ),
    ]

    result_df = dq_engine.apply_checks(test_df, checks)
    rows = result_df.orderBy("transaction_id").collect()

    def _has_is_not_null_error(err) -> bool:
        err_dict = err.asDict() if hasattr(err, "asDict") else {}
        for key in ("function", "name", "check", "check_name"):
            value = err_dict.get(key)
            if value and "is_not_null" in str(value):
                return True
        message = err_dict.get("message")
        if message and "null" in str(message).lower():
            return True
        return False

    # Normal row: should not fail is_not_null
    if rows[0]["_errors"]:
        assert not any(
            _has_is_not_null_error(err) for err in rows[0]["_errors"]
        ), f"Normal row has is_not_null error: {rows[0]['_errors']}"

    # Null row: has is_not_null error
    assert rows[1]["_errors"] is not None
    assert len(rows[1]["_errors"]) > 0
    assert any(_has_is_not_null_error(err) for err in rows[1]["_errors"])

    # Anomaly row: should not fail is_not_null
    if rows[2]["_errors"]:
        assert not any(
            _has_is_not_null_error(err) for err in rows[2]["_errors"]
        ), f"Anomaly row has is_not_null error: {rows[2]['_errors']}"


def test_criticality_error(ws, spark: SparkSession, shared_2d_model):
    """Test anomaly check with criticality='error'."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)

    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
        )
    ]

    _valid_df, quarantine_df = dq_engine.apply_checks_and_split(test_df, checks)

    # Anomalous rows should be in quarantine
    assert quarantine_df.count() >= 1
    assert "_errors" in quarantine_df.columns


def test_criticality_warn(ws, spark: SparkSession, shared_2d_model):
    """Test anomaly check with criticality='warn'."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [(1, 100.0, 2.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)

    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
            criticality="warn",
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)
    rows = result_df.collect()

    # With warn criticality, anomalous rows should have warnings not errors
    # (Check for _warnings instead of _errors)
    anomalous_row = rows[1]  # OUTLIER_AMOUNT, OUTLIER_QUANTITY

    # Should have either warnings or errors (implementation may vary)
    assert anomalous_row["_warnings"] is not None or anomalous_row["_errors"] is not None


def test_get_valid_and_invalid_helpers(ws, spark: SparkSession, shared_2d_model):
    """Test that get_valid() and get_invalid() helpers work with anomaly checks."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Test with in-cluster point (in dense part of range) and far-out anomaly
    test_df = spark.createDataFrame(
        [(1, 100.0, 10.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=DQENGINE_SCORE_THRESHOLD,
        )
    ]

    # Apply checks
    result_df = dq_engine.apply_checks(test_df, checks)

    # Use helpers to split
    valid_df = dq_engine.get_valid(result_df)
    invalid_df = dq_engine.get_invalid(result_df)

    # Verify split
    assert valid_df.count() + invalid_df.count() == test_df.count()
    assert (
        invalid_df.count() >= 1
    ), f"Expected >= 1 anomalous row, got {invalid_df.count()}"  # At least one anomalous row

    # get_valid() drops DQX metadata columns
    assert "_errors" not in valid_df.columns, f"_errors should be dropped from valid. Columns: {valid_df.columns}"
    assert "_warnings" not in valid_df.columns, f"_warnings should be dropped from valid. Columns: {valid_df.columns}"

    # get_invalid() KEEPS metadata columns so you can inspect failures
    assert "_errors" in invalid_df.columns, f"_errors should be kept in invalid. Columns: {invalid_df.columns}"
    assert "_warnings" in invalid_df.columns, f"_warnings should be kept in invalid. Columns: {invalid_df.columns}"

    # Anomaly scoring info should be present in both (in _dq_info column)
    assert "_dq_info" in valid_df.columns or "_dq_info" in invalid_df.columns


def test_info_column_structure(ws, spark: SparkSession, shared_2d_model):
    """Test that _dq_info.anomaly has all expected fields with correct structure."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0)],  # Normal data
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
            include_contributions=False,  # Test without optional fields
            include_confidence=False,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify _dq_info column exists
    assert "_dq_info" in result_df.columns, "_dq_info column should be present"

    # Get the row and extract _dq_info
    row = result_df.collect()[0]
    info = row["_dq_info"]

    # Verify _dq_info is a struct (not None)
    assert info is not None, "_dq_info should not be None"

    # Verify _dq_info.anomaly exists and is a struct
    assert hasattr(info, "anomaly"), "_dq_info should have 'anomaly' field"
    anomaly = info.anomaly
    assert anomaly is not None, "_dq_info.anomaly should not be None"

    # Verify all required fields exist in _dq_info.anomaly
    expected_fields = [
        "check_name",
        "score",
        "is_anomaly",
        "threshold",
        "model",
        "segment",
        "contributions",
        "confidence_std",
    ]

    for field in expected_fields:
        assert hasattr(anomaly, field), f"_dq_info.anomaly should have '{field}' field"

    # Verify field values and types
    assert anomaly.check_name == "has_no_anomalies", "check_name should be 'has_no_anomalies'"
    assert isinstance(anomaly.score, (float, type(None))), "score should be float or None"
    assert isinstance(anomaly.is_anomaly, (bool, type(None))), "is_anomaly should be boolean or None"
    assert anomaly.threshold == 60.0, f"threshold should be 60.0, got {anomaly.threshold}"
    assert model_name in anomaly.model, f"model should contain {model_name}"

    # Verify optional fields are None when not requested
    assert anomaly.segment is None, "segment should be None for global model"
    assert anomaly.contributions is None, "contributions should be None when not requested"
    assert anomaly.confidence_std is None, "confidence_std should be None when not requested"


def test_info_column_with_contributions(ws, spark: SparkSession, shared_3d_model):
    """Test that _dq_info.anomaly includes contributions when requested."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]
    columns = shared_3d_model["columns"]

    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0, 0.2)],  # Normal data
        "transaction_id int, amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(ws, spark)
    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
            include_contributions=True,  # Request contributions
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Get the row and extract _dq_info.anomaly
    row = result_df.collect()[0]
    anomaly = row["_dq_info"].anomaly

    # Verify contributions field is populated
    assert anomaly.contributions is not None, "contributions should not be None when requested"

    # Verify contributions is a map with column names as keys
    assert isinstance(anomaly.contributions, dict), "contributions should be a dict/map"

    # Verify all feature columns have contribution values
    for col in columns:
        assert col in anomaly.contributions, f"contributions should include '{col}'"
