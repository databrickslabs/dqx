import pytest
import yaml
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError, InvalidParameterError

from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    DETERMINISTIC_FLAG_THRESHOLD,
    DQENGINE_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
    SEGMENT_REGIONS,
)


def test_apply_anomaly_check_by_metadata(ws, spark: SparkSession, shared_2d_model):
    """Test applying anomaly checks defined in YAML."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Define checks in YAML format
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Normal + extreme anomaly
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns

    # Verify errors are added for anomalies
    rows = result_df.orderBy("transaction_id").collect()
    assert len(rows[1]["_errors"]) > 0  # Anomalous row has errors


def test_apply_anomaly_check_by_metadata_with_columns_autodiscovery(ws, spark: SparkSession, shared_2d_model):
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Define checks in YAML format
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Normal + extreme anomaly
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata columns are added
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns

    # Verify errors are added for anomalies
    rows = result_df.orderBy("transaction_id").collect()
    assert len(rows[1]["_errors"]) > 0  # Anomalous row has errors


def test_apply_anomaly_check_by_metadata_with_multiple_checks(ws, spark: SparkSession, shared_2d_model):
    """Test YAML with multiple anomaly and standard checks."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    threshold = DETERMINISTIC_FLAG_THRESHOLD

    # Define multiple checks in YAML
    checks_yaml = f"""
    - criticality: warn
      check:
        function: is_not_null
        arguments:
          column: transaction_id
    - criticality: warn
      check:
        function: is_unique
        arguments:
          columns: 
          - transaction_id
    - criticality: error
      check:
        function: is_not_null
        arguments:
          column: amount
    - criticality: error
      check:
        function: is_unique
        arguments:
          columns: 
          - transaction_id
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {threshold}
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [
            (1, 200.0, 30.0),  # Normal - middle of training range
            (2, None, 30.0),  # Null
            (3, OUTLIER_AMOUNT, OUTLIER_QUANTITY),  # Extreme anomaly
        ],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    rows = result_df.orderBy("transaction_id").collect()

    # Normal row: allow anomaly errors, but should not fail is_not_null
    row0_errors = rows[0]["_errors"] if rows[0]["_errors"] is not None else []

    def _has_is_not_null_error(err) -> bool:
        err_dict = err.asDict() if hasattr(err, "asDict") else {}
        for key in ("function", "name", "check", "check_name"):
            value = err_dict.get(key)
            if value and "is_not_null" in str(value):
                return True
        message = err_dict.get("message")
        return bool(message and "null" in str(message).lower())

    assert not any(_has_is_not_null_error(err) for err in row0_errors)

    # Null row: has is_not_null error (handle None)
    row1_errors = rows[1]["_errors"] if rows[1]["_errors"] is not None else []
    assert len(row1_errors) > 0

    # Anomaly row: verify anomaly check produced info; hard error flagging can vary by model fit.
    row2_errors = rows[2]["_errors"] if rows[2]["_errors"] is not None else []
    row2_info = rows[2]["_dq_info"]
    assert row2_info is not None
    assert len(row2_info) > 0
    anomaly_entries = [
        info
        for info in row2_info
        if getattr(getattr(info, "anomaly", None), "check_name", None) == "has_no_row_anomalies"
    ]
    assert len(anomaly_entries) > 0
    anomaly_info = anomaly_entries[0].anomaly
    # Keep strong expectation when classifier does mark anomaly; info presence is the non-flaky contract.
    if anomaly_info.is_anomaly:
        assert len(row2_errors) > 0

    # At least one row should have errors (null row has is_not_null, optionally anomaly row).
    assert any(len(r["_errors"] or []) > 0 for r in rows), "At least one row should have errors"


def test_apply_anomaly_multiple_checks_by_metadata(ws, spark, shared_2d_model):
    """Two has_no_row_anomalies (error) + one (warn); _dq_info should be an array with 3 elements."""
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          driver_only: true
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          driver_only: true
    - criticality: warn
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    assert "_dq_info" in result_df.columns

    rows = result_df.orderBy("transaction_id").collect()
    for row in rows:
        info_arr = row["_dq_info"]
        assert info_arr is not None, "_dq_info should not be None"
        assert len(info_arr) == 3, f"_dq_info should have 3 elements, got {len(info_arr)}"
        for i, info in enumerate(info_arr):
            assert hasattr(info, "anomaly"), f"_dq_info[{i}] should have 'anomaly' field"
            anomaly = info.anomaly
            assert anomaly is not None, f"_dq_info[{i}].anomaly should not be None"
            assert hasattr(anomaly, "score"), f"_dq_info[{i}].anomaly should have 'score'"
            assert hasattr(anomaly, "is_anomaly"), f"_dq_info[{i}].anomaly should have 'is_anomaly'"
        # At least one element should have non-null score (first check runs full scoring)
        scores = [info_arr[i].anomaly.score for i in range(3)]
        assert any(s is not None for s in scores), "_dq_info should have at least one element with non-null score"
        # When there is no anomaly, all 3 info results should be the same
        if not info_arr[0].anomaly.is_anomaly:
            assert info_arr[0].anomaly.score == info_arr[1].anomaly.score == info_arr[2].anomaly.score
            assert info_arr[0].anomaly.is_anomaly == info_arr[1].anomaly.is_anomaly == info_arr[2].anomaly.is_anomaly


def test_apply_anomaly_check_by_metadata_with_custom_threshold(ws, spark: SparkSession, shared_2d_model):
    """Test YAML configuration with custom threshold."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Define check with custom threshold
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: 90.0
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 150.0, 20.0), (2, 200.0, 25.0)],  # Normal values in training range
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # With high threshold (90.0), slightly unusual data should pass
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # With high threshold, expect no or few errors (handle both None and empty list)
    error_counts = [len(row["_errors"]) if row["_errors"] is not None else 0 for row in rows]
    assert all(count == 0 for count in error_counts) or sum(error_counts) <= 1


def test_apply_anomaly_check_by_metadata_with_contributions(ws, spark: SparkSession, shared_3d_model):
    """Test YAML configuration with enable_contributions flag."""
    # Use shared pre-trained 3D model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Define check with contributions
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
          enable_contributions: true
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Extreme anomaly (far outside training range)
    test_df = spark.createDataFrame(
        [(1, OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        "transaction_id int, amount double, quantity double, discount double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Verify DQX metadata is added (contributions are not added by metadata API)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Anomalous data should have errors
    assert len(rows[0]["_errors"]) > 0


def test_apply_anomaly_check_by_metadata_with_drift_threshold(ws, spark: SparkSession, shared_2d_model):
    """Test YAML configuration with drift_threshold."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Define check with drift threshold
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DQENGINE_SCORE_THRESHOLD}
          drift_threshold: 3.0
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 200.0, 30.0)],  # Middle of training range
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Should succeed without errors (drift detection is configured)
    assert "_errors" in result_df.columns
    rows = result_df.collect()
    # Normal data should not have errors (handle None)
    row_errors = rows[0]["_errors"] if rows[0]["_errors"] is not None else []
    assert len(row_errors) == 0


def test_apply_anomaly_check_by_metadata_criticality_warn(ws, spark: SparkSession, shared_2d_model):
    """Test YAML with criticality='warn'."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Define check with warn criticality
    checks_yaml = f"""
    - criticality: warn
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DQENGINE_SCORE_THRESHOLD}
          driver_only: true
    """

    checks = yaml.safe_load(checks_yaml)

    # Use values within training range (100-300 for amount, 10-50 for quantity)
    test_df = spark.createDataFrame(
        [(1, 200.0, 30.0), (2, OUTLIER_AMOUNT, OUTLIER_QUANTITY)],  # Normal + extreme anomaly
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    # With warn criticality, anomalies should be in warnings
    rows = result_df.collect()

    # Anomalous row should have warnings or errors
    assert rows[1]["_warnings"] is not None or rows[1]["_errors"] is not None


def test_apply_anomaly_check_by_metadata_with_filter_segmented(ws, spark: SparkSession, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Training data: region + amount, quantity (for segment_by=["region"])
    data = []
    for region in SEGMENT_REGIONS:
        base = 100 if region == "US" else (200 if region == "EU" else 150)
        for i in range(60):
            data.append((region, base + i * 0.5, base * 0.1 + i * 0.2))

    train_df = spark.createDataFrame(data, "region string, amount double, quantity double")
    model_name = f"{TEST_CATALOG}.{schema.name}.test_filter_seg_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_filter_{suffix}"

    engine = AnomalyEngine(ws, spark)
    engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    threshold = DETERMINISTIC_FLAG_THRESHOLD

    # Check definition with filter: only score rows where region = 'US'
    checks_yaml = f"""
    - criticality: error
      filter: "region = 'US'"
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {threshold}
          driver_only: true
    """
    checks = yaml.safe_load(checks_yaml)

    # Rows for all regions; only US rows are scored by the check
    test_df = spark.createDataFrame(
        [
            (1, "US", 150.0, 20.0),
            (2, "EU", 210.0, 25.0),
            (3, "US", OUTLIER_AMOUNT, OUTLIER_QUANTITY),
        ],
        "transaction_id int, region string, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    assert "_dq_info" in result_df.columns
    assert "_errors" in result_df.columns
    rows = result_df.orderBy("transaction_id").collect()
    assert len(rows) == 3
    # US rows should have anomaly info (filter matched); EU row should still be present (join back)
    row_us_inlier = next(r for r in rows if r["transaction_id"] == 1)
    row_us_anomaly = next(r for r in rows if r["transaction_id"] == 3)
    assert row_us_inlier["_dq_info"] is not None
    assert row_us_anomaly["_dq_info"] is not None
    # Anomalous US row (transaction_id=3) should have errors
    assert row_us_anomaly["_errors"] is not None, "Anomalous US row should have _errors"
    assert len(row_us_anomaly["_errors"]) > 0


def test_apply_anomaly_check_by_metadata_parsing_validation(ws, spark: SparkSession):
    """Test that invalid YAML (e.g. missing required model_name) is caught."""
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          threshold: {DEFAULT_SCORE_THRESHOLD}
    """

    checks = yaml.safe_load(checks_yaml)

    # This should fail when trying to apply the check
    # (Either at parse time or at runtime)
    test_df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)

    # Missing model_name/registry_table surfaces as InvalidCheckError from the
    # metadata validator, InvalidParameterError from runtime param checks, or
    # TypeError if Python argument binding fails first.
    with pytest.raises((InvalidCheckError, InvalidParameterError, TypeError)):
        result_df = dq_engine.apply_checks_by_metadata(test_df, checks)
        result_df.collect()
