"""Integration tests for anomaly detection with custom column naming.

Note: This test file is separate because it tests the _dq_info column renaming behavior,
which is specific to anomaly detection. In the future, if _dq_info is used by other modules
(e.g., profiling, validation), these tests can be moved to a more general location like
tests/integration/test_apply_checks.py where other column renaming tests live.
"""

from pyspark.sql import SparkSession

from databricks.labs.dqx.engine import DQEngine, ExtraParams, ColumnArguments
from tests.integration_anomaly.test_anomaly_utils import create_anomaly_check_rule


def test_anomaly_check_with_custom_info_column_name(
    ws, spark: SparkSession, anomaly_engine, anomaly_registry_prefix, make_random
):
    """Test that _dq_info column can be renamed via result_column_names.

    This test verifies that the engine.py line:
        if "_dq_info" in result_df.columns and info_col_name != "_dq_info":
            result_df = result_df.withColumnRenamed("_dq_info", info_col_name)

    Works correctly for anomaly detection checks.
    """
    # Train a simple anomaly model
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_custom_info_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    train_df = spark.createDataFrame([(i, 100.0 + i * 0.1) for i in range(100)], "transaction_id int, amount double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Apply anomaly check with CUSTOM column name for _dq_info
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            result_column_names={
                ColumnArguments.INFO.value: "custom_anomaly_info",  # Rename _dq_info → custom_anomaly_info
                ColumnArguments.ERRORS.value: "_error",
                ColumnArguments.WARNINGS.value: "_warning",
            },
        ),
    )

    test_df = spark.createDataFrame([(1, 100.0)], "transaction_id int, amount double")

    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify _dq_info was renamed to custom_anomaly_info
    assert "custom_anomaly_info" in result_df.columns
    assert "_dq_info" not in result_df.columns

    # Verify it contains anomaly metadata with expected structure
    row = result_df.collect()[0]
    assert row["custom_anomaly_info"] is not None
    assert row["custom_anomaly_info"]["anomaly"] is not None
    assert row["custom_anomaly_info"]["anomaly"]["score"] is not None


def test_anomaly_check_with_default_info_column_name(
    ws, spark: SparkSession, anomaly_engine, anomaly_registry_prefix, make_random
):
    """Test that _dq_info column is preserved with default name when not renamed.

    This verifies the condition:
        if "_dq_info" in result_df.columns and info_col_name != "_dq_info":

    When info_col_name == "_dq_info" (default), no renaming should occur.
    """
    # Train a simple anomaly model
    unique_id = make_random(8).lower()
    model_name = f"{anomaly_registry_prefix}.test_default_info_{make_random(4).lower()}"
    registry_table = f"{anomaly_registry_prefix}.{unique_id}_registry"

    train_df = spark.createDataFrame([(i, 100.0 + i * 0.1) for i in range(100)], "transaction_id int, amount double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Apply anomaly check WITHOUT custom column names (use defaults)
    dq_engine = DQEngine(ws)

    test_df = spark.createDataFrame([(1, 100.0)], "transaction_id int, amount double")

    checks = [
        create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            threshold=60.0,
        )
    ]

    result_df = dq_engine.apply_checks(test_df, checks)

    # Verify _dq_info column exists with default name
    assert "_dq_info" in result_df.columns

    # Verify it contains anomaly metadata
    row = result_df.collect()[0]
    assert row["_dq_info"] is not None
    assert row["_dq_info"]["anomaly"] is not None
    assert row["_dq_info"]["anomaly"]["score"] is not None
