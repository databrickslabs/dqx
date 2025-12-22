"""
Shared utilities and test data for anomaly detection integration tests.

This module provides reusable test data patterns and helper functions
to reduce duplication across anomaly detection tests.
"""

from typing import Any

from pyspark.sql import SparkSession
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.anomaly import has_no_anomalies


# ============================================================================
# Standard Training Data Patterns
# ============================================================================


def get_standard_2d_training_data() -> list[tuple[float, float]]:
    """
    Standard 2D training data for amount/quantity anomaly detection.

    Creates realistic cluster with sufficient variance:
    - amount: 100.0 to 300.0 (step 0.5)
    - quantity: 10.0 to 50.0 (step 0.1)
    - 400 data points

    This pattern works well with RobustScaler and provides stable
    anomaly detection across different test scenarios.

    Returns:
        List of (amount, quantity) tuples

    Example:
        train_df = spark.createDataFrame(
            get_standard_2d_training_data(),
            "amount double, quantity double"
        )
    """
    return [(100.0 + i * 0.5, 10.0 + i * 0.1) for i in range(400)]


def get_standard_3d_training_data() -> list[tuple[float, float, float]]:
    """
    Standard 3D training data for anomaly detection with contributions.

    Creates realistic cluster with variance in all dimensions:
    - amount: 100.0 to 300.0 (step 0.5)
    - quantity: 10.0 to 50.0 (step 0.1)
    - discount: 0.1 to 0.5 (step 0.001)
    - 400 data points

    This pattern is commonly used for explainability tests where
    feature contributions need to be computed.

    Returns:
        List of (amount, quantity, discount) tuples

    Example:
        train_df = spark.createDataFrame(
            get_standard_3d_training_data(),
            "amount double, quantity double, discount double"
        )
    """
    return [(100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001) for i in range(400)]


def get_standard_4d_training_data() -> list[tuple[float, float, float, float]]:
    """
    Standard 4D training data for multi-column anomaly detection.

    Creates realistic cluster with variance in all dimensions:
    - amount: 100.0 to 300.0 (step 0.5)
    - quantity: 10.0 to 50.0 (step 0.1)
    - discount: 0.1 to 0.5 (step 0.001)
    - weight: 50.0 to 130.0 (step 0.2)
    - 400 data points

    Returns:
        List of (amount, quantity, discount, weight) tuples

    Example:
        train_df = spark.createDataFrame(
            get_standard_4d_training_data(),
            "amount double, quantity double, discount double, weight double"
        )
    """
    return [(100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001, 50.0 + i * 0.2) for i in range(400)]


# ============================================================================
# Standard Test Data Points
# ============================================================================


def get_standard_test_points_2d() -> dict[str, tuple[float, float]]:
    """
    Pre-validated test points for 2D anomaly detection tests.

    These points are designed to work with get_standard_2d_training_data():
    - normal_in_center: (200.0, 30.0) - Center of distribution
    - normal_near_center: (210.0, 32.0) - Near center
    - clear_anomaly: (9999.0, 1.0) - Far outside

    All normal points score ~0.55-0.57 with RobustScaler.
    Anomaly points score ~0.65+ with RobustScaler.

    Returns:
        Dict mapping semantic names to (amount, quantity) tuples

    Example:
        test_points = get_standard_test_points_2d()
        test_df = spark.createDataFrame([
            test_points["normal_in_center"],
            test_points["clear_anomaly"],
        ], "amount double, quantity double")
    """
    return {
        "normal_in_center": (200.0, 30.0),
        "normal_near_center": (210.0, 32.0),
        "clear_anomaly": (9999.0, 1.0),
    }


def get_standard_test_points_4d() -> dict[str, tuple[float, float, float, float]]:
    """
    Pre-validated test points for 4D anomaly detection tests.

    These points are designed to work with get_standard_4d_training_data():
    - normal_in_center: Center of 4D distribution
    - clear_anomaly: Far outside in all dimensions

    Returns:
        Dict mapping semantic names to (amount, quantity, discount, weight) tuples

    Example:
        test_points = get_standard_test_points_4d()
        test_df = spark.createDataFrame([
            test_points["normal_in_center"],
            test_points["clear_anomaly"],
        ], "amount double, quantity double, discount double, weight double")
    """
    return {
        "normal_in_center": (200.0, 30.0, 0.25, 90.0),
        "clear_anomaly": (9999.0, 1.0, 0.95, 1.0),
    }


# ============================================================================
# Training Data Range Information
# ============================================================================


def get_standard_training_ranges() -> dict[str, dict[str, tuple[float, float]]]:
    """
    Get the expected ranges for standard training data.

    Useful for understanding what constitutes "normal" vs "anomalous"
    in tests using standard training data.

    Returns:
        Dict with "2d" and "4d" keys, each containing column ranges

    Example:
        ranges = get_standard_training_ranges()
        amount_range = ranges["2d"]["amount"]  # (100.0, 300.0)
    """
    return {
        "2d": {
            "amount": (100.0, 300.0),
            "quantity": (10.0, 50.0),
        },
        "4d": {
            "amount": (100.0, 300.0),
            "quantity": (10.0, 50.0),
            "discount": (0.1, 0.5),
            "weight": (50.0, 130.0),
        },
    }


# ============================================================================
# Recommended Thresholds
# ============================================================================


def get_recommended_threshold(use_case: str = "standard") -> float:
    """
    Get recommended score_threshold for different test scenarios.

    Args:
        use_case: One of "standard", "strict", "permissive"

    Returns:
        Recommended threshold value

    Thresholds based on empirical testing with RobustScaler:
    - standard (0.6): Good balance, works for most tests
    - strict (0.55): Catches more anomalies, may have false positives
    - permissive (0.65): Only very clear anomalies
    """
    thresholds = {
        "standard": 0.6,
        "strict": 0.55,
        "permissive": 0.65,
    }
    return thresholds.get(use_case, 0.6)


# ============================================================================
# Helper Functions
# ============================================================================


def create_test_model_names(make_random_fn, prefix: str = "test") -> dict[str, str]:
    """
    Create consistent naming for anomaly detection test artifacts.

    Args:
        make_random_fn (Callable[[int], str]): The make_random fixture function
        prefix (str): Prefix for model name (default: "test")

    Returns:
        Dict with "model_name" and "registry_suffix" keys

    Example:
        names = create_test_model_names(make_random, "my_test")
        model_name = names["model_name"]  # "my_test_abc4"
        registry_table = f"{catalog}.{schema}.{names['registry_suffix']}"
    """
    return {
        "model_name": f"{prefix}_{make_random_fn(4).lower()}",
        "registry_suffix": f"{make_random_fn(8).lower()}_registry",
    }


def assert_anomaly_separation(
    result_rows: list[Any],
    expected_normal_count: int,
    expected_anomaly_count: int,
    score_threshold: float,
) -> None:
    """
    Assert that anomaly scores properly separate normal from anomalous rows.

    Args:
        result_rows: Collected rows from result DataFrame
        expected_normal_count: Number of rows expected to be normal (score < threshold)
        expected_anomaly_count: Number of rows expected to be anomalous (score >= threshold)
        score_threshold: The threshold used for classification

    Raises:
        AssertionError: If separation doesn't match expectations

    Example:
        result_df = dq_engine.apply_checks(test_df, checks)
        rows = result_df.collect()
        assert_anomaly_separation(rows, expected_normal_count=2,
                                  expected_anomaly_count=1, score_threshold=0.6)
    """
    normal_count = sum(1 for row in result_rows if row.get("anomaly_score") and row["anomaly_score"] < score_threshold)
    anomaly_count = sum(
        1 for row in result_rows if row.get("anomaly_score") and row["anomaly_score"] >= score_threshold
    )

    assert normal_count == expected_normal_count, f"Expected {expected_normal_count} normal rows, got {normal_count}"
    assert (
        anomaly_count == expected_anomaly_count
    ), f"Expected {expected_anomaly_count} anomalous rows, got {anomaly_count}"


def create_anomaly_check_rule(
    model_name: str,
    registry_table: str,
    columns: list[str],
    score_threshold: float = 0.6,
    criticality: str = "error",
    merge_columns: list[str] | None = None,
    **kwargs: Any,
) -> DQDatasetRule:
    """
    Create a standard DQDatasetRule for anomaly detection.

    Reduces duplication in tests that need to set up anomaly checks.

    Args:
        model_name: Name of the trained model
        registry_table: Full path to registry table
        columns: List of columns to check
        score_threshold: Anomaly score threshold (default: 0.6)
        criticality: Rule criticality (default: "error")
        merge_columns: Columns for merging results (optional)
        **kwargs: Additional check_func_kwargs

    Returns:
        Configured DQDatasetRule

    Example:
        check = create_anomaly_check_rule(
            model_name="test_model",
            registry_table="main.default.registry",
            columns=["amount", "quantity"]
        )
    """
    check_kwargs = {
        "columns": columns,
        "model": model_name,
        "registry_table": registry_table,
        "score_threshold": score_threshold,
    }
    if merge_columns:
        check_kwargs["merge_columns"] = merge_columns
    check_kwargs.update(kwargs)

    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_anomalies,
        check_func_kwargs=check_kwargs,
    )


def train_standard_2d_model(  # pylint: disable=no-spark-argument-in-function
    anomaly_engine: Any,
    unique_id: str,
    columns: list[str] | None = None,
    catalog: str = "main",
    schema: str = "default",
) -> dict[str, Any]:
    """
    Train a standard 2D anomaly model with common test parameters.

    Reduces duplication in tests that need to train a simple model.

    Args:
        anomaly_engine: AnomalyEngine instance
        unique_id: Unique identifier for model/registry naming
        columns: Columns to train on (default: ["amount", "quantity"])
        catalog: Catalog name (default: "main")
        schema: Schema name (default: "default")

    Returns:
        Dict with "model_name", "registry_table", "columns" keys

    Example:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        train_df = spark.createDataFrame(
            get_standard_2d_training_data(),
            "amount double, quantity double"
        )
        info = train_standard_2d_model(anomaly_engine, "test123", train_df=train_df)
    """
    if columns is None:
        columns = ["amount", "quantity"]

    model_name = f"test_model_{unique_id}"
    registry_table = f"{catalog}.{schema}.{unique_id}_registry"

    spark = SparkSession.builder.getOrCreate()
    train_df = spark.createDataFrame(get_standard_2d_training_data(), "amount double, quantity double")

    anomaly_engine.train(
        df=train_df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
    )

    return {
        "model_name": model_name,
        "registry_table": registry_table,
        "columns": columns,
    }


def apply_anomaly_check_direct(
    test_df: Any,
    model_name: str,
    registry_table: str,
    columns: list[str] | None = None,
    score_threshold: float = 0.5,
    **kwargs: Any,
) -> Any:
    """
    Apply anomaly detection directly (without DQEngine) to get anomaly_score column.

    Helper to reduce duplication when tests need direct access to anomaly_score.

    Args:
        test_df: Test DataFrame
        model_name: Model name
        registry_table: Registry table path
        columns: Columns to check
        score_threshold: Score threshold
        **kwargs: Additional has_no_anomalies kwargs

    Returns:
        DataFrame with anomaly_score column

    Example:
        result_df = apply_anomaly_check_direct(
            test_df, "my_model", "main.default.registry",
            columns=["amount", "quantity"]
        )
    """
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,  # type: ignore[arg-type]
        model=model_name,
        registry_table=registry_table,
        score_threshold=score_threshold,
        **kwargs,
    )
    return apply_fn(test_df)
