"""
Shared utilities and test data for anomaly detection integration tests.

This module provides reusable test data patterns and helper functions
to reduce duplication across anomaly detection tests.
"""

from typing import Any


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
        make_random_fn: The make_random fixture function
        prefix: Prefix for model name (default: "test")

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
