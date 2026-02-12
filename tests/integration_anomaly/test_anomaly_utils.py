from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.rule import DQDatasetRule
from tests.integration_anomaly.test_anomaly_constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)


def qualify_model_name(model_name: str, registry_table: str) -> str:
    """Return a fully qualified model name using the registry table prefix."""
    if model_name.count(".") >= 2:
        return model_name
    registry_prefix = registry_table.rsplit(".", 1)[0]
    return f"{registry_prefix}.{model_name}"


def _create_anomaly_apply_fn(model_name: str, registry_table: str, **check_kwargs):
    """
    Create apply function from has_no_anomalies check.

    This is a shared helper to reduce code duplication between
    the anomaly_scorer fixture and score_with_anomaly_check function.

    Args:
        model_name: Name of the trained model
        registry_table: Registry table path
        **check_kwargs: Additional arguments for has_no_anomalies

    Returns:
        Apply function from has_no_anomalies check
    """
    _, apply_fn = has_no_anomalies(
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        **check_kwargs,
    )
    return apply_fn


def train_model_with_params(
    anomaly_engine: AnomalyEngine,
    df: DataFrame,
    model_name: str,
    registry_table: str,
    columns: list[str],
    params: AnomalyParams,
    segment_by: list[str] | None = None,
    expected_anomaly_rate: float = 0.02,
) -> str:
    """
    Train a model with internal params (test-only).
    """
    return anomaly_engine.train(
        df=df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        segment_by=segment_by,
        params=params,
        expected_anomaly_rate=expected_anomaly_rate,
    )


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


def get_standard_test_points_2d() -> dict[str, tuple[float, float]]:
    """
    Pre-validated test points for 2D anomaly detection tests.

    These points are designed to work with get_standard_2d_training_data():
    - normal_in_center: (200.0, 30.0) - Center of distribution
    - normal_near_center: (210.0, 32.0) - Near center
    - clear_anomaly: (OUTLIER_AMOUNT, OUTLIER_QUANTITY) - Far outside

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
        "clear_anomaly": (OUTLIER_AMOUNT, OUTLIER_QUANTITY),
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
        "clear_anomaly": (OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95, 1.0),
    }


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


def get_percentile_threshold_from_data(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    percentile: float = 0.95,
) -> float:
    """Derive a severity percentile threshold (0–100) from a target percentile."""
    _ = (df, model_name, registry_table)
    # Severity threshold is expressed directly in percentile terms (0–100)
    return float(percentile * 100.0)


def create_anomaly_check_rule(
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    criticality: str = "error",
    **kwargs: Any,
) -> DQDatasetRule:
    """
    Create a standard DQDatasetRule for anomaly detection.

    Reduces duplication in tests that need to set up anomaly checks.

    Args:
        model_name: Name of the trained model
        registry_table: Full path to registry table
        threshold: Severity percentile threshold (default: 60.0)
        criticality: Rule criticality (default: "error")
        **kwargs: Additional check_func_kwargs

    Returns:
        Configured DQDatasetRule

    Example:
        check = create_anomaly_check_rule(
            model_name="test_model",
            registry_table="main.default.registry",
        )
    """
    check_kwargs = {
        "model": qualify_model_name(model_name, registry_table),
        "registry_table": registry_table,
        "threshold": threshold,
    }
    check_kwargs.update(kwargs)

    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_anomalies,
        check_func_kwargs=check_kwargs,
    )


def apply_anomaly_check_direct(
    test_df: Any,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    **kwargs: Any,
) -> Any:
    """
    Apply anomaly detection directly (without DQEngine) to get anomaly_score column.

    Helper to reduce duplication when tests need direct access to anomaly_score.

    Args:
        test_df: Test DataFrame
        model_name: Model name
        registry_table: Registry table path
        threshold: Severity percentile threshold (0–100)
        **kwargs: Additional has_no_anomalies kwargs

    Returns:
        DataFrame with anomaly_score column (extracted from _dq_info.anomaly.score)

    Example:
        result_df = apply_anomaly_check_direct(
            test_df, "my_model", "main.default.registry"
        )
    """
    _, apply_fn = has_no_anomalies(
        model=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=threshold,
        **kwargs,
    )
    result_df = apply_fn(test_df)

    # Extract _dq_info fields as top-level columns for test convenience
    return result_df.withColumn("anomaly_score", F.col("_dq_info.anomaly.score")).withColumn(
        "severity_percentile", F.col("_dq_info.anomaly.severity_percentile")
    )


def train_simple_2d_model(
    spark: SparkSession,
    anomaly_engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
):
    """
    Helper to train a simple 2D model with standard amount/quantity columns.

    Reduces duplication of training boilerplate across integration tests.

    Args:
        spark (SparkSession): SparkSession instance
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        model_name (str): Model name
        registry_table (str): Registry table path
        train_size (int): Number of training rows (default: 50, ignored if train_data provided)
        params (AnomalyParams | None): Optional internal training params (test-only)
        train_data (list[tuple] | None): Custom training data tuples (overrides train_size)

    Example:
        train_simple_2d_model(spark, engine, "my_model", "main.default.registry")
        train_simple_2d_model(spark, engine, "my_model", "registry", train_data=[(100, 2), (101, 2)])
    """
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]

    train_df = spark.createDataFrame(train_data, "amount double, quantity double")
    full_model_name = qualify_model_name(model_name, registry_table)

    if params is None:
        anomaly_engine.train(
            df=train_df,
            columns=["amount", "quantity"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return

    train_model_with_params(
        anomaly_engine=anomaly_engine,
        df=train_df,
        columns=["amount", "quantity"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_simple_3d_model(
    spark: SparkSession,
    anomaly_engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
):
    """
    Helper to train a simple 3D model with amount, quantity, discount columns.

    Reduces duplication of training boilerplate across integration tests.

    Args:
        spark (SparkSession): SparkSession instance
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        model_name (str): Model name
        registry_table (str): Registry table path
        train_size (int): Number of training rows (default: 50, ignored if train_data provided)
        params (AnomalyParams | None): Optional internal training params (test-only)
        train_data (list[tuple] | None): Custom training data tuples (overrides train_size)

    Example:
        train_simple_3d_model(spark, engine, "my_model", "main.default.registry")
    """
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0, 0.1) for i in range(train_size)]

    train_df = spark.createDataFrame(train_data, "amount double, quantity double, discount double")
    full_model_name = qualify_model_name(model_name, registry_table)

    if params is None:
        anomaly_engine.train(
            df=train_df,
            columns=["amount", "quantity", "discount"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return

    train_model_with_params(
        anomaly_engine=anomaly_engine,
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_large_dataset_model(
    spark: SparkSession,
    anomaly_engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    num_rows: int,
    columns: list[str] | None = None,
    params: AnomalyParams | None = None,
):
    """
    Helper to train a model on large synthetic dataset using spark.range().

    Efficient for large dataset tests without materializing lists in driver memory.
    Reduces duplication of large dataset training boilerplate across integration tests.

    Args:
        spark (SparkSession): SparkSession instance
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        model_name (str): Model name
        registry_table (str): Registry table path
        num_rows (int): Number of rows to generate
        columns (list[str] | None): Column names (default: ["amount", "quantity"])
        params (AnomalyParams | None): Optional internal training params (test-only)

    Example:
        train_large_dataset_model(spark, engine, "model", "registry", num_rows=200_000)
        train_large_dataset_model(
            spark, engine, "model", "registry", num_rows=10_000,
            params=AnomalyParams(sample_fraction=0.1, max_rows=500)
        )
    """
    if columns is None:
        columns = ["amount", "quantity"]

    # Generate large dataset efficiently using spark.range
    train_df = spark.range(num_rows).selectExpr("cast(id as double) as amount", "2.0 as quantity")
    full_model_name = qualify_model_name(model_name, registry_table)

    if params is None:
        anomaly_engine.train(
            df=train_df,
            columns=columns,
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return

    train_model_with_params(
        anomaly_engine=anomaly_engine,
        df=train_df,
        columns=columns,
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def score_with_anomaly_check(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
):
    """
    Helper to score a DataFrame using has_no_anomalies and collect results.

    Reduces duplication of scoring pattern across integration tests.

    Args:
        df (DataFrame): DataFrame to score
        model_name (str): Model name
        registry_table (str): Registry table path
        threshold (float): Severity percentile threshold (0–100, default: 60.0)

    Returns:
        Scored DataFrame with results collected

    Example:
        result = score_with_anomaly_check(
            test_df, "my_model", "main.default.registry"
        )
    """
    apply_fn = _create_anomaly_apply_fn(
        model_name=model_name,
        registry_table=registry_table,
        threshold=threshold,
    )
    result_df = apply_fn(df)
    result_df.collect()  # Force evaluation
    return result_df


def score_3d_with_contributions(
    spark: SparkSession,
    test_df_factory,
    anomaly_scorer,
    model_name: str,
    registry_table: str,
    normal_rows: list[tuple[float, float, float]] | None = None,
    anomaly_rows: list[tuple[float, float, float]] | None = None,
    include_confidence: bool = False,
):
    """Score a 3D dataset with contributions enabled (optionally confidence)."""
    test_df = test_df_factory(
        spark,
        normal_rows=normal_rows or [],
        anomaly_rows=anomaly_rows or [(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )
    return anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        include_contributions=True,
        include_confidence=include_confidence,
        extract_score=False,
    )


def create_anomaly_dataset_rule(
    model_name: str,
    registry_table: str,
    criticality: str = "error",
    threshold: float = 60.0,
    **kwargs,
):
    """
    Helper to create DQDatasetRule for anomaly detection.

    Reduces duplication of rule creation across integration tests.

    Args:
        model_name: Model name
        registry_table: Registry table path
        criticality: Rule criticality (default: "error")
        threshold: Severity percentile threshold (default: 60.0)
        **kwargs: Additional has_no_anomalies arguments

    Returns:
        DQDatasetRule configured for anomaly detection

    Example:
        rule = create_anomaly_dataset_rule(
            "my_model", "main.default.registry"
        )
    """
    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": qualify_model_name(model_name, registry_table),
            "registry_table": registry_table,
            "threshold": threshold,
            **kwargs,
        },
    )
