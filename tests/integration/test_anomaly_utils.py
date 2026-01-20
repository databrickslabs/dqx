"""
Shared utilities and test data for anomaly detection integration tests.

This module provides reusable test data patterns and helper functions
to reduce duplication across anomaly detection tests.

## Helper Functions

### Training Helpers
Use these to eliminate boilerplate in test model training:

1. **train_simple_2d_model()** - Standard 2D training (amount, quantity)
   - Use for: Most basic anomaly tests
   - Example: `train_simple_2d_model(spark, engine, "model", "registry")`
   - Custom data: `train_simple_2d_model(..., train_data=my_data)`

2. **train_simple_3d_model()** - Standard 3D training (amount, quantity, discount)
   - Use for: Tests requiring 3 features
   - Example: `train_simple_3d_model(spark, engine, "model", "registry")`

3. **train_simple_4d_model()** - Standard 4D training (amount, quantity, discount, weight)
   - Use for: Tests requiring 4 features
   - Example: `train_simple_4d_model(spark, engine, "model", "registry")`
   - Defaults to get_standard_4d_training_data()

4. **train_segmented_model()** - Segmented training (by region/category)
   - Use for: Tests with segment_by parameter
   - Example:
     ```python
     data = [(region, amount, discount) for region in ["US", "EU"] for i in range(200)]
     train_segmented_model(
         spark, engine, "model", "registry",
         segment_columns=["region"],
         feature_columns=["amount", "discount"],
         data=data,
         schema="region string, amount double, discount double"
     )
     ```

5. **train_large_dataset_model()** - Efficient large dataset training
   - Use for: Sampling tests, performance tests
   - Example: `train_large_dataset_model(spark, engine, "model", "registry", num_rows=200_000)`
   - With params: `train_large_dataset_model(..., params=AnomalyParams(sample_fraction=0.5))`

### Scoring Helpers
6. **score_with_anomaly_check()** - Score DataFrame and collect results
   - Use for: Tests that need to verify scoring behavior
   - Example: `result = score_with_anomaly_check(df, "model", "registry", ["amount"])`

7. **create_anomaly_dataset_rule()** - Create DQDatasetRule for anomaly checks
   - Use for: Tests using DQEngine.apply_checks()
   - Example: `rule = create_anomaly_dataset_rule("model", "registry", ["amount", "quantity"])`

### Standard Training Data
- **get_standard_2d_training_data()** - 400 points for amount/quantity
- **get_standard_3d_training_data()** - 400 points for amount/quantity/discount  
- **get_standard_4d_training_data()** - 150 points for amount/quantity/discount/weight

## Usage Guidelines

- **For simple tests**: Use train_simple_2d_model() for most cases
- **For custom data**: Pass train_data parameter to any training helper
- **For sampling tests**: Use train_large_dataset_model() with num_rows
- **For segmented models**: Use train_segmented_model() with segment_by
- **For scoring tests**: Use score_with_anomaly_check() to avoid manual DataFrame creation

All helpers automatically handle:
- DataFrame creation from data
- Column specification
- Model name and registry table parameters
- Optional AnomalyParams configuration
"""

from typing import Any

import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame, SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly import AnomalyEngine, AnomalyParams, has_no_anomalies
from databricks.labs.dqx.rule import DQDatasetRule

from tests.integration.test_anomaly_constants import OUTLIER_AMOUNT, OUTLIER_QUANTITY

pytestmark = pytest.mark.anomaly

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
                                  expected_anomaly_count=1, score_threshold=DQENGINE_SCORE_THRESHOLD)
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
    # If merge_columns not provided, use a default value since has_no_anomalies requires it
    if merge_columns is None:
        merge_columns = ["transaction_id"]

    check_kwargs = {
        "merge_columns": merge_columns,
        "columns": columns,
        "model": model_name,
        "registry_table": registry_table,
        "score_threshold": score_threshold,
    }
    check_kwargs.update(kwargs)

    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_anomalies,
        check_func_kwargs=check_kwargs,
    )


def train_standard_2d_model(
    spark: SparkSession,
    unique_id: str,
    columns: list[str] | None = None,
    catalog: str = "main",
    schema: str = "default",
) -> dict[str, Any]:
    """
    Train a standard 2D anomaly model with common test parameters.

    Reduces duplication in tests that need to train a simple model.
    Follows Databricks convention of spark as first argument.

    Args:
        spark: SparkSession instance (required first for Databricks convention)
        unique_id: Unique identifier for model/registry naming
        columns: Columns to train on (default: ["amount", "quantity"])
        catalog: Catalog name (default: "main")
        schema: Schema name (default: "default")

    Returns:
        Dict with "model_name", "registry_table", "columns", "anomaly_engine" keys

    Example:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        info = train_standard_2d_model(spark, "test123")
        # Access trained model
        model_name = info["model_name"]
        anomaly_engine = info["anomaly_engine"]
    """
    if columns is None:
        columns = ["amount", "quantity"]

    model_name = f"test_model_{unique_id}"
    registry_table = f"{catalog}.{schema}.{unique_id}_registry"

    train_df = spark.createDataFrame(get_standard_2d_training_data(), "amount double, quantity double")

    # Create AnomalyEngine internally
    anomaly_engine = AnomalyEngine(WorkspaceClient(), spark)
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
        "anomaly_engine": anomaly_engine,  # Return for convenience
    }


def apply_anomaly_check_direct(
    test_df: Any,
    model_name: str,
    registry_table: str,
    columns: list[str],
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
        columns: Columns to check (required for this helper)
        score_threshold: Score threshold
        **kwargs: Additional has_no_anomalies kwargs

    Returns:
        DataFrame with anomaly_score column (extracted from _info.anomaly.score)

    Example:
        result_df = apply_anomaly_check_direct(
            test_df, "my_model", "main.default.registry",
            columns=["amount", "quantity"]
        )
    """
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=score_threshold,
        **kwargs,
    )
    result_df = apply_fn(test_df)

    # Extract _info.anomaly.score as top-level anomaly_score column for test convenience
    return result_df.withColumn("anomaly_score", F.col("_info.anomaly.score"))


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
        params (AnomalyParams | None): Optional AnomalyParams
        train_data (list[tuple] | None): Custom training data tuples (overrides train_size)

    Example:
        train_simple_2d_model(spark, engine, "my_model", "main.default.registry")
        train_simple_2d_model(spark, engine, "my_model", "registry", train_data=[(100, 2), (101, 2)])
    """
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]

    train_df = spark.createDataFrame(train_data, "amount double, quantity double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity"],
        model_name=model_name,
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
        params (AnomalyParams | None): Optional AnomalyParams
        train_data (list[tuple] | None): Custom training data tuples (overrides train_size)

    Example:
        train_simple_3d_model(spark, engine, "my_model", "main.default.registry")
    """
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0, 0.1) for i in range(train_size)]

    train_df = spark.createDataFrame(train_data, "amount double, quantity double, discount double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=model_name,
        registry_table=registry_table,
        params=params,
    )


def train_simple_4d_model(
    spark: SparkSession,
    anomaly_engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
):
    """
    Helper to train a simple 4D model with amount, quantity, discount, weight columns.

    Reduces duplication of training boilerplate across integration tests.

    Args:
        spark (SparkSession): SparkSession instance
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        model_name (str): Model name
        registry_table (str): Registry table path
        params (AnomalyParams | None): Optional AnomalyParams
        train_data (list[tuple] | None): Custom training data tuples (defaults to get_standard_4d_training_data())

    Example:
        train_simple_4d_model(spark, engine, "my_model", "main.default.registry")
        train_simple_4d_model(spark, engine, "model", "registry", train_data=custom_data)
    """
    if train_data is None:
        train_data = get_standard_4d_training_data()

    train_df = spark.createDataFrame(train_data, "amount double, quantity double, discount double, weight double")

    anomaly_engine.train(
        df=train_df,
        columns=["amount", "quantity", "discount", "weight"],
        model_name=model_name,
        registry_table=registry_table,
        params=params,
    )


def train_segmented_model(
    spark: SparkSession,
    anomaly_engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    segment_columns: list[str],
    feature_columns: list[str],
    data: list[tuple],
    schema: str,
    params: AnomalyParams | None = None,
):
    """
    Helper to train a segmented model with segment_by parameter.

    Reduces duplication of segmented training boilerplate across integration tests.

    Args:
        spark (SparkSession): SparkSession instance
        anomaly_engine (AnomalyEngine): AnomalyEngine instance
        model_name (str): Model name
        registry_table (str): Registry table path
        segment_columns (list[str]): Columns to segment by (e.g., ["region"])
        feature_columns (list[str]): Feature columns for training (e.g., ["amount", "discount"])
        data (list[tuple]): Training data tuples
        schema (str): DataFrame schema string (e.g., "region string, amount double, discount double")
        params (AnomalyParams | None): Optional AnomalyParams

    Example:
        data = [(region, base + i * 0.5, base * 0.8) for region in ["US", "EU"] for i in range(200)]
        train_segmented_model(
            spark, engine, "model", "registry",
            segment_columns=["region"],
            feature_columns=["amount", "discount"],
            data=data,
            schema="region string, amount double, discount double"
        )
    """
    train_df = spark.createDataFrame(data, schema)

    anomaly_engine.train(
        df=train_df,
        columns=feature_columns,
        segment_by=segment_columns,
        model_name=model_name,
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
        params (AnomalyParams | None): Optional AnomalyParams

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

    anomaly_engine.train(
        df=train_df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        params=params,
    )


def score_with_anomaly_check(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    columns: list[str],
    score_threshold: float = 0.5,
    merge_columns: list[str] | None = None,
):
    """
    Helper to score a DataFrame using has_no_anomalies and collect results.

    Reduces duplication of scoring pattern across integration tests.

    Args:
        df (DataFrame): DataFrame to score
        model_name (str): Model name
        registry_table (str): Registry table path
        columns (list[str]): Columns to check
        score_threshold (float): Anomaly score threshold (default: 0.5)
        merge_columns (list[str] | None): Merge columns (default: ["transaction_id"])

    Returns:
        Scored DataFrame with results collected

    Example:
        result = score_with_anomaly_check(
            test_df, "my_model", "main.default.registry", ["amount", "quantity"]
        )
    """
    if merge_columns is None:
        merge_columns = ["transaction_id"]

    _, apply_fn = has_no_anomalies(
        merge_columns=merge_columns,
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=score_threshold,
    )
    result_df = apply_fn(df)
    result_df.collect()  # Force evaluation
    return result_df


def create_anomaly_dataset_rule(
    model_name: str,
    registry_table: str,
    columns: list[str],
    criticality: str = "error",
    score_threshold: float = 0.5,
    merge_columns: list[str] | None = None,
    **kwargs,
):
    """
    Helper to create DQDatasetRule for anomaly detection.

    Reduces duplication of rule creation across integration tests.

    Args:
        model_name: Model name
        registry_table: Registry table path
        columns: Columns to check
        criticality: Rule criticality (default: "error")
        score_threshold: Anomaly score threshold (default: 0.5)
        merge_columns: Merge columns (default: ["transaction_id"])
        **kwargs: Additional has_no_anomalies arguments

    Returns:
        DQDatasetRule configured for anomaly detection

    Example:
        rule = create_anomaly_dataset_rule(
            "my_model", "main.default.registry", ["amount", "quantity"]
        )
    """
    if merge_columns is None:
        merge_columns = ["transaction_id"]

    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_anomalies,
        check_func_kwargs={
            "merge_columns": merge_columns,
            "columns": columns,
            "model": model_name,
            "registry_table": registry_table,
            "score_threshold": score_threshold,
            **kwargs,
        },
    )
