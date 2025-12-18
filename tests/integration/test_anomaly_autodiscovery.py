"""Integration tests for auto-discovery of anomaly detection columns and segments."""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import train
from databricks.labs.dqx.anomaly.profiler import auto_discover
from tests.conftest import TEST_CATALOG


@pytest.fixture
def skip_if_runtime_not_anomaly_compatible(ws, debug_env):
    """Skip tests if runtime doesn't support anomaly detection (Spark < 3.4)."""
    import pytest
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    major, minor, *_ = spark.version.split(".")
    if int(major) < 3 or (int(major) == 3 and int(minor) < 4):
        pytest.skip("Anomaly detection requires Spark >= 3.4")


def test_auto_discover_numeric_columns(spark: SparkSession, skip_if_runtime_not_anomaly_compatible):
    """Test auto-discovery selects numeric columns with variance."""
    df = spark.createDataFrame(
        [
            (1, 100.0, 50.0, "2024-01-01", "id123"),
            (2, 101.0, 51.0, "2024-01-02", "id456"),
            (3, 102.0, 52.0, "2024-01-03", "id789"),
            (4, 103.0, 53.0, "2024-01-04", "id101"),
        ],
        "row_id int, amount double, discount double, date_col string, user_id string",
    )

    profile = auto_discover(df)

    # Should select amount and discount (numeric with variance)
    # Should exclude: row_id (pattern match), date_col (not numeric), user_id (pattern match)
    assert "amount" in profile.recommended_columns
    assert "discount" in profile.recommended_columns
    assert "row_id" not in profile.recommended_columns
    assert "user_id" not in profile.recommended_columns


def test_auto_discover_segments(spark: SparkSession, skip_if_runtime_not_anomaly_compatible):
    """Test auto-discovery identifies categorical columns for segmentation."""
    # Create data with good segment candidates
    data = []
    for region in ["US", "EU", "APAC"]:
        for i in range(1500):  # >1000 rows per segment
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover(df)

    # Should select region as segment (3 distinct values, >1000 rows per segment)
    assert "region" in profile.recommended_segments
    assert profile.segment_count == 3


def test_auto_discover_excludes_high_cardinality(spark: SparkSession, skip_if_runtime_not_anomaly_compatible):
    """Test that high-cardinality columns are excluded from segmentation."""
    # Create data with too many distinct values
    data = [(f"category_{i}", 100.0 + i) for i in range(100)]
    df = spark.createDataFrame(data, "category string, amount double")

    profile = auto_discover(df)

    # Should not recommend category for segmentation (high cardinality)
    assert "category" not in profile.recommended_segments
    # Should warn about high cardinality (excludes columns with "id" in name)
    assert any("category" in w for w in profile.warnings)


def test_zero_config_training(spark: SparkSession, make_schema, make_random, skip_if_runtime_not_anomaly_compatible):
    """Test zero-configuration training with auto-discovery."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Create data with clear numeric and segment columns
    data = []
    for region in ["US", "EU"]:
        for i in range(200):
            base = 100 if region == "US" else 200
            data.append((region, base + i * 0.5, base * 0.8 + i * 0.3))

    df = spark.createDataFrame(data, "region string, amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.auto_train_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Train with zero config (should auto-discover columns and segments)
    model_uri = train(
        df=spark.table(table_name),
        model_name=f"test_auto_{suffix}",
        registry_table=f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}",
    )

    # Verify models were created
    assert model_uri is not None

    # Check registry for segment models
    registry = spark.table(f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}")
    models = registry.filter("status = 'active'").collect()

    # Should create 2 segment models (US and EU)
    assert len(models) == 2

    # Verify auto-discovered columns (amount and discount)
    for model in models:
        assert set(model.columns) == {"amount", "discount"}


def test_explicit_columns_no_auto_segment(
    spark: SparkSession, make_schema, make_random, skip_if_runtime_not_anomaly_compatible
):
    """Test that providing explicit columns disables auto-segmentation."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Create data with segment column
    data = []
    for region in ["US", "EU"]:
        for i in range(200):
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.explicit_cols_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Train with explicit columns (should NOT auto-segment)
    train(
        df=spark.table(table_name),
        columns=["amount"],  # Explicit columns provided
        model_name=f"test_explicit_{suffix}",
        registry_table=f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}",
    )

    # Verify only 1 global model created (no segmentation)
    registry = spark.table(f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}")
    models = registry.filter("status = 'active'").collect()
    assert len(models) == 1
    assert models[0].is_global_model is True


def test_warnings_for_small_segments(spark: SparkSession, skip_if_runtime_not_anomaly_compatible):
    """Test that warnings are issued for segments with <1000 rows."""
    # Create data with small segments
    data = []
    for region in ["US", "EU", "APAC"]:
        for i in range(500):  # Only 500 rows per segment
            data.append((region, 100.0 + i))

    df = spark.createDataFrame(data, "region string, amount double")

    profile = auto_discover(df)

    # Should still recommend region but warn about small segments
    assert "region" in profile.recommended_segments
    assert any("1000 rows" in w for w in profile.warnings)
