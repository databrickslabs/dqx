"""Integration tests for segment-based anomaly detection."""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import train, has_no_anomalies
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule
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


def test_explicit_segment_training(
    spark: SparkSession, make_schema: str, make_random: str, skip_if_runtime_not_anomaly_compatible
):
    """Test explicit segment-based training."""
    # Generate multi-region data
    data = []
    for region in ["US", "EU", "APAC"]:
        base = 100 if region == "US" else (200 if region == "EU" else 150)
        for i in range(200):
            data.append((region, base + i * 0.5, base * 0.8 + i * 0.3))
    
    df = spark.createDataFrame(data, "region string, amount double, discount double")
    suffix = make_random(8).lower()
    table_name = f"{make_schema}.segment_test_{suffix}"
    df.write.saveAsTable(table_name)
    
    # Train with explicit segments
    train(
        df=spark.table(table_name),
        columns=["amount", "discount"],
        segment_by=["region"],
        model_name=f"test_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
    )
    
    # Verify segmented models were created
    registry = spark.table(f"{make_schema}.dqx_anomaly_models_{suffix}")
    models = registry.filter("status = 'active'").collect()
    assert len(models) == 3  # One per segment
    
    # Check segment model names
    model_names = [row.model_name for row in models]
    assert any("region=US" in name for name in model_names)
    assert any("region=EU" in name for name in model_names)
    assert any("region=APAC" in name for name in model_names)


def test_segment_scoring(
    spark: SparkSession, make_schema: str, make_random: str, skip_if_runtime_not_anomaly_compatible
):
    """Test that segment scoring uses correct regional models."""
    # Train
    data = []
    for region in ["US", "EU"]:
        base = 100 if region == "US" else 200
        for i in range(200):
            data.append((region, base + i * 0.5))
    
    df = spark.createDataFrame(data, "region string, amount double")
    suffix = make_random(8).lower()
    table_name = f"{make_schema}.segment_score_test_{suffix}"
    df.write.saveAsTable(table_name)
    
    train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=f"test_score_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
    )
    
    # Score with anomalous data
    test_data = [
        ("US", 100.0),   # Normal
        ("US", 500.0),   # Anomaly
        ("EU", 200.0),   # Normal
        ("EU", 900.0),   # Anomaly
    ]
    test_df = spark.createDataFrame(test_data, "region string, amount double")
    
    dq_engine = DQEngine(spark)
    check = DQDatasetRule(has_no_anomalies(
        columns=["amount"],
        segment_by=["region"],
        model=f"test_score_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
        score_threshold=0.7,
    ))
    
    result = dq_engine.apply_checks(test_df, [check])
    
    # Verify anomalies detected
    assert result.count() == 4
    anomalies = [row for row in result.collect() if row.anomaly_score and row.anomaly_score > 0.7]
    assert len(anomalies) == 2  # Two anomalies


def test_multi_column_segments(
    spark: SparkSession, make_schema: str, make_random: str, skip_if_runtime_not_anomaly_compatible
):
    """Test segmentation with multiple segment columns."""
    # Generate data with region + product_type
    data = []
    for region in ["US", "EU"]:
        for product in ["A", "B"]:
            base = 100 + (50 if region == "EU" else 0) + (25 if product == "B" else 0)
            for i in range(150):
                data.append((region, product, base + i * 0.5))
    
    df = spark.createDataFrame(data, "region string, product_type string, amount double")
    suffix = make_random(8).lower()
    table_name = f"{make_schema}.multi_segment_test_{suffix}"
    df.write.saveAsTable(table_name)
    
    # Train with multiple segment columns
    train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region", "product_type"],
        model_name=f"test_multi_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
    )
    
    # Verify 4 segment models created (2 regions × 2 products)
    registry = spark.table(f"{make_schema}.dqx_anomaly_models_{suffix}")
    models = registry.filter("status = 'active'").collect()
    assert len(models) == 4


def test_unknown_segment_handling(
    spark: SparkSession, make_schema: str, make_random: str, skip_if_runtime_not_anomaly_compatible
):
    """Test scoring with unknown segment values (not in training data)."""
    # Train on US and EU only
    data = []
    for region in ["US", "EU"]:
        base = 100 if region == "US" else 200
        for i in range(200):
            data.append((region, base + i * 0.5))
    
    df = spark.createDataFrame(data, "region string, amount double")
    suffix = make_random(8).lower()
    table_name = f"{make_schema}.unknown_segment_test_{suffix}"
    df.write.saveAsTable(table_name)
    
    train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=f"test_unknown_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
    )
    
    # Score with unknown region "APAC"
    test_data = [
        ("US", 100.0),
        ("APAC", 300.0),  # Unknown segment
    ]
    test_df = spark.createDataFrame(test_data, "region string, amount double")
    
    dq_engine = DQEngine(spark)
    check = DQDatasetRule(has_no_anomalies(
        columns=["amount"],
        segment_by=["region"],
        model=f"test_unknown_segments_{suffix}",
        registry_table=f"{make_schema}.dqx_anomaly_models_{suffix}",
    ))
    
    result = dq_engine.apply_checks(test_df, [check])
    
    # APAC row should have null score
    apac_row = [row for row in result.collect() if row.region == "APAC"][0]
    assert apac_row.anomaly_score is None

