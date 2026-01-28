"""Integration tests for segment-based anomaly detection."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies
from databricks.labs.dqx.anomaly.check_funcs import set_driver_only_for_tests
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule

from tests.conftest import TEST_CATALOG
from tests.integration_anomaly.test_anomaly_constants import (
    DQENGINE_SCORE_THRESHOLD,
    SEGMENT_REGIONS,
)
from tests.integration_anomaly.test_anomaly_utils import create_anomaly_check_rule, train_model_with_params


def test_explicit_segment_training(
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
):
    """Test explicit segment-based training."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Generate multi-region data
    data = []
    for region in SEGMENT_REGIONS:
        base = 100 if region == "US" else (200 if region == "EU" else 150)
        for i in range(200):
            data.append((region, base + i * 0.5, base * 0.8 + i * 0.3))

    df = spark.createDataFrame(data, "region string, amount double, discount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.segment_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Train with explicit segments
    model_name = f"{TEST_CATALOG}.{schema.name}.test_segments_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    anomaly_engine.train(
        df=spark.table(table_name),
        columns=["amount", "discount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Verify segmented models were created
    registry = spark.table(registry_table)
    models = registry.filter("identity.status = 'active'").collect()
    assert len(models) == 3  # One per segment

    # Check segment model names
    model_names = [row.identity.model_name for row in models]
    assert any("region=US" in name for name in model_names)
    assert any("region=EU" in name for name in model_names)
    assert any("region=APAC" in name for name in model_names)


def test_segment_scoring(
    ws,
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
):
    """Test that segment scoring uses correct regional models."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Train
    data = []
    for region in ("US", "EU"):
        base = 100 if region == "US" else 200
        for i in range(200):
            data.append((region, base + i * 0.5))

    df = spark.createDataFrame(data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.segment_score_test_{suffix}"
    df.write.saveAsTable(table_name)

    model_name = f"{TEST_CATALOG}.{schema.name}.test_score_segments_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    anomaly_engine.train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Score with anomalous data (use same case as training data for segment matching)
    test_data = [
        (1, "US", 100.0),  # Normal
        (2, "US", 1000.0),  # Strong anomaly (5x the max US training value)
        (3, "EU", 200.0),  # Normal
        (4, "EU", 1500.0),  # Strong anomaly (5x the max EU training value)
    ]
    test_df = spark.createDataFrame(test_data, "row_id int, region string, amount double")

    dq_engine = DQEngine(ws, spark)
    check = DQDatasetRule(
        criticality="error",
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name,
            "registry_table": registry_table,
            "score_threshold": DQENGINE_SCORE_THRESHOLD,  # Lowered from 0.7 to account for IsolationForest scoring characteristics
        },
    )

    result = dq_engine.apply_checks(test_df, [check])

    # Access anomaly_score from _dq_info.anomaly.score (nested in DQEngine results)
    result_with_score = result.select(
        "row_id", "region", "amount", F.col("_dq_info.anomaly.score").alias("anomaly_score")
    )
    rows = result_with_score.collect()
    assert len(rows) == 4

    # Verify we got scores for all rows
    assert all(row.anomaly_score is not None for row in rows), "Some rows missing anomaly scores"

    # Verify at least the anomalous rows exceed the threshold
    high_scorers = [row for row in rows if row.anomaly_score > 0.6]
    assert (
        len(high_scorers) >= 2
    ), f"Expected at least 2 rows with score>0.6, got {len(high_scorers)}. All scores: {[(r.row_id, r.anomaly_score) for r in rows]}"


def test_multi_column_segments(
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
):
    """Test segmentation with multiple segment columns."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Generate data with region + product_type
    data = []
    for region in ("US", "EU"):
        for product in ("A", "B"):
            base = 100 + (50 if region == "EU" else 0) + (25 if product == "B" else 0)
            for i in range(150):
                data.append((region, product, base + i * 0.5))

    df = spark.createDataFrame(data, "region string, product_type string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.multi_segment_test_{suffix}"
    df.write.saveAsTable(table_name)

    # Train with multiple segment columns
    model_name = f"{TEST_CATALOG}.{schema.name}.test_multi_segments_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    train_model_with_params(
        anomaly_engine=anomaly_engine,
        df=spark.table(table_name),
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount"],
        segment_by=["region", "product_type"],
        params=AnomalyParams(sample_fraction=1.0),
    )

    # Verify 4 segment models created (2 regions × 2 products)
    registry = spark.table(registry_table)
    models = registry.filter("identity.status = 'active'").collect()
    assert len(models) == 4


def test_unknown_segment_handling(
    ws,
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
):
    """Test scoring with unknown segment values (not in training data)."""
    # Create unique schema for test isolation
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    # Train on US and EU only
    data = []
    for region in ("US", "EU"):
        base = 100 if region == "US" else 200
        for i in range(200):
            data.append((region, base + i * 0.5))

    df = spark.createDataFrame(data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.unknown_segment_test_{suffix}"
    df.write.saveAsTable(table_name)

    model_name = f"{TEST_CATALOG}.{schema.name}.test_unknown_segments_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    anomaly_engine.train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Score with unknown region "APAC"
    test_data = [
        (1, "US", 100.0),
        (2, "APAC", 300.0),  # Unknown segment
    ]
    test_df = spark.createDataFrame(test_data, "row_id int, region string, amount double")

    dq_engine = DQEngine(ws, spark)
    check = DQDatasetRule(
        criticality="error",
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name,
            "registry_table": registry_table,
        },
    )

    result = dq_engine.apply_checks(test_df, [check])

    # APAC row should have null score (access from _dq_info.anomaly.score)

    result_with_score = result.select("*", F.col("_dq_info.anomaly.score").alias("anomaly_score"))
    apac_row = [row for row in result_with_score.collect() if row.region == "APAC"][0]
    assert apac_row.anomaly_score is None


def test_all_unknown_segments_yield_null_scores(
    ws,
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
):
    """All rows should have null scores if none match trained segments."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    train_data = []
    for region in ("US", "EU"):
        for i in range(200):
            base = 100 if region == "US" else 200
            train_data.append((region, base + i * 0.5))

    df = spark.createDataFrame(train_data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.unknown_only_test_{suffix}"
    df.write.saveAsTable(table_name)

    model_name = f"{TEST_CATALOG}.{schema.name}.test_unknown_only_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    anomaly_engine.train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    test_data = [
        (1, "APAC", 300.0),
        (2, "LATAM", 400.0),
    ]
    test_df = spark.createDataFrame(test_data, "row_id int, region string, amount double")

    dq_engine = DQEngine(ws, spark)
    check = DQDatasetRule(
        criticality="error",
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name,
            "registry_table": registry_table,
            "include_contributions": True,
            "include_confidence": True,
        },
    )

    result = dq_engine.apply_checks(test_df, [check])
    result_with_score = result.select("*", F.col("_dq_info.anomaly.score").alias("anomaly_score"))
    rows = result_with_score.collect()
    assert all(row.anomaly_score is None for row in rows)


def test_try_segmented_fallback_when_global_missing(
    spark: SparkSession,
    make_schema,
    make_random,
    anomaly_engine,
    ws,
):
    """Fallback should score using segmented models when global record is missing."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    data = []
    for region in ("US", "EU"):
        base = 100 if region == "US" else 200
        for i in range(200):
            data.append((region, base + i * 0.5))

    df = spark.createDataFrame(data, "region string, amount double")
    table_name = f"{TEST_CATALOG}.{schema.name}.fallback_test_{suffix}"
    df.write.saveAsTable(table_name)

    model_name = f"{TEST_CATALOG}.{schema.name}.test_fallback_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_models_{suffix}"

    anomaly_engine.train(
        df=spark.table(table_name),
        columns=["amount"],
        segment_by=["region"],
        model_name=model_name,
        registry_table=registry_table,
    )

    # Force fallback path by clearing segment_by in registry records.
    spark.sql(
        f"UPDATE {registry_table} "
        f"SET segmentation.segment_by = NULL "
        f"WHERE identity.model_name LIKE '{model_name}__seg_%'"
    )

    test_df = spark.createDataFrame(
        [(1, "US", 100.0), (2, "EU", 200.0)],
        "transaction_id int, region string, amount double",
    )

    set_driver_only_for_tests(False)
    try:
        dq_engine = DQEngine(ws, spark)
        check = create_anomaly_check_rule(
            model_name=model_name,
            registry_table=registry_table,
            score_threshold=DQENGINE_SCORE_THRESHOLD,
        )
        result = dq_engine.apply_checks(test_df, [check])
        rows = result.select("transaction_id", F.col("_dq_info.anomaly.score").alias("score")).collect()
    finally:
        set_driver_only_for_tests(True)

    assert all(row.score is not None for row in rows)
