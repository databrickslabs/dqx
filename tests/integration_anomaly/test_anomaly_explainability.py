"""Integration tests for public anomaly feature contributions and rendered messages."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, DoubleType, MapType, StringType, StructField, StructType

from databricks.labs.dqx.anomaly import explainability as explainability_mod
from databricks.labs.dqx.anomaly.explainability import (
    add_top_contributors_to_message,
)
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)
from tests.integration_anomaly.conftest import score_3d_with_contributions


def test_feature_contributions_added(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that anomaly_contributions column is added when requested."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use helper to score with contributions
    result_df = score_3d_with_contributions(
        spark,
        test_df_factory,
        anomaly_scorer,
        model_name,
        registry_table,
    )
    row = result_df.collect()[0]

    # Verify contributions exist in _dq_info[0].anomaly.contributions
    assert row["_dq_info"][0]["anomaly"]["contributions"] is not None

    # Extract contributions map from _dq_info[0].anomaly.contributions
    contribs = row["_dq_info"][0]["anomaly"]["contributions"]

    # Verify contributions contain feature names
    assert contribs is not None
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs


def test_contribution_percentages_sum_to_hundred(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that contribution percentages sum to approximately 100.0."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )

    # Use anomaly_scorer with enable_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info[0].anomaly.contributions
    contribs = row["_dq_info"][0]["anomaly"]["contributions"]

    # Filter out None values before summing
    total = sum(v for v in contribs.values() if v is not None)

    # Allow small floating point error
    assert abs(total - 100.0) < 0.5


def test_multi_feature_contributions(spark: SparkSession, shared_4d_model, test_df_factory, anomaly_scorer):
    """Test contributions with 4+ columns."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_4d_model["model_name"]
    registry_table = shared_4d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95, 1.0)],
        columns_schema="amount double, quantity double, discount double, weight double",
    )

    # Use anomaly_scorer with enable_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info[0].anomaly.contributions
    contribs = row["_dq_info"][0]["anomaly"]["contributions"]

    # Verify all features are represented
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs
    assert "weight" in contribs


def test_contributions_without_flag_not_added(spark: SparkSession, shared_2d_model, test_df_factory, anomaly_scorer):
    """Test that contributions are not added when enable_contributions=False."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Use factory to create test DataFrame with transaction_id
    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    # Use anomaly_scorer with enable_contributions=False
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=False,  # Explicitly False
        extract_score=False,
    )

    rows = result_df.collect()
    assert len(rows) == test_df.count()
    assert all(row["_dq_info"][0]["anomaly"]["contributions"] is None for row in rows)
    assert all(row["_dq_info"][0]["anomaly"]["basis_contributions"] is None for row in rows)
    assert "anomaly_contributions" not in result_df.columns


def test_top_contributor_is_reasonable(spark: SparkSession, shared_3d_model, test_df_factory, anomaly_scorer):
    """Test that the top contributor makes sense for the anomaly."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]

    # Use factory to create test DataFrame with extreme amount value
    test_df = test_df_factory(
        spark,
        normal_rows=[],
        anomaly_rows=[(OUTLIER_AMOUNT, 30.0, 0.3)],  # Only amount is outside the training distribution
        columns_schema="amount double, quantity double, discount double",
    )

    # Use anomaly_scorer with enable_contributions
    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]

    # Extract contributions map from _dq_info[0].anomaly.contributions
    contribs = row["_dq_info"][0]["anomaly"]["contributions"]

    # Filter out None values
    valid_contribs = {k: v for k, v in contribs.items() if v is not None}

    # Find top contributor
    top_feature = max(valid_contribs, key=lambda k: valid_contribs[k])

    # Amount should likely be the top contributor (or at least significant)
    # Since amount is the most anomalous feature
    assert top_feature == "amount", f"only amount was perturbed, but attribution ranked {valid_contribs}"
    assert valid_contribs["amount"] > sum(value for name, value in valid_contribs.items() if name != "amount")


def test_add_top_contributors_message(spark: SparkSession):
    """Top contributors are added only for rows above threshold."""
    df = spark.createDataFrame(
        [
            (0.7, 70.0, {"amount": 80.0, "quantity": 20.0}),
            (0.2, 20.0, {"amount": 90.0, "quantity": 10.0}),
        ],
        "anomaly_score double, severity_percentile double, anomaly_contributions map<string,double>",
    )

    result = add_top_contributors_to_message(df, threshold=60.0, top_n=2)
    assert "_top_contributors" in result.columns

    rows = result.orderBy("severity_percentile").collect()
    assert rows[0]["_top_contributors"] == ""
    assert rows[1]["_top_contributors"] == "amount (80%), quantity (20%)"


def test_driver_only_contributions_smoke(spark: SparkSession, shared_2d_model, test_df_factory, anomaly_scorer):
    """Compute SHAP contributions in driver-only mode (Spark Connect safe)."""
    if explainability_mod.SHAP is None:
        pytest.skip("Explainability dependencies not available")

    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    test_df = test_df_factory(
        spark,
        normal_rows=[(100.0, 2.0)],
        anomaly_rows=[],
        columns_schema="amount double, quantity double",
    )

    result_df = anomaly_scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        extract_score=False,
    )
    row = result_df.collect()[0]
    contribs = row["_dq_info"][0]["anomaly"]["contributions"]
    assert contribs is not None
    assert "amount" in contribs and "quantity" in contribs


def test_add_top_contributors_uses_dq_info(spark: SparkSession):
    """Top contributors can be derived from _dq_info[0].anomaly.severity_percentile (array of struct)."""
    # _dq_info is array<struct<anomaly: struct<...>>>
    info_element_schema = StructType(
        [
            StructField(
                "anomaly",
                StructType([StructField("severity_percentile", DoubleType(), True)]),
                True,
            )
        ]
    )
    schema = StructType(
        [
            StructField("anomaly_contributions", MapType(StringType(), DoubleType()), True),
            StructField("_dq_info", ArrayType(info_element_schema), True),
        ]
    )
    one_row = (
        {"amount": 70.0, "quantity": 30.0},
        [{"anomaly": {"severity_percentile": 80.0}}],
    )
    df = spark.createDataFrame([one_row], schema=schema)

    result = add_top_contributors_to_message(df, threshold=60.0, top_n=2)
    assert "_top_contributors" in result.columns
    row = result.collect()[0]
    assert row["_top_contributors"] == "amount (70%), quantity (30%)"
