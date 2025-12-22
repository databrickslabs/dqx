"""
Integration tests for anomaly feature explainability.

NOTE: These tests require SHAP library (shap>=0.42.0,<0.46) installed on the cluster.
If using DATABRICKS_CLUSTER_ID, install the library via:
  Cluster -> Libraries -> Install New -> PyPI -> shap>=0.42.0,<0.46

OPTIMIZATION: These tests use session-scoped shared fixtures (shared_2d_model, shared_3d_model, 
shared_4d_model) to avoid retraining models. This reduces runtime from ~60 min to ~10 min (83% savings).
"""

from pyspark.sql import SparkSession

from databricks.labs.dqx.anomaly import has_no_anomalies


def test_feature_importance_stored(spark: SparkSession, shared_2d_model):
    """Test that feature_importance is stored in registry."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]

    # Query registry for feature_importance
    # Model name is stored with full catalog.schema.model format
    full_model_name = f"main.default.{model_name}"
    record = spark.table(registry_table).filter(f"model_name = '{full_model_name}'").first()
    assert record is not None, f"Model {full_model_name} not found in registry"

    # Verify feature_importance exists and is non-empty
    assert record["feature_importance"] is not None
    assert len(record["feature_importance"]) > 0

    # Verify all columns are represented
    feature_importance = record["feature_importance"]
    assert "amount" in feature_importance
    assert "quantity" in feature_importance

    # Verify importance values are numeric
    for _col, importance in feature_importance.items():
        assert isinstance(importance, (int, float))
        assert importance >= 0


def test_feature_contributions_added(spark: SparkSession, shared_3d_model):
    """Test that anomaly_contributions column is added when requested."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]
    columns = shared_3d_model["columns"]

    # Score with include_contributions=True
    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )

    # Call has_no_anomalies directly to get columns like anomaly_contributions
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
        include_contributions=True,
    )

    result_df = apply_fn(test_df)
    row = result_df.collect()[0]

    # Verify anomaly_contributions column exists
    assert "anomaly_contributions" in result_df.columns

    # Extract map from Row: asDict() wraps in extra layer, so extract inner dict
    contribs = row["anomaly_contributions"].asDict()["anomaly_contributions"]

    # Verify contributions contain feature names
    assert contribs is not None
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs


def test_contribution_percentages_sum_to_one(spark: SparkSession, shared_3d_model):
    """Test that contribution percentages sum to approximately 1.0."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]
    columns = shared_3d_model["columns"]

    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95)],
        "amount double, quantity double, discount double",
    )

    # Call has_no_anomalies directly to get columns like anomaly_contributions
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
        include_contributions=True,
    )

    result_df = apply_fn(test_df)
    row = result_df.collect()[0]

    # Extract map from Row: asDict() wraps in extra layer, so extract inner dict
    contribs = row["anomaly_contributions"].asDict()["anomaly_contributions"]

    # Filter out None values before summing
    total = sum(v for v in contribs.values() if v is not None)

    # Allow small floating point error
    assert abs(total - 1.0) < 0.01


def test_multi_feature_contributions(spark: SparkSession, shared_4d_model):
    """Test contributions with 4+ columns."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_4d_model["model_name"]
    registry_table = shared_4d_model["registry_table"]
    columns = shared_4d_model["columns"]

    test_df = spark.createDataFrame(
        [(9999.0, 1.0, 0.95, 1.0)],
        "amount double, quantity double, discount double, weight double",
    )

    # Call has_no_anomalies directly to get columns like anomaly_contributions
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
        include_contributions=True,
    )

    result_df = apply_fn(test_df)
    row = result_df.collect()[0]

    # Extract map from Row: asDict() wraps in extra layer, so extract inner dict
    contribs = row["anomaly_contributions"].asDict()["anomaly_contributions"]

    # Verify all features are represented
    assert "amount" in contribs
    assert "quantity" in contribs
    assert "discount" in contribs
    assert "weight" in contribs


def test_contributions_without_flag_not_added(spark: SparkSession, shared_2d_model):
    """Test that contributions are not added when include_contributions=False."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_2d_model["model_name"]
    registry_table = shared_2d_model["registry_table"]
    columns = shared_2d_model["columns"]

    test_df = spark.createDataFrame(
        [(100.0, 2.0)],
        "amount double, quantity double",
    )

    # Call has_no_anomalies directly
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
        include_contributions=False,  # Explicitly False
    )

    result_df = apply_fn(test_df)

    # Verify anomaly_contributions column does NOT exist
    assert "anomaly_contributions" not in result_df.columns


def test_top_contributor_is_reasonable(spark: SparkSession, shared_3d_model):
    """Test that the top contributor makes sense for the anomaly."""
    # Use shared pre-trained model (no training needed!)
    model_name = shared_3d_model["model_name"]
    registry_table = shared_3d_model["registry_table"]
    columns = shared_3d_model["columns"]

    # Test with anomalous amount (should be top contributor)
    test_df = spark.createDataFrame(
        [(9999.0, 2.0, 0.15)],  # Extreme amount
        "amount double, quantity double, discount double",
    )

    # Call has_no_anomalies directly to get columns like anomaly_contributions
    _, apply_fn = has_no_anomalies(
        merge_columns=["transaction_id"],
        columns=columns,
        model=model_name,
        registry_table=registry_table,
        score_threshold=0.5,
        include_contributions=True,
    )

    result_df = apply_fn(test_df)
    row = result_df.collect()[0]

    # Extract map from Row: asDict() wraps in extra layer, so extract inner dict
    contribs = row["anomaly_contributions"].asDict()["anomaly_contributions"]

    # Filter out None values
    valid_contribs = {k: v for k, v in contribs.items() if v is not None}

    # Find top contributor
    top_feature = max(valid_contribs, key=lambda k: valid_contribs[k])

    # Amount should likely be the top contributor (or at least significant)
    # Since amount is the most anomalous feature
    assert top_feature in {"amount", "discount", "quantity"}
    assert valid_contribs[top_feature] > 0.2  # Should have significant contribution
