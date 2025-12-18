"""
Integration tests for multi-type anomaly detection.

Tests end-to-end workflows with categorical, datetime, boolean, and numeric columns.
"""

import pytest
from datetime import datetime, timedelta

from databricks.labs.dqx.anomaly import train
from databricks.labs.dqx.anomaly.check_funcs import has_no_anomalies
from databricks.labs.dqx.config import AnomalyParams, FeatureEngineeringConfig


@pytest.fixture
def mixed_type_df(spark):
    """Create DataFrame with mixed column types for testing."""
    base_date = datetime(2023, 1, 1)

    data = []
    for i in range(1000):
        data.append(
            (
                i,  # id (will be excluded)
                float(i % 100),  # numeric feature
                ["A", "B", "C"][i % 3],  # categorical (low cardinality)
                i % 2 == 0,  # boolean
                base_date + timedelta(hours=i % 24),  # datetime with hourly pattern
                float((i % 50) + (10 if i % 100 == 0 else 0)),  # target with some anomalies
            )
        )

    df = spark.createDataFrame(
        data, "id int, value double, category string, flag boolean, timestamp timestamp, target double"
    )

    return df


@pytest.fixture
def catalog_schema(spark):
    """Get current catalog and schema."""
    catalog_row = spark.sql("SELECT current_catalog()").first()
    schema_row = spark.sql("SELECT current_schema()").first()
    assert catalog_row is not None and schema_row is not None, "Failed to get catalog/schema"
    catalog = catalog_row[0]
    schema = schema_row[0]
    return catalog, schema


class TestMultiTypeTraining:
    """Test training with mixed column types."""

    def test_train_with_categorical_and_numeric(self, spark, mixed_type_df, catalog_schema):
        """Test training with categorical and numeric columns."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train with numeric + categorical
        model_uri = train(
            df=mixed_type_df,
            columns=["value", "category", "target"],
            model_name=f"{catalog}.{schema}.test_cat_num_model",
            registry_table=registry_table,
            params=AnomalyParams(
                sample_fraction=0.5,
                max_rows=500,
            ),
        )

        assert model_uri is not None
        assert "models:/" in model_uri

    def test_train_with_datetime_features(self, spark, mixed_type_df, catalog_schema):
        """Test training with datetime columns."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train with datetime + numeric
        model_uri = train(
            df=mixed_type_df,
            columns=["value", "timestamp", "target"],
            model_name=f"{catalog}.{schema}.test_datetime_model",
            registry_table=registry_table,
            params=AnomalyParams(
                sample_fraction=0.5,
                max_rows=500,
            ),
        )

        assert model_uri is not None

    def test_train_with_boolean_features(self, spark, mixed_type_df, catalog_schema):
        """Test training with boolean columns."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train with boolean + numeric
        model_uri = train(
            df=mixed_type_df,
            columns=["value", "flag", "target"],
            model_name=f"{catalog}.{schema}.test_boolean_model",
            registry_table=registry_table,
            params=AnomalyParams(
                sample_fraction=0.5,
                max_rows=500,
            ),
        )

        assert model_uri is not None

    def test_train_with_all_types(self, spark, mixed_type_df, catalog_schema):
        """Test training with all supported column types."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train with all types
        model_uri = train(
            df=mixed_type_df,
            columns=["value", "category", "flag", "timestamp", "target"],
            model_name=f"{catalog}.{schema}.test_all_types_model",
            registry_table=registry_table,
            params=AnomalyParams(
                sample_fraction=0.5,
                max_rows=500,
            ),
        )

        assert model_uri is not None


class TestMultiTypeScoring:
    """Test scoring with mixed column types."""

    def test_score_with_categorical(self, spark, mixed_type_df, catalog_schema):
        """Test scoring with categorical columns."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train
        _model_uri = train(
            df=mixed_type_df.limit(500),
            columns=["value", "category"],
            model_name=f"{catalog}.{schema}.test_score_cat",
            registry_table=registry_table,
            params=AnomalyParams(sample_fraction=0.8, max_rows=400),
        )

        # Score new data
        test_df = mixed_type_df.filter("id >= 500")

        check_rule, apply_func = has_no_anomalies(
            columns=["value", "category"],
            model=f"{catalog}.{schema}.test_score_cat",
            registry_table=registry_table,
            score_threshold=0.6,
        )

        scored_df = apply_func(test_df)

        # Verify scoring succeeded
        assert "anomaly_score" in scored_df.columns
        assert scored_df.count() > 0

    def test_score_with_unknown_category(self, spark, catalog_schema):
        """Test that unknown categories are handled gracefully."""
        catalog, schema = catalog_schema
        registry_table = f"{catalog}.{schema}.test_multitype_models"

        # Train with limited categories
        train_df = spark.createDataFrame([(1.0, "A"), (2.0, "B"), (3.0, "A")], "value double, category string")

        _model_uri = train(
            df=train_df,
            columns=["value", "category"],
            model_name=f"{catalog}.{schema}.test_unknown_cat",
            registry_table=registry_table,
        )

        # Score with unknown category C
        test_df = spark.createDataFrame([(1.0, "A"), (2.0, "C")], "value double, category string")  # C is unknown

        check_rule, apply_func = has_no_anomalies(
            columns=["value", "category"],
            model=f"{catalog}.{schema}.test_unknown_cat",
            registry_table=registry_table,
        )

        scored_df = apply_func(test_df)

        # Should not crash - unknown categories handled gracefully
        assert scored_df.count() == 2
        assert "anomaly_score" in scored_df.columns


class TestLimits:
    """Test that limits are enforced."""

    def test_too_many_columns(self, spark, catalog_schema):
        """Test that training fails with >10 columns."""
        catalog, schema = catalog_schema

        # Create DataFrame with 15 columns
        cols = [f"col{i}" for i in range(15)]
        data = [(i,) * 15 for i in range(100)]
        df = spark.createDataFrame(data, ", ".join(f"{c} int" for c in cols))

        with pytest.raises(Exception, match="max 10 columns"):
            train(
                df=df,
                columns=cols,
                model_name=f"{catalog}.{schema}.test_too_many",
                registry_table=f"{catalog}.{schema}.test_multitype_models",
            )

    def test_too_many_features(self, spark, catalog_schema):
        """Test that training fails when engineered features exceed 50."""
        catalog, schema = catalog_schema

        # Create DataFrame with high-cardinality categoricals
        categories = [f"cat{i}" for i in range(25)]
        data = [(i, categories[i % 25], categories[(i + 1) % 25]) for i in range(100)]
        df = spark.createDataFrame(data, "id int, cat1 string, cat2 string")

        # This should exceed 50 features (25 + 25 = 50 for one-hot)
        with pytest.raises(Exception, match="Feature engineering would create"):
            train(
                df=df,
                columns=["cat1", "cat2"],
                model_name=f"{catalog}.{schema}.test_too_many_feat",
                registry_table=f"{catalog}.{schema}.test_multitype_models",
                params=AnomalyParams(
                    feature_engineering=FeatureEngineeringConfig(categorical_cardinality_threshold=30)  # Force one-hot
                ),
            )


class TestNullHandling:
    """Test null handling across different types."""

    def test_null_indicators_added(self, spark, catalog_schema):
        """Test that null indicators are added for columns with nulls."""
        catalog, schema = catalog_schema

        # DataFrame with nulls in all types
        df = spark.createDataFrame(
            [
                (1.0, "A", True, "2023-01-01"),
                (None, "B", False, "2023-01-02"),
                (3.0, None, None, None),
            ],
            "num double, cat string, flag boolean, date_str string",
        )

        df = df.withColumn("date", df.date_str.cast("timestamp")).drop("date_str")

        model_uri = train(
            df=df,
            columns=["num", "cat", "flag", "date"],
            model_name=f"{catalog}.{schema}.test_nulls",
            registry_table=f"{catalog}.{schema}.test_multitype_models",
        )

        assert model_uri is not None

    def test_high_null_rate_warning(self, spark, catalog_schema):
        """Test that high null rate generates warning but doesn't fail."""
        catalog, schema = catalog_schema

        # DataFrame with 70% nulls
        data = [(None,)] * 70 + [(1.0,)] * 30
        df = spark.createDataFrame(data, "value double")

        # Should warn but not fail
        with pytest.warns(UserWarning, match="73%|70%"):
            model_uri = train(
                df=df,
                columns=["value"],
                model_name=f"{catalog}.{schema}.test_high_nulls",
                registry_table=f"{catalog}.{schema}.test_multitype_models",
            )

        assert model_uri is not None


class TestAutoDiscovery:
    """Test auto-discovery with multi-type support."""

    def test_auto_discover_mixed_types(self, spark, mixed_type_df, catalog_schema):
        """Test that auto-discovery selects mixed column types."""
        catalog, schema = catalog_schema

        # Auto-discover columns (should prioritize numeric, then boolean, then categorical)
        model_uri = train(
            df=mixed_type_df,
            # columns=None,  # Auto-discover
            model_name=f"{catalog}.{schema}.test_autodiscover",
            registry_table=f"{catalog}.{schema}.test_multitype_models",
            params=AnomalyParams(sample_fraction=0.5, max_rows=500),
        )

        assert model_uri is not None
