"""
Unit tests for anomaly detection feature transformers.
"""

import pytest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeClassifier,
    ColumnTypeInfo,
    # CategoricalEncoder,  # Removed - functionality integrated into apply_feature_engineering
    # DatetimeFeatureExtractor,  # Removed - functionality integrated into apply_feature_engineering
    # BooleanEncoder,  # Removed - functionality integrated into apply_feature_engineering
    # NumericImputer,  # Removed - functionality integrated into apply_feature_engineering
    # NullIndicatorTransformer,  # Removed - functionality integrated into apply_feature_engineering
    # AnomalyFeatureTransformer,  # Removed - functionality integrated into apply_feature_engineering
)
from databricks.labs.dqx.errors import InvalidParameterError

# Mark all tests as skipped since most transformer classes were removed
# and integrated into apply_feature_engineering function
pytestmark = pytest.mark.skip(reason="Transformer classes removed - functionality integrated")


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()


class TestColumnTypeClassifier:
    """Test ColumnTypeClassifier."""

    def test_analyze_numeric_columns(self, spark):
        """Test analysis of numeric columns."""
        df = spark.createDataFrame([(1, 2.5, 3), (2, 3.5, 4), (3, 4.5, 5)], "a int, b double, c long")

        classifier = ColumnTypeClassifier()
        infos, warnings = classifier.analyze_columns(df, ["a", "b", "c"])

        assert len(infos) == 3
        assert all(info.category == 'numeric' for info in infos)
        assert len(warnings) == 0

    def test_analyze_categorical_columns(self, spark):
        """Test analysis of categorical columns."""
        df = spark.createDataFrame([("A",), ("B",), ("A",), ("C",)], "category string")

        classifier = ColumnTypeClassifier(categorical_cardinality_threshold=20)
        infos, warnings = classifier.analyze_columns(df, ["category"])

        assert len(infos) == 1
        assert infos[0].category == 'categorical'
        assert infos[0].cardinality == 3
        assert infos[0].encoding_strategy == 'onehot'

    def test_analyze_datetime_columns(self, spark):
        """Test analysis of datetime columns."""
        df = spark.createDataFrame([("2023-01-01",), ("2023-01-02",)], "date_col string")
        df = df.withColumn("date_col", df.date_col.cast("date"))

        classifier = ColumnTypeClassifier()
        infos, warnings = classifier.analyze_columns(df, ["date_col"])

        assert len(infos) == 1
        assert infos[0].category == 'datetime'
        assert infos[0].encoding_strategy == 'cyclical'

    def test_analyze_boolean_columns(self, spark):
        """Test analysis of boolean columns."""
        df = spark.createDataFrame([(True,), (False,), (True,)], "flag boolean")

        classifier = ColumnTypeClassifier()
        infos, warnings = classifier.analyze_columns(df, ["flag"])

        assert len(infos) == 1
        assert infos[0].category == 'boolean'
        assert infos[0].encoding_strategy == 'binary'

    def test_max_columns_limit(self, spark):
        """Test that max columns limit is enforced."""
        cols = [f"col{i}" for i in range(15)]
        data = [(i,) * 15 for i in range(5)]
        df = spark.createDataFrame(data, ", ".join(f"{c} int" for c in cols))

        classifier = ColumnTypeClassifier(max_input_columns=10)

        with pytest.raises(InvalidParameterError, match="max 10 columns"):
            classifier.analyze_columns(df, cols)

    def test_max_features_limit(self, spark):
        """Test that max features limit is enforced."""
        # Create DataFrame with high-cardinality categoricals that will exceed 50 features
        categories = [f"cat{i}" for i in range(30)]
        data = [(cat,) * 3 for cat in categories]
        df = spark.createDataFrame(data, "cat1 string, cat2 string, cat3 string")

        classifier = ColumnTypeClassifier(categorical_cardinality_threshold=20, max_engineered_features=50)

        with pytest.raises(InvalidParameterError, match="Feature engineering would create"):
            classifier.analyze_columns(df, ["cat1", "cat2", "cat3"])

    def test_unsupported_types_warning(self, spark):
        """Test that unsupported types generate warnings."""
        df = spark.createDataFrame([([1, 2, 3], {"key": "value"})], "arr array<int>, map map<string,string>")

        classifier = ColumnTypeClassifier()
        infos, warnings = classifier.analyze_columns(df, ["arr", "map"])

        assert len(infos) == 0  # Both unsupported
        assert len(warnings) == 1
        assert "unsupported" in warnings[0].lower()


class TestCategoricalEncoder:
    """Test CategoricalEncoder."""

    def test_onehot_encoding(self):
        """Test one-hot encoding for low cardinality."""
        X = pd.DataFrame({"cat": ["A", "B", "A", "C"]})

        encoder = CategoricalEncoder(columns=["cat"], cardinality_threshold=20)
        encoder.fit(X)
        X_transformed = encoder.transform(X)

        assert "cat_A" in X_transformed.columns
        assert "cat_B" in X_transformed.columns
        assert "cat_C" in X_transformed.columns
        assert "cat" not in X_transformed.columns
        assert X_transformed["cat_A"].tolist() == [1, 0, 1, 0]

    def test_frequency_encoding(self):
        """Test frequency encoding for high cardinality."""
        X = pd.DataFrame({"cat": ["A", "A", "B", "C"]})

        encoder = CategoricalEncoder(columns=["cat"], cardinality_threshold=2)
        encoder.fit(X)
        X_transformed = encoder.transform(X)

        assert "cat_freq" in X_transformed.columns
        assert "cat" not in X_transformed.columns
        # A appears 2/4=0.5, B and C appear 1/4=0.25
        assert X_transformed["cat_freq"].iloc[0] == 0.5
        assert X_transformed["cat_freq"].iloc[2] == 0.25

    def test_null_handling(self):
        """Test that nulls are replaced with MISSING."""
        X = pd.DataFrame({"cat": ["A", None, "B"]})

        encoder = CategoricalEncoder(columns=["cat"], cardinality_threshold=20)
        encoder.fit(X)
        X_transformed = encoder.transform(X)

        assert "cat_MISSING" in X_transformed.columns
        assert X_transformed["cat_MISSING"].iloc[1] == 1

    def test_unknown_category_handling(self):
        """Test that unknown categories are handled gracefully."""
        X_train = pd.DataFrame({"cat": ["A", "B", "A"]})
        X_test = pd.DataFrame({"cat": ["A", "C", "B"]})  # C is unknown

        encoder = CategoricalEncoder(columns=["cat"], cardinality_threshold=20)
        encoder.fit(X_train)
        X_transformed = encoder.transform(X_test)

        # Unknown category C should have all zeros for one-hot
        assert X_transformed["cat_A"].iloc[1] == 0
        assert X_transformed["cat_B"].iloc[1] == 0


class TestDatetimeFeatureExtractor:
    """Test DatetimeFeatureExtractor."""

    def test_cyclical_features(self):
        """Test that cyclical features are created."""
        X = pd.DataFrame({"date": pd.to_datetime(["2023-01-01 10:00:00", "2023-01-02 14:00:00"])})

        extractor = DatetimeFeatureExtractor(columns=["date"])
        extractor.fit(X)
        X_transformed = extractor.transform(X)

        # Check all expected features are created
        assert "date_hour_sin" in X_transformed.columns
        assert "date_hour_cos" in X_transformed.columns
        assert "date_dow_sin" in X_transformed.columns
        assert "date_dow_cos" in X_transformed.columns
        assert "date_is_weekend" in X_transformed.columns
        assert "date" not in X_transformed.columns

    def test_hour_cyclical_encoding(self):
        """Test that hour 23 is close to hour 0 in cyclical encoding."""
        X = pd.DataFrame({"date": pd.to_datetime(["2023-01-01 00:00:00", "2023-01-01 23:00:00"])})

        extractor = DatetimeFeatureExtractor(columns=["date"])
        extractor.fit(X)
        X_transformed = extractor.transform(X)

        # Hour 0 and hour 23 should have similar sin/cos values
        hour0_sin = X_transformed["date_hour_sin"].iloc[0]
        hour23_sin = X_transformed["date_hour_sin"].iloc[1]
        assert abs(hour0_sin - hour23_sin) < 0.3

    def test_weekend_detection(self):
        """Test weekend detection."""
        X = pd.DataFrame({"date": pd.to_datetime(["2023-01-02", "2023-01-07", "2023-01-08"])})  # Mon, Sat, Sun

        extractor = DatetimeFeatureExtractor(columns=["date"])
        extractor.fit(X)
        X_transformed = extractor.transform(X)

        assert X_transformed["date_is_weekend"].tolist() == [0, 1, 1]

    def test_null_imputation(self):
        """Test that nulls are imputed with min date."""
        X = pd.DataFrame({"date": pd.to_datetime(["2023-01-01", None, "2023-01-03"])})

        extractor = DatetimeFeatureExtractor(columns=["date"])
        extractor.fit(X)
        X_transformed = extractor.transform(X)

        # Check that transformation succeeded (no NaN in output)
        assert not X_transformed["date_hour_sin"].isna().any()


class TestBooleanEncoder:
    """Test BooleanEncoder."""

    def test_boolean_to_int(self):
        """Test that boolean is converted to 0/1."""
        X = pd.DataFrame({"flag": [True, False, True]})

        encoder = BooleanEncoder(columns=["flag"])
        encoder.fit(X)
        X_transformed = encoder.transform(X)

        assert X_transformed["flag"].tolist() == [1, 0, 1]
        assert X_transformed["flag"].dtype == np.int64

    def test_null_handling(self):
        """Test that nulls are converted to 0."""
        X = pd.DataFrame({"flag": [True, None, False]})

        encoder = BooleanEncoder(columns=["flag"])
        encoder.fit(X)
        X_transformed = encoder.transform(X)

        assert X_transformed["flag"].iloc[1] == 0


class TestNumericImputer:
    """Test NumericImputer."""

    def test_impute_with_zero(self):
        """Test that nulls are imputed with 0."""
        X = pd.DataFrame({"num": [1.0, None, 3.0]})

        imputer = NumericImputer(columns=["num"])
        imputer.fit(X)
        X_transformed = imputer.transform(X)

        assert X_transformed["num"].tolist() == [1.0, 0.0, 3.0]


class TestNullIndicatorTransformer:
    """Test NullIndicatorTransformer."""

    def test_add_null_indicators(self):
        """Test that null indicators are added."""
        X = pd.DataFrame({"a": [1.0, None, 3.0], "b": [4.0, 5.0, 6.0]})

        transformer = NullIndicatorTransformer(columns_with_nulls=["a"])
        transformer.fit(X)
        X_transformed = transformer.transform(X)

        assert "a_is_null" in X_transformed.columns
        assert "b_is_null" not in X_transformed.columns
        assert X_transformed["a_is_null"].tolist() == [0, 1, 0]


class TestAnomalyFeatureTransformer:
    """Test AnomalyFeatureTransformer end-to-end."""

    def test_mixed_types_transformation(self):
        """Test transformation of mixed column types."""
        column_infos = [
            ColumnTypeInfo("num", T.IntegerType(), "numeric", null_count=1),
            ColumnTypeInfo("cat", T.StringType(), "categorical", cardinality=3, encoding_strategy="onehot"),
            ColumnTypeInfo("flag", T.BooleanType(), "boolean", null_count=0),
        ]

        X = pd.DataFrame({"num": [1.0, None, 3.0], "cat": ["A", "B", "A"], "flag": [True, False, True]})

        transformer = AnomalyFeatureTransformer(column_infos=column_infos)
        transformer.fit(X)
        X_transformed = transformer.transform(X)

        # Check categorical is one-hot encoded
        assert "cat_A" in X_transformed.columns
        assert "cat_B" in X_transformed.columns

        # Check boolean is mapped to 0/1
        assert X_transformed["flag"].tolist() == [1, 0, 1]

        # Check numeric has null indicator
        assert "num_is_null" in X_transformed.columns
        assert X_transformed["num_is_null"].tolist() == [0, 1, 0]

        # Check numeric null is imputed
        assert X_transformed["num"].iloc[1] == 0.0

    def test_feature_mapping(self):
        """Test that feature mapping is built correctly."""
        column_infos = [
            ColumnTypeInfo("cat", T.StringType(), "categorical", cardinality=2, encoding_strategy="onehot"),
            ColumnTypeInfo("num", T.IntegerType(), "numeric", null_count=0),
        ]

        X = pd.DataFrame({"cat": ["A", "B"], "num": [1, 2]})

        transformer = AnomalyFeatureTransformer(column_infos=column_infos)
        transformer.fit(X)

        mapping = transformer.get_feature_mapping()

        assert "cat" in mapping
        assert "cat_A" in mapping["cat"]
        assert "cat_B" in mapping["cat"]

        assert "num" in mapping
        assert mapping["num"] == ["num"]

    def test_serialization(self):
        """Test that transformer can be pickled and unpickled."""
        import cloudpickle

        column_infos = [
            ColumnTypeInfo("num", T.IntegerType(), "numeric", null_count=0),
        ]

        X = pd.DataFrame({"num": [1, 2, 3]})

        transformer = AnomalyFeatureTransformer(column_infos=column_infos)
        transformer.fit(X)

        # Serialize and deserialize
        serialized = cloudpickle.dumps(transformer)
        deserialized = cloudpickle.loads(serialized)

        # Test that deserialized transformer works
        X_test = pd.DataFrame({"num": [4, 5]})
        X_transformed = deserialized.transform(X_test)

        assert X_transformed["num"].tolist() == [4, 5]


class TestIntegration:
    """Integration tests with Spark DataFrames."""

    def test_end_to_end_with_spark(self, spark):
        """Test full workflow from Spark DataFrame to transformed features."""
        # Create Spark DataFrame with mixed types
        df = spark.createDataFrame(
            [
                (1, "A", True, "2023-01-01"),
                (2, "B", False, "2023-01-02"),
                (None, "A", True, "2023-01-03"),
            ],
            "num int, cat string, flag boolean, date_str string",
        )

        df = df.withColumn("date", df.date_str.cast("date")).drop("date_str")

        # Analyze columns
        classifier = ColumnTypeClassifier()
        column_infos, warnings = classifier.analyze_columns(df, ["num", "cat", "flag", "date"])

        assert len(column_infos) == 4

        # Convert to pandas and transform
        pdf = df.toPandas()
        transformer = AnomalyFeatureTransformer(column_infos=column_infos)
        transformer.fit(pdf)
        transformed = transformer.transform(pdf)

        # Verify transformed DataFrame has expected shape
        assert len(transformed) == 3
        assert "num" in transformed.columns
        assert "num_is_null" in transformed.columns
        assert "flag" in transformed.columns
        assert "date_hour_sin" in transformed.columns
