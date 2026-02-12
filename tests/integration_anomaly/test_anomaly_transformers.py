"""Integration tests for anomaly feature transformers."""

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeClassifier,
    SparkFeatureMetadata,
    apply_feature_engineering,
    reconstruct_column_infos,
)


def test_column_type_classifier_encodings(spark):
    rows = [(i, "A" if i % 2 == 0 else "B", f"user_{i}") for i in range(30)]
    df = spark.createDataFrame(rows, "amount int, category string, user_id string")

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=5)
    infos, _warnings = classifier.analyze_columns(df, ["amount", "category", "user_id"])

    info_by_name = {info.name: info for info in infos}

    assert info_by_name["amount"].category == "numeric"
    assert info_by_name["category"].category == "categorical"
    assert info_by_name["category"].encoding_strategy == "onehot"
    assert info_by_name["user_id"].category == "categorical"
    assert info_by_name["user_id"].encoding_strategy == "frequency"


def test_column_type_classifier_boolean_and_decimal(spark):
    rows = [(True, Decimal("1.50")), (False, Decimal("2.00")), (True, Decimal("3.00"))]
    df = spark.createDataFrame(rows, "flag boolean, amount decimal(10,2)")

    classifier = ColumnTypeClassifier()
    infos, _warnings = classifier.analyze_columns(df, ["flag", "amount"])

    info_by_name = {info.name: info for info in infos}
    assert info_by_name["flag"].category == "boolean"
    assert info_by_name["amount"].category == "numeric"


def test_column_type_classifier_all_null_string(spark):
    df = spark.createDataFrame([(None,), (None,), (None,)], "maybe_id string")

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=3)
    infos, _warnings = classifier.analyze_columns(df, ["maybe_id"])

    assert len(infos) == 1
    assert infos[0].category == "categorical"
    assert infos[0].null_count == df.select(F.count(F.when(F.col("maybe_id").isNull(), 1))).first()[0]


def test_column_type_classifier_high_cardinality_string(spark):
    rows = [(f"id_{i}",) for i in range(20)]
    df = spark.createDataFrame(rows, "user_id string")

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=5)
    infos, _warnings = classifier.analyze_columns(df, ["user_id"])

    assert infos[0].category == "categorical"
    assert infos[0].encoding_strategy == "frequency"


def test_apply_feature_engineering_all_types_with_metadata(spark):
    rows = [
        (1, "A", "user_1", True, "2024-01-01 01:00:00", 1.1, 10),
        (2, "B", "user_2", None, "2024-01-02 12:00:00", 2.2, 20),
        (None, None, "user_3", False, None, 3.3, 30),
    ]
    df = spark.createDataFrame(
        rows,
        "amount int, category string, user_id string, flag boolean, event_ts string, metric double, _dqx_row_id int",
    ).withColumn("event_ts", F.to_timestamp(F.col("event_ts")))

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=2)
    infos, warnings_list = classifier.analyze_columns(
        df, ["amount", "category", "user_id", "flag", "event_ts", "metric"]
    )

    assert warnings_list  # should include ID field warning for user_id

    engineered_df, metadata = apply_feature_engineering(df, infos, categorical_cardinality_threshold=2)
    engineered_cols = set(engineered_df.columns)

    # OneHot for category, frequency for user_id
    assert any(col.startswith("category_") for col in engineered_cols)
    assert "user_id_freq" in engineered_cols

    # Null indicators
    assert "amount_is_null" in engineered_cols
    assert "category_is_null" in engineered_cols
    assert "flag_is_null" in engineered_cols
    assert "event_ts_is_null" in engineered_cols

    # Datetime engineered features and drop original timestamp
    assert "event_ts_hour_sin" in engineered_cols
    assert "event_ts_dow_cos" in engineered_cols
    assert "event_ts_month_cos" in engineered_cols
    assert "event_ts_is_weekend" in engineered_cols
    assert "event_ts" not in engineered_cols

    # Boolean and numeric features
    assert "flag_bool" in engineered_cols
    assert "amount" in engineered_cols
    assert "metric" in engineered_cols

    # Extra columns preserved
    assert "_dqx_row_id" in engineered_cols

    # Metadata round-trip
    roundtrip = SparkFeatureMetadata.from_json(metadata.to_json())
    assert roundtrip.engineered_feature_names
    reconstructed = reconstruct_column_infos(roundtrip)
    assert any(info.name == "category" and info.category == "categorical" for info in reconstructed)


def test_apply_feature_engineering_missing_onehot_metadata_raises(spark):
    rows = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(rows, "amount int, category string")

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=5)
    infos, _warnings = classifier.analyze_columns(df, ["amount", "category"])

    with pytest.raises(ValueError, match="OneHot categories"):
        apply_feature_engineering(
            df,
            infos,
            categorical_cardinality_threshold=5,
            frequency_maps={"category": {}},
            onehot_categories={},
        )


def test_column_type_classifier_warnings_for_limits_and_unsupported(spark):
    rows = [(1, [1, 2], "user_1", "2024-01-01 00:00:00")]
    df = spark.createDataFrame(
        rows,
        T.StructType(
            [
                T.StructField("amount", T.IntegerType()),
                T.StructField("arr", T.ArrayType(T.IntegerType())),
                T.StructField("user_id", T.StringType()),
                T.StructField("event_ts", T.StringType()),
            ]
        ),
    ).withColumn("event_ts", F.to_timestamp(F.col("event_ts")))

    classifier = ColumnTypeClassifier(
        categorical_cardinality_threshold=1, max_input_columns=1, max_engineered_features=1
    )
    _infos, warnings_list = classifier.analyze_columns(df, ["amount", "arr", "user_id", "event_ts"])

    warnings_text = " ".join(warnings_list)
    assert "Training with" in warnings_text
    assert "Skipping unsupported columns" in warnings_text
    assert "Feature engineering will create" in warnings_text
    assert "appears to be an ID field" in warnings_text
    assert "datetime → 7 features" in warnings_text


def test_id_field_detection_with_high_cardinality_integers(spark):
    """Test ID field detection for integer columns with >1000 distinct and >80% unique."""
    # Create integer column with >1000 distinct values and >80% unique ratio
    data = [(i, 100.0 + i) for i in range(1500)]
    df = spark.createDataFrame(data, "user_id int, amount double")

    classifier = ColumnTypeClassifier()
    _infos, warnings = classifier.analyze_columns(df, ["user_id", "amount"])

    # Should have warning about user_id being an ID field
    warnings_text = " ".join(warnings)
    assert "user_id" in warnings_text
    assert "appears to be an ID field" in warnings_text
    assert "high cardinality" in warnings_text
    assert "exclude_columns" in warnings_text or "removing from columns list" in warnings_text


def test_float_columns_high_cardinality_no_id_warning(spark):
    """Test that float/double columns with high cardinality do NOT trigger ID warnings."""
    # Float columns naturally have high cardinality due to decimal precision
    # These should NOT be flagged as ID fields
    data = [(i * 1.5, i * 2.3, i * 0.7) for i in range(1500)]
    df = spark.createDataFrame(data, "speed_mph double, revenue double, temperature float")

    classifier = ColumnTypeClassifier()
    infos, warnings = classifier.analyze_columns(df, ["speed_mph", "revenue", "temperature"])

    # Should NOT have any ID field warnings (floats excluded from cardinality check)
    warnings_text = " ".join(warnings)
    assert "appears to be an ID field" not in warnings_text
    assert "high cardinality" not in warnings_text

    # All should be classified as numeric
    info_by_name = {info.name: info for info in infos}
    assert info_by_name["speed_mph"].category == "numeric"
    assert info_by_name["revenue"].category == "numeric"
    assert info_by_name["temperature"].category == "numeric"


def test_id_pattern_detection_in_column_names(spark):
    """Test ID field detection based on column name patterns (_id, _key, etc)."""
    data = [(i, i * 2, i * 3, 100.0 + i) for i in range(100)]
    df = spark.createDataFrame(data, "user_id int, account_key int, transaction_uuid int, amount double")

    classifier = ColumnTypeClassifier()
    _infos, warnings = classifier.analyze_columns(df, ["user_id", "account_key", "transaction_uuid", "amount"])

    # Should have warnings for columns matching ID patterns
    warnings_text = " ".join(warnings)
    assert "user_id" in warnings_text and "appears to be an ID field" in warnings_text
    assert "account_key" in warnings_text and "appears to be an ID field" in warnings_text
    assert "transaction_uuid" in warnings_text and "appears to be an ID field" in warnings_text

    # amount should NOT have ID warning
    assert warnings_text.count("appears to be an ID field") >= 3


def test_numeric_columns_with_null_handling(spark):
    """Test numeric column processing with null values and null indicators."""
    data = [(100.0, None, 50.0), (101.0, 200.0, None), (None, 201.0, 52.0)]
    df = spark.createDataFrame(data, "col1 double, col2 double, col3 double")

    classifier = ColumnTypeClassifier()
    infos, _warnings = classifier.analyze_columns(df, ["col1", "col2", "col3"])

    engineered_df, _metadata = apply_feature_engineering(df, infos)
    engineered_cols = set(engineered_df.columns)

    # Should have numeric columns
    assert "col1" in engineered_cols
    assert "col2" in engineered_cols
    assert "col3" in engineered_cols

    # Should have null indicators for all columns (all have nulls)
    assert "col1_is_null" in engineered_cols
    assert "col2_is_null" in engineered_cols
    assert "col3_is_null" in engineered_cols

    # Verify null imputation (should be 0.0)
    result = engineered_df.collect()
    assert result[0]["col2"] == 0.0  # Was None, imputed to 0.0
    assert result[1]["col3"] == 0.0  # Was None, imputed to 0.0
    assert result[2]["col1"] == 0.0  # Was None, imputed to 0.0


def test_categorical_onehot_vs_frequency_encoding(spark):
    """Test that low cardinality uses OneHot and high cardinality uses Frequency encoding."""
    # Low cardinality: 3 distinct values
    # High cardinality: 50 distinct values
    data = []
    for i in range(200):
        data.append((f"cat_{i % 3}", f"user_{i % 50}", 100.0 + i))

    df = spark.createDataFrame(data, "low_card string, high_card string, amount double")

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=20)
    infos, _warnings = classifier.analyze_columns(df, ["low_card", "high_card", "amount"])

    engineered_df, metadata = apply_feature_engineering(df, infos, categorical_cardinality_threshold=20)
    engineered_cols = set(engineered_df.columns)

    # Low cardinality should use OneHot (creates multiple columns)
    assert any(col.startswith("low_card_") for col in engineered_cols)
    assert "low_card_cat_0" in engineered_cols or "low_card_cat_1" in engineered_cols

    # High cardinality should use Frequency (creates single _freq column)
    assert "high_card_freq" in engineered_cols
    assert not any(col.startswith("high_card_user_") for col in engineered_cols)

    # Verify metadata stores encoding info
    assert "low_card" in metadata.onehot_categories
    assert "high_card" in metadata.categorical_frequency_maps


def test_frequency_encoding_unseen_category_maps_to_zero(spark):
    """Frequency encoding should map unseen categories to 0.0 during scoring."""
    train_df = spark.createDataFrame(
        [("u1", 10.0), ("u1", 11.0), ("u2", 12.0), ("u3", 13.0)],
        "high_card string, amount double",
    )
    score_df = spark.createDataFrame(
        [("u1", 20.0), ("u999", 21.0), (None, 22.0)],
        "high_card string, amount double",
    )

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=1)
    infos, _warnings = classifier.analyze_columns(train_df, ["high_card", "amount"])

    _engineered_train_df, metadata = apply_feature_engineering(train_df, infos, categorical_cardinality_threshold=1)
    engineered_score_df, _ = apply_feature_engineering(
        score_df,
        infos,
        categorical_cardinality_threshold=1,
        frequency_maps=metadata.categorical_frequency_maps,
        onehot_categories=metadata.onehot_categories,
    )

    rows = engineered_score_df.select("high_card_freq").collect()
    assert rows[0]["high_card_freq"] > 0.0  # seen category
    assert rows[1]["high_card_freq"] == 0.0  # unseen category
    assert rows[2]["high_card_freq"] == 0.0  # null -> MISSING unseen


def test_datetime_cyclical_encoding(spark):
    """Test datetime columns are encoded with cyclical sin/cos features."""
    data = [
        (datetime(2024, 1, 1, 10, 0, 0), 100.0),  # Monday, 10am
        (datetime(2024, 1, 6, 14, 0, 0), 101.0),  # Saturday, 2pm
        (datetime(2024, 1, 7, 18, 0, 0), 102.0),  # Sunday, 6pm
    ]
    df = spark.createDataFrame(data, "event_ts timestamp, amount double")

    classifier = ColumnTypeClassifier()
    infos, _warnings = classifier.analyze_columns(df, ["event_ts", "amount"])

    engineered_df, _metadata = apply_feature_engineering(df, infos)
    engineered_cols = set(engineered_df.columns)

    # Should have cyclical encoding features
    assert "event_ts_hour_sin" in engineered_cols
    assert "event_ts_hour_cos" in engineered_cols
    assert "event_ts_dow_sin" in engineered_cols
    assert "event_ts_dow_cos" in engineered_cols
    assert "event_ts_month_sin" in engineered_cols
    assert "event_ts_month_cos" in engineered_cols
    assert "event_ts_is_weekend" in engineered_cols

    # Original timestamp column should be dropped
    assert "event_ts" not in engineered_cols

    # Verify is_weekend feature (Saturday and Sunday should be 1.0)
    result = engineered_df.collect()
    assert result[0]["event_ts_is_weekend"] == 0.0  # Monday
    assert result[1]["event_ts_is_weekend"] == 1.0  # Saturday
    assert result[2]["event_ts_is_weekend"] == 1.0  # Sunday


def test_boolean_to_binary_encoding(spark):
    """Test boolean columns are encoded to 0.0/1.0 with null handling."""
    data = [(True, None, 100.0), (False, True, 101.0), (None, False, 102.0)]
    df = spark.createDataFrame(data, "flag1 boolean, flag2 boolean, amount double")

    classifier = ColumnTypeClassifier()
    infos, _warnings = classifier.analyze_columns(df, ["flag1", "flag2", "amount"])

    engineered_df, _metadata = apply_feature_engineering(df, infos)
    engineered_cols = set(engineered_df.columns)

    # Should have boolean features with _bool suffix
    assert "flag1_bool" in engineered_cols
    assert "flag2_bool" in engineered_cols

    # Should have null indicators
    assert "flag1_is_null" in engineered_cols
    assert "flag2_is_null" in engineered_cols

    # Verify boolean encoding: True→1.0, False→0.0, None→0.0
    result = engineered_df.collect()
    assert result[0]["flag1_bool"] == 1.0  # True
    assert result[0]["flag2_bool"] == 0.0  # None (imputed)
    assert result[1]["flag1_bool"] == 0.0  # False
    assert result[1]["flag2_bool"] == 1.0  # True
    assert result[2]["flag1_bool"] == 0.0  # None (imputed)


def test_mixed_column_types_in_single_dataframe(spark):
    """Test feature engineering with all column types mixed together."""
    data = [
        (100, 150.0, "cat_A", True, datetime(2024, 1, 1), "user_1"),
        (200, 250.0, "cat_B", False, datetime(2024, 1, 2), "user_2"),
        (None, None, None, None, None, "user_3"),
    ]
    df = spark.createDataFrame(
        data, "int_col int, float_col double, category string, flag boolean, ts timestamp, id string"
    )

    classifier = ColumnTypeClassifier(categorical_cardinality_threshold=5)
    infos, _warnings = classifier.analyze_columns(df, ["int_col", "float_col", "category", "flag", "ts"])

    engineered_df, _metadata = apply_feature_engineering(df, infos, categorical_cardinality_threshold=5)
    engineered_cols = set(engineered_df.columns)

    # Numeric features
    assert "int_col" in engineered_cols
    assert "float_col" in engineered_cols

    # Categorical (OneHot due to low cardinality)
    assert any(col.startswith("category_") for col in engineered_cols)

    # Boolean
    assert "flag_bool" in engineered_cols

    # Datetime (cyclical encoding)
    assert "ts_hour_sin" in engineered_cols
    assert "ts_is_weekend" in engineered_cols

    # Null indicators (all columns have nulls in row 3)
    assert "int_col_is_null" in engineered_cols
    assert "float_col_is_null" in engineered_cols
    assert "category_is_null" in engineered_cols
    assert "flag_is_null" in engineered_cols
    assert "ts_is_null" in engineered_cols

    # Extra columns preserved
    assert "id" in engineered_cols
