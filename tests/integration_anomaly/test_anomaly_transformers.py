"""Integration tests for anomaly feature transformers."""

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
