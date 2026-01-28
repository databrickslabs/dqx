"""Integration tests for anomaly feature transformers."""

from decimal import Decimal

from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly.transformers import ColumnTypeClassifier


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
