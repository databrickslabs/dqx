import warnings

import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_anomalies
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.dqx.anomaly.transformers import ColumnTypeClassifier
from databricks.labs.dqx.engine import DQEngine
from tests.conftest import TEST_CATALOG
from tests.integration_anomaly.test_anomaly_utils import get_standard_2d_training_data


def test_exclude_columns_precedence(ws, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    model_name = f"{TEST_CATALOG}.{schema.name}.test_exclude_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"

    data = [(1, 100.0, 2.0), (2, 110.0, 3.0), (3, 120.0, 4.0)]
    df = spark.createDataFrame(data, "user_id int, amount double, quantity double")

    engine = AnomalyEngine(ws, spark)
    engine.train(
        df=df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity", "user_id"],
        exclude_columns=["user_id"],
    )

    record = spark.table(registry_table).filter(F.col("identity.model_name") == model_name).first()
    assert record is not None
    assert "user_id" not in record.training.columns


def test_drift_warning_emitted(ws, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    model_name = f"{TEST_CATALOG}.{schema.name}.test_drift_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"

    train_df = spark.createDataFrame(get_standard_2d_training_data(), "amount double, quantity double")
    engine = AnomalyEngine(ws, spark)
    engine.train(
        df=train_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity"],
    )

    drift_data = [(1000.0 + i, 200.0 + i) for i in range(1500)]
    drift_df = spark.createDataFrame(drift_data, "amount double, quantity double")

    dq_engine = DQEngine(ws, spark)
    check = DQDatasetRule(
        check_func=has_no_anomalies,
        check_func_kwargs={
            "model": model_name,
            "registry_table": registry_table,
            "drift_threshold": 0.5,
        },
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = dq_engine.apply_checks(drift_df, [check])
        drift_warnings = [w for w in caught if "DISTRIBUTION DRIFT DETECTED" in str(w.message)]

    assert drift_warnings, "Expected a drift warning to be emitted"


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
