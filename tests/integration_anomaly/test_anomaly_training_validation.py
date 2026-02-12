"""Integration tests for anomaly training parameter validation."""

import pytest

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.errors import InvalidParameterError
from tests.constants import TEST_CATALOG


def _build_training_df(spark):
    rows = [(100.0, 10.0), (101.0, 11.0), (102.0, 12.0), (103.0, 13.0)]
    return spark.createDataFrame(rows, "amount double, quantity double")


def test_train_rejects_invalid_sample_fraction(anomaly_engine, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.invalid_sample_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.invalid_sample_reg_{make_random(4).lower()}"

    with pytest.raises(InvalidParameterError, match="params.sample_fraction"):
        anomaly_engine.train(
            df=_build_training_df(spark),
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            params=AnomalyParams(sample_fraction=0.0),
        )


def test_train_rejects_invalid_expected_anomaly_rate(anomaly_engine, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.invalid_expected_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.invalid_expected_reg_{make_random(4).lower()}"

    with pytest.raises(InvalidParameterError, match="expected_anomaly_rate"):
        anomaly_engine.train(
            df=_build_training_df(spark),
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            expected_anomaly_rate=0.9,
        )


def test_train_rejects_invalid_contamination(anomaly_engine, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.invalid_contam_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.invalid_contam_reg_{make_random(4).lower()}"

    with pytest.raises(InvalidParameterError, match="params.algorithm_config.contamination"):
        anomaly_engine.train(
            df=_build_training_df(spark),
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            params=AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.75)),
        )
