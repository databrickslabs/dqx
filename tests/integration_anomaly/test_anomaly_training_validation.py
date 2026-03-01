"""Integration tests for anomaly training parameter validation."""

import pytest

from databricks.labs.dqx.anomaly.service import AnomalyTrainingService
from databricks.labs.dqx.config import AnomalyParams, FeatureEngineeringConfig, IsolationForestConfig
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


def test_build_context_raises_for_empty_model_name(spark, make_schema, make_random):
    """build_context raises when model_name is empty."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    registry_table = f"{TEST_CATALOG}.{schema}.empty_model_reg_{make_random(4).lower()}"
    df = _build_training_df(spark)
    service = AnomalyTrainingService(spark)

    with pytest.raises(InvalidParameterError, match="model_name is required and must be fully qualified"):
        service.build_context(
            df,
            model_name="",
            registry_table=registry_table,
            columns=["amount", "quantity"],
            segment_by=None,
            params=None,
            exclude_columns=None,
            expected_anomaly_rate=0.02,
        )


def test_build_context_raises_for_empty_registry_table(spark, make_schema, make_random):
    """build_context raises when registry_table is empty."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.empty_reg_{make_random(4).lower()}"
    df = _build_training_df(spark)
    service = AnomalyTrainingService(spark)

    with pytest.raises(InvalidParameterError, match="registry_table is required and must be fully qualified"):
        service.build_context(
            df,
            model_name=model_name,
            registry_table="",
            columns=["amount", "quantity"],
            segment_by=None,
            params=None,
            exclude_columns=None,
            expected_anomaly_rate=0.02,
        )


def test_build_context_raises_for_empty_columns(spark, make_schema, make_random):
    """build_context raises when columns is empty."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.empty_cols_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.empty_cols_reg_{make_random(4).lower()}"
    df = _build_training_df(spark)
    service = AnomalyTrainingService(spark)

    with pytest.raises(
        InvalidParameterError, match="No columns provided or auto-discovered. Provide columns explicitly."
    ):
        service.build_context(
            df,
            model_name=model_name,
            registry_table=registry_table,
            columns=[],
            segment_by=None,
            params=None,
            exclude_columns=None,
            expected_anomaly_rate=0.02,
        )


def test_train_warns_when_model_already_exists(anomaly_engine, spark, make_schema, make_random, caplog):
    """Second train with same model_name logs warning."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.existing_model_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.existing_model_reg_{make_random(4).lower()}"

    data = [(float(i), 10.0 + i) for i in range(20)]
    df = spark.createDataFrame(data, "amount double, quantity double")

    # Single model (ensemble_size=None) so the base model_name is registered in UC
    params = AnomalyParams(ensemble_size=None)
    anomaly_engine.train(
        df=df,
        model_name=model_name,
        registry_table=registry_table,
        columns=["amount", "quantity"],
        params=params,
    )

    with caplog.at_level("WARNING"):
        anomaly_engine.train(
            df=df,
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            params=params,
        )

    assert "already exists" in caplog.text
    assert "Creating a new version" in caplog.text


def test_build_context_logs_validation_warnings(spark, make_schema, make_random, caplog):
    """build_context logs each validation warning from validate_columns."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.warn_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.warn_reg_{make_random(4).lower()}"
    df = _build_training_df(spark)
    # Params with max_input_columns=1 so 2 columns trigger a validation warning
    params = AnomalyParams(feature_engineering=FeatureEngineeringConfig(max_input_columns=1))
    service = AnomalyTrainingService(spark)

    with caplog.at_level("WARNING"):
        service.build_context(
            df,
            model_name=model_name,
            registry_table=registry_table,
            columns=["amount", "quantity"],
            segment_by=None,
            params=params,
            exclude_columns=None,
            expected_anomaly_rate=0.02,
        )

    assert "Training with 2 columns" in caplog.text
    assert "recommended max: 1" in caplog.text


def test_build_context_excludes_columns_from_auto_discovery(spark, make_schema, make_random):
    """With columns=None and exclude_columns, df_filtered drops excluded cols."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.exclude_cols_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.exclude_cols_reg_{make_random(4).lower()}"

    data = [(float(i), float(i * 2), float(i * 3)) for i in range(1, 11)]
    df = spark.createDataFrame(data, schema=["a", "b", "c"])
    service = AnomalyTrainingService(spark)
    ctx = service.build_context(
        df,
        model_name=model_name,
        registry_table=registry_table,
        columns=None,
        segment_by=None,
        params=None,
        exclude_columns=["b"],
        expected_anomaly_rate=0.02,
    )
    assert ctx.df_filtered.columns == ["a", "c"]
    assert ctx.auto_discovery_used is True


def test_build_context_raises_when_exclude_columns_not_in_dataframe(spark, make_schema, make_random):
    """build_context raises when exclude_columns lists columns not in the DataFrame."""
    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.exclude_invalid_{make_random(4).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.exclude_invalid_reg_{make_random(4).lower()}"

    df = spark.createDataFrame([(1.0, 2.0)], "a double, b double")
    service = AnomalyTrainingService(spark)

    with pytest.raises(InvalidParameterError, match="exclude_columns contains columns not in DataFrame"):
        service.build_context(
            df,
            model_name=model_name,
            registry_table=registry_table,
            columns=["a", "b"],
            segment_by=None,
            params=None,
            exclude_columns=["c"],
            expected_anomaly_rate=0.02,
        )
