"""Pytest configuration and fixtures for anomaly detection integration tests."""

import logging
import os

import mlflow
import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.anomaly import AnomalyEngine
from databricks.labs.dqx.anomaly import check_funcs as anomaly_check_funcs

from tests.conftest import TEST_CATALOG
from tests.integration_anomaly.test_anomaly_constants import (
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)
from tests.integration_anomaly.test_anomaly_utils import (
    _create_anomaly_apply_fn,
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    get_standard_4d_training_data,
    train_model_with_params,
)

# Must be set before configuring MLflow
os.environ.setdefault("MLFLOW_ENABLE_DB_SDK", "true")

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def enable_driver_only_scoring_for_anomaly_tests():
    """Enable driver-only scoring for all anomaly tests to avoid cluster library requirements."""
    anomaly_check_funcs.set_driver_only_for_tests(True)
    try:
        yield
    finally:
        anomaly_check_funcs.set_driver_only_for_tests(False)


@pytest.fixture(autouse=True)
def configure_mlflow_tracking():
    """Configure MLflow to use Databricks workspace tracking backend for integration tests."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        local_mlflow_db = os.environ.get("MLFLOW_LOCAL_DB")
        if not local_mlflow_db:
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
            local_mlflow_db = f"/tmp/dqx-mlflow-{worker_id}.db"
        tracking_uri = f"sqlite:///{local_mlflow_db}"
        os.environ.setdefault("MLFLOW_TRACKING_URI", tracking_uri)
        os.environ.setdefault("MLFLOW_REGISTRY_URI", tracking_uri)

    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "databricks-uc")

    try:
        os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_registry_uri(registry_uri)
        experiment = mlflow.set_experiment("/Shared/dqx_integration_tests")
        if experiment and getattr(experiment, "experiment_id", None):
            os.environ["MLFLOW_EXPERIMENT_ID"] = experiment.experiment_id
        logger.info(f"MLflow configured: tracking_uri={tracking_uri} registry_uri={registry_uri}")
    except Exception as e:
        logger.error(f"Failed to configure MLflow: {e}")
        raise

    yield


@pytest.fixture
def anomaly_registry_schema(make_schema):
    """Schema for anomaly detection test isolation."""
    return make_schema(catalog_name=TEST_CATALOG)


@pytest.fixture
def anomaly_registry_prefix(anomaly_registry_schema):
    """Registry prefix for anomaly detection tests."""
    return f"{TEST_CATALOG}.{anomaly_registry_schema.name}"


@pytest.fixture
def shared_2d_model(ws, spark, make_schema, make_random):
    """Function-scoped 2D anomaly model for testing."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.test_2d_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"
    columns = ["amount", "quantity"]

    training_data = get_standard_2d_training_data()
    train_df = spark.createDataFrame(training_data, "amount double, quantity double")

    engine = AnomalyEngine(ws, spark)
    full_model_name = engine.train(df=train_df, columns=columns, model_name=model_name, registry_table=registry_table)

    return {
        "model_name": full_model_name,
        "registry_table": registry_table,
        "columns": columns,
        "training_data": training_data,
    }


@pytest.fixture
def shared_3d_model(ws, spark, make_schema, make_random):
    """Function-scoped 3D anomaly model for testing."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.test_3d_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"
    columns = ["amount", "quantity", "discount"]

    training_data = get_standard_3d_training_data()
    train_df = spark.createDataFrame(training_data, "amount double, quantity double, discount double")

    engine = AnomalyEngine(ws, spark)
    full_model_name = engine.train(df=train_df, columns=columns, model_name=model_name, registry_table=registry_table)

    return {
        "model_name": full_model_name,
        "registry_table": registry_table,
        "columns": columns,
        "training_data": training_data,
    }


@pytest.fixture
def shared_4d_model(ws, spark, make_schema, make_random):
    """Function-scoped 4D anomaly model for testing."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"{TEST_CATALOG}.{schema.name}.test_4d_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.reg_{suffix}"
    columns = ["amount", "quantity", "discount", "weight"]

    training_data = get_standard_4d_training_data()
    train_df = spark.createDataFrame(training_data, "amount double, quantity double, discount double, weight double")

    engine = AnomalyEngine(ws, spark)
    full_model_name = engine.train(df=train_df, columns=columns, model_name=model_name, registry_table=registry_table)

    return {
        "model_name": full_model_name,
        "registry_table": registry_table,
        "columns": columns,
        "training_data": training_data,
    }


@pytest.fixture
def test_df_factory():
    """Factory for creating test DataFrames with transaction_id."""

    def _create(
        session,
        normal_rows: list[tuple] | None = None,
        anomaly_rows: list[tuple] | None = None,
        columns_schema: str = "amount double, quantity double",
        id_column: str = "transaction_id",
    ):
        if normal_rows is None:
            normal_rows = [(100.0, 2.0)]
        if anomaly_rows is None:
            anomaly_rows = [(OUTLIER_AMOUNT, OUTLIER_QUANTITY)]

        all_rows = []
        for idx, row in enumerate(normal_rows + anomaly_rows, start=1):
            all_rows.append((idx,) + row)

        schema = f"{id_column} int, {columns_schema}"
        return session.createDataFrame(all_rows, schema)

    return _create


@pytest.fixture
def anomaly_scorer():
    """Helper to score DataFrames with anomaly check."""

    def _score(
        test_df,
        model_name: str,
        registry_table: str,
        extract_score: bool = True,
        **check_kwargs,
    ):
        apply_fn = _create_anomaly_apply_fn(
            model_name=model_name,
            registry_table=registry_table,
            **check_kwargs,
        )

        result_df = apply_fn(test_df)

        if extract_score:
            return result_df.select("*", F.col("_dq_info.anomaly.score").alias("anomaly_score"))
        return result_df

    return _score


@pytest.fixture
def quick_model_factory(ws, spark, make_random, make_schema):
    """
    Factory for training lightweight models with custom parameters.

    Use when tests need specific training params (internal, e.g., AnomalyParams, segment_by).
    For simple 2D scoring tests, prefer function-scoped shared_2d_model instead.

    Returns a callable that accepts spark and training parameters.
    """

    def _train(
        session,
        train_size: int = 50,
        columns: list[str] | None = None,
        train_data: list[tuple] | None = None,
        params=None,
        segment_by: list[str] | None = None,
        catalog: str = TEST_CATALOG,
        schema: str | None = None,
    ):
        """
        Train a quick test model.

        Args:
            session (SparkSession): SparkSession instance
            train_size (int): Number of training rows (default: 50)
            columns (list[str] | None): Column names (default: ["amount", "quantity"])
            train_data (list[tuple] | None): Custom training data tuples (overrides train_size)
            params (AnomalyParams | None): Internal training params (test-only)
            segment_by (list[str] | None): Segment columns for segmented models
            catalog (str): Catalog name
            schema (str | None): Schema name

        Returns:
            tuple: (model_name, registry_table, columns)

        Example:
            model, registry, cols = quick_model_factory(
                spark, params=AnomalyParams(sample_fraction=1.0, max_rows=100)
            )
        """
        if columns is None:
            columns = ["amount", "quantity"]

        if schema is None:
            schema = make_schema(catalog_name=catalog).name

        unique_id = make_random(8).lower()
        model_name = f"{catalog}.{schema}.test_model_{make_random(4).lower()}"
        registry_table = f"{catalog}.{schema}.{unique_id}_registry"

        if train_data is None:
            train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]

        # Infer schema from columns
        schema_str = ", ".join(f"{col} double" for col in columns)
        train_df = session.createDataFrame(train_data, schema_str)

        # Create engine with shared ws client
        engine = AnomalyEngine(ws, session)

        if params is None:
            full_model_name = engine.train(
                df=train_df,
                columns=columns,
                model_name=model_name,
                registry_table=registry_table,
                segment_by=segment_by,
            )
        else:
            full_model_name = train_model_with_params(
                anomaly_engine=engine,
                df=train_df,
                model_name=model_name,
                registry_table=registry_table,
                columns=columns,
                params=params,
                segment_by=segment_by,
            )

        return full_model_name, registry_table, columns

    return _train
