"""Pytest configuration and fixtures for row anomaly detection integration tests."""

import logging
import os
from uuid import uuid4

import mlflow
import pytest
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.config import AnomalyConfig, AnomalyParams, InputConfig, IsolationForestConfig
from databricks.labs.pytester.fixtures.baseline import factory
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import OUTLIER_AMOUNT, OUTLIER_QUANTITY
from tests.integration_anomaly.test_helpers import (
    create_anomaly_apply_fn,
    train_model_with_params,
)
from tests.integration_anomaly.test_helpers_data import (
    get_standard_2d_training_data,
    get_standard_3d_training_data,
    get_standard_4d_training_data,
)


# Must be set before configuring MLflow
os.environ.setdefault("MLFLOW_ENABLE_DB_SDK", "true")

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)
_MLFLOW_WORKER_EXPERIMENT_CACHE: dict[str, str | None] = {"id": None, "path": None}


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def anomaly_engine(ws, spark):
    """Provide an AnomalyEngine instance for anomaly integration tests."""
    return AnomalyEngine(ws, spark)


def _delete_mlflow_experiment(experiment_id: str) -> None:
    """Delete an MLflow experiment; log and ignore errors so teardown does not fail the run."""
    try:
        mlflow.delete_experiment(experiment_id)
        msg = f"Deleted MLflow experiment {experiment_id}"
        logger.debug(msg)
    except Exception as e:
        msg = f"Could not delete MLflow experiment {experiment_id}: {e}"
        logger.warning(msg)


def _delete_mlflow_experiment_by_name(experiment_path: str) -> None:
    """Resolve experiment by name and delete; log and ignore errors."""
    try:
        exp = mlflow.get_experiment_by_name(experiment_path)
        if exp is not None and getattr(exp, "experiment_id", None):
            mlflow.delete_experiment(exp.experiment_id)
            msg = f"Deleted MLflow experiment {experiment_path} (id={exp.experiment_id})"
            logger.debug(msg)
    except Exception as e:
        msg = f"Could not delete MLflow experiment by name {experiment_path}: {e}"
        logger.warning(msg)


@pytest.fixture
def mlflow_worker_experiment(ws):
    """Create one unique MLflow experiment per xdist worker process and reuse it across tests."""
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        local_mlflow_db = os.environ.get("MLFLOW_LOCAL_DB")
        if not local_mlflow_db:
            worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
            local_mlflow_db = f"/tmp/dqx-mlflow-{worker_id}.db"
        tracking_uri = f"sqlite:///{local_mlflow_db}"
        os.environ.setdefault("MLFLOW_TRACKING_URI", tracking_uri)
        os.environ.setdefault("MLFLOW_REGISTRY_URI", tracking_uri)
    registry_uri = os.environ.get("MLFLOW_REGISTRY_URI", "databricks-uc")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(registry_uri)

    cached_path = _MLFLOW_WORKER_EXPERIMENT_CACHE["path"]
    if cached_path is not None:
        return (_MLFLOW_WORKER_EXPERIMENT_CACHE["id"], cached_path)

    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
    user_name = ws.current_user.me().user_name
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "main")
    suffix = uuid4().hex[:8]
    experiment_path = f"/Users/{user_name}/dqx_integration_tests_{worker_id}_{suffix}"
    experiment = mlflow.set_experiment(experiment_path)
    experiment_id = getattr(experiment, "experiment_id", None) if experiment else None
    logger.debug(f"Created MLflow experiment {experiment_path} (id={experiment_id})")
    _MLFLOW_WORKER_EXPERIMENT_CACHE["id"] = experiment_id
    _MLFLOW_WORKER_EXPERIMENT_CACHE["path"] = experiment_path
    return (experiment_id, experiment_path)


@pytest.fixture(autouse=True, scope="session")
def _cleanup_mlflow_worker_experiment():
    """Delete the cached worker experiment once at the end of the worker session."""
    yield
    experiment_path = _MLFLOW_WORKER_EXPERIMENT_CACHE["path"]
    if experiment_path is None:
        return
    experiment_id = _MLFLOW_WORKER_EXPERIMENT_CACHE["id"]
    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
    os.environ.pop("MLFLOW_EXPERIMENT_NAME", None)
    if experiment_id:
        _delete_mlflow_experiment(experiment_id)
    else:
        _delete_mlflow_experiment_by_name(experiment_path)
    _MLFLOW_WORKER_EXPERIMENT_CACHE["id"] = None
    _MLFLOW_WORKER_EXPERIMENT_CACHE["path"] = None


@pytest.fixture(autouse=True)
def configure_mlflow_tracking(mlflow_worker_experiment):
    """Set MLflow experiment env for each test from the worker-scoped experiment fixture."""
    experiment_id, experiment_path = mlflow_worker_experiment
    if experiment_id:
        os.environ["MLFLOW_EXPERIMENT_ID"] = experiment_id
    os.environ["MLFLOW_EXPERIMENT_NAME"] = experiment_path
    logger.debug(f"MLflow configured: experiment={experiment_path}")


@pytest.fixture
def anomaly_registry_schema(make_schema):
    """Schema for row anomaly detection test isolation."""
    return make_schema(catalog_name=TEST_CATALOG)


@pytest.fixture
def anomaly_registry_prefix(request):
    """Registry prefix for row anomaly detection tests."""
    schema = request.getfixturevalue("anomaly_registry_schema")
    return f"{TEST_CATALOG}.{schema.name}"


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
    params = AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.1, random_seed=42))
    full_model_name = engine.train(
        df=train_df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        params=params,
    )

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
        apply_fn = create_anomaly_apply_fn(
            model_name=model_name,
            registry_table=registry_table,
            **check_kwargs,
        )

        result_df = apply_fn(test_df)

        if extract_score:
            # Use element_at(_, 1) for first array element to avoid Spark Connect resolving "0" as struct field
            first_info = F.element_at(F.col("_dq_info"), 1)
            return result_df.select(
                "*",
                first_info.getField("anomaly").getField("score").alias("anomaly_score"),
            )
        return result_df

    return _score


@pytest.fixture
def setup_anomaly_deployed_workflow(ws, spark, installation_ctx, make_schema, make_random):
    def create(_spark, **_kwargs):
        schema = make_schema(catalog_name=TEST_CATALOG)
        suffix = make_random(8).lower()
        table_name = f"{TEST_CATALOG}.{schema.name}.workflow_deploy_train_{suffix}"
        registry_table = f"{TEST_CATALOG}.{schema.name}.workflow_deploy_registry_{suffix}"
        model_name = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_deploy_{suffix}"

        train_df = _spark.createDataFrame(
            get_standard_2d_training_data(),
            "amount double, quantity double",
        )
        train_df.write.saveAsTable(table_name)

        config = installation_ctx.config
        run_config = config.get_run_config()
        run_config.input_config = InputConfig(location=table_name)
        run_config.anomaly_config = AnomalyConfig(
            columns=["amount", "quantity"],
            registry_table=registry_table,
            model_name=model_name,
        )
        installation_ctx.installation.save(config)

        installation_ctx.installation_service.run()

        return (installation_ctx, run_config, registry_table, model_name)

    def delete(resource):
        ctx, run_config, _, _ = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        try:
            ws.workspace.delete(checks_location)
        except Exception:
            pass

    yield from factory("anomaly_workflows", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def quick_model_factory(ws, make_random, make_schema):
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
                engine=engine,
                df=train_df,
                model_name=model_name,
                registry_table=registry_table,
                columns=columns,
                params=params,
                segment_by=segment_by,
            )

        return full_model_name, registry_table, columns

    return _train
