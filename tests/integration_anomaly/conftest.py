"""Pytest configuration and fixtures for row anomaly detection integration tests."""

import logging
import os
import time
from typing import Any
from uuid import uuid4

import mlflow
import mlflow.sklearn
import pytest
import pyspark.sql.functions as F
from mlflow.exceptions import RestException
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import AnomalyConfig, AnomalyParams, InputConfig, IsolationForestConfig
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.pytester.fixtures.baseline import factory
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import (
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)


# Must be set before configuring MLflow
os.environ.setdefault("MLFLOW_ENABLE_DB_SDK", "true")

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)
# Process-scoped cache for MLflow experiment (one per xdist worker); lifecycle cleared by
# _cleanup_mlflow_worker_experiment at session end. Not thread-safe.
_MLFLOW_WORKER_EXPERIMENT_CACHE: dict[str, str | None] = {"id": None, "path": None}


# -----------------------------------------------------------------------------
# Helper functions (reusable across tests)
# -----------------------------------------------------------------------------


def qualify_model_name(model_name: str, registry_table: str) -> str:
    """Return a fully qualified model name using the registry table prefix."""
    if model_name.count(".") >= 2:
        return model_name
    registry_prefix = registry_table.rsplit(".", 1)[0]
    return f"{registry_prefix}.{model_name}"


def get_standard_2d_training_data() -> list[tuple[float, float]]:
    """Standard 2D training data for amount/quantity anomaly detection (400 points)."""
    return [(100.0 + i * 0.5, 10.0 + i * 0.1) for i in range(400)]


def get_standard_3d_training_data() -> list[tuple[float, float, float]]:
    """Standard 3D training data for anomaly detection with contributions (400 points)."""
    return [(100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001) for i in range(400)]


def get_standard_4d_training_data() -> list[tuple[float, float, float, float]]:
    """Standard 4D training data for multi-column anomaly detection (400 points)."""
    return [(100.0 + i * 0.5, 10.0 + i * 0.1, 0.1 + i * 0.001, 50.0 + i * 0.2) for i in range(400)]


def get_standard_test_points_2d() -> dict[str, tuple[float, float]]:
    """Pre-validated test points for 2D anomaly detection tests."""
    return {
        "normal_in_center": (200.0, 30.0),
        "normal_near_center": (210.0, 32.0),
        "clear_anomaly": (OUTLIER_AMOUNT, OUTLIER_QUANTITY),
    }


def get_standard_test_points_4d() -> dict[str, tuple[float, float, float, float]]:
    """Pre-validated test points for 4D anomaly detection tests."""
    return {
        "normal_in_center": (200.0, 30.0, 0.25, 90.0),
        "clear_anomaly": (OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95, 1.0),
    }


def get_standard_training_ranges() -> dict[str, dict[str, tuple[float, float]]]:
    """Expected ranges for standard training data (2d and 4d)."""
    return {
        "2d": {"amount": (100.0, 300.0), "quantity": (10.0, 50.0)},
        "4d": {
            "amount": (100.0, 300.0),
            "quantity": (10.0, 50.0),
            "discount": (0.1, 0.5),
            "weight": (50.0, 130.0),
        },
    }


def _normalize_anomaly_apply_fn(apply_fn, info_col: str):
    """Wrap apply_fn so the result DataFrame exposes _dq_info for direct-access tests."""

    def normalized(df: DataFrame) -> DataFrame:
        result = apply_fn(df)
        if info_col not in result.columns:
            return result
        return result.withColumn("_dq_info", F.array(F.col(info_col))).drop(info_col)

    return normalized


def create_anomaly_apply_fn(
    model_name: str,
    registry_table: str,
    *,
    driver_only: bool = True,
    **check_kwargs,
):
    """Create apply function from has_no_row_anomalies check. Default driver_only=True for tests."""
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        driver_only=driver_only,
        **check_kwargs,
    )
    return _normalize_anomaly_apply_fn(apply_fn, info_col)


def train_model_with_params(
    engine: AnomalyEngine,
    df: DataFrame,
    model_name: str,
    registry_table: str,
    columns: list[str],
    params: AnomalyParams,
    segment_by: list[str] | None = None,
    expected_anomaly_rate: float = 0.02,
) -> str:
    """Train a model with internal params (test-only)."""
    return engine.train(
        df=df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        segment_by=segment_by,
        params=params,
        expected_anomaly_rate=expected_anomaly_rate,
    )


def get_percentile_threshold_from_data(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    percentile: float = 0.95,
) -> float:
    """Return a fixed severity percentile threshold (0–100) for test stability."""
    _ = (df, model_name, registry_table)
    return float(percentile * 100.0)


def create_anomaly_check_rule(
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    criticality: str = "error",
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> DQDatasetRule:
    """Create a standard DQDatasetRule for anomaly detection. Default driver_only=True for tests."""
    check_kwargs = {
        "model_name": qualify_model_name(model_name, registry_table),
        "registry_table": registry_table,
        "threshold": threshold,
        "driver_only": driver_only,
    }
    check_kwargs.update(kwargs)
    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_row_anomalies,
        check_func_kwargs=check_kwargs,
    )


def apply_anomaly_check_direct(
    test_df: Any,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> Any:
    """Apply anomaly detection directly (without DQEngine) to get anomaly_score column."""
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=qualify_model_name(model_name, registry_table),
        registry_table=registry_table,
        threshold=threshold,
        driver_only=driver_only,
        **kwargs,
    )
    result_df = apply_fn(test_df)
    return result_df.withColumn("anomaly_score", F.col(f"{info_col}.anomaly.score")).withColumn(
        "severity_percentile", F.col(f"{info_col}.anomaly.severity_percentile")
    )


def train_simple_2d_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
) -> None:
    """Train a simple 2D model with standard amount/quantity columns."""
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]
    train_df = spark.createDataFrame(train_data, "amount double, quantity double")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=["amount", "quantity"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=["amount", "quantity"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_simple_3d_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    train_size: int = 50,
    params: AnomalyParams | None = None,
    train_data: list[tuple] | None = None,
) -> None:
    """Train a simple 3D model with amount, quantity, discount columns."""
    if train_data is None:
        train_data = [(100.0 + i * 0.5, 2.0, 0.1) for i in range(train_size)]
    train_df = spark.createDataFrame(train_data, "amount double, quantity double, discount double")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=["amount", "quantity", "discount"],
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=["amount", "quantity", "discount"],
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def train_large_dataset_model(
    spark: SparkSession,
    engine: AnomalyEngine,
    model_name: str,
    registry_table: str,
    num_rows: int,
    columns: list[str] | None = None,
    params: AnomalyParams | None = None,
) -> None:
    """Train a model on large synthetic dataset using spark.range()."""
    if columns is None:
        columns = ["amount", "quantity"]
    train_df = spark.range(num_rows).selectExpr("cast(id as double) as amount", "2.0 as quantity")
    full_model_name = qualify_model_name(model_name, registry_table)
    if params is None:
        engine.train(
            df=train_df,
            columns=columns,
            model_name=full_model_name,
            registry_table=registry_table,
        )
        return
    train_model_with_params(
        engine=engine,
        df=train_df,
        columns=columns,
        model_name=full_model_name,
        registry_table=registry_table,
        params=params,
    )


def score_with_anomaly_check(
    df: DataFrame,
    model_name: str,
    registry_table: str,
    threshold: float = 60.0,
) -> DataFrame:
    """Score a DataFrame using has_no_row_anomalies and collect results."""
    apply_fn = create_anomaly_apply_fn(
        model_name=model_name,
        registry_table=registry_table,
        threshold=threshold,
    )
    result_df = apply_fn(df)
    result_df.collect()
    return result_df


def score_3d_with_contributions(
    spark: SparkSession,
    df_factory,
    scorer,
    model_name: str,
    registry_table: str,
    normal_rows: list[tuple[float, float, float]] | None = None,
    anomaly_rows: list[tuple[float, float, float]] | None = None,
    enable_confidence_std: bool = False,
) -> DataFrame:
    """Score a 3D dataset with contributions enabled (optionally confidence)."""
    test_df = df_factory(
        spark,
        normal_rows=normal_rows or [],
        anomaly_rows=anomaly_rows or [(OUTLIER_AMOUNT, OUTLIER_QUANTITY, 0.95)],
        columns_schema="amount double, quantity double, discount double",
    )
    return scorer(
        test_df,
        model_name=model_name,
        registry_table=registry_table,
        threshold=DEFAULT_SCORE_THRESHOLD,
        enable_contributions=True,
        enable_confidence_std=enable_confidence_std,
        extract_score=False,
    )


def create_anomaly_dataset_rule(
    model_name: str,
    registry_table: str,
    criticality: str = "error",
    threshold: float = 60.0,
    *,
    driver_only: bool = True,
    **kwargs: Any,
) -> DQDatasetRule:
    """Create DQDatasetRule for anomaly detection. Default driver_only=True for tests."""
    return DQDatasetRule(
        criticality=criticality,
        check_func=has_no_row_anomalies,
        check_func_kwargs={
            "model_name": qualify_model_name(model_name, registry_table),
            "registry_table": registry_table,
            "threshold": threshold,
            "driver_only": driver_only,
            **kwargs,
        },
    )


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


_AUTH_FLAKE_MARKERS = ("Credential was not sent", "401")


def _is_transient_auth_flake(exc: RestException) -> bool:
    return any(marker in str(exc) for marker in _AUTH_FLAKE_MARKERS)


@pytest.fixture(autouse=True)
def _retry_mlflow_auth_flakes(monkeypatch):
    """Retry mlflow.sklearn.load_model on transient GHA OIDC metadata-service flakes.

    Test-only workaround: the Databricks SDK's metadata-service auth path occasionally
    returns an empty token under CI load, surfacing as a 401 "Credential was not sent"
    on the next MLflow REST call. Production code intentionally does not retry — real
    users with genuine auth issues should see the error immediately.
    """
    original_load_model = mlflow.sklearn.load_model

    def retrying_load_model(*args, **kwargs):
        last_exc: RestException | None = None
        for attempt in range(5):
            try:
                return original_load_model(*args, **kwargs)
            except RestException as e:
                if not _is_transient_auth_flake(e):
                    raise
                last_exc = e
                time.sleep(2**attempt)  # 1s, 2s, 4s, 8s, 16s (max ~31s total)
        assert last_exc is not None
        raise last_exc

    monkeypatch.setattr(mlflow.sklearn, "load_model", retrying_load_model)


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


@pytest.fixture(autouse=True)
def _ensure_databricks_config_file(ws, tmp_path_factory):
    """Ensure DATABRICKS_CONFIG_FILE points to a valid profile.

    MLflow requires a Databricks config profile to exist even when using SDK auth.
    If DATABRICKS_CONFIG_FILE is already set and the file exists, this is a no-op.
    Otherwise, create a dummy profile with the real host from the workspace client.
    """
    config_file = os.environ.get("DATABRICKS_CONFIG_FILE")
    if config_file and os.path.isfile(config_file):
        return

    profile_dir = tmp_path_factory.mktemp("databricks_config")
    profile_path = profile_dir / "databricks_profile"
    host = ws.config.host
    profile_path.write_text(f"[DEFAULT]\nhost = {host}\ntoken = dummy\n")
    os.environ["DATABRICKS_CONFIG_FILE"] = str(profile_path)


@pytest.fixture
def mlflow_worker_experiment(ws):
    """Create one MLflow experiment per xdist worker (via module cache); reuse across tests.
    Cleanup is done by _cleanup_mlflow_worker_experiment at session end. Function-scoped so
    we can depend on ws (pytester provides ws as function-scoped)."""
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
    """Clear MLflow experiment and cache at end of worker session."""
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
    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
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
