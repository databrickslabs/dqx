"""Pytest configuration and fixtures for row anomaly detection integration tests."""

import hashlib
import json
import re
import logging
import os
import platform
import time
from collections.abc import Callable
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version as distribution_version
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

import pandas as pd

import mlflow.utils.rest_utils as mlflow_rest_utils
import pytest
import pyspark.sql.functions as F
from mlflow.exceptions import RestException
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.core import fit_isolation_forest
from databricks.labs.dqx.config import (
    AnomalyConfig,
    AnomalyParams,
    InputConfig,
    IsolationForestConfig,
    LLMModelConfig,
)
from databricks.labs.dqx.rule import DQDatasetRule
from databricks.labs.pytester.fixtures.baseline import factory
from tests.anomaly_diagnostics import (
    AnomalyDiagnostics,
    DEPENDENCIES,
    dependency_provenance,
    endpoint_unavailable_reason,
    exception_chain,
    runtime_observation,
    compute_observation,
    SCORER_MODULES,
)
from tests import anomaly_resources
from tests.anomaly_resources import drop_registered_models_in_schemas
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.constants import (
    DEFAULT_AI_QUERY_ENDPOINT,
    DEFAULT_SCORE_THRESHOLD,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
)


# Must be set before configuring MLflow
os.environ.setdefault("MLFLOW_ENABLE_DB_SDK", "true")

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)

# Bind the shared fixtures into this conftest module so pytest discovers them here.
fixture_make_mlflow_experiment_resource = anomaly_resources.fixture_make_mlflow_experiment_resource
fixture_mlflow_worker_experiment = anomaly_resources.fixture_mlflow_worker_experiment


def pytest_configure(config: pytest.Config) -> None:
    """Enable payload-free diagnostics only when an artifact directory is supplied."""
    directory = os.environ.get("DQX_TEST_DIAGNOSTICS_DIR")
    if not directory:
        return
    versions = {}
    for package in DEPENDENCIES:
        try:
            versions[package] = distribution_version(package)
        except PackageNotFoundError:
            versions[package] = "unavailable"
    wheel = os.environ.get("DQX_PREBUILT_WHEEL")
    wheel_digest = None
    if wheel:
        try:
            wheel_digest = hashlib.sha256(Path(wheel).read_bytes()).hexdigest()
        except OSError:
            logger.warning("Prebuilt wheel is unavailable for diagnostic provenance")
    provenance = dependency_provenance(
        versions,
        platform.python_version(),
        wheel_sha256=wheel_digest,
        revision=os.environ.get("GITHUB_SHA"),
        source=os.environ.get("DQX_TEST_CODE_SOURCE", "checkout"),
    )
    provenance["acceptance_retry"] = bool(os.environ.get("TEST_FILTER"))
    diagnostics = AnomalyDiagnostics(Path(directory), Path(__file__).parent, f"{os.getpid()}-{uuid4().hex}", provenance)
    config.pluginmanager.register(diagnostics, "dqx-anomaly-diagnostics")


def _worker_environment_probe() -> Callable[[pd.Series], pd.Series]:
    # The nested closure carries only values and installed-library references;
    # workers do not need the test source tree to import this function.
    packages = DEPENDENCIES
    modules = SCORER_MODULES

    def observe(batch: pd.Series) -> pd.Series:
        versions = {}
        for package in packages:
            try:
                versions[package] = distribution_version(package)
            except PackageNotFoundError:
                versions[package] = "unavailable"
        hashes: dict[str, str | None] = {}
        for module in modules:
            try:
                location = import_module(module).__file__
                if location:
                    hashes[module] = hashlib.sha256(Path(location).read_bytes()).hexdigest()
            except (ImportError, AttributeError, OSError):
                hashes[module] = None
        value = {
            "python": platform.python_version(),
            "packages": versions,
            "module_sha256": hashes,
            "runtime": os.environ.get("DATABRICKS_RUNTIME_VERSION"),
        }
        return pd.Series([json.dumps(value)] * len(batch))

    return observe


def _observe_attached_compute(session: SparkSession, ws: WorkspaceClient) -> dict[str, object]:
    """Resolve the runtime's attached cluster through documented, public APIs."""
    # Documented for classic and serverless environment 6; older serverless
    # environments may reject the configuration key and remain unknown.
    try:
        cluster_id = session.conf.get("spark.databricks.clusterUsageTags.clusterId", None)
    except PySparkException:
        return compute_observation(None)
    if not cluster_id:
        return compute_observation(None)
    try:
        cluster = ws.clusters.get(cluster_id=cluster_id)
    except DatabricksError:
        return compute_observation(cluster_id)
    if cluster.cluster_id != cluster_id:
        return compute_observation(cluster_id)
    return compute_observation(
        cluster_id,
        kind=cluster.kind.value if cluster.kind else None,
        source=cluster.cluster_source.value if cluster.cluster_source else None,
        runtime=cluster.spark_version,
    )


@pytest.fixture(autouse=True)
def anomaly_worker_provenance(request: pytest.FixtureRequest) -> None:
    """Observe a real pandas worker once per test process when diagnostics are on."""
    diagnostics = request.config.pluginmanager.get_plugin("dqx-anomaly-diagnostics")
    if not isinstance(diagnostics, AnomalyDiagnostics) or diagnostics.runtime_recorded:
        return
    diagnostics.runtime_recorded = True
    expected = {}
    wheel = os.environ.get("DQX_PREBUILT_WHEEL")
    if wheel:
        try:
            with ZipFile(wheel) as archive:
                expected = {
                    module: hashlib.sha256(archive.read(module.replace(".", "/") + ".py")).hexdigest()
                    for module in SCORER_MODULES
                }
        except (OSError, KeyError, BadZipFile):
            logger.warning("The worker provenance wheel manifest is unavailable")
    report: dict[str, object] = {"compute": "unknown", "worker": "unavailable"}
    try:
        session: SparkSession = request.getfixturevalue("spark")
        spark_version = session.version
        report["spark"] = spark_version if re.fullmatch(r"[0-9][A-Za-z0-9.+_-]{0,60}", spark_version) else "unavailable"
        report.update(_observe_attached_compute(session, request.getfixturevalue("ws")))

        observe = F.pandas_udf(_worker_environment_probe(), "string", F.PandasUDFType.SCALAR)
        raw = session.range(1).select(observe("id").alias("provenance")).collect()[0]
        report["worker"] = runtime_observation(json.loads(raw["provenance"]), expected)
    except Exception as exc:
        # Diagnostics must not turn a probe failure into a setup failure for every
        # test. The actual scoring tests still surface their original exception.
        report["exception_chain"] = exception_chain(exc)
    diagnostics.record_runtime(report)


@pytest.fixture
def scoring_contract_data(
    spark: SparkSession,
) -> Callable[[str], tuple[DataFrame, DataFrame, list[str], list[str], str | None]]:
    """Provide deterministic mixed inputs for public driver/worker scoring contracts."""
    start = datetime(2025, 1, 1)

    def create(context: str) -> tuple[DataFrame, DataFrame, list[str], list[str], str | None]:
        if context not in {"plain", "combined"}:
            raise ValueError("Unknown scoring contract context")
        training = spark.createDataFrame(
            [
                (
                    100.0 + offset + 0.2 * index + ((index * 7) % 13 - 6),
                    10.0 + 0.03 * index + ((index * 11) % 7 - 3) * 0.2,
                    "open" if index % 3 else "closed",
                    region,
                    start + timedelta(hours=index),
                )
                for region, offset in (("North", 0.0), ("South", 200.0))
                for index in range(200)
            ],
            "amount double, quantity double, status string, region string, event_time timestamp",
        )
        timestamp = start + timedelta(hours=100)
        probes = [
            (1, 119.0, 13.0, "open", "North", timestamp),
            (2, 5000.0, 150.0, "open", "North", timestamp),
            (2, 5000.0, 150.0, "open", "North", timestamp),
            (3, None, 13.0, "open", "North", timestamp),
            (4, 119.0, 13.0, "open", "Unseen", timestamp),
            (5, 119.0, 13.0, "open", "North", None),
            (6, 119.0, 13.0, "open", "North", start + timedelta(hours=400)),
            (7, 119.0, 13.0, "open", "North", timestamp),
            (8, 119.0, 13.0, "open", None, timestamp),
        ]
        payload = (0.42, 0.1, {"original": 0.2}, {"original basis": 0.3}, 12.0)
        probe = spark.createDataFrame(
            [(*row, *payload) for row in probes],
            "probe_id int, amount double, quantity double, status string, region string, event_time timestamp, "
            "anomaly_score double, anomaly_score_std double, anomaly_contributions map<string,double>, "
            "anomaly_basis_contributions map<string,double>, severity_percentile double",
        )
        return (
            training,
            probe,
            ["amount", "quantity", "status"],
            ["region"] if context == "combined" else [],
            "event_time" if context == "combined" else None,
        )

    return create


@pytest.fixture
def discovery_basis_df(spark):
    """Two equally eligible groups make accidental implicit exclusions visible."""
    start = datetime(2025, 1, 6)
    return spark.createDataFrame(
        [
            (region, channel, float(i), start + timedelta(minutes=i))
            for region in ("North", "South")
            for channel in ("Store", "Online")
            for i in range(200)
        ],
        "region string, channel string, amount double, event_time timestamp",
    )


@pytest.fixture
def grouped_categorical_model(spark):
    """A local fitted estimator with categorical-only features and a nullable comparison group."""
    df = spark.createDataFrame(
        [(region, "open" if i % 3 else "closed", i % 2 == 0) for region in ("North", None) for i in range(40)],
        "region string, status string, enabled boolean",
    )
    params = AnomalyParams(baseline_by=["region"], algorithm_config=IsolationForestConfig(num_trees=10, random_seed=42))
    model, _, metadata = fit_isolation_forest(df, ["status", "enabled"], params)
    return df, model, metadata


# -----------------------------------------------------------------------------
# Helper functions (reusable across tests)
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# ai_query (LLM) endpoint probing — shared by every test that needs a live endpoint
# -----------------------------------------------------------------------------

AI_QUERY_TEST_ENDPOINT = os.environ.get("DQX_AI_QUERY_TEST_ENDPOINT", DEFAULT_AI_QUERY_ENDPOINT)


def ai_query_llm_config(endpoint: str) -> LLMModelConfig:
    """LLMModelConfig pointing at a Model Serving endpoint reached through SQL ``ai_query``."""
    return LLMModelConfig(model_name=endpoint)


def ai_query_endpoint_available(session: SparkSession) -> tuple[bool, str | None]:
    """Cheap probe — does ai_query against the configured endpoint succeed?

    Known missing, denied, or temporarily unavailable endpoints produce a safe skip
    reason. Unexpected SQL, schema, and execution failures propagate as test failures.
    """
    try:
        session.sql(
            f"SELECT ai_query('{AI_QUERY_TEST_ENDPOINT}', 'reply with the single word: ok', "
            f"modelParameters => named_struct('max_tokens', 8, 'temperature', 0.0)) AS r"
        ).collect()
        return True, None
    except PySparkException as exc:
        reason = endpoint_unavailable_reason(exc.getErrorClass(), str(exc))
        if reason is None:
            raise
        return False, reason


@pytest.fixture
def ai_query_endpoint(ws, spark):
    """Skip the test when the workspace cannot reach the configured ai_query endpoint."""
    assert ws.current_user.me() is not None  # fail-fast if workspace auth is broken
    available, error = ai_query_endpoint_available(spark)
    if not available:
        pytest.skip(f"{error}; set DQX_AI_QUERY_TEST_ENDPOINT to use an available endpoint")
    return AI_QUERY_TEST_ENDPOINT


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
    # Production defaults enable_contributions / enable_ai_explanation to True, but the broad
    # anomaly suite shouldn't pay the SHAP + ai_query (LLM) cost on every test — so this scaffold
    # defaults them OFF. Tests that exercise contributions/explanations pass the flags explicitly.
    check_kwargs.setdefault("enable_contributions", False)
    check_kwargs.setdefault("enable_ai_explanation", False)
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
    baseline_by: list[str] | None = None,
    baseline_over_time: str | None = None,
    profile: str | None = None,
) -> str:
    """Train a model with internal params (test-only)."""
    return engine.train(
        df=df,
        columns=columns,
        model_name=model_name,
        registry_table=registry_table,
        baseline_by=baseline_by,
        baseline_over_time=baseline_over_time,
        params=params,
        profile=profile,
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
    # See create_anomaly_apply_fn: default the (now production-on) contributions/explanation flags
    # OFF in tests to avoid SHAP + ai_query cost; tests that need them pass the flags explicitly.
    check_kwargs.setdefault("enable_contributions", False)
    check_kwargs.setdefault("enable_ai_explanation", False)
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
    # See create_anomaly_apply_fn: default the (now production-on) contributions/explanation flags
    # OFF in tests to avoid SHAP + ai_query (LLM) cost; tests that need them pass the flags explicitly.
    kwargs.setdefault("enable_contributions", False)
    kwargs.setdefault("enable_ai_explanation", False)
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
    # See create_anomaly_apply_fn: default the (now production-on) contributions/explanation flags
    # OFF in tests to avoid SHAP + ai_query (LLM) cost; tests that need them pass the flags explicitly.
    kwargs.setdefault("enable_contributions", False)
    kwargs.setdefault("enable_ai_explanation", False)
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
    """Retry transient 401s from Databricks metadata-service auth on MLflow REST calls.

    Test-only workaround: the Databricks SDK's metadata-service auth path occasionally
    returns stale/empty headers without raising. MLflow then sends the request and
    Databricks rejects it with 401 "Credential was not sent". Production code
    intentionally does not retry — real users with genuine auth issues should see the
    error immediately.

    Patches *mlflow.utils.rest_utils.http_request* — the single HTTP call every MLflow
    REST operation routes through (load_model, MlflowClient methods, mlflow.* helpers,
    log_model, etc.). This is MLflow-internal API; if the symbol relocates in a future
    MLflow release, tests will fail with AttributeError at startup rather than silently
    skipping the retry.
    """
    original_http_request = mlflow_rest_utils.http_request

    def retrying_http_request(*args: Any, **kwargs: Any) -> Any:
        last_exc: RestException | None = None
        for attempt in range(5):
            try:
                return original_http_request(*args, **kwargs)
            except RestException as e:
                if not _is_transient_auth_flake(e):
                    raise
                last_exc = e
                time.sleep(2**attempt)  # 1s, 2s, 4s, 8s, 16s (max ~31s total)
        assert last_exc is not None
        raise last_exc

    monkeypatch.setattr(mlflow_rest_utils, "http_request", retrying_http_request)


@pytest.fixture
def anomaly_engine(ws, spark):
    """Provide an AnomalyEngine instance for anomaly integration tests."""
    return AnomalyEngine(ws, spark)


@pytest.fixture(autouse=True)
def configure_mlflow_tracking(mlflow_worker_experiment):
    """Set MLflow experiment env for each test from the worker-scoped experiment fixture."""
    experiment_id, experiment_path = mlflow_worker_experiment
    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
    if experiment_id:
        os.environ["MLFLOW_EXPERIMENT_ID"] = experiment_id
    os.environ["MLFLOW_EXPERIMENT_NAME"] = experiment_path


@pytest.fixture(name="make_schema")
def fixture_make_schema(make_schema, ws):
    """Wrap pytester's ``make_schema`` so registered models in each schema are dropped at teardown.

    Anomaly training registers UC models inside the test schema, and a forced schema delete does
    not cascade them — pytester's own teardown leaves them behind, so they accumulate toward the
    metastore registered-model quota and can block the schema drop. This override records every
    schema handed out during the test and drops its registered models *before* pytester deletes the
    schema (this fixture depends on pytester's, so its finalizer runs first). Covers both the model
    fixtures and inline ``make_schema`` + train calls, and is scoped to schemas this test created
    (xdist-safe — never touches another worker's schema).
    """
    pytester_make_schema = make_schema
    created = []

    def _make(**kwargs):
        schema = pytester_make_schema(**kwargs)
        created.append(schema)
        return schema

    yield _make
    drop_registered_models_in_schemas(ws, [(schema.catalog_name, schema.name) for schema in created])


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

    Use when tests need specific training params (internal, e.g., AnomalyParams, profile).
    For simple 2D scoring tests, prefer function-scoped shared_2d_model instead.

    Returns a callable that accepts spark and training parameters.
    """

    def _train(
        session,
        train_size: int = 50,
        columns: list[str] | None = None,
        train_data: list[tuple] | None = None,
        params=None,
        catalog: str = TEST_CATALOG,
        schema: str | None = None,
        baseline_by: list[str] | None = None,
        baseline_over_time: str | None = None,
        train_schema: str | None = None,
        profile: str | None = None,
    ):
        """
        Train a quick test model.

        Args:
            session (SparkSession): SparkSession instance
            train_size (int): Number of training rows (default: 50)
            columns (list[str] | None): Column names (default: ["amount", "quantity"])
            train_data (list[tuple] | None): Custom training data tuples (overrides train_size)
            params (AnomalyParams | None): Internal training params (test-only)
            baseline_by (list[str] | None): Group columns for baseline-conditioned models
            baseline_over_time (str | None): Time column each metric's expected level is fitted along
            profile (str | None): Which detector to train ("distribution" / "correlation"); None means
                the default, so existing callers keep the IsolationForest path untouched.
            train_schema (str | None): Explicit DDL for train_data (needed when group columns
                are not doubles)
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

        # Infer schema from columns unless the caller gave explicit DDL (group columns are
        # typically strings, which cannot be inferred from the feature column list)
        schema_str = train_schema or ", ".join(f"{col} double" for col in columns)
        train_df = session.createDataFrame(train_data, schema_str)

        # Create engine with shared ws client
        engine = AnomalyEngine(ws, session)

        if params is None:
            full_model_name = engine.train(
                df=train_df,
                columns=columns,
                model_name=model_name,
                registry_table=registry_table,
                baseline_by=baseline_by,
                baseline_over_time=baseline_over_time,
                profile=profile,
            )
        else:
            full_model_name = train_model_with_params(
                engine=engine,
                df=train_df,
                model_name=model_name,
                registry_table=registry_table,
                columns=columns,
                params=params,
                baseline_by=baseline_by,
                baseline_over_time=baseline_over_time,
                profile=profile,
            )

        return full_model_name, registry_table, columns

    return _train
