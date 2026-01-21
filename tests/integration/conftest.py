import logging
import os
from io import BytesIO
from typing import Any
from unittest.mock import patch

import pyspark.sql.functions as F
import pytest
from chispa import assert_df_equality  # type: ignore
from pyspark.sql import DataFrame
from databricks.labs.blueprint.installation import Installation
from databricks.labs.dqx.checks_storage import InstallationChecksStorageHandler
from databricks.labs.dqx.config import InputConfig, OutputConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.installer.mixins import InstallationMixin
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk.service.compute import DataSecurityMode, Kind
from databricks.sdk.service.workspace import ImportFormat

# Optional anomaly detection imports - placed here to keep databricks imports grouped
try:
    from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
    from databricks.labs.dqx.anomaly import check_funcs as anomaly_check_funcs
except ImportError:
    AnomalyEngine = None  # type: ignore[assignment,misc]
    has_no_anomalies = None  # type: ignore[assignment]
    anomaly_check_funcs = None  # type: ignore[assignment]

from tests.conftest import TEST_CATALOG
from tests.integration.test_anomaly_constants import (
    EXTRA_PARAMS,
    OUTLIER_AMOUNT,
    OUTLIER_QUANTITY,
    REPORTING_COLUMNS,
    RUN_ID,
    RUN_TIME,
)

# Optional test utilities - placed here to keep tests imports grouped
try:
    from tests.integration.test_anomaly_utils import (
        get_standard_2d_training_data,
        get_standard_3d_training_data,
        get_standard_4d_training_data,
    )
except ImportError:
    get_standard_2d_training_data = None  # type: ignore[assignment]
    get_standard_3d_training_data = None  # type: ignore[assignment]
    get_standard_4d_training_data = None  # type: ignore[assignment]

# HAS_ANOMALY flag set after all conditional imports
HAS_ANOMALY = AnomalyEngine is not None

# Must be set before importing mlflow
os.environ.setdefault("MLFLOW_ENABLE_DB_SDK", "true")

# Optional MLflow import for anomaly detection tests
try:
    import mlflow

    HAS_MLFLOW = True
except ImportError:
    HAS_MLFLOW = False

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def enable_driver_only_scoring_for_anomaly_tests(request):
    if request.node.get_closest_marker("anomaly") is None:
        yield
        return
    if anomaly_check_funcs is None:
        yield
        return

    anomaly_check_funcs.set_driver_only_for_tests(True)
    try:
        yield
    finally:
        anomaly_check_funcs.set_driver_only_for_tests(False)


@pytest.fixture(scope="session", autouse=True)
def configure_mlflow_tracking():
    """Configure MLflow to use Databricks workspace tracking backend for integration tests."""
    if not HAS_MLFLOW:
        yield
        return

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
        logger.warning(f"Failed to configure MLflow: {e}")
        raise

    yield


def build_quality_violation(
    name: str,
    message: str,
    columns: list[str] | None,
    *,
    function: str = "is_not_null_and_not_empty",
) -> dict[str, Any]:
    """Helper for constructing expected violation entries with shared metadata."""

    return {
        "name": name,
        "message": message,
        "columns": columns,
        "filter": None,
        "function": function,
        "run_time": RUN_TIME,
        "run_id": RUN_ID,
        "user_metadata": {},
    }


@pytest.fixture
def webbrowser_open():
    with patch("webbrowser.open") as mock_open:
        yield mock_open


@pytest.fixture
def setup_workflows(ws, spark, installation_ctx, make_schema, make_table, make_random):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    """

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx, make_schema, make_table, make_random, checks_location, quarantine
        )
        return installation_ctx, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_serverless_workflows(ws, spark, serverless_installation_ctx, make_schema, make_table, make_random):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    """

    if not os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        serverless_installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(serverless_installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            serverless_installation_ctx,
            make_schema,
            make_table,
            make_random,
            checks_location,
            quarantine,
            is_streaming=kwargs.get("is_streaming", False),
        )
        return serverless_installation_ctx, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_workflows_with_metrics(ws, spark, installation_ctx, make_schema, make_table, make_cluster, make_random):
    """Set up workflows with metrics configuration for testing."""

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        cluster = make_cluster(
            single_node=True,
            kind=Kind.CLASSIC_PREVIEW,
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
        )
        cluster_id = cluster.cluster_id
        installation_ctx.config.serverless_clusters = False
        installation_ctx.config.profiler_override_clusters["default"] = cluster_id
        installation_ctx.config.quality_checker_override_clusters["default"] = cluster_id
        installation_ctx.config.e2e_override_clusters["default"] = cluster_id
        installation_ctx.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = _setup_quality_checks(installation_ctx, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx,
            make_schema,
            make_table,
            make_random,
            checks_location,
            quarantine,
            is_streaming=kwargs.get("is_streaming", False),
            is_continuous_streaming=kwargs.get("is_continuous_streaming", False),
        )

        config = installation_ctx.config
        run_config = config.get_run_config()

        catalog_name = TEST_CATALOG
        schema_name = run_config.output_config.location.split(".")[1]
        metrics_table_name = f"{catalog_name}.{schema_name}.metrics_{make_random(6).lower()}"
        run_config.metrics_config = OutputConfig(location=metrics_table_name)

        custom_metrics = kwargs.get("custom_metrics")
        if custom_metrics:
            config.custom_metrics = custom_metrics

        installation_ctx.installation.save(config)

        return installation_ctx, run_config

    def delete(resource):
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        try:
            ws.workspace.delete(checks_location)
        except Exception:
            pass

    yield from factory("workflows_with_metrics", lambda **kw: create(spark, **kw), delete)


@pytest.fixture
def setup_workflows_with_custom_folder(
    ws, spark, installation_ctx_custom_install_folder, make_schema, make_table, make_random
):
    """
    Set up the workflows with installation in the custom install folder.
    """

    if os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID"):
        pytest.skip()

    def create(_spark, **kwargs):
        installation_ctx_custom_install_folder.installation_service.run()

        quarantine = False
        if "quarantine" in kwargs and kwargs["quarantine"]:
            quarantine = True

        checks_location = None
        if "checks" in kwargs and kwargs["checks"]:
            checks_location = _setup_quality_checks(installation_ctx_custom_install_folder, _spark, ws)

        run_config = _setup_workflows_deps(
            installation_ctx_custom_install_folder, make_schema, make_table, make_random, checks_location, quarantine
        )
        return installation_ctx_custom_install_folder, run_config

    def delete(resource) -> None:
        ctx, run_config = resource
        checks_location = f"{ctx.installation.install_folder()}/{run_config.checks_location}"
        ws.workspace.delete(checks_location)

    yield from factory("workflows", lambda **kw: create(spark, **kw), delete)


class TestInstallationMixin(InstallationMixin):
    def get_my_username(self):
        return self._my_username

    def get_me(self):
        return self._me

    def get_installation(
        self, product_name: str, assume_user: bool = True, install_folder: str | None = None
    ) -> Installation:
        return self._get_installation(product_name, assume_user, install_folder)


def _setup_workflows_deps(
    ctx,
    make_schema,
    make_table,
    make_random,
    checks_location: str | None = None,
    quarantine: bool = False,
    is_streaming: bool = False,
    is_continuous_streaming: bool = False,
):
    # prepare test data
    catalog_name = TEST_CATALOG
    schema = make_schema(catalog_name=catalog_name)

    input_table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        # sample data
        ctas="SELECT * FROM VALUES "
        "(1, 'a'), (2, 'b'), (3, NULL), (NULL, 'c'), (3, NULL), (1, 'a'), (6, 'a'), (2, 'c'), (4, 'a'), (5, 'd') "
        "AS data(id, name)",
    )

    # update input and output locations
    config = ctx.config
    config.extra_params = EXTRA_PARAMS

    run_config = config.get_run_config()
    run_config.input_config = InputConfig(
        location=input_table.full_name,
        options={"versionAsOf": "0"} if not is_streaming else {},
        is_streaming=is_streaming,
    )

    trigger: dict[str, Any] = {}
    if is_streaming:
        if is_continuous_streaming:
            trigger = {"processingTime": "60 seconds"}
        else:
            trigger = {"availableNow": True}

    output_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    run_config.output_config = OutputConfig(
        location=output_table,
        trigger=trigger,
        options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_out_ckpt"} if is_streaming else {}),
    )

    if checks_location:
        run_config.checks_location = checks_location

    if quarantine:
        quarantine_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}_quarantine"
        run_config.quarantine_config = OutputConfig(
            location=quarantine_table,
            trigger=trigger,
            options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_qr_ckpt"} if is_streaming else {}),
        )

    # ensure tests are deterministic
    run_config.profiler_config.sample_fraction = 1.0
    run_config.profiler_config.sample_seed = 100

    ctx.installation.save(ctx.config)

    return run_config


@pytest.fixture
def expected_quality_checking_output(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            [1, "a", None, None],
            [2, "b", None, None],
            [
                3,
                None,
                [
                    build_quality_violation(
                        "name_is_not_null_and_not_empty", "Column 'name' value is null or empty", ["name"]
                    )
                ],
                None,
            ],
            [
                None,
                "c",
                [
                    build_quality_violation(
                        "id_is_not_null", "Column 'id' value is null", ["id"], function="is_not_null"
                    )
                ],
                None,
            ],
            [
                3,
                None,
                [
                    build_quality_violation(
                        "name_is_not_null_and_not_empty", "Column 'name' value is null or empty", ["name"]
                    )
                ],
                None,
            ],
            [1, "a", None, None],
            [6, "a", None, None],
            [2, "c", None, None],
            [4, "a", None, None],
            [5, "d", None, None],
        ],
        f"id int, name string {REPORTING_COLUMNS}",
    )


def _setup_quality_checks(ctx, spark, ws):
    config = ctx.config
    checks_location = config.get_run_config().checks_location
    checks = [
        {
            "name": "id_is_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
        },
        {
            "name": "name_is_not_null_and_not_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "name"}},
        },
    ]
    config = InstallationChecksStorageConfig(
        location=checks_location,
        product_name=ctx.installation.product(),
        install_folder=ctx.installation.install_folder(),
    )

    InstallationChecksStorageHandler(ws, spark).save(checks=checks, config=config)
    return checks_location


def setup_custom_check_func(ws, installation_ctx, custom_checks_funcs_location):
    content = '''from databricks.labs.dqx.check_funcs import make_condition, register_rule
from pyspark.sql import functions as F

@register_rule("row")
def not_ends_with_suffix(column: str, suffix: str):
    """
    Example of custom python row-level check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column '{column}' ends with '{suffix}'", f"{column}_ends_with_{suffix}"
    )
'''
    if custom_checks_funcs_location.startswith("/Workspace/"):
        ws.workspace.upload(
            path=custom_checks_funcs_location, format=ImportFormat.AUTO, content=content.encode(), overwrite=True
        )
    elif custom_checks_funcs_location.startswith("/Volumes/"):
        binary_data = BytesIO(content.encode("utf-8"))
        ws.files.upload(custom_checks_funcs_location, binary_data, overwrite=True)
    else:  # relative workspace path
        installation_dir = installation_ctx.installation.install_folder()
        ws.workspace.upload(
            path=f"{installation_dir}/{custom_checks_funcs_location}",
            format=ImportFormat.AUTO,
            content=content.encode(),
            overwrite=True,
        )

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.custom_check_functions = {"not_ends_with_suffix": custom_checks_funcs_location}
    installation_ctx.installation.save(config)


def contains_expected_workflows(workflows, state):
    for workflow in workflows:
        if all(item in workflow.items() for item in state.items()):
            return True
    return False


def assert_quarantine_and_output_dfs(ws, spark, expected_output, output_config, quarantine_config):
    dq_engine = DQEngine(ws, spark)
    expected_output_df = dq_engine.get_valid(expected_output)
    expected_quarantine_df = dq_engine.get_invalid(expected_output)

    output_df = spark.table(output_config.location)
    assert_df_equality(output_df, expected_output_df, ignore_nullable=True)

    quarantine_df = spark.table(quarantine_config.location)
    assert_df_equality(quarantine_df, expected_quarantine_df, ignore_nullable=True)


def assert_output_df(spark, expected_output, output_config):
    checked_df = spark.table(output_config.location)
    assert_df_equality(checked_df, expected_output, ignore_nullable=True)


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
    if not HAS_ANOMALY:
        pytest.skip("Anomaly extras not installed")

    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"test_2d_{suffix}"
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
    if not HAS_ANOMALY:
        pytest.skip("Anomaly extras not installed")

    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"test_3d_{suffix}"
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
    if not HAS_ANOMALY:
        pytest.skip("Anomaly extras not installed")

    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()
    model_name = f"test_4d_{suffix}"
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
    if not HAS_ANOMALY:
        pytest.skip("Anomaly extras not installed")

    def _score(
        test_df,
        model_name: str,
        registry_table: str,
        columns: list[str],
        merge_columns: list[str] | None = None,
        extract_score: bool = True,
        **check_kwargs,
    ):
        if merge_columns is None:
            merge_columns = ["transaction_id"]

        _, apply_fn = has_no_anomalies(
            merge_columns=merge_columns,
            columns=columns,
            model=model_name,
            registry_table=registry_table,
            **check_kwargs,
        )

        result_df = apply_fn(test_df)

        if extract_score:
            return result_df.select("*", F.col("_info.anomaly.score").alias("anomaly_score"))
        return result_df

    return _score


@pytest.fixture
def quick_model_factory(ws, spark, make_random, make_schema):
    """
    Factory for training lightweight models with custom parameters.

    Use when tests need specific training params (e.g., AnomalyParams, segment_by).
    For simple 2D scoring tests, prefer function-scoped shared_2d_model instead.

    Returns a callable that accepts spark and training parameters.
    """
    if not HAS_ANOMALY:
        pytest.skip("Anomaly extras not installed")

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
            params (AnomalyParams | None): AnomalyParams for custom training config
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
        model_name = f"test_model_{make_random(4).lower()}"
        registry_table = f"{catalog}.{schema}.{unique_id}_registry"

        if train_data is None:
            train_data = [(100.0 + i * 0.5, 2.0) for i in range(train_size)]

        # Infer schema from columns
        schema_str = ", ".join(f"{col} double" for col in columns)
        train_df = session.createDataFrame(train_data, schema_str)

        # Create engine with shared ws client
        engine = AnomalyEngine(ws, session)

        full_model_name = engine.train(
            df=train_df,
            columns=columns,
            model_name=model_name,
            registry_table=registry_table,
            params=params,
            segment_by=segment_by,
        )

        return full_model_name, registry_table, columns

    return _train
