import logging
import os
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

from chispa import assert_df_equality  # type: ignore
from pyspark.sql import DataFrame
import pytest
from databricks.labs.blueprint.installation import Installation
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.labs.dqx.checks_storage import InstallationChecksStorageHandler
from databricks.labs.dqx.config import InputConfig, OutputConfig, InstallationChecksStorageConfig, ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.installer.mixins import InstallationMixin
from databricks.labs.dqx.schema import dq_result_schema
from databricks.sdk.service.compute import DataSecurityMode, Kind

from tests.conftest import TEST_CATALOG


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


REPORTING_COLUMNS = f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
RUN_ID = "2f9120cf-e9f2-446a-8278-12d508b00639"
EXTRA_PARAMS = ExtraParams(run_time_overwrite=RUN_TIME.isoformat(), run_id_overwrite=RUN_ID)


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
                    {
                        "name": "name_is_not_null_and_not_empty",
                        "message": "Column 'name' value is null or empty",
                        "columns": ["name"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                None,
                "c",
                [
                    {
                        "name": "id_is_not_null",
                        "message": "Column 'id' value is null",
                        "columns": ["id"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                3,
                None,
                [
                    {
                        "name": "name_is_not_null_and_not_empty",
                        "message": "Column 'name' value is null or empty",
                        "columns": ["name"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
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
