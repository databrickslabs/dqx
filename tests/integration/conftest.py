import logging
import re
from datetime import datetime, timezone
from unittest.mock import patch
from pyspark.sql import DataFrame
import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.labs.dqx.checks_storage import InstallationChecksStorageHandler
from databricks.labs.dqx.config import InputConfig, OutputConfig, InstallationChecksStorageConfig, ExtraParams
from databricks.labs.dqx.schema import dq_result_schema

logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


REPORTING_COLUMNS = f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME.isoformat())


@pytest.fixture
def webbrowser_open():
    with patch("webbrowser.open") as mock_open:
        yield mock_open


@pytest.fixture
def setup_workflows(ws, spark, installation_ctx, make_schema, make_table, make_random):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    """

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


def _setup_workflows_deps(
    ctx,
    make_schema,
    make_table,
    make_random,
    checks_location: str | None = None,
    quarantine: bool = False,
    is_streaming: bool = False,
):
    # prepare test data
    catalog_name = "main"
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
    output_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    run_config.output_config = OutputConfig(
        location=output_table,
        trigger={"availableNow": True} if is_streaming else {},
        options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_out_ckpt"} if is_streaming else {}),
    )

    if checks_location:
        run_config.checks_location = checks_location

    if quarantine:
        quarantine_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}_quarantine"
        run_config.quarantine_config = OutputConfig(
            location=quarantine_table,
            trigger={"availableNow": True} if is_streaming else {},
            options=({"checkpointLocation": f"/tmp/dqx_tests/{make_random(10)}_qr_ckpt"} if is_streaming else {}),
        )

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


@pytest.fixture
def skip_if_runtime_not_geo_compatible(ws, debug_env):
    """
    Skip the test if the cluster runtime does not support the required geo functions, i.e.
    * serverless clusters have the required geo functions
    * standard clusters require runtime 17.1 or above

    Args:
        ws (WorkspaceClient): Workspace client to interact with Databricks.
        debug_env (dict): Test environment variables.
    """
    if "DATABRICKS_SERVERLESS_COMPUTE_ID" in debug_env:
        return  # serverless clusters have the required geo functions

    # standard clusters require runtime 17.1 or above
    cluster_id = debug_env.get("DATABRICKS_CLUSTER_ID")
    if not cluster_id:
        raise ValueError("DATABRICKS_CLUSTER_ID is not set in debug_env")

    # Fetch cluster details
    cluster_info = ws.clusters.get(cluster_id)
    runtime_version = cluster_info.spark_version

    if not runtime_version:
        raise ValueError(f"Unable to retrieve runtime version for cluster {cluster_id}")

    # Extract major and minor version numbers
    match = re.match(r"(\d+)\.(\d+)", runtime_version)
    if not match:
        raise ValueError(f"Invalid runtime version format: {runtime_version}")

    major, minor = [int(x) for x in match.groups()]
    valid = major > 17 or (major == 17 and minor >= 1)

    if not valid:
        pytest.skip("This test requires a cluster with runtime 17.1 or above")
