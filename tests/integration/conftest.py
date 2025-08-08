import logging
from datetime import datetime, timezone

from unittest.mock import patch
import pytest
from pyspark.sql import DataFrame
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
def setup_workflows(installation_ctx, make_schema, make_table):
    """
    Set up the workflows for the tests in the workspace.
    Existing cluster can be configured for the test by adding:
    config.profiler_override_clusters = {Task.job_cluster: installation_ctx.workspace_client.config.cluster_id}

    """
    installation_ctx.workspace_installation.run()
    yield from _setup_workflows(installation_ctx, make_schema, make_table)


@pytest.fixture
def setup_serverless_workflows(serverless_installation_ctx, make_schema, make_table):
    """
    Set up the workflows with serverless cluster for the tests in the workspace.
    Existing cluster can be configured for the test by adding:
    config.profiler_override_clusters = {Task.job_cluster: installation_ctx.workspace_client.config.cluster_id}

    """
    serverless_installation_ctx.workspace_installation.run()
    yield from _setup_workflows(serverless_installation_ctx, make_schema, make_table)


@pytest.fixture
def setup_workflows_with_checks(ws, spark, installation_ctx, setup_quality_checks, make_schema, make_table):
    """
    Set up the workflows with quality checks for the tests in the workspace.
    """
    installation_ctx.workspace_installation.run()
    checks_location = setup_quality_checks
    yield from _setup_workflows(installation_ctx, make_schema, make_table, checks_location)


@pytest.fixture
def setup_serverless_workflows_with_checks(
    ws, spark, serverless_installation_ctx, setup_quality_checks_serverless, make_schema, make_table
):
    """
    Set up the workflows with serverless cluster and quality checks for the tests in the workspace.
    """
    serverless_installation_ctx.workspace_installation.run()
    checks_location = setup_quality_checks_serverless
    yield from _setup_workflows(serverless_installation_ctx, make_schema, make_table, checks_location)


def _setup_workflows(installation_ctx, make_schema, make_table, checks_location: str | None = None):
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

    # create an output table with dummy columns
    output_table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
    )

    # update input and output locations
    config = installation_ctx.config
    config.extra_params = EXTRA_PARAMS

    run_config = config.get_run_config()
    run_config.input_config = InputConfig(location=input_table.full_name, options={"versionAsOf": "0"})
    run_config.output_config = OutputConfig(
        location=output_table.full_name, mode="overwrite", options={"overwriteSchema": "true"}
    )
    if checks_location:
        run_config.checks_location = checks_location
    installation_ctx.installation.save(installation_ctx.config)

    yield installation_ctx, run_config


@pytest.fixture
def expected_output(spark) -> DataFrame:
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


@pytest.fixture
def setup_quality_checks(installation_ctx, ws, spark):
    """
    Set up sample quality checks for the tests.
    This should align with the sample data created for `setup_workflows`.
    """
    yield from _setup_quality_checks(installation_ctx, spark, ws)


@pytest.fixture
def setup_quality_checks_serverless(serverless_installation_ctx, ws, spark):
    """
    Set up sample quality checks for the tests.
    This should align with the sample data created for `setup_workflows`.
    """
    yield from _setup_quality_checks(serverless_installation_ctx, spark, ws)


def _setup_quality_checks(installation_ctx, spark, ws):
    config = installation_ctx.config
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
        product_name=installation_ctx.installation.product(),
    )
    InstallationChecksStorageHandler(ws, spark).save(checks=checks, config=config)
    yield checks_location


def contains_expected_workflows(workflows, state):
    for workflow in workflows:
        if all(item in workflow.items() for item in state.items()):
            return True
    return False
