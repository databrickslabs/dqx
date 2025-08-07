import logging

from unittest.mock import patch
import pytest

from databricks.labs.dqx.config import InputConfig, OutputConfig


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture
def webbrowser_open():
    with patch("webbrowser.open") as mock_open:
        yield mock_open


@pytest.fixture
def setup_workflows(installation_ctx, make_schema, make_table):
    """
    Set up the workflows for the tests
    Existing cluster can be configured for the test by adding:
    config.profiler_override_clusters = {Task.job_cluster: installation_ctx.workspace_client.config.cluster_id}

    """
    # install dqx in the workspace
    installation_ctx.workspace_installation.run()
    yield from _setup_workflows(installation_ctx, make_schema, make_table)


@pytest.fixture
def setup_serverless_workflows(serverless_installation_ctx, make_schema, make_table):
    """
    Set up the workflows for the tests
    Existing cluster can be configured for the test by adding:
    config.profiler_override_clusters = {Task.job_cluster: installation_ctx.workspace_client.config.cluster_id}

    """
    # install dqx in the workspace
    serverless_installation_ctx.workspace_installation.run()
    yield from _setup_workflows(serverless_installation_ctx, make_schema, make_table)


def _setup_workflows(installation_ctx, make_schema, make_table):
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

    # create an output location
    output_table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        ctas="SELECT * FROM VALUES "
        "(1, 'a', NULL, NULL), (2, 'b', NULL, NULL), (3, NULL, NULL, NULL), (NULL, 'c', NULL, NULL), "
        "(3, NULL, NULL, NULL), (1, 'a', NULL, NULL), (6, 'a', NULL, NULL), (2, 'c', NULL, NULL), "
        "(4, 'a', NULL, NULL), (5, 'd', NULL, NULL) AS data(id, name, _errors, _warnings)",
    )

    # update input and output locations
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.input_config = InputConfig(location=input_table.full_name, options={"versionAsOf": "0"})
    run_config.output_config = OutputConfig(location=output_table.full_name)
    installation_ctx.installation.save(installation_ctx.config)

    yield installation_ctx, run_config


def contains_expected_workflows(workflows, state):
    for workflow in workflows:
        if all(item in workflow.items() for item in state.items()):
            return True
    return False
