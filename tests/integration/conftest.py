import os
import logging

from collections.abc import Callable, Generator
from functools import cached_property
from dataclasses import replace
from unittest.mock import patch
import pytest

from databricks.labs.dqx.contexts.workflows import RuntimeContext
from databricks.labs.dqx.__about__ import __version__
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.dqx.installer.install import WorkspaceInstaller, WorkspaceInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.dqx.runtime import Workflows
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.labs.dqx.installer.workflows_installer import WorkflowsDeployment


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture
def debug_env_name():
    return "ws"  # Specify the name of the debug environment from ~/.databricks/debug-env.json


@pytest.fixture
def product_info():
    return "dqx", __version__


@pytest.fixture
def set_utc_timezone():
    """
    Set the timezone to UTC for the duration of the test to make sure spark timestamps
    are handled the same way regardless of the environment.
    """
    os.environ["TZ"] = "UTC"
    yield
    os.environ.pop("TZ")


class CommonUtils:
    def __init__(self, env_or_skip_fixture, ws):
        self._env_or_skip = env_or_skip_fixture
        self._ws = ws

    @cached_property
    def installation(self):
        return MockInstallation()

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws


class MockRuntimeContext(CommonUtils, RuntimeContext):
    def __init__(self, env_or_skip_fixture, ws_fixture) -> None:
        super().__init__(
            env_or_skip_fixture,
            ws_fixture,
        )
        self._env_or_skip = env_or_skip_fixture

    @cached_property
    def config(self) -> WorkspaceConfig:
        return WorkspaceConfig(
            run_configs=[RunConfig()],
            connect=self.workspace_client.config,
        )


class MockInstallationContext(MockRuntimeContext):
    __test__ = False

    def __init__(
        self,
        env_or_skip_fixture,
        ws,
        check_file,
    ):
        super().__init__(env_or_skip_fixture, ws)
        self.check_file = check_file

    @cached_property
    def installation(self):
        return Installation(self.workspace_client, self.product_info.product_name())

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self):
        return WorkspaceInstaller(
            self.workspace_client,
            self.environ,
        ).replace(prompts=self.prompts, installation=self.installation, product_info=self.product_info)

    @cached_property
    def config_transform(self) -> Callable[[WorkspaceConfig], WorkspaceConfig]:
        return lambda wc: wc

    @cached_property
    def config(self) -> WorkspaceConfig:
        workspace_config = self.workspace_installer.configure()

        for i, run_config in enumerate(workspace_config.run_configs):
            workspace_config.run_configs[i] = replace(run_config, checks_file=self.check_file)

        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self):
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def tasks(self) -> list[Task]:
        return Workflows.all().tasks()

    @cached_property
    def workflows_deployment(self) -> WorkflowsDeployment:
        return WorkflowsDeployment(
            self.config,
            self.config.get_run_config().name,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.product_info.wheels(self.workspace_client),
            self.product_info,
            self.tasks,
        )

    @cached_property
    def prompts(self):
        return MockPrompts(
            {
                r'Provide location for the input data (path or a table)': 'skip',
                r'Do you want to uninstall DQX.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r".*": "",
            }
            | (self.extend_prompts or {})
        )

    @cached_property
    def extend_prompts(self):
        return {}

    @cached_property
    def workspace_installation(self) -> WorkspaceInstallation:
        return WorkspaceInstallation(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.workflows_deployment,
            self.prompts,
            self.product_info,
        )


@pytest.fixture
def installation_ctx(
    ws,
    env_or_skip,
    check_file="checks.yml",
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(
        env_or_skip,
        ws,
        check_file,
    )
    yield ctx.replace(workspace_client=ws)
    ctx.workspace_installation.uninstall()


@pytest.fixture
def webbrowser_open():
    with patch("webbrowser.open") as mock_open:
        yield mock_open


@pytest.fixture
def setup_workflows(installation_ctx: MockInstallationContext, make_schema, make_table):
    """
    Set up the workflows for the tests

    Existing cluster can be used by adding:
    run_config.override_clusters = {Task.job_cluster: installation_ctx.workspace_client.config.cluster_id}
    """
    # install dqx in the workspace
    installation_ctx.workspace_installation.run()

    # prepare test data
    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    table = make_table(
        catalog_name=catalog_name,
        schema_name=schema.name,
        # sample data
        ctas="SELECT * FROM VALUES "
        "(1, 'a'), (2, 'b'), (3, NULL), (NULL, 'c'), (3, NULL), (1, 'a'), (6, 'a'), (2, 'c'), (4, 'a'), (5, 'd') "
        "AS data(id, name)",
    )

    # update input location
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.input_location = table.full_name
    run_config.input_read_options = {"versionAsOf": "0"}
    installation_ctx.installation.save(installation_ctx.config)

    yield installation_ctx, run_config


def contains_expected_workflows(workflows, state):
    for workflow in workflows:
        if all(item in workflow.items() for item in state.items()):
            return True
    return False
