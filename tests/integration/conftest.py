import os
import logging
from pathlib import Path
from collections.abc import Callable, Generator
from functools import cached_property
from dataclasses import replace
import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.labs.dqx.contexts.workflow_task import RuntimeContext
from databricks.labs.dqx.__about__ import __version__
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.dqx.install import WorkspaceInstaller, WorkspaceInstallation
from databricks.labs.blueprint.tui import MockPrompts


logging.getLogger("tests").setLevel("DEBUG")
logging.getLogger("databricks.labs.dqx").setLevel("DEBUG")

logger = logging.getLogger(__name__)


@pytest.fixture
def debug_env_name():
    return "ws"


@pytest.fixture
def product_info():
    return "dqx", __version__


@pytest.fixture
def make_check_file_path():
    return "ws"


@pytest.fixture
def make_check_file_as_yaml(ws, make_random, make_directory):
    def create(**kwargs):
        base_path = str(Path(__file__).resolve().parent.parent)
        local_file_path = base_path + "/test_data/checks.yml"
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.yml"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.yml"

        with open(local_file_path, "rb") as f:
            ws.workspace.upload(path=workspace_file_path, format=ImportFormat.AUTO, content=f.read(), overwrite=True)

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("secret scope", create, delete)


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

        workspace_config = replace(workspace_config, checks_file=self.check_file)
        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self):
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def prompts(self):
        return MockPrompts(
            {
                r'Provide location for the input data (path or a table)': 'skip',
                r'Do you want to uninstall DQX.*': 'yes',
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