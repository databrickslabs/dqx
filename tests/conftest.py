import os
from collections.abc import Callable, Generator
from dataclasses import replace
from functools import cached_property

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.contexts.workflow import WorkflowContext
from databricks.labs.dqx.installer.install import WorkspaceInstaller, WorkspaceInstallation
from databricks.labs.dqx.installer.workflows_installer import WorkflowsDeployment
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.labs.dqx.workflows_runner import WorkflowsRunner
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat


@pytest.fixture
def debug_env_name():
    return "ws"  # Specify the name of the debug environment from ~/.databricks/debug-env.json


@pytest.fixture
def product_info():
    return "dqx", __version__


@pytest.fixture
def set_utc_timezone():
    """
    Set the timezone to UTC for the duration of the test to make sure spark timestamps    are handled the same way regardless of the environment.
    """
    os.environ["TZ"] = "UTC"
    yield
    os.environ.pop("TZ")


class CommonUtils:
    def __init__(self, env_or_skip_fixture: Callable[[str], str], ws: WorkspaceClient):
        self._env_or_skip = env_or_skip_fixture
        self._ws = ws

    @cached_property
    def installation(self):
        return MockInstallation()

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws


class MockWorkflowContext(CommonUtils, WorkflowContext):
    def __init__(self, env_or_skip_fixture: Callable[[str], str], ws_fixture) -> None:
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


class MockInstallationContext(MockWorkflowContext):
    __test__ = False

    def __init__(
        self,
        env_or_skip_fixture: Callable[[str], str],
        ws: WorkspaceClient,
        checks_location,
        serverless_cluster: bool = True,
    ):
        super().__init__(env_or_skip_fixture, ws)
        self.checks_location = checks_location
        self.serverless_cluster = serverless_cluster

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
        workspace_config.serverless_cluster = self.serverless_cluster

        for i, run_config in enumerate(workspace_config.run_configs):
            workspace_config.run_configs[i] = replace(run_config, checks_location=self.checks_location)

        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self):
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def tasks(self) -> list[Task]:
        return WorkflowsRunner.all(self.config).tasks()

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
                r'Provide location for the input data .*': 'main.dqx_test.input_table',
                r'Provide output table .*': 'main.dqx_test.output_table',
                r'Do you want to uninstall DQX .*': 'yes',
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
    ws: WorkspaceClient, env_or_skip: Callable[[str], str], checks_location="checks.yml"
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(env_or_skip, ws, checks_location, serverless_cluster=False)
    yield ctx.replace(workspace_client=ws)
    ctx.workspace_installation.uninstall()


@pytest.fixture
def serverless_installation_ctx(
    ws: WorkspaceClient, env_or_skip: Callable[[str], str], checks_location="checks.yml"
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(env_or_skip, ws, checks_location, serverless_cluster=True)
    yield ctx.replace(workspace_client=ws)
    ctx.workspace_installation.uninstall()


@pytest.fixture
def checks_yaml_content():
    return """- criticality: error
  check:
    function: is_not_null
    for_each_column:
      - col1
      - col2
    arguments: {}
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      column: col3
      trim_strings: true
- criticality: warn
  check:
    function: is_in_list
    arguments:
      column: col4
      allowed:
      - 1
      - 2
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col1 not like "Team %"
- criticality: error
  check:
    function: sql_expression
    arguments:
      expression: col2 not like 'Team %'
- name: check_with_user_metadata
  criticality: error
  check:
    function: is_not_null
    arguments:
      column: col1
  user_metadata:
    check_type: completeness
    check_owner: "someone@email.com"
    """


@pytest.fixture
def checks_json_content():
    return """[
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "for_each_column": ["col1", "col2"],
            "arguments": {}
        }
    },
    {
        "name": "col_col3_is_null_or_empty",
        "criticality": "error",
        "check": {
            "function": "is_not_null_and_not_empty",
            "arguments": {
                "column": "col3",
                "trim_strings": true
            }
        }
    },
    {
        "criticality": "warn",
        "check": {
            "function": "is_in_list",
            "arguments": {
                "column": "col4",
                "allowed": [1, 2]
            }
        }
    },
    {
        "criticality": "error",
        "check": {"function": "sql_expression", "arguments": {"expression": "col1 not like \\"Team %\\""}}
    },
    {
        "criticality": "error",
        "check": {"function": "sql_expression", "arguments": {"expression": "col2 not like 'Team %'"}}
    },
    {
        "name": "check_with_user_metadata", "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"}
    }
]
    """


@pytest.fixture
def checks_yaml_invalid_content():
    """This YAML has wrong indentation for the function field."""
    return """- criticality: error
  check:
function: is_not_null_and_not_empty
    for_each_column:
    col1
    - col2
    arguments: {}
    """


@pytest.fixture
def checks_json_invalid_content():
    """This JSON is missing a comma after criticality field."""
    return """[
    {
        "criticality": "error"
        "function": "is_not_null_and_not_empty",
        "for_each_column": ["col1", "col2"],
        "check": {
            "arguments": {}
        }
    }
]
    """


@pytest.fixture
def expected_checks():
    return [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "name": "col_col3_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "col3", "trim_strings": True}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "col4", "allowed": [1, 2]}},
        },
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": 'col1 not like "Team %"'}},
        },
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": "col2 not like 'Team %'"}},
        },
        {
            "name": "check_with_user_metadata",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
            "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
        },
    ]


@pytest.fixture
def make_local_check_file_as_yaml(checks_yaml_content):
    file_path = "checks.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_yaml_diff_ext(checks_yaml_content):
    file_path = "checks.yaml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_json(checks_json_content):
    file_path = "checks.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_yaml(checks_yaml_invalid_content):
    file_path = "invalid_checks.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_empty_local_yaml_file():
    file_path = "empty.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("")
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_empty_local_json_file():
    file_path = "empty.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("{}")
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_json(checks_json_invalid_content):
    file_path = "invalid_checks.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_check_file_as_yaml(ws, make_directory, checks_yaml_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.yml"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.yml"

        ws.workspace.upload(
            path=workspace_file_path, format=ImportFormat.AUTO, content=checks_yaml_content.encode(), overwrite=True
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_check_file_as_json(ws, make_directory, checks_json_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.json"

        ws.workspace.upload(
            path=workspace_file_path, format=ImportFormat.AUTO, content=checks_json_content.encode(), overwrite=True
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_invalid_check_file_as_yaml(ws, make_directory, checks_yaml_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.yml"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.yml"

        ws.workspace.upload(
            path=workspace_file_path,
            format=ImportFormat.AUTO,
            content=checks_yaml_invalid_content.encode(),
            overwrite=True,
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_invalid_check_file_as_json(ws, make_directory, checks_json_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            workspace_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            workspace_file_path = str(folder.absolute()) + "/checks.json"

        ws.workspace.upload(
            path=workspace_file_path,
            format=ImportFormat.AUTO,
            content=checks_json_invalid_content.encode(),
            overwrite=True,
        )

        return workspace_file_path

    def delete(workspace_file_path: str) -> None:
        ws.workspace.delete(workspace_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_volume_check_file_as_yaml(ws, make_directory, checks_yaml_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            volume_file_path = kwargs["install_dir"] + "/checks.yaml"
        else:
            folder = make_directory()
            volume_file_path = str(folder.absolute()) + "/checks.yaml"

        ws.files.upload(volume_file_path, checks_yaml_content.encode(), overwrite=True)

        return volume_file_path

    def delete(volume_file_path: str) -> None:
        ws.files.delete(volume_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_volume_check_file_as_json(ws, make_directory, checks_json_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            volume_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            volume_file_path = str(folder.absolute()) + "/checks.json"

        ws.files.upload(volume_file_path, checks_json_content.encode(), overwrite=True)

        return volume_file_path

    def delete(volume_file_path: str) -> None:
        ws.files.delete(volume_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_volume_invalid_check_file_as_yaml(ws, make_directory, checks_yaml_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            volume_file_path = kwargs["install_dir"] + "/checks.yaml"
        else:
            folder = make_directory()
            volume_file_path = str(folder.absolute()) + "/checks.yaml"

        ws.files.upload(volume_file_path, checks_yaml_invalid_content.encode(), overwrite=True)

        return volume_file_path

    def delete(volume_file_path: str) -> None:
        ws.files.delete(volume_file_path)

    yield from factory("file", create, delete)


@pytest.fixture
def make_volume_invalid_check_file_as_json(ws, make_directory, checks_json_invalid_content):
    def create(**kwargs):
        if kwargs["install_dir"]:
            volume_file_path = kwargs["install_dir"] + "/checks.json"
        else:
            folder = make_directory()
            volume_file_path = str(folder.absolute()) + "/checks.json"

        ws.files.upload(volume_file_path, checks_json_invalid_content.encode(), overwrite=True)

        return volume_file_path

    def delete(volume_file_path: str) -> None:
        ws.files.delete(volume_file_path)

    yield from factory("file", create, delete)
