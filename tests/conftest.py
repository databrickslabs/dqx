import os
from typing import Any
import re
from collections.abc import Callable, Generator
from dataclasses import replace
from functools import cached_property

import pytest
from databricks.labs.blueprint.installation import Installation, MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.installer.warehouse_installer import WarehouseInstaller
from databricks.labs.dqx.installer.workflow_installer import WorkflowDeployment
from databricks.labs.dqx.installer.workflow_task import Task
from databricks.labs.dqx.installer.install import WorkspaceInstaller, InstallationService
from databricks.labs.dqx.workflows_runner import WorkflowsRunner
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.database import DatabaseInstance, DatabaseCatalog


@pytest.fixture(scope="session")
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
        )

    @cached_property
    def run_config_name(self) -> str | None:
        return self.config.get_run_config().name

    @cached_property
    def patterns(self) -> str | None:
        return ""


class MockInstallationContext(MockWorkflowContext):
    __test__ = False

    def __init__(
        self,
        env_or_skip_fixture: Callable[[str], str],
        ws: WorkspaceClient,
        checks_location,
        serverless_clusters: bool = True,
        install_folder: str | None = None,
    ):
        super().__init__(env_or_skip_fixture, ws)
        self.checks_location = checks_location
        self.serverless_clusters = serverless_clusters
        self.install_folder = install_folder

    @cached_property
    def installation(self):
        return Installation(self.workspace_client, self.product_info.product_name(), install_folder=self.install_folder)

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self):
        return WorkspaceInstaller(
            self.workspace_client,
            self.environ,
            self.install_folder,
        ).replace(prompts=self.prompts, installation=self.installation, product_info=self.product_info)

    @cached_property
    def config_transform(self) -> Callable[[WorkspaceConfig], WorkspaceConfig]:
        return lambda wc: wc

    @cached_property
    def config(self) -> WorkspaceConfig:
        workspace_config = self.workspace_installer.configure()
        workspace_config.serverless_clusters = self.serverless_clusters

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
    def workflows_deployment(self) -> WorkflowDeployment:
        return WorkflowDeployment(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            WheelsV2(self.installation, self.product_info),
            self.product_info,
            self.tasks,
        )

    @cached_property
    def warehouse_installer(self) -> WarehouseInstaller:
        return WarehouseInstaller(self.workspace_client, self.prompts)

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
    def installation_service(self) -> InstallationService:
        return InstallationService(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.workflows_deployment,
            self.warehouse_installer,
            self.prompts,
            self.product_info,
        )


@pytest.fixture
def installation_ctx(
    ws: WorkspaceClient, env_or_skip: Callable[[str], str], checks_location="checks.yml"
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(env_or_skip, ws, checks_location, serverless_clusters=False)
    yield ctx.replace(workspace_client=ws)
    ctx.installation_service.uninstall()


@pytest.fixture
def serverless_installation_ctx(
    ws: WorkspaceClient, env_or_skip: Callable[[str], str], checks_location="checks.yml"
) -> Generator[MockInstallationContext, None, None]:
    ctx = MockInstallationContext(env_or_skip, ws, checks_location, serverless_clusters=True)
    yield ctx.replace(workspace_client=ws)
    ctx.installation_service.uninstall()


@pytest.fixture
def installation_ctx_custom_install_folder(
    ws: WorkspaceClient, make_directory, env_or_skip: Callable[[str], str], checks_location="checks.yml"
) -> Generator[MockInstallationContext, None, None]:
    custom_folder = str(make_directory().absolute())
    ctx = MockInstallationContext(
        env_or_skip, ws, checks_location, serverless_clusters=False, install_folder=custom_folder
    )
    yield ctx.replace(workspace_client=ws)
    ctx.installation_service.uninstall()


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
def make_local_check_file_as_yaml(checks_yaml_content, make_random):
    file_path = f"checks_{make_random(10).lower()}.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_yaml_diff_ext(checks_yaml_content, make_random):
    file_path = f"checks_{make_random(10).lower()}.yaml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_local_check_file_as_json(checks_json_content, make_random):
    file_path = f"checks_{make_random(10).lower()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_yaml(checks_yaml_invalid_content, make_random):
    file_path = f"invalid_checks_{make_random(10).lower()}.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_yaml_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_empty_local_yaml_file(make_random):
    file_path = f"empty_{make_random(10).lower()}.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("")
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_empty_local_json_file(make_random):
    file_path = f"empty_{make_random(10).lower()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("{}")
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_invalid_local_check_file_as_json(checks_json_invalid_content, make_random):
    file_path = f"invalid_checks_{make_random(10).lower()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(checks_json_invalid_content)
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


@pytest.fixture
def make_check_file_as_yaml(ws, make_directory, checks_yaml_content):
    def create(**kwargs):
        if "install_dir" in kwargs and kwargs["install_dir"]:
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


@pytest.fixture
def make_lakebase_instance_and_catalog(ws, make_random):
    import logging

    logger = logging.getLogger(__name__)

    instances = {}

    def create() -> str:
        database_instance_name = f"dqxtest-{make_random(10).lower()}"
        database_name = "dqx"  # does not need to be random
        catalog_name = f"dqxtest-{make_random(10).lower()}"
        capacity = "CU_2"

        instance = ws.database.create_database_instance_and_wait(
            database_instance=DatabaseInstance(name=database_instance_name, capacity=capacity)
        )

        ws.database.create_database_catalog(
            DatabaseCatalog(
                name=catalog_name,
                database_name=database_name,
                database_instance_name=database_instance_name,
                create_database_if_not_exists=True,
            )
        )

        connection_string = (
            f"postgresql://{instance.creator}:password@{instance.read_only_dns}:5432/{database_name}?sslmode=require"
        )

        instances[connection_string] = {"database_instance_name": database_instance_name, "catalog_name": catalog_name}

        return connection_string

    def delete(connection_string: str) -> None:
        if connection_string not in instances:
            logger.warning(f"No resources found for connection string: {connection_string}")
            return

        metadata = instances[connection_string]
        database_instance_name = metadata["database_instance_name"]
        catalog_name = metadata["catalog_name"]

        try:
            ws.database.delete_database_catalog(name=catalog_name)
            logger.info(f"Successfully deleted database catalog: {catalog_name}")
        except Exception as e:
            logger.warning(f"Failed to delete database catalog {catalog_name}: {e}")

        try:
            ws.database.delete_database_instance(name=database_instance_name)
            logger.info(f"Successfully deleted database instance: {database_instance_name}")
        except Exception as e:
            logger.error(f"Failed to delete database instance {database_instance_name}: {e}")
            raise
        finally:
            instances.pop(connection_string, None)

    yield from factory("lakebase", create, delete)


def sort_key(check: dict[str, Any]) -> str:
    """
    Sorts a checks dictionary by the 'name' field.

    Args:
        check: The check dictionary.

    Returns:
        The name of the check as a string, or an empty string if not found.
    """
    return str(check.get("name", ""))


def compare_checks(result: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    """
    Compares two lists of checks dictionaries for equality, ensuring
    they contain the same checks with identical fields.

    Args:
        result: The result checks list.
        expected: The expected checks list.

    Returns:
        None
    """
    assert len(result) == len(expected), f"Expected {len(expected)} checks, got {len(result)}"
    sorted_result = sorted(result, key=sort_key)
    sorted_expected = sorted(expected, key=sort_key)
    for res, exp in zip(sorted_result, sorted_expected):
        for key in exp:
            assert res.get(key) == exp[key], f"Mismatch for key '{key}': {res.get(key)} != {exp[key]}"
