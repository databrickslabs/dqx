import os
import base64
import yaml
import pytest
from databricks.labs.dqx.config import WorkspaceConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.errors import InvalidConfigError


@pytest.fixture
def save_local(ws, make_random):
    temp_files = []

    def _save_local(config_path):
        temp_file = f"{make_random(10)}.yml"
        export = ws.workspace.export(config_path)
        content = base64.b64decode(export.content).decode('utf-8')
        yaml_content = yaml.safe_load(content)
        with open(temp_file, 'w', encoding="utf-8") as local_file:
            yaml.dump(yaml_content, local_file)
        temp_files.append(temp_file)
        return temp_file

    yield _save_local

    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)


def test_runtime_config(ws, installation_ctx, save_local):
    installation_ctx.installation.save(installation_ctx.config)
    run_config = installation_ctx.config.get_run_config()

    install_config_path = f"{installation_ctx.installation.install_folder()}/{WorkspaceConfig.__file__}"
    # need to bring the config locally since the context is expected to run in the workspace with local access
    local_config_path = save_local(install_config_path)

    runtime_context = WorkflowContext(
        named_parameters={
            "config": local_config_path,
            "run_config_name": run_config.name,
        }
    )

    actual_config = runtime_context.config
    actual_run_config = runtime_context.run_config

    # the actual context prefix relative path with installation folder
    actual_run_config.checks_location = os.path.basename(actual_run_config.checks_location)

    assert actual_config
    assert actual_config.get_run_config() == run_config
    assert actual_run_config
    assert actual_run_config == run_config
    assert runtime_context.workspace_client
    assert runtime_context.installation


def test_runtime_config_when_missing_run_config():
    runtime_context = WorkflowContext(named_parameters={"config": "temp"})
    with pytest.raises(InvalidConfigError, match="Run config flag is required"):
        run_config = runtime_context.run_config
        assert not run_config
