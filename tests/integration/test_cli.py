import logging
from dataclasses import dataclass
import yaml
import pytest

from tests.integration.conftest import contains_expected_workflows
from databricks.labs.dqx.cli import (
    open_remote_config,
    installations,
    validate_checks,
    profile,
    workflows,
    logs,
    open_dashboards,
)
from databricks.labs.dqx.config import WorkspaceConfig, InstallationChecksStorageConfig
from databricks.sdk.errors import NotFound
from databricks.labs.dqx.engine import DQEngine

logger = logging.getLogger(__name__)


def test_open_remote_config(ws, installation_ctx, webbrowser_open):
    installation_ctx.installation.save(installation_ctx.config)
    open_remote_config(w=installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)
    webbrowser_open.assert_called_once_with(installation_ctx.installation.workspace_link(WorkspaceConfig.__file__))


def test_open_dashboards_directory(ws, installation_ctx, webbrowser_open):
    installation_ctx.installation.save(installation_ctx.config)
    open_dashboards(w=installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)
    webbrowser_open.assert_called_once_with(installation_ctx.installation.workspace_link("") + "dashboards/")


def test_installations_output(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    output = installations(
        w=installation_ctx.workspace_client, product_name=installation_ctx.product_info.product_name()
    )

    expected_output = [
        {"version": installation_ctx.config.__version__, "path": installation_ctx.installation.install_folder()}
    ]
    assert output == expected_output


def test_installations_output_not_found(ws, installation_ctx):
    output = installations(
        w=installation_ctx.workspace_client, product_name=installation_ctx.product_info.product_name()
    )
    assert not output


def test_installations_output_serde_error(ws, installation_ctx):
    @dataclass
    class InvalidConfig:
        __version__ = WorkspaceConfig.__version__
        fake = "fake"

    installation_ctx.installation.save(InvalidConfig(), filename=WorkspaceConfig.__file__)
    output = installations(
        w=installation_ctx.workspace_client, product_name=installation_ctx.product_info.product_name()
    )
    assert not output


def test_validate_checks(ws, make_workspace_file, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    checks = [{"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]

    run_config = installation_ctx.config.get_run_config()
    checks_file = f"{installation_ctx.installation.install_folder()}/{run_config.checks_file}"
    make_workspace_file(path=checks_file, content=yaml.dump(checks))

    errors_list = validate_checks(
        installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer
    )

    assert not errors_list


def test_validate_checks_when_given_invalid_checks(ws, make_workspace_file, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    checks = [
        {"criticality": "warn", "check": {"function": "invalid_func", "arguments": {"column": "a"}}},
        {"criticality": "warn", "check_missing": {"function": "is_not_null", "arguments": {"column": "b"}}},
    ]
    run_config = installation_ctx.config.get_run_config()
    checks_file = f"{installation_ctx.installation.install_folder()}/{run_config.checks_file}"
    make_workspace_file(path=checks_file, content=yaml.dump(checks))

    errors = validate_checks(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    expected_errors = [
        "function 'invalid_func' is not defined",
        "'check' field is missing",
    ]
    assert len(errors) == len(expected_errors)
    for e in expected_errors:
        assert any(e in error["error"] for error in errors)


def test_validate_checks_invalid_run_config(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)

    with pytest.raises(ValueError, match="No run configurations available"):
        validate_checks(
            installation_ctx.workspace_client, run_config="unavailable", ctx=installation_ctx.workspace_installer
        )


def test_validate_checks_when_checks_file_missing(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)

    with pytest.raises(NotFound, match="Checks file checks.yml missing"):
        validate_checks(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)


def test_profiler(ws, setup_workflows, caplog):
    installation_ctx, run_config = setup_workflows

    profile(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)

    checks = DQEngine(ws).load_checks(
        config=InstallationChecksStorageConfig(
            location="installation",
            run_config_name=run_config.name,
            assume_user=True,
            product_name=installation_ctx.installation.product(),
        ),
    )
    assert checks, "Checks were not loaded correctly"

    install_folder = installation_ctx.installation.install_folder()
    status = ws.workspace.get_status(f"{install_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "Completed profiler workflow run" in caplog.text


def test_profiler_when_run_config_missing(ws, installation_ctx):
    installation_ctx.workspace_installation.run()

    with pytest.raises(ValueError, match="No run configurations available"):
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config_name="unavailable")


def test_workflows(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    installed_workflows = workflows(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    expected_workflows_state = [{'workflow': 'profiler', 'state': 'UNKNOWN', 'started': '<never run>'}]
    for state in expected_workflows_state:
        assert contains_expected_workflows(installed_workflows, state)


def test_workflows_not_installed(ws, installation_ctx):
    installed_workflows = workflows(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)
    assert not installed_workflows


def test_logs(ws, installation_ctx, caplog):
    installation_ctx.workspace_installation.run()

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "No jobs to relay logs for" in caplog.text
