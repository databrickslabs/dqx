import logging
from dataclasses import dataclass
import yaml
import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.dqx.cli import (
    open_remote_config,
    installations,
    validate_checks,
    profile,
    workflows,
    logs,
    open_dashboards,
    apply_checks,
    e2e,
)
from databricks.labs.dqx.config import WorkspaceConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.sdk.errors import NotFound
from tests.integration.conftest import contains_expected_workflows


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
    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    make_workspace_file(path=checks_location, content=yaml.dump(checks))

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
    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    make_workspace_file(path=checks_location, content=yaml.dump(checks))

    errors = validate_checks(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    expected_errors = [
        "function 'invalid_func' is not defined",
        "'check' field is missing",
    ]
    assert len(errors) == len(expected_errors)
    for e in expected_errors:
        assert any(e in error["error"] for error in errors)


def test_validate_checks_disable_validate_custom_check_functions(ws, make_workspace_file, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    checks = [
        {"criticality": "warn", "check": {"function": "invalid_func", "arguments": {"column": "a"}}},
    ]
    run_config = installation_ctx.config.get_run_config()
    checks_location = f"{installation_ctx.installation.install_folder()}/{run_config.checks_location}"
    make_workspace_file(path=checks_location, content=yaml.dump(checks))

    errors = validate_checks(
        installation_ctx.workspace_client,
        validate_custom_check_functions=False,
        ctx=installation_ctx.workspace_installer,
    )
    assert not errors


def test_validate_checks_invalid_run_config(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)

    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        validate_checks(
            installation_ctx.workspace_client, run_config="unavailable", ctx=installation_ctx.workspace_installer
        )


def test_validate_checks_when_checks_file_missing(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    install_dir = installation_ctx.installation.install_folder()
    file = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    with pytest.raises(NotFound, match=f"Checks file {file} missing"):
        validate_checks(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)


def test_profiler(ws, spark, setup_workflows, caplog):
    installation_ctx, run_config = setup_workflows()

    profile(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)

    checks = DQEngine(ws, spark).load_checks(
        config=InstallationChecksStorageConfig(
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


def test_profiler_serverless(ws, spark, setup_serverless_workflows, caplog):
    installation_ctx, run_config = setup_serverless_workflows()

    profile(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)

    checks = DQEngine(ws, spark).load_checks(
        config=InstallationChecksStorageConfig(
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


def test_profiler_no_input_configured(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows()

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.input_config = None  # Simulate no input data source configured
    installation_ctx.installation.save(config)

    with pytest.raises(ManyError, match="No input data source configured during installation"):
        profile(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)


def test_quality_checker(ws, spark, setup_workflows, caplog, expected_quality_checking_output):
    installation_ctx, run_config = setup_workflows(checks=True)

    apply_checks(
        installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer
    )

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "Completed quality-checker workflow run" in caplog.text


def test_quality_checker_serverless(ws, spark, setup_serverless_workflows, caplog, expected_quality_checking_output):
    installation_ctx, run_config = setup_serverless_workflows(checks=True)

    apply_checks(
        installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer
    )

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "Completed quality-checker workflow run" in caplog.text


def test_quality_checker_no_input_configured(ws, spark, setup_serverless_workflows):
    installation_ctx, run_config = setup_serverless_workflows(checks=True)

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.input_config = None  # Simulate no input data source configured
    installation_ctx.installation.save(config)

    with pytest.raises(ManyError, match="No input data source configured during installation"):
        apply_checks(
            installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer
        )


def test_e2e_workflow(ws, spark, setup_workflows, caplog):
    installation_ctx, run_config = setup_workflows()
    e2e(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)
    _assert_e2e_workflow(caplog, installation_ctx, run_config, spark)


def test_e2e_workflow_serverless(ws, spark, setup_serverless_workflows, caplog):
    installation_ctx, run_config = setup_serverless_workflows()
    e2e(installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer)
    _assert_e2e_workflow(caplog, installation_ctx, run_config, spark)


def _assert_e2e_workflow(caplog, installation_ctx, run_config, spark):
    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    # this is sanity check only, we cannot predict the exact output as it depends on the generated rules
    assert checked_df.count() == input_df.count(), "Output table is empty"

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "Completed e2e workflow run" in caplog.text


def test_profiler_when_run_config_missing(ws, installation_ctx):
    installation_ctx.installation_service.run()

    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        installation_ctx.deployed_workflows.run_workflow("profiler", run_config_name="unavailable")


def test_quality_checker_when_run_config_missing(ws, installation_ctx):
    installation_ctx.installation_service.run()

    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        installation_ctx.deployed_workflows.run_workflow("quality-checker", run_config_name="unavailable")


def test_workflows(ws, installation_ctx):
    installation_ctx.installation_service.run()
    installed_workflows = workflows(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    expected_workflows_state = [{'workflow': 'profiler', 'state': 'UNKNOWN', 'started': '<never run>'}]
    for state in expected_workflows_state:
        assert contains_expected_workflows(installed_workflows, state)


def test_workflows_not_installed(ws, installation_ctx):
    installed_workflows = workflows(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)
    assert not installed_workflows


def test_logs(ws, installation_ctx, caplog):
    installation_ctx.installation_service.run()

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "No jobs to relay logs for" in caplog.text


def test_open_remote_config_with_custom_folder(ws, installation_ctx_custom_install_folder, webbrowser_open):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()
    open_remote_config(w=installation_ctx_custom_install_folder.workspace_client, install_folder=custom_folder)
    webbrowser_open.assert_called_once_with(
        installation_ctx_custom_install_folder.installation.workspace_link(WorkspaceConfig.__file__)
    )


def test_open_dashboards_with_custom_folder(ws, installation_ctx_custom_install_folder, webbrowser_open):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()
    open_dashboards(w=installation_ctx_custom_install_folder.workspace_client, install_folder=custom_folder)
    webbrowser_open.assert_called_once_with(
        installation_ctx_custom_install_folder.installation.workspace_link("") + "dashboards/"
    )


def test_validate_checks_with_custom_folder(ws, make_workspace_file, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()

    checks = [{"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"column": "a"}}}]
    run_config = installation_ctx_custom_install_folder.config.get_run_config()
    checks_location = f"{custom_folder}/{run_config.checks_location}"
    make_workspace_file(path=checks_location, content=yaml.dump(checks))

    errors_list = validate_checks(
        installation_ctx_custom_install_folder.workspace_client,
        run_config=run_config.name,
        install_folder=custom_folder,
    )

    assert not errors_list


def test_validate_checks_with_custom_folder_invalid_checks(
    ws, make_workspace_file, installation_ctx_custom_install_folder
):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()

    checks = [
        {"criticality": "warn", "check": {"function": "invalid_func", "arguments": {"column": "a"}}},
        {"criticality": "warn", "check_missing": {"function": "is_not_null", "arguments": {"column": "b"}}},
    ]
    run_config = installation_ctx_custom_install_folder.config.get_run_config()
    checks_location = f"{custom_folder}/{run_config.checks_location}"
    make_workspace_file(path=checks_location, content=yaml.dump(checks))

    errors = validate_checks(installation_ctx_custom_install_folder.workspace_client, install_folder=custom_folder)

    expected_errors = [
        "function 'invalid_func' is not defined",
        "'check' field is missing",
    ]
    assert len(errors) == len(expected_errors)
    for e in expected_errors:
        assert any(e in error["error"] for error in errors)


def test_profile_with_custom_folder(ws, spark, setup_workflows_with_custom_folder, caplog):
    installation_ctx, run_config = setup_workflows_with_custom_folder()
    custom_folder = installation_ctx.installation.install_folder()

    profile(installation_ctx.workspace_client, run_config=run_config.name, install_folder=custom_folder)

    checks = DQEngine(ws, spark).load_checks(
        config=InstallationChecksStorageConfig(
            run_config_name=run_config.name,
            assume_user=True,
            product_name=installation_ctx.installation.product(),
            install_folder=custom_folder,
        ),
    )
    assert checks, "Checks were not loaded correctly"

    status = ws.workspace.get_status(f"{custom_folder}/{run_config.profiler_config.summary_stats_file}")
    assert status, f"Profile summary stats file {run_config.profiler_config.summary_stats_file} does not exist."

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, install_folder=custom_folder)

    assert "Completed profiler workflow run" in caplog.text


def test_apply_checks_with_custom_folder(
    ws, spark, setup_workflows_with_custom_folder, caplog, expected_quality_checking_output
):
    installation_ctx, run_config = setup_workflows_with_custom_folder(checks=True)
    custom_folder = installation_ctx.installation.install_folder()

    apply_checks(installation_ctx.workspace_client, run_config=run_config.name, install_folder=custom_folder)

    checked_df = spark.table(run_config.output_config.location)
    assert_df_equality(checked_df, expected_quality_checking_output, ignore_nullable=True)

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, install_folder=custom_folder)

    assert "Completed quality-checker workflow run" in caplog.text


def test_e2e_with_custom_folder(ws, spark, setup_workflows_with_custom_folder, caplog):
    installation_ctx, run_config = setup_workflows_with_custom_folder()
    custom_folder = installation_ctx.installation.install_folder()

    e2e(installation_ctx.workspace_client, run_config=run_config.name, install_folder=custom_folder)

    checked_df = spark.table(run_config.output_config.location)
    input_df = spark.table(run_config.input_config.location)

    assert checked_df.count() == input_df.count(), "Output table is empty"

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, install_folder=custom_folder)

    assert "Completed e2e workflow run" in caplog.text


def test_workflows_with_custom_folder(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation_service.run()

    installed_workflows = workflows(
        installation_ctx_custom_install_folder.workspace_client,
        ctx=installation_ctx_custom_install_folder.workspace_installer,
    )

    expected_workflows_state = [{'workflow': 'profiler', 'state': 'UNKNOWN', 'started': '<never run>'}]
    for state in expected_workflows_state:
        assert contains_expected_workflows(installed_workflows, state)


def test_logs_with_custom_folder(ws, installation_ctx_custom_install_folder, caplog):
    installation_ctx_custom_install_folder.installation_service.run()
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()

    with caplog.at_level(logging.INFO):
        logs(installation_ctx_custom_install_folder.workspace_client, install_folder=custom_folder)

    assert "No jobs to relay logs for" in caplog.text


def test_validate_checks_with_custom_folder_missing_file(ws, installation_ctx_custom_install_folder):
    installation_ctx_custom_install_folder.installation.save(installation_ctx_custom_install_folder.config)
    custom_folder = installation_ctx_custom_install_folder.installation.install_folder()

    file = f"{custom_folder}/{installation_ctx_custom_install_folder.config.get_run_config().checks_location}"

    with pytest.raises(NotFound, match=f"Checks file {file} missing"):
        validate_checks(installation_ctx_custom_install_folder.workspace_client, install_folder=custom_folder)
