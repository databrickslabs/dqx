import pytest
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.config import (
    FileChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
)


DEFAULT_RUN_CONFIG_NAME = "default"
DEFAULT_RUN_CONFIG = RunConfig(
    name=DEFAULT_RUN_CONFIG_NAME,
)

CONFIG = WorkspaceConfig(
    run_configs=[
        DEFAULT_RUN_CONFIG,
        RunConfig(
            name="another_run_config",
        ),
    ]
)


@pytest.mark.parametrize(
    "run_config_name, expected_result",
    [
        ("default", CONFIG.run_configs[0]),
        (None, CONFIG.run_configs[0]),
        ("another_run_config", CONFIG.run_configs[1]),
    ],
)
def test_get_run_config_valid_names(run_config_name, expected_result):
    assert CONFIG.get_run_config(run_config_name) == expected_result


def test_get_run_config_when_name_not_found():
    with pytest.raises(ValueError, match="No run configurations available"):
        CONFIG.get_run_config("not_found")


def test_get_run_config_when_no_run_configs():
    config = WorkspaceConfig(run_configs=[])
    with pytest.raises(ValueError, match="No run configurations available"):
        config.get_run_config(None)


@pytest.mark.parametrize(
    "config_class, location, expected_message",
    [
        (FileChecksStorageConfig, None, "The file path \\('location' field\\) must not be empty or None"),
        (FileChecksStorageConfig, "", "The file path \\('location' field\\) must not be empty or None"),
        (
            WorkspaceFileChecksStorageConfig,
            None,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            WorkspaceFileChecksStorageConfig,
            "",
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (TableChecksStorageConfig, None, "The table name \\('location' field\\) must not be empty or None"),
        (TableChecksStorageConfig, "", "The table name \\('location' field\\) must not be empty or None"),
        (
            InstallationChecksStorageConfig,
            None,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            InstallationChecksStorageConfig,
            "",
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
    ],
)
def test_post_init_validation(config_class, location, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        config_class(location=location)
