import pytest
from databricks.labs.dqx.config import WorkspaceConfig, RunConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.config import (
    FileChecksStorageConfig,
    LakebaseChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,
)
from databricks.labs.dqx.errors import InvalidConfigError, InvalidParameterError

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
    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        CONFIG.get_run_config("not_found")


def test_get_run_config_when_no_run_configs():
    config = WorkspaceConfig(run_configs=[])
    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        config.get_run_config(None)


@pytest.mark.parametrize(
    "config_class, location, expected_exception, expected_message",
    [
        (
            FileChecksStorageConfig,
            None,
            InvalidConfigError,
            "The file path \\('location' field\\) must not be empty or None",
        ),
        (
            FileChecksStorageConfig,
            "",
            InvalidConfigError,
            "The file path \\('location' field\\) must not be empty or None",
        ),
        (
            WorkspaceFileChecksStorageConfig,
            None,
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            WorkspaceFileChecksStorageConfig,
            "",
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            TableChecksStorageConfig,
            None,
            InvalidConfigError,
            "The table name \\('location' field\\) must not be empty or None",
        ),
        (
            TableChecksStorageConfig,
            "",
            InvalidConfigError,
            "The table name \\('location' field\\) must not be empty or None",
        ),
        (
            InstallationChecksStorageConfig,
            None,
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (
            InstallationChecksStorageConfig,
            "",
            InvalidConfigError,
            "The workspace file path \\('location' field\\) must not be empty or None",
        ),
        (LakebaseChecksStorageConfig, "", InvalidParameterError, "Location must not be empty or None."),
        (
            VolumeFileChecksStorageConfig,
            "",
            InvalidParameterError,
            "The Unity Catalog volume file path \\('location' field\\) must not be empty or None.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "invalid_volume_path/files",
            InvalidParameterError,
            "The volume path must start with '/Volumes/'.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes",
            InvalidParameterError,
            "The volume path must start with '/Volumes/'.",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main",
            InvalidParameterError,
            "Invalid path: Path is missing a schema name",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main/demo",
            InvalidParameterError,
            "Invalid path: Path is missing a volume name",
        ),
        (
            VolumeFileChecksStorageConfig,
            "/Volumes/main/demo/files/my_file.txt",
            InvalidParameterError,
            "Invalid path: Path must include a file name after the volume",
        ),
    ],
)
def test_post_init_validation(config_class, location, expected_exception, expected_message):
    with pytest.raises(expected_exception, match=expected_message):
        config_class(location=location)
