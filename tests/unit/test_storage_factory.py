from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.checks_storage import (
    ChecksStorageHandlerFactory,
    TableChecksStorageHandler,
    VolumeFileChecksStorageHandler,
    WorkspaceFileChecksStorageHandler,
    FileChecksStorageHandler,
    TableChecksStorageConfig,
    VolumeFileChecksStorageConfig,
    WorkspaceFileChecksStorageConfig,
    FileChecksStorageConfig,
    InstallationChecksStorageHandler,
)
from databricks.labs.dqx.config import InstallationChecksStorageConfig, BaseChecksStorageConfig
from databricks.labs.dqx.errors import InvalidConfigError

workspace_client_mock = create_autospec(ChecksStorageHandlerFactory, instance=True)
spark_mock = create_autospec(ChecksStorageHandlerFactory, instance=True)
STORAGE_FACTORY = ChecksStorageHandlerFactory(workspace_client_mock, spark_mock)


def test_create_file_checks_storage_handler():
    config = FileChecksStorageConfig(location="path/to/file.yaml")
    handler = STORAGE_FACTORY.create(config)
    assert isinstance(handler, FileChecksStorageHandler)


def test_create_installation_checks_storage_handler():
    config = InstallationChecksStorageConfig(run_config_name="test_run_config", product_name="test_product")
    handler = STORAGE_FACTORY.create(config)
    assert isinstance(handler, InstallationChecksStorageHandler)


def test_create_workspace_file_checks_storage_handler():
    config = WorkspaceFileChecksStorageConfig(location="/Workspace/path/to/file.yaml")
    handler = STORAGE_FACTORY.create(config)
    assert isinstance(handler, WorkspaceFileChecksStorageHandler)


def test_create_table_checks_storage_handler():
    config = TableChecksStorageConfig(location="delta.`/path/to/table`", run_config_name="test_run_config")
    handler = STORAGE_FACTORY.create(config)
    assert isinstance(handler, TableChecksStorageHandler)


def test_create_volume_file_checks_storage_handler():
    config = VolumeFileChecksStorageConfig(location="/Volumes/my_volume/path/to/file.yaml")
    handler = STORAGE_FACTORY.create(config)
    assert isinstance(handler, VolumeFileChecksStorageHandler)


def test_create_unsupported_config():
    class UnsupportedConfig(BaseChecksStorageConfig):
        pass

    config = UnsupportedConfig()
    with pytest.raises(InvalidConfigError, match="Unsupported storage config type"):
        STORAGE_FACTORY.create(config)


def test_create_for_location_table():
    location = "catalog.schema.table"

    handler, config = STORAGE_FACTORY.create_for_location(location)

    assert isinstance(handler, TableChecksStorageHandler)
    assert isinstance(config, TableChecksStorageConfig)
    assert config.location == location


def test_create_for_location_volume():
    location = "/Volumes/my_volume/path/to/file.yaml"

    handler, config = STORAGE_FACTORY.create_for_location(location)

    assert isinstance(handler, VolumeFileChecksStorageHandler)
    assert isinstance(config, VolumeFileChecksStorageConfig)
    assert config.location == location


def test_create_for_location_workspace():
    location = "/Workspace/my_workspace/path/to/file.yaml"

    handler, config = STORAGE_FACTORY.create_for_location(location)

    assert isinstance(handler, WorkspaceFileChecksStorageHandler)
    assert isinstance(config, WorkspaceFileChecksStorageConfig)
    assert config.location == location


def test_create_for_location_file():
    location = "relative/path/to/file.yaml"

    handler, config = STORAGE_FACTORY.create_for_location(location)

    assert isinstance(handler, FileChecksStorageHandler)
    assert isinstance(config, FileChecksStorageConfig)
    assert config.location == location
