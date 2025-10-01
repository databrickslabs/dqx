from unittest.mock import create_autospec

import pytest
from testing.postgresql import Postgresql
from sqlalchemy import create_engine, insert

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.files import DownloadResponse

from databricks.labs.dqx.checks_storage import VolumeFileChecksStorageHandler, LakebaseChecksStorageHandler
from databricks.labs.dqx.config import LakebaseChecksStorageConfig, VolumeFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.errors import InvalidCheckError, CheckDownloadError, InvalidConfigError

from tests.conftest import compare_checks
from tests.unit.test_save_checks import TEST_CHECKS


def test_load_checks_from_local_file_json(make_local_check_file_as_json, expected_checks):
    file = make_local_check_file_as_json
    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == expected_checks, "The loaded checks do not match the expected checks."


def test_load_checks_from_local_file_yaml(make_local_check_file_as_yaml, expected_checks):
    file = make_local_check_file_as_yaml
    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == expected_checks, "The loaded checks do not match the expected checks."


def test_load_checks_from_local_file_yml(make_local_check_file_as_yaml_diff_ext, expected_checks):
    file = make_local_check_file_as_yaml_diff_ext
    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == expected_checks, "The loaded checks do not match the expected checks."


def test_load_invalid_checks_from_local_file_json(make_invalid_local_check_file_as_json, expected_checks):
    file = make_invalid_local_check_file_as_json
    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {file}"):
        DQEngineCore.load_checks_from_local_file(file)


def test_load_invalid_checks_from_local_file_yaml(make_invalid_local_check_file_as_yaml, expected_checks):
    file = make_invalid_local_check_file_as_yaml
    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {file}"):
        DQEngineCore.load_checks_from_local_file(file)


def test_load_empty_checks_from_local_file_yaml(make_empty_local_yaml_file):
    file = make_empty_local_yaml_file
    assert DQEngineCore.load_checks_from_local_file(file) == []


def test_load_empty_checks_from_local_file_json(make_empty_local_json_file):
    file = make_empty_local_json_file
    assert DQEngineCore.load_checks_from_local_file(file) == []


@pytest.mark.parametrize(
    "filename, expected_exception, expected_message",
    [
        ("", InvalidConfigError, "The file path \\('location' field\\) must not be empty or None"),
        (None, InvalidConfigError, "The file path \\('location' field\\) must not be empty or None"),
        ("missing.yml", FileNotFoundError, "Checks file missing.yml missing"),
    ],
)
def test_load_checks_from_local_file_exceptions(filename, expected_exception, expected_message):
    with pytest.raises(expected_exception, match=expected_message):
        DQEngineCore.load_checks_from_local_file(filename)


def test_file_download_contents_none():
    ws = create_autospec(WorkspaceClient)
    handler = VolumeFileChecksStorageHandler(ws)
    # Simulate file_download.contents being None
    ws.files.download.return_value.contents = None
    with pytest.raises(CheckDownloadError, match="File download failed at Unity Catalog volume path"):
        handler.load(VolumeFileChecksStorageConfig(location="test_path"))


def test_file_download_contents_read_none():
    # Simulate file_download.contents.read() returning None
    ws = create_autospec(WorkspaceClient)
    handler = VolumeFileChecksStorageHandler(ws)

    mock_file_download = create_autospec(DownloadResponse, instance=True)
    mock_file_download.contents.read.return_value = None
    ws.files.download.return_value = mock_file_download

    with pytest.raises(NotFound, match="No contents at Unity Catalog volume path"):
        handler.load(VolumeFileChecksStorageConfig(location="test_path"))


def test_lakebase_checks_storage_handler_load(ws, spark):
    location = "test.public.checks"

    with Postgresql() as postgresql:
        connection_string = postgresql.url()
        engine = create_engine(connection_string)
        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(location, connection_string)

        schema_name, table_name = handler.get_schema_and_table_name(config)
        table = handler.get_table_definition(schema_name=schema_name, table_name=table_name)
        table.metadata.create_all(engine, checkfirst=True)

        with engine.begin() as conn:
            conn.execute(insert(table), TEST_CHECKS)

        result = handler.load(config)

        compare_checks(result, TEST_CHECKS)
