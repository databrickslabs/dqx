from unittest.mock import create_autospec
import pytest
from databricks.sdk.service.files import DownloadResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_storage import VolumeFileChecksStorageHandler, LakebaseChecksStorageHandler
from databricks.labs.dqx.config import LakebaseChecksStorageConfig, VolumeFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngineCore

import testing.postgresql
from sqlalchemy import (
    Engine,
    create_engine,
    text,
    MetaData,
    Table,
    Column,
    String,
    Text,
    insert,
    select,
    delete,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError, DatabaseError, ProgrammingError


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
    with pytest.raises(ValueError, match=f"Invalid checks in file: {file}"):
        DQEngineCore.load_checks_from_local_file(file)


def test_load_invalid_checks_from_local_file_yaml(make_invalid_local_check_file_as_yaml, expected_checks):
    file = make_invalid_local_check_file_as_yaml
    with pytest.raises(ValueError, match=f"Invalid checks in file: {file}"):
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
        ("", ValueError, "The file path \\('location' field\\) must not be empty or None"),
        (None, ValueError, "The file path \\('location' field\\) must not be empty or None"),
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
    with pytest.raises(ValueError, match="File download failed at Unity Catalog volume path"):
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


def test_lakebase_checks_storage_handler_load():
    # Simulate successful loading of checks from Lakebase instance
    ws = create_autospec(WorkspaceClient)
    spark = create_autospec("pyspark.sql.SparkSession")

    schema_name = "public"
    table_name = "checks"
    metadata = MetaData(schema=schema_name)

    table = Table(
        table_name,
        metadata,
        Column("name", String(255)),
        Column("criticality", String(50), default="error"),
        Column("check", JSONB),
        Column("filter", Text),
        Column("run_config_name", String(255), default="default"),
        Column("user_metadata", JSONB),
    )

    expected_checks = [
        {
            'name': 'id_is_null',
            'criticality': 'error',
            'check': {'function': 'is_not_null', 'arguments': {'column': 'id'}},
            'filter': None,
            'run_config_name': 'default',
            'user_metadata': None,
        },
        {
            'name': 'name_is_null',
            'criticality': 'error',
            'check': {'function': 'is_not_null', 'arguments': {'column': 'name'}},
            'filter': None,
            'run_config_name': 'default',
            'user_metadata': None,
        },
    ]

    with testing.postgresql.Postgresql() as postgresql:
        engine = create_engine(postgresql.url())
        table.metadata.create_all(engine)

        with engine.begin() as conn:
            conn.execute(insert(table), expected_checks)

        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(instance_name="test", schema=schema_name)
        result = handler.load(config)

        assert result == expected_checks, "Loaded checks do not match expected checks"
        assert len(result) == 2, "Expected exactly 2 checks"
        assert all('name' in check for check in result), "All checks should have 'name' field"
        assert all('criticality' in check for check in result), "All checks should have 'criticality' field"
        assert all('check' in check for check in result), "All checks should have 'check' field"
        assert result[0]['name'] == 'id_is_null', "First check name should be 'id_is_null'"
        assert result[1]['name'] == 'name_is_null', "Second check name should be 'name_is_null'"
