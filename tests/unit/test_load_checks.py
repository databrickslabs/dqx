from unittest.mock import create_autospec

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.checks_storage import (
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandler,
    VolumeFileChecksStorageHandler,
)
from databricks.labs.dqx.config import FileChecksStorageConfig, VolumeFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.errors import InvalidCheckError, CheckDownloadError, InvalidConfigError
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.files import DownloadResponse


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
        handler.load(VolumeFileChecksStorageConfig(location="/Volumes/catalog/schema/volume/test_path.yml"))


def test_file_download_contents_read_none():
    # Simulate file_download.contents.read() returning None
    ws = create_autospec(WorkspaceClient)
    handler = VolumeFileChecksStorageHandler(ws)

    mock_file_download = create_autospec(DownloadResponse, instance=True)
    mock_file_download.contents.read.return_value = None
    ws.files.download.return_value = mock_file_download

    with pytest.raises(NotFound, match="No contents at Unity Catalog volume path"):
        handler.load(VolumeFileChecksStorageConfig(location="/Volumes/catalog/schema/volume/test_path.yml"))


def test_load_checks_from_local_file_with_variables(tmp_path):
    content = """- criticality: "{{ crit }}"
  check:
    function: is_not_null
    arguments:
      column: "{{ col }}"
"""
    file_path = tmp_path / "checks.yml"
    file_path.write_text(content, encoding="utf-8")

    checks = DQEngineCore.load_checks_from_local_file(str(file_path), variables={"crit": "error", "col": "id"})

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]


def test_load_checks_from_local_file_variables_none(tmp_path):
    content = """- criticality: error
  check:
    function: is_not_null
    arguments:
      column: id
"""
    file_path = tmp_path / "checks.yml"
    file_path.write_text(content, encoding="utf-8")

    checks = DQEngineCore.load_checks_from_local_file(str(file_path), variables=None)

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]


def test_load_checks_from_local_file_variables_empty(tmp_path):
    content = """- criticality: error
  check:
    function: is_not_null
    arguments:
      column: id
"""
    file_path = tmp_path / "checks.yml"
    file_path.write_text(content, encoding="utf-8")

    checks = DQEngineCore.load_checks_from_local_file(str(file_path), variables={})

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]


def test_load_checks_with_variables():
    ws = create_autospec(WorkspaceClient)
    mock_spark = create_autospec(SparkSession)

    raw_checks = [
        {"criticality": "{{ crit }}", "check": {"function": "is_not_null", "arguments": {"column": "{{ col }}"}}}
    ]

    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_factory.create.return_value = mock_handler
    mock_handler.load.return_value = raw_checks

    engine = DQEngine(ws, spark=mock_spark, checks_handler_factory=mock_factory)
    config = FileChecksStorageConfig(location="checks.yml")

    checks = engine.load_checks(config, variables={"crit": "error", "col": "id"})

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}},
    ]


def test_load_checks_variables_none():
    ws = create_autospec(WorkspaceClient)
    mock_spark = create_autospec(SparkSession)

    raw_checks = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]

    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_factory.create.return_value = mock_handler
    mock_handler.load.return_value = raw_checks

    engine = DQEngine(ws, spark=mock_spark, checks_handler_factory=mock_factory)
    config = FileChecksStorageConfig(location="checks.yml")

    checks = engine.load_checks(config, variables=None)

    assert checks == raw_checks
