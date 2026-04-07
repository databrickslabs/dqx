import logging
from unittest.mock import create_autospec

import pytest
from pyspark.sql import SparkSession

from databricks.labs.dqx.checks_storage import (
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandler,
    VolumeFileChecksStorageHandler,
)
from databricks.labs.dqx.config import FileChecksStorageConfig, VolumeFileChecksStorageConfig, ExtraParams
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


def test_load_checks_from_local_file_unresolved_placeholder(tmp_path, caplog):
    content = """- criticality: error
  check:
    function: is_not_null
    arguments:
      column: "{{ col }}"
"""
    file_path = tmp_path / "checks.yml"
    file_path.write_text(content, encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        checks = DQEngineCore.load_checks_from_local_file(str(file_path), variables={"other": "value"})

    assert checks[0]["check"]["arguments"]["column"] == "{{ col }}"
    assert any("Unresolved placeholder" in msg for msg in caplog.messages)


def test_load_checks_with_engine_default_variables():
    ws = create_autospec(WorkspaceClient)
    mock_spark = create_autospec(SparkSession)

    raw_checks = [
        {"criticality": "{{ crit }}", "check": {"function": "is_not_null", "arguments": {"column": "{{ col }}"}}}
    ]

    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_factory.create.return_value = mock_handler
    mock_handler.load.return_value = raw_checks

    extra_params = ExtraParams(variables={"crit": "error", "col": "default_col"})
    engine = DQEngine(ws, spark=mock_spark, checks_handler_factory=mock_factory, extra_params=extra_params)
    config = FileChecksStorageConfig(location="checks.yml")

    checks = engine.load_checks(config)

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "default_col"}}},
    ]


def test_load_checks_per_call_overrides_engine_defaults():
    ws = create_autospec(WorkspaceClient)
    mock_spark = create_autospec(SparkSession)

    raw_checks = [
        {"criticality": "{{ crit }}", "check": {"function": "is_not_null", "arguments": {"column": "{{ col }}"}}}
    ]

    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_factory.create.return_value = mock_handler
    mock_handler.load.return_value = raw_checks

    extra_params = ExtraParams(variables={"crit": "warn", "col": "default_col"})
    engine = DQEngine(ws, spark=mock_spark, checks_handler_factory=mock_factory, extra_params=extra_params)
    config = FileChecksStorageConfig(location="checks.yml")

    checks = engine.load_checks(config, variables={"crit": "error"})

    assert checks == [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "default_col"}}},
    ]


def test_extra_params_variables_substitution_and_overrides(tmp_path):
    ws = create_autospec(WorkspaceClient)
    mock_spark = create_autospec(SparkSession)

    checks_yaml = """
        - criticality: error
          name: "id_check"
          check:
            function: is_not_null
            arguments:
              column: "{{ target_col }}"
          user_metadata:
            env: "{{ environment }}"
            rule_id: "{{ nested_var }}"
        """
    checks_file = tmp_path / "checks_extra.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")

    raw_checks = DQEngineCore.load_checks_from_local_file(str(checks_file))
    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_factory.create.return_value = mock_handler
    mock_handler.load.return_value = raw_checks

    extra_params = ExtraParams(variables={"target_col": "id", "environment": "dev", "nested_var": "old"})
    engine = DQEngine(ws, spark=mock_spark, checks_handler_factory=mock_factory, extra_params=extra_params)
    config = FileChecksStorageConfig(location=str(checks_file))

    checks = engine.load_checks(config, variables={"environment": "prod", "nested_var": "new"})

    assert checks[0]["check"]["arguments"]["column"] == "id"
    assert checks[0]["user_metadata"]["env"] == "prod"
    assert checks[0]["user_metadata"]["rule_id"] == "new"


def test_load_checks_by_metadata_and_split_with_variables(tmp_path):

    checks_yaml = """
        - criticality: error
          name: "{{ col }}_null_check"
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: "{{ col }}"
        - criticality: warn
          check:
            function: sql_expression
            arguments:
              expression: "{{ expr_col }} > {{ threshold }}"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(
        str(checks_file), variables={"col": "b", "expr_col": "a", "threshold": 1}
    )

    assert checks == [
        {
            "criticality": "error",
            "name": "b_null_check",
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "b"},
            },
        },
        {
            "criticality": "warn",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a > 1"},
            },
        },
    ]


def test_load_checks_by_metadata_with_variables_name_and_filter(tmp_path):

    checks_yaml = """
        - criticality: error
          name: "{{ col }}_greater_than_{{ threshold }}"
          check:
            function: sql_expression
            arguments:
              expression: "{{ col }} > {{ threshold }}"
          filter: "{{ filter_col }} IS NOT NULL"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(
        str(checks_file), variables={"col": "a", "threshold": 1, "filter_col": "a"}
    )

    assert checks == [
        {
            "criticality": "error",
            "name": "a_greater_than_1",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a > 1"},
            },
            "filter": "a IS NOT NULL",
        }
    ]
