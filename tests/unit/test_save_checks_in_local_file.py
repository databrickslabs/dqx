import os
import json

import pytest
import yaml

from databricks.labs.dqx.engine import DQEngineCore


TEST_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "for_each_column": ["col1", "col2"],
            "arguments": {},
            "user_metadata": {"rule_type": "completeness"},
        },
    },
]


def test_save_checks_to_local_file_as_yaml(make_local_check_file_as_yaml):
    file = make_local_check_file_as_yaml
    DQEngineCore.save_checks_in_local_file(TEST_CHECKS, file)
    _validate_file(file, "yaml")

    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


def test_save_checks_to_local_file_as_json(make_local_check_file_as_json):
    file = make_local_check_file_as_json
    DQEngineCore.save_checks_in_local_file(TEST_CHECKS, file)
    _validate_file(file, "json")

    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


@pytest.mark.parametrize(
    "filename, expected_exception, expected_message",
    [
        ("", ValueError, "The file path \\('location' field\\) must not be empty or None"),
        (None, ValueError, "The file path \\('location' field\\) must not be empty or None"),
    ],
)
def test_load_checks_from_local_file_exceptions(filename, expected_exception, expected_message):
    with pytest.raises(expected_exception, match=expected_message):
        DQEngineCore.save_checks_in_local_file(TEST_CHECKS, filename)


def _validate_file(file_path: str, file_format: str = "yaml") -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, "r", encoding="utf-8") as file:
        if file_format == "json":
            json.load(file)
        yaml.safe_load(file)
