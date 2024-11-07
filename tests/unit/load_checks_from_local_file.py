from pathlib import Path
import pytest
from databricks.labs.dqx.engine import load_checks_from_local_file


EXPECTED_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"col_names": ["col1", "col2"]}},
    },
    {
        "name": "col_col3_is_null_or_empty",
        "criticality": "error",
        "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "col3"}},
    },
    {
        "criticality": "warn",
        "check": {"function": "value_is_in_list", "arguments": {"col_name": "col4", "allowed": [1, 2]}},
    },
]
BASE_PATH = str(Path(__file__).resolve().parent.parent)


def test_load_check_from_local_file_json():
    file = BASE_PATH + "/test_data/checks.json"
    checks = load_checks_from_local_file(file)
    assert checks == EXPECTED_CHECKS, "The loaded checks do not match the expected checks."


def test_load_check_from_local_file_yml():
    file = BASE_PATH + "/test_data/checks.yml"
    checks = load_checks_from_local_file(file)

    assert checks == EXPECTED_CHECKS, "The loaded checks do not match the expected checks."


def test_load_check_from_local_file_when_filename_is_empty():
    with pytest.raises(ValueError, match="filename must be provided"):
        load_checks_from_local_file("")


def test_load_check_from_local_file_when_filename_is_missing():
    filename = "missing.yaml"
    with pytest.raises(FileNotFoundError, match=f"Checks file {filename} missing"):
        load_checks_from_local_file(filename)
