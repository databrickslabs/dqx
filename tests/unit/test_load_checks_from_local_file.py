import pytest
from databricks.labs.dqx.engine import DQEngine


def test_load_checks_from_local_file_json(make_local_check_file_as_json, expected_checks):
    file = make_local_check_file_as_json
    checks = DQEngine.load_checks_from_local_file(file)
    assert checks == expected_checks, "The loaded checks do not match the expected checks."


def test_load_checks_from_local_file_yaml(make_local_check_file_as_yaml, expected_checks):
    file = make_local_check_file_as_yaml
    checks = DQEngine.load_checks_from_local_file(file)
    assert checks == expected_checks, "The loaded checks do not match the expected checks."


def test_load_invalid_checks_from_local_file_json(make_invalid_local_check_file_as_json, expected_checks):
    file = make_invalid_local_check_file_as_json
    with pytest.raises(ValueError, match=f"Invalid or no checks in file: {file}"):
        DQEngine.load_checks_from_local_file(file)


def test_load_invalid_checks_from_local_file_yaml(make_invalid_local_check_file_as_yaml, expected_checks):
    file = make_invalid_local_check_file_as_yaml
    with pytest.raises(ValueError, match=f"Invalid or no checks in file: {file}"):
        DQEngine.load_checks_from_local_file(file)


def test_load_checks_from_local_file_when_filename_is_empty():
    with pytest.raises(ValueError, match="filepath must be provided"):
        DQEngine.load_checks_from_local_file("")


def test_load_checks_from_local_file_when_filename_is_missing():
    filename = "missing.yaml"
    with pytest.raises(FileNotFoundError, match=f"Checks file {filename} missing"):
        DQEngine.load_checks_from_local_file(filename)
