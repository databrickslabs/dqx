import pytest
from pyspark.sql.functions import col
from databricks.labs.dqx.engine import DQEngine


def dummy_func(col_name):
    return col(col_name)


def test_valid_checks():
    checks = [
        {"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": ["col1", "col2"]}}}
    ]
    glbs = {"dummy_func": dummy_func}
    DQEngine.validate_checks(checks, glbs)


def test_valid_multiple_checks():
    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "a"}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
        {
            "name": "col_a_value_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_name": "a", "allowed": [1, 3, 4]}},
        },
    ]
    DQEngine.validate_checks(checks)


def test_invalid_multiple_checks():
    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "criticality": "test",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
        {
            "name": "col_a_value_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_name": "a", "allowed": 2}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
    ]
    with pytest.raises(ValueError) as error:
        DQEngine.validate_checks(checks)
    error_message = str(error.value)
    assert "No arguments provided for function 'is_not_null_and_not_empty' in the 'arguments' block" in error_message
    assert "Invalid value for 'criticality' field" in error_message
    assert (
        "Argument 'allowed' should be of type 'list' for function 'value_is_in_list' in the 'arguments' block"
        in error_message
    )


def test_invalid_criticality():
    checks = [
        {"criticality": "invalid", "check": {"function": "dummy_func", "arguments": {"col_names": ["col1", "col2"]}}}
    ]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(ValueError, match="Invalid value for 'criticality' field"):
        DQEngine.validate_checks(checks, glbs)


def test_missing_check_key():
    checks = [{"criticality": "warn"}]
    with pytest.raises(ValueError, match="'check' field is missing"):
        DQEngine.validate_checks(checks)


def test_check_not_dict():
    checks = [{"criticality": "warn", "check": "not_a_dict"}]
    with pytest.raises(ValueError, match="'check' field should be a dictionary"):
        DQEngine.validate_checks(checks)


def test_missing_function_key():
    checks = [{"criticality": "warn", "check": {"arguments": {"col_names": ["col1", "col2"]}}}]
    with pytest.raises(ValueError, match="'function' field is missing in the 'check' block"):
        DQEngine.validate_checks(checks)


def test_undefined_function():
    checks = [
        {"criticality": "warn", "check": {"function": "undefined_func", "arguments": {"col_names": ["col1", "col2"]}}}
    ]
    with pytest.raises(ValueError, match="function 'undefined_func' is not defined"):
        DQEngine.validate_checks(checks)


def test_missing_arguments_key():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func"}}]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(ValueError, match="No arguments provided for function 'dummy_func' in the 'arguments' block"):
        DQEngine.validate_checks(checks, glbs)


def test_arguments_not_dict():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": "not_a_dict"}}]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(ValueError, match="'arguments' should be a dictionary in the 'check' block"):
        DQEngine.validate_checks(checks, glbs)


def test_col_names_not_list():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": "not_a_list"}}}]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(ValueError, match="'col_names' should be a list in the 'arguments' block"):
        DQEngine.validate_checks(checks, glbs)


def test_col_names_empty_list():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": []}}}]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(ValueError, match="'col_names' should not be empty in the 'arguments' block"):
        DQEngine.validate_checks(checks, glbs)


def test_unexpected_argument():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"unexpected_arg": "value"}},
        }
    ]
    with pytest.raises(
        ValueError,
        match="Unexpected argument 'unexpected_arg' for function 'is_not_null_and_not_empty' in the 'arguments' block",
    ):
        DQEngine.validate_checks(checks)


def test_argument_type_mismatch():
    def dummy_func(arg1: int):
        return col("test").isin(arg1)

    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"arg1": "not_an_int"}}}]
    glbs = {"dummy_func": dummy_func}
    with pytest.raises(
        ValueError, match="Argument 'arg1' should be of type 'int' for function 'dummy_func' in the 'arguments' block"
    ):
        DQEngine.validate_checks(checks, glbs)


def test_col_names_argument_type_list():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_names": ["a", "b"], "allowed": [1, 3, 4]}},
        }
    ]
    DQEngine.validate_checks(checks)


def test_col_functions_argument_mismtach_type():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_name": "a", "allowed": 2}},
        }
    ]
    with pytest.raises(
        ValueError,
        match="Argument 'allowed' should be of type 'list' for function 'value_is_in_list' in the 'arguments' block",
    ):
        DQEngine.validate_checks(checks)


def test_col_names_function_mismtach():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_older_than_col2_for_n_days", "arguments": {"col_names": ["a", "b"], "days": 2}},
        }
    ]
    with pytest.raises(
        ValueError,
        match="Unexpected argument 'col_name' for function 'is_older_than_col2_for_n_days' in the 'arguments' block",
    ):
        DQEngine.validate_checks(checks)


def test_col_position_arguments_function():
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_older_than_col2_for_n_days",
                "arguments": {"col_name2": "a", "col_name1": "b", "days": 1},
            },
        }
    ]
    DQEngine.validate_checks(checks)


def test_unsupported_check_type():
    checks = ["unsupported_type"]
    with pytest.raises(TypeError, match="Unsupported check type"):
        DQEngine.validate_checks(checks)
