from pyspark.sql.functions import col
from databricks.labs.dqx.engine import DQEngine


def dummy_func(col_name):
    return col(col_name)


def test_valid_checks():
    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null", "arguments": {"col_names": ["col1", "col2"]}}},
        {"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": ["col1", "col2"]}}},
    ]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert not status.has_errors
    assert "No errors found" in str(status)


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
        {
            "name": "col_a_is_null_or_empty_array",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty_array", "arguments": {"col_name": "a"}},
        },
    ]
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


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
        {
            "name": "col_b_is_null_or_empty",
            "check_invalid_field": {"function": "is_not_null_and_not_empty", "arguments": {"col_name": "b"}},
        },
    ]

    status = DQEngine.validate_checks(checks)
    assert status.has_errors

    expected_errors = [
        "No arguments provided for function 'is_not_null_and_not_empty' in the 'arguments' block",
        "Invalid value for 'criticality' field",
        "Argument 'allowed' should be of type 'list' for function 'value_is_in_list' in the 'arguments' block",
        "'check' field is missing",
    ]
    assert len(status.errors) == len(expected_errors)
    for e in expected_errors:
        assert any(e in error for error in status.errors)


def test_invalid_criticality():
    checks = [
        {"criticality": "invalid", "check": {"function": "dummy_func", "arguments": {"col_names": ["col1", "col2"]}}}
    ]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "Invalid value for 'criticality' field" in status.to_string()


def test_missing_check_key():
    checks = [{"criticality": "warn"}]
    status = DQEngine.validate_checks(checks)
    assert "'check' field is missing" in str(status)


def test_check_not_dict():
    checks = [{"criticality": "warn", "check": "not_a_dict"}]
    status = DQEngine.validate_checks(checks)
    assert "'check' field should be a dictionary" in str(status)


def test_missing_function_key():
    checks = [{"criticality": "warn", "check": {"arguments": {"col_names": ["col1", "col2"]}}}]
    status = DQEngine.validate_checks(checks)
    assert "'function' field is missing in the 'check' block" in str(status)


def test_undefined_function():
    checks = [
        {"criticality": "warn", "check": {"function": "undefined_func", "arguments": {"col_names": ["col1", "col2"]}}}
    ]
    status = DQEngine.validate_checks(checks)
    assert "function 'undefined_func' is not defined" in str(status)


def test_missing_arguments_key():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func"}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "No arguments provided for function 'dummy_func' in the 'arguments' block" in str(status)


def test_arguments_not_dict():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": "not_a_dict"}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "'arguments' should be a dictionary in the 'check' block" in str(status)


def test_col_names_not_list():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": "not_a_list"}}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "'col_names' should be a list in the 'arguments' block" in str(status)


def test_col_names_empty_list():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"col_names": []}}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "'col_names' should not be empty in the 'arguments' block" in str(status)


def test_unexpected_argument():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"unexpected_arg": "value"}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert (
        "Unexpected argument 'unexpected_arg' for function 'is_not_null_and_not_empty' in the 'arguments' block"
        in str(status)
    )


def test_argument_type_mismatch():
    def dummy_func(arg1: int):
        return col("test").isin(arg1)

    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"arg1": "not_an_int"}}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "Argument 'arg1' should be of type 'int' for function 'dummy_func' in the 'arguments' block" in str(status)


def test_col_names_argument_type_list():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_names": ["a", "b"], "allowed": [1, 3, 4]}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


def test_col_functions_argument_mismtach_type():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "value_is_in_list", "arguments": {"col_name": "a", "allowed": 2}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert (
        "Argument 'allowed' should be of type 'list' for function 'value_is_in_list' in the 'arguments' block"
        in str(status)
    )


def test_col_names_function_mismtach():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_older_than_col2_for_n_days", "arguments": {"col_names": ["a", "b"], "days": 2}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert (
        "Unexpected argument 'col_name' for function 'is_older_than_col2_for_n_days' in the 'arguments' block"
        in str(status)
    )


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
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


def test_unsupported_check_type():
    checks = ["unsupported_type"]
    status = DQEngine.validate_checks(checks)
    assert "Unsupported check type" in str(status)
