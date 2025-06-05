from pyspark.sql.functions import col
from databricks.labs.dqx.engine import DQEngine


def dummy_func(column):
    return col(column)


def dummy_func_with_optional_args(column, arg1: bool | str):
    assert arg1
    return col(column)


def test_valid_checks():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "criticality": "warn",
            "check": {"function": "dummy_func", "for_each_column": ["col1", "col2"], "arguments": {}},
        },
        {
            "criticality": "warn",
            "check": {
                "function": "dummy_func_with_optional_args",
                "for_each_column": ["col1", "col2"],
                "arguments": {},
            },
        },
    ]
    custom_check_functions = {"dummy_func": dummy_func, "dummy_func_with_optional_args": dummy_func_with_optional_args}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert not status.has_errors
    assert "No errors found" in str(status)


def test_valid_multiple_checks():
    checks = [
        {
            "name": "col_a_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "col_a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": [1, 3, 4]}},
        },
        {
            "name": "col_a_is_null_or_empty_array",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty_array", "arguments": {"column": "a"}},
        },
        {
            "criticality": "warn",
            "check": {
                "function": "is_unique",
                "for_each_column": [["col1", "col2"], ["col3"]],
                "arguments": {"nulls_distinct": True},
            },
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
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "col_a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": 2}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "check_invalid_field": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "criticality": "warn",
            "check": {
                "function": "is_unique",
                "for_each_column": ["col1", "col2"],
                "arguments": {"nulls_distinct": True},
            },
        },
    ]

    status = DQEngine.validate_checks(checks)
    assert status.has_errors

    expected_errors = [
        "No arguments provided for function 'is_not_null_and_not_empty' in the 'arguments' block",
        "Invalid 'criticality' value",
        "Argument 'allowed' should be of type 'list' for function 'is_in_list' in the 'arguments' block",
        "'check' field is missing",
        "Argument 'columns' should be of type 'list' for function 'is_unique' in the 'arguments' block",
        "Argument 'columns' should be of type 'list' for function 'is_unique' in the 'arguments' block",
    ]
    assert len(status.errors) == len(expected_errors)
    for e in expected_errors:
        assert any(e in error for error in status.errors)


def test_invalid_criticality():
    checks = [
        {
            "criticality": "invalid",
            "check": {"function": "dummy_func", "for_each_column": ["col1", "col2"], "arguments": {}},
        }
    ]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "Invalid 'criticality' value" in status.to_string()


def test_missing_check_key():
    checks = [{"criticality": "warn"}]
    status = DQEngine.validate_checks(checks)
    assert "'check' field is missing" in str(status)


def test_check_not_dict():
    checks = [{"criticality": "warn", "check": "not_a_dict"}]
    status = DQEngine.validate_checks(checks)
    assert "'check' field should be a dictionary" in str(status)


def test_missing_function_key():
    checks = [{"criticality": "warn", "check": {"for_each_column": ["col1", "col2"], "arguments": {}}}]
    status = DQEngine.validate_checks(checks)
    assert "'function' field is missing in the 'check' block" in str(status)


def test_undefined_function():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "undefined_func", "for_each_column": ["col1", "col2"], "arguments": {}},
        }
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


def test_for_each_column_not_list():
    checks = [
        {"criticality": "warn", "check": {"function": "dummy_func", "for_each_column": "not_a_list", "arguments": {}}}
    ]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "'for_each_column' should be a list in the 'check' block" in str(status)


def test_for_each_column_empty_list():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "for_each_column": [], "arguments": {}}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "'for_each_column' should not be empty in the 'check' block" in str(status)


def test_define_column_and_columns_for_a_check():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null", "arguments": {"column": "col1", "columns": ["col2"]}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'columns' for function 'is_not_null' in the 'arguments' block" in str(status)


def test_unexpected_argument_for_check_taking_column_as_arg():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"columns": ["col1"]}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'columns' for function 'is_not_null_and_not_empty' in the 'arguments' block" in str(
        status
    )


def test_unexpected_argument_for_check_taking_columns_as_arg():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_unique", "arguments": {"column": "col1"}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'column' for function 'is_unique' in the 'arguments' block" in str(status)


def test_unexpected_argument_for_check_not_taking_column_as_arg():
    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a", "msg": "a not found", "column": "a"},
            },
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'column' for function 'sql_expression' in the 'arguments' block" in str(status)


def test_unexpected_argument_for_check_not_taking_columns_as_arg():
    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a", "msg": "a not found", "columns": "a"},
            },
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'columns' for function 'sql_expression' in the 'arguments' block" in str(status)


def test_argument_type_mismatch():
    def dummy_func(arg1: int):
        return col("test").isin(arg1)

    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"arg1": "not_an_int"}}}]
    custom_check_functions = {"dummy_func": dummy_func}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert "Argument 'arg1' should be of type 'int' for function 'dummy_func' in the 'arguments' block" in str(status)


def test_argument_type_mismatch_optional_args():
    checks = [{"criticality": "warn", "check": {"function": "dummy_func", "arguments": {"arg1": 1}}}]
    custom_check_functions = {"dummy_func": dummy_func_with_optional_args}
    status = DQEngine.validate_checks(checks, custom_check_functions)
    assert (
        "Argument 'arg1' should be of type 'bool | str' for function "
        "'dummy_func_with_optional_args' in the 'arguments' block" in str(status)
    )


def test_for_each_column_argument_type_list():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "for_each_column": ["a", "b"], "arguments": {"allowed": [1, 3, 4]}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


def test_check_funcs_argument_mismatch_type():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": 2}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Argument 'allowed' should be of type 'list' for function 'is_in_list' in the 'arguments' block" in str(
        status
    )


def test_for_each_column_function_mismtach():
    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "is_older_than_col2_for_n_days",
                "for_each_column": ["a", "b"],
                "arguments": {"days": 2},
            },
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Unexpected argument 'column' for function 'is_older_than_col2_for_n_days' in the 'arguments' block" in str(
        status
    )


def test_col_position_arguments_function():
    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_older_than_col2_for_n_days",
                "arguments": {"column2": "a", "column1": "b", "days": 1},
            },
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


def test_unsupported_check_type():
    checks = ["unsupported_type"]
    status = DQEngine.validate_checks(checks)
    assert "Unsupported check type" in str(status)


def test_argument_type_list_mismatch():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_unique", "arguments": {"columns": "a"}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert "Argument 'columns' should be of type 'list' for function 'is_unique' in the 'arguments' block" in str(
        status
    )


def test_argument_type_list_mismatch_args():
    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_unique", "arguments": {"columns": ["a", 1]}},
        }
    ]
    status = DQEngine.validate_checks(checks)
    assert (
        "Item 1 in argument 'columns' should be of type 'str | pyspark.sql.column.Column' "
        "for function 'is_unique' in the 'arguments' block" in str(status)
    )
