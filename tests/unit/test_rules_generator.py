import datetime

from databricks.labs.dqx.profiler.generator import generate_dq_rules
from databricks.labs.dqx.profiler.profiler import DQProfile

test_rules = [
    DQProfile(
        name="is_not_null", column="vendor_id", description="Column vendor_id has 0.3% of null values (allowed 1.0%)"
    ),
    DQProfile(name="is_in", column="vendor_id", parameters={"in": ["1", "4", "2"]}),
    DQProfile(name="is_not_null_or_empty", column="vendor_id", parameters={"trim_strings": True}),
    DQProfile(
        name="min_max",
        column="rate_code_id",
        parameters={"min": 1, "max": 265},
        description="Real min/max values were used",
    ),
    DQProfile(
        name="min_max",
        column="product_launch_date",
        parameters={"min": datetime.date(2020, 1, 1), "max": None},
        description="Real min/max values were used",
    ),
    DQProfile(
        name="min_max",
        column="product_expiry_ts",
        parameters={"min": None, "max": datetime.datetime(2020, 1, 1)},
        description="Real min/max values were used",
    ),
]


def test_generate_dq_rules():
    expectations = generate_dq_rules(test_rules)
    expected = [
        {
            "check": {"function": "col_is_not_null", "arguments": {"col_name": "vendor_id"}},
            "name": "vendor_id_is_null",
            "criticality": "error",
        },
        {
            "check": {
                "function": "col_value_is_in_list",
                "arguments": {"col_name": "vendor_id", "allowed": ["1", "4", "2"]},
            },
            "name": "vendor_id_other_value",
            "criticality": "error",
        },
        {
            "check": {
                "function": "col_is_not_null_and_not_empty",
                "arguments": {"col_name": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "error",
        },
        {
            "check": {
                "function": "col_is_in_range",
                "arguments": {"col_name": "rate_code_id", "min_limit": 1, "max_limit": 265},
            },
            "name": "rate_code_id_isnt_in_range",
            "criticality": "error",
        },
        {
            "check": {
                "function": "col_not_less_than",
                "arguments": {"col_name": "product_launch_date", "val": "2020-01-01"},
            },
            "name": "product_launch_date_not_less_than",
            "criticality": "error",
        },
        {
            "check": {
                "function": "col_not_greater_than",
                "arguments": {"col_name": "product_expiry_ts", "val": "2020-01-01T00:00:00.000000"},
            },
            "name": "product_expiry_ts_not_greater_than",
            "criticality": "error",
        },
    ]
    assert expectations == expected


def test_generate_dq_rules_warn():
    expectations = generate_dq_rules(test_rules, level="warn")
    expected = [
        {
            "check": {"function": "col_is_not_null", "arguments": {"col_name": "vendor_id"}},
            "name": "vendor_id_is_null",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "col_value_is_in_list",
                "arguments": {"col_name": "vendor_id", "allowed": ["1", "4", "2"]},
            },
            "name": "vendor_id_other_value",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "col_is_not_null_and_not_empty",
                "arguments": {"col_name": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "col_is_in_range",
                "arguments": {"col_name": "rate_code_id", "min_limit": 1, "max_limit": 265},
            },
            "name": "rate_code_id_isnt_in_range",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "col_not_less_than",
                "arguments": {"col_name": "product_launch_date", "val": "2020-01-01"},
            },
            "name": "product_launch_date_not_less_than",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "col_not_greater_than",
                "arguments": {"col_name": "product_expiry_ts", "val": "2020-01-01T00:00:00.000000"},
            },
            "name": "product_expiry_ts_not_greater_than",
            "criticality": "warn",
        },
    ]
    assert expectations == expected
