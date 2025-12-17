import logging
import datetime
import json
from decimal import Decimal

from databricks.labs.dqx.profiler.generator import DQGenerator
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
        parameters={"min": None, "max": datetime.datetime(2021, 1, 1)},
        description="Real min/max values were used",
    ),
    DQProfile(name="is_random", column="vendor_id", parameters={"in": ["1", "4", "2"]}),
    DQProfile(
        name='min_max',
        column='d1',
        description='Real min/max values were used',
        parameters={'max': Decimal('333323.00'), 'min': Decimal('1.23')},
    ),
]


def test_generate_dq_rules(ws, spark):
    generator = DQGenerator(ws, spark)
    expectations = generator.generate_dq_rules(test_rules)
    expected = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "vendor_id"}},
            "name": "vendor_id_is_null",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "vendor_id", "allowed": ["1", "4", "2"]},
            },
            "name": "vendor_id_other_value",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "rate_code_id", "min_limit": 1, "max_limit": 265},
            },
            "name": "rate_code_id_isnt_in_range",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "product_launch_date", "limit": datetime.date(2020, 1, 1)},
            },
            "name": "product_launch_date_not_less_than",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "product_expiry_ts", "limit": datetime.datetime(2021, 1, 1)},
            },
            "name": "product_expiry_ts_not_greater_than",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "d1", "min_limit": Decimal('1.23'), "max_limit": Decimal('333323.00')},
            },
            "name": "d1_isnt_in_range",
            "criticality": "error",
        },
    ]
    assert expectations == expected, f"Actual expectations: {expectations}"


def test_generate_dq_rules_warn(ws, spark):
    generator = DQGenerator(ws, spark)
    expectations = generator.generate_dq_rules(test_rules, criticality="warn")
    expected = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "vendor_id"}},
            "name": "vendor_id_is_null",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_in_list",
                "arguments": {"column": "vendor_id", "allowed": ["1", "4", "2"]},
            },
            "name": "vendor_id_other_value",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "rate_code_id", "min_limit": 1, "max_limit": 265},
            },
            "name": "rate_code_id_isnt_in_range",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_not_less_than",
                "arguments": {"column": "product_launch_date", "limit": datetime.date(2020, 1, 1)},
            },
            "name": "product_launch_date_not_less_than",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "product_expiry_ts", "limit": datetime.datetime(2021, 1, 1)},
            },
            "name": "product_expiry_ts_not_greater_than",
            "criticality": "warn",
        },
        {
            "check": {
                "function": "is_in_range",
                "arguments": {"column": "d1", "min_limit": Decimal('1.23'), "max_limit": Decimal('333323.00')},
            },
            "name": "d1_isnt_in_range",
            "criticality": "warn",
        },
    ]
    assert expectations == expected, f"Actual expectations: {expectations}"


def test_generate_dq_rules_logging(ws, spark, caplog):
    # capture INFO from the generator module where the skip log is emitted
    caplog.set_level(logging.INFO, logger="databricks.labs.dqx.profiler.generator")

    generator = DQGenerator(ws, spark)
    # add an unknown rule to trigger the "skipping..." log
    unknown_rule = DQProfile(name="is_random", column="vendor_id")
    generator.generate_dq_rules(test_rules + [unknown_rule])

    assert "No rule 'is_random' for column 'vendor_id'. skipping..." in caplog.text


def test_generate_dq_no_rules(ws, spark):
    generator = DQGenerator(ws, spark)
    expectations = generator.generate_dq_rules(None, criticality="warn")
    assert not expectations


def test_generate_dq_rules_dataframe_filter(ws, spark):
    generator = DQGenerator(ws, spark)
    test_rules_filter = [
        DQProfile(
            name="is_not_null",
            column="machine_id",
            description=None,
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
        ),
        DQProfile(
            name="is_in",
            column="vendor_id",
            parameters={"in": ["1", "4", "2"]},
            filter="machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
        ),
        DQProfile(
            name="is_not_null",
            column="cost",
            description=None,
        ),
        DQProfile(
            name="is_not_null",
            column="next_scheduled_date",
            description=None,
        ),
        DQProfile(
            name="is_not_null",
            column="safety_check_passed",
            description=None,
        ),
        DQProfile(name="is_not_null_or_empty", column="vendor_id", parameters={"trim_strings": True}),
    ]
    expectations = generator.generate_dq_rules(test_rules_filter)

    expected = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "machine_id"}},
            "filter": "machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
            "name": "machine_id_is_null",
            "criticality": "error",
        },
        {
            "check": {"function": "is_in_list", "arguments": {"allowed": ["1", "4", "2"], "column": "vendor_id"}},
            "filter": "machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
            "criticality": "error",
            "name": "vendor_id_other_value",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "cost"}},
            "name": "cost_is_null",
            "criticality": "error",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "next_scheduled_date"}},
            "name": "next_scheduled_date_is_null",
            "criticality": "error",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "safety_check_passed"}},
            "name": "safety_check_passed_is_null",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "error",
        },
    ]
    assert expectations == expected


def test_generate_dq_rules_dataframe_filter_none(ws, spark):
    generator = DQGenerator(ws, spark)
    test_rules_no_filter = [
        DQProfile(
            name="is_not_null",
            column="machine_id",
            description=None,
            filter=None,
        ),
        DQProfile(
            name="is_in",
            column="vendor_id",
            parameters={"in": ["1", "4", "2"]},
            filter=None,
        ),
        DQProfile(
            name="is_not_null",
            column="next_scheduled_date",
            description=None,
            filter=None,
        ),
        DQProfile(name="is_not_null_or_empty", column="vendor_id", parameters={"trim_strings": True}, filter=None),
    ]
    expectations = generator.generate_dq_rules(test_rules_no_filter)

    expected = [
        {
            "check": {"function": "is_not_null", "arguments": {"column": "machine_id"}},
            "name": "machine_id_is_null",
            "criticality": "error",
        },
        {
            "check": {"function": "is_in_list", "arguments": {"allowed": ["1", "4", "2"], "column": "vendor_id"}},
            "criticality": "error",
            "name": "vendor_id_other_value",
        },
        {
            "check": {"function": "is_not_null", "arguments": {"column": "next_scheduled_date"}},
            "name": "next_scheduled_date_is_null",
            "criticality": "error",
        },
        {
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "vendor_id", "trim_strings": True},
            },
            "name": "vendor_id_is_null_or_empty",
            "criticality": "error",
        },
    ]
    assert expectations == expected


def test_generate_is_unique_dq_rule(ws, spark):
    generator = DQGenerator(ws, spark)
    test_is_unique_rules = [
        DQProfile(
            name='is_unique',
            column='col1,col2',
            description='LLM-detected primary key columns: col1, col2',
            parameters={"nulls_distinct": False, "confidence": "high"},
        ),
    ]
    checks = generator.generate_dq_rules(test_is_unique_rules, criticality="warn")

    expected_checks = [
        {
            "check": {"function": "is_unique", "arguments": {"columns": ["col1", "col2"], "nulls_distinct": False}},
            "name": "primary_key_col1_col2_validation",
            "criticality": "warn",
            "user_metadata": {"pk_detection_confidence": "high"},
        }
    ]
    assert checks == expected_checks


def test_generate_is_unique_dq_rule_default_criticality(ws, spark):
    generator = DQGenerator(ws, spark)
    test_is_unique_rules = [
        DQProfile(
            name='is_unique',
            column='col1',
            description='LLM-detected primary key columns: col1, col2',
            parameters={"nulls_distinct": True, "confidence": "low"},
        ),
    ]
    checks = generator.generate_dq_rules(test_is_unique_rules)

    expected_checks = [
        {
            "check": {"function": "is_unique", "arguments": {"columns": ["col1"], "nulls_distinct": True}},
            "name": "primary_key_col1_validation",
            "criticality": "error",
            "user_metadata": {"pk_detection_confidence": "low"},
        }
    ]
    assert checks == expected_checks
