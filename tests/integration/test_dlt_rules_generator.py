import pytest
from integration.test_rules_generator import test_rules
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.profiler.profiler import DQProfile


test_empty_rules: list[DQProfile] = []


def test_generate_dlt_sql_expect(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules)
    expected = [
        "CONSTRAINT vendor_id_is_not_null EXPECT (vendor_id is not null)",
        "CONSTRAINT vendor_id_is_in EXPECT (vendor_id in ('1', '4', '2'))",
        "CONSTRAINT vendor_id_is_not_null_or_empty EXPECT (vendor_id is not null and trim(vendor_id) <> '')",
        "CONSTRAINT rate_code_id_min_max EXPECT (rate_code_id >= 1 and rate_code_id <= 265)",
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-01')",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-01T00:00:00.000000')",
    ]
    assert expectations == expected


def test_generate_dlt_sql_drop(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, action="drop")
    expected = [
        "CONSTRAINT vendor_id_is_not_null EXPECT (vendor_id is not null) ON VIOLATION DROP ROW",
        "CONSTRAINT vendor_id_is_in EXPECT (vendor_id in ('1', '4', '2')) ON VIOLATION DROP ROW",
        "CONSTRAINT vendor_id_is_not_null_or_empty EXPECT (vendor_id is not null and trim(vendor_id) <> '') ON VIOLATION DROP ROW",
        "CONSTRAINT rate_code_id_min_max EXPECT (rate_code_id >= 1 and rate_code_id <= 265) ON VIOLATION DROP ROW",
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-01') ON VIOLATION DROP ROW",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-01T00:00:00.000000') ON VIOLATION DROP ROW",
    ]
    assert expectations == expected


def test_generate_dlt_sql_fail(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, action="fail")
    expected = [
        "CONSTRAINT vendor_id_is_not_null EXPECT (vendor_id is not null) ON VIOLATION FAIL UPDATE",
        "CONSTRAINT vendor_id_is_in EXPECT (vendor_id in ('1', '4', '2')) ON VIOLATION FAIL UPDATE",
        "CONSTRAINT vendor_id_is_not_null_or_empty EXPECT (vendor_id is not null and trim(vendor_id) <> '') ON VIOLATION FAIL UPDATE",
        "CONSTRAINT rate_code_id_min_max EXPECT (rate_code_id >= 1 and rate_code_id <= 265) ON VIOLATION FAIL UPDATE",
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-01') ON VIOLATION FAIL UPDATE",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-01T00:00:00.000000') ON VIOLATION FAIL UPDATE",
    ]
    assert expectations == expected


def test_generate_dlt_python_expect(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python")
    expected = """@dlt.expect_all(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-01'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-01T00:00:00.000000'"}
)"""
    assert expectations == expected


def test_generate_dlt_python_drop(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python", action="drop")
    expected = """@dlt.expect_all_or_drop(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-01'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-01T00:00:00.000000'"}
)"""
    assert expectations == expected


def test_generate_dlt_python_fail(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python", action="fail")
    expected = """@dlt.expect_all_or_fail(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-01'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-01T00:00:00.000000'"}
)"""
    assert expectations == expected


def test_generate_dlt_python_empty_rule(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_empty_rules, language="Python")

    assert expectations == ""


def test_generate_dlt_rules_unsupported_language(ws):
    generator = DQDltGenerator(ws)
    rules = []  # or some valid list of DQProfile instances
    with pytest.raises(
        ValueError, match="Unsupported language 'unsupported_language'. Only 'SQL' and 'Python' are supported."
    ):
        generator.generate_dlt_rules(rules, language="unsupported_language")


def test_generate_dlt_rules_empty_expression(ws):
    generator = DQDltGenerator(ws)
    rules = [DQProfile(name="is_not_null", column="test_column", parameters={})]
    expectations = generator.generate_dlt_rules(rules, language="Python")
    assert "test_column_is_not_null" in expectations


def test_generate_dlt_rules_empty(ws):
    generator = DQDltGenerator(ws)
    rules = None
    expectations = generator.generate_dlt_rules(rules, language="SQL")
    assert expectations == []


def test_generate_dlt_rules_no_expectations(ws):
    generator = DQDltGenerator(ws)
    rules = []  # or some valid list of DQProfile instances
    expectations = generator.generate_dlt_rules(rules, language="Python")
    assert expectations == ""
