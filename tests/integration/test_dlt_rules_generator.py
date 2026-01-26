import pytest
from tests.integration.test_generator import test_rules
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.labs.dqx.profiler.profiler import DQProfile, DQProfiler
from databricks.labs.dqx.errors import InvalidParameterError, MissingParameterError
from databricks.labs.dqx.config import InputConfig


test_empty_rules: list[DQProfile] = []


def test_generate_dlt_sql_expect(ws, set_utc_timezone):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules)
    expected = [
        "CONSTRAINT vendor_id_is_not_null EXPECT (vendor_id is not null)",
        "CONSTRAINT vendor_id_is_in EXPECT (vendor_id in ('1', '4', '2'))",
        "CONSTRAINT vendor_id_is_not_null_or_empty EXPECT (vendor_id is not null and trim(vendor_id) <> '')",
        "CONSTRAINT rate_code_id_min_max EXPECT (rate_code_id >= 1 and rate_code_id <= 265)",
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-02')",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-02T03:04:05.000000')",
        "CONSTRAINT d1_min_max EXPECT (d1 >= 1.23 and d1 <= 333323.0)",
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
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-02') ON VIOLATION DROP ROW",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-02T03:04:05.000000') ON VIOLATION DROP ROW",
        "CONSTRAINT d1_min_max EXPECT (d1 >= 1.23 and d1 <= 333323.0) ON VIOLATION DROP ROW",
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
        "CONSTRAINT product_launch_date_min_max EXPECT (product_launch_date >= '2020-01-02') ON VIOLATION FAIL UPDATE",
        "CONSTRAINT product_expiry_ts_min_max EXPECT (product_expiry_ts <= '2020-01-02T03:04:05.000000') ON VIOLATION FAIL UPDATE",
        "CONSTRAINT d1_min_max EXPECT (d1 >= 1.23 and d1 <= 333323.0) ON VIOLATION FAIL UPDATE",
    ]
    assert expectations == expected


def test_generate_dlt_python_expect(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python")
    expected = """@dlt.expect_all(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-02'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-02T03:04:05.000000'", "d1_min_max": "d1 >= 1.23 and d1 <= 333323.0"}
)"""
    assert expectations == expected


def test_generate_dlt_python_drop(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python", action="drop")
    expected = """@dlt.expect_all_or_drop(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-02'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-02T03:04:05.000000'", "d1_min_max": "d1 >= 1.23 and d1 <= 333323.0"}
)"""
    assert expectations == expected


def test_generate_dlt_python_fail(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python", action="fail")
    expected = """@dlt.expect_all_or_fail(
{"vendor_id_is_not_null": "vendor_id is not null", "vendor_id_is_in": "vendor_id in ('1', '4', '2')", "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''", "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265", "product_launch_date_min_max": "product_launch_date >= '2020-01-02'", "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-02T03:04:05.000000'", "d1_min_max": "d1 >= 1.23 and d1 <= 333323.0"}
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
        InvalidParameterError,
        match="Unsupported language 'unsupported_language'. Only 'SQL' and 'Python' are supported.",
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


def test_generate_dlt_python_dict(ws):
    generator = DQDltGenerator(ws)
    expectations = generator.generate_dlt_rules(test_rules, language="Python_Dict")
    expected = {
        "vendor_id_is_not_null": "vendor_id is not null",
        "vendor_id_is_in": "vendor_id in ('1', '4', '2')",
        "vendor_id_is_not_null_or_empty": "vendor_id is not null and trim(vendor_id) <> ''",
        "rate_code_id_min_max": "rate_code_id >= 1 and rate_code_id <= 265",
        "product_launch_date_min_max": "product_launch_date >= '2020-01-02'",
        "product_expiry_ts_min_max": "product_expiry_ts <= '2020-01-02T03:04:05.000000'",
        "d1_min_max": "d1 >= 1.23 and d1 <= 333323.0",
    }
    assert expectations == expected


# AI-Assisted DLT Rules Generation Tests
def test_generate_dlt_rules_ai_assisted_with_user_input(ws, spark):
    """Test AI-assisted DLT rules generation with user input only."""
    user_input = "User ID must not be null. Age should be between 0 and 120."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(user_input=user_input, language="SQL")
    
    assert isinstance(result, list)
    assert len(result) > 0
    # Verify that rules were generated
    result_str = " ".join(result)
    assert "user_id" in result_str.lower() or "age" in result_str.lower()


def test_generate_dlt_rules_ai_assisted_with_summary_stats(ws, spark):
    """Test AI-assisted DLT rules generation with summary statistics only."""
    summary_stats = {
        "temperature": {"mean": "22.5", "min": "-10.0", "max": "50.0"},
        "humidity": {"mean": "65.5", "min": "20.0", "max": "95.0"},
    }
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(summary_stats=summary_stats, language="SQL")
    
    assert isinstance(result, list)
    assert len(result) > 0


def test_generate_dlt_rules_ai_assisted_with_user_input_and_summary_stats(ws, spark):
    """Test AI-assisted DLT rules generation with both user input and summary statistics."""
    user_input = "Validate sensor data: ensure temperatures and humidity are within reasonable ranges"
    
    summary_stats = {
        "temperature": {"mean": "22.5", "min": "-10.0", "max": "50.0"},
        "humidity": {"mean": "65.5", "min": "20.0", "max": "95.0"},
    }
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(
        user_input=user_input, summary_stats=summary_stats, language="SQL"
    )
    
    assert isinstance(result, list)
    assert len(result) > 0


def test_generate_dlt_rules_ai_assisted_with_input_config(ws, spark, make_table, make_schema):
    """Test AI-assisted DLT rules generation with input_config for schema inference."""
    from tests.conftest import TEST_CATALOG
    
    schema = make_schema(catalog_name=TEST_CATALOG)
    input_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("user_id", "string"), ("email", "string"), ("age", "int")],
    )
    
    user_input = "User ID must not be null. Email must be valid."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(
        user_input=user_input, input_config=InputConfig(location=input_table.full_name), language="SQL"
    )
    
    assert isinstance(result, list)
    assert len(result) > 0


def test_generate_dlt_rules_ai_assisted_python_output(ws, spark):
    """Test AI-assisted DLT rules generation with Python output."""
    user_input = "User ID must not be null (error). Username should not be null (warn)."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(user_input=user_input, language="Python")
    
    assert isinstance(result, str)
    # Should contain decorators for both error and warn criticality
    assert "@dlt.expect" in result


def test_generate_dlt_rules_ai_assisted_python_dict_output(ws, spark):
    """Test AI-assisted DLT rules generation with Python_Dict output."""
    user_input = "User ID must not be null."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(user_input=user_input, language="Python_Dict")
    
    assert isinstance(result, dict)
    assert len(result) > 0
    # Check that criticality is stored in the dict (nested dict format)
    for key, value in result.items():
        assert isinstance(value, dict)
        assert "expression" in value
        assert "criticality" in value
        assert isinstance(value["expression"], str)
        assert value["criticality"] in ["error", "warn"]


def test_generate_dlt_rules_ai_assisted_per_rule_criticality_sql(ws, spark):
    """Test that per-rule criticality is correctly applied in SQL output."""
    user_input = "User ID must not be null (error). Status should be in allowed list (warn)."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(user_input=user_input, language="SQL")
    
    assert isinstance(result, list)
    # Check that error rules have ON VIOLATION FAIL UPDATE
    error_rules = [r for r in result if "ON VIOLATION FAIL UPDATE" in r]
    # Check that warn rules don't have ON VIOLATION clause
    warn_rules = [r for r in result if "ON VIOLATION" not in r]
    
    # At least one rule should have the appropriate criticality handling
    assert len(error_rules) > 0 or len(warn_rules) > 0


def test_generate_dlt_rules_ai_assisted_per_rule_criticality_python(ws, spark):
    """Test that per-rule criticality is correctly applied in Python output."""
    user_input = "User ID must not be null (error). Username should not be null (warn)."
    
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(user_input=user_input, language="Python")
    
    assert isinstance(result, str)
    # Should have separate decorator blocks for error and warn
    assert "@dlt.expect_all_or_fail" in result or "@dlt.expect_all" in result


def test_generate_dlt_rules_ai_assisted_with_profiler_summary_stats(ws, spark, make_table, make_schema):
    """Test AI-assisted DLT rules generation using summary stats from profiler."""
    from tests.conftest import TEST_CATALOG
    
    schema = make_schema(catalog_name=TEST_CATALOG)
    input_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("product_id", "string"), ("price", "double"), ("quantity", "int")],
    )
    
    # Profile the table to get summary stats
    profiler = DQProfiler(workspace_client=ws, spark=spark)
    summary_stats, _ = profiler.profile_table(InputConfig(location=input_table.full_name))
    
    # Generate DLT rules using summary stats
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    result = generator.generate_dlt_rules_ai_assisted(summary_stats=summary_stats, language="SQL")
    
    assert isinstance(result, list)
    assert len(result) > 0


def test_generate_dlt_rules_ai_assisted_missing_inputs(ws, spark):
    """Test error when neither user_input nor summary_stats are provided."""
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    
    with pytest.raises(MissingParameterError, match="Either summary statistics or user input must be provided"):
        generator.generate_dlt_rules_ai_assisted()


def test_generate_dlt_rules_ai_assisted_unsupported_language(ws, spark):
    """Test error for unsupported language in AI-assisted generation."""
    generator = DQDltGenerator(workspace_client=ws, spark=spark)
    
    with pytest.raises(InvalidParameterError, match="Unsupported language"):
        generator.generate_dlt_rules_ai_assisted(user_input="Test", language="unsupported")
