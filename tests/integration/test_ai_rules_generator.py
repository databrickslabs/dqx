import re

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from tests.constants import TEST_CATALOG
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig, InputConfig
from databricks.labs.dqx.check_funcs import make_condition, register_rule


# Sample user input with specific requirements to avoid flakiness in tests
USER_INPUT = """
Users at age 18 or above must have a valid email address.
Age should be between 0 and 120. Output the rules in the given order.
"""

EXPECTED_CHECKS = [
    {
        "check": {"arguments": {"column": "email"}, "function": "is_valid_email"},
        "criticality": "error",
        "filter": "age >= 18",
    },
    {
        "check": {"arguments": {"column": "age", "max_limit": 120, "min_limit": 0}, "function": "is_in_range"},
        "criticality": "error",
    },
]


# Name the placeholder explicitly so the test does not depend on the model choosing a non-default one
CUSTOM_INPUT_PLACEHOLDER = "orders"
SQL_QUERY_CUSTOM_PLACEHOLDER_USER_INPUT = (
    "Each order's total must not exceed 10 times the average order total for that day. "
    f"Use sql_query for the check function with input_placeholder set to '{CUSTOM_INPUT_PLACEHOLDER}'."
)


def assert_sql_query_checks_use_placeholder(checks: list[dict], placeholder: str) -> None:
    """Asserts that the generated sql_query checks set *placeholder* and reference it in the query."""
    sql_query_checks = [c for c in checks if c.get("check", {}).get("function") == "sql_query"]
    assert sql_query_checks, f"Expected at least one sql_query check, got: {checks}"

    # the placeholder is resolved with optional whitespace inside the braces, so match it the same way
    placeholder_pattern = re.compile(r"\{\{\s*" + re.escape(placeholder) + r"\s*\}\}")
    for check in sql_query_checks:
        args = check["check"]["arguments"]
        assert (
            args.get("input_placeholder") == placeholder
        ), f"Expected input_placeholder '{placeholder}', got: {args.get('input_placeholder')}"
        query = args.get("query", "")
        assert placeholder_pattern.search(query), f"Query must reference {{{{ {placeholder} }}}}, got: {query}"


def test_generate_dq_rules_ai_assisted(ws, spark):
    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=USER_INPUT)
    assert actual_checks == EXPECTED_CHECKS


def test_generate_dq_rules_ai_assisted_with_input_table(ws, spark, make_table, make_schema):
    schema = make_schema(catalog_name=TEST_CATALOG)
    input_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("user_id", "string"), ("username", "string"), ("email", "string"), ("age", "int")],
    )
    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(
        user_input=USER_INPUT, input_config=InputConfig(location=input_table.full_name)
    )
    assert actual_checks == EXPECTED_CHECKS


def test_generate_dq_rules_ai_assisted_with_input_table_and_user_input(ws, spark, make_table, make_schema):
    schema = make_schema(catalog_name=TEST_CATALOG)
    input_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("id", "int"), ("name", "string")],
    )

    generator = DQGenerator(ws, spark)

    actual_checks = generator.generate_dq_rules_ai_assisted(
        user_input="name should not start with 'c'",
        input_config=InputConfig(location=input_table.full_name),
    )

    assert len(actual_checks) == 1
    assert actual_checks[0] == {
        "check": {
            "arguments": {"column": "name", "negate": False, "regex": "^[^cC].*"},
            "function": "regex_match",
        },
        "criticality": "error",
    }, "AI generated check should match profiler workflow example"


def test_generate_dq_rules_ai_assisted_with_input_path(ws, spark, make_directory):
    folder = make_directory()
    workspace_file_path = str(folder.absolute()) + "/input_data.parquet"

    schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    test_data = [
        ("user1", "john_doe", "john@example.com", 25),
    ]
    df = spark.createDataFrame(test_data, schema=schema)
    df.write.mode("overwrite").parquet(workspace_file_path)

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(
        user_input=USER_INPUT, input_config=InputConfig(location=workspace_file_path, format="parquet")
    )
    assert actual_checks == EXPECTED_CHECKS


def test_generate_dq_rules_ai_assisted_custom_model(ws, spark):
    llm_model_config = LLMModelConfig(model_name="databricks/databricks-llama-4-maverick")
    generator = DQGenerator(ws, spark, llm_model_config=llm_model_config)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=USER_INPUT)
    assert not DQEngineCore.validate_checks(actual_checks).has_errors


def test_generate_dq_rules_ai_assisted_with_custom_functions(ws, spark):
    @register_rule("row")
    def not_ends_with_suffix(column: str, suffix: str):
        """
        Example of custom python row-level check function.
        """
        return make_condition(
            F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
        )

    custom_check_functions = {"not_ends_with_suffix": not_ends_with_suffix}

    user_input = USER_INPUT + (
        "\nEmail address must not end with '@gmail.com'. "
        "Use not_ends_with_suffix function and don't add any extra quotes for suffix."
    )

    generator = DQGenerator(ws, spark, custom_check_functions=custom_check_functions)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)

    expected_checks = EXPECTED_CHECKS + [
        {
            "check": {
                "arguments": {"column": "email", "suffix": "@gmail.com"},
                "function": "not_ends_with_suffix",
            },
            "criticality": "error",
        }
    ]
    assert actual_checks == expected_checks


def test_generate_dq_rules_ai_assisted_with_is_not_equal_to_str(ws, spark):
    user_input = "Device name must not be equal 'test'"

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)

    expected_checks = [
        {
            "check": {
                "arguments": {"column": "device_name", "value": "'test'"},
                "function": "is_not_equal_to",
            },
            "criticality": "error",
        },
    ]

    assert actual_checks == expected_checks


def test_generate_dq_rules_ai_assisted_with_sql_expression(ws, spark):
    user_input = "Users email must not end with @gmail.com checked using sql expression with 'NOT LIKE', skip msg."

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)

    expected_checks = [
        {
            "check": {
                "arguments": {"columns": ["email"], "expression": "email NOT LIKE '%@gmail.com'"},
                "function": "sql_expression",
            },
            "criticality": "error",
        },
    ]

    assert actual_checks == expected_checks


def test_generate_dq_rules_ai_assisted_with_summary_stats_and_user_input(ws, spark):
    """Test AI rule generation using summary statistics with business description."""
    user_input = "Validate product inventory: ensure prices and quantities are within reasonable ranges"

    summary_stats = {
        "product_code": {"mean": None, "min": "PROD-1000-A", "max": "PROD-9999-Z"},
        "price": {"mean": "125.50", "min": "10.00", "max": "500.00"},
        "stock_quantity": {"mean": "150", "min": "0", "max": "1000"},
    }

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input, summary_stats=summary_stats)

    # Verify checks were generated and are valid
    assert len(actual_checks) > 0
    assert not DQEngineCore.validate_checks(actual_checks).has_errors


def test_generate_dq_rules_ai_assisted_with_summary_stats_only(ws, spark):
    """Test AI rule generation using summary statistics without business description."""
    summary_stats = {
        "temperature": {"mean": "22.5", "min": "-10.0", "max": "50.0"},
        "humidity": {"mean": "65.5", "min": "20.0", "max": "95.0"},
        "sensor_id": {"mean": None, "min": "SEN001", "max": "SEN100"},
    }

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(summary_stats=summary_stats)

    # Verify checks were generated and are valid
    assert len(actual_checks) > 0
    assert not DQEngineCore.validate_checks(actual_checks).has_errors


def test_generate_dq_rules_ai_assisted_email_format_with_summary_stats(ws, spark):
    """Email column from summary stats should yield a valid is_valid_email check.

    Regression for the stats training example that referenced the non-existent
    `is_email` function (corrected to `is_valid_email`). Previously the model
    imitated the bad example and the rule was silently dropped by validation,
    producing no email-format check at all.
    """
    user_input = "Email addresses must be present and follow a valid email format."
    summary_stats = {
        "email": {"mean": None, "min": "alice@example.com", "max": "zoe@example.com"},
        "username": {"mean": None, "min": "alice", "max": "zoe"},
        "account_status": {"mean": None, "min": "active", "max": "suspended"},
    }

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input, summary_stats=summary_stats)

    assert not DQEngineCore.validate_checks(actual_checks).has_errors
    email_format_checks = [
        c
        for c in actual_checks
        if c.get("check", {}).get("function") == "is_valid_email"
        and c.get("check", {}).get("arguments", {}).get("column") == "email"
    ]
    assert len(email_format_checks) == 1, f"Expected one is_valid_email check for email, got: {actual_checks}"


def test_generate_dq_rules_ai_assisted_sql_query_with_custom_input_placeholder(ws, spark):
    user_input = SQL_QUERY_CUSTOM_PLACEHOLDER_USER_INPUT

    summary_stats = {
        "order_id": {"mean": None, "min": "ORD001", "max": "ORD999"},
        "order_date": {"mean": None, "min": "2024-01-01", "max": "2024-12-31"},
        "order_total": {"mean": "250.75", "min": "10.00", "max": "5000.00"},
    }

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input, summary_stats=summary_stats)

    assert_sql_query_checks_use_placeholder(actual_checks, CUSTOM_INPUT_PLACEHOLDER)
    assert not DQEngineCore.validate_checks(actual_checks).has_errors


def test_generate_dq_rules_ai_assisted_sql_query_with_custom_input_placeholder_no_summary_stats(
    ws, spark, make_table, make_schema
):
    user_input = SQL_QUERY_CUSTOM_PLACEHOLDER_USER_INPUT

    schema = make_schema(catalog_name=TEST_CATALOG)
    input_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("order_id", "string"), ("order_date", "string"), ("order_total", "double")],
    )

    generator = DQGenerator(ws, spark)
    actual_checks = generator.generate_dq_rules_ai_assisted(
        user_input=user_input, input_config=InputConfig(location=input_table.full_name)
    )

    assert_sql_query_checks_use_placeholder(actual_checks, CUSTOM_INPUT_PLACEHOLDER)
    assert not DQEngineCore.validate_checks(actual_checks).has_errors


def test_multiple_generator_instances_no_reconfiguration_error(ws, spark):
    """
    Test that creating multiple DQGenerator instances doesn't cause DSPy reconfiguration errors.
    """
    user_input = "Age should be between 0 and 120"

    # Create first generator and generate checks
    generator1 = DQGenerator(ws, spark)
    checks1 = generator1.generate_dq_rules_ai_assisted(user_input=user_input)
    assert len(checks1) > 0

    # Create second generator and generate checks
    generator2 = DQGenerator(ws, spark)
    checks2 = generator2.generate_dq_rules_ai_assisted(user_input=user_input)
    assert len(checks2) > 0
