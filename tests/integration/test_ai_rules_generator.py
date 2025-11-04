import pyspark.sql.functions as F
from tests.conftest import TEST_CATALOG
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.check_funcs import make_condition, register_rule


USER_INPUT = """
Username should not start with 's' if age is less than 18. Use exact wording if needed in the generated rule.
All users must have a valid email address.
Age should be between 0 and 120.
"""

EXPECTED_CHECKS = [
    {
        "check": {
            "arguments": {
                "columns": ["username", "age"],
                "expression": "NOT (username LIKE 's%' AND age < 18)",
                "msg": "Username should not start with 's' if age is less than 18",
            },
            "function": "sql_expression",
        },
        "criticality": "error",
    },
    {
        "check": {
            "arguments": {"column": "email", "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
            "function": "regex_match",
        },
        "criticality": "error",
    },
    {
        "check": {"arguments": {"column": "age", "max_limit": 120, "min_limit": 0}, "function": "is_in_range"},
        "criticality": "error",
    },
]


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
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=USER_INPUT, table_name=input_table.full_name)
    assert actual_checks == EXPECTED_CHECKS


def test_generate_dq_rules_ai_assisted_custom_model(ws, spark):
    llm_model_config = LLMModelConfig(model="databricks/databricks-llama-4-maverick")
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

    user_input = USER_INPUT + "\nEmail address must not end with '@gmail.com'."

    generator = DQGenerator(ws, spark, custom_check_functions=custom_check_functions)
    actual_checks = generator.generate_dq_rules_ai_assisted(user_input=user_input)

    expected_checks = EXPECTED_CHECKS + [
        {
            'check': {
                'arguments': {'column': 'email', 'suffix': '@gmail.com'},
                'function': 'not_ends_with_suffix',
            },
            'criticality': 'error',
        }
    ]
    assert actual_checks == expected_checks
