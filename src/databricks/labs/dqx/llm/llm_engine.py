import json
import dspy  # type: ignore
from databricks.labs.dqx.llm.llm_core import DQRuleGeneration
from databricks.labs.dqx.llm.utils import _get_required_check_function_info

sample_schema = json.dumps(
    {
        "columns": [
            {"name": "user_id", "type": "string"},
            {"name": "username", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "age", "type": "integer"},
            {"name": "country", "type": "string"},
            {"name": "join_date", "type": "string"},
            {"name": "is_verified", "type": "boolean"},
            {"name": "followers_count", "type": "integer"},
        ]
    }
)


def get_business_rules_with_llm(
    user_input: str, dspy_compiler: DQRuleGeneration, schema_info: str = sample_schema
) -> dspy.primitives.prediction.Prediction:
    """A function to get the dqx rules based on natural language request and schema information.

    Args:
        user_input: Natural language description of data quality requirements.
        dspy_compiler: The compiled DQRuleGeneration model.
        schema_info: Optional JSON string containing table schema with column names and types.
                    If not provided, an sample schema will be used.

    Returns:
        Prediction: A Prediction object containing the dqx rules and reasoning.
    """

    return dspy_compiler(
        schema_info=schema_info,
        business_description=f"""{user_input}""",
        available_functions=json.dumps(_get_required_check_function_info()),
    )
