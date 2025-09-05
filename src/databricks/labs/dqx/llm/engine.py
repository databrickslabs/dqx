from databricks.labs.dqx.llm.core import DQRuleGeneration
from databricks.labs.dqx.llm.utils import get_required_check_function_info, get_check_function_definition
import json
import dspy


def get_business_rules_with_llm(
    user_input: str, schema_info: str, dspy_compiler: DQRuleGeneration
) -> dspy.primitives.prediction.Prediction:
    """A function to get the dql rules based on natural language request and schema information.

    Returns:
        Prediction: A Prediction object containing the dqx rules and reasoning.
    """
    return dspy_compiler(
        schema_info=schema_info,
        business_description=f"""{user_input}""",
        available_functions=json.dumps(get_required_check_function_info()),
    )
