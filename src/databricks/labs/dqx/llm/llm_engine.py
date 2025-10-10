import json
import dspy  # type: ignore
from databricks.labs.dqx.llm.llm_core import DQRuleGeneration
from databricks.labs.dqx.llm.llm_utils import _get_required_check_function_info


def get_business_rules_with_llm(
    user_input: str, dspy_compiler: DQRuleGeneration, schema_info: str = ""
) -> dspy.primitives.prediction.Prediction:
    """
    Get DQX rules based on natural language request with optional schema information.

    If schema_info is empty (default) and the dspy_compiler has schema inference enabled,
    it will automatically infer the schema from the user_input before generating rules.

    Args:
        user_input: Natural language description of data quality requirements.
        dspy_compiler: The compiled DQRuleGeneration model.
        schema_info: Optional JSON string containing table schema with column names and types.
                    If empty (default), triggers schema inference if enabled.
                    If provided, uses that schema directly.

    Returns:
        Prediction: A Prediction object containing:
                   - quality_rules: The generated DQ rules
                   - reasoning: Explanation of the rules (includes schema inference info if schema was inferred)
                   - guessed_schema_json: The inferred schema (if schema was inferred)
                   - assumptions_bullets: Assumptions made about schema (if schema was inferred)
                   - schema_info: The final schema used (if schema was inferred)
    """
    return dspy_compiler(
        schema_info=schema_info,
        business_description=user_input,
        available_functions=json.dumps(_get_required_check_function_info()),
    )
