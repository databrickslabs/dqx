import dspy
from dspy.teleprompt import BootstrapFewShot
from databricks.labs.dqx.llm.utils import create_optimizer_training_set
import json
import logging
from databricks.labs.dqx.engine import DQEngine

logger = logging.getLogger(__name__)


class RuleSignature(dspy.Signature):
    schema_info: str = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description: str = dspy.InputField(desc="Natural language description of data quality requirements")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc="""JSON String of data quality rule. Each rule must follow this exact structure: {"criticality":"error|warn","check":{"function":"<function_name>","arguments":{"column":"<column_name>","additional_args":"<values>"}}}

Rules:
1. Valid values for criticality are "error" or "warn".
2. Use integers for numeric limits (e.g., min_limit:1 not min_limit:0.01).
3. Use correct argument names: min_limit, max_limit.
4. For is_in_range: use min_limit and max_limit as integers.
5. If regex is absolutely necessary, use simple patterns without special characters.
6. Use is in list funtion only the values are fixed. Try using regex otherwise.
7. Do not include quotes or spaces.

Examples:
[{"criticality":"error","check":{"function":"is_not_null","arguments":{"column":"customer_id"}}},{"criticality":"error","check":{"function":"is_not_null_and_not_empty","arguments":{"column":"first_name","trim_strings":true}}},{"criticality":"error","check":{"function":"is_in_range","arguments":{"column":"amount","min_limit":1}}},{"criticality":"error","check":{"function":"is_unique","arguments":{"columns":["customer_id"],"nulls_distinct":true}}},{"criticality":"error","check":{"function":"is_in_list","arguments":{"column":"country","allowed":["US","CA","UK","DE","FR","AU","JP","IN"]}}}]
"""
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGeneration(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(RuleSignature)

    def forward(self, schema_info: str, user_input: str, available_functions: str):
        return self.generator(schema_info=schema_info, user_input=user_input, available_functions=available_functions)


def _configure_dspy_lm(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = None, api_base: str = None
):
    """Configure the Dspy language model.

    :param model: The model to use for the Dspy language model.
    """
    lm = dspy.LM(
        model=model,
        model_type="chat",
        api_key=api_key,
        api_base=api_base,
    )
    dspy.configure(lm=lm)


class AssessDQRules(dspy.Signature):

    schema_info = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description = dspy.InputField(desc="Natural language description of quality requirements")
    expected_rules = dspy.InputField(desc="YAML string of expected quality rules")
    actual_rules = dspy.InputField(desc="YAML string of actual generated quality rules")
    assessment_question = dspy.InputField(desc="Specific question about the quality of the rules")
    assessment_answer: bool = dspy.OutputField(desc="Boolean assessment of the quality")


def validate_generated_rules(expected: str, actual: str) -> float:
    """Validate generated rules against expected rules with better error handling."""
    try:
        # Parse Json
        expected_rules = json.loads(expected)
        actual_rules = json.loads(actual)

        if not actual_rules:
            return 0.0

        # Basic DQX validation

        validation_status = DQEngine.validate_checks(actual_rules)

        if validation_status.has_errors:
            print(f"DQX validation errors: {validation_status.errors}")
            return 0.0

        # Calculate similarity score
        score = 0.0
        total_checks = 0

        # Compare each rule
        for expected_rule in expected_rules:
            total_checks += 1
            for actual_rule in actual_rules:
                if expected_rule.get('criticality') == actual_rule.get('criticality') and expected_rule.get(
                    'check', {}
                ).get('function') == actual_rule.get('check', {}).get('function'):
                    score += 1.0
                    break

        if total_checks == 0:
            return 0.0

        return score / total_checks

    except Exception as e:
        print(f"Validation error: {e}")
        return 0.0


def get_dspy_compiler(
    api_key: str = None, api_base: str = None, model: str = "databricks/databricks-meta-llama-3-3-70b-instruct"
) -> DQRuleGeneration:
    """A utility function to get the Dspy compiler.

    :param custom_check_functions: A dictionary of custom check functions.
        If provided, the function will use the custom check functions to resolve the check function.
        If not provided, the function will use only the built-in check functions.
    """

    if not api_key or not api_base:
        raise ValueError("api_key and api_base must be provided")

    _configure_dspy_lm(api_key=api_key, api_base=api_base, model=model)

    model = DQRuleGeneration()
    trainset = create_optimizer_training_set()

    optimizer = dspy.BootstrapFewShot(
        metric=lambda x, y, trace=None: validate_generated_rules(x.quality_rules, y.quality_rules)
    )
    optimized_model = optimizer.compile(model, trainset=trainset)

    return optimized_model
