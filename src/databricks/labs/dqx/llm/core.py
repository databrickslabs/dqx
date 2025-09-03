import dspy
from dspy.teleprompt import BootstrapFewShot
from databricks.labs.dqx.llm.utils import create_optimizer_training_set
import json
import logging

logger = logging.getLogger(__name__)


class RuleSignature(dspy.Signature):
    schema_info: str = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description: str = dspy.InputField(desc="Natural language description of data quality requirements")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc="""JSON String of data quality rules. Each rule must follow this exact structure: {
  "criticality": "error|warn",
  "check": {
    "function": "<function_name>",
    "arguments": {
      "column": "<column_name>",
      "additional_args": "<values>"
    }
  }
}

IMPORTANT RULES:
1. Valid values for criticality are "error" or "warn"
2. Use integers for numeric limits (e.g., min_limit: 1, not min_limit: 0.01)
3. Use correct argument names: min_limit, max_limit
4. For is_in_range: use min_limit and max_limit as integers
5. Prefer is_in_list for known finite sets of values
6. Use regex_match for pattern-based validation when needed
7. For regex_match: escape backslashes properly in JSON (use \\\\ instead of \\)

EXAMPLES:
[{"criticality":"error","check":{"function":"is_not_null","arguments":{"column":"customer_id"}}},{"criticality":"error","check":{"function":"regex_match","arguments":{"column":"email","regex":"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"}}},{"criticality":"error","check":{"function":"is_in_range","arguments":{"column":"amount","min_limit":1}}},{"criticality":"error","check":{"function":"is_unique","arguments":{"columns":["customer_id"],"nulls_distinct":true}}}]

CRITICAL: Return ONLY a single-line JSON array with no newlines, no indentation, no extra whitespace, and no additional text. The output must be parseable JSON."""
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGen(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(RuleSignature)

    def forward(self, schema_info: str, business_description: str, available_functions: str):
        return self.generator(schema_info=schema_info, business_description=business_description, available_functions=available_functions)


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
        max_retries=3,  # Add retry mechanism
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
        # # Clean up the actual output - remove markdown code blocks if present
        # if "```yaml" in actual:
        #     actual = actual.split("```yaml")[1].split("```")[0].strip()
        # elif "```" in actual:
        #     actual = actual.split("```")[1].split("```")[0].strip()

        # # Fix regex patterns before parsing YAML
        # actual = fix_regex_patterns_in_yaml(actual)

        # Parse YAML
        expected_rules = json.loads(expected)
        actual_rules = json.loads(actual)

        if not actual_rules:
            return 0.0

        # Basic DQX validation
        from databricks.labs.dqx.engine import DQEngine

        validation_status = DQEngine.validate_checks(actual_rules)

        if validation_status.has_errors:
            print(f"DQX validation errors: {validation_status.errors}")
            # Try to fix common issues in the LLM output
            # fixed_rules = fix_common_llm_issues(actual_rules)
            # if fixed_rules:
            #     validation_status = DQEngine.validate_checks(fixed_rules)
            #     if not validation_status.has_errors:
            #         actual_rules = fixed_rules
            #         print("Fixed common LLM issues")
            #     else:
            #         return 0.0
            # else:
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
    api_key: str = None, api_base: str = None, model: str = "databricks/dqx-gpt-oss-12b"
) -> DQRuleGen:
    """A utility function to get the Dspy compiler.

    :param custom_check_functions: A dictionary of custom check functions.
        If provided, the function will use the custom check functions to resolve the check function.
        If not provided, the function will use only the built-in check functions.
    """

    if not api_key or not api_base:
        raise ValueError("api_key and api_base must be provided")

    _configure_dspy_lm(api_key=api_key, api_base=api_base, model=model)

    model = DQRuleGen()
    trainset = create_optimizer_training_set()

    optimizer = dspy.BootstrapFewShot(
        metric=lambda x, y, trace=None: validate_generated_rules(x.quality_rules, y.quality_rules),
        max_bootstrapped_demos=3,  # Use 3 examples to balance quality and rate limits
        max_labeled_demos=3
    )
    optimized_model = optimizer.compile(model, trainset=trainset)

    return optimized_model
