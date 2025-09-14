import dspy  # type: ignore
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
        desc="""You must generate a valid JSON array of data quality rules. 

Format for each rule:
{
  "criticality": "error" | "warn",
  "check": {
    "function": "<function_name>",
    "arguments": {
      "column": "<column_name>",
      "<arg_name>": <value>
    }
  }
}

Constraints:
1. Pick the most suitable function based on business description and available functions.
2. "criticality" must be either "error" or "warn"
3. Use integers for numeric limits (e.g., min_limit: 1, not 0.01)
4. Argument names must be correct (e.g., min_limit, max_limit)
5. For `is_in_range`, include both min_limit and max_limit as integers
6. For finite sets, use `is_in_list`
7. For patterns, use `regex_match` with properly escaped regex (use `\\\\` for backslashes)
8. The final output must be a **single-line JSON array** with no extra text, no newlines, and valid JSON syntax

Examples:
[{"criticality":"error","check":{"function":"is_not_null","arguments":{"column":"customer_id"}}},{"criticality":"error","check":{"function":"regex_match","arguments":{"column":"email","regex":"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"}}},{"criticality":"error","check":{"function":"is_in_range","arguments":{"column":"amount","min_limit":1,"max_limit":1000}}},{"criticality":"error","check":{"function":"is_unique","arguments":{"columns":["customer_id"],"nulls_distinct":true}}}]
"""
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGeneration(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generator = dspy.ChainOfThought(RuleSignature)

    def forward(self, schema_info: str, business_description: str, available_functions: str):
        return self.generator(
            schema_info=schema_info, business_description=business_description, available_functions=available_functions
        )


def _configure_dspy_lm(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = "", api_base: str = ""
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
    """Validate generated rules with granular scoring for better optimizer feedback.

    Scoring breakdown:
    - JSON parsing (20%): Can parse actual output as valid JSON?
    - Structure validation (20%): Has expected rule structure?
    - DQX validation (30%): Passes DQX validation checks?
    - Content similarity (30%): Matches expected rules content?
    """
    total_score = 0.0

    # Score weights
    JSON_WEIGHT = 0.2
    STRUCTURE_WEIGHT = 0.2
    DQX_WEIGHT = 0.3
    SIMILARITY_WEIGHT = 0.3

    # Parse expected rules (assume this always works)
    try:
        expected_rules = json.loads(expected)
    except Exception as e:
        print(f"ERROR: Expected rules JSON parsing failed: {e}")
        return 0.0

    # 1. JSON Parsing Score (20%)
    actual_rules = None
    try:
        actual_rules = json.loads(actual)
        total_score += JSON_WEIGHT
        print(f"✓ JSON parsing successful (+{JSON_WEIGHT:.1f})")
    except Exception as e:
        print(f"✗ JSON parsing failed: {e}")
        # Early return if we can't parse JSON at all
        return total_score

    # 2. Structure Validation Score (20%)
    if _validate_rule_structure(actual_rules):
        total_score += STRUCTURE_WEIGHT
        print(f"✓ Structure validation passed (+{STRUCTURE_WEIGHT:.1f})")
    else:
        print("✗ Structure validation failed")

    # 3. DQX Validation Score (30%)
    try:
        validation_status = DQEngine.validate_checks(actual_rules)
        if not validation_status.has_errors:
            total_score += DQX_WEIGHT
            print(f"✓ DQX validation passed (+{DQX_WEIGHT:.1f})")
        else:
            print(f"✗ DQX validation errors: {validation_status.errors}")
    except Exception as e:
        print(f"✗ DQX validation exception: {e}")

    # 4. Content Similarity Score (30%)
    similarity_score = _calculate_rule_similarity(expected_rules, actual_rules)
    content_points = similarity_score * SIMILARITY_WEIGHT
    total_score += content_points
    print(f"✓ Content similarity: {similarity_score:.2f} (+{content_points:.2f})")

    print(f"Final score: {total_score:.2f}")
    return total_score


def _validate_rule_structure(rules) -> bool:
    """Validate that rules have expected structure."""
    try:
        if not isinstance(rules, list):
            return False

        for rule in rules:
            if not isinstance(rule, dict):
                return False
            if 'criticality' not in rule or 'check' not in rule:
                return False
            if not isinstance(rule.get('check'), dict):
                return False
            if 'function' not in rule.get('check', {}):
                return False

        return True
    except Exception:
        return False


def _calculate_rule_similarity(expected_rules: list, actual_rules: list) -> float:
    """Calculate similarity between expected and actual rules."""
    if not expected_rules or not actual_rules:
        return 0.0

    total_expected = len(expected_rules)
    matches = 0
    partial_matches = 0

    for expected_rule in expected_rules:
        best_match_score = 0.0

        for actual_rule in actual_rules:
            match_score = 0.0

            # Check criticality match (30% of rule score)
            if expected_rule.get('criticality') == actual_rule.get('criticality'):
                match_score += 0.3

            # Check function match (50% of rule score)
            expected_func = expected_rule.get('check', {}).get('function')
            actual_func = actual_rule.get('check', {}).get('function')
            if expected_func == actual_func:
                match_score += 0.5

            # Check arguments similarity (20% of rule score)
            expected_args = expected_rule.get('check', {}).get('arguments', {})
            actual_args = actual_rule.get('check', {}).get('arguments', {})

            if expected_args and actual_args:
                # Simple argument similarity - count matching keys
                common_keys = set(expected_args.keys()) & set(actual_args.keys())
                total_keys = set(expected_args.keys()) | set(actual_args.keys())

                if total_keys:
                    arg_similarity = len(common_keys) / len(total_keys)
                    match_score += 0.2 * arg_similarity

            best_match_score = max(best_match_score, match_score)

        if best_match_score >= 0.8:  # Almost perfect match
            matches += 1
        elif best_match_score >= 0.3:  # Partial match
            partial_matches += 1

    # Calculate final similarity score
    full_match_score = matches / total_expected
    partial_match_score = (partial_matches * 0.5) / total_expected

    return min(1.0, full_match_score + partial_match_score)


def get_dspy_compiler(
    api_key: str = "", api_base: str = "", model: str = "databricks/databricks-meta-llama-3-3-70b-instruct"
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

    optimizer = dspy.MIPROv2(
        metric=lambda x, y, trace=None: validate_generated_rules(x.quality_rules, y.quality_rules),
        # max_bootstrapped_demos=4,  # Use 3 examples to balance quality and rate limits
        # max_labeled_demos=4,
        auto="light",
        num_threads=2,
        # num_candidates=5,
        # bsize=10,
        # max_steps=1,
    )
    optimized_model = optimizer.compile(model, trainset=trainset)

    return optimized_model
