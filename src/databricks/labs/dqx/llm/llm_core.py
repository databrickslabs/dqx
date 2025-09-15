import json
import logging
import dspy  # type: ignore
from databricks.labs.dqx.llm.utils import create_optimizer_training_set
from databricks.labs.dqx.engine import DQEngine

logger = logging.getLogger(__name__)


class RuleSignature(dspy.Signature):
    """Generate data quality rules with improved output format."""

    schema_info: str = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description: str = dspy.InputField(desc="Natural language description of data quality requirements")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc=(
            "Return a valid JSON array of data quality rules. Use double quotes only. "
            "Criticality can be error or warn. "
            "Format: [{\"criticality\":\"error\",\"check\":{\"function\":\"name\",\"arguments\":{\"column\":\"col\"}}}] "
            "Example: [{\"criticality\":\"error\",\"check\":{\"function\":\"is_not_null\",\"arguments\":{\"column\":\"customer_id\"}}}]"
        )
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGeneration(dspy.Module):
    """DQ rule generation with improved JSON output reliability."""

    def __init__(self):
        super().__init__()
        # Use Predict for reliable output
        self.generator = dspy.Predict(RuleSignature)

    def forward(self, schema_info: str, business_description: str, available_functions: str):
        result = self.generator(
            schema_info=schema_info, business_description=business_description, available_functions=available_functions
        )

        # Validate and clean the JSON output
        if hasattr(result, 'quality_rules'):
            try:
                # Try to parse the JSON to ensure it's valid
                parsed_rules = json.loads(result.quality_rules)
                # Re-serialize to ensure consistent formatting
                result.quality_rules = json.dumps(parsed_rules, separators=(',', ':'))
            except json.JSONDecodeError as e:
                logger.warning(f"Generated invalid JSON: {e}. Raw output: {result.quality_rules}")
                # Return a fallback empty array if JSON is invalid
                result.quality_rules = "[]"

        return result

    def get_json_rules(self, schema_info: str, business_description: str, available_functions: str) -> str:
        """Get rules as JSON string."""
        result = self.forward(schema_info, business_description, available_functions)
        return result.quality_rules


def _configure_dspy_lm(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = "", api_base: str = ""
):
    """Configure the Dspy language model.

    :param model: The model to use for the Dspy language model.
    :param api_key: API key for authenticating with the language model service.
    :param api_base: Base URL for the language model API endpoint.
    """
    language_model = dspy.LM(
        model=model,
        model_type="chat",
        api_key=api_key,
        api_base=api_base,
        max_retries=3,  # Add retry mechanism
    )
    dspy.configure(lm=language_model)


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
    json_weight = 0.2
    structure_weight = 0.2
    dqx_weight = 0.3
    similarity_weight = 0.3

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
        total_score += json_weight
        print(f"✓ JSON parsing successful (+{json_weight:.1f})")
    except Exception as e:
        print(f"✗ JSON parsing failed: {e}")
        print(f"  Raw output: {repr(actual[:200])}")  # Show first 200 chars for debugging
        return total_score

    # 2. Structure Validation Score (20%)
    if _validate_rule_structure(actual_rules):
        total_score += structure_weight
        print(f"✓ Structure validation passed (+{structure_weight:.1f})")
    else:
        print("✗ Structure validation failed")

    # 3. DQX Validation Score (30%)
    try:
        validation_status = DQEngine.validate_checks(actual_rules)
        if not validation_status.has_errors:
            total_score += dqx_weight
            print(f"✓ DQX validation passed (+{dqx_weight:.1f})")
        else:
            print(f"✗ DQX validation errors: {validation_status.errors}")
    except Exception as e:
        print(f"✗ DQX validation exception: {e}")

    # 4. Content Similarity Score (30%)
    similarity_score = _calculate_rule_similarity(expected_rules, actual_rules)
    content_points = similarity_score * similarity_weight
    total_score += content_points
    print(f"✓ Content similarity: {similarity_score:.2f} (+{content_points:.2f})")

    print(f"Final score: {total_score:.2f}")
    return total_score


def _validate_single_rule(rule) -> bool:
    """Validate that a single rule has the expected structure."""
    if not isinstance(rule, dict):
        return False
    if 'criticality' not in rule or 'check' not in rule:
        return False
    if not isinstance(rule.get('check'), dict):
        return False
    if 'function' not in rule.get('check', {}):
        return False
    return True


def _validate_rule_structure(rules) -> bool:
    """Validate that rules have expected structure."""
    try:
        if not isinstance(rules, list):
            return False
        return all(_validate_single_rule(rule) for rule in rules)
    except Exception:
        return False


def _calculate_argument_similarity(expected_args: dict, actual_args: dict) -> float:
    """Calculate similarity between rule arguments based on matching keys."""
    if not expected_args or not actual_args:
        return 0.0

    common_keys = set(expected_args.keys()) & set(actual_args.keys())
    total_keys = set(expected_args.keys()) | set(actual_args.keys())

    if not total_keys:
        return 0.0

    return len(common_keys) / len(total_keys)


def _calculate_single_rule_match(expected_rule: dict, actual_rule: dict) -> float:
    """Calculate match score between two individual rules."""
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
    arg_similarity = _calculate_argument_similarity(expected_args, actual_args)
    match_score += 0.2 * arg_similarity

    return match_score


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
            match_score = _calculate_single_rule_match(expected_rule, actual_rule)
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
    api_key: str = "",
    api_base: str = "",
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct",
):
    """A utility function to get the Dspy compiler.

    Args:
        api_key: API key for the LLM service
        api_base: Base URL for the LLM API
        model: Model name to use

    Returns:
        Configured and optimized DQ rule generation model with improved JSON output
    """

    if not api_key or not api_base:
        raise ValueError("api_key and api_base must be provided")

    _configure_dspy_lm(api_key=api_key, api_base=api_base, model=model)

    # Use standard DSPy approach with improved prompting
    model = DQRuleGeneration()
    trainset = create_optimizer_training_set()

    # Standard metric for JSON output validation
    def json_metric(example, pred, _trace=None):
        try:
            if hasattr(pred, 'quality_rules'):
                return validate_generated_rules(example.quality_rules, pred.quality_rules)
            return 0.0
        except Exception:
            return 0.0

    optimizer = dspy.BootstrapFewShot(
        metric=json_metric,
        max_bootstrapped_demos=3,
        max_labeled_demos=5,
        teacher_settings={},
    )

    optimized_model = optimizer.compile(model, trainset=trainset)
    return optimized_model
