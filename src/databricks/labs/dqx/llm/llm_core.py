import json
import logging
import dspy  # type: ignore
from databricks.labs.dqx.llm.utils import create_optimizer_training_set
from databricks.labs.dqx.engine import DQEngineCore

logger = logging.getLogger(__name__)


class RuleSignature(dspy.Signature):
    """
    Generate data quality rules with improved output format.

    This class defines the schema for generating data quality rules based on
    schema information, business descriptions, and available functions.
    """

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
    """
    Generate data quality rules with improved JSON output reliability.

    This class provides functionality to generate data quality rules based on schema information,
    business descriptions, and available functions. It ensures that the output is a valid JSON
    array and includes mechanisms to validate and clean the generated rules.
    """

    def __init__(self):
        super().__init__()
        # Use Predict for reliable output
        self.generator = dspy.Predict(RuleSignature)

    def forward(
        self, schema_info: str, business_description: str, available_functions: str
    ) -> dspy.primitives.prediction.Prediction:
        """
        Generate data quality rules based on schema information, business descriptions, and available functions.

        Args:
            schema_info (str): JSON string containing table schema with column names, types, and sample data.
            business_description (str): Natural language description of data quality requirements.
            available_functions (str): JSON string of available DQX check functions.

        Returns:
            dspy.primitives.prediction.Prediction: A Prediction object containing the generated data quality rules
            and reasoning.
        """
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
        """
        Get rules as a JSON string.

        Args:
            schema_info (str): JSON string containing table schema with column names, types, and sample data.
            business_description (str): Natural language description of data quality requirements.
            available_functions (str): JSON string of available DQX check functions.

        Returns:
            str: A JSON string representing the generated data quality rules.
        """
        result = self.forward(schema_info, business_description, available_functions)
        return result.quality_rules


def _configure_dspy_model(model: str, api_key: str = "", api_base: str = ""):
    """
    Configure the Dspy language model.

    Args:
        model (str): The model to use for the Dspy language model.
        api_key (str): The API key for the model. Not required by Databricks foundational models.
        api_base (str): The API base URL for the model. Not required by Databricks foundational models.
    """
    language_model = dspy.LM(
        model=model,
        model_type="chat",
        api_key=api_key,
        api_base=api_base,
        max_retries=3,
    )
    dspy.configure(lm=language_model)


def validate_generated_rules(expected: str, actual: str) -> float:
    """
    Validate generated rules with granular scoring for better optimizer feedback.

    Scoring breakdown:
        - JSON parsing (20%): Checks if the actual output can be parsed as valid JSON.
        - Rules validation (50%): Ensures the rules pass DQX validation checks.
        - Content similarity (30%): Compares the actual rules content with the expected rules.

    Args:
        expected (str): JSON string of the expected rules.
        actual (str): JSON string of the actual generated rules.

    Returns:
        float: A score between 0.0 and 1.0 representing the quality of the generated rules.
    """
    total_score = 0.0

    # Score weights
    json_weight = 0.2
    rule_weight = 0.5
    similarity_weight = 0.3

    expected_rules = json.loads(expected)

    # Json parsing score (20%)
    try:
        actual_rules = json.loads(actual)
        total_score += json_weight
        logger.info(f"✓ JSON parsing successful (+{json_weight:.1f})")
    except json.JSONDecodeError as e:
        logger.warning(f"✗ JSON parsing failed: {e}")
        logger.debug(f"  Raw output: {repr(actual[:200])}")
        # Early return if we can't parse JSON at all
        return total_score

    # Rules validation score (50%)
    validation_status = DQEngineCore.validate_checks(actual_rules)
    if not validation_status.has_errors:
        total_score += rule_weight
        logger.info(f"✓ Rules validation passed (+{rule_weight:.1f})")
    else:
        logger.warning(f"✗ Rules validation errors: {validation_status.errors}")

    # Content similarity score (30%)
    similarity_score = _calculate_rule_similarity(expected_rules, actual_rules)
    content_points = similarity_score * similarity_weight
    total_score += content_points
    logger.info(f"✓ Content similarity: {similarity_score:.2f} (+{content_points:.2f})")

    logger.info(f"Final score: {total_score:.2f}")
    return total_score


def _calculate_rule_similarity(expected_rules: list, actual_rules: list) -> float:
    """
    Calculate the similarity between expected and actual rules.

    This function compares two lists of rules and computes a similarity score based on
    the criticality, function, and arguments of the rules. Full matches and partial matches
    are considered in the final similarity score.

    Args:
        expected_rules (list): A list of dictionaries representing the expected rules.
        actual_rules (list): A list of dictionaries representing the actual rules.

    Returns:
        float: A similarity score between 0.0 and 1.0, where 1.0 indicates perfect similarity.
    """
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


def _calculate_single_rule_match(expected_rule: dict, actual_rule: dict) -> float:
    """
    Calculate the match score between two individual rules.

    This function compares the criticality, function, and arguments of two rules
    and computes a match score based on their similarity.

    Args:
        expected_rule (dict): A dictionary representing the expected rule.
        actual_rule (dict): A dictionary representing the actual rule.

    Returns:
        float: A match score between 0.0 and 1.0, where 1.0 indicates a perfect match.
    """
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


def _calculate_argument_similarity(expected_args: dict, actual_args: dict) -> float:
    """
    Calculate similarity between expected and actual rule arguments.

    Args:
        expected_args: Dictionary of expected arguments
        actual_args: Dictionary of actual arguments

    Returns:
        float: Argument similarity score between 0.0 and 1.0
    """
    if not expected_args or not actual_args:
        return 0.0

    common_keys = set(expected_args.keys()) & set(actual_args.keys())
    total_keys = set(expected_args.keys()) | set(actual_args.keys())

    if not total_keys:
        return 0.0

    return len(common_keys) / len(total_keys)


def get_dspy_compiler(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = "", api_base: str = ""
) -> dspy.Module:
    """
    Get the Dspy compiler configured with an optimizer.

    This function initializes and configures the Dspy compiler with a training set and an optimizer
    to validate and optimize the generated data quality rules.

    Args:
        model (str): The model to use for the Dspy language model.
        api_key (str): The API key for the model. Not required by Databricks foundational models.
        api_base (str): The API base URL for the model. Not required by Databricks foundational models.

    Returns:
        dspy.Module: An optimized Dspy module for generating data quality rules.
    """
    _configure_dspy_model(api_key=api_key, api_base=api_base, model=model)

    # Use standard DSPy approach with improved prompting
    model = DQRuleGeneration()
    train_set = create_optimizer_training_set()

    # Standard metric for JSON output validation
    def json_metric(example, pred, _trace=None):
        if hasattr(pred, 'quality_rules'):
            return validate_generated_rules(example.quality_rules, pred.quality_rules)
        return 0.0

    optimizer = dspy.BootstrapFewShot(
        metric=json_metric,
        max_bootstrapped_demos=3,
        max_labeled_demos=5,
        teacher_settings={},
    )

    optimized_model = optimizer.compile(model, trainset=train_set)
    return optimized_model
