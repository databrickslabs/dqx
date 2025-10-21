import json
import logging
import dspy  # type: ignore
from databricks.labs.dqx.llm.llm_utils import create_optimizer_training_set
from databricks.labs.dqx.engine import DQEngineCore

logger = logging.getLogger(__name__)


class SchemaGuesserSignature(dspy.Signature):
    """
    Guess a table schema based on business description.

    This class defines the schema for inferring complete table structure from
    natural language descriptions.
    """

    business_description: str = dspy.InputField(
        desc=(
            "Natural language summary of the dataset and its use. "
            "Including some column hints (e.g., id, amount, status, email, dates)."
        )
    )
    guessed_schema_json: str = dspy.OutputField(
        desc=(
            "Strict JSON with shape: "
            '{"columns":[{"name":"<col>","type":"<spark_type>","example":"<opt>"}]}. '
            "Prefer: ids:string, money:decimal(18,2), timestamps:timestamp, dates:date. "
            "Return one line JSON with no extra text."
        )
    )
    assumptions_bullets: str = dspy.OutputField(
        desc=(
            "Concise bullet list (1-6 lines) of assumptions made about columns, types, "
            "and examples. Keep each bullet short."
        )
    )


class SchemaGuesser(dspy.Module):
    """
    Guess table schema from business description.

    This class provides functionality to infer a complete table schema based on
    natural language descriptions of the dataset and its intended use.
    """

    def __init__(self):
        super().__init__()
        self.guess = dspy.ChainOfThought(SchemaGuesserSignature)

    def forward(self, business_description: str) -> dspy.primitives.prediction.Prediction:
        """
        Guess schema based on business description.

        Args:
            business_description (str): Natural language description of the dataset and its use case.

        Returns:
            dspy.primitives.prediction.Prediction: A Prediction object containing the guessed schema
            and assumptions made during the inference process.
        """
        return self.guess(business_description=business_description)


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
            "Check function name and doc to select the appropriate check function. "
            "Format: [{\"criticality\":\"error\",\"check\":{\"function\":\"name\",\"arguments\":{\"column\":\"col\"}}}] "
            "Example: [{\"criticality\":\"error\",\"check\":{\"function\":\"is_not_null\",\"arguments\":{\"column\":\"customer_id\"}}}]"
        )
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DQRuleGeneration(dspy.Module):
    """
    Generate data quality rules with improved JSON output reliability.

    This class provides functionality to generate data quality rules based on schema information,
    business descriptions, and available functions. It can optionally infer the schema from the
    business description if schema_info is not provided or is empty.
    """

    def __init__(self, schema_guesser: SchemaGuesser = SchemaGuesser()):
        """
        Initialize the DQ rule generation module.

        Args:
            schema_guesser (SchemaGuesser): Schema guesser instance for inferring schema when needed.
        """
        super().__init__()
        # Use Predict for reliable output
        self.generator = dspy.Predict(RuleSignature)
        self.schema_guesser = schema_guesser

    def forward(
        self, schema_info: str, business_description: str, available_functions: str
    ) -> dspy.primitives.prediction.Prediction:
        """
        Generate data quality rules based on schema information, business descriptions, and available functions.

        If schema_info is empty and enable_schema_inference is True, it will first use SchemaGuesser
        to infer the schema from the business description.

        Args:
            schema_info (str): JSON string containing table schema with column names, types, and sample data.
                              If empty and enable_schema_inference=True, schema will be inferred.
            business_description (str): Natural language description of data quality requirements.
            available_functions (str): JSON string of available DQX check functions.

        Returns:
            dspy.primitives.prediction.Prediction: A Prediction object containing the generated data quality rules,
            reasoning, and optionally guessed_schema_json and assumptions_bullets if schema was inferred.
        """
        # Step 1: Infer schema if needed
        guessed_schema_json = None
        assumptions_bullets = None

        if not schema_info or not schema_info.strip():
            logger.info("Inferring schema from business description...")
            schema_result = self.schema_guesser(business_description=business_description)
            schema_info = schema_result.guessed_schema_json
            guessed_schema_json = schema_result.guessed_schema_json
            assumptions_bullets = schema_result.assumptions_bullets
            logger.info(f"Inferred schema: {schema_info}")
        else:
            logger.debug(f"Using provided schema: {schema_info}")

        # Step 2: Generate rules using the schema (provided or inferred)
        result = self.generator(
            schema_info=schema_info, business_description=business_description, available_functions=available_functions
        )

        # Validate and clean the JSON output
        if result.quality_rules:
            try:
                # Try to parse the JSON to ensure it's valid
                json.loads(result.quality_rules)
            except json.JSONDecodeError as e:
                logger.warning(f"Generated invalid JSON: {e}. Raw output: {result.quality_rules}")
                # Return a fallback empty array if JSON is invalid
                result.quality_rules = "[]"

        # Add schema inference results to the prediction if they exist
        if guessed_schema_json:
            result.guessed_schema_json = guessed_schema_json
            result.assumptions_bullets = assumptions_bullets
            result.schema_info = schema_info

            # Enhance reasoning to show that schema was inferred
            original_reasoning = result.reasoning if hasattr(result, 'reasoning') else ""
            result.reasoning = (
                f"[Schema Inference] The schema was automatically inferred from the question:\n"
                f"{guessed_schema_json}\n\n"
                f"Assumptions made:\n{assumptions_bullets}\n\n"
                f"[Rule Generation] {original_reasoning}"
            )

        return result


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


def validate_generated_rules(actual: str) -> float:
    """
    Validate generated rules with granular scoring for better optimizer feedback.

    Scoring breakdown:
        - JSON parsing (40%): Checks if the actual output can be parsed as valid JSON.
        - Rules validation (60%): Ensures the rules pass DQX validation checks.

    Args:
        actual (str): JSON string of the actual generated rules.

    Returns:
        float: A score between 0.0 and 1.0 representing the quality of the generated rules.
    """
    total_score = 0.0

    # Score weights
    json_weight = 0.2
    rule_weight = 0.8

    # Json parsing score (40%)
    try:
        actual_rules = json.loads(actual)
        total_score += json_weight
        logger.debug(f"✓ JSON parsing successful (+{json_weight:.1f})")
    except json.JSONDecodeError as e:
        logger.warning(f"✗ JSON parsing failed: {e}")
        logger.debug(f"  Raw output: {repr(actual[:200])}")
        # Early return if we can't parse JSON at all
        return total_score

    # Rules validation score (60%)
    validation_status = DQEngineCore.validate_checks(actual_rules)
    if not validation_status.has_errors:
        total_score += rule_weight
        logger.debug(f"✓ Rules validation passed (+{rule_weight:.1f})")
    else:
        logger.warning(f"✗ Rules validation errors: {validation_status.errors}")

    logger.debug(f"Final score: {total_score:.2f}")
    return total_score


def get_dspy_compiler(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct",
    api_key: str = "",
    api_base: str = "",
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
    dq_model = DQRuleGeneration()
    train_set = create_optimizer_training_set()

    # Standard metric for JSON output validation
    def json_metric(_example, pred, _trace=None):
        if hasattr(pred, 'quality_rules'):
            return validate_generated_rules(pred.quality_rules)
        return 0.0

    optimizer = dspy.BootstrapFewShot(
        metric=json_metric,
        max_bootstrapped_demos=3,
        max_labeled_demos=5,
        teacher_settings={},
    )

    optimized_model = optimizer.compile(dq_model, trainset=train_set)
    return optimized_model
