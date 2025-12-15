import json
import logging
from collections.abc import Callable
from functools import cached_property

import dspy  # type: ignore

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_utils import create_optimizer_training_set, create_optimizer_training_set_with_stats
from databricks.labs.dqx.llm.optimizers import BootstrapFewShotOptimizer
from databricks.labs.dqx.llm.validators import RuleValidator

logger = logging.getLogger(__name__)


class LLMModelConfigurator:
    """
    Configures DSPy language models.
    """

    def __init__(self, model_config: LLMModelConfig):
        """
        Initialize model configurator.

        Args:
            model_config: Configuration for the LLM model.
        """
        self._model_config = model_config

    def configure(self) -> None:
        """Configure the DSPy language model with the provided settings."""
        language_model = dspy.LM(
            model=self._model_config.model_name,
            model_type="chat",
            api_key=self._model_config.api_key or "",
            api_base=self._model_config.api_base or "",
            max_retries=3,
        )
        dspy.configure(lm=language_model)
        logger.info(f"Configured DSPy model: {self._model_config.model_name}")


class DspySchemaGuesserSignature(dspy.Signature):
    """Guess a table schema based on business description."""

    business_description: str = dspy.InputField(
        desc=(
            "Natural language summary of the dataset and its use. "
            "Including some column hints (eg. id, amount, status, email, dates)."
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
            "Concise bullet list (1-6 lines) of assumptions made about columns, types, and examples. "
            "Keep each bullet short."
        )
    )


class DspySchemaGuesser(dspy.Module):
    """Guess table schema from business description."""

    def __init__(self):
        super().__init__()
        self.guess = dspy.ChainOfThought(DspySchemaGuesserSignature)

    def forward(self, business_description: str) -> dspy.primitives.prediction.Prediction:
        """
        Guess schema based on business description.

        Args:
            business_description: Natural language description of the dataset.

        Returns:
            Prediction containing guessed schema and assumptions.
        """
        return self.guess(business_description=business_description)


class DspyRuleSignature(dspy.Signature):
    """Generate data quality rules with improved output format."""

    schema_info: str = dspy.InputField(desc="JSON string of table schema with column names, types, and sample data")
    business_description: str = dspy.InputField(desc="Natural language description of data quality requirements")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc=(
            "Return a valid JSON array of data quality rules. Use double quotes for JSON syntax. "
            "For string literal values in check arguments (eg. value or limit parameter), wrap them in single quotes. "
            "In SQL filter expressions, use single quotes for string literals and capitalize SQL keywords. "
            "Criticality can be error or warn. "
            "Filter may be used to apply the rule to the relevant records only. "
            "Check function name and doc to select the appropriate check function. "
            "Format: [{\"criticality\":\"error\",\"check\":{\"function\":\"name\",\"arguments\":{\"column\":\"col\"}},\"filter\":\"expression\"}] "
            "Example: [{\"criticality\":\"error\",\"check\":{\"function\":\"is_not_null\",\"arguments\":{\"column\":\"customer_id\"}},\"filter\":\"customer_name is not null\"}]"
        )
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DspyRuleGeneration(dspy.Module):
    """
    Generate data quality rules.

    Now focused solely on rule generation, with schema inference delegated.
    """

    def __init__(self, generator: dspy.Predict | None = None):
        super().__init__()
        self.generator = generator or dspy.Predict(DspyRuleSignature)

    def forward(
        self, schema_info: str, business_description: str, available_functions: str
    ) -> dspy.primitives.prediction.Prediction:
        """
        Generate data quality rules.

        Args:
            schema_info: JSON string containing table schema.
            business_description: Natural language description of requirements.
            available_functions: JSON string of available check functions.

        Returns:
            Prediction containing quality_rules and reasoning.
        """
        result = self.generator(
            schema_info=schema_info, business_description=business_description, available_functions=available_functions
        )

        # Validate JSON output
        if result.quality_rules:
            try:
                json.loads(result.quality_rules)
            except json.JSONDecodeError as e:
                logger.warning(f"Generated invalid JSON: {e}. Returning empty rules.")
                result.quality_rules = "[]"

        return result


class DspyRuleGenerationWithSchemaInference(dspy.Module):
    """
    Combines schema inference and rule generation.

    Follows Dependency Inversion Principle by depending on abstractions (protocols).
    """

    def __init__(self, generator: DspyRuleGeneration | None = None, schema_inferrer: DspySchemaGuesser | None = None):
        super().__init__()

        self.generator = generator or DspyRuleGeneration()
        self.schema_inferrer = schema_inferrer or DspySchemaGuesser()

    def forward(
        self, schema_info: str, business_description: str, available_functions: str
    ) -> dspy.primitives.prediction.Prediction:
        """
        Generate rules with optional schema inference.

        Args:
            schema_info: JSON string of schema (can be empty to trigger inference).
            business_description: Natural language requirements.
            available_functions: JSON string of available functions.

        Returns:
            Prediction with quality_rules, reasoning, and optional schema info.
        """
        guessed_schema_json = None
        assumptions_bullets = None

        # Infer schema if not provided
        if not schema_info or not schema_info.strip():
            logger.info("Inferring schema from business description...")
            schema_result = self.schema_inferrer.forward(business_description)
            schema_info = schema_result.guessed_schema_json
            guessed_schema_json = schema_result.guessed_schema_json
            assumptions_bullets = schema_result.assumptions_bullets
            logger.info(f"Inferred schema: {schema_info}")

        # Generate rules
        result = self.generator(
            schema_info=schema_info, business_description=business_description, available_functions=available_functions
        )

        # Add schema inference results if present
        if guessed_schema_json:
            result.guessed_schema_json = guessed_schema_json
            result.assumptions_bullets = assumptions_bullets
            result.schema_info = schema_info

            # Enhance reasoning
            original_reasoning = result.reasoning if hasattr(result, "reasoning") else ""
            result.reasoning = (
                f"[Schema Inference] The schema was automatically inferred:\n"
                f"{guessed_schema_json}\n\n"
                f"Assumptions:\n{assumptions_bullets}\n\n"
                f"[Rule Generation] {original_reasoning}"
            )

        return result


class DspyRuleUsingDataStatsSignature(dspy.Signature):
    """Generate data quality rules using data summary statistics."""

    business_description: str | None = dspy.InputField(
        desc="Optional natural language description of data quality requirements"
    )
    data_summary_stats: str = dspy.InputField(desc="JSON string of summary statistics of the data")
    available_functions: str = dspy.InputField(desc="JSON string of available DQX check functions")
    quality_rules: str = dspy.OutputField(
        desc=(
            "Return a valid JSON array of data quality rules. Use double quotes for JSON syntax. "
            "For string literal values in check arguments (eg. value or limit parameter), wrap them in single quotes. "
            "In SQL filter expressions, use single quotes for string literals and capitalize SQL keywords. "
            "Criticality can be error or warn. "
            "Filter may be used to apply the rule to the relevant records only. "
            "Check function name and doc to select the appropriate check function. "
            "Format: [{\"criticality\":\"error\",\"check\":{\"function\":\"name\",\"arguments\":{\"column\":\"col\"}},\"filter\":\"expression\"}] "
            "Example: [{\"criticality\":\"error\",\"check\":{\"function\":\"is_not_null\",\"arguments\":{\"column\":\"customer_id\"}},\"filter\":\"customer_name is not null\"}]"
        )
    )
    reasoning: str = dspy.OutputField(desc="Explanation of why these rules were chosen")


class DspyRuleUsingDataStats(dspy.Module):
    """
    Generate data quality rules using data summary statistics.
    """

    def __init__(self, generator: dspy.Predict | None = None):
        super().__init__()
        self.generator = generator or dspy.Predict(DspyRuleUsingDataStatsSignature)

    def forward(
        self, data_summary_stats: str, available_functions: str, business_description: str | None = None
    ) -> dspy.primitives.prediction.Prediction:
        """
        Generate data quality rules.

        Args:
            data_summary_stats: JSON string containing summary statistics of the data.
            available_functions: JSON string of available check functions.
            business_description: Optional natural language description of data quality requirements.

        Returns:
            Prediction containing quality_rules and reasoning.
        """
        result = self.generator(
            data_summary_stats=data_summary_stats,
            available_functions=available_functions,
            business_description=business_description or "",
        )

        # Validate JSON output
        if result.quality_rules:
            try:
                json.loads(result.quality_rules)
            except json.JSONDecodeError as e:
                logger.warning(f"Generated invalid JSON: {e}. Returning empty rules.")
                result.quality_rules = "[]"

        return result


class LLMRuleCompiler:
    """
    Compiles and optimizes LLM-based data quality rules.

    Note: This class assumes DSPy is already configured with a language model.
    The configuration should be done externally before instantiating this class.
    """

    def __init__(
        self,
        custom_check_functions: dict[str, Callable] | None = None,
        rule_validator: RuleValidator | None = None,
        optimizer: BootstrapFewShotOptimizer | None = None,
    ):
        """
        Initialize the rule compiler.

        Note: DSPy must be configured before creating this instance.

        Args:
            custom_check_functions: Optional custom check functions.
            rule_validator: Optional rule validator instance.
            optimizer: Optional optimizer instance.
        """
        self._custom_check_functions = custom_check_functions
        self._optimizer = optimizer or BootstrapFewShotOptimizer()
        self._rule_validator = rule_validator or RuleValidator(custom_check_functions)
        self._dq_model = DspyRuleGenerationWithSchemaInference()
        self._dq_model_using_data_stats = DspyRuleUsingDataStats()

    @cached_property
    def model(self) -> dspy.Module:
        """
        Get the optimized DSPy model.

        Returns:
            Optimized DSPy module for generating data quality rules.
        """
        train_set = create_optimizer_training_set(self._custom_check_functions)

        def metric(_example, pred, _trace=None):
            """Metric function for optimization."""
            if hasattr(pred, "quality_rules"):
                return self._rule_validator.validate(pred.quality_rules)
            return 0.0

        optimized_model = self._optimizer.compile(self._dq_model, train_set, metric)
        return optimized_model

    @cached_property
    def model_using_data_stats(self) -> dspy.Module:
        """
        Get the optimized DSPy model for generating rules from data summary statistics.

        Returns:
            Optimized DSPy module for generating data quality rules from data stats.
        """
        train_set = create_optimizer_training_set_with_stats(self._custom_check_functions)

        def metric(_example, pred, _trace=None):
            """Metric function for optimization."""
            if hasattr(pred, "quality_rules"):
                return self._rule_validator.validate(pred.quality_rules)
            return 0.0

        optimized_model = self._optimizer.compile(self._dq_model_using_data_stats, train_set, metric)
        return optimized_model
