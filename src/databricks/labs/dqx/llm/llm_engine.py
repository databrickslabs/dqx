import json
import logging
from collections.abc import Callable
from typing import Any

import dspy  # type: ignore
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator, LLMRuleCompiler
from databricks.labs.dqx.llm.llm_utils import (
    get_required_check_functions_definitions,
    get_required_summary_stats,
)

logger = logging.getLogger(__name__)


class DQLLMEngine:
    """
    High-level interface for LLM-based data quality rule generation.

    This class serves as a Facade pattern, providing a simple interface
    to the underlying complex LLM system.

    Note:
        For LLM-based primary key detection, which scans the data to verify uniqueness and
        requires a Spark session, use *DQLLMPrimaryKeyEngine* instead.
    """

    def __init__(
        self,
        model_config: LLMModelConfig,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initializes the LLM engine.

        Args:
            model_config: Configuration for the LLM model.
            custom_check_functions: Optional custom check functions to include.
        """
        self._available_check_functions = json.dumps(get_required_check_functions_definitions(custom_check_functions))

        # Store configurator for creating per-request LM instances with current token
        # We do NOT call configure() - each request uses context() with the current token
        self._configurator = LLMModelConfigurator(model_config)
        self._llm_rule_compiler = LLMRuleCompiler(custom_check_functions=custom_check_functions)

    def detect_business_rules_with_llm(
        self, user_input: str = "", schema_info: str = "", summary_stats: dict[str, Any] | None = None
    ) -> dspy.primitives.prediction.Prediction:
        """
        Detect DQX rules based on natural language request with optional schema or summary statistics.

        If schema_info is empty (default), it will automatically infer the schema
        from the user_input before generating rules.

        Args:
            user_input: Optional natural language description of data quality requirements.
            schema_info: Optional JSON string containing table schema.
                        If empty (default), triggers schema inference.
            summary_stats: Optional dictionary containing summary statistics of the input data.

        Returns:
            A Prediction object containing:
                - quality_rules: The generated DQ rules
                - reasoning: Explanation of the rules
                - guessed_schema_json: The inferred schema (if schema was inferred)
                - assumptions_bullets: Assumptions made (if schema was inferred)
                - schema_info: The final schema used (if schema was inferred)
        """
        with self._configurator.lm_context():
            if summary_stats is not None:
                return self._llm_rule_compiler.model_using_data_stats(
                    business_description=user_input or None,
                    data_summary_stats=json.dumps(get_required_summary_stats(summary_stats=summary_stats)),
                    available_functions=self._available_check_functions,
                )
            return self._llm_rule_compiler.model(
                schema_info=schema_info,
                business_description=user_input,
                available_functions=self._available_check_functions,
            )
