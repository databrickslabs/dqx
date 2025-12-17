import json
import logging
from collections.abc import Callable
from typing import Any

import dspy  # type: ignore
from pyspark.sql import SparkSession
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator, LLMRuleCompiler
from databricks.labs.dqx.llm.llm_pk_detector import LLMPrimaryKeyDetector
from databricks.labs.dqx.llm.llm_utils import (
    get_required_check_functions_definitions,
    get_required_summary_stats,
    TableManager,
)

logger = logging.getLogger(__name__)


class DQLLMEngine:
    """
    High-level interface for LLM-based data quality rule generation.

    This class serves as a Facade pattern, providing a simple interface
    to the underlying complex LLM system.
    """

    def __init__(
        self,
        model_config: LLMModelConfig,
        spark: SparkSession | None = None,
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initialize the LLM engine.

        This class configures the DSPy model once and then creates components
        that rely on this global configuration.

        Args:
            model_config: Configuration for the LLM model.
            spark: Optional Spark session. If None, a new session is created.
            custom_check_functions: Optional custom check functions to include.
        """
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

        self._available_check_functions = json.dumps(get_required_check_functions_definitions(custom_check_functions))

        # Configure DSPy model once for all LLM components
        configurator = LLMModelConfigurator(model_config)
        configurator.configure()

        self._llm_rule_compiler = LLMRuleCompiler(custom_check_functions=custom_check_functions)
        self._llm_pk_detector = LLMPrimaryKeyDetector(table_manager=TableManager(spark=self.spark))

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

    def detect_primary_keys_with_llm(self, table: str) -> dict[str, Any]:
        """
        Detects primary keys using LLM-based analysis.

        This method analyzes table schema and metadata to identify primary key columns.

        Args:
            table: The table name to analyze.

        Returns:
            A dictionary containing the primary key detection result with the following keys:
            - table: The table name
            - success: Whether detection was successful
            - primary_key_columns: List of detected primary key columns (if successful)
            - confidence: Confidence level (high/medium/low)
            - reasoning: LLM reasoning for the selection
            - has_duplicates: Whether duplicates were found (if validation performed)
            - duplicate_count: Number of duplicate combinations (if validation performed)
            - error: Error message (if failed)
        """
        return self._llm_pk_detector.detect_primary_keys_with_llm(table=table)
