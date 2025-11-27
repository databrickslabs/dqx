import json
import logging
from collections.abc import Callable
from typing import Any

import dspy  # type: ignore
from pyspark.sql import SparkSession
from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_core import LLMRuleCompiler
from databricks.labs.dqx.llm.llm_pk_detector import PrimaryKeyDetector
from databricks.labs.dqx.llm.llm_utils import get_required_check_functions_definitions

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
        custom_check_functions: dict[str, Callable] | None = None,
    ):
        """
        Initialize the LLM engine.

        Args:
            model_config: Configuration for the LLM model.
            custom_check_functions: Optional custom check functions to include.
        """
        self._available_check_functions = json.dumps(get_required_check_functions_definitions(custom_check_functions))

        self._llm_compiler = LLMRuleCompiler(
            model_config=model_config,
            custom_check_functions=custom_check_functions,
        )

        logger.info(f"LLM engine initialized with model: {model_config.model_name}")

    def detect_business_rules_with_llm(
        self, user_input: str, schema_info: str = ""
    ) -> dspy.primitives.prediction.Prediction:
        """
        Detect DQX rules based on natural language request with optional schema.

        If schema_info is empty (default), it will automatically infer the schema
        from the user_input before generating rules.

        Args:
            user_input: Natural language description of data quality requirements.
            schema_info: Optional JSON string containing table schema.
                        If empty (default), triggers schema inference.

        Returns:
            A Prediction object containing:
                - quality_rules: The generated DQ rules
                - reasoning: Explanation of the rules
                - guessed_schema_json: The inferred schema (if schema was inferred)
                - assumptions_bullets: Assumptions made (if schema was inferred)
                - schema_info: The final schema used (if schema was inferred)
        """
        return self._llm_compiler.model(
            schema_info=schema_info,
            business_description=user_input,
            available_functions=self._available_check_functions,
        )

    def detect_primary_keys_with_llm(
        self,
        spark: SparkSession,
        table: str,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Detects primary keys using LLM-based analysis.

        This method analyzes table schema and metadata to identify primary key columns.

        Args:
            spark: Spark session to access the table.
            table: The table name to analyze.
            max_retries: Maximum number of retries to find unique primary key combination.

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

        Raises:
            MissingParameterError: If LLM engine is not available or input_config is missing.
        """
        detector = PrimaryKeyDetector(
            table=table,
            spark_session=spark,
            max_retries=max_retries,
        )

        result = detector.detect_primary_keys_with_llm()

        logger.info(f"Primary key detection completed for {table}: {result.get('success', False)}")
        return result
