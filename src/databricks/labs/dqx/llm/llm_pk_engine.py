import logging
from typing import Any

import dspy  # type: ignore
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import LLMModelConfig
from databricks.labs.dqx.llm.llm_core import LLMModelConfigurator
from databricks.labs.dqx.llm.llm_pk_detector import LLMPrimaryKeyDetector
from databricks.labs.dqx.table_manager import TableManager

logger = logging.getLogger(__name__)


class DQLLMPrimaryKeyEngine:
    """
    High-level interface for LLM-based primary key detection.

    Primary key detection inspects table metadata and scans the data to verify uniqueness, so it
    requires a Spark session. It is kept separate from *DQLLMEngine*, which generates quality rules
    from table metadata alone and needs no Spark session.
    """

    def __init__(
        self,
        model_config: LLMModelConfig,
        spark: SparkSession | None = None,
        detector: LLMPrimaryKeyDetector | None = None,
    ):
        """
        Initializes the primary key detection engine.

        Args:
            model_config: Configuration for the LLM model.
            spark: Optional Spark session. If None, a new session is created.
            detector: Optional primary key detector. If None, one is created using *spark*.
        """
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self._configurator = LLMModelConfigurator(model_config)
        self._llm_pk_detector = detector or LLMPrimaryKeyDetector(table_manager=TableManager(spark=self.spark))

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
        with dspy.settings.context(lm=self._configurator.create_lm()):
            return self._llm_pk_detector.detect_primary_keys_with_llm(table=table)
