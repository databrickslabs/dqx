import logging
from typing import Any

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
            spark: Optional Spark session. If not provided, a new session will be created on first use.
            detector: Optional primary key detector. If None, one is created on first use using *spark*.
        """
        self._spark = spark
        self._configurator = LLMModelConfigurator(model_config)
        self._detector = detector

    @property
    def spark(self) -> SparkSession:
        """
        Gets a Spark session. Gets an available one or creates a new one if none was provided.

        Returns:
            Spark session instance.
        """
        if self._spark is None:
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    @property
    def detector(self) -> LLMPrimaryKeyDetector:
        """
        Gets the primary key detector, creating one on first use if none was provided.

        Resolved lazily so that constructing the engine does not require a Spark session before any
        detection is actually requested.

        Returns:
            Primary key detector instance.
        """
        if self._detector is None:
            self._detector = LLMPrimaryKeyDetector(table_manager=TableManager(spark=self.spark))
        return self._detector

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
        with self._configurator.lm_context():
            return self.detector.detect_primary_keys_with_llm(table=table)
