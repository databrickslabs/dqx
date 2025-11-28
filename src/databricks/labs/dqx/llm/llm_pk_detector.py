import logging
from typing import Any

import dspy  # type: ignore

from pyspark.sql import SparkSession

from databricks.labs.dqx.llm.llm_utils import TableManager

logger = logging.getLogger(__name__)


class DspPrimaryKeyDetectionSignature(dspy.Signature):
    """Analyze table schema and metadata step-by-step to identify the most likely primary key columns."""

    table: str = dspy.InputField(desc="Fully qualified table name")
    table_definition: str = dspy.InputField(desc="Complete table schema definition")
    context: str = dspy.InputField(desc="Context about similar tables or patterns")
    previous_attempts: str = dspy.InputField(desc="Previous failed attempts and why they failed")
    metadata_info: str = dspy.InputField(desc="Table metadata and column statistics to aid analysis")

    primary_key_columns: str = dspy.OutputField(desc="Comma-separated list of primary key columns")
    confidence: str = dspy.OutputField(desc="Confidence level: high, medium, or low")
    reasoning: str = dspy.OutputField(desc="Step-by-step reasoning for the selection based on metadata analysis")


class LLMPrimaryKeyDetector:
    """
    The Primary Key Detector uses DSPy to analyze table metadata.
    The initialization of the DSPy model should be done prior to instantiating this class.
    """

    def __init__(
        self,
        spark: SparkSession | None = None,
        show_live_reasoning: bool = True,
        max_retries: int = 3,
    ):
        self.show_live_reasoning = show_live_reasoning
        self.max_retries = max_retries
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark
        self.table_manager = TableManager(spark=self.spark)
        # Dspy model should be configured before
        self.detector = dspy.ChainOfThought(DspPrimaryKeyDetectionSignature)

    def detect_primary_keys_with_llm(self, table: str, context: str = "") -> dict[str, Any]:
        """
        Detect primary keys for tables and views.

        Args:
            table: The fully qualified table name to analyze.
            context: Optional context about similar tables or patterns.

        Returns:
            A dictionary containing the primary key detection result.
        """
        logger.info(f"Starting primary key detection for table: {table}")
        result = self._detect_primary_keys_from_table(table, context)
        self.print_pk_detection_summary(result)
        return result

    def _detect_primary_keys_from_table(self, table: str, context: str) -> dict[str, Any]:
        """Detect primary keys from a registered table or view."""
        try:
            columns = self.table_manager.get_table_column_names(table)
            table_definition = self.table_manager.get_table_definition(table)
            metadata_info = self.table_manager.get_table_metadata_info(table)
        except Exception as e:
            logger.error(f"Unexpected error during table metadata retrieval: {e}")
            return {
                "table": table,
                "success": False,
                "primary_key_columns": [],
                "error": f"Unexpected error retrieving table metadata: {str(e)}",
                "retries_attempted": 0,
            }

        return self._predict_with_retry_logic(
            table,
            columns,
            table_definition,
            context,
            metadata_info,
        )

    def check_duplicates(
        self,
        table: str,
        pk_columns: list[str],
    ) -> tuple[bool, int]:
        """Check for duplicates using Spark SQL GROUP BY and HAVING."""
        try:
            has_duplicates, duplicate_count, duplicates_df = self._execute_duplicate_check_query(table, pk_columns)
            self._report_duplicate_results(has_duplicates, duplicate_count, pk_columns, duplicates_df)
            return has_duplicates, duplicate_count
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return False, 0

    def _execute_duplicate_check_query(self, table: str, pk_columns: list[str]) -> tuple[bool, int, Any]:
        """Execute the duplicate check query and return results."""
        pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
        logger.debug(f"🔍 Checking for duplicates in {table} using columns: {pk_cols_str}")

        # Treats nulls as NOT distinct (NULL and NULL is considered equal)
        duplicate_query = f"""
        SELECT {pk_cols_str}, COUNT(*) as duplicate_count
        FROM {table}
        GROUP BY {pk_cols_str}
        HAVING COUNT(*) > 1
        """

        duplicate_result = self.spark.sql(duplicate_query)
        duplicates_df = duplicate_result.toPandas()
        return len(duplicates_df) > 0, len(duplicates_df), duplicates_df

    @staticmethod
    def _report_duplicate_results(
        has_duplicates: bool, duplicate_count: int, pk_columns: list[str], duplicates_df=None
    ):
        """Report the results of duplicate checking."""
        if has_duplicates and duplicates_df is not None:
            total_duplicate_records = duplicates_df["duplicate_count"].sum()
            logger.warning(
                f"Found {duplicate_count} duplicate key combinations for: {', '.join(pk_columns)} affecting {total_duplicate_records} total records"
            )
            if len(duplicates_df) > 0:
                logger.warning("Sample duplicates:")
                logger.warning(duplicates_df.head().to_string(index=False))
        else:
            logger.info(f"✅ No duplicates found for predicted primary key: {', '.join(pk_columns)}")

    def _check_duplicates_and_update_result(self, table: str, pk_columns: list[str], result: dict) -> tuple[bool, int]:
        """Check for duplicates and update result with validation info."""
        has_duplicates, duplicate_count = self.check_duplicates(table, pk_columns)

        result["has_duplicates"] = has_duplicates
        result["duplicate_count"] = duplicate_count

        return has_duplicates, duplicate_count

    def _handle_duplicates_found(
        self,
        pk_columns: list[str],
        duplicate_count: int,
        attempt: int,
        result: dict,
        all_attempts: list,
        previous_attempts: str,
    ) -> tuple[dict, str, bool]:
        """Handle case where duplicates are found."""
        logger.info(f"Found {duplicate_count} duplicate groups - Retrying with enhanced context")

        if attempt < self.max_retries:
            failed_pk = ", ".join(pk_columns)
            previous_attempts += (
                f"\nAttempt {attempt + 1}: Tried [{failed_pk}] but found {duplicate_count} "
                f"duplicate key combinations. "
            )
            previous_attempts += (
                "This indicates the combination is not unique enough. Need to find additional columns "
                "or a different combination that ensures complete uniqueness. "
            )
            previous_attempts += (
                "Consider adding timestamp fields, sequence numbers, or other differentiating columns "
                "that would make each row unique."
            )
            return result, previous_attempts, False  # Continue retrying

        logger.info(f"Maximum retries ({self.max_retries}) reached. Primary key validation failed.")

        # Always fail when duplicates are found - primary keys must be unique
        result["success"] = False
        result["error"] = (
            f"Primary key validation failed: Found {duplicate_count} duplicate combinations "
            f"in suggested columns {pk_columns} after {self.max_retries} retry attempts"
        )
        result["retries_attempted"] = attempt
        result["all_attempts"] = all_attempts
        result["final_status"] = "max_retries_reached_with_duplicates"

        return result, previous_attempts, True  # Stop retrying, max attempts reached

    @staticmethod
    def _handle_successful_validation(
        result: dict, attempt: int, all_attempts: list, previous_attempts: str
    ) -> tuple[dict, str, bool]:
        """Handle successful validation (no duplicates found)."""
        logger.info("No duplicates found - Primary key prediction validated!")
        result["retries_attempted"] = attempt
        result["all_attempts"] = all_attempts
        result["final_status"] = "success"

        return result, previous_attempts, True  # Success, stop retrying

    @staticmethod
    def _handle_validation_error(
        error: Exception, result: dict, attempt: int, all_attempts: list, previous_attempts: str
    ) -> tuple[dict, str, bool]:
        """Handle validation errors."""
        logger.error(f"Error during duplicate validation: {error}")
        result["validation_error"] = str(error)
        result["retries_attempted"] = attempt
        result["all_attempts"] = all_attempts
        result["final_status"] = "validation_error"

        return result, previous_attempts, True  # Stop retrying due to error

    def _validate_pk_duplicates(
        self,
        table: str,
        pk_columns: list[str],
        result: dict,
        attempt: int,
        all_attempts: list,
        previous_attempts: str,
    ) -> tuple[dict, str, bool]:
        """Validate primary key for duplicates and handle retry logic."""
        try:
            has_duplicates, duplicate_count = self._check_duplicates_and_update_result(table, pk_columns, result)

            if not has_duplicates:
                return self._handle_successful_validation(result, attempt, all_attempts, previous_attempts)

            return self._handle_duplicates_found(
                pk_columns, duplicate_count, attempt, result, all_attempts, previous_attempts
            )
        except Exception as e:
            return self._handle_validation_error(e, result, attempt, all_attempts, previous_attempts)

    def _execute_single_prediction(
        self, table: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> dict[str, Any]:
        """Execute a single prediction with the LLM."""
        if self.show_live_reasoning:
            with dspy.context(show_guidelines=True):
                logger.info("AI is analyzing metadata step by step...")
                result = self.detector(
                    table=table,
                    table_definition=table_definition,
                    context=context,
                    previous_attempts=previous_attempts,
                    metadata_info=metadata_info,
                )
        else:
            result = self.detector(
                table=table,
                table_definition=table_definition,
                context=context,
                previous_attempts=previous_attempts,
                metadata_info=metadata_info,
            )

        pk_columns = [col.strip() for col in result.primary_key_columns.split(",")]

        final_result = {
            "table": table,
            "primary_key_columns": pk_columns,
            "confidence": result.confidence,
            "reasoning": result.reasoning,
            "success": True,
        }

        logger.info(f"Primary Key: {', '.join(pk_columns)}")
        logger.info(f"Confidence: {result.confidence}")

        return final_result

    def _predict_with_retry_logic(
        self,
        table: str,
        columns: list[str],
        table_definition: str,
        context: str,
        metadata_info: str,
    ) -> dict[str, Any]:
        """Handle prediction with retry logic for duplicate validation."""

        previous_attempts = ""
        all_attempts = []

        for attempt in range(self.max_retries + 1):
            logger.info(f"Prediction attempt {attempt + 1}/{self.max_retries + 1}")

            result = self._single_prediction(table, table_definition, context, previous_attempts, metadata_info)

            if not result["success"]:
                return result

            all_attempts.append(result.copy())
            pk_columns = result["primary_key_columns"]

            invalid_columns = [col for col in pk_columns if col not in columns]
            if invalid_columns:
                error_msg = f"Predicted columns do not exist in table: {', '.join(invalid_columns)}"
                result["primary_key_columns"] = []
                result["success"] = False
                result["error"] = error_msg
                result["retries_attempted"] = attempt
                result["all_attempts"] = all_attempts
                result["final_status"] = "invalid_columns"
                return result

            logger.info("Validating primary key prediction...")
            result, previous_attempts, should_stop = self._validate_pk_duplicates(
                table, pk_columns, result, attempt, all_attempts, previous_attempts
            )

            if should_stop:
                return result

        # This shouldn't be reached, but just in case
        return all_attempts[-1] if all_attempts else {"table": table, "success": False, "error": "No attempts made"}

    def _single_prediction(
        self, table: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> dict[str, Any]:
        """Make a single primary key prediction using metadata."""

        logger.info("Analyzing table schema and metadata patterns...")

        try:
            final_result = self._execute_single_prediction(
                table, table_definition, context, previous_attempts, metadata_info
            )

            # Print reasoning if available
            if "reasoning" in final_result:
                self._print_reasoning_formatted(final_result["reasoning"])

            self._print_trace_if_available()

            return final_result
        except Exception as e:
            error_msg = f"Error during prediction: {str(e)}"
            logger.error(error_msg)
            logger.debug("Full traceback:", exc_info=True)
            return {"table": table, "success": False, "error": error_msg}

    @staticmethod
    def _print_reasoning_formatted(reasoning):
        """Format and print reasoning step by step."""
        if not reasoning:
            logger.debug("No reasoning provided")
            return

        lines = reasoning.split("\n")
        step_counter = 1

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.lower().startswith("step"):
                logger.debug(f"📝 {line}")
            elif line.startswith("-") or line.startswith("•"):
                logger.debug(f"   {line}")
            elif len(line) > 10 and any(
                word in line.lower() for word in ("analyze", "consider", "look", "notice", "think")
            ):
                logger.debug(f"📝 Step {step_counter}: {line}")
                step_counter += 1
            else:
                logger.debug(f"   {line}")

    @staticmethod
    def _print_trace_if_available():
        """Print DSPy trace if available."""
        try:
            if hasattr(dspy.settings, "trace") and dspy.settings.trace:
                logger.debug("\n🔬 TRACE INFORMATION:")
                logger.debug("-" * 60)
                for i, trace_item in enumerate(dspy.settings.trace[-3:]):
                    logger.debug(f"Trace {i+1}: {str(trace_item)[:200]}...")
        except (AttributeError, IndexError):
            # Silently continue if trace information is not available
            pass

    @staticmethod
    def print_pk_detection_summary(result):
        """Print summary based on result dictionary."""

        retries_attempted = result.get("retries_attempted", 0)

        logger.info("=" * 60)
        logger.info("🎯 PRIMARY KEY DETECTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Table: {result['table']}")
        logger.info(f"Status: {'✅ SUCCESS' if result['success'] else '❌ FAILED'}")
        logger.info(f"Attempts: {retries_attempted + 1}")
        if retries_attempted > 0:
            logger.info(f"Retries needed: {retries_attempted}")
        logger.info("")

        if not result["success"]:
            return

        logger.info("📋 FINAL PRIMARY KEY:")
        for col in result["primary_key_columns"]:
            logger.info(f"   • {col}")
        logger.info("")

        logger.info(f"🎯 Confidence: {result['confidence'].upper()}")

        validation_msg = (
            "No duplicates found"
            if not result.get("has_duplicates", True)
            else f"Found {result.get('duplicate_count', 0)} duplicates"
        )
        logger.info(f"🔍 Validation: {validation_msg}")
        logger.info("")

        if result.get("all_attempts", None) and len(result["all_attempts"]) > 1:
            logger.info("📝 ATTEMPT HISTORY:")
            for i, attempt in enumerate(result["all_attempts"]):
                cols_str = ", ".join(attempt["primary_key_columns"])
                if i == 0:
                    logger.info(f"   1st attempt: {cols_str} → Found duplicates")
                else:
                    attempt_num = i + 1
                    suffix = "nd" if attempt_num == 2 else "rd" if attempt_num == 3 else "th"
                    status_msg = "Success!" if i == len(result["all_attempts"]) - 1 else "Still had duplicates"
                    logger.info(f"   {attempt_num}{suffix} attempt: {cols_str} → {status_msg}")
            logger.info("")

        status = result.get("final_status", "unknown")
        if status == "success":
            logger.info("✅ RECOMMENDATION: Use as a primary key")
        elif status == "max_retries_reached_with_duplicates":
            logger.info("⚠️  RECOMMENDATION: Manual review needed - duplicates persist")
        else:
            logger.info(f"ℹ️  STATUS: {status}")

        logger.info("=" * 60)
