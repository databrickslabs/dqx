"""Primary key detection using LLM analysis."""

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

import dspy  # type: ignore

from databricks.labs.dqx.llm.table_manager import TableManager

logger = logging.getLogger(__name__)


@dataclass
class PredictionResult:
    """Result from LLM primary key prediction."""

    table: str
    columns: list[str]
    confidence: str
    reasoning: str
    success: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "table": self.table,
            "primary_key_columns": self.columns,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "success": self.success,
        }


@dataclass
class ValidationResult:
    """Result from primary key validation."""

    valid: bool
    should_retry: bool = False
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableMetadata:
    """Metadata gathered from a table for PK detection."""

    table: str
    columns: list[str]
    definition: str
    info: str


class PrimaryKeyValidator(Protocol):
    """Protocol for primary key validation strategies."""

    def validate(
        self,
        table: str,
        pk_columns: list[str],
        table_columns: list[str],
        result: dict[str, Any],
    ) -> ValidationResult:
        """
        Validate primary key candidates.

        Args:
            table: Fully qualified table name.
            pk_columns: Predicted primary key columns.
            table_columns: All columns available in the table.
            result: Current detection result dictionary.

        Returns:
            ValidationResult indicating if validation passed.
        """


class ColumnExistenceValidator:
    """Validates that predicted columns exist in the table."""

    def validate(
        self,
        table: str,
        pk_columns: list[str],
        table_columns: list[str],
        _result: dict[str, Any],
    ) -> ValidationResult:
        """
        Check if all predicted columns exist in the table.

        Args:
            table: Fully qualified table name.
            pk_columns: Predicted primary key columns.
            table_columns: All columns available in the table.

        Returns:
            ValidationResult with valid=False if columns don't exist.
        """
        invalid_columns = [col for col in pk_columns if col not in table_columns]

        if invalid_columns:
            error_msg = f"Predicted columns do not exist in table: {', '.join(invalid_columns)}"
            logger.error(error_msg)
            return ValidationResult(
                valid=False, should_retry=False, error=error_msg, metadata={"invalid_columns": invalid_columns}
            )

        logger.debug(f"✓ All predicted columns exist in table {table}")
        return ValidationResult(valid=True)


class DuplicateValidator:
    """Validates that primary key combination is unique."""

    def __init__(self, table_manager: TableManager):
        """
        Initialize the duplicate validator.

        Args:
            table_manager: Manager for executing SQL queries on tables.
        """
        self.table_manager = table_manager

    def validate(
        self,
        table: str,
        pk_columns: list[str],
        _table_columns: list[str],
        result: dict[str, Any],
    ) -> ValidationResult:
        """
        Check if the primary key combination is unique (no duplicates).

        Args:
            table: Fully qualified table name.
            pk_columns: Predicted primary key columns.
            result: Current detection result dictionary.

        Returns:
            ValidationResult with valid=False if duplicates are found.
        """
        has_duplicates, duplicate_count = self._check_duplicates(table, pk_columns)

        # Update result with validation info
        result["has_duplicates"] = has_duplicates
        result["duplicate_count"] = duplicate_count

        if has_duplicates:
            logger.warning(f"Found {duplicate_count} duplicate key combinations for: {', '.join(pk_columns)}")
            return ValidationResult(
                valid=False,
                should_retry=True,
                error=f"Found {duplicate_count} duplicate combinations",
                metadata={"duplicate_count": duplicate_count, "has_duplicates": True},
            )

        logger.info(f"✅ No duplicates found for predicted primary key: {', '.join(pk_columns)}")
        return ValidationResult(valid=True, metadata={"has_duplicates": False, "duplicate_count": 0})

    def _check_duplicates(self, table: str, pk_columns: list[str]) -> tuple[bool, int]:
        """
        Execute duplicate check query.

        Args:
            table: Fully qualified table name.
            pk_columns: Primary key columns to check.

        Returns:
            Tuple of (has_duplicates, duplicate_count).
        """
        pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
        logger.debug(f"🔍 Checking for duplicates in {table} using columns: {pk_cols_str}")

        # Treats nulls as NOT distinct (NULL and NULL is considered equal)
        duplicate_query = f"""
        SELECT {pk_cols_str}, COUNT(*) as duplicate_count
        FROM {table}
        GROUP BY {pk_cols_str}
        HAVING COUNT(*) > 1
        """

        try:
            duplicate_result = self.table_manager.run_sql(duplicate_query)
            duplicates_df = duplicate_result.toPandas()
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return False, 0

        has_duplicates = len(duplicates_df) > 0
        duplicate_count = len(duplicates_df)

        if has_duplicates:
            total_duplicate_records = duplicates_df["duplicate_count"].sum()
            logger.warning(f"Sample duplicates affecting {total_duplicate_records} total records:")
            logger.warning(duplicates_df.head().to_string(index=False))

        return has_duplicates, duplicate_count


class ValidationChain:
    """Chains multiple validators together using Chain of Responsibility pattern."""

    def __init__(self, validators: list[PrimaryKeyValidator]):
        """
        Initialize the validation chain.

        Args:
            validators: List of validators to run in sequence.
        """
        self.validators = validators

    def validate_all(
        self,
        table: str,
        pk_columns: list[str],
        table_columns: list[str],
        result: dict[str, Any],
    ) -> ValidationResult:
        """
        Run all validators in sequence, stopping at first failure.

        Args:
            table: Fully qualified table name.
            pk_columns: Predicted primary key columns.
            table_columns: All columns available in the table.
            result: Current detection result dictionary.

        Returns:
            ValidationResult from first failing validator, or success if all pass.
        """
        logger.info("Validating primary key prediction...")

        for validator in self.validators:
            validation_result = validator.validate(table, pk_columns, table_columns, result)

            if not validation_result.valid:
                logger.debug(f"Validation failed: {validation_result.error}")
                return validation_result

        logger.debug("✓ All validations passed")
        return ValidationResult(valid=True)


class PrimaryKeyPredictor:
    """
    Handles LLM predictions for primary key detection.

    This class is responsible solely for interacting with the LLM (DSPy)
    and parsing the prediction results. It has no knowledge of validation
    or retry logic.
    """

    def __init__(self, detector: dspy.Module, show_live_reasoning: bool = True):
        """
        Initialize the predictor.

        Args:
            detector: DSPy module configured for primary key detection.
            show_live_reasoning: Whether to display live reasoning during prediction.
        """
        self.detector = detector
        self.show_live_reasoning = show_live_reasoning

    def predict(
        self,
        table: str,
        table_definition: str,
        context: str,
        previous_attempts: str,
        metadata_info: str,
    ) -> PredictionResult:
        """
        Execute a single prediction with the LLM.

        Args:
            table: Fully qualified table name.
            table_definition: Complete table schema definition.
            context: Context about similar tables or patterns.
            previous_attempts: Previous failed attempts and why they failed.
            metadata_info: Table metadata and column statistics.

        Returns:
            PredictionResult containing the predicted primary key columns.

        Raises:
            Exception: If prediction fails.
        """
        logger.info("Analyzing table schema and metadata patterns...")

        try:
            result = self._execute_prediction(table, table_definition, context, previous_attempts, metadata_info)

            pk_columns = [col.strip() for col in result.primary_key_columns.split(",")]

            logger.info(f"Primary Key: {', '.join(pk_columns)}")
            logger.info(f"Confidence: {result.confidence}")

            return PredictionResult(
                table=table,
                columns=pk_columns,
                confidence=result.confidence,
                reasoning=result.reasoning,
                success=True,
            )
        except Exception as e:
            error_msg = f"Error during prediction: {str(e)}"
            logger.error(error_msg)
            logger.debug("Full traceback:", exc_info=True)
            raise

    def _execute_prediction(
        self,
        table: str,
        table_definition: str,
        context: str,
        previous_attempts: str,
        metadata_info: str,
    ) -> Any:
        """Execute the DSPy prediction with optional live reasoning."""
        if self.show_live_reasoning:
            with dspy.context(show_guidelines=True):
                logger.info("AI is analyzing metadata step by step...")
                return self.detector(
                    table=table,
                    table_definition=table_definition,
                    context=context,
                    previous_attempts=previous_attempts,
                    metadata_info=metadata_info,
                )
        else:
            return self.detector(
                table=table,
                table_definition=table_definition,
                context=context,
                previous_attempts=previous_attempts,
                metadata_info=metadata_info,
            )


class RetryStrategy:
    """
    Manages retry logic and feedback generation for failed predictions.

    This class encapsulates the retry policy and generates contextual feedback
    for the LLM based on validation failures.
    """

    def __init__(self, max_retries: int = 3):
        """
        Initialize the retry strategy.

        Args:
            max_retries: Maximum number of retries allowed.
        """
        self.max_retries = max_retries

    def should_retry(self, attempt: int, validation_result: ValidationResult) -> bool:
        """
        Determine if we should retry based on attempt number and validation result.

        Args:
            attempt: Current attempt number (0-indexed).
            validation_result: Result from validation.

        Returns:
            True if we should retry, False otherwise.
        """
        if attempt >= self.max_retries:
            logger.info(f"Maximum retries ({self.max_retries}) reached")
            return False

        if not validation_result.should_retry:
            logger.info(f"Validation error is not retryable: {validation_result.error}")
            return False

        logger.info(f"Retrying (attempt {attempt + 1}/{self.max_retries})")
        return True

    def generate_feedback(
        self,
        attempt: int,
        pk_columns: list[str],
        validation_result: ValidationResult,
        previous_feedback: str,
    ) -> str:
        """
        Generate feedback for LLM based on validation failure.

        Args:
            attempt: Current attempt number (0-indexed).
            pk_columns: Columns that were predicted.
            validation_result: Result from validation.
            previous_feedback: Previous feedback string.

        Returns:
            Updated feedback string with new information.
        """
        failed_pk = ", ".join(pk_columns)
        feedback = previous_feedback

        feedback += f"\nAttempt {attempt + 1}: Tried [{failed_pk}] but {validation_result.error}. "

        if validation_result.should_retry:
            feedback += self._get_retry_hint(validation_result)

        logger.debug(f"Generated feedback for LLM: {feedback[-200:]}")  # Log last 200 chars
        return feedback

    def _get_retry_hint(self, validation_result: ValidationResult) -> str:
        """
        Generate contextual hints for the LLM based on the validation failure.

        Args:
            validation_result: Result from validation.

        Returns:
            Hint string to guide the LLM.
        """
        if "duplicate" in validation_result.error.lower():
            return (
                "This indicates the combination is not unique enough. "
                "Need to find additional columns or a different combination that ensures complete uniqueness. "
                "Consider adding timestamp fields, sequence numbers, or other differentiating columns "
                "that would make each row unique."
            )

        # Generic hint for other retryable errors
        return (
            "Please reconsider the primary key selection and try a different combination "
            "that would uniquely identify each row in the table."
        )


class DetectionResultBuilder:
    """
    Builds detection result dictionaries using the Builder pattern.

    This class provides a fluent API for constructing consistent result
    dictionaries with proper structure and all required fields.
    """

    def __init__(self, table: str):
        """
        Initialize the builder for a specific table.

        Args:
            table: Fully qualified table name.
        """
        self.result: dict[str, Any] = {"table": table, "success": False}
        self.attempts: list[dict[str, Any]] = []

    def with_success(self, pk_columns: list[str], confidence: str, reasoning: str) -> "DetectionResultBuilder":
        """
        Mark the detection as successful with primary key details.

        Args:
            pk_columns: Detected primary key columns.
            confidence: Confidence level (high/medium/low).
            reasoning: LLM reasoning for the selection.

        Returns:
            Self for method chaining.
        """
        self.result.update(
            {
                "success": True,
                "primary_key_columns": pk_columns,
                "confidence": confidence,
                "reasoning": reasoning,
            }
        )
        return self

    def with_validation(self, has_duplicates: bool, duplicate_count: int = 0) -> "DetectionResultBuilder":
        """
        Add validation results to the detection result.

        Args:
            has_duplicates: Whether duplicates were found.
            duplicate_count: Number of duplicate combinations found.

        Returns:
            Self for method chaining.
        """
        self.result.update({"has_duplicates": has_duplicates, "duplicate_count": duplicate_count})
        return self

    def with_error(self, error: str) -> "DetectionResultBuilder":
        """
        Mark the detection as failed with an error message.

        Args:
            error: Error message describing the failure.

        Returns:
            Self for method chaining.
        """
        self.result["error"] = error
        self.result["success"] = False
        return self

    def with_status(self, final_status: str) -> "DetectionResultBuilder":
        """
        Set the final status of the detection.

        Args:
            final_status: Status string (e.g., 'success', 'max_retries_reached', 'invalid_columns').

        Returns:
            Self for method chaining.
        """
        self.result["final_status"] = final_status
        return self

    def add_attempt(self, attempt_result: dict[str, Any]) -> "DetectionResultBuilder":
        """
        Add an attempt result to the history.

        Args:
            attempt_result: Dictionary containing attempt details.

        Returns:
            Self for method chaining.
        """
        self.attempts.append(attempt_result)
        return self

    def build(self) -> dict[str, Any]:
        """
        Build and return the final result dictionary.

        Returns:
            Complete detection result dictionary.
        """
        if self.attempts:
            self.result["all_attempts"] = self.attempts
            self.result["retries_attempted"] = len(self.attempts) - 1

        # Set final status if not already set
        if "final_status" not in self.result:
            if self.result["success"]:
                self.result["final_status"] = "success"
            elif len(self.attempts) > 1:
                self.result["final_status"] = "max_retries_reached"

        return self.result


class DetectionResultFormatter:
    """
    Formats and logs detection results for display.

    This class separates presentation logic from business logic,
    making it easy to test and swap formatters.
    """

    @staticmethod
    def print_summary(result: dict[str, Any]) -> None:
        """
        Print a formatted summary of the detection result.

        Args:
            result: Detection result dictionary.
        """
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
            if "error" in result:
                logger.error(f"Error: {result['error']}")
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

        if result.get("all_attempts") and len(result["all_attempts"]) > 1:
            DetectionResultFormatter._print_attempt_history(result["all_attempts"])

        status = result.get("final_status", "unknown")
        if status == "success":
            logger.info("✅ RECOMMENDATION: Use as a primary key")
        elif status == "max_retries_reached_with_duplicates":
            logger.info("⚠️  RECOMMENDATION: Manual review needed - duplicates persist")
        elif status == "max_retries_reached":
            logger.info("⚠️  RECOMMENDATION: Manual review needed - max retries reached")
        else:
            logger.info(f"ℹ️  STATUS: {status}")

        logger.info("=" * 60)

    @staticmethod
    def _print_attempt_history(attempts: list[dict[str, Any]]) -> None:
        """
        Print history of all attempts.

        Args:
            attempts: List of attempt result dictionaries.
        """
        logger.info("📝 ATTEMPT HISTORY:")
        for i, attempt in enumerate(attempts):
            cols_str = ", ".join(attempt["primary_key_columns"])
            if i == 0:
                logger.info(f"   1st attempt: {cols_str} → Found duplicates")
            else:
                attempt_num = i + 1
                suffix = "nd" if attempt_num == 2 else "rd" if attempt_num == 3 else "th"
                status_msg = "Success!" if i == len(attempts) - 1 else "Still had duplicates"
                logger.info(f"   {attempt_num}{suffix} attempt: {cols_str} → {status_msg}")
        logger.info("")

    @staticmethod
    def format_reasoning(reasoning: str) -> None:
        """
        Format and print LLM reasoning step by step.

        Args:
            reasoning: LLM reasoning text.
        """
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
    def print_trace_if_available() -> None:
        """Print DSPy trace information if available."""
        try:
            if hasattr(dspy.settings, "trace") and dspy.settings.trace:
                logger.debug("\n🔬 TRACE INFORMATION:")
                logger.debug("-" * 60)
                for i, trace_item in enumerate(dspy.settings.trace[-3:]):
                    logger.debug(f"Trace {i+1}: {str(trace_item)[:200]}...")
        except (AttributeError, IndexError):
            # Silently continue if trace information is not available
            pass


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
    Coordinates primary key detection using LLM analysis.

    This class orchestrates the detection process by delegating to specialized
    components for prediction, validation, and retry logic. It follows SOLID
    principles with clear separation of concerns.

    Note: This class assumes DSPy is already configured with a language model.
    The configuration should be done externally before instantiating this class.
    """

    def __init__(
        self,
        table_manager: TableManager,
        predictor: PrimaryKeyPredictor | None = None,
        validators: list[PrimaryKeyValidator] | None = None,
        retry_strategy: RetryStrategy | None = None,
        formatter: DetectionResultFormatter | None = None,
        show_live_reasoning: bool = True,
        max_retries: int = 3,
    ):
        """
        Initialize the primary key detector.

        Note: DSPy must be configured before creating this instance.

        Args:
            table_manager: Manager for table metadata and SQL operations.
            predictor: Predictor for LLM interactions (created if not provided).
            validators: List of validators to apply (defaults created if not provided).
            retry_strategy: Strategy for retry logic (created if not provided).
            formatter: Formatter for results (created if not provided).
            show_live_reasoning: Whether to display live reasoning during detection.
            max_retries: Maximum number of retries for detection.
        """
        self.table_manager = table_manager

        # Create default components if not provided
        self.predictor = predictor or PrimaryKeyPredictor(
            detector=dspy.ChainOfThought(DspPrimaryKeyDetectionSignature),
            show_live_reasoning=show_live_reasoning,
        )

        if validators is None:
            validators = [
                ColumnExistenceValidator(),
                DuplicateValidator(table_manager),
            ]

        self.validation_chain = ValidationChain(validators)
        self.retry_strategy = retry_strategy or RetryStrategy(max_retries=max_retries)
        self.formatter = formatter or DetectionResultFormatter()

    def detect_primary_keys_with_llm(self, table: str, context: str = "") -> dict[str, Any]:
        """
        Detect primary keys for a table using LLM analysis.

        Args:
            table: Fully qualified table name to analyze.
            context: Optional context about similar tables or patterns.

        Returns:
            Dictionary containing detection results with the following keys:
            - table: The table name
            - success: Whether detection was successful
            - primary_key_columns: List of detected primary key columns (if successful)
            - confidence: Confidence level (high/medium/low)
            - reasoning: LLM reasoning for the selection
            - has_duplicates: Whether duplicates were found (if validation performed)
            - duplicate_count: Number of duplicate combinations (if validation performed)
            - error: Error message (if failed)
            - final_status: Final status of the detection
            - all_attempts: List of all attempts (if retries occurred)
            - retries_attempted: Number of retries (if retries occurred)
        """
        logger.info(f"Starting primary key detection for table: {table}")

        # Step 1: Gather table metadata
        try:
            metadata = self._gather_table_metadata(table)
        except Exception as e:
            logger.error(f"Failed to gather table metadata: {e}")
            result = DetectionResultBuilder(table).with_error(str(e)).with_status("metadata_error").build()
            self.formatter.print_summary(result)
            return result

        # Step 2: Prediction loop with validation and retry
        result = self._detection_loop(table, context, metadata)

        # Step 3: Print summary
        self.formatter.print_summary(result)

        return result

    def _gather_table_metadata(self, table: str) -> TableMetadata:
        """
        Gather metadata from the table.

        Args:
            table: Fully qualified table name.

        Returns:
            TableMetadata containing all necessary information.

        Raises:
            Exception: If metadata retrieval fails.
        """
        columns = self.table_manager.get_table_column_names(table)
        table_definition = self.table_manager.get_table_definition(table)
        metadata_info = self.table_manager.get_table_metadata_info(table)

        return TableMetadata(table=table, columns=columns, definition=table_definition, info=metadata_info)

    def _detection_loop(self, table: str, context: str, metadata: TableMetadata) -> dict[str, Any]:
        """
        Run the main detection loop with prediction, validation, and retry.

        Args:
            table: Fully qualified table name.
            context: Optional context about similar tables.
            metadata: Table metadata.

        Returns:
            Detection result dictionary.
        """
        builder = DetectionResultBuilder(table)
        feedback = ""

        for attempt in range(self.retry_strategy.max_retries + 1):
            logger.info(f"Prediction attempt {attempt + 1}/{self.retry_strategy.max_retries + 1}")

            # Predict primary key
            try:
                prediction = self.predictor.predict(table, metadata.definition, context, feedback, metadata.info)
            except Exception as e:
                logger.error(f"Prediction failed: {e}")
                return builder.with_error(f"Prediction error: {str(e)}").with_status("prediction_error").build()

            # Format and print reasoning
            if prediction.reasoning:
                self.formatter.format_reasoning(prediction.reasoning)

            self.formatter.print_trace_if_available()

            builder.add_attempt(prediction.to_dict())

            # Validate prediction
            validation = self.validation_chain.validate_all(table, prediction.columns, metadata.columns, builder.result)

            if validation.valid:
                # Success!
                return (
                    builder.with_success(prediction.columns, prediction.confidence, prediction.reasoning)
                    .with_status("success")
                    .build()
                )

            # Check if we should retry
            if not self.retry_strategy.should_retry(attempt, validation):
                # Determine final status based on validation error
                if "duplicate" in validation.error.lower():
                    status = "max_retries_reached_with_duplicates"
                elif "not exist" in validation.error.lower():
                    status = "invalid_columns"
                else:
                    status = "validation_failed"

                return builder.with_error(validation.error).with_status(status).build()

            # Generate feedback for next attempt
            feedback = self.retry_strategy.generate_feedback(attempt, prediction.columns, validation, feedback)

        # Should not reach here, but just in case
        return builder.with_error("Max retries reached without resolution").with_status("max_retries_reached").build()
