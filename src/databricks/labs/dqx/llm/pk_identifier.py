"""Primary Key Detection using DSPy with Databricks Model Serving."""

import logging
from typing import Any

import dspy
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage
from pyspark.sql import SparkSession

from databricks.labs.dqx.utils import generate_table_definition_from_dataframe

logger = logging.getLogger(__name__)


class DatabricksLM(dspy.LM):
    """Custom DSPy LM adapter for Databricks Model Serving."""

    def __init__(self, endpoint: str, temperature: float = 0.1, max_tokens: int = 1000, chat_databricks_cls=None):
        self.endpoint = endpoint
        self.temperature = temperature
        self.max_tokens = max_tokens

        # Allow injection of ChatDatabricks class for testing
        chat_cls = chat_databricks_cls or ChatDatabricks
        self.llm = chat_cls(endpoint=endpoint, temperature=temperature)
        super().__init__(model=endpoint)

    def __call__(self, prompt=None, messages=None, **kwargs):
        """Call the Databricks model serving endpoint."""
        try:
            if messages:
                response = self.llm.invoke(messages)
            else:
                response = self.llm.invoke([HumanMessage(content=prompt)])

            return [response.content]

        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"Error calling Databricks model: {e}")
            return [f"Error: {str(e)}"]
        except Exception as e:
            print(f"Unexpected error calling Databricks model: {e}")
            return [f"Unexpected error: {str(e)}"]


class SparkManager:
    """Manages Spark SQL operations for table schema and duplicate checking."""

    def __init__(self, spark_session=None):
        """Initialize with Spark session."""
        self.spark = spark_session
        if not self.spark:
            try:
                self.spark = SparkSession.getActiveSession()
                if not self.spark:
                    self.spark = SparkSession.builder.appName("PKDetection").getOrCreate()
            except ImportError:
                logger.warning("PySpark not available. Some features may not work.")
            except RuntimeError as e:
                # Handle case where only remote Spark sessions are supported (e.g., in unit tests)
                if "Only remote Spark sessions using Databricks Connect are supported" in str(e):
                    logger.warning(
                        "Local Spark session not available. Using None - features requiring Spark will not work."
                    )
                    self.spark = None
                else:
                    raise

    def _get_table_columns(self, table_name: str) -> list[str]:
        """Get table column definitions from DESCRIBE TABLE."""
        describe_query = f"DESCRIBE TABLE EXTENDED {table_name}"
        describe_result = self.spark.sql(describe_query)
        describe_df = describe_result.toPandas()

        definition_lines = []
        in_column_section = True

        for _, row in describe_df.iterrows():
            col_name = row['col_name']
            data_type = row['data_type']
            comment = row['comment'] if 'comment' in row else ''

            if col_name.startswith('#') or col_name.strip() == '':
                in_column_section = False
                continue

            if in_column_section and not col_name.startswith('#'):
                nullable = "" if "not null" in str(comment).lower() else ""
                definition_lines.append(f"    {col_name} {data_type}{nullable}")

        return definition_lines

    def _get_existing_primary_key(self, table_name: str) -> str | None:
        """Get existing primary key from table properties."""
        try:
            pk_query = f"SHOW TBLPROPERTIES {table_name}"
            pk_result = self.spark.sql(pk_query)
            pk_df = pk_result.toPandas()

            for _, row in pk_df.iterrows():
                if 'primary' in str(row.get('key', '')).lower():
                    return row.get('value', '')
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            pass
        return None

    def _build_table_definition_string(self, definition_lines: list[str], existing_pk: str | None) -> str:
        """Build the final table definition string."""
        table_definition = "{\n" + ",\n".join(definition_lines) + "\n}"
        if existing_pk:
            table_definition += f"\n-- Existing Primary Key: {existing_pk}"
        return table_definition

    def get_table_definition(self, table_name: str) -> str:
        """Retrieve table definition using Spark SQL DESCRIBE commands."""
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            print(f"🔍 Retrieving schema for table: {table_name}")

            definition_lines = self._get_table_columns(table_name)
            existing_pk = self._get_existing_primary_key(table_name)
            table_definition = self._build_table_definition_string(definition_lines, existing_pk)

            print("✅ Table definition retrieved successfully")
            return table_definition

        except (ValueError, RuntimeError) as e:
            logger.error(f"Error retrieving table definition for {table_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving table definition for {table_name}: {e}")
            raise RuntimeError(f"Failed to retrieve table definition: {e}") from e

    def _execute_duplicate_check_query(
        self, full_table_name: str, pk_columns: list[str]
    ) -> tuple[bool, int, Any]:
        """Execute the duplicate check query and return results."""
        pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
        print(f"🔍 Checking for duplicates in {full_table_name} using columns: {pk_cols_str}")

        duplicate_query = f"""
        SELECT {pk_cols_str}, COUNT(*) as duplicate_count
        FROM {full_table_name}
        GROUP BY {pk_cols_str}
        HAVING COUNT(*) > 1
        """

        duplicate_result = self.spark.sql(duplicate_query)
        duplicates_df = duplicate_result.toPandas()
        return len(duplicates_df) > 0, len(duplicates_df), duplicates_df

    def _report_duplicate_results(
        self, has_duplicates: bool, duplicate_count: int, pk_columns: list[str], duplicates_df=None
    ):
        """Report the results of duplicate checking."""
        if has_duplicates and duplicates_df is not None:
            total_duplicate_records = duplicates_df['duplicate_count'].sum()
            logger.warning(
                f"Found {duplicate_count} duplicate key combinations affecting {total_duplicate_records} total records"
            )
            print(f"⚠️  Found {duplicate_count} duplicate combinations for: {', '.join(pk_columns)}")
            if len(duplicates_df) > 0:
                print("Sample duplicates:")
                print(duplicates_df.head().to_string(index=False))
        else:
            logger.info(f"No duplicates found for predicted primary key: {', '.join(pk_columns)}")
            print(f"✅ No duplicates found for: {', '.join(pk_columns)}")

    def check_duplicates(
        self,
        table_name: str,
        pk_columns: list[str],
    ) -> tuple[bool, int]:
        """Check for duplicates using Spark SQL GROUP BY and HAVING."""
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            has_duplicates, duplicate_count, duplicates_df = self._execute_duplicate_check_query(
                table_name, pk_columns
            )
            self._report_duplicate_results(has_duplicates, duplicate_count, pk_columns, duplicates_df)
            return has_duplicates, duplicate_count

        except (ValueError, RuntimeError) as e:
            logger.error(f"Error checking duplicates: {e}")
            print(f"❌ Error checking duplicates: {e}")
            return False, 0
        except Exception as e:
            logger.error(f"Unexpected error checking duplicates: {e}")
            print(f"❌ Unexpected error checking duplicates: {e}")
            return False, 0

    def _extract_useful_properties(self, stats_df) -> list[str]:
        """Extract useful properties from table properties DataFrame."""
        metadata_info = []
        for _, row in stats_df.iterrows():
            key = row.get('key', '')
            value = row.get('value', '')
            if any(keyword in key.lower() for keyword in ('numrows', 'rawdatasize', 'totalsize', 'primary', 'unique')):
                metadata_info.append(f"{key}: {value}")
        return metadata_info

    def _get_table_properties(self, table_name: str) -> list[str]:
        """Get table properties metadata."""
        try:
            stats_query = f"SHOW TBLPROPERTIES {table_name}"
            stats_result = self.spark.sql(stats_query)
            stats_df = stats_result.toPandas()
            return self._extract_useful_properties(stats_df)
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            return []

    def _categorize_columns_by_type(self, col_df) -> tuple[list[str], list[str], list[str], list[str]]:
        """Categorize columns by their data types."""
        numeric_cols = []
        string_cols = []
        date_cols = []
        timestamp_cols = []

        for _, row in col_df.iterrows():
            col_name = row.get('col_name', '')
            data_type = str(row.get('data_type', '')).lower()

            if col_name.startswith('#') or col_name.strip() == '':
                break

            if any(t in data_type for t in ('int', 'long', 'bigint', 'decimal', 'double', 'float')):
                numeric_cols.append(col_name)
            elif any(t in data_type for t in ('string', 'varchar', 'char')):
                string_cols.append(col_name)
            elif 'date' in data_type:
                date_cols.append(col_name)
            elif 'timestamp' in data_type:
                timestamp_cols.append(col_name)

        return numeric_cols, string_cols, date_cols, timestamp_cols

    def _format_column_distribution(
        self, numeric_cols: list[str], string_cols: list[str], date_cols: list[str], timestamp_cols: list[str]
    ) -> list[str]:
        """Format column type distribution information."""
        metadata_info = ["Column type distribution:"]
        metadata_info.append(f"  Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:5])}")
        metadata_info.append(f"  String columns ({len(string_cols)}): {', '.join(string_cols[:5])}")
        metadata_info.append(f"  Date columns ({len(date_cols)}): {', '.join(date_cols)}")
        metadata_info.append(f"  Timestamp columns ({len(timestamp_cols)}): {', '.join(timestamp_cols)}")
        return metadata_info

    def _get_column_statistics(self, table_name: str) -> list[str]:
        """Get column statistics and type distribution."""
        try:
            col_stats_query = f"DESCRIBE TABLE EXTENDED {table_name}"
            col_result = self.spark.sql(col_stats_query)
            col_df = col_result.toPandas()

            numeric_cols, string_cols, date_cols, timestamp_cols = self._categorize_columns_by_type(col_df)
            return self._format_column_distribution(numeric_cols, string_cols, date_cols, timestamp_cols)
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            return []

    def get_table_metadata_info(self, table_name: str) -> str:
        """Get additional metadata information to help with primary key detection."""
        if not self.spark:
            return "No metadata available (Spark session not found)"

        try:
            metadata_info = []

            # Get table properties
            metadata_info.extend(self._get_table_properties(table_name))

            # Get column statistics
            metadata_info.extend(self._get_column_statistics(table_name))

            return (
                "Metadata information:\n" + "\n".join(metadata_info) if metadata_info else "Limited metadata available"
            )

        except (ValueError, RuntimeError) as e:
            return f"Could not retrieve metadata: {e}"
        except Exception as e:
            logger.warning(f"Unexpected error retrieving metadata: {e}")
            return f"Could not retrieve metadata due to unexpected error: {e}"


def configure_databricks_llm(endpoint: str = "", temperature: float = 0.1, chat_databricks_cls=None):
    """Configure DSPy to use Databricks model serving."""
    language_model = DatabricksLM(endpoint=endpoint, temperature=temperature, chat_databricks_cls=chat_databricks_cls)
    dspy.configure(lm=language_model)
    return language_model


def configure_with_tracing():
    """Enable DSPy tracing to see live reasoning."""
    dspy.settings.configure(trace=[])
    return True


class PrimaryKeyDetection(dspy.Signature):
    """Analyze table schema and metadata step-by-step to identify the most likely primary key columns."""

    table_name: str = dspy.InputField(desc="Name of the database table")
    table_definition: str = dspy.InputField(desc="Complete table schema definition")
    context: str = dspy.InputField(desc="Context about similar tables or patterns")
    previous_attempts: str = dspy.InputField(desc="Previous failed attempts and why they failed")
    metadata_info: str = dspy.InputField(desc="Table metadata and column statistics to aid analysis")

    primary_key_columns: str = dspy.OutputField(desc="Comma-separated list of primary key columns")
    confidence: str = dspy.OutputField(desc="Confidence level: high, medium, or low")
    reasoning: str = dspy.OutputField(desc="Step-by-step reasoning for the selection based on metadata analysis")


class DatabricksPrimaryKeyDetector:
    """Primary Key Detector optimized for Databricks Spark environment."""

    def __init__(
        self,
        table_name: str,
        *,
        endpoint: str = "databricks-meta-llama-3-1-8b-instruct",
        context: str = "",
        validate_duplicates: bool = True,
        fail_on_duplicates: bool = True,
        spark_session=None,
        show_live_reasoning: bool = True,
        max_retries: int = 3,
        chat_databricks_cls=None,
    ):
        self.table_name = table_name
        self.context = context
        self.llm_provider = "databricks"  # Fixed to databricks provider
        self.endpoint = endpoint
        self.validate_duplicates = validate_duplicates
        self.fail_on_duplicates = fail_on_duplicates
        self.spark = spark_session
        self.detector = dspy.ChainOfThought(PrimaryKeyDetection)
        self.spark_manager = SparkManager(spark_session)
        self.show_live_reasoning = show_live_reasoning
        self.max_retries = max_retries

        configure_databricks_llm(endpoint=endpoint, temperature=0.1, chat_databricks_cls=chat_databricks_cls)
        configure_with_tracing()

    def detect_primary_keys(self) -> dict[str, Any]:
        """
        Detect primary keys for tables and views.
        """
        logger.info(f"Starting primary key detection for table: {self.table_name}")
        return self._detect_primary_keys_from_table()


    def _detect_primary_keys_from_table(self) -> dict[str, Any]:
        """Detect primary keys from a registered table or view."""
        try:
            table_definition = self.spark_manager.get_table_definition(self.table_name)
            metadata_info = self.spark_manager.get_table_metadata_info(self.table_name)
        except (ValueError, RuntimeError, OSError) as e:
            return {
                'table_name': self.table_name,
                'success': False,
                'error': f"Failed to retrieve table metadata: {str(e)}",
                'retries_attempted': 0,
            }
        except Exception as e:
            logger.error(f"Unexpected error during table metadata retrieval: {e}")
            return {
                'table_name': self.table_name,
                'success': False,
                'error': f"Unexpected error retrieving table metadata: {str(e)}",
                'retries_attempted': 0,
            }

        return self._predict_with_retry_logic(
            self.table_name,
            table_definition,
            self.context,
            metadata_info,
            self.validate_duplicates,
        )

    def _generate_table_definition_from_dataframe(self, df: Any) -> str:
        """Generate a CREATE TABLE statement from a DataFrame schema."""
        return generate_table_definition_from_dataframe(df, self.table_name)

    def detect_primary_key(self, table_name: str, table_definition: str, context: str = "") -> dict[str, Any]:
        """Detect primary key with provided table definition."""
        return self._single_prediction(table_name, table_definition, context, "", "")

    def _check_duplicates_and_update_result(
        self, table_name: str, pk_columns: list[str], result: dict
    ) -> tuple[bool, int]:
        """Check for duplicates and update result with validation info."""
        has_duplicates, duplicate_count = self.spark_manager.check_duplicates(table_name, pk_columns)

        result['has_duplicates'] = has_duplicates
        result['duplicate_count'] = duplicate_count
        result['validation_performed'] = True

        return has_duplicates, duplicate_count

    def _handle_successful_validation(
        self, result: dict, attempt: int, all_attempts: list, previous_attempts: str
    ) -> tuple[dict, str, bool]:
        """Handle successful validation (no duplicates found)."""
        logger.info("No duplicates found - Primary key prediction validated!")
        result['retries_attempted'] = attempt
        result['all_attempts'] = all_attempts
        result['final_status'] = 'success'
        return result, previous_attempts, True  # Success, stop retrying

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
                f"\nAttempt {attempt + 1}: Tried [{failed_pk}] but found {duplicate_count} duplicate key combinations. "
            )
            previous_attempts += "This indicates the combination is not unique enough. Need to find additional columns or a different combination that ensures complete uniqueness. "
            previous_attempts += "Consider adding timestamp fields, sequence numbers, or other differentiating columns that would make each row unique."
            return result, previous_attempts, False  # Continue retrying

        logger.info(f"Maximum retries ({self.max_retries}) reached. Returning best attempt with duplicates noted.")
        
        # Check if we should fail when duplicates are found
        if hasattr(self, 'fail_on_duplicates') and self.fail_on_duplicates:
            result['success'] = False  # Mark as failed since duplicates were found
            result['error'] = f"Primary key validation failed: Found {duplicate_count} duplicate combinations in suggested columns {pk_columns}"
        else:
            # Return best attempt with warning but don't fail
            result['success'] = True
            result['warning'] = f"Primary key has duplicates: Found {duplicate_count} duplicate combinations in suggested columns {pk_columns}"
        
        result['retries_attempted'] = attempt
        result['all_attempts'] = all_attempts
        result['final_status'] = 'max_retries_reached_with_duplicates'
        return result, previous_attempts, True  # Stop retrying, max attempts reached

    def _handle_validation_error(
        self, error: Exception, result: dict, attempt: int, all_attempts: list, previous_attempts: str
    ) -> tuple[dict, str, bool]:
        """Handle validation errors."""
        logger.error(f"Error during duplicate validation: {error}")
        result['validation_error'] = str(error)
        result['retries_attempted'] = attempt
        result['all_attempts'] = all_attempts
        result['final_status'] = 'validation_error'
        return result, previous_attempts, True  # Stop retrying due to error

    def _validate_pk_duplicates(
        self,
        table_name: str,
        pk_columns: list[str],
        result: dict,
        attempt: int,
        all_attempts: list,
        previous_attempts: str,
    ) -> tuple[dict, str, bool]:
        """Validate primary key for duplicates and handle retry logic."""
        try:
            has_duplicates, duplicate_count = self._check_duplicates_and_update_result(table_name, pk_columns, result)

            if not has_duplicates:
                return self._handle_successful_validation(result, attempt, all_attempts, previous_attempts)

            return self._handle_duplicates_found(
                pk_columns, duplicate_count, attempt, result, all_attempts, previous_attempts
            )

        except (ValueError, RuntimeError) as e:
            return self._handle_validation_error(e, result, attempt, all_attempts, previous_attempts)

    def _execute_single_prediction(
        self, table_name: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> dict[str, Any]:
        """Execute a single prediction with the LLM."""
        if self.show_live_reasoning:
            with dspy.context(show_guidelines=True):
                logger.info("AI is analyzing metadata step by step...")
                result = self.detector(
                    table_name=table_name,
                    table_definition=table_definition,
                    context=context,
                    previous_attempts=previous_attempts,
                    metadata_info=metadata_info,
                )
        else:
            result = self.detector(
                table_name=table_name,
                table_definition=table_definition,
                context=context,
                previous_attempts=previous_attempts,
                metadata_info=metadata_info,
            )

        pk_columns = [col.strip() for col in result.primary_key_columns.split(',')]

        final_result = {
            'table_name': table_name,
            'primary_key_columns': pk_columns,
            'confidence': result.confidence,
            'reasoning': result.reasoning,
            'success': True,
        }

        logger.info(f"Primary Key: {', '.join(pk_columns)}")
        logger.info(f"Confidence: {result.confidence}")

        return final_result

    def _predict_with_retry_logic(
        self,
        table_name: str,
        table_definition: str,
        context: str,
        metadata_info: str,
        validate_duplicates: bool,
    ) -> dict[str, Any]:
        """Handle prediction with retry logic for duplicate validation."""

        previous_attempts = ""
        all_attempts = []

        for attempt in range(self.max_retries + 1):
            logger.info(f"Prediction attempt {attempt + 1}/{self.max_retries + 1}")

            result = self._single_prediction(table_name, table_definition, context, previous_attempts, metadata_info)

            if not result['success']:
                return result

            all_attempts.append(result.copy())
            pk_columns = result['primary_key_columns']

            if not validate_duplicates:
                result['validation_performed'] = False
                result['retries_attempted'] = attempt
                return result

            logger.info("Validating primary key prediction...")
            result, previous_attempts, should_stop = self._validate_pk_duplicates(
                table_name, pk_columns, result, attempt, all_attempts, previous_attempts
            )

            if should_stop:
                return result

        # This shouldn't be reached, but just in case
        return all_attempts[-1] if all_attempts else {'success': False, 'error': 'No attempts made'}

    def _single_prediction(
        self, table_name: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> dict[str, Any]:
        """Make a single primary key prediction using metadata."""

        logger.info("Analyzing table schema and metadata patterns...")

        try:
            final_result = self._execute_single_prediction(
                table_name, table_definition, context, previous_attempts, metadata_info
            )

            # Print reasoning if available
            if 'reasoning' in final_result:
                self._print_reasoning_formatted(final_result['reasoning'])

            self._print_trace_if_available()

            return final_result

        except (ValueError, RuntimeError, AttributeError) as e:
            error_msg = f"Error during prediction: {str(e)}"
            logger.error(error_msg)
            return {'table_name': table_name, 'success': False, 'error': error_msg}
        except Exception as e:
            error_msg = f"Unexpected error during prediction: {str(e)}"
            logger.error(error_msg)
            logger.debug("Full traceback:", exc_info=True)
            return {'table_name': table_name, 'success': False, 'error': error_msg}

    def _print_reasoning_formatted(self, reasoning):
        """Format and print reasoning step by step."""
        if not reasoning:
            print("No reasoning provided")
            return

        lines = reasoning.split('\n')
        step_counter = 1

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.lower().startswith('step'):
                print(f"📝 {line}")
            elif line.startswith('-') or line.startswith('•'):
                print(f"   {line}")
            elif len(line) > 10 and any(
                word in line.lower() for word in ('analyze', 'consider', 'look', 'notice', 'think')
            ):
                print(f"📝 Step {step_counter}: {line}")
                step_counter += 1
            else:
                print(f"   {line}")

    def _print_trace_if_available(self):
        """Print DSPy trace if available."""
        try:
            if hasattr(dspy.settings, 'trace') and dspy.settings.trace:
                print("\n🔬 TRACE INFORMATION:")
                print("-" * 60)
                for i, trace_item in enumerate(dspy.settings.trace[-3:]):
                    print(f"Trace {i+1}: {str(trace_item)[:200]}...")
        except (AttributeError, IndexError):
            # Silently continue if trace information is not available
            pass

    def print_pk_detection_summary(self, result):
        """Print summary based on result dictionary."""

        logger.info("=" * 60)
        logger.info("🎯 PRIMARY KEY DETECTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Table: {result['table_name']}")
        logger.info(f"Status: {'✅ SUCCESS' if result['success'] else '❌ FAILED'}")
        logger.info(f"Attempts: {result.get('retries_attempted', 0) + 1}")
        if result.get('retries_attempted', 0) > 0:
            logger.info(f"Retries needed: {result['retries_attempted']}")
        logger.info("")

        logger.info("📋 FINAL PRIMARY KEY:")
        for col in result['primary_key_columns']:
            logger.info(f"   • {col}")
        logger.info("")

        logger.info(f"🎯 Confidence: {result['confidence'].upper()}")

        if result.get('validation_performed', False):
            validation_msg = (
                "No duplicates found"
                if not result.get('has_duplicates', True)
                else f"Found {result.get('duplicate_count', 0)} duplicates"
            )
            logger.info(f"🔍 Validation: {validation_msg}")
        else:
            logger.info("🔍 Validation: Not performed")
        logger.info("")

        if result.get('all_attempts') and len(result['all_attempts']) > 1:
            logger.info("📝 ATTEMPT HISTORY:")
            for i, attempt in enumerate(result['all_attempts']):
                cols_str = ', '.join(attempt['primary_key_columns'])
                if i == 0:
                    logger.info(f"   1st attempt: {cols_str} → Found duplicates")
                else:
                    attempt_num = i + 1
                    suffix = "nd" if attempt_num == 2 else "rd" if attempt_num == 3 else "th"
                    status_msg = "Success!" if i == len(result['all_attempts']) - 1 else "Still had duplicates"
                    logger.info(f"   {attempt_num}{suffix} attempt: {cols_str} → {status_msg}")
            logger.info("")

        status = result.get('final_status', 'unknown')
        if status == 'success':
            logger.info("✅ RECOMMENDATION: Use the detected composite key")
        elif status == 'max_retries_reached_with_duplicates':
            logger.info("⚠️  RECOMMENDATION: Manual review needed - duplicates persist")
        else:
            logger.info(f"ℹ️  STATUS: {status}")

        logger.info("=" * 60)
