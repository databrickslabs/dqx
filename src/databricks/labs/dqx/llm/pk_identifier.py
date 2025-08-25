"""
Enhanced Primary Key Detection using DSPy with Databricks Model Serving
Optimized for Databricks Notebook Environment with Spark SQL (Metadata-Only Approach)

New Features:
1. Dynamic Table Name Input - Auto-infer table definitions using Spark SQL metadata
2. Duplicate Check and Retry Mechanism - Validate predictions and retry up to 3 times
3. Databricks Notebook Compatibility - All operations use spark.sql()
4. Metadata-Only Analysis - Uses only table schema and metadata (no actual data)

Requirements:
pip install dspy-ai databricks_langchain

Make sure you have:
1. Databricks workspace access
2. Model serving endpoint configured
3. Proper authentication (token/credentials)
4. Active Spark session in Databricks notebook
"""

import dspy  # type: ignore
from typing import List, Dict, Optional, Tuple
from databricks_langchain import ChatDatabricks  # type: ignore
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Databricks Model Serving LM for DSPy
class DatabricksLM(dspy.LM):
    """Custom DSPy LM adapter for Databricks Model Serving"""

    def __init__(self, endpoint: str, temperature: float = 0.1, max_tokens: int = 1000):
        self.endpoint = endpoint
        self.temperature = temperature
        self.max_tokens = max_tokens

        # Initialize Databricks LangChain client
        self.llm = ChatDatabricks(endpoint=endpoint, temperature=temperature)
        # dspy.configure(lm=self.llm)
        # dspy.settings.configure(trace=[])
        super().__init__(model=endpoint)

    def __call__(self, prompt=None, messages=None, **kwargs):
        """Call the Databricks model serving endpoint"""
        try:
            if messages:
                # Handle chat format
                response = self.llm.invoke(messages)
            else:
                # Handle simple prompt
                from langchain_core.messages import HumanMessage  # type: ignore

                response = self.llm.invoke([HumanMessage(content=prompt)])

            return [response.content]

        except Exception as e:
            print(f"Error calling Databricks model: {e}")
            return [f"Error: {str(e)}"]


# Spark Manager for Databricks Operations
class SparkManager:
    """Manages Spark SQL operations for table schema and duplicate checking"""

    def __init__(self, spark_session=None):
        """
        Initialize with Spark session
        In Databricks notebook, spark session is automatically available as 'spark'
        """
        self.spark = spark_session
        if not self.spark:
            try:
                # Try to get the global spark session (available in Databricks notebooks)
                from pyspark.sql import SparkSession

                self.spark = SparkSession.getActiveSession()
                if not self.spark:
                    self.spark = SparkSession.builder.appName("PKDetection").getOrCreate()
            except ImportError:
                logger.warning("PySpark not available. Some features may not work.")

    def get_table_definition(self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> str:
        """
        Retrieve table definition using Spark SQL DESCRIBE commands

        Args:
            table_name: Name of the table
            catalog: Catalog name (optional, for Unity Catalog)
            schema: Schema/database name (optional)

        Returns:
            Formatted table definition string
        """
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            # Build full table name
            full_table_name = table_name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            if catalog:
                full_table_name = f"{catalog}.{full_table_name}"

            print(f"🔍 Retrieving schema for table: {full_table_name}")

            # Get detailed table information
            describe_query = f"DESCRIBE TABLE EXTENDED {full_table_name}"
            describe_result = self.spark.sql(describe_query)

            # Convert to pandas for easier processing
            describe_df = describe_result.toPandas()

            # Extract column information
            definition_lines = []
            in_column_section = True
            existing_pk = None

            for _, row in describe_df.iterrows():
                col_name = row['col_name']
                data_type = row['data_type']
                comment = row['comment'] if 'comment' in row else ''

                # Stop at table properties section
                if col_name.startswith('#') or col_name.strip() == '':
                    in_column_section = False
                    continue

                if in_column_section and not col_name.startswith('#'):
                    # Check for NOT NULL constraints (approximation)
                    nullable = "" if "not null" in str(comment).lower() else ""
                    definition_lines.append(f"    {col_name} {data_type}{nullable}")

            # Try to get primary key information
            try:
                pk_query = f"SHOW TBLPROPERTIES {full_table_name}"
                pk_result = self.spark.sql(pk_query)
                pk_df = pk_result.toPandas()

                # Look for primary key in table properties
                for _, row in pk_df.iterrows():
                    if 'primary' in str(row.get('key', '')).lower():
                        existing_pk = row.get('value', '')
                        break
            except Exception:
                # If SHOW TBLPROPERTIES fails, continue without PK info
                pass

            # Build table definition
            table_definition = "{\n" + ",\n".join(definition_lines) + "\n}"

            # Add existing primary key info if found
            if existing_pk:
                table_definition += f"\n-- Existing Primary Key: {existing_pk}"

            print("✅ Table definition retrieved successfully")
            return table_definition

        except Exception as e:
            logger.error(f"Error retrieving table definition for {table_name}: {e}")
            raise

    def check_duplicates(
        self,
        table_name: str,
        pk_columns: List[str],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        sample_size: int = 10000,
    ) -> Tuple[bool, int]:
        """
        Check for duplicates using Spark SQL GROUP BY and HAVING

        Args:
            table_name: Name of the table
            pk_columns: List of primary key columns to check
            catalog: Catalog name (optional)
            schema: Schema name (optional)
            sample_size: Maximum number of duplicate groups to check

        Returns:
            Tuple of (has_duplicates, duplicate_count)
        """
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            # Build full table name
            full_table_name = table_name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            if catalog:
                full_table_name = f"{catalog}.{full_table_name}"

            # Build column list for GROUP BY
            pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])  # Escape column names

            print(f"🔍 Checking for duplicates in {full_table_name} using columns: {pk_cols_str}")

            # Query to find duplicates
            duplicate_query = f"""
            SELECT {pk_cols_str}, COUNT(*) as duplicate_count
            FROM {full_table_name}
            GROUP BY {pk_cols_str}
            HAVING COUNT(*) > 1
            LIMIT {sample_size}
            """

            # Execute query
            duplicate_result = self.spark.sql(duplicate_query)
            duplicates_df = duplicate_result.toPandas()

            has_duplicates = len(duplicates_df) > 0
            duplicate_count = len(duplicates_df)

            if has_duplicates:
                total_duplicate_records = duplicates_df['duplicate_count'].sum()
                logger.warning(
                    f"Found {duplicate_count} duplicate key combinations affecting {total_duplicate_records} total records"
                )
                print(f"⚠️  Found {duplicate_count} duplicate combinations for: {', '.join(pk_columns)}")

                # Show sample of duplicates
                if len(duplicates_df) > 0:
                    print("Sample duplicates:")
                    print(duplicates_df.head().to_string(index=False))
            else:
                logger.info(f"No duplicates found for predicted primary key: {', '.join(pk_columns)}")
                print(f"✅ No duplicates found for: {', '.join(pk_columns)}")

            return has_duplicates, duplicate_count

        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            print(f"❌ Error checking duplicates: {e}")
            return False, 0  # Assume no duplicates if check fails

    def get_table_metadata_info(
        self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> str:
        """Get additional metadata information to help with primary key detection (no actual data)"""
        if not self.spark:
            return "No metadata available (Spark session not found)"

        try:
            # Build full table name
            full_table_name = table_name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            if catalog:
                full_table_name = f"{catalog}.{full_table_name}"

            metadata_info = []

            # Get table statistics if available
            try:
                stats_query = f"SHOW TBLPROPERTIES {full_table_name}"
                stats_result = self.spark.sql(stats_query)
                stats_df = stats_result.toPandas()

                # Look for useful metadata
                for _, row in stats_df.iterrows():
                    key = row.get('key', '')
                    value = row.get('value', '')
                    if any(
                        keyword in key.lower()
                        for keyword in ['numrows', 'rawdatasize', 'totalsize', 'primary', 'unique']
                    ):
                        metadata_info.append(f"{key}: {value}")

            except Exception:
                pass

            # Get column statistics
            try:
                col_stats_query = f"DESCRIBE TABLE EXTENDED {full_table_name}"
                col_result = self.spark.sql(col_stats_query)
                col_df = col_result.toPandas()

                # Count different column types for pattern analysis
                numeric_cols = []
                string_cols = []
                date_cols = []
                timestamp_cols = []

                for _, row in col_df.iterrows():
                    col_name = row.get('col_name', '')
                    data_type = str(row.get('data_type', '')).lower()

                    if col_name.startswith('#') or col_name.strip() == '':
                        break

                    if any(t in data_type for t in ['int', 'long', 'bigint', 'decimal', 'double', 'float']):
                        numeric_cols.append(col_name)
                    elif any(t in data_type for t in ['string', 'varchar', 'char']):
                        string_cols.append(col_name)
                    elif 'date' in data_type:
                        date_cols.append(col_name)
                    elif 'timestamp' in data_type:
                        timestamp_cols.append(col_name)

                metadata_info.append("Column type distribution:")
                metadata_info.append(f"  Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:5])}")
                metadata_info.append(f"  String columns ({len(string_cols)}): {', '.join(string_cols[:5])}")
                metadata_info.append(f"  Date columns ({len(date_cols)}): {', '.join(date_cols)}")
                metadata_info.append(f"  Timestamp columns ({len(timestamp_cols)}): {', '.join(timestamp_cols)}")

            except Exception:
                pass

            return (
                "Metadata information:\n" + "\n".join(metadata_info) if metadata_info else "Limited metadata available"
            )

        except Exception as e:
            return f"Could not retrieve metadata: {e}"


# Configure DSPy with Databricks Model Serving
def configure_databricks_llm(endpoint: str = "", temperature: float = 0.1):
    """Configure DSPy to use Databricks model serving"""
    lm = DatabricksLM(endpoint=endpoint, temperature=temperature)
    dspy.configure(lm=lm)
    return lm


# Configure DSPy with tracing enabled
def configure_with_tracing():
    """Enable DSPy tracing to see live reasoning"""
    dspy.settings.configure(trace=[])  # Enable tracing
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
    """Enhanced Primary Key Detector optimized for Databricks Spark environment"""

    def __init__(
        self,
        table_name: str,
        schema: str,
        catalog: str,
        endpoint: str = "databricks-meta-llama-3-1-8b-instruct",
        context: str = "",
        llm_provider: str = "databricks",
        validate_duplicates: bool = True,
        spark_session=None,
        show_live_reasoning: bool = True,
        max_retries: int = 3,
    ):
        self.table_name = table_name
        self.catalog = catalog
        self.schema = schema
        self.context = context
        self.llm_provider = llm_provider
        self.endpoint = endpoint
        self.validate_duplicates = validate_duplicates
        self.spark = spark_session
        self.detector = dspy.ChainOfThought(PrimaryKeyDetection)
        self.spark_manager = SparkManager(spark_session)
        self.show_live_reasoning = show_live_reasoning
        self.max_retries = max_retries
        configure_databricks_llm(endpoint=endpoint, temperature=0.1)
        configure_with_tracing()

    def detect_primary_key_from_table_name(self) -> Dict:
        """
        Main method: Detect primary key using only table name and metadata (no actual data)

        Args:
            table_name: Name of the table
            catalog: Catalog name for Unity Catalog (optional)
            schema: Schema/database name (optional)
            context: Additional context for prediction
            validate_duplicates: Whether to check for duplicates and retry if found

        Returns:
            Dictionary with prediction results and validation info
        """
        logger.info("\n🚀 DATABRICKS METADATA-BASED PRIMARY KEY DETECTION")
        logger.info("=" * 60)
        logger.info(f"Table: {self.table_name}")
        if self.catalog:
            logger.info(f"Catalog: {self.catalog}")
        if self.schema:
            logger.info(f"Schema: {self.schema}")
        logger.info("🔍 Using only metadata (no actual data)")
        logger.info("=" * 60)

        # Step 1: Retrieve table definition and metadata using Spark SQL (no data)
        try:
            table_definition = self.spark_manager.get_table_definition(self.table_name, self.catalog, self.schema)
            metadata_info = self.spark_manager.get_table_metadata_info(self.table_name, self.catalog, self.schema)
        except Exception as e:
            return {
                'table_name': self.table_name,
                'success': False,
                'error': f"Failed to retrieve table metadata: {str(e)}",
                'retries_attempted': 0,
            }

        # Step 2: Predict primary key with duplicate validation and retry logic
        return self._predict_with_retry_logic(
            self.table_name,
            table_definition,
            self.context,
            metadata_info,
            self.catalog,
            self.schema,
            self.validate_duplicates,
        )

    def detect_primary_key(self, table_name: str, table_definition: str, context: str = "") -> Dict:
        """Original method - detect primary key with provided table definition (backward compatibility)"""
        return self._single_prediction(table_name, table_definition, context, "", "")

    def _predict_with_retry_logic(
        self,
        table_name: str,
        table_definition: str,
        context: str,
        metadata_info: str,
        catalog: Optional[str],
        schema: Optional[str],
        validate_duplicates: bool,
    ) -> Dict:
        """Handle prediction with retry logic for duplicate validation (metadata-based)"""

        previous_attempts = ""
        all_attempts = []

        for attempt in range(self.max_retries + 1):
            logger.info(f"\n🎯 Prediction Attempt {attempt + 1}/{self.max_retries + 1}")
            logger.info("=" * 60)

            # Make prediction using only metadata
            result = self._single_prediction(table_name, table_definition, context, previous_attempts, metadata_info)

            if not result['success']:
                return result

            all_attempts.append(result.copy())
            pk_columns = result['primary_key_columns']

            # Skip duplicate validation if not requested
            if not validate_duplicates:
                result['validation_performed'] = False
                result['retries_attempted'] = attempt
                return result

            # Step 3: Validate for duplicates using Spark SQL
            logger.info("\n🔍 Validating primary key prediction using Spark SQL...")
            try:
                has_duplicates, duplicate_count = self.spark_manager.check_duplicates(
                    table_name, pk_columns, catalog, schema
                )

                result['has_duplicates'] = has_duplicates
                result['duplicate_count'] = duplicate_count
                result['validation_performed'] = True

                if not has_duplicates:
                    logger.info("✅ No duplicates found - Primary key prediction validated!")
                    result['retries_attempted'] = attempt
                    result['all_attempts'] = all_attempts
                    result['final_status'] = 'success'
                    return result
                else:
                    logger.info(f"⚠️  Found {duplicate_count} duplicate groups - Retrying with enhanced context")

                    if attempt < self.max_retries:
                        # Prepare enhanced context for next attempt
                        failed_pk = ", ".join(pk_columns)
                        previous_attempts += f"\nAttempt {attempt + 1}: Tried [{failed_pk}] but found {duplicate_count} duplicate key combinations. "
                        previous_attempts += "This indicates the combination is not unique enough. Need to find additional columns or a different combination that ensures complete uniqueness. "
                        previous_attempts += "Consider adding timestamp fields, sequence numbers, or other differentiating columns that would make each row unique."
                    else:
                        logger.info(
                            f"❌ Maximum retries ({self.max_retries}) reached. Returning best attempt with duplicates noted."
                        )
                        result['retries_attempted'] = attempt
                        result['all_attempts'] = all_attempts
                        result['final_status'] = 'max_retries_reached_with_duplicates'
                        return result

            except Exception as e:
                logger.error(f"Error during duplicate validation: {e}")
                result['validation_error'] = str(e)
                result['validation_performed'] = False
                result['retries_attempted'] = attempt
                result['final_status'] = 'validation_error'
                return result

        # This shouldn't be reached, but just in case
        return all_attempts[-1] if all_attempts else {'success': False, 'error': 'No attempts made'}

    def _single_prediction(
        self, table_name: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> Dict:
        """Make a single primary key prediction using only metadata"""

        logger.info("\n🔍 Analyzing table schema and metadata patterns...")
        logger.info("-" * 60)

        try:
            if self.show_live_reasoning:
                with dspy.context(show_guidelines=True):
                    logger.info("🧠 AI is analyzing metadata step by step...")
                    logger.info("-" * 40)

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

            # Print reasoning
            logger.info("\n💭 AI REASONING PROCESS:")
            logger.info("-" * 60)
            if hasattr(result, 'reasoning'):
                self._print_reasoning_formatted(result.reasoning)

            # Access trace if available
            self._print_trace_if_available()

            pk_columns = [col.strip() for col in result.primary_key_columns.split(',')]

            final_result = {
                'table_name': table_name,
                'primary_key_columns': pk_columns,
                'confidence': result.confidence,
                'reasoning': result.reasoning,
                'success': True,
            }

            logger.info("\n✅ PREDICTION RESULT:")
            logger.info("-" * 60)
            logger.info(f"Primary Key: {', '.join(pk_columns)}")
            logger.info(f"Confidence: {result.confidence}")

            return final_result

        except Exception as e:
            error_msg = f"Error during prediction: {str(e)}"
            logger.error(error_msg)
            return {'table_name': table_name, 'success': False, 'error': error_msg}

    def _print_reasoning_formatted(self, reasoning):
        """Format and print reasoning step by step"""
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
            else:
                if len(line) > 10 and any(
                    word in line.lower() for word in ['analyze', 'consider', 'look', 'notice', 'think']
                ):
                    print(f"📝 Step {step_counter}: {line}")
                    step_counter += 1
                else:
                    print(f"   {line}")

    def _print_trace_if_available(self):
        """Print DSPy trace if available"""
        try:
            if hasattr(dspy.settings, 'trace') and dspy.settings.trace:
                print("\n🔬 TRACE INFORMATION:")
                print("-" * 60)
                for i, trace_item in enumerate(dspy.settings.trace[-3:]):
                    print(f"Trace {i+1}: {str(trace_item)[:200]}...")
        except Exception:
            pass

    def print_pk_detection_summary(self, result):
        """Dynamic summary printing based on result dictionary"""

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
