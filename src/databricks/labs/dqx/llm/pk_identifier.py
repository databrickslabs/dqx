"""Primary Key Detection using DSPy with Databricks Model Serving."""

import dspy  # type: ignore
from typing import List, Dict, Optional, Tuple
from databricks_langchain import ChatDatabricks  # type: ignore
import logging

logger = logging.getLogger(__name__)


class DatabricksLM(dspy.LM):
    """Custom DSPy LM adapter for Databricks Model Serving."""

    def __init__(self, endpoint: str, temperature: float = 0.1, max_tokens: int = 1000):
        self.endpoint = endpoint
        self.temperature = temperature
        self.max_tokens = max_tokens

        self.llm = ChatDatabricks(endpoint=endpoint, temperature=temperature)
        super().__init__(model=endpoint)

    def __call__(self, prompt=None, messages=None, **kwargs):
        """Call the Databricks model serving endpoint."""
        try:
            if messages:
                response = self.llm.invoke(messages)
            else:
                from langchain_core.messages import HumanMessage  # type: ignore

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
                from pyspark.sql import SparkSession

                self.spark = SparkSession.getActiveSession()
                if not self.spark:
                    self.spark = SparkSession.builder.appName("PKDetection").getOrCreate()
            except ImportError:
                logger.warning("PySpark not available. Some features may not work.")

    def get_table_definition(self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None) -> str:
        """Retrieve table definition using Spark SQL DESCRIBE commands."""
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            full_table_name = table_name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            if catalog:
                full_table_name = f"{catalog}.{full_table_name}"

            print(f"🔍 Retrieving schema for table: {full_table_name}")

            describe_query = f"DESCRIBE TABLE EXTENDED {full_table_name}"
            describe_result = self.spark.sql(describe_query)
            describe_df = describe_result.toPandas()

            definition_lines = []
            in_column_section = True
            existing_pk = None

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

            try:
                pk_query = f"SHOW TBLPROPERTIES {full_table_name}"
                pk_result = self.spark.sql(pk_query)
                pk_df = pk_result.toPandas()

                for _, row in pk_df.iterrows():
                    if 'primary' in str(row.get('key', '')).lower():
                        existing_pk = row.get('value', '')
                        break
            except (ValueError, RuntimeError, KeyError):
                # Silently continue if table properties are not accessible
                pass

            table_definition = "{\n" + ",\n".join(definition_lines) + "\n}"

            if existing_pk:
                table_definition += f"\n-- Existing Primary Key: {existing_pk}"

            print("✅ Table definition retrieved successfully")
            return table_definition

        except (ValueError, RuntimeError) as e:
            logger.error(f"Error retrieving table definition for {table_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving table definition for {table_name}: {e}")
            raise RuntimeError(f"Failed to retrieve table definition: {e}") from e

    def check_duplicates(
        self,
        table_name: str,
        pk_columns: List[str],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        sample_size: int = 10000,
    ) -> Tuple[bool, int]:
        """Check for duplicates using Spark SQL GROUP BY and HAVING."""
        if not self.spark:
            raise ValueError("Spark session not available")

        try:
            full_table_name = table_name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            if catalog:
                full_table_name = f"{catalog}.{full_table_name}"

            pk_cols_str = ", ".join([f"`{col}`" for col in pk_columns])
            print(f"🔍 Checking for duplicates in {full_table_name} using columns: {pk_cols_str}")

            duplicate_query = f"""
            SELECT {pk_cols_str}, COUNT(*) as duplicate_count
            FROM {full_table_name}
            GROUP BY {pk_cols_str}
            HAVING COUNT(*) > 1
            LIMIT {sample_size}
            """

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

                if len(duplicates_df) > 0:
                    print("Sample duplicates:")
                    print(duplicates_df.head().to_string(index=False))
            else:
                logger.info(f"No duplicates found for predicted primary key: {', '.join(pk_columns)}")
                print(f"✅ No duplicates found for: {', '.join(pk_columns)}")

            return has_duplicates, duplicate_count

        except (ValueError, RuntimeError) as e:
            logger.error(f"Error checking duplicates: {e}")
            print(f"❌ Error checking duplicates: {e}")
            return False, 0
        except Exception as e:
            logger.error(f"Unexpected error checking duplicates: {e}")
            print(f"❌ Unexpected error checking duplicates: {e}")
            return False, 0

    def get_table_metadata_info(
        self, table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None
    ) -> str:
        """Get additional metadata information to help with primary key detection."""
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

            except (ValueError, RuntimeError, KeyError):
                # Silently continue if table properties are not accessible
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

            except (ValueError, RuntimeError, KeyError):
                # Silently continue if table properties are not accessible
                pass

            return (
                "Metadata information:\n" + "\n".join(metadata_info) if metadata_info else "Limited metadata available"
            )

        except (ValueError, RuntimeError) as e:
            return f"Could not retrieve metadata: {e}"
        except Exception as e:
            logger.warning(f"Unexpected error retrieving metadata: {e}")
            return f"Could not retrieve metadata due to unexpected error: {e}"


def configure_databricks_llm(endpoint: str = "", temperature: float = 0.1):
    """Configure DSPy to use Databricks model serving."""
    lm = DatabricksLM(endpoint=endpoint, temperature=temperature)
    dspy.configure(lm=lm)
    return lm


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

    def detect_primary_keys(self) -> Dict:
        """
        Detect primary keys for any data source (tables, views, or file paths).

        This method automatically detects whether the source is a table/view or a file path
        and uses the appropriate detection strategy.
        """
        logger.info(f"Starting primary key detection for: {self.table_name}")

        # Check if this looks like a file path
        if self._is_file_path(self.table_name):
            return self._detect_primary_keys_from_path()
        else:
            return self._detect_primary_keys_from_table()

    def detect_primary_key_from_table_name(self) -> Dict:
        """
        Detect primary key using only table name and metadata.

        Deprecated: Use detect_primary_keys() instead for better flexibility.
        """
        logger.warning("detect_primary_key_from_table_name() is deprecated. Use detect_primary_keys() instead.")
        return self._detect_primary_keys_from_table()

    def _detect_primary_keys_from_table(self) -> Dict:
        """Detect primary keys from a registered table or view."""
        try:
            table_definition = self.spark_manager.get_table_definition(self.table_name, self.catalog, self.schema)
            metadata_info = self.spark_manager.get_table_metadata_info(self.table_name, self.catalog, self.schema)
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
            self.catalog,
            self.schema,
            self.validate_duplicates,
        )

    def _detect_primary_keys_from_path(self) -> Dict:
        """Detect primary keys from a file path by reading the schema."""
        try:
            logger.info(f"Detecting primary keys from file path: {self.table_name}")

            # Read the file to get schema information
            df = self.spark.read.option("inferSchema", "true").option("header", "true").load(self.table_name)

            # Generate table definition from DataFrame schema
            table_definition = self._generate_table_definition_from_dataframe(df)

            # Generate basic metadata info
            metadata_info = f"File: {self.table_name}, Columns: {len(df.columns)}, Inferred schema from file"

            logger.info(f"Generated table definition from file: {self.table_name}")

            return self._predict_with_retry_logic(
                self.table_name,
                table_definition,
                f"File path analysis: {self.table_name}",
                metadata_info,
                None,  # No catalog for file paths
                None,  # No schema for file paths
                self.validate_duplicates,
            )

        except (ValueError, RuntimeError, OSError) as e:
            return {
                'table_name': self.table_name,
                'success': False,
                'error': f"Failed to read file path {self.table_name}: {str(e)}",
                'retries_attempted': 0,
            }
        except Exception as e:
            logger.error(f"Unexpected error reading file path {self.table_name}: {e}")
            return {
                'table_name': self.table_name,
                'success': False,
                'error': f"Unexpected error reading file path {self.table_name}: {str(e)}",
                'retries_attempted': 0,
            }

    def _is_file_path(self, name: str) -> bool:
        """
        Determine if the given name is a file path rather than a table name.

        Args:
            name: The name to check

        Returns:
            True if it looks like a file path, False if it looks like a table name
        """
        # File path indicators
        file_indicators = [
            name.startswith('/'),  # Absolute path
            name.startswith('s3://'),  # S3 path
            name.startswith('gs://'),  # Google Cloud Storage
            name.startswith('abfss://'),  # Azure Data Lake Storage Gen2
            name.startswith('wasbs://'),  # Azure Blob Storage
            name.startswith('hdfs://'),  # HDFS
            name.startswith('file://'),  # Local file system
            '/' in name and '.' not in name.split('/')[-1].split('.')[0],  # Path with directories
        ]

        # File extension indicators
        file_extensions = ['.parquet', '.json', '.csv', '.orc', '.avro', '.delta']
        has_file_extension = any(name.lower().endswith(ext) for ext in file_extensions)

        # If it has a file extension or matches file path patterns, it's likely a file
        if has_file_extension or any(file_indicators):
            return True

        # If it has exactly 2 or 3 dots (catalog.schema.table or schema.table), it's likely a table
        dot_count = name.count('.')
        if dot_count in [1, 2]:
            return False

        # Default to table for ambiguous cases
        return False

    def _generate_table_definition_from_dataframe(self, df) -> str:
        """
        Generate a CREATE TABLE statement from a DataFrame schema.

        Args:
            df: The DataFrame to generate a table definition for

        Returns:
            A string representing a CREATE TABLE statement
        """
        table_definition = f"CREATE TABLE {self.table_name} (\n"

        column_definitions = []
        for field in df.schema.fields:
            # Convert Spark data types to SQL-like representation
            sql_type = self._spark_type_to_sql_type(field.dataType)
            nullable = "" if field.nullable else " NOT NULL"
            column_definitions.append(f"    {field.name} {sql_type}{nullable}")

        table_definition += ",\n".join(column_definitions)
        table_definition += "\n)"

        return table_definition

    def _spark_type_to_sql_type(self, spark_type) -> str:
        """Convert Spark data types to SQL-like string representations."""
        from pyspark.sql import types as T

        if isinstance(spark_type, T.StringType):
            return "STRING"
        elif isinstance(spark_type, T.IntegerType):
            return "INT"
        elif isinstance(spark_type, T.LongType):
            return "BIGINT"
        elif isinstance(spark_type, T.DoubleType):
            return "DOUBLE"
        elif isinstance(spark_type, T.FloatType):
            return "FLOAT"
        elif isinstance(spark_type, T.BooleanType):
            return "BOOLEAN"
        elif isinstance(spark_type, T.DateType):
            return "DATE"
        elif isinstance(spark_type, T.TimestampType):
            return "TIMESTAMP"
        elif isinstance(spark_type, T.DecimalType):
            return f"DECIMAL({spark_type.precision},{spark_type.scale})"
        elif isinstance(spark_type, T.ArrayType):
            return f"ARRAY<{self._spark_type_to_sql_type(spark_type.elementType)}>"
        elif isinstance(spark_type, T.MapType):
            return f"MAP<{self._spark_type_to_sql_type(spark_type.keyType)},{self._spark_type_to_sql_type(spark_type.valueType)}>"
        elif isinstance(spark_type, T.StructType):
            return "STRUCT<...>"  # Simplified for LLM analysis
        else:
            return str(spark_type).upper()

    def detect_primary_key(self, table_name: str, table_definition: str, context: str = "") -> Dict:
        """Detect primary key with provided table definition."""
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
            try:
                has_duplicates, duplicate_count = self.spark_manager.check_duplicates(
                    table_name, pk_columns, catalog, schema
                )

                result['has_duplicates'] = has_duplicates
                result['duplicate_count'] = duplicate_count
                result['validation_performed'] = True

                if not has_duplicates:
                    logger.info("No duplicates found - Primary key prediction validated!")
                    result['retries_attempted'] = attempt
                    result['all_attempts'] = all_attempts
                    result['final_status'] = 'success'
                    return result
                else:
                    logger.info(f"Found {duplicate_count} duplicate groups - Retrying with enhanced context")

                    if attempt < self.max_retries:
                        failed_pk = ", ".join(pk_columns)
                        previous_attempts += f"\nAttempt {attempt + 1}: Tried [{failed_pk}] but found {duplicate_count} duplicate key combinations. "
                        previous_attempts += "This indicates the combination is not unique enough. Need to find additional columns or a different combination that ensures complete uniqueness. "
                        previous_attempts += "Consider adding timestamp fields, sequence numbers, or other differentiating columns that would make each row unique."
                    else:
                        logger.info(
                            f"Maximum retries ({self.max_retries}) reached. Returning best attempt with duplicates noted."
                        )
                        result['retries_attempted'] = attempt
                        result['all_attempts'] = all_attempts
                        result['final_status'] = 'max_retries_reached_with_duplicates'
                        return result

            except (ValueError, RuntimeError) as e:
                logger.error(f"Error during duplicate validation: {e}")
                result['validation_error'] = str(e)
                result['validation_performed'] = False
            except Exception as e:
                logger.error(f"Unexpected error during duplicate validation: {e}")
                result['validation_error'] = f"Unexpected error: {str(e)}"
                result['validation_performed'] = False
                result['retries_attempted'] = attempt
                result['final_status'] = 'validation_error'
                return result

        # This shouldn't be reached, but just in case
        return all_attempts[-1] if all_attempts else {'success': False, 'error': 'No attempts made'}

    def _single_prediction(
        self, table_name: str, table_definition: str, context: str, previous_attempts: str, metadata_info: str
    ) -> Dict:
        """Make a single primary key prediction using metadata."""

        logger.info("Analyzing table schema and metadata patterns...")

        try:
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

            if hasattr(result, 'reasoning'):
                self._print_reasoning_formatted(result.reasoning)

            self._print_trace_if_available()

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
            else:
                if len(line) > 10 and any(
                    word in line.lower() for word in ['analyze', 'consider', 'look', 'notice', 'think']
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
