import logging
import inspect
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import Any
import json
import yaml
import dspy  # type: ignore
from pyspark.sql import SparkSession
from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.errors import DQXError
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.io import read_input_data

logger = logging.getLogger(__name__)


class TableManager:
    """Manages table operations for schema retrieval and metadata checking."""

    def __init__(self, spark: SparkSession | None = None):
        """Initialize with Spark session."""
        self.spark = SparkSession.builder.getOrCreate() if spark is None else spark

    def get_table_definition(self, table: str) -> str:
        """Retrieve table definition using Spark SQL DESCRIBE commands."""
        logger.info(f"🔍 Retrieving schema for table: {table}")

        definition_lines = self._get_table_columns(table)
        existing_pk = self._get_existing_primary_key(table)
        table_definition = self._build_table_definition_string(definition_lines, existing_pk)

        logger.info("✅ Table definition retrieved successfully")
        return table_definition

    def get_table_metadata_info(self, table: str) -> str:
        """Get additional metadata information to help with primary key detection."""
        try:
            metadata_info = []

            # Get table properties
            metadata_info.extend(self._get_table_properties(table))

            # Get column statistics
            metadata_info.extend(self._get_column_statistics(table))

            return (
                "Metadata information:\n" + "\n".join(metadata_info) if metadata_info else "Limited metadata available"
            )
        except Exception as e:
            logger.warning(f"Unexpected error retrieving metadata: {e}")
            return f"Could not retrieve metadata due to unexpected error: {e}"

    def get_table_column_names(self, table: str) -> list[str]:
        """Get table column names."""
        df = self.spark.table(table)
        return df.columns

    def run_sql(self, query: str):
        """Run a SQL query and return the result DataFrame."""
        return self.spark.sql(query)

    def _get_table_columns(self, table: str) -> list[str]:
        """Get table column definitions from DESCRIBE TABLE."""
        describe_query = f"DESCRIBE TABLE EXTENDED {table}"
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
                nullable = " NOT NULL" if "not null" in str(comment).lower() else ""
                definition_lines.append(f"    {col_name} {data_type}{nullable}")

        return definition_lines

    def _get_existing_primary_key(self, table: str) -> str | None:
        """Get existing primary key from table properties."""
        try:
            pk_query = f"SHOW TBLPROPERTIES {table}"
            pk_result = self.spark.sql(pk_query)
            pk_df = pk_result.toPandas()

            for _, row in pk_df.iterrows():
                if 'primary' in str(row.get('key', '')).lower():
                    return row.get('value', '')
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            pass
        return None

    @staticmethod
    def _build_table_definition_string(definition_lines: list[str], existing_pk: str | None) -> str:
        """Build the final table definition string."""
        table_definition = "{\n" + ",\n".join(definition_lines) + "\n}"
        if existing_pk:
            table_definition += f"\n-- Existing Primary Key: {existing_pk}"
        return table_definition

    @staticmethod
    def _extract_useful_properties(stats_df) -> list[str]:
        """Extract useful properties from table properties DataFrame."""
        metadata_info = []
        for _, row in stats_df.iterrows():
            key = row.get('key', '')
            value = row.get('value', '')
            if any(keyword in key.lower() for keyword in ('numrows', 'rawdatasize', 'totalsize', 'primary', 'unique')):
                metadata_info.append(f"{key}: {value}")
        return metadata_info

    def _get_table_properties(self, table: str) -> list[str]:
        """Get table properties metadata."""
        try:
            stats_query = f"SHOW TBLPROPERTIES {table}"
            stats_result = self.spark.sql(stats_query)
            stats_df = stats_result.toPandas()
            return self._extract_useful_properties(stats_df)
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            return []

    @staticmethod
    def _categorize_columns_by_type(col_df) -> tuple[list[str], list[str], list[str], list[str]]:
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

    @staticmethod
    def _format_column_distribution(
        numeric_cols: list[str], string_cols: list[str], date_cols: list[str], timestamp_cols: list[str]
    ) -> list[str]:
        """Format column type distribution information."""
        metadata_info = [
            "Column type distribution:",
            f"  Numeric columns ({len(numeric_cols)}): {', '.join(numeric_cols[:5])}",
            f"  String columns ({len(string_cols)}): {', '.join(string_cols[:5])}",
            f"  Date columns ({len(date_cols)}): {', '.join(date_cols)}",
            f"  Timestamp columns ({len(timestamp_cols)}): {', '.join(timestamp_cols)}",
        ]
        return metadata_info

    def _get_column_statistics(self, table) -> list[str]:
        """Get column statistics and type distribution."""
        try:
            col_stats_query = f"DESCRIBE TABLE EXTENDED {table}"
            col_result = self.spark.sql(col_stats_query)
            col_df = col_result.toPandas()

            numeric_cols, string_cols, date_cols, timestamp_cols = self._categorize_columns_by_type(col_df)
            return self._format_column_distribution(numeric_cols, string_cols, date_cols, timestamp_cols)
        except (ValueError, RuntimeError, KeyError):
            # Silently continue if table properties are not accessible
            return []


def get_check_function_definitions(custom_check_functions: dict[str, Callable] | None = None) -> list[dict[str, str]]:
    """
    A utility function to get the definition of all check functions.
    This function is primarily used to generate a prompt for the LLM to generate check functions.

    If provided, the function will use the custom check functions to resolve the check function.
    If not provided, the function will use only the built-in check functions.

    Args:
        custom_check_functions: A dictionary of custom check functions.

    Returns:
        list[dict]: A list of dictionaries, each containing the definition of a check function.
    """
    function_docs: list[dict[str, str]] = []
    for name, func_type in CHECK_FUNC_REGISTRY.items():
        func = resolve_check_function(name, custom_check_functions, fail_on_missing=False)
        if func is None:
            logger.warning(f"Check function {name} not found in the registry")
            continue
        sig = inspect.signature(func)
        doc = inspect.getdoc(func)
        function_docs.append(
            {
                "name": name,
                "type": func_type,
                "doc": doc or "",
                "signature": str(sig),
                "parameters": str(sig.parameters),
                "implementation": inspect.getsource(func),
            }
        )
    return function_docs


def get_required_check_functions_definitions(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dict[str, str]]:
    """
    Extract only required function information (name and doc).

    Returns:
        list[dict[str, str]]: A list of dictionaries containing the required fields for each check function.
    """
    required_function_docs: list[dict[str, str]] = []
    for func in get_check_function_definitions(custom_check_functions):
        # Tests showed that using function name and parameters alone yields better results
        # compared to full specification while reducing token count.
        # LLMs often dilute attention given too much specification.
        required_func_info = {
            "check_function_name": func.get("name", ""),
            "parameters": func.get("parameters", ""),
        }
        required_function_docs.append(required_func_info)
    return required_function_docs


def create_optimizer_training_set(custom_check_functions: dict[str, Callable] | None = None) -> list[dspy.Example]:
    """
    Get quality check training examples for the dspy optimizer.

    Args:
        custom_check_functions: A dictionary of custom check functions.

    Returns:
        list[dspy.Example]: A list of dspy.Example objects created from training examples.
    """
    training_examples = _load_training_examples()

    examples = []
    for example_data in training_examples:
        # Convert schema_info to JSON string format expected by dspy.Example
        schema_info_json = json.dumps(example_data["schema_info"])

        example = dspy.Example(
            schema_info=schema_info_json,
            business_description=example_data["business_description"],
            available_functions=json.dumps(get_required_check_functions_definitions(custom_check_functions)),
            quality_rules=example_data["quality_rules"],
            reasoning=example_data["reasoning"],
        ).with_inputs("schema_info", "business_description", "available_functions")

        examples.append(example)

    return examples


def get_column_metadata(spark: SparkSession, input_config: InputConfig) -> str:
    """
    Get the column metadata for a given table.

    Args:
        input_config (InputConfig): Input configuration for the table.
        spark (SparkSession): The Spark session used to access the table.

    Returns:
        str: A JSON string containing the column metadata with columns wrapped in a "columns" key.
    """
    df = read_input_data(spark, input_config)
    columns = [{"name": field.name, "type": field.dataType.simpleString()} for field in df.schema.fields]
    schema_info = {"columns": columns}
    return json.dumps(schema_info)


def _load_training_examples() -> list[dict[str, Any]]:
    """A function to load the training examples from the llm/resources/training_examples.yml file.

    Returns:
        list[dict[str, Any]]: Training examples as a list of dictionaries.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)

    if not isinstance(training_examples, list):
        raise DQXError("YAML file must contain a list at the root level.")

    return training_examples
