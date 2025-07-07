import logging
import re
import ast
from typing import Any

from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from databricks.labs.dqx.config import InputConfig, OutputConfig

logger = logging.getLogger(__name__)


STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
# catalog.schema.table or schema.table or database.table
TABLE_PATTERN = re.compile(r"^(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")
COLUMN_NORMALIZE_EXPRESSION = re.compile("[^a-zA-Z0-9]+")
COLUMN_PATTERN = re.compile(r"Column<'(.*?)(?: AS (\w+))?'>$")


def get_column_as_string(column: str | Column, normalize: bool = False) -> str:
    """
    Extracts the column alias or name from a PySpark Column expression.

    PySpark does not provide direct access to the alias of an unbound column, so this function
    parses the alias from the column's string representation.

    - Supports columns with one or multiple aliases.
    - Ensures the extracted expression is truncated to 255 characters.
    - Provides an optional normalization step for consistent naming.

    :param column: Column or string representing a column.
    :param normalize: If True, normalizes the column name (removes special characters, converts to lowercase).
    :return: The extracted column alias or name.
    :raises ValueError: If the column expression is invalid.
    """
    if isinstance(column, str):
        col_str = column
    else:
        # Extract the last alias or column name from the PySpark Column string representation
        match = COLUMN_PATTERN.search(str(column))
        if not match:
            raise ValueError(f"Invalid column expression: {column}")
        col_expr, alias = match.groups()
        if alias:
            return alias
        col_str = col_expr

        if normalize:
            col_str = normalize_col_str(col_str)

    return col_str


def normalize_col_str(col_str: str) -> str:
    """
    Normalizes string to be compatible with metastore column names by applying the following transformations:
    * remove special characters
    * convert to lowercase
    * limit the length to 255 characters to be compatible with metastore column names

    :param col_str: Column or string representing a column.
    :return: Normalized column name.
    """
    max_chars = 255
    return re.sub(COLUMN_NORMALIZE_EXPRESSION, "_", col_str[:max_chars].lower()).rstrip("_")


def read_input_data(
    spark: SparkSession,
    input_config: InputConfig,
) -> DataFrame:
    """
    Reads input data from the specified location and format.

    :param spark: SparkSession
    :param input_config: InputConfig with source location/table name, format, and options
    :return: DataFrame with values read from the input data
    """
    if not input_config.location:
        raise ValueError("Input location not configured")

    if TABLE_PATTERN.match(input_config.location):
        return _read_table_data(spark, input_config)

    if STORAGE_PATH_PATTERN.match(input_config.location):
        return _read_file_data(spark, input_config)

    raise ValueError(
        f"Invalid input location. It must be a 2 or 3-level table namespace or storage path, given {input_config.location}"
    )


def _read_file_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from files (e.g. JSON). Streaming reads must use auto loader with a 'cloudFiles' format.
    :param spark: SparkSession
    :param input_config: InputConfig with source location, format, and options
    :return: DataFrame with values read from the file data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).load(
            input_config.location, format=input_config.format, schema=input_config.schema
        )

    if input_config.format != "cloudFiles":
        raise ValueError("Streaming reads from file sources must use 'cloudFiles' format")

    return spark.readStream.options(**input_config.options).load(
        input_config.location, format=input_config.format, schema=input_config.schema
    )


def _read_table_data(spark: SparkSession, input_config: InputConfig) -> DataFrame:
    """
    Reads input data from a table registered in Unity Catalog.
    :param spark: SparkSession
    :param input_config: InputConfig with source location, format, and options
    :return: DataFrame with values read from the table data
    """
    if not input_config.is_streaming:
        return spark.read.options(**input_config.options).table(input_config.location)
    return spark.readStream.options(**input_config.options).table(input_config.location)


def deserialize_dicts(checks: list[dict[str, str]]) -> list[dict]:
    """
    Deserialize string fields instances containing dictionaries.
    This is needed as nested dictionaries from installation files are loaded as strings.
    @param checks: list of checks
    @return:
    """

    def parse_nested_fields(obj):
        """Recursively parse all string representations of dictionaries."""
        if isinstance(obj, str):
            if obj.startswith("{") and obj.endswith("}"):
                parsed_obj = ast.literal_eval(obj)
                return parse_nested_fields(parsed_obj)
            return obj
        if isinstance(obj, dict):
            return {k: parse_nested_fields(v) for k, v in obj.items()}
        return obj

    return [parse_nested_fields(check) for check in checks]


def save_dataframe_as_table(df: DataFrame, output_config: OutputConfig):
    """
    Helper method to save a DataFrame to a Delta table.
    :param df: The DataFrame to save
    :param output_config: Output table name, write mode, and options
    """
    logger.info(f"Saving data to {output_config.location} table")

    if df.isStreaming:
        if not output_config.trigger:
            query = (
                df.writeStream.format(output_config.format)
                .outputMode(output_config.mode)
                .options(**output_config.options)
                .toTable(output_config.location)
            )
        else:
            trigger: dict[str, Any] = output_config.trigger
            query = (
                df.writeStream.format(output_config.format)
                .outputMode(output_config.mode)
                .options(**output_config.options)
                .trigger(**trigger)
                .toTable(output_config.location)
            )
        query.awaitTermination()
    else:
        (
            df.write.format(output_config.format)
            .mode(output_config.mode)
            .options(**output_config.options)
            .saveAsTable(output_config.location)
        )


def is_sql_query_safe(query: str) -> bool:
    # Normalize the query by removing extra whitespace and converting to lowercase
    normalized_query = re.sub(r"\s+", " ", query).strip().lower()

    # Check for prohibited statements
    forbidden_statements = [
        "delete",
        "insert",
        "update",
        "drop",
        "truncate",
        "alter",
        "create",
        "replace",
        "grant",
        "revoke",
        "merge",
        "use",
        "refresh",
        "analyze",
        "optimize",
        "zorder",
    ]
    return not any(re.search(rf"\b{kw}\b", normalized_query) for kw in forbidden_statements)
