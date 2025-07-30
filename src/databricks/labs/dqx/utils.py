import json
import logging
import re
import ast
from typing import Any
import datetime

from pyspark.sql import Column, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.connect.column import Column as ConnectColumn
from databricks.labs.dqx.config import InputConfig, OutputConfig

logger = logging.getLogger(__name__)


STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
# catalog.schema.table or schema.table or database.table
TABLE_PATTERN = re.compile(r"^(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")
COLUMN_NORMALIZE_EXPRESSION = re.compile("[^a-zA-Z0-9]+")
COLUMN_PATTERN = re.compile(r"Column<'(.*?)(?: AS (\w+))?'>$")
INVALID_COLUMN_NAME_PATTERN = re.compile(r"[\s,;{}\(\)\n\t=]+")


def get_column_name_or_alias(column: str | Column | ConnectColumn, normalize: bool = False) -> str:
    """
    Extracts the column alias or name from a PySpark Column or ConnectColumn expression.

    PySpark does not provide direct access to the alias of an unbound column, so this function
    parses the alias from the column's string representation.

    - Supports columns with one or multiple aliases.
    - Ensures the extracted expression is truncated to 255 characters.
    - Provides an optional normalization step for consistent naming.

    :param column: Column, ConnectColumn or string representing a column.
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


def get_columns_as_string(columns: list[str | Column]) -> list[str]:
    """
    Extracts column names from a list of PySpark Column or ConnectColumn expressions.

    This function processes each column, ensuring that only valid column names are returned.

    :param columns: List of columns, ConnectColumns or strings representing columns.
    :return: List of column names as strings.
    """
    valid_str_columns = []
    for col in columns:
        col_str = get_column_name_or_alias(col) if not isinstance(col, str) else col
        validate_column_string(col_str)
        valid_str_columns.append(col_str)
    return valid_str_columns


def validate_column_string(col_name: str) -> None:
    """
    Returns True if the column name does not contain any disallowed characters:
    space, comma, semicolon, curly braces, parentheses, newline, tab, or equals sign.

    :param col_name: Column name to validate.
    :raises ValueError: If the column name contains disallowed characters.
    """
    not_valid = bool(INVALID_COLUMN_NAME_PATTERN.search(col_name))

    if not_valid:
        raise ValueError(
            "Unable to interpret column expression. Only simple references are allowed, e.g: F.col('name')"
        )


def normalize_bound_args(val: Any, normalize: bool = False) -> Any:
    """
    Normalize a value or collection of values for consistent processing.

    Handles primitives, dates, and column-like objects. Lists, tuples, and sets are
    recursively normalized with type preserved.

    :param val: Value or collection of values to normalize.
    :param normalize: Whether to normalize column-like string representations.
    :return: Normalized value or collection.
    :raises ValueError: If a column resolves to an invalid name.
    :raises TypeError: If a column type is unsupported.
    """
    if isinstance(val, (list, tuple, set)):
        normalized = [normalize_bound_args(v, normalize) for v in val]
        return normalized

    if isinstance(val, (str, int, float, bool)):
        return val

    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)

    if isinstance(val, (Column, ConnectColumn)):
        col_str = get_column_name_or_alias(val, normalize)
        validate_column_string(col_str)
        return col_str
    raise TypeError(f"Unsupported type for normalization: {type(val).__name__}")


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


def safe_json_load(value: str):
    """
    Safely load a JSON string, returning the original value if it fails to parse.
    This allows to specify string value without a need to escape the quotes.

    :param value: The value to parse as JSON.
    """
    try:
        return json.loads(value)  # load as json if possible
    except json.JSONDecodeError:
        return value
