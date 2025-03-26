import re
import ast
from pyspark.sql import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


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
        max_chars = 255  # limit the length so that we can safely use it as a column name in a metastore
        return re.sub(COLUMN_NORMALIZE_EXPRESSION, "_", col_str[:max_chars].lower()).rstrip("_")

    return col_str


def read_input_data(
    spark: SparkSession,
    input_location: str | None,
    input_format: str | None = None,
    input_schema: str | None = None,
    input_read_options: dict[str, str] | None = None,
) -> DataFrame:
    """
    Reads input data from the specified location and format.

    :param spark: SparkSession
    :param input_location: The input data location (2 or 3-level namespace table or a path).
    :param input_format: The input data format, e.g. delta, parquet, csv, json
    :param input_schema: The schema to use to read the input data (applicable to json and csv files), e.g. col1 int, col2 string
    :param input_read_options: Additional read options to pass to the DataFrame reader, e.g. {"header": "true"}
    :return: DataFrame
    """
    if not input_location:
        raise ValueError("Input location not configured")

    if not input_read_options:
        input_read_options = {}

    if TABLE_PATTERN.match(input_location):
        return spark.read.options(**input_read_options).table(input_location)

    if STORAGE_PATH_PATTERN.match(input_location):
        if not input_format:
            raise ValueError("Input format not configured")
        return (
            spark.read.options(**input_read_options).format(str(input_format)).load(input_location, schema=input_schema)
        )

    raise ValueError(
        f"Invalid input location. It must be a 2 or 3-level table namespace or storage path, given {input_location}"
    )


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
