import re
import ast
from pyspark.sql import Column
from pyspark.sql import SparkSession


STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
UNITY_CATALOG_TABLE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")
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
        max_chars = 255  # limit the string from expr so that the result can be safely used as Unity Catalog column name
        col_str = alias if alias else col_expr[:max_chars]

    return re.sub(COLUMN_NORMALIZE_EXPRESSION, "_", col_str.lower()).rstrip("_") if normalize else col_str


def read_input_data(spark: SparkSession, input_location: str | None, input_format: str | None):
    """
    Reads input data from the specified location and format.

    :param spark: SparkSession
    :param input_location: The input data location.
    :param input_format: The input data format.
    """
    if not input_location:
        raise ValueError("Input location not configured")

    if UNITY_CATALOG_TABLE_PATTERN.match(input_location):
        return spark.read.table(input_location)  # must provide 3-level Unity Catalog namespace

    if STORAGE_PATH_PATTERN.match(input_location):
        if not input_format:
            raise ValueError("Input format not configured")
        # TODO handle spark options while reading data from a file location
        # https://github.com/databrickslabs/dqx/issues/161
        return spark.read.format(str(input_format)).load(input_location)

    raise ValueError(
        f"Invalid input location. It must be Unity Catalog table / view or storage location, " f"given {input_location}"
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
