import re
from pyspark.sql import Column
from pyspark.sql import SparkSession


STORAGE_PATH_PATTERN = re.compile(r"^(/|s3:/|abfss:/|gs:/)")
UNITY_CATALOG_TABLE_PATTERN = re.compile(r"^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$")


def get_column_name(col: Column) -> str:
    """
    PySpark doesn't allow to directly access the column name with respect to aliases from an unbound column.
    It is necessary to parse this out from the string representation.

    This works on columns with one or more aliases as well as not aliased columns.

    :param col: Column
    :return: Col name alias as str
    """
    return str(col).removeprefix("Column<'").removesuffix("'>").split(" AS ")[-1]


def read_input_data(spark: SparkSession, input_location: str | None, input_format: str | None):
    if not input_location:
        raise ValueError("Input location not configured")

    if UNITY_CATALOG_TABLE_PATTERN.match(input_location):
        return spark.read.table(input_location)  # must provide 3-level Unity Catalog namespace

    if STORAGE_PATH_PATTERN.match(input_location):
        if not input_format:
            raise ValueError("Input format not configured")
        return spark.read.format(str(input_format)).load(input_location)

    raise ValueError(f"Input location must be Unity Catalog table / view or storage location, given {input_location}")
