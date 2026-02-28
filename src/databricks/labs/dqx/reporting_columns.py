from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DefaultColumnNames(Enum):
    """Enum class to represent columns in the dataframe that will be used for error and warning reporting."""

    ERRORS = "_errors"
    WARNINGS = "_warnings"
    INFO = "_dq_info"


class ColumnArguments(Enum):
    """Enum class that is used as input parsing for custom column naming."""

    ERRORS = "errors"
    WARNINGS = "warnings"
    INFO = "info"


def merge_info_columns(dest_name: str, df: DataFrame, info_col_names: list[str] | None = None) -> DataFrame:
    """Merge dataset-level info columns into a single column as an array of structs.

    Each source column must be a struct with the shared wide schema (e.g. fields like
    ``anomaly``). Each such column becomes one element in the output array. Names in
    info_col_names that are not present in the DataFrame are skipped.

    The result is ``array<struct<...>>``. Element order matches the order of info_col_names
    (e.g. first anomaly check = dest_name[0], second = dest_name[1]). If dest_name already
    exists, it must be an array of structs; new structs are appended via concat.

    Args:
        dest_name: Name of the output column (e.g. _dq_info).
        df: DataFrame that may contain the info columns.
        info_col_names: Names of the info columns to merge; None or empty means no merge.

    Returns:
        DataFrame with the merged column (array of structs) and source info columns dropped.
    """
    info_cols = [c for c in (info_col_names or []) if c in df.columns]

    if not info_cols and dest_name not in df.columns:
        return df

    new_structs = F.array(*[F.col(c) for c in info_cols])
    if dest_name in df.columns:
        result_col = F.concat(F.col(dest_name), new_structs)
    else:
        result_col = new_structs

    return df.withColumn(dest_name, result_col).drop(*info_cols)
