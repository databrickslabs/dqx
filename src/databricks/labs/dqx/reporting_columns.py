from typing import cast
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
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
    """Dataset-level checks can optionally add an info column to the result DataFrame.
    This function merges those info columns into a single struct column.

    Each listed column must be a struct (e.g. with an "anomaly" field). Their top-level fields
    are combined into one struct under dest_name. Columns in info_col_names that are missing
    from the DataFrame are skipped.

    If dest_name already exists, it is merged with the new info columns.

    Args:
        dest_name: Name of the output column (e.g. _dq_info).
        df: DataFrame that may contain the info columns.
        info_col_names: Names of the info columns to merge; None or empty means no merge.

    Returns:
        DataFrame with the new merged column and the source info columns removed.
    """
    info_cols = [c for c in (info_col_names or []) if c in df.columns]

    if not info_cols and dest_name not in df.columns:
        return df

    # Include existing dest + new info
    cols_to_merge = ([dest_name] if dest_name in df.columns else []) + info_cols
    field_exprs = [
        F.col(col_name)[field.name].alias(field.name)
        for col_name in cols_to_merge
        for field in cast(StructType, df.schema[col_name].dataType).fields
    ]
    return df.withColumn(dest_name, F.struct(*field_exprs)).drop(*info_cols)
