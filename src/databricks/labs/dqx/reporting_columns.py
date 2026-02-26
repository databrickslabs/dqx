import re
import uuid
from typing import cast
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


COLLISION_FREE_COL_NAME_PATTERN = re.compile(r"^__dqx_info_[0-9a-f]{32}$")


class DefaultColumnNames(Enum):
    """Enum class to represent columns in the dataframe that will be used for error and warning reporting."""

    ERRORS = "_errors"
    WARNINGS = "_warnings"
    INFO = "_dq_info"


class InfoColumn:
    """Class handling optional info column"""

    @staticmethod
    def get_collision_free_name() -> str:
        """Return a new collision-safe info column name"""
        return f"__dqx_info_{uuid.uuid4().hex}"

    @staticmethod
    def merge_cols(dest_name: str, df: DataFrame) -> DataFrame:
        """Collect all info columns and merge into destination name"""
        info_cols = [c for c in df.columns if COLLISION_FREE_COL_NAME_PATTERN.match(c)]

        result_df = df
        if info_cols:
            field_exprs = [
                F.col(col_name)[field.name].alias(field.name)
                for col_name in info_cols
                for field in cast(StructType, df.schema[col_name].dataType).fields
            ]
            result_df = result_df.withColumn(dest_name, F.struct(*field_exprs))
            return result_df.drop(*info_cols)
        return result_df


class ColumnArguments(Enum):
    """Enum class that is used as input parsing for custom column naming."""

    ERRORS = "errors"
    WARNINGS = "warnings"
    INFO = "info"
