"""Helpers for anomaly scoring: row filter, join results, reserved column checks."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from databricks.labs.dqx.errors import InvalidParameterError


def check_reserved_row_id_columns(df: DataFrame) -> None:
    """Raise if DataFrame has reserved _dqx_row_id / __dqx_row_id columns."""
    reserved_prefixes = ("_dqx_row_id", "__dqx_row_id")
    for col_name in df.columns:
        if col_name.startswith(reserved_prefixes) or col_name == "_dqx_row_id":
            raise InvalidParameterError(
                f"Input DataFrame must not contain reserved column '{col_name}'. "
                "Rename or drop this column before running the anomaly check."
            )


def join_filtered_results_back(
    df: DataFrame,
    result: DataFrame,
    merge_columns: list[str],
    score_col: str,
    info_col: str,
) -> DataFrame:
    """Left-join scored result onto df so every input row is preserved.

    Rows that were scored get score/info; rows that were not (e.g. filtered out by
    row_filter) get null. merge_columns (e.g. row_id) must exist on both df and result.
    """
    score_cols_to_join = [score_col, info_col]
    scored_subset = result.select(*merge_columns, *score_cols_to_join)

    agg_exprs = [
        F.max(score_col).alias(score_col),
        F.max_by(info_col, score_col).alias(info_col),
    ]
    scored_subset_unique = scored_subset.groupBy(*merge_columns).agg(*agg_exprs)

    return df.join(scored_subset_unique, on=merge_columns, how="left")


def apply_row_filter(df: DataFrame, row_filter: str | None) -> DataFrame:
    """Return only rows that match row_filter for scoring; if no filter, return df unchanged.

    row_filter is a SQL expression (e.g. \"region = 'US'\"). Only these rows are run
    through anomaly detection; elsewhere we join results back so output has same row count.
    """
    return df.filter(F.expr(row_filter)) if row_filter else df
