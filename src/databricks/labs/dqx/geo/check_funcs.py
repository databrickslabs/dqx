from pyspark.sql import Column
import pyspark.sql.functions as F
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_normalized_column_and_expr


@register_rule("row")
def is_valid_geometry(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geometries
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in databricks as of 2025-09-19.
    # TODO: `pyspark.sql.functions.try_to_geometry` is not (yet) available. Replace with 
    # `pyspark.sql.functions.try_to_geometry` when available in OSS PySpark.
    geometry_col = F.expr(f"try_to_geometry({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"' in Column '{col_expr_str}' is not a valid geometry"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_geometry",
    )

@register_rule("row")
def is_valid_geography(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geographies.

    Args:
        column: column to check; can be a string column name or a column expression
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in databricks as of 2025-09-19.
    # TODO: `pyspark.sql.functions.try_to_geography` is not (yet) available. Replace with 
    # `pyspark.sql.functions.try_to_geography` when available in OSS PySpark.
    geometry_col = F.expr(f"try_to_geography({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"' in Column '{col_expr_str}' is not a valid geometry"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_geography",
    )