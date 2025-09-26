from pyspark.sql import Column
import pyspark.sql.functions as F
from databricks.labs.dqx.rule import register_rule
from databricks.labs.dqx.check_funcs import make_condition, _get_normalized_column_and_expr

POINT_TYPE = "ST_Point"
LINESTRING_TYPE = "ST_LineString"
POLYGON_TYPE = "ST_Polygon"
MULTIPOINT_TYPE = "ST_MultiPoint"
MULTILINESTRING_TYPE = "ST_MultiLineString"
MULTIPOLYGON_TYPE = "ST_MultiPolygon"
GEOMETRYCOLLECTION_TYPE = "ST_GeometryCollection"


@register_rule("row")
def is_geometry(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` function.
    # TODO: `pyspark.sql.functions.try_to_geometry` is not (yet) available. Replace with
    #   `pyspark.sql.functions.try_to_geometry` when available in OSS PySpark.
    geometry_col = F.expr(f"try_to_geometry({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"` in column `{col_expr_str}` is not a geometry"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_geometry",
    )


@register_rule("row")
def is_geography(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geographies.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geographies

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geography` function.
    # TODO: `pyspark.sql.functions.try_to_geography` is not (yet) available. Replace with
    #   `pyspark.sql.functions.try_to_geography` when available in OSS PySpark.
    geometry_col = F.expr(f"try_to_geography({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"` in column `{col_expr_str}` is not a geography"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_geography",
    )


@register_rule("row")
def is_point(column: str | Column) -> Column:
    """Checks whether the values in the input column are point geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are point geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{POINT_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a point geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_point",
    )


@register_rule("row")
def is_linestring(column: str | Column) -> Column:
    """Checks whether the values in the input column are linestring geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are linestring geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{LINESTRING_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a linestring geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_linestring",
    )


@register_rule("row")
def is_polygon(column: str | Column) -> Column:
    """Checks whether the values in the input column are polygon geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are polygon geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{POLYGON_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a polygon geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_polygon",
    )


@register_rule("row")
def is_multipoint(column: str | Column) -> Column:
    """Checks whether the values in the input column are multipoint geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multipoint geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTIPOINT_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multipoint geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_multipoint",
    )


@register_rule("row")
def is_multilinestring(column: str | Column) -> Column:
    """Checks whether the values in the input column are multilinestring geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multilinestring geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTILINESTRING_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multilinestring geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_multilinestring",
    )


@register_rule("row")
def is_multipolygon(column: str | Column) -> Column:
    """Checks whether the values in the input column are multipolygon geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multipolygon geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTIPOLYGON_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multipolygon geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_multipolygon",
    )


@register_rule("row")
def is_geometrycollection(column: str | Column) -> Column:
    """Checks whether the values in the input column are geometrycollection geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are geometrycollection geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{GEOMETRYCOLLECTION_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a geometrycollection geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_geometrycollection",
    )


@register_rule("row")
def is_ogc_valid(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geometries in the OGC sense.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geometries

    Note:
        This function requires Databricks runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_isvalid` functions.
    # TODO: Above mentioned functions are not (yet) available. Replace with equivalent functions
    #   when available in OSS PySpark.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"NOT st_isvalid(try_to_geometry({col_str_norm}))")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a valid geometry (in the OGC sense)"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_a_valid_geometry",
    )


@register_rule("row")
def is_latitude(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid latitudes.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid latitudes
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(F.col(col_str_norm).between(-90.0, 90.0))
    condition_str = f"' in Column '{col_expr_str}' is not a valid latitude must be between -90 and 90"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_latitude",
    )


@register_rule("row")
def is_longitude(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid longitudes.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid longitudes
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(F.col(col_str_norm).between(-180.0, 180.0))
    condition_str = f"' in Column '{col_expr_str}' is not a valid longitude (must be between -180 and 180)"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("Value '"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_longitude",
    )
