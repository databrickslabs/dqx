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
def is_latitude(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid latitudes.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid latitudes
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    condition = ~F.when(col_expr.isNull(), F.lit(None)).otherwise(
        F.col(col_str_norm).try_cast("double").between(-90.0, 90.0)
    )
    condition_str = f"` in column `{col_expr_str}` is not a valid latitude (must be between -90 and 90)"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
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
    condition = ~F.when(col_expr.isNull(), F.lit(None)).otherwise(
        F.col(col_str_norm).try_cast("double").between(-180.0, 180.0)
    )
    condition_str = f"` in column `{col_expr_str}` is not a valid longitude (must be between -180 and 180)"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_longitude",
    )


@register_rule("row")
def is_geometry(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` function.
    geometry_col = F.expr(f"try_to_geometry({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"` in column `{col_expr_str}` is not a geometry"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_geometry",
    )


@register_rule("row")
def is_geography(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geographies.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geographies

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geography` function.
    geometry_col = F.expr(f"try_to_geography({col_str_norm})")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geometry_col.isNull())
    condition_str = f"` in column `{col_expr_str}` is not a geography"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_geography",
    )


@register_rule("row")
def is_point(column: str | Column) -> Column:
    """Checks whether the values in the input column are point geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are point geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{POINT_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a point geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_point",
    )


@register_rule("row")
def is_linestring(column: str | Column) -> Column:
    """Checks whether the values in the input column are linestring geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are linestring geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{LINESTRING_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a linestring geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_linestring",
    )


@register_rule("row")
def is_polygon(column: str | Column) -> Column:
    """Checks whether the values in the input column are polygon geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are polygon geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{POLYGON_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a polygon geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_polygon",
    )


@register_rule("row")
def is_multipoint(column: str | Column) -> Column:
    """Checks whether the values in the input column are multipoint geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multipoint geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTIPOINT_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multipoint geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_multipoint",
    )


@register_rule("row")
def is_multilinestring(column: str | Column) -> Column:
    """Checks whether the values in the input column are multilinestring geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multilinestring geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTILINESTRING_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multilinestring geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_multilinestring",
    )


@register_rule("row")
def is_multipolygon(column: str | Column) -> Column:
    """Checks whether the values in the input column are multipolygon geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are multipolygon geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{MULTIPOLYGON_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a multipolygon geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_multipolygon",
    )


@register_rule("row")
def is_geometrycollection(column: str | Column) -> Column:
    """Checks whether the values in the input column are geometrycollection geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are geometrycollection geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_geometrytype` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_geometrytype(try_to_geometry({col_str_norm})) <> '{GEOMETRYCOLLECTION_TYPE}'")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a geometrycollection geometry"
    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_geometrycollection",
    )


@register_rule("row")
def is_ogc_valid(column: str | Column) -> Column:
    """Checks whether the values in the input column are valid geometries in the OGC sense.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are valid geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_isvalid` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"NOT st_isvalid(try_to_geometry({col_str_norm}))")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is not a valid geometry (in the OGC sense)"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_not_valid_geometry",
    )


@register_rule("row")
def is_non_empty_geometry(column: str | Column) -> Column:
    """Checks whether the values in the input column are empty geometries.

    Args:
        column: column to check; can be a string column name or a column expression

    Returns:
        Column object indicating whether the values in the input column are empty geometries

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_isempty` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_isempty(try_to_geometry({col_str_norm}))")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` is an empty geometry"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_is_empty_geometry",
    )


@register_rule("row")
def has_dimension(column: str | Column, dimension: int) -> Column:
    """Checks whether the geometries/geographies in the input column have a given dimension.

    Args:
        column: column to check; can be a string column name or a column expression
        dimension: required dimension of the geometries/geographies

    Returns:
        Column object indicating whether the geometries/geographies in the input column have a given dimension

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry` and `st_dimension` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(f"st_dimension(try_to_geometry({col_str_norm})) <> {dimension}")
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` does not have the required dimension ({dimension})"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_does_not_have_required_geo_dimension",
    )


@register_rule("row")
def has_x_coordinate_between(column: str | Column, min_value: float, max_value: float) -> Column:
    """Checks whether the x coordinates of the geometries in the input column are between a given range.

    Args:
        column: column to check; can be a string column name or a column expression
        min_value: minimum value of the x coordinates
        max_value: maximum value of the x coordinates

    Returns:
        Column object indicating whether the x coordinates of the geometries in the input column are between a given range

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry`, `st_xmax` and `st_xmin` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(
        f"st_xmax(try_to_geometry({col_str_norm})) > {max_value} OR st_xmin(try_to_geometry({col_str_norm})) < {min_value}"
    )
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` has x coordinates outside the range [{min_value}, {max_value}]"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_has_x_coordinates_outside_range",
    )


@register_rule("row")
def has_y_coordinate_between(column: str | Column, min_value: float, max_value: float) -> Column:
    """Checks whether the y coordinates of the geometries in the input column are between a given range.

    Args:
        column: column to check; can be a string column name or a column expression
        min_value: minimum value of the y coordinates
        max_value: maximum value of the y coordinates

    Returns:
        Column object indicating whether the y coordinates of the geometries in the input column are between a given range

    Note:
        This function requires Databricks serverless compute or runtime 17.1 or above.
    """
    col_str_norm, col_expr_str, col_expr = _get_normalized_column_and_expr(column)
    # NOTE: This function is currently only available in Databricks runtime 17.1 or above or in
    #   Databricks SQL, due to the use of the `try_to_geometry`, `st_ymax` and `st_ymin` functions.
    geom_cond = F.expr(f"try_to_geometry({col_str_norm}) IS NULL")
    geom_type_cond = F.expr(
        f"st_ymax(try_to_geometry({col_str_norm})) > {max_value} OR st_ymin(try_to_geometry({col_str_norm})) < {min_value}"
    )
    condition = F.when(col_expr.isNull(), F.lit(None)).otherwise(geom_cond | geom_type_cond)
    condition_str = f"` in column `{col_expr_str}` has y coordinates outside the range [{min_value}, {max_value}]"

    return make_condition(
        condition,
        F.concat_ws("", F.lit("value `"), col_expr.cast("string"), F.lit(condition_str)),
        f"{col_str_norm}_has_y_coordinates_outside_range",
    )
