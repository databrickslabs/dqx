# databricks.labs.dqx.geo.check_funcs

### is_latitude[​](#is_latitude "Direct link to is_latitude")

```python
@register_rule("row")
def is_latitude(column: str | Column) -> Column

```

Checks whether the values in the input column are valid latitudes.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid latitudes

### is_longitude[​](#is_longitude "Direct link to is_longitude")

```python
@register_rule("row")
def is_longitude(column: str | Column) -> Column

```

Checks whether the values in the input column are valid longitudes.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid longitudes

### is_geometry[​](#is_geometry "Direct link to is_geometry")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geometry(column: str | Column) -> Column

```

Checks whether the values in the input column are valid geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_geography[​](#is_geography "Direct link to is_geography")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geography(column: str | Column) -> Column

```

Checks whether the values in the input column are valid geographies.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid geographies

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_point[​](#is_point "Direct link to is_point")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_point(column: str | Column, convert_column: bool = True) -> Column

```

Checks whether the values in the input column are point geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are point geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_linestring[​](#is_linestring "Direct link to is_linestring")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_linestring(column: str | Column, convert_column: bool = True) -> Column

```

Checks whether the values in the input column are linestring geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are linestring geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_polygon[​](#is_polygon "Direct link to is_polygon")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_polygon(column: str | Column, convert_column: bool = True) -> Column

```

Checks whether the values in the input column are polygon geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are polygon geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_multipoint[​](#is_multipoint "Direct link to is_multipoint")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_multipoint(column: str | Column, convert_column: bool = True) -> Column

```

Checks whether the values in the input column are multipoint geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are multipoint geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_multilinestring[​](#is_multilinestring "Direct link to is_multilinestring")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_multilinestring(column: str | Column,
                       convert_column: bool = True) -> Column

```

Checks whether the values in the input column are multilinestring geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are multilinestring geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_multipolygon[​](#is_multipolygon "Direct link to is_multipolygon")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_multipolygon(column: str | Column,
                    convert_column: bool = True) -> Column

```

Checks whether the values in the input column are multipolygon geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are multipolygon geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_geometrycollection[​](#is_geometrycollection "Direct link to is_geometrycollection")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geometrycollection(column: str | Column,
                          convert_column: bool = True) -> Column

```

Checks whether the values in the input column are geometrycollection geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are geometrycollection geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_ogc_valid[​](#is_ogc_valid "Direct link to is_ogc_valid")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_ogc_valid(column: str | Column, convert_column: bool = True) -> Column

```

Checks whether the values in the input column are valid geometries in the OGC sense.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY value. GEOGRAPHY is not supported (*st_isvalid* accepts GEOMETRY only). A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are valid geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_non_empty_geometry[​](#is_non_empty_geometry "Direct link to is_non_empty_geometry")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_non_empty_geometry(column: str | Column,
                          convert_column: bool = True) -> Column

```

Checks whether the values in the input column are empty geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value. A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are empty geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_not_null_island[​](#is_not_null_island "Direct link to is_not_null_island")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_not_null_island(column: str | Column,
                       convert_column: bool = True) -> Column

```

Checks whether the values in the input column are NULL island geometries (e.g. POINT(0 0), POINTZ(0 0 0), or POINTZM(0 0 0 0)).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY value. GEOGRAPHY is not supported (*st\_x*/*st\_y* accept POINT GEOMETRY only). A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the values in the input column are NULL island geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has_dimension[​](#has_dimension "Direct link to has_dimension")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def has_dimension(column: str | Column,
                  dimension: int,
                  convert_column: bool = True) -> Column

```

Checks whether the geometries in the input column have a given dimension.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `dimension` - required dimension of the geometries
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY value. GEOGRAPHY is not supported (*st_dimension* accepts GEOMETRY only). A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the geometries in the input column have a given dimension

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has_x_coordinate_between[​](#has_x_coordinate_between "Direct link to has_x_coordinate_between")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def has_x_coordinate_between(column: str | Column,
                             min_value: float,
                             max_value: float,
                             convert_column: bool = True) -> Column

```

Checks whether the x coordinates of the geometries in the input column are between a given range.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_value` - minimum value of the x coordinates
* `max_value` - maximum value of the x coordinates
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY value. GEOGRAPHY is not supported (*st_xmin*/*st_xmax* accept GEOMETRY only). A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the x coordinates of the geometries in the input column are between a given range

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has_y_coordinate_between[​](#has_y_coordinate_between "Direct link to has_y_coordinate_between")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def has_y_coordinate_between(column: str | Column,
                             min_value: float,
                             max_value: float,
                             convert_column: bool = True) -> Column

```

Checks whether the y coordinates of the geometries in the input column are between a given range.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_value` - minimum value of the y coordinates
* `max_value` - maximum value of the y coordinates
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY value. GEOGRAPHY is not supported (*st_ymin*/*st_ymax* accept GEOMETRY only). A null value is skipped; a non-null value that fails to parse is reported as unparseable.

**Returns**:

Column object indicating whether the y coordinates of the geometries in the input column are between a given range

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_area_equal_to[​](#is_area_equal_to "Direct link to is_area_equal_to")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_area_equal_to(column: str | Column,
                     value: int | float | str | Column,
                     srid: int | None = 3857,
                     geodesic: bool = False,
                     convert_column: bool = True) -> Column

```

Checks if the areas of values in a geometry or geography column are equal to a specified value. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the area the geometries in the input column are equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_area_not_equal_to[​](#is_area_not_equal_to "Direct link to is_area_not_equal_to")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_area_not_equal_to(column: str | Column,
                         value: int | float | str | Column,
                         srid: int | None = 3857,
                         geodesic: bool = False,
                         convert_column: bool = True) -> Column

```

Checks if the areas of values in a geometry column are not equal to a specified value. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the area the geometries in the input column are not equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_area_not_greater_than[​](#is_area_not_greater_than "Direct link to is_area_not_greater_than")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_area_not_greater_than(column: str | Column,
                             value: int | float | str | Column,
                             srid: int | None = 3857,
                             geodesic: bool = False,
                             convert_column: bool = True) -> Column

```

Checks if the areas of values in a geometry column are not greater than a specified limit. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the area the geometries in the input column is greater than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_area_not_less_than[​](#is_area_not_less_than "Direct link to is_area_not_less_than")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_area_not_less_than(column: str | Column,
                          value: int | float | str | Column,
                          srid: int | None = 3857,
                          geodesic: bool = False,
                          convert_column: bool = True) -> Column

```

Checks if the areas of values in a geometry column are not less than a specified limit. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the area the geometries in the input column is less than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_num_points_equal_to[​](#is_num_points_equal_to "Direct link to is_num_points_equal_to")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_num_points_equal_to(column: str | Column,
                           value: int | float | str | Column,
                           convert_column: bool = True) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is equal to a specified value.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_num_points_not_equal_to[​](#is_num_points_not_equal_to "Direct link to is_num_points_not_equal_to")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_num_points_not_equal_to(column: str | Column,
                               value: int | float | str | Column,
                               convert_column: bool = True) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is not equal to a specified value.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is not equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_num_points_not_greater_than[​](#is_num_points_not_greater_than "Direct link to is_num_points_not_greater_than")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_num_points_not_greater_than(column: str | Column,
                                   value: int | float | str | Column,
                                   convert_column: bool = True) -> Column

```

Checks if the number of coordinate pairs in the values of a geometry column is not greater than a specified limit.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is greater than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_num_points_not_less_than[​](#is_num_points_not_less_than "Direct link to is_num_points_not_less_than")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_num_points_not_less_than(column: str | Column,
                                value: int | float | str | Column,
                                convert_column: bool = True) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is not less than a specified limit.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `convert_column` - When True (default), the column value is parsed with *try_to_geometry* (WKT, WKB, EWKT, EWKB, or GeoJSON). When False, the column is treated as already holding a native GEOMETRY/GEOGRAPHY value.

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is less than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### are_polygons_mutually_disjoint[​](#are_polygons_mutually_disjoint "Direct link to are_polygons_mutually_disjoint")

```python
@requires_dbr_version("17.1")
@row_preserving
@register_rule("dataset")
def are_polygons_mutually_disjoint(
        column: str | Column,
        row_filter: str | None = None) -> tuple[Column, Callable]

```

Checks if the polygons in a geometry column are mutually disjoint. By default, native Spark spatial intersections are used.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `row_filter` - Optional SQL expression to filter rows before checking for intersections.

**Returns**:

A tuple of:

* A Spark Column representing the condition for mutual disjoint violations.
* A closure that applies the polygon intersection check and adds the condition column to the DataFrame.

**Notes**:

This function requires Databricks runtime 17.1 or above. This check performs a self-join over all valid polygons in the input DataFrame, which is O(n²). Use `row_filter` to limit the rows evaluated on large tables. Photon activation is suggested to use optimised spatial computation.

### is_geo_contains[​](#is_geo_contains "Direct link to is_geo_contains")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_contains(column: str | Column,
                    reference_geometry: str | bytes | Column,
                    convert_column: bool = False,
                    convert_reference_geometry: bool = False) -> Column

```

Checks if the reference geometry contains the column geometry using `st_contains` with meter-level precision.

A geometry A *contains* geometry B when B lies entirely within the interior of A, with no boundary points of B lying on the boundary of A. Points on the shared boundary are not considered contained; use *is_geo_covers* for boundary-inclusive coverage checks.

Both the target column and the reference geometry are always handled as `GEOMETRY`. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied to parse the value from any supported format (WKT, WKB, EWKT, EWKB). See <https://docs.databricks.com/aws/en/sql/language-manual/functions/try_to_geometry> for details. When conversion is not requested, the input is assumed to already hold a native `GEOMETRY` value. A null value is skipped; a non-null value or reference that fails to parse is reported as unparseable.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation.
* `reference_geometry` - Reference geometry as a literal WKT/WKB/EWKT/EWKB string or bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name.
* `convert_column` - When True, *try_to_geometry* is applied to convert the column values to GEOMETRY. When False (default), the column is assumed to already hold a native GEOMETRY value.
* `convert_reference_geometry` - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. When False (default), the reference geometry is assumed to already hold a native GEOMETRY value.

**Returns**:

Column object indicating whether values in the input column are not contained by the reference geometry.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_geo_covers[​](#is_geo_covers "Direct link to is_geo_covers")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_covers(column: str | Column,
                  reference_geometry: str | bytes | Column,
                  precise: bool = False,
                  resolution: int | Column | None = None,
                  convert_column: bool = False,
                  convert_reference_geometry: bool = False) -> Column

```

Checks if the reference geometry covers the column geometry.

When *precise* is True, uses `st_covers` for exact computation with meter-level precision. A geometry A *covers* geometry B when every point of B lies within A, including boundary points. This differs from `st_contains`, which excludes points on A's boundary.

When *precise* is False (default), approximates coverage using H3 cell indexing: all hexagonal cells that represent the column geometry must also be present in the H3 cells of the reference geometry. Higher *resolution* values give finer precision at the cost of more cells. Because H3 cells are discrete approximations, boundary behaviour differs from the exact `st_covers` predicate and geometries near the boundary may be misclassified.

Both the target column and the reference geometry are always handled as `GEOMETRY` in precise mode. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied. These flags are ignored in approximate mode. In both modes a null value is skipped and a non-null value or reference that fails to parse is reported as unparseable. In approximate mode an empty column geometry is skipped, while a column value with an SRID other than 0 or 4326 (WGS 84) or any other column value with no H3 representation (e.g. a multipoint) is reported, and a reference with no H3 representation (e.g. an empty polygon, a multipoint or a non-WGS 84 SRID) is reported on every row. Points, linestrings, polygons, multilinestrings and multipolygons have H3 cells.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation.
* `reference_geometry` - Reference geometry as a literal WKT/WKB/EWKT/EWKB string or bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name. Bytes (WKB) are only supported in precise mode.
* `precise` - When True, uses exact `st_covers` computation. When False (default), uses the H3 approximate method which requires *resolution*.
* `resolution` - H3 resolution integer (0–15) or column reference. Required when *precise* is False. Higher values give finer precision at the cost of more cells.
* `convert_column` - When True, *try_to_geometry* is applied to convert column values to GEOMETRY. Only used in precise mode.
* `st_contains`0 - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. Only used in precise mode.

**Returns**:

Column object indicating whether values in the input column are not covered by the reference geometry.

**Raises**:

* `st_contains`1 - If *precise* is False and *resolution* is not provided or is outside 0–15.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above if `precise` is True.

### is_geo_intersects[​](#is_geo_intersects "Direct link to is_geo_intersects")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_intersects(column: str | Column,
                      reference_geometry: str | bytes | Column,
                      precise: bool = False,
                      resolution: int | Column | None = None,
                      convert_column: bool = False,
                      convert_reference_geometry: bool = False) -> Column

```

Checks if the column geometry intersects the reference geometry.

When *precise* is True, uses `st_intersects` for exact computation with meter-level precision. Two geometries intersect when they share at least one point, whether interior or boundary.

When *precise* is False (default), approximates intersection using H3 cell indexing: at least one hexagonal cell must be shared between the H3 representations of the column and reference geometries. Higher *resolution* values give finer precision at the cost of more cells. Because H3 cells are discrete approximations, boundary behaviour differs from the exact `st_intersects` predicate.

Both the target column and the reference geometry are always handled as `GEOMETRY` in precise mode. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied. These flags are ignored in approximate mode. In both modes a null value is skipped and a non-null value or reference that fails to parse is reported as unparseable. In approximate mode an empty column geometry is skipped, while a column value with an SRID other than 0 or 4326 (WGS 84) or any other column value with no H3 representation (e.g. a multipoint) is reported, and a reference with no H3 representation (e.g. an empty polygon, a multipoint or a non-WGS 84 SRID) is reported on every row. Points, linestrings, polygons, multilinestrings and multipolygons have H3 cells.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation.
* `reference_geometry` - Reference geometry as a literal WKT/WKB/EWKT/EWKB string or bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name. Bytes (WKB) are only supported in precise mode.
* `precise` - When True, uses exact `st_intersects` computation. When False (default), uses the H3 approximate method which requires *resolution*.
* `resolution` - H3 resolution integer (0–15) or column reference. Required when *precise* is False. Higher values give finer precision at the cost of more cells.
* `convert_column` - When True, *try_to_geometry* is applied to convert column values to GEOMETRY. Only used in precise mode.
* `convert_reference_geometry` - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. Only used in precise mode.

**Returns**:

Column object indicating whether values in the input column do not intersect the reference geometry.

**Raises**:

* `st_intersects`0 - If *precise* is False and *resolution* is not provided or is outside 0–15.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above if `precise` is True.

### is_geo_touches[​](#is_geo_touches "Direct link to is_geo_touches")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_touches(column: str | Column,
                   reference_geometry: str | bytes | Column,
                   convert_column: bool = False,
                   convert_reference_geometry: bool = False) -> Column

```

Checks if the column geometry touches the reference geometry using `st_touches` with meter-level precision.

Two geometries *touch* when they share at least one boundary point but their interiors do not intersect. A point strictly inside a polygon does not touch it; a point on the polygon boundary does.

Both the target column and the reference geometry are always handled as `GEOMETRY`. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied to parse the value from any supported format (WKT, WKB, EWKT, EWKB). When conversion is not requested, the input is assumed to already hold a native `GEOMETRY` value. A null value is skipped; a non-null value or reference that fails to parse is reported as unparseable.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation.
* `reference_geometry` - Reference geometry as a literal WKT/WKB/EWKT/EWKB string or bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name.
* `convert_column` - When True, *try_to_geometry* is applied to convert column values to GEOMETRY. When False (default), the column is assumed to already hold a native GEOMETRY value.
* `convert_reference_geometry` - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. When False (default), the reference geometry is assumed to already hold a native GEOMETRY value.

**Returns**:

Column object indicating whether values in the input column do not touch the reference geometry.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_geo_within[​](#is_geo_within "Direct link to is_geo_within")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_within(column: str | Column,
                  reference_geometry: str | bytes | Column,
                  convert_column: bool = False,
                  convert_reference_geometry: bool = False) -> Column

```

Checks if the reference geometry is within the column geometry using `st_within` with meter-level precision.

*NOTE*: the column geometry is the OUTER container in this check; the reference is the inner test geometry. Contrast with is_geo_contains where the reference is the outer container.

`st_within(reference, column)` returns true when the reference geometry lies entirely within the column geometry. This is the converse of `st_contains`: the column geometry must contain the reference, with the reference's boundary and interior both lying inside the column's interior.

Both the target column and the reference geometry are always handled as `GEOMETRY`. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied to parse the value from any supported format (WKT, WKB, EWKT, EWKB). When conversion is not requested, the input is assumed to already hold a native `GEOMETRY` value. A null value is skipped; a non-null value or reference that fails to parse is reported as unparseable.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation.
* `reference_geometry` - Reference geometry as a literal WKT/WKB/EWKT/EWKB string or bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name.
* `convert_column` - When True, *try_to_geometry* is applied to convert column values to GEOMETRY. When False (default), the column is assumed to already hold a native GEOMETRY value.
* `convert_reference_geometry` - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. When False (default), the reference geometry is assumed to already hold a native GEOMETRY value.

**Returns**:

Column object indicating whether the reference geometry is not within the values in the input column.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is_geo_within_distance[​](#is_geo_within_distance "Direct link to is_geo_within_distance")

```python
@requires_dbr_version("17.1")
@register_rule("row")
def is_geo_within_distance(column: str | Column,
                           reference_geometry: str | bytes | Column,
                           distance: int | float | str | Column,
                           convert_column: bool = False,
                           convert_reference_geometry: bool = False) -> Column

```

Checks if the column point is within a geodesic distance of the reference point using *st_distancespheroid*.

The distance is measured in meters along the WGS 84 ellipsoid. This is what makes the check meaningful for longitude/latitude data: the planar *st_distance* returns coordinate units, which for such data are degrees. A value is reported when the geodesic distance between it and the reference point is strictly greater than *distance*.

Only point geometries are supported, because *st_distancespheroid* is defined for points only. Coordinates are always interpreted as WGS 84 degrees: an operand with SRID 0 (unknown, as produced by WKT and WKB input) is treated as WGS 84, an operand with SRID 4326 (as produced by EWKT and GeoJSON input) is used as is, and the two can be mixed freely. A column value or reference that cannot be parsed as a geometry, is not a point, is empty, or carries any other SRID is reported rather than measured, since none of those has a location to measure from.

Both the target column and the reference geometry are always handled as GEOMETRY. When conversion is requested (*convert_column* or *convert_reference_geometry* set to True), *try_to_geometry* is applied to parse the value from any supported format (WKT, WKB, EWKT, EWKB, GeoJSON). See <https://docs.databricks.com/aws/en/sql/language-manual/functions/try_to_geometry> for details. When conversion is not requested, the input is assumed to already hold a native GEOMETRY value.

**Arguments**:

* `column` - Column to check. Null values are skipped for validation; empty points are reported, as they have no location to measure.
* `reference_geometry` - Reference point as a literal WKT/EWKT/GeoJSON string or WKB/EWKB bytes value, or a Column expression (e.g. *F.col('col_name')*) to reference another column. A plain string is always treated as a literal, not a column name. Rows where a referenced column is null are skipped.
* `distance` - Maximum allowed distance in meters. Accepts a non-negative number, a Column expression (e.g. *F.col('radius_m')*), or a string SQL expression evaluated against the input DataFrame. Numeric literals, including numeric strings such as *"1000"*, are validated up front; Column and expression distances are evaluated per row and are not range-checked. Rows where the distance expression evaluates to null are skipped.
* `convert_column` - When True, *try_to_geometry* is applied to convert column values to GEOMETRY. When False (default), the column is assumed to already hold a native GEOMETRY value.
* `convert_reference_geometry` - When True, *try_to_geometry* is applied to convert the reference geometry to GEOMETRY. When False (default), the reference geometry is assumed to already hold a native GEOMETRY value.

**Returns**:

Column object indicating whether values in the input column are farther than *distance* meters from the reference point, or cannot be measured against it.

**Raises**:

* `InvalidParameterError` - If *distance* is a boolean, or a numeric literal (number or numeric string) that is negative, NaN, infinite or too large to represent as a float.

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.
