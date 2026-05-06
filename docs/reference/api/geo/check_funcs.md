# databricks.labs.dqx.geo.check\_funcs

### is\_latitude[​](#is_latitude "Direct link to is_latitude")

```python
@register_rule("row")
def is_latitude(column: str | Column) -> Column

```

Checks whether the values in the input column are valid latitudes.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid latitudes

### is\_longitude[​](#is_longitude "Direct link to is_longitude")

```python
@register_rule("row")
def is_longitude(column: str | Column) -> Column

```

Checks whether the values in the input column are valid longitudes.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid longitudes

### is\_geometry[​](#is_geometry "Direct link to is_geometry")

```python
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

### is\_geography[​](#is_geography "Direct link to is_geography")

```python
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

### is\_point[​](#is_point "Direct link to is_point")

```python
@register_rule("row")
def is_point(column: str | Column) -> Column

```

Checks whether the values in the input column are point geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are point geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_linestring[​](#is_linestring "Direct link to is_linestring")

```python
@register_rule("row")
def is_linestring(column: str | Column) -> Column

```

Checks whether the values in the input column are linestring geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are linestring geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_polygon[​](#is_polygon "Direct link to is_polygon")

```python
@register_rule("row")
def is_polygon(column: str | Column) -> Column

```

Checks whether the values in the input column are polygon geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are polygon geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_multipoint[​](#is_multipoint "Direct link to is_multipoint")

```python
@register_rule("row")
def is_multipoint(column: str | Column) -> Column

```

Checks whether the values in the input column are multipoint geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are multipoint geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_multilinestring[​](#is_multilinestring "Direct link to is_multilinestring")

```python
@register_rule("row")
def is_multilinestring(column: str | Column) -> Column

```

Checks whether the values in the input column are multilinestring geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are multilinestring geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_multipolygon[​](#is_multipolygon "Direct link to is_multipolygon")

```python
@register_rule("row")
def is_multipolygon(column: str | Column) -> Column

```

Checks whether the values in the input column are multipolygon geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are multipolygon geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_geometrycollection[​](#is_geometrycollection "Direct link to is_geometrycollection")

```python
@register_rule("row")
def is_geometrycollection(column: str | Column) -> Column

```

Checks whether the values in the input column are geometrycollection geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are geometrycollection geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_ogc\_valid[​](#is_ogc_valid "Direct link to is_ogc_valid")

```python
@register_rule("row")
def is_ogc_valid(column: str | Column) -> Column

```

Checks whether the values in the input column are valid geometries in the OGC sense.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are valid geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_non\_empty\_geometry[​](#is_non_empty_geometry "Direct link to is_non_empty_geometry")

```python
@register_rule("row")
def is_non_empty_geometry(column: str | Column) -> Column

```

Checks whether the values in the input column are empty geometries.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are empty geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_not\_null\_island[​](#is_not_null_island "Direct link to is_not_null_island")

```python
@register_rule("row")
def is_not_null_island(column: str | Column) -> Column

```

Checks whether the values in the input column are NULL island geometries (e.g. POINT(0 0), POINTZ(0 0 0), or POINTZM(0 0 0 0)).

**Arguments**:

* `column` - column to check; can be a string column name or a column expression

**Returns**:

Column object indicating whether the values in the input column are NULL island geometries

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has\_dimension[​](#has_dimension "Direct link to has_dimension")

```python
@register_rule("row")
def has_dimension(column: str | Column, dimension: int) -> Column

```

Checks whether the geometries/geographies in the input column have a given dimension.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `dimension` - required dimension of the geometries/geographies

**Returns**:

Column object indicating whether the geometries/geographies in the input column have a given dimension

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has\_x\_coordinate\_between[​](#has_x_coordinate_between "Direct link to has_x_coordinate_between")

```python
@register_rule("row")
def has_x_coordinate_between(column: str | Column, min_value: float,
                             max_value: float) -> Column

```

Checks whether the x coordinates of the geometries in the input column are between a given range.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_value` - minimum value of the x coordinates
* `max_value` - maximum value of the x coordinates

**Returns**:

Column object indicating whether the x coordinates of the geometries in the input column are between a given range

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### has\_y\_coordinate\_between[​](#has_y_coordinate_between "Direct link to has_y_coordinate_between")

```python
@register_rule("row")
def has_y_coordinate_between(column: str | Column, min_value: float,
                             max_value: float) -> Column

```

Checks whether the y coordinates of the geometries in the input column are between a given range.

**Arguments**:

* `column` - column to check; can be a string column name or a column expression
* `min_value` - minimum value of the y coordinates
* `max_value` - maximum value of the y coordinates

**Returns**:

Column object indicating whether the y coordinates of the geometries in the input column are between a given range

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_area\_equal\_to[​](#is_area_equal_to "Direct link to is_area_equal_to")

```python
@register_rule("row")
def is_area_equal_to(column: str | Column,
                     value: int | float | str | Column,
                     srid: int | None = 3857,
                     geodesic: bool = False) -> Column

```

Checks if the areas of values in a geometry or geography column are equal to a specified value. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).

**Returns**:

Column object indicating whether the area the geometries in the input column are equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_area\_not\_equal\_to[​](#is_area_not_equal_to "Direct link to is_area_not_equal_to")

```python
@register_rule("row")
def is_area_not_equal_to(column: str | Column,
                         value: int | float | str | Column,
                         srid: int | None = 3857,
                         geodesic: bool = False) -> Column

```

Checks if the areas of values in a geometry column are not equal to a specified value. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).

**Returns**:

Column object indicating whether the area the geometries in the input column are not equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_area\_not\_greater\_than[​](#is_area_not_greater_than "Direct link to is_area_not_greater_than")

```python
@register_rule("row")
def is_area_not_greater_than(column: str | Column,
                             value: int | float | str | Column,
                             srid: int | None = 3857,
                             geodesic: bool = False) -> Column

```

Checks if the areas of values in a geometry column are not greater than a specified limit. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).

**Returns**:

Column object indicating whether the area the geometries in the input column is greater than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_area\_not\_less\_than[​](#is_area_not_less_than "Direct link to is_area_not_less_than")

```python
@register_rule("row")
def is_area_not_less_than(column: str | Column,
                          value: int | float | str | Column,
                          srid: int | None = 3857,
                          geodesic: bool = False) -> Column

```

Checks if the areas of values in a geometry column are not less than a specified limit. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator) with units of meters squared is used. An SRID can be specified to transform the input values and compute areas with specific units of measure.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression
* `srid` - Optional integer SRID to use for computing the area of the geometry or geography value (default `None`). If an SRID is provided, the input value is translated and area is calculated using the units of measure of the specified coordinate reference system (e.g. meters squared for `srid=3857`).
* `geodesic` - Whether to use the 2D geodesic area (default `False`).

**Returns**:

Column object indicating whether the area the geometries in the input column is less than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_num\_points\_equal\_to[​](#is_num_points_equal_to "Direct link to is_num_points_equal_to")

```python
@register_rule("row")
def is_num_points_equal_to(column: str | Column,
                           value: int | float | str | Column) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is equal to a specified value.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_num\_points\_not\_equal\_to[​](#is_num_points_not_equal_to "Direct link to is_num_points_not_equal_to")

```python
@register_rule("row")
def is_num_points_not_equal_to(column: str | Column,
                               value: int | float | str | Column) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is not equal to a specified value.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is not equal to the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_num\_points\_not\_greater\_than[​](#is_num_points_not_greater_than "Direct link to is_num_points_not_greater_than")

```python
@register_rule("row")
def is_num_points_not_greater_than(
        column: str | Column, value: int | float | str | Column) -> Column

```

Checks if the number of coordinate pairs in the values of a geometry column is not greater than a specified limit.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is greater than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### is\_num\_points\_not\_less\_than[​](#is_num_points_not_less_than "Direct link to is_num_points_not_less_than")

```python
@register_rule("row")
def is_num_points_not_less_than(column: str | Column,
                                value: int | float | str | Column) -> Column

```

Checks if the number of coordinate pairs in values of a geometry column is not less than a specified limit.

**Arguments**:

* `column` - Column to check; can be a string column name or a column expression
* `value` - Value to use in the condition as number, column name or sql expression

**Returns**:

Column object indicating whether the number of coordinate pairs in the geometries of the input column is less than the provided value

**Notes**:

This function requires Databricks serverless compute or runtime 17.1 or above.

### are\_polygons\_mutually\_disjoint[​](#are_polygons_mutually_disjoint "Direct link to are_polygons_mutually_disjoint")

```python
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
