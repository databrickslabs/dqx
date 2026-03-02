---
name: dqx-geospatial-checks
description: Geospatial DQX quality check functions. Each check validates geometry/geography columns (coordinates, types, validity, measurements) and adds results to _errors or _warnings columns.
---

# Geospatial Check Functions

Geospatial checks validate geometry and geography columns. Use `column` in `arguments` to specify which column to check.

**Runtime requirement:** All geometry/geography functions (everything except `is_latitude` and `is_longitude`) require Databricks serverless compute or runtime 17.1+.

## Parameter Reference

**IMPORTANT:** Parameters listed as **required** MUST be provided -- omitting them causes a runtime error.

### Coordinate Validation

| Function | Required | Optional |
|----------|----------|----------|
| `is_latitude` | `column` | -- |
| `is_longitude` | `column` | -- |

### Geometry and Geography Type

| Function | Required | Optional |
|----------|----------|----------|
| `is_geometry` | `column` | -- |
| `is_geography` | `column` | -- |
| `is_point` | `column` | -- |
| `is_linestring` | `column` | -- |
| `is_polygon` | `column` | -- |
| `is_multipoint` | `column` | -- |
| `is_multilinestring` | `column` | -- |
| `is_multipolygon` | `column` | -- |
| `is_geometrycollection` | `column` | -- |

### Geometry Validation

| Function | Required | Optional |
|----------|----------|----------|
| `is_ogc_valid` | `column` | -- |
| `is_non_empty_geometry` | `column` | -- |
| `is_not_null_island` | `column` | -- |

### Geometry Properties

| Function | Required | Optional |
|----------|----------|----------|
| `has_dimension` | `column`, `dimension` | -- |
| `has_x_coordinate_between` | `column`, `min_value`, `max_value` | -- |
| `has_y_coordinate_between` | `column`, `min_value`, `max_value` | -- |

### Area Comparison

| Function | Required | Optional |
|----------|----------|----------|
| `is_area_equal_to` | `column`, `value` | `srid` (default: 3857), `geodesic` (default: false) |
| `is_area_not_equal_to` | `column`, `value` | `srid` (default: 3857), `geodesic` (default: false) |
| `is_area_not_greater_than` | `column`, `value` | `srid` (default: 3857), `geodesic` (default: false) |
| `is_area_not_less_than` | `column`, `value` | `srid` (default: 3857), `geodesic` (default: false) |

`value` can be a number, column name, or SQL expression. By default, the 2D Cartesian area in WGS84 (Pseudo-Mercator, SRID 3857) with units of meters squared is used. Set `geodesic: true` for geodesic area.

### Point Count

| Function | Required | Optional |
|----------|----------|----------|
| `is_num_points_equal_to` | `column`, `value` | -- |
| `is_num_points_not_equal_to` | `column`, `value` | -- |
| `is_num_points_not_greater_than` | `column`, `value` | -- |
| `is_num_points_not_less_than` | `column`, `value` | -- |

`value` can be a number, column name, or SQL expression.

---

## Examples

### Coordinate Validation

```yaml
# Latitude must be between -90 and 90
- criticality: error
  check:
    function: is_latitude
    arguments:
      column: lat

# Longitude must be between -180 and 180
- criticality: error
  check:
    function: is_longitude
    arguments:
      column: lon
```

### Geometry Type Checks

```yaml
# Must be a valid geometry (WKT or GeoJSON)
- criticality: error
  check:
    function: is_geometry
    arguments:
      column: geom

# Must be a valid geography
- criticality: error
  check:
    function: is_geography
    arguments:
      column: geom

# Must be a point
- criticality: error
  check:
    function: is_point
    arguments:
      column: location

# Must be a polygon
- criticality: error
  check:
    function: is_polygon
    arguments:
      column: boundary
```

### Geometry Validation

```yaml
# Must be valid per OGC rules
- criticality: error
  check:
    function: is_ogc_valid
    arguments:
      column: geom

# Must not be an empty geometry
- criticality: warn
  check:
    function: is_non_empty_geometry
    arguments:
      column: geom

# Must not be a null island (POINT(0 0))
- criticality: warn
  check:
    function: is_not_null_island
    arguments:
      column: location
```

### Geometry Properties

```yaml
# Must be 2-dimensional
- criticality: warn
  check:
    function: has_dimension
    arguments:
      column: geom
      dimension: 2

# X coordinates must be within range (longitude-like)
- criticality: error
  check:
    function: has_x_coordinate_between
    arguments:
      column: geom
      min_value: -180.0
      max_value: 180.0

# Y coordinates must be within range (latitude-like)
- criticality: error
  check:
    function: has_y_coordinate_between
    arguments:
      column: geom
      min_value: -90.0
      max_value: 90.0
```

### Area Comparison

```yaml
# Area must not exceed 1,000,000 m² (Pseudo-Mercator)
- criticality: error
  check:
    function: is_area_not_greater_than
    arguments:
      column: boundary
      value: 1000000

# Area with custom SRID
- criticality: warn
  check:
    function: is_area_not_less_than
    arguments:
      column: boundary
      value: 100
      srid: 3857
```

### Point Count

```yaml
# Polygon must have at least 4 coordinate pairs (closed ring)
- criticality: error
  check:
    function: is_num_points_not_less_than
    arguments:
      column: boundary
      value: 4

# Geometry must not exceed 10000 points
- criticality: warn
  check:
    function: is_num_points_not_greater_than
    arguments:
      column: geom
      value: 10000
```

## Runnable Examples

| Example | File |
|---------|------|
| Coordinate validation (latitude/longitude) | `examples/22_geo_coordinates.py` |
| Geometry type checks | `examples/23_geo_type_validation.py` |
| Geometry validation (OGC, empty, null island) | `examples/24_geo_quality.py` |
| Measurements (coordinates, area, point count) | `examples/25_geo_measurements.py` |

Full reference: https://databrickslabs.github.io/dqx/docs/reference/quality_checks/#geospatial-checks-reference
