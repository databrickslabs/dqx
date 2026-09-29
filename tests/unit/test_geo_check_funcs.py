import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import (
    has_x_coordinate_between,
    is_area_not_greater_than,
    is_geo_contains,
    is_geo_covers,
    is_geo_intersects,
    is_geo_touches,
    is_geo_within,
    is_non_empty_geometry,
    is_num_points_not_greater_than,
    is_point,
)

_REFERENCE_GEOMETRY_WKT = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"


def test_is_geo_contains_does_not_raise():
    is_geo_contains("location", _REFERENCE_GEOMETRY_WKT)


def test_is_geo_contains_with_conversion_does_not_raise():
    is_geo_contains("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)


def test_is_geo_touches_does_not_raise():
    is_geo_touches("location", _REFERENCE_GEOMETRY_WKT)


def test_is_geo_touches_with_conversion_does_not_raise():
    is_geo_touches("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)


def test_is_geo_within_does_not_raise():
    is_geo_within("location", _REFERENCE_GEOMETRY_WKT)


def test_is_geo_within_with_conversion_does_not_raise():
    is_geo_within("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)


def test_is_geo_covers_precise_does_not_raise():
    is_geo_covers("location", _REFERENCE_GEOMETRY_WKT, precise=True)


def test_is_geo_covers_precise_with_conversion_does_not_raise():
    is_geo_covers(
        "location", _REFERENCE_GEOMETRY_WKT, precise=True, convert_column=True, convert_reference_geometry=True
    )


def test_is_geo_covers_approximate_with_resolution_does_not_raise():
    is_geo_covers("location", _REFERENCE_GEOMETRY_WKT, precise=False, resolution=7)


def test_is_geo_covers_approximate_missing_resolution_raises():
    """Raises InvalidParameterError when precise=False and resolution is not provided."""
    with pytest.raises(InvalidParameterError):
        is_geo_covers("location", _REFERENCE_GEOMETRY_WKT, precise=False)


def test_is_geo_covers_approximate_invalid_resolution_raises():
    """Raises InvalidParameterError when resolution is outside 0–15."""
    with pytest.raises(InvalidParameterError):
        is_geo_covers("location", _REFERENCE_GEOMETRY_WKT, precise=False, resolution=16)


def test_is_geo_covers_approximate_bytes_reference_raises():
    """Raises InvalidParameterError when bytes reference geometry is used in approximate mode."""
    with pytest.raises(InvalidParameterError):
        is_geo_covers("location", b"\x00\x01", precise=False, resolution=5)


def test_is_geo_intersects_precise_does_not_raise():
    is_geo_intersects("location", _REFERENCE_GEOMETRY_WKT, precise=True)


def test_is_geo_intersects_precise_with_conversion_does_not_raise():
    is_geo_intersects(
        "location", _REFERENCE_GEOMETRY_WKT, precise=True, convert_column=True, convert_reference_geometry=True
    )


def test_is_geo_intersects_approximate_with_resolution_does_not_raise():
    is_geo_intersects("location", _REFERENCE_GEOMETRY_WKT, precise=False, resolution=5)


def test_is_geo_intersects_approximate_missing_resolution_raises():
    """Raises InvalidParameterError when precise=False and resolution is not provided."""
    with pytest.raises(InvalidParameterError):
        is_geo_intersects("location", _REFERENCE_GEOMETRY_WKT, precise=False)


def test_is_geo_intersects_approximate_invalid_resolution_raises():
    """Raises InvalidParameterError when resolution is outside 0–15."""
    with pytest.raises(InvalidParameterError):
        is_geo_intersects("location", _REFERENCE_GEOMETRY_WKT, precise=False, resolution=-1)


def test_is_geo_intersects_approximate_bytes_reference_raises():
    """Raises InvalidParameterError when bytes reference geometry is used in approximate mode."""
    with pytest.raises(InvalidParameterError):
        is_geo_intersects("location", b"\x00\x01", precise=False, resolution=5)


def test_is_geo_contains_precise_has_proper_alias():
    column = is_geo_contains("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)
    column_str = _column_expression_clean(column)
    assert column_str.endswith("location_is_not_in_reference_geometry"), f'{column_str} has incorrect alias suffix'


def test_is_geo_intersects_precise_has_proper_alias():
    column = is_geo_intersects(
        "location", _REFERENCE_GEOMETRY_WKT, precise=True, convert_column=True, convert_reference_geometry=True
    )
    column_str = _column_expression_clean(column)
    assert column_str.endswith(
        "location_does_not_intersect_reference_geometry_precisely"
    ), f'{column_str} has incorrect alias suffix'


def test_is_geo_intersects_approximate_has_proper_alias():
    column = is_geo_intersects(
        "location",
        _REFERENCE_GEOMETRY_WKT,
        precise=False,
        convert_column=True,
        resolution=10,
        convert_reference_geometry=True,
    )
    column_str = _column_expression_clean(column)
    assert column_str.endswith(
        "location_does_not_intersect_reference_geometry_approximately"
    ), f'{column_str} has incorrect alias suffix'


def test_is_is_geo_covers_precise_has_proper_alias():
    column = is_geo_covers(
        "location", _REFERENCE_GEOMETRY_WKT, precise=True, convert_column=True, convert_reference_geometry=True
    )
    column_str = _column_expression_clean(column)
    assert column_str.endswith(
        "location_is_not_covered_by_reference_geometry_precisely"
    ), f'{column_str} has incorrect alias suffix'


def test_is_is_geo_covers_approximate_has_proper_alias():
    column = is_geo_covers(
        "location",
        _REFERENCE_GEOMETRY_WKT,
        precise=False,
        convert_column=True,
        resolution=10,
        convert_reference_geometry=True,
    )
    column_str = _column_expression_clean(column)
    assert column_str.endswith(
        "location_is_not_covered_by_reference_geometry_approximately"
    ), f'{column_str} has incorrect alias suffix'


def test_is_geo_touches_has_proper_alias():
    column = is_geo_touches("location", _REFERENCE_GEOMETRY_WKT)
    column_str = _column_expression_clean(column)
    assert column_str.endswith("location_does_not_touch_reference_geometry"), f'{column_str} has incorrect alias suffix'


def test_is_geo_touches_with_conversion_has_proper_alias():
    column = is_geo_touches("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)
    column_str = _column_expression_clean(column)
    assert column_str.endswith("location_does_not_touch_reference_geometry"), f'{column_str} has incorrect alias suffix'


def test_is_geo_within_has_proper_alias():
    column = is_geo_within("location", _REFERENCE_GEOMETRY_WKT, convert_column=True, convert_reference_geometry=True)
    column_str = _column_expression_clean(column)
    assert column_str.endswith(
        "location_does_not_contain_reference_geometry"
    ), f'{column_str} has incorrect alias suffix'


def _column_expression_clean(column) -> str:
    return str(column).removeprefix("Column<'").removesuffix("'>")


# convert_column controls whether a single-column geo check parses the value with try_to_geometry
# (for WKT/WKB string columns) or references it directly (for native GEOMETRY/GEOGRAPHY columns, which
# the profiler generates checks for). These cover the check families the profiler emits.
@pytest.mark.parametrize(
    "column",
    [
        is_point("geom", convert_column=False),
        has_x_coordinate_between("geom", -1.0, 1.0, convert_column=False),
        is_area_not_greater_than("geom", 100, convert_column=False),
        is_num_points_not_greater_than("geom", 5, convert_column=False),
        is_non_empty_geometry("geom", convert_column=False),
    ],
)
def test_geo_check_without_conversion_references_column_directly(column):
    assert "try_to_geometry" not in _column_expression_clean(column)


@pytest.mark.parametrize(
    "column",
    [
        is_point("geom", convert_column=True),
        has_x_coordinate_between("geom", -1.0, 1.0, convert_column=True),
        is_area_not_greater_than("geom", 100, convert_column=True),
        is_num_points_not_greater_than("geom", 5, convert_column=True),
        is_non_empty_geometry("geom", convert_column=True),
    ],
)
def test_geo_check_with_conversion_parses_column(column):
    assert "try_to_geometry" in _column_expression_clean(column)
