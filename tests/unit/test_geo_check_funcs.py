import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import (
    is_geo_contains,
    is_geo_covers,
    is_geo_intersects,
    is_geo_touches,
    is_geo_within,
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
