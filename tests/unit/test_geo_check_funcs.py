import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import is_within_polygon_precise

_REFERENCE_POLYGON_WKT = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"

_VALID_KWARGS = {
    "column_representation": "WKT",
    "reference_polygon_representation": "WKT",
    "topological_relationship": "WITHIN",
}


def test_is_within_polygon_precise_valid_params_does_not_raise():
    """A fully specified call with all valid params must not raise at call time."""
    is_within_polygon_precise("location", _REFERENCE_POLYGON_WKT, **_VALID_KWARGS)


def test_is_within_polygon_precise_no_conversion_params_does_not_raise():
    """Both representation params as None (column already holds GEOMETRY) must not raise."""
    is_within_polygon_precise(
        "location",
        _REFERENCE_POLYGON_WKT,
        column_representation=None,
        reference_polygon_representation=None,
    )


@pytest.mark.parametrize("invalid_value", ["JSON", "CSV", "HEX", "TEXT", "BINARY", ""])
def test_is_within_polygon_precise_invalid_column_representation_raises(invalid_value):
    """Raises InvalidParameterError for any column_representation value outside {WKT, WKB, EWKT, EWKB}."""
    with pytest.raises(InvalidParameterError):
        is_within_polygon_precise(
            "location",
            _REFERENCE_POLYGON_WKT,
            **{**_VALID_KWARGS, "column_representation": invalid_value},
        )


@pytest.mark.parametrize("invalid_value", ["JSON", "CSV", "HEX", "TEXT", "BINARY", ""])
def test_is_within_polygon_precise_invalid_reference_polygon_representation_raises(invalid_value):
    """Raises InvalidParameterError for any reference_polygon_representation value outside {WKT, WKB, EWKT, EWKB}."""
    with pytest.raises(InvalidParameterError):
        is_within_polygon_precise(
            "location",
            _REFERENCE_POLYGON_WKT,
            **{**_VALID_KWARGS, "reference_polygon_representation": invalid_value},
        )


@pytest.mark.parametrize("invalid_value", ["INTERSECTS", "DISJOINT", "TOUCHES", "OVERLAPS", "within", ""])
def test_is_within_polygon_precise_invalid_topological_relationship_raises(invalid_value):
    """Raises InvalidParameterError for any topological_relationship value outside {CONTAINS, COVERS, WITHIN}."""
    with pytest.raises(InvalidParameterError):
        is_within_polygon_precise(
            "location",
            _REFERENCE_POLYGON_WKT,
            **{**_VALID_KWARGS, "topological_relationship": invalid_value},
        )
