import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import is_within_polygon_precise, is_within_polygon_approximate

_REFERENCE_POLYGON_WKT = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"

_VALID_KWARGS = {
    "convert_column": True,
    "convert_reference_polygon": True,
    "topological_relationship": "WITHIN",
}


def test_is_within_polygon_precise_valid_params_does_not_raise():
    """A fully specified call with all valid params must not raise at call time."""
    is_within_polygon_precise("location", _REFERENCE_POLYGON_WKT, **_VALID_KWARGS)


def test_is_within_polygon_precise_no_conversion_does_not_raise():
    """Both convert flags as False (column already holds native GEOMETRY) must not raise."""
    is_within_polygon_precise(
        "location",
        _REFERENCE_POLYGON_WKT,
        convert_column=False,
        convert_reference_polygon=False,
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

def test_is_within_polygon_approximate_resolution_invalid():
    """Raises InvalidParameterError for any topological_relationship value outside {CONTAINS, COVERS, WITHIN}."""
    with pytest.raises(InvalidParameterError):
        is_within_polygon_approximate(
            "location",
            _REFERENCE_POLYGON_WKT,
            resolution=-1,
        )
