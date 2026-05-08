import pytest

from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.geo.check_funcs import (
    has_topological_relationship_precise,
    has_topological_relationship_approximate,
)

_REFERENCE_GEOMETRY_WKT = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"

_VALID_KWARGS = {
    "convert_column": True,
    "convert_reference_geometry": True,
    "topological_relationship": "WITHIN",
}


def test_has_topological_relationship_precise_valid_params_does_not_raise():
    """A fully specified call with all valid params must not raise at call time."""
    has_topological_relationship_precise("location", _REFERENCE_GEOMETRY_WKT, **_VALID_KWARGS)


def test_has_topological_relationship_precise_no_conversion_does_not_raise():
    """Both convert flags as False (column already holds native GEOMETRY) must not raise."""
    has_topological_relationship_precise(
        "location",
        _REFERENCE_GEOMETRY_WKT,
        convert_column=False,
        convert_reference_geometry=False,
    )


@pytest.mark.parametrize("invalid_value", ["DISJOINT", "OVERLAPS", "within", ""])
def test_has_topological_relationship_precise_invalid_topological_relationship_raises(invalid_value):
    """Raises InvalidParameterError for any topological_relationship value outside {CONTAINS, COVERS, WITHIN}."""
    with pytest.raises(InvalidParameterError):
        has_topological_relationship_precise(
            "location",
            _REFERENCE_GEOMETRY_WKT,
            **{**_VALID_KWARGS, "topological_relationship": invalid_value},
        )


def test_has_topological_relationship_approximate_resolution_invalid():
    """Raises InvalidParameterError when resolution is outside the 0–15 range."""
    with pytest.raises(InvalidParameterError):
        has_topological_relationship_approximate(
            "location",
            _REFERENCE_GEOMETRY_WKT,
            resolution=-1,
        )


@pytest.mark.parametrize("invalid_value", ["WITHIN", "TOUCHES", "intersects", ""])
def test_has_topological_relationship_approximate_invalid_topological_relationship_raises(invalid_value):
    """Raises InvalidParameterError for any topological_relationship value outside {CONTAINS, INTERSECTS}."""
    with pytest.raises(InvalidParameterError):
        has_topological_relationship_approximate(
            "location",
            _REFERENCE_GEOMETRY_WKT,
            resolution=7,
            topological_relationship=invalid_value,
        )
