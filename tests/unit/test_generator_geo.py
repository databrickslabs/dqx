from unittest.mock import create_autospec

import pytest

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profile import DQProfile
from databricks.labs.dqx.profiler.profile_builder import GEOSPATIAL_PROFILE_NAMES


@pytest.mark.parametrize(
    "geometry_type, expected_function",
    [
        ("ST_Point", "is_point"),
        ("ST_LineString", "is_linestring"),
        ("ST_Polygon", "is_polygon"),
        ("ST_MultiPoint", "is_multipoint"),
        ("ST_MultiLineString", "is_multilinestring"),
        ("ST_MultiPolygon", "is_multipolygon"),
        ("ST_GeometryCollection", "is_geometrycollection"),
    ],
)
def test_geometry_type_maps_to_matching_check(geometry_type, expected_function):
    result = DQGenerator.dq_generate_geometry_type("geom", "error", type=geometry_type)
    assert result["check"]["function"] == expected_function
    assert result["check"]["arguments"] == {"column": "geom"}
    assert result["criticality"] == "error"


def test_geometry_type_returns_none_for_unknown_type():
    assert DQGenerator.dq_generate_geometry_type("geom", "error", type="ST_Unknown") is None


def test_has_x_coordinate_between():
    result = DQGenerator.dq_generate_has_x_coordinate_between("geom", "warn", min_value=-123.0, max_value=-122.0)
    assert result["check"]["function"] == "has_x_coordinate_between"
    assert result["check"]["arguments"] == {"column": "geom", "min_value": -123.0, "max_value": -122.0}
    assert result["criticality"] == "warn"


def test_has_y_coordinate_between():
    result = DQGenerator.dq_generate_has_y_coordinate_between("geom", "error", min_value=37.0, max_value=38.0)
    assert result["check"]["function"] == "has_y_coordinate_between"
    assert result["check"]["arguments"] == {"column": "geom", "min_value": 37.0, "max_value": 38.0}


def test_area_not_less_than_includes_srid_when_set():
    result = DQGenerator.dq_generate_is_area_not_less_than("geom", "error", value=10, srid=3857)
    assert result["check"]["function"] == "is_area_not_less_than"
    assert result["check"]["arguments"] == {"column": "geom", "value": 10, "srid": 3857}


def test_area_not_greater_than_omits_srid_when_none():
    result = DQGenerator.dq_generate_is_area_not_greater_than("geom", "error", value=500, srid=None)
    assert result["check"]["function"] == "is_area_not_greater_than"
    assert result["check"]["arguments"] == {"column": "geom", "value": 500}


def test_num_points_not_less_than():
    result = DQGenerator.dq_generate_is_num_points_not_less_than("geom", "error", value=1)
    assert result["check"]["function"] == "is_num_points_not_less_than"
    assert result["check"]["arguments"] == {"column": "geom", "value": 1}


def test_num_points_not_greater_than():
    result = DQGenerator.dq_generate_is_num_points_not_greater_than("geom", "error", value=12)
    assert result["check"]["function"] == "is_num_points_not_greater_than"
    assert result["check"]["arguments"] == {"column": "geom", "value": 12}


@pytest.mark.parametrize(
    "generator, expected_function",
    [
        (DQGenerator.dq_generate_is_non_empty_geometry, "is_non_empty_geometry"),
        (DQGenerator.dq_generate_is_ogc_valid, "is_ogc_valid"),
        (DQGenerator.dq_generate_is_not_null_island, "is_not_null_island"),
    ],
)
def test_no_arg_geo_checks(generator, expected_function):
    result = generator("geom", "error")
    assert result["check"] == {"function": expected_function, "arguments": {"column": "geom"}}
    assert result["name"] == f"geom_{expected_function}"
    assert result["criticality"] == "error"


def test_generate_dq_rules_maps_all_geospatial_profiles():
    ws = create_autospec(WorkspaceClient, instance=True)
    generator = DQGenerator(ws)
    profiles = [
        DQProfile(name="geometry_type", column="geom", parameters={"type": "ST_Polygon"}),
        DQProfile(name="has_x_coordinate_between", column="geom", parameters={"min_value": -123, "max_value": -122}),
        DQProfile(name="has_y_coordinate_between", column="geom", parameters={"min_value": 37, "max_value": 38}),
        DQProfile(name="is_area_not_less_than", column="geom", parameters={"value": 10, "srid": 3857}),
        DQProfile(name="is_area_not_greater_than", column="geom", parameters={"value": 501, "srid": 3857}),
        DQProfile(name="is_num_points_not_less_than", column="geom", parameters={"value": 1}),
        DQProfile(name="is_num_points_not_greater_than", column="geom", parameters={"value": 12}),
        DQProfile(name="is_non_empty_geometry", column="geom"),
        DQProfile(name="is_ogc_valid", column="geom"),
        DQProfile(name="is_not_null_island", column="geom"),
    ]

    rules = generator.generate_dq_rules(profiles, criticality="warn")

    functions = [rule["check"]["function"] for rule in rules]
    assert len(functions) == len(profiles)
    assert functions[0] == "is_polygon"
    assert set(functions) == (set(GEOSPATIAL_PROFILE_NAMES) - {"geometry_type"}) | {"is_polygon"}
    assert all(rule["criticality"] == "warn" for rule in rules)


def test_generate_dq_rules_skips_unknown_geometry_type():
    ws = create_autospec(WorkspaceClient, instance=True)
    generator = DQGenerator(ws)
    profiles = [DQProfile(name="geometry_type", column="geom", parameters={"type": "ST_Weird"})]
    assert not generator.generate_dq_rules(profiles)
