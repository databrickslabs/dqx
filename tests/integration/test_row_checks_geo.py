import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.geo.check_funcs import (
    has_dimension,
    has_x_coordinate_between,
    has_y_coordinate_between,
    is_non_empty_geometry,
    is_geometry,
    is_geography,
    is_geometrycollection,
    is_latitude,
    is_linestring,
    is_longitude,
    is_multilinestring,
    is_multipoint,
    is_multipolygon,
    is_point,
    is_polygon,
    is_ogc_valid,
)


def test_is_geometry(spark):
    input_schema = "geom_string: string, geom_binary: binary, geom_int: int"
    test_df = spark.createDataFrame(
        [
            ["POINT(1 1)", None, None],  # valid WKT
            ["not-a-geometry", None, None],  # invalid (not valid WKT)
            [None, bytes.fromhex("01E9030000000000000000F03F00000000000000400000000000005940"), None],  # valid WKB
            [None, None, 42],  # invalid (wrong data type)
        ],
        input_schema,
    )

    actual = test_df.select(is_geometry("geom_string"), is_geometry("geom_binary"), is_geometry("geom_int"))

    checked_schema = "geom_string_is_not_geometry: string, geom_binary_is_not_geometry: string, geom_int_is_not_geometry: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            ["value `not-a-geometry` in column `geom_string` is not a geometry", None, None],
            [None, None, None],
            [None, None, "value `42` in column `geom_int` is not a geometry"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_geography(spark):
    input_schema = "geography_string: string, geography_binary: binary, geography_int: int"
    test_df = spark.createDataFrame(
        [
            ["POINT(1 1)", None, None],  # valid WKT
            ["POINT(181 91)", None, None],  # invalid (lat/lon out of range)
            ["not-a-geography", None, None],  # invalid (not valid WKT)
            [None, bytes.fromhex("0101000000000000000000f03f0000000000000040"), None],  # valid WKB
            [None, None, 42],  # invalid (wrong data type)
        ],
        input_schema,
    )

    actual = test_df.select(
        is_geography("geography_string"), is_geography("geography_binary"), is_geography("geography_int")
    )

    checked_schema = "geography_string_is_not_a_geography: string, geography_binary_is_not_a_geography: string, geography_int_is_not_a_geography: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            ["value `POINT(181 91)` in column `geography_string` is not a geography", None, None],
            ["value `not-a-geography` in column `geography_string` is not a geography", None, None],
            [None, None, None],
            [None, None, "value `42` in column `geography_int` is not a geography"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_point(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POINT(1 1)"], ["nonsense"], ["POLYGON((1 1, 2 2, 3 3, 1 1))"], [None]],
        input_schema,
    )

    actual = test_df.select(is_point("geom"))

    checked_schema = "geom_is_not_a_point: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a point geometry"],
            ["value `POLYGON((1 1, 2 2, 3 3, 1 1))` in column `geom` is not a point geometry"],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_linestring(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["LINESTRING(1 1, 2 2)"], ["nonsense"], ["POLYGON((1 1, 2 2, 3 3, 1 1))"], [None]],
        input_schema,
    )

    actual = test_df.select(is_linestring("geom"))

    checked_schema = "geom_is_not_a_linestring: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a linestring geometry"],
            ["value `POLYGON((1 1, 2 2, 3 3, 1 1))` in column `geom` is not a linestring geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_polygon(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POLYGON((1 1, 2 2, 3 3, 1 1))"], ["nonsense"], ["LINESTRING(1 1, 2 2)"], [None]],
        input_schema,
    )

    actual = test_df.select(is_polygon("geom"))

    checked_schema = "geom_is_not_a_polygon: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a polygon geometry"],
            ["value `LINESTRING(1 1, 2 2)` in column `geom` is not a polygon geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_multipoint(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["MULTIPOINT(1 1, 2 2)"], ["nonsense"], ["LINESTRING(1 1, 2 2)"], [None]],
        input_schema,
    )

    actual = test_df.select(is_multipoint("geom"))

    checked_schema = "geom_is_not_a_multipoint: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a multipoint geometry"],
            ["value `LINESTRING(1 1, 2 2)` in column `geom` is not a multipoint geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_multilinestring(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["MULTILINESTRING((1 1, 2 2), (3 3, 4 4))"], ["nonsense"], ["POLYGON((1 1, 2 2, 3 3, 1 1))"], [None]],
        input_schema,
    )

    actual = test_df.select(is_multilinestring("geom"))

    checked_schema = "geom_is_not_a_multilinestring: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a multilinestring geometry"],
            ["value `POLYGON((1 1, 2 2, 3 3, 1 1))` in column `geom` is not a multilinestring geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_multipolygon(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["MULTIPOLYGON(((1 1, 2 2, 3 3, 1 1)))"], ["nonsense"], ["LINESTRING(1 1, 2 2)"], [None]],
        input_schema,
    )

    actual = test_df.select(is_multipolygon("geom"))

    checked_schema = "geom_is_not_a_multipolygon: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a multipolygon geometry"],
            ["value `LINESTRING(1 1, 2 2)` in column `geom` is not a multipolygon geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_geometrycollection(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [
            ["GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(2 2, 3 3))"],
            ["nonsense"],
            ["POLYGON((1 1, 2 2, 3 3, 1 1))"],
            [None],
        ],
        input_schema,
    )

    actual = test_df.select(is_geometrycollection("geom"))

    checked_schema = "geom_is_not_a_geometrycollection: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a geometrycollection geometry"],
            ["value `POLYGON((1 1, 2 2, 3 3, 1 1))` in column `geom` is not a geometrycollection geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_ogc_valid(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POLYGON((0 0,10 0,0 10,0 0))"], ["nonsense"], ["POLYGON((0 0,10 10,10 0,0 10,0 0))"], [None]],
        input_schema,
    )

    actual = test_df.select(is_ogc_valid("geom"))

    checked_schema = "geom_is_not_valid_geometry: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is not a valid geometry (in the OGC sense)"],
            ["value `POLYGON((0 0,10 10,10 0,0 10,0 0))` in column `geom` is not a valid geometry (in the OGC sense)"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_longitude(spark):
    input_schema = "long_string: string, long_int: int, long_double: double"
    test_df = spark.createDataFrame(
        [["1", 120, 180.0], ["-181", None, 180.01]],
        input_schema,
    )

    actual = test_df.select(is_longitude("long_string"), is_longitude("long_int"), is_longitude("long_double"))

    checked_schema = "long_string_is_not_valid_longitude: string, long_int_is_not_valid_longitude: string, long_double_is_not_valid_longitude: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            [
                "value `-181` in column `long_string` is not a valid longitude (must be between -180 and 180)",
                None,
                "value `180.01` in column `long_double` is not a valid longitude (must be between -180 and 180)",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_latitude(spark):
    input_schema = "lat_string: string, lat_int: int, lat_double: double"
    test_df = spark.createDataFrame(
        [["1", 60, 90.0], ["-91", None, 90.01]],
        input_schema,
    )

    actual = test_df.select(is_latitude("lat_string"), is_latitude("lat_int"), is_latitude("lat_double"))

    checked_schema = "lat_string_is_not_valid_latitude: string, lat_int_is_not_valid_latitude: string, lat_double_is_not_valid_latitude: string"
    expected = spark.createDataFrame(
        [
            [None, None, None],
            [
                "value `-91` in column `lat_string` is not a valid latitude (must be between -90 and 90)",
                None,
                "value `90.01` in column `lat_double` is not a valid latitude (must be between -90 and 90)",
            ],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_non_empty_geometry(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POINT(1 1)"], ["nonsense"], ["POLYGON EMPTY"], [None]],
        input_schema,
    )

    actual = test_df.select(is_non_empty_geometry("geom"))

    checked_schema = "geom_is_empty_geometry: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` is an empty geometry"],
            ["value `POLYGON EMPTY` in column `geom` is an empty geometry"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)


def test_has_dimension(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POINT(1 1)"], ["nonsense"], ["POLYGON((0 0, 2 0, 0 2, 0 0))"], [None]],
        input_schema,
    )

    actual = test_df.select(has_dimension("geom", 0))

    checked_schema = "geom_does_not_have_required_dimension: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` does not have the required dimension (0)"],
            ["value `POLYGON((0 0, 2 0, 0 2, 0 0))` in column `geom` does not have the required dimension (0)"],
            [None],
        ],
        checked_schema,
    )
    assert_df_equality(actual, expected, ignore_nullable=True)
def test_has_x_coordinate_between(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POINT(1 1)"], ["nonsense"], ["POLYGON((0 0, 2 0, 0 2, 0 0))"], [None]],
        input_schema,
    )

    actual = test_df.select(has_x_coordinate_between("geom", 0, 1))

    checked_schema = "geom_has_x_coordinates_outside_range: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` has x coordinates outside the range [0, 1]"],
            ["value `POLYGON((0 0, 2 0, 0 2, 0 0))` in column `geom` has x coordinates outside the range [0, 1]"],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)
def test_has_y_coordinate_between(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame(
        [["POINT(1 1)"], ["nonsense"], ["POLYGON((0 0, 2 0, 0 2, 0 0))"], [None]],
        input_schema,
    )

    actual = test_df.select(has_y_coordinate_between("geom", 0, 1))

    checked_schema = "geom_has_y_coordinates_outside_range: string"
    expected = spark.createDataFrame(
        [
            [None],
            ["value `nonsense` in column `geom` has y coordinates outside the range [0, 1]"],
            ["value `POLYGON((0 0, 2 0, 0 2, 0 0))` in column `geom` has y coordinates outside the range [0, 1]"],
            [None],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)