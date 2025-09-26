import pytest
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.geo.check_funcs import (
    is_geometry,
    is_geography,
    is_geometrycollection,
    is_linestring,
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

    checked_schema = "geom_string_is_not_a_geometry: string, geom_binary_is_not_a_geometry: string, geom_int_is_not_a_geometry: string"
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

    checked_schema = "geom_is_not_a_valid_geometry: string"
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
