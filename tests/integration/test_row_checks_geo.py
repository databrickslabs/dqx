from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.geo.check_funcs import is_valid_geometry, is_valid_geography


def test_is_valid_geometry(spark):
    input_schema = "geom: string"
    test_df = spark.createDataFrame([("POINT(1 1)",), ("POINT(2 2)",), ("not-a-geometry",)], input_schema)

    actual = test_df.select(is_valid_geometry("geom"))

    checked_schema = "geom_is_not_valid_geometry: string"
    expected = spark.createDataFrame(
        [
            [None],
            [None],
            ["Value 'not-a-geometry' in Column 'geom' is not a valid geometry"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_is_valid_geography(spark):
    input_schema = "geography: string"
    test_df = spark.createDataFrame(
        [("POINT(1 1)",), ("POINT(2 2)",), ("not-a-geography",)],
        input_schema,
    )

    actual = test_df.select(is_valid_geography("geography"))

    checked_schema = "geography_is_not_valid_geography: string"
    expected = spark.createDataFrame(
        [
            [None],
            [None],
            ["Value 'not-a-geography' in Column 'geography' is not a valid geography"],
        ],
        checked_schema,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)
