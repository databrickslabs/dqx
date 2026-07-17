import pyspark.sql.types as T
import pytest

from databricks.labs.dqx.profiler.profile_builder import make_has_no_outliers_profile

@pytest.mark.parametrize(
    "col_type",
    [(T.ByteType()), (T.IntegerType()), (T.LongType()), (T.ShortType()), (T.FloatType()), (T.DoubleType())],
)
def test_make_has_no_outliers_profile_empty_data_frame(spark,col_type):
    """No profile when count_non_null is zero (early-exit path)."""
    df = spark.createDataFrame([], T.StructType([T.StructField("col", col_type)]))
    profiler_metrics = {"count_non_null": 0}
    profiler_options = {"outliers_ratio": 0.01}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is None


def test_make_has_no_outliers_profile_bounds_none(spark):
    """No profile when MAD bounds cannot be computed because the effective DataFrame is empty.

    count_non_null is non-zero so the early-exit check is bypassed; the empty df makes
    calculate_median_absolute_deviation_bounds return None, exercising the bounds=None path.
    """
    df = spark.createDataFrame([], T.StructType([T.StructField("col", T.IntegerType())]))
    profiler_metrics = {"count_non_null": 5}
    profiler_options = {"outliers_ratio": 0.01}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is None


@pytest.mark.parametrize(
    "col_type,data",
    [
        (T.ByteType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (127,)]),
        (T.IntegerType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (1000,)]),
        (T.LongType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (1000,)]),
        (T.ShortType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (1000,)]),
        (T.FloatType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (1000,)]),
        (T.DoubleType(), [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (1000,)]),
    ]
)
def test_make_has_no_outliers_profile_outliers_below_threshold(spark, col_type, data):
    """
    Test expects profile to be created for the following case: 11 values with one extreme value.
    1 outlier out of 11 ≈ 9 %. The threshold configuration is 10 %, hence profile is expected to return.
    MAD bounds: median=6; MAD=3; lower=-4.5; upper=16.5; Hence only 1000 is an outlier.
    """
    df = spark.createDataFrame(data, T.StructType([T.StructField("col", col_type)]))
    profiler_metrics = {"count_non_null": len(data)}
    profiler_options = {"outliers_ratio": 0.1}
    profile = make_has_no_outliers_profile(df, "col", col_type, profiler_metrics, profiler_options)
    assert profile is not None
    assert profile.name == "has_no_outliers"
    assert profile.column == "col"
    assert profile.filter is None


@pytest.mark.parametrize(
    "col_type,data",
    [
        (T.ByteType(), [(1,), (2,), (3,), (4,), (100,), (120,), (127,)]),
        (T.IntegerType(), [(1,), (2,), (3,), (4,), (100,), (200,), (300,)]),
        (T.LongType(), [(1,), (2,), (3,), (4,), (100,), (200,), (300,)]),
        (T.ShortType(), [(1,), (2,), (3,), (4,), (100,), (200,), (300,)]),
        (T.FloatType(), [(1,), (2,), (3,), (4,), (100,), (200,), (300,)]),
        (T.DoubleType(), [(1,), (2,), (3,), (4,), (100,), (200,), (300,)]),
    ]
)
def test_make_has_no_outliers_profile_outliers_above_threshold(spark,col_type,data):
    """
    Test expects no profile for the following case: 7 values with three extreme values.
    3 outliers out of 7 ≈ 43 %. The threshold configuration is 10 %, hence no profile is expected to return.
    MAD bounds: median=4; MAD=3; lower=-6.5; upper=14.5; Hence 100, 200, 300 are outliers.
    """
    # [1..4] + three extreme values → 3 outliers out of 7 ≈ 43 %, threshold 10 % → None
    # MAD bounds: median=4, MAD=3 → lower=-6.5, upper=14.5 → 100, 200, 300 are outliers
    df = spark.createDataFrame(data, T.StructType([T.StructField("col", col_type)]))
    profiler_metrics = {"count": len(data)}
    profiler_options = {"outliers_ratio": 0.1}
    profile = make_has_no_outliers_profile(df, "col", col_type, profiler_metrics, profiler_options)
    assert profile is None

@pytest.mark.parametrize(
    "col_type",
    [
        (T.ByteType()),
        (T.IntegerType()),
        (T.LongType()),
        (T.ShortType()),
        (T.FloatType()),
        (T.DoubleType()),
    ]
)
def test_make_has_no_outliers_profile_outliers_null_values(spark, col_type):
    """
    Test expects no profile for the following case: data frame is not empty but consists of null values.
    """
    data = [(None,), (None,)]
    df = spark.createDataFrame(data, T.StructType([T.StructField("col", col_type)]))
    profiler_metrics = {"count_non_null": 0}
    profiler_options = {"outliers_ratio": 0.1}
    profile = make_has_no_outliers_profile(df, "col", col_type, profiler_metrics, profiler_options)
    assert profile is None