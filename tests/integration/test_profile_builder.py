import pyspark.sql.types as T

from databricks.labs.dqx.profiler.profile_builder import make_has_no_outliers_profile


def test_make_has_no_outliers_profile_empty_data_frame(spark):
    """Test verifies that no profile should be created for an empty input dataframe"""
    df = spark.createDataFrame([], T.StructType([T.StructField("col", T.IntegerType())]))
    profiler_metrics = {"count": 0}
    profiler_options = {"outliers_ratio": 0.01}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is None


def test_make_has_no_outliers_profile_filter_empty_data_frame(spark):
    """Test verifies that no profile should be created for a dataframe empty after filtering"""
    df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], "col: int")
    profiler_metrics = {"count": 5}
    profiler_options = {"outliers_ratio": 0.01, "filter": "col > 1000"}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is None


def test_make_has_no_outliers_profile_outliers_bellow_threshold(spark):
    """
    Test expects profile to be created for the following case: 11 values with one extreme value.
    1 outlier out of 11 ≈ 9 %. The threshold configuration is 10 %, hence profile is expected to return.
    MAD bounds: median=6; MAD=3; lower=-4.5; upper=16.5; Hence only 1000 is an outlier.
    """
    data = [(v,) for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000]]
    df = spark.createDataFrame(data, "col: int")
    profiler_metrics = {"count": len(data)}
    profiler_options = {"outliers_ratio": 0.1}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is not None
    assert profile.name == "has_no_outliers"
    assert profile.column == "col"
    assert profile.filter is None


def test_make_has_no_outliers_profile_outliers_above_threshold(spark, ws):
    """
    Test expects no profile for the following case: 7 values with three extreme values.
    3 outliers out of 7 ≈ 43 %. The threshold configuration is 10 %, hence no profile is expected to return.
    MAD bounds: median=4; MAD=3; lower=-6.5; upper=14.5; Hence 100, 200, 300 are outliers.
    """
    # [1..4] + three extreme values → 3 outliers out of 7 ≈ 43 %, threshold 10 % → None
    # MAD bounds: median=4, MAD=3 → lower=-6.5, upper=14.5 → 100, 200, 300 are outliers
    data = [(v,) for v in [1, 2, 3, 4, 100, 200, 300]]
    df = spark.createDataFrame(data, "col: int")
    profiler_metrics = {"count": len(data)}
    profiler_options = {"outliers_ratio": 0.1}
    profile = make_has_no_outliers_profile(df, "col", T.IntegerType(), profiler_metrics, profiler_options)
    assert profile is None
