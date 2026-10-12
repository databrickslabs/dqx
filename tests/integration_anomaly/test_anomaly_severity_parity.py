"""Spark and NumPy must agree at repeated calibration knots and their boundaries."""

import math

import numpy as np
import pytest
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly.explainability import severity_from_scores
from databricks.labs.dqx.anomaly.scoring_utils import add_severity_percentile_column, displayed_severity_expr


@pytest.mark.parametrize(
    "points",
    [
        [(0.0, 0.0), (90.0, 1.0), (95.0, 1.0), (99.0, 2.0), (100.0, 3.0)],
        [(0.0, 1.0), (95.0, 1.0), (99.0, 1.0), (100.0, 1.0)],
        [(0.0, 0.0), (90.0, 1.0), (95.0, 1.0)],
        [(0.0, 0.0), (95.0, 1.0), (99.0, 1.0), (100.0, 2.0)],
    ],
)
def test_numpy_severity_matches_spark_at_tied_and_adjacent_knots(spark, points):
    values = [0.0, float(np.nextafter(1.0, 0.0)), 1.0, float(np.nextafter(1.0, 2.0)), 1.5, 2.0, 3.0]
    df = spark.createDataFrame(list(enumerate(values)), "row_id int, score double")
    actual = add_severity_percentile_column(df, score_col="score", severity_col="severity", quantile_points=points)
    observed = [row.severity for row in actual.orderBy("row_id").collect()]
    np.testing.assert_allclose(observed, severity_from_scores(np.array(values), points), atol=1e-10)


@pytest.mark.parametrize("threshold", [0.0, 1e-10, 0.9, 90.01, 99.01, 99.995, 100.0])
def test_published_severity_matches_flag_at_adjacent_threshold_values(spark, threshold: float) -> None:
    """Execute the actual published Spark expression at boundaries that binary scaling gets wrong."""
    values = [
        max(0.0, math.nextafter(threshold, -math.inf)),
        threshold,
        min(100.0, math.nextafter(threshold, math.inf)),
        100.0,
    ]
    source = spark.createDataFrame([*enumerate(values), (len(values), None)], "row_id int, severity double")
    observed = (
        source.select("row_id", "severity", displayed_severity_expr(F.col("severity"), threshold).alias("published"))
        .orderBy("row_id")
        .collect()
    )
    assert len(observed) == len(values) + 1
    assert observed[-1].published is None
    for row in observed[:-1]:
        assert row.published <= row.severity
        assert (row.published >= threshold) is (row.severity >= threshold)
    if threshold > 0.0:
        assert observed[0].published < threshold
    assert observed[-2].published == 100.0
