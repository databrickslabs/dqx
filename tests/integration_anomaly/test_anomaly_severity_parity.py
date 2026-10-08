"""Spark and NumPy must agree at repeated calibration knots and their boundaries."""

import numpy as np
import pytest

from databricks.labs.dqx.anomaly.explainability import severity_from_scores
from databricks.labs.dqx.anomaly.scoring_utils import add_severity_percentile_column


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
