"""Measure whether a table has enough structure over time for a temporal baseline to be worth fitting.

Separate from :mod:`databricks.labs.dqx.anomaly.temporal` because that module is deliberately Spark-free:
it is pure numpy so it can be unit-tested and evaluated per row inside a pandas UDF. This one needs Spark,
because the measurement runs over the training frame before any model exists.

The statistic exists to advise, never to decide. ``baseline_over_time`` stays something the caller asks
for: whether a metric's own history is worth comparing against is a judgement about the data, and DQX
cannot verify it without labels. But a caller deserves to be told when the training window shows nothing
to expect, because the transform is not free -- on a largely stationary dataset it measured *worse* than
leaving it off, costing Isolation Forest 15 points of event coverage.

Measured separation, which is what makes a warning worth emitting at all: the Server Machine Dataset sits
at a median 0.016 while synthetic trending data reaches 0.999.
"""

import logging

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

from databricks.labs.dqx.anomaly.temporal import TemporalBasis, candidate_periods, trend_strength

logger = logging.getLogger(__name__)

#: Below this share of variance explained, the caller is told the parameter is unlikely to help. Set
#: between the two measured regimes rather than at a round number: SMD's stationary metrics sit at 0.016
#: and a genuinely trending series at 0.999, so anything in this range separates them with room to spare.
TREND_STRENGTH_ADVISORY_FLOOR = 0.10

#: Buckets the measurement is taken over. Bounded so the advisory costs a fixed aggregation regardless of
#: table size, matching how the fit itself is computed.
ADVISORY_BUCKETS = 1000


def measure_trend_strength(df: DataFrame, time_column: str, metrics: list[str]) -> float:
    """Share of variance a trend-and-seasonality fit removes, as the median across *metrics*.

    Reduced to a bucketed aggregate first, for the same reason the fit is: driver memory has to stay
    bounded, and a per-bucket median attenuates outliers before the fit sees them.

    Returns 0.0 when the measurement cannot be taken, which reads as "no evidence of structure" and so
    produces the advisory. That is the conservative direction for a warning: it speaks up rather than
    staying quiet when it does not know.
    """
    seconds = F.unix_timestamp(F.col(time_column).cast(TimestampType())).cast(DoubleType())
    bounds = df.agg(F.min(seconds).alias("lo"), F.max(seconds).alias("hi")).first()
    if bounds is None or bounds["lo"] is None or bounds["hi"] is None:
        return 0.0

    span = max(float(bounds["hi"]) - float(bounds["lo"]), 1.0)
    relative = seconds - F.lit(float(bounds["lo"]))
    width = span / ADVISORY_BUCKETS
    rows = (
        df.withColumn("__dqx_advisory_bucket", F.floor(relative / F.lit(width)))
        .groupBy("__dqx_advisory_bucket")
        .agg(
            F.min(relative).alias("__dqx_seconds"),
            *[F.percentile_approx(F.col(metric).cast(DoubleType()), 0.5).alias(metric) for metric in metrics],
        )
        .orderBy("__dqx_advisory_bucket")
        .collect()
    )
    if len(rows) < 3:
        return 0.0

    axis = np.array([float(row["__dqx_seconds"]) for row in rows], dtype=float)
    columns = {
        metric: np.array([row[metric] for row in rows], dtype=float)
        for metric in metrics
        if any(row[metric] is not None for row in rows)
    }
    usable = {name: values[~np.isnan(values)] for name, values in columns.items()}
    usable = {name: values for name, values in usable.items() if values.size >= 3}
    if not usable:
        return 0.0

    # Measure against the basis the fit would actually use, so the advisory answers the question the
    # caller is about to ask rather than a different one. A window too short for a seasonal term reports
    # trend only, which is exactly what it would get.
    periods, _ = candidate_periods(axis)
    basis = TemporalBasis(trend=True, periods=periods, span=span)
    trimmed_axis = axis[: min(len(axis), min(values.size for values in usable.values()))]
    aligned = {name: values[: trimmed_axis.size] for name, values in usable.items()}
    return trend_strength(trimmed_axis, aligned, basis)
