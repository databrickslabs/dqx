"""
Integration tests for has_no_aggr_outliers.

All time-series data is built inline using createDataFrame so no external
fixtures or tables are required beyond a running SparkSession.

Test matrix
-----------
1.  test_detects_spike_over_3_sigma         – 14 quiet days + 1 spike → violation
2.  test_passes_when_today_within_band      – 15 stable days → no violation
3.  test_warmup_passes                      – only 3 days of history → pass (warmup)
4.  test_constant_series_passes             – all identical values → sigma==0 → pass
5.  test_null_current_passes                – most-recent bucket missing → pass
6.  test_group_by_isolates_bands            – two groups, one spikes, one flat
7.  test_row_filter                         – junk rows excluded before baseline
8.  test_hour_truncation                    – time_interval="hour"
9.  test_yaml_round_trip                    – load check from YAML via DQEngine
10. test_with_column_expression_column      – column is a Column expression
11. test_count_star_aggr_type               – column="*", aggr_type="count"
12. test_sum_with_group_by                  – sum aggregate + group_by
13. test_warmup_equals_lookback_boundary    – warmup_num_intervals == lookback_num_intervals
14. test_aggr_params_percentile             – aggr_type="percentile" with aggr_params
15. test_invalid_time_column_type           – non-date/timestamp time_column raises error
"""

from collections.abc import Callable
from datetime import date, datetime, timedelta
import yaml

import pytest
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from databricks.labs.dqx.check_funcs import has_no_aggr_outliers
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

BASE_DATE = date(2024, 1, 1)


def _dates(num_days: int, *, offset: int = 0) -> list[date]:
    """Return a list of consecutive calendar dates starting at BASE_DATE + offset."""
    return [BASE_DATE + timedelta(days=offset + i) for i in range(num_days)]


def _apply(df: DataFrame, checks: list[tuple[Column, Callable]]) -> DataFrame:
    """Apply apply-closures in sequence and return the condition columns."""
    checked = df
    for _, apply_fn in checks:
        checked = apply_fn(checked)
    return checked.select(*[cond for cond, _ in checks])


def _apply_with_original(df: DataFrame, checks: list[tuple[Column, Callable]]) -> DataFrame:
    """Apply checks and return original columns + condition columns."""
    checked = df
    for _, apply_fn in checks:
        checked = apply_fn(checked)
    original_cols = [F.col(c) for c in df.columns]
    return checked.select(*original_cols, *[cond for cond, _ in checks])


# ---------------------------------------------------------------------------
# 1. Spike detection
# ---------------------------------------------------------------------------


def test_detects_spike_over_3_sigma(spark: SparkSession):
    """
    14 historical days of values drawn from a narrow band
    (mean=10, stddev_pop=1.0) followed by a spike value of 50 (~40 sigma).
    The check must report a violation on every row.
    """
    hist_values = [9.0, 11.0] * 7  # 14 values: mean=10, stddev_pop=1.0
    hist_dates = _dates(14)

    rows = list(zip(hist_dates, hist_values))
    today = BASE_DATE + timedelta(days=14)
    rows.append((today, 50.0))  # spike > 3 sigma above baseline of 10

    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert all(m is not None for m in msgs), f"Expected all rows to violate but got: {msgs}"
    # Check the message format — comma-separated metrics, no Unicode symbols
    assert "avg(metric): current=" in msgs[0]
    assert ", baseline=" in msgs[0]
    assert ", stddev=" in msgs[0]
    assert ", delta=" in msgs[0]
    assert "exceeds 3.0 x stddev" in msgs[0]
    assert "lookback=14 intervals" in msgs[0]


# ---------------------------------------------------------------------------
# 2. Values within band → pass
# ---------------------------------------------------------------------------


def test_passes_when_today_within_band(spark: SparkSession):
    """
    15 days of stable values (9/11 alternating, today=10).
    Mean=10, stddev=1, today=10 → delta=0 <= 3 stddev → all rows pass.
    """
    values = [9.0, 11.0] * 7 + [9.0, 10.0]  # 16 values — use first 15
    rows = [(BASE_DATE + timedelta(days=i), float(values[i])) for i in range(15)]
    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert all(m is None for m in msgs), f"Expected no violations but got: {msgs}"


# ---------------------------------------------------------------------------
# 3. Warmup guard
# ---------------------------------------------------------------------------


def test_warmup_passes(spark: SparkSession):
    """
    Only 3 days of data but warmup_num_intervals=7 → check should pass.
    """
    rows = [(BASE_DATE + timedelta(days=i), float(10 + i)) for i in range(3)]
    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=7,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert all(m is None for m in msgs), f"Warmup should suppress violation but got: {msgs}"


# ---------------------------------------------------------------------------
# 4. Constant series (stddev == 0) → pass
# ---------------------------------------------------------------------------


def test_constant_series_passes(spark: SparkSession):
    """
    15 identical values (stddev_pop=0) → the check must pass even when the
    current bucket differs slightly, because we guard on stddev==0.
    """
    rows = [(BASE_DATE + timedelta(days=i), 10.0) for i in range(14)]
    rows.append((BASE_DATE + timedelta(days=14), 10.5))  # slightly different today
    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert all(m is None for m in msgs), f"Constant-series guard should suppress violation but got: {msgs}"


# ---------------------------------------------------------------------------
# 5. Missing current bucket → pass
# ---------------------------------------------------------------------------


def test_null_current_passes(spark: SparkSession):
    """
    Only historical rows — no row for the most-recent expected bucket.
    The check should pass (let a freshness check handle missing data).
    """
    rows = [(BASE_DATE + timedelta(days=i), float(9 + (i % 3))) for i in range(14)]
    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    # All rows from history — current bucket is missing → condition column should be NULL
    msgs = [r[0] for r in result.collect()]
    assert all(m is None for m in msgs), f"Missing current should not fire violation but got: {msgs}"


# ---------------------------------------------------------------------------
# 6. group_by isolates per-group bands
# ---------------------------------------------------------------------------


def test_group_by_isolates_bands(spark: SparkSession):
    """
    Two groups: 'stable' (14 days at 10 +/- 1, today=10) and 'spike' (14 days
    at 10 +/- 1, today=50). Only the 'spike' group should violate.
    """
    hist_values = [9.0, 11.0] * 7  # mean=10, stddev_pop=1.0

    rows = []
    for i, value in enumerate(hist_values):
        event_date = BASE_DATE + timedelta(days=i)
        rows.append(("stable", event_date, value))
        rows.append(("spike", event_date, value))

    today = BASE_DATE + timedelta(days=14)
    rows.append(("stable", today, 10.0))  # within band
    rows.append(("spike", today, 50.0))  # spike

    df = spark.createDataFrame(rows, "grp: string, event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
        group_by=["grp"],
    )
    result = _apply_with_original(df, [(condition, apply_fn)])

    stable_msgs = [r[-1] for r in result.filter(F.col("grp") == "stable").collect()]
    spike_msgs = [r[-1] for r in result.filter(F.col("grp") == "spike").collect()]

    assert all(m is None for m in stable_msgs), f"Stable group should not violate: {stable_msgs}"
    assert all(m is not None for m in spike_msgs), f"Spike group should violate: {spike_msgs}"


# ---------------------------------------------------------------------------
# 7. row_filter excludes junk rows from baseline
# ---------------------------------------------------------------------------


def test_row_filter(spark: SparkSession):
    """
    Mix of valid rows (status='GOOD') and junk rows (status='DROP').
    The junk rows have extreme values that would skew the baseline if included.
    row_filter should exclude junk so baseline remains clean and the spike fires.
    """
    hist_values = [9.0, 11.0] * 7

    rows = []
    for i, value in enumerate(hist_values):
        event_date = BASE_DATE + timedelta(days=i)
        rows.append(("GOOD", event_date, value))
        rows.append(("DROP", event_date, 1000.0))  # would inflate baseline if not filtered

    today = BASE_DATE + timedelta(days=14)
    rows.append(("GOOD", today, 50.0))  # spike on good rows
    rows.append(("DROP", today, 1000.0))

    df = spark.createDataFrame(rows, "status: string, event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
        row_filter="status = 'GOOD'",
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    # With row_filter applied, the spike of 50 (GOOD) should fire
    assert any(m is not None for m in msgs), f"Expected violation with row_filter but got: {msgs}"


# ---------------------------------------------------------------------------
# 8. Hour truncation
# ---------------------------------------------------------------------------


def test_hour_truncation(spark: SparkSession):
    """
    48 hourly buckets: 47 quiet hours (alternating 9/11) and 1 spike at hour 47.
    With time_interval='hour' the spike must be detected.
    """
    base_dt = datetime(2024, 1, 1, 0, 0, 0)
    quiet_values = [9.0, 11.0] * 23 + [9.0]  # 47 values
    rows = [(base_dt + timedelta(hours=i), v) for i, v in enumerate(quiet_values)]
    rows.append((base_dt + timedelta(hours=47), 200.0))  # spike

    df = spark.createDataFrame(rows, "event_time: timestamp, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_time",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=46,
        warmup_num_intervals=3,
        time_interval="hour",
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert any(m is not None for m in msgs), f"Expected hourly spike violation but got: {msgs}"


# ---------------------------------------------------------------------------
# 9. YAML round-trip
# ---------------------------------------------------------------------------


def test_yaml_round_trip(ws, spark: SparkSession):
    """
    Load has_no_aggr_outliers from YAML via DQEngine and verify it produces
    the same result as the directly-constructed check.
    """
    hist_values = [9.0, 11.0] * 7
    rows = [(BASE_DATE + timedelta(days=i), v) for i, v in enumerate(hist_values)]
    rows.append((BASE_DATE + timedelta(days=14), 50.0))  # spike
    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    checks_yaml = yaml.safe_load(
        """
        - criticality: warn
          check:
            function: has_no_aggr_outliers
            arguments:
              column: metric
              time_column: event_date
              aggr_type: avg
              sigma: 3.0
              lookback_num_intervals: 14
              warmup_num_intervals: 3
        """
    )

    dq_engine = DQEngine(workspace_client=ws)
    result_yaml = dq_engine.apply_checks_by_metadata(df, checks_yaml)

    violations_yaml = result_yaml.filter(F.size(F.col("_warnings")) > 0).count()
    assert violations_yaml > 0, "YAML round-trip should detect the spike"

    # Also verify with the Python API and confirm counts match
    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result_api = apply_fn(df).select(condition)
    violations_api = result_api.filter(F.col(result_api.columns[0]).isNotNull()).count()
    assert (
        violations_api == violations_yaml
    ), f"YAML ({violations_yaml}) and API ({violations_api}) should report the same violation count"


# ---------------------------------------------------------------------------
# 10. Column expression as `column` argument
# ---------------------------------------------------------------------------


def test_with_column_expression_column(spark: SparkSession):
    """
    `column` can be a Spark Column expression (e.g. F.col('a') - F.col('b'))
    rather than a plain string; the check should still work correctly.
    """
    hist_values_a = [20.0, 22.0] * 7  # a values
    hist_values_b = [10.0, 10.0] * 7  # b values → a-b alternates 10/12

    rows = [(BASE_DATE + timedelta(days=i), hist_values_a[i], hist_values_b[i]) for i in range(14)]
    # Today: a=100, b=10 → metric=90 (massive spike)
    rows.append((BASE_DATE + timedelta(days=14), 100.0, 10.0))

    df = spark.createDataFrame(rows, "event_date: date, a: double, b: double")

    condition, apply_fn = has_no_aggr_outliers(
        F.col("a") - F.col("b"),
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert any(m is not None for m in msgs), f"Expected Column-expr spike to be detected but got: {msgs}"


# ---------------------------------------------------------------------------
# 11. count(*) aggregate
# ---------------------------------------------------------------------------


def test_count_star_aggr_type(spark: SparkSession):
    """
    column='*' with aggr_type='count' should count rows per bucket.
    14 days alternating 4/6 rows/day (mean=5, stddev_pop=1), today has 100 rows → spike detected.
    """
    rows = []
    for i in range(14):
        event_date = BASE_DATE + timedelta(days=i)
        # Alternate 4 and 6 rows per day so stddev_pop > 0 and the constant-series guard does not fire.
        for _ in range(4 if i % 2 == 0 else 6):
            rows.append((event_date,))

    # Today: 100 rows (spike)
    today = BASE_DATE + timedelta(days=14)
    for _ in range(100):
        rows.append((today,))

    df = spark.createDataFrame(rows, "event_date: date")

    condition, apply_fn = has_no_aggr_outliers(
        "*",
        "event_date",
        aggr_type="count",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert any(m is not None for m in msgs), f"Expected count(*) spike to be detected but got: {msgs}"


# ---------------------------------------------------------------------------
# 12. sum aggregate with group_by
# ---------------------------------------------------------------------------


def test_sum_with_group_by(spark: SparkSession):
    """
    sum aggregate + group_by: two regions, one spikes in total daily revenue.
    Only the spiking region should violate.
    """
    rows = []
    for i in range(14):
        event_date = BASE_DATE + timedelta(days=i)
        # Alternate 90/110 so daily sums vary (mean=100, stddev_pop=10) and the
        # constant-series guard does not suppress the spike.
        revenue = 90.0 if i % 2 == 0 else 110.0
        rows.append(("us-east", event_date, revenue))
        rows.append(("eu-west", event_date, revenue))

    today = BASE_DATE + timedelta(days=14)
    rows.append(("us-east", today, 100.0))  # normal
    rows.append(("eu-west", today, 5000.0))  # spike

    df = spark.createDataFrame(rows, "region: string, event_date: date, revenue: double")

    condition, apply_fn = has_no_aggr_outliers(
        "revenue",
        "event_date",
        aggr_type="sum",
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
        group_by=["region"],
    )
    result = _apply_with_original(df, [(condition, apply_fn)])

    east_msgs = [r[-1] for r in result.filter(F.col("region") == "us-east").collect()]
    west_msgs = [r[-1] for r in result.filter(F.col("region") == "eu-west").collect()]

    assert all(m is None for m in east_msgs), f"us-east should not violate: {east_msgs}"
    assert all(m is not None for m in west_msgs), f"eu-west should violate: {west_msgs}"


# ---------------------------------------------------------------------------
# 13. warmup_num_intervals == lookback_num_intervals boundary
# ---------------------------------------------------------------------------


def test_warmup_equals_lookback_boundary(spark: SparkSession):
    """
    When warmup_num_intervals == lookback_num_intervals, the check fires only
    once exactly that many history buckets are available.

    Setup: lookback=warmup=5. Feed 5 history days + 1 spike. Expect violation.
    Then feed only 4 history days + 1 spike. Expect no violation (warmup guard).
    """
    hist_values = [9.0, 11.0, 9.0, 11.0, 9.0]  # 5 days: mean=10, stddev_pop~=1

    # Case A: exactly 5 history days → warmup satisfied → should violate
    rows_a = [(BASE_DATE + timedelta(days=i), v) for i, v in enumerate(hist_values)]
    rows_a.append((BASE_DATE + timedelta(days=5), 50.0))  # spike
    df_a = spark.createDataFrame(rows_a, "event_date: date, metric: double")

    condition_a, apply_fn_a = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=5,
        warmup_num_intervals=5,
    )
    msgs_a = [r[0] for r in apply_fn_a(df_a).select(condition_a).collect()]
    assert any(m is not None for m in msgs_a), "Should fire when warmup == lookback and history is satisfied"

    # Case B: only 4 history days → warmup not satisfied → should NOT violate
    rows_b = [(BASE_DATE + timedelta(days=i), v) for i, v in enumerate(hist_values[:4])]
    rows_b.append((BASE_DATE + timedelta(days=4), 50.0))  # spike but warmup guard fires
    df_b = spark.createDataFrame(rows_b, "event_date: date, metric: double")

    condition_b, apply_fn_b = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="avg",
        sigma=3.0,
        lookback_num_intervals=5,
        warmup_num_intervals=5,
    )
    msgs_b = [r[0] for r in apply_fn_b(df_b).select(condition_b).collect()]
    assert all(m is None for m in msgs_b), f"Should not fire when history < warmup: {msgs_b}"


# ---------------------------------------------------------------------------
# 14. aggr_params — percentile aggregate
# ---------------------------------------------------------------------------


def test_aggr_params_percentile(spark: SparkSession):
    """
    aggr_type="percentile" with aggr_params={"percentile": 0.95} should compute
    the 95th-percentile per bucket. 14 days of tight values (median~10), then a
    spike day where even the 95th percentile is 50 → violation detected.
    """
    low_day = (8.0, 9.0, 9.0, 10.0, 10.0, 10.0, 11.0, 11.0, 12.0, 12.0)  # p95 ≈ 12
    high_day = (9.0, 10.0, 10.0, 11.0, 11.0, 11.0, 12.0, 12.0, 13.0, 13.0)  # p95 ≈ 13
    rows = []
    for i in range(14):
        event_date = BASE_DATE + timedelta(days=i)
        # Alternate per-day distributions so p95 varies day-to-day (stddev_pop > 0)
        # and the constant-series guard does not suppress the spike.
        for value in low_day if i % 2 == 0 else high_day:
            rows.append((event_date, value))

    # Spike day: all values between 45 and 55 → p95 ≈ 54
    today = BASE_DATE + timedelta(days=14)
    for value in (45.0, 47.0, 49.0, 50.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0):
        rows.append((today, value))

    df = spark.createDataFrame(rows, "event_date: date, metric: double")

    condition, apply_fn = has_no_aggr_outliers(
        "metric",
        "event_date",
        aggr_type="percentile",
        aggr_params={"percentile": 0.95},
        sigma=3.0,
        lookback_num_intervals=14,
        warmup_num_intervals=3,
    )
    result = apply_fn(df).select(condition)

    msgs = [r[0] for r in result.collect()]
    assert any(
        m is not None for m in msgs
    ), f"Expected percentile spike to be detected with aggr_params but got: {msgs}"


# ---------------------------------------------------------------------------
# 15. Invalid time_column type → raise at apply() time
# ---------------------------------------------------------------------------


def test_invalid_time_column_type(spark: SparkSession):
    """
    Passing a string column (not date/timestamp) as time_column must raise
    InvalidParameterError when apply() is called and the DataFrame schema is
    inspected, rather than failing with a cryptic Spark error at query time.
    """
    df = spark.createDataFrame(
        [(1, "2024-01-01"), (2, "2024-01-02")],
        "metric: int, ts_str: string",
    )
    _, apply_fn = has_no_aggr_outliers("metric", "ts_str")
    with pytest.raises(InvalidParameterError, match="date or timestamp"):
        apply_fn(df)
