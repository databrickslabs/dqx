"""Integration tests for group-relative features (databrickslabs/dqx#1484).

The Python/Spark group-key parity test here is the most important one in this file: training
persists baselines under keys built in Python and scoring looks them up with keys built in Spark.
A disagreement raises nothing — every lookup misses and silently falls back to the global
baseline, producing a model that trains and scores cleanly while conditioning on nothing at all.
"""

import datetime
import math

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.scoring_utils import join_filtered_results_back
from databricks.labs.dqx.anomaly.segment_utils import (
    BASELINE_KEY_NULL,
    build_baseline_key,
    baseline_key_column,
)
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    apply_feature_engineering,
)


def _first(df) -> Row:
    """`.first()` narrowed to a Row. An empty frame is a test bug, not a valid outcome."""
    row = df.first()
    assert row is not None, "expected at least one row"
    return row


# ============================================================================
# The parity contract
# ============================================================================


@pytest.mark.parametrize(
    "rows",
    [
        [("DE", "casino"), ("IT", "live"), ("AT", "sports")],
        [("with space", "x"), ("with-dash", "y"), ("with_underscore", "z")],
        [("Ünïcödé", "a"), ("日本", "b")],
        [("", "empty-left"), ("empty-right", "")],
    ],
)
def test_python_and_spark_group_keys_agree(spark: SparkSession, rows):
    """The two halves of the key must produce byte-identical strings."""
    df = spark.createDataFrame(rows, "country string, product string")

    actual = [
        row["key"]
        for row in df.withColumn("key", baseline_key_column(["country", "product"]))
        .orderBy("country", "product")
        .select("key")
        .collect()
    ]
    expected = sorted(build_baseline_key({"country": c, "product": p}) for c, p in rows)

    assert actual == expected


def test_python_and_spark_agree_on_null_group_values(spark: SparkSession):
    """A null dimension must land in the same bucket on both sides, not propagate."""
    df = spark.createDataFrame([(None, "casino")], "country string, product string")

    spark_key = _first(df.withColumn("key", baseline_key_column(["country", "product"])))["key"]

    assert spark_key == build_baseline_key({"country": None, "product": "casino"})
    assert BASELINE_KEY_NULL in spark_key


def test_python_and_spark_agree_on_integral_group_values(spark: SparkSession):
    """Integral group columns are permitted, so their rendering must agree."""
    df = spark.createDataFrame([(42, -7)], "store_id int, offset int")

    spark_key = _first(df.withColumn("key", baseline_key_column(["store_id", "offset"])))["key"]

    assert spark_key == build_baseline_key({"store_id": 42, "offset": -7})


@pytest.mark.parametrize("flag", [True, False])
def test_python_and_spark_agree_on_boolean_group_values(spark: SparkSession, flag):
    """Regression: Spark casts booleans lowercase, Python's str() capitalises them.

    This is the disagreement that motivated ``_as_spark_string``. Left unhandled it produced
    ``True\\x1f42`` at training and ``true\\x1f42`` at scoring — every baseline lookup missing, the
    global baseline used instead, and no error raised anywhere.
    """
    df = spark.createDataFrame([(flag, 42)], "is_vip boolean, store_id int")

    spark_key = _first(df.withColumn("key", baseline_key_column(["is_vip", "store_id"])))["key"]

    assert spark_key == build_baseline_key({"is_vip": flag, "store_id": 42})
    assert str(flag).lower() in spark_key


def test_python_and_spark_agree_on_date_group_values(spark: SparkSession):
    """Dates are a permitted group type, so their rendering must agree too."""
    day = datetime.date(2026, 8, 25)
    df = spark.createDataFrame([(day, "DE")], "day date, country string")

    spark_key = _first(df.withColumn("key", baseline_key_column(["day", "country"])))["key"]

    assert spark_key == build_baseline_key({"day": day, "country": "DE"})


def test_group_key_is_independent_of_column_order(spark: SparkSession):
    """Sorting by column name means the declared order cannot change the key."""
    df = spark.createDataFrame([("DE", "casino")], "country string, product string")

    forward = _first(df.withColumn("key", baseline_key_column(["country", "product"])))["key"]
    reversed_order = _first(df.withColumn("key", baseline_key_column(["product", "country"])))["key"]

    assert forward == reversed_order


# ============================================================================
# The transform itself
# ============================================================================


def _numeric_info(name: str) -> ColumnTypeInfo:
    return ColumnTypeInfo(name=name, spark_type=T.DoubleType(), category="numeric", null_count=0)


def test_relative_feature_is_appended_last(spark: SparkSession):
    """Feature order is positional, so the new feature must land at the tail."""
    df = spark.createDataFrame(
        [("DE", 100.0), ("DE", 110.0), ("IT", 20.0), ("IT", 22.0)],
        "country string, amount double",
    )

    _, metadata = apply_feature_engineering(df, [_numeric_info("amount")], baseline_by=["country"])

    assert metadata.engineered_feature_names[-1] == "amount_rel_baseline"


def test_all_relative_features_form_the_trailing_block(spark: SparkSession):
    """With several metrics, every _rel_baseline feature is a trailing entry, one per metric.

    Strengthens the single-metric case above: an already-trained model is handed features in this
    order, so the relative block must stay contiguous at the tail — not interleaved with base
    features, which would silently reorder the model's inputs.
    """
    df = spark.createDataFrame(
        [("DE", 100.0, 5.0), ("DE", 110.0, 6.0), ("IT", 20.0, 1.0), ("IT", 22.0, 2.0)],
        "country string, amount double, quantity double",
    )

    _, metadata = apply_feature_engineering(
        df, [_numeric_info("amount"), _numeric_info("quantity")], baseline_by=["country"]
    )

    names = metadata.engineered_feature_names
    relative = [name for name in names if name.endswith("_rel_baseline")]
    assert set(relative) == {"amount_rel_baseline", "quantity_rel_baseline"}
    assert names[-len(relative) :] == relative  # a contiguous trailing block


def test_feature_prefix_matches_an_ungrouped_model(spark: SparkSession):
    """Everything before the appended tail must be identical to the ungrouped feature list.

    This is what makes an already-trained model safe: its features occupy the same positions.
    """
    df = spark.createDataFrame(
        [("DE", 100.0), ("DE", 110.0), ("IT", 20.0), ("IT", 22.0)],
        "country string, amount double",
    )

    _, ungrouped = apply_feature_engineering(df.select("amount"), [_numeric_info("amount")])
    _, grouped = apply_feature_engineering(df, [_numeric_info("amount")], baseline_by=["country"])

    prefix_len = len(ungrouped.engineered_feature_names)
    assert grouped.engineered_feature_names[:prefix_len] == ungrouped.engineered_feature_names


def test_no_group_by_leaves_the_feature_list_untouched(spark: SparkSession):
    """An empty grouping must make the transform a no-op, not merely a cheap one."""
    df = spark.createDataFrame([(100.0,), (110.0,)], "amount double")

    _, metadata = apply_feature_engineering(df, [_numeric_info("amount")], baseline_by=[])

    assert not [f for f in metadata.engineered_feature_names if f.endswith("_rel_baseline")]
    assert not metadata.baseline_medians


def test_group_columns_do_not_become_features(spark: SparkSession):
    """A group column is the comparison basis, not a metric; it must not reach the model."""
    df = spark.createDataFrame(
        [("DE", 100.0), ("DE", 110.0), ("IT", 20.0), ("IT", 22.0)],
        "country string, amount double",
    )

    engineered, metadata = apply_feature_engineering(df, [_numeric_info("amount")], baseline_by=["country"])

    assert "country" not in metadata.engineered_feature_names
    assert "country" not in engineered.columns


def test_relative_value_is_the_signed_log_ratio_to_the_group_median(spark: SparkSession):
    """Hand-computed value, so the formula is pinned rather than merely self-consistent."""
    rows = [("DE", 100.0), ("DE", 100.0), ("DE", 100.0), ("IT", 10.0), ("IT", 10.0), ("IT", 10.0)]
    df = spark.createDataFrame(rows, "country string, amount double")

    engineered, _ = apply_feature_engineering(df, [_numeric_info("amount")], baseline_by=["country"])
    values = {
        round(row["amount"], 4): round(row["amount_rel_baseline"], 4)
        for row in engineered.select("amount", "amount_rel_baseline").collect()
    }

    # Every row sits exactly on its own group's median, so the deviation is zero for both groups
    # despite their levels differing by an order of magnitude. That is the whole point.
    assert values[100.0] == pytest.approx(0.0, abs=1e-6)
    assert values[10.0] == pytest.approx(0.0, abs=1e-6)

    # And a value at twice its group median gives signed_log(200) - signed_log(100)
    df2 = spark.createDataFrame(rows + [("DE", 200.0)], "country string, amount double")
    engineered2, _ = apply_feature_engineering(df2, [_numeric_info("amount")], baseline_by=["country"])
    row = _first(engineered2.filter(F.col("amount") == 200.0))
    assert row["amount_rel_baseline"] == pytest.approx(math.log1p(200.0) - math.log1p(100.0), abs=1e-6)


def test_negative_metrics_stay_finite(spark: SparkSession):
    """Plain log1p is NaN for x <= -1; the signed form must survive signed metrics."""
    df = spark.createDataFrame(
        [("DE", -500.0), ("DE", -400.0), ("DE", -450.0), ("IT", -2.0), ("IT", -3.0)],
        "country string, profit double",
    )

    engineered, _ = apply_feature_engineering(df, [_numeric_info("profit")], baseline_by=["country"])
    values = [row["profit_rel_baseline"] for row in engineered.select("profit_rel_baseline").collect()]

    assert all(v is not None and math.isfinite(v) for v in values), values


def test_unseen_group_falls_back_to_the_global_baseline(spark: SparkSession):
    """A group absent at training must resolve to a defined value, not null the feature."""
    train = spark.createDataFrame([("DE", 100.0), ("DE", 110.0)], "country string, amount double")
    _, metadata = apply_feature_engineering(train, [_numeric_info("amount")], baseline_by=["country"])

    score = spark.createDataFrame([("BRAND_NEW", 105.0)], "country string, amount double")
    engineered, _ = apply_feature_engineering(
        score,
        [_numeric_info("amount")],
        baseline_by=metadata.baseline_by,
        baseline_medians=metadata.baseline_medians,
        global_medians=metadata.global_medians,
    )

    value = _first(engineered)["amount_rel_baseline"]
    assert value is not None


# ============================================================================
# is_new_baseline survives the row_filter merge-back
# ============================================================================


def test_unscored_row_keeps_its_info_when_merged_back(spark: SparkSession):
    """An unseen-group row carries a null score but a real is_new_baseline flag to report.

    join_filtered_results_back groups by the row id and aggregates with
    ``max_by(info, score)`` — which is null when the row's only score is null — so the merge would
    drop the info struct for exactly those rows. The ``coalesce(..., first(info))`` is what rescues
    it. This pins that a null-scored row keeps its info; regressing the coalesce would silently lose
    is_new_baseline on every unseen-group row.
    """
    df = spark.createDataFrame([(1,), (2,)], "row_id long")
    result = spark.createDataFrame(
        [(1, None, "unseen-group"), (2, 0.9, "scored")],
        "row_id long, score double, info string",
    )

    merged = join_filtered_results_back(df, result, ["row_id"], "score", "info")

    info_by_id = {row["row_id"]: row["info"] for row in merged.collect()}
    assert info_by_id[1] == "unseen-group"  # kept despite a null score
    assert info_by_id[2] == "scored"
