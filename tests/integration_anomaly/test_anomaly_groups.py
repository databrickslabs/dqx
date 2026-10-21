"""The regression test that justifies databrickslabs/dqx#1484.

A group whose volume collapses 80% while the daily total is held exactly flat. The collapsed value
is ordinary in the global distribution, so a model that compares against the whole table cannot see
it — only a comparison against that group's own baseline can. Measured on the real harness: 45.1
without conditioning (the 45th percentile, missed) versus 95.5 with it.

Deliberately one model, not ninety: `baseline_by` trains a single pooled model whatever the group
count, which is the scaling property being tested alongside the detection claim.

The pooled counterpart of these numbers is asserted offline, without Spark, in
``tests/unit/test_anomaly_relative_feature_separability.py`` — there a pooled Isolation Forest scored
PR-AUC 0.0028 against a 0.0026 base rate, i.e. chance.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly.segment_utils import baseline_key_column
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError

from tests.integration_anomaly.synthetic_generators import generate_group_conditional_data

BASELINE_COLUMNS = ["country", "event_type", "product"]
TRAIN_SCHEMA = "day int, country string, event_type string, product string, event_count double, is_anomaly double"

pytestmark = pytest.mark.slow


def _severity_of_incident_group(result_df, incident_key: str):
    """Severity for the incident group on the incident day (the latest day present).

    Matches the **whole** baseline key. An earlier version filtered on the country alone and took
    ``first()``, but country ``C0`` spans fifteen groups here and only one of them collapsed, so it
    returned an arbitrary sibling — passing at severity 95.5 on one run and failing at 82.3 on the
    next with identical data.

    Building the key with the production :func:`baseline_key_column` rather than by hand also makes
    this assertion depend on Python/Spark key agreement, which is the invariant most expensive to
    get wrong.
    """
    latest_day = result_df.agg(F.max("day")).first()[0]
    return (
        result_df.filter(F.col("day") == latest_day)
        .filter(baseline_key_column(BASELINE_COLUMNS) == F.lit(incident_key))
        .select(
            F.element_at(F.col("_dq_info"), 1).getField("anomaly").getField("severity_percentile").alias("severity")
        )
        .first()
    )


def test_baseline_conditioning_catches_a_contextual_collapse(spark: SparkSession, quick_model_factory, anomaly_scorer):
    """The core claim: conditioning finds what comparing against the whole table cannot."""
    columns, train_df, test_df, incident_key = generate_group_conditional_data(spark)
    train_rows = [tuple(r) for r in train_df.collect()]

    model, registry, _ = quick_model_factory(
        spark,
        columns=columns,
        train_data=train_rows,
        train_schema=TRAIN_SCHEMA,
        params=AnomalyParams(sample_fraction=1.0),
        baseline_by=BASELINE_COLUMNS,
    )

    result = anomaly_scorer(test_df, model, registry, extract_score=False)
    incident = _severity_of_incident_group(result, incident_key)

    assert incident is not None
    assert incident["severity"] is not None
    # A range rather than an exact value: severity interpolates an approximate quantile grid.
    assert incident["severity"] >= 90.0, f"expected the collapse to be flagged, got {incident['severity']}"


def test_baseline_conditioning_trains_one_model_regardless_of_group_count(spark: SparkSession, quick_model_factory):
    """The scaling property. Ninety groups must not mean ninety models."""
    columns, train_df, _, _ = generate_group_conditional_data(spark)
    train_rows = [tuple(r) for r in train_df.collect()]

    _, registry, _ = quick_model_factory(
        spark,
        columns=columns,
        train_data=train_rows,
        train_schema=TRAIN_SCHEMA,
        params=AnomalyParams(sample_fraction=1.0),
        baseline_by=BASELINE_COLUMNS,
    )

    segmented = spark.table(registry).filter(F.col("identity.model_name").contains("__seg_")).count()
    assert segmented == 0, "baseline_by must not register per-segment models"


# ============================================================================
# API surface
# ============================================================================


def test_float_baseline_columns_are_rejected(spark: SparkSession, quick_model_factory):
    """Spark and Python format floats differently, which would break the baseline key silently."""
    with pytest.raises(InvalidParameterError, match="unsupported types"):
        quick_model_factory(
            spark,
            columns=["amount"],
            train_data=[(float(i), float(i % 3)) for i in range(60)],
            train_schema="amount double, ratio double",
            baseline_by=["ratio"],
        )


def test_baseline_column_cannot_also_be_a_feature(spark: SparkSession, quick_model_factory):
    """A column cannot be both the basis of comparison and the thing compared."""
    with pytest.raises(InvalidParameterError, match="both as features and as baseline_by"):
        quick_model_factory(
            spark,
            columns=["amount", "country"],
            train_data=[(float(i), "DE") for i in range(60)],
            train_schema="amount double, country string",
            baseline_by=["country"],
        )
