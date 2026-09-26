"""Detection quality of baseline conditioning, end to end through Spark and MLflow.

This is the assertion that encodes the design claim, so that a future change which reverses it fails
a test rather than quietly shipping. It complements, rather than duplicates,
``tests/unit/test_anomaly_relative_feature_separability.py``: that one measures the *mechanism* in
numpy in under five seconds, this one measures the *pipeline* — Spark feature engineering, persisted
baselines, the scoring UDF, and severity calibration all included.

Deliberately **one test with several assertions** rather than four tests. Every property below is
read off the same pair of trained models, and each model is a real Isolation Forest registered in
Unity Catalog: splitting them would retrain that pair four times over. pytester's ``spark`` fixture
is function-scoped, so a module-scoped fixture cannot hold the pair across tests. Each assertion
carries its own message, so a failure still says which property broke.

Metric discipline lives in ``quality_metrics.py``; in particular there is no point-adjusted F1
anywhere, for the reason documented there.
"""

import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from databricks.labs.dqx.config import AnomalyParams

from tests.integration_anomaly.quality_metrics import (
    macro_average,
    per_group_metrics,
    pr_auc,
    trivial_baselines,
    worst_group_false_positive_rate,
)
from tests.integration_anomaly.synthetic_generators import generate_group_conditional_data

BASELINE_COLUMNS = ["country", "event_type", "product"]
TRAIN_SCHEMA = "day int, country string, event_type string, product string, event_count double, is_anomaly double"

# Enough positives for average precision to mean something: 6 groups affected across 3 days.
INCIDENT_DAYS = 3
INCIDENT_GROUPS = 6

# Enough normal rows per group for a per-group false-positive *rate* to exist. With one control day
# each of the 90 groups has a handful of test rows, so the worst rate across them reaches 1.0 by
# chance — which is exactly what the first run of this test reported.
CONTROL_DAYS = 25
MIN_NORMAL_ROWS_PER_GROUP = 10

# The gap conditioning must clear. Loose on purpose — the claim is the direction and rough size, not
# a number tight enough to make this a tripwire for Isolation Forest's RNG. The offline measurement
# of the same mechanism moved PR-AUC from 0.0028 to 0.6962, so 0.10 is a wide margin.
MIN_PR_AUC_GAIN = 0.10

# Above this, a single group is absorbing so many false alarms that the model is unusable for it.
# The per-group configuration this replaces reached 0.56 on the Server Machine Dataset.
MAX_WORST_GROUP_FPR = 0.5

pytestmark = pytest.mark.slow


def _scored_frame(anomaly_scorer, test_df, model, registry):
    """Score, then collect the columns the metrics need into pandas."""
    result = anomaly_scorer(test_df, model, registry, extract_score=False)
    anomaly = F.element_at(F.col("_dq_info"), 1).getField("anomaly")
    return (
        result.select(
            F.col("country"),
            F.col("event_type"),
            F.col("product"),
            F.col("event_count"),
            F.col("is_anomaly").alias("label"),
            anomaly.getField("score").alias("score"),
            anomaly.getField("is_anomaly").cast("double").alias("flagged"),
            F.concat_ws("|", F.col("country"), F.col("event_type"), F.col("product")).alias("grp"),
        )
        .toPandas()
        .dropna(subset=["score"])
    )


def test_baseline_conditioning_detection_quality(spark: SparkSession, quick_model_factory, anomaly_scorer):
    """Conditioning beats whole-table comparison, beats doing almost nothing, and stays fair.

    Trains two models on identical data — one conditioned on ``baseline_by``, one not — and compares
    them on the same scored rows.
    """
    columns, train_df, test_df, _ = generate_group_conditional_data(
        spark,
        n_incident_days=INCIDENT_DAYS,
        n_incident_groups=INCIDENT_GROUPS,
        n_control_days=CONTROL_DAYS,
    )
    train_rows = [tuple(r) for r in train_df.collect()]

    frames = {}
    for name, baseline_by in (("conditioned", BASELINE_COLUMNS), ("pooled", [])):
        model, registry, _ = quick_model_factory(
            spark,
            columns=columns,
            train_data=train_rows,
            train_schema=TRAIN_SCHEMA,
            params=AnomalyParams(sample_fraction=1.0),
            baseline_by=baseline_by,
        )
        frames[name] = _scored_frame(anomaly_scorer, test_df, model, registry)

    conditioned, pooled = frames["conditioned"], frames["pooled"]
    conditioned_pr_auc = pr_auc(conditioned["label"], conditioned["score"])
    pooled_pr_auc = pr_auc(pooled["label"], pooled["score"])

    # 1. The design claim: a contextual collapse is invisible to a whole-table comparison.
    assert conditioned_pr_auc > pooled_pr_auc + MIN_PR_AUC_GAIN, (
        f"baseline conditioning should beat whole-table comparison on a contextual anomaly "
        f"(pooled PR-AUC {pooled_pr_auc:.4f}, conditioned {conditioned_pr_auc:.4f})"
    )

    # 2. Kim et al.'s recommendation: report improvement over doing almost nothing. max_abs_z is a
    #    genuine competitor on data whose anomalies are simply extreme, which is what makes beating
    #    it evidence that the model contributes something a one-liner would not.
    baselines = trivial_baselines(conditioned, ["event_count"])
    assert (
        conditioned_pr_auc > baselines["random"]
    ), f"conditioned {conditioned_pr_auc:.4f} did not beat random {baselines['random']:.4f}"
    assert conditioned_pr_auc > baselines["max_abs_z"], (
        f"conditioned {conditioned_pr_auc:.4f} did not beat a max-abs-z baseline "
        f"{baselines['max_abs_z']:.4f}; the fixture's anomalies may be globally extreme rather "
        "than contextual"
    )

    # 3. Fairness across groups. One pooled model shares a single calibration, so no group should
    #    carry a wildly disproportionate share of the false positives. Asserted on the worst group
    #    rather than the average, because the average is what hid this failure on SMD.
    group_metrics = per_group_metrics(conditioned, group_col="grp")
    worst = worst_group_false_positive_rate(group_metrics, min_normal_rows=MIN_NORMAL_ROWS_PER_GROUP)
    assert worst < MAX_WORST_GROUP_FPR, f"worst per-group false-positive rate was {worst:.3f}"

    # 4. Assertion 3 must have actually been measured. A NaN worst-rate means every group was too
    #    small to have one, which would make the assertion above pass vacuously.
    assert np.isfinite(worst), (
        f"no group had at least {MIN_NORMAL_ROWS_PER_GROUP} normal rows, so the worst-group "
        "false-positive rate was never measured"
    )
    assert len(group_metrics) > 1, "expected more than one group to evaluate"
    assert group_metrics["n_anomalies"].sum() > 1, "fixture must plant more than one positive"
    macro_average(group_metrics, "pr_auc")  # must not raise on this shape
