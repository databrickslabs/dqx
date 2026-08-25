"""Performance benchmarks for row anomaly detection.

Two things make these different from the check benchmarks in ``test_apply_checks.py``, and both
shape how they are written:

**They are Databricks-backed.** Each one trains an Isolation Forest and registers it in MLflow under
Unity Catalog, so a single round costs minutes of control-plane latency rather than milliseconds of
Spark. They therefore use ``benchmark.pedantic(rounds=1)`` rather than the fixture's default
calibration — ``--benchmark-min-rounds=5`` in the nightly would otherwise mean five full
train-and-register cycles per test, twice over, since the benchmark job runs ``pytest tests/perf``
again for the comparison step.

**They carry quality metrics, not just timings.** ROC-AUC and friends ride along in
``benchmark.extra_info`` so the published report can show them. Treat those as *indicative and
first-observed only*: the nightly merges baselines keep-old-on-conflict, so once a benchmark exists in
``baseline.json`` its ``extra_info`` is never refreshed. Real quality-regression detection lives in
``tests/integration_anomaly/test_anomaly_quality.py``, which asserts rather than reports.

The module is marked ``anomaly`` so the nightly can deselect it from the timing-comparison gate
(``--benchmark-compare-fail=mean:25%``), which is global and would flake on control-plane variance.
"""

from typing import cast

import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import mlflow
from mlflow.tracking import MlflowClient

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.synthetic_generators import (
    generate_heavy_tail_data,
    generate_overlapping_gaussian_data,
)

pytestmark = pytest.mark.anomaly

BENCHMARK_GROUP = "anomaly_synthetic"

# Fixed so the published quality numbers mean something across runs, and recorded in extra_info so
# the report can state what they were measured on.
SEED = 42
N_SAMPLES = 3000
N_FEATURES = 16
ANOMALY_FRAC = 0.03


def _cleanup_anomaly_mlflow(model_name: str, registry_table: str, spark: SparkSession) -> None:
    """Delete the MLflow run and UC registered model; no-op if missing. Ignores errors."""
    if not model_name or not registry_table:
        return
    registry = AnomalyModelRegistry(spark)
    record = registry.get_active_model(registry_table, model_name)
    if not record:
        return
    run_id = record.identity.mlflow_run_id
    if run_id:
        try:
            mlflow.delete_run(run_id)
        except Exception:
            pass
    try:
        MlflowClient().delete_registered_model(model_name)
    except Exception:
        pass


def _prepare_synthetic_data(spark) -> tuple[list[str], DataFrame, DataFrame]:
    """Blend overlapping and heavy-tail scenarios for less optimistic quality estimates."""
    overlap_cols, overlap_train, overlap_test = generate_overlapping_gaussian_data(
        spark,
        seed=SEED,
        n_samples=N_SAMPLES,
        n_features=N_FEATURES,
        anomaly_frac=ANOMALY_FRAC,
        anomaly_shift=2.0,
    )
    _heavy_cols, heavy_train, heavy_test = generate_heavy_tail_data(
        spark,
        seed=SEED + 7,
        n_samples=N_SAMPLES,
        n_features=N_FEATURES,
        anomaly_frac=ANOMALY_FRAC,
    )

    train_df = overlap_train.unionByName(heavy_train)
    test_df = overlap_test.unionByName(heavy_test)
    return overlap_cols, train_df, test_df


def _record_provenance(benchmark, n_train: int, n_test: int) -> None:
    """Attach what the numbers were measured on.

    A published table of quality metrics with no stated dataset invites being read as a general
    claim about DQX's detection quality, which it is not — this is blended synthetic data.
    """
    benchmark.extra_info["dataset"] = "synthetic: overlapping-gaussian + heavy-tail blend"
    benchmark.extra_info["seed"] = SEED
    benchmark.extra_info["n_features"] = N_FEATURES
    benchmark.extra_info["anomaly_frac"] = ANOMALY_FRAC
    benchmark.extra_info["n_train_rows"] = n_train
    benchmark.extra_info["n_test_rows"] = n_test


def _new_model_names(make_schema, make_random) -> tuple[str, str]:
    schema = make_schema(catalog_name=TEST_CATALOG).name
    suffix = make_random(6).lower()
    return (
        f"{TEST_CATALOG}.{schema}.bench_model_{suffix}",
        f"{TEST_CATALOG}.{schema}.bench_registry_{suffix}",
    )


@pytest.mark.benchmark(group=BENCHMARK_GROUP)
def test_benchmark_anomaly_train(benchmark, request, spark, ws, make_schema, make_random):
    """Time a full train-and-register cycle.

    One round: the cost is dominated by MLflow and Unity Catalog control-plane latency, so repeating
    it buys variance rather than precision, and each repeat would register another model version
    under the same name.
    """
    feature_cols, train_df, test_df = _prepare_synthetic_data(spark)
    model_name, registry_table = _new_model_names(make_schema, make_random)
    request.addfinalizer(lambda: _cleanup_anomaly_mlflow(model_name, registry_table, spark))

    engine = AnomalyEngine(workspace_client=ws, spark=spark)

    def run_train():
        engine.train(
            df=train_df,
            model_name=model_name,
            registry_table=registry_table,
            columns=feature_cols,
        )

    benchmark.pedantic(run_train, rounds=1, iterations=1, warmup_rounds=0)
    _record_provenance(benchmark, train_df.count(), test_df.count())


@pytest.mark.benchmark(group=BENCHMARK_GROUP)
def test_benchmark_anomaly_score(benchmark, request, spark, ws, make_schema, make_random):
    """Time scoring, and record the detection quality achieved on the same data.

    Trains its own model as unmeasured setup rather than sharing one with the train benchmark. The
    previous shared-module-global arrangement made this test order-dependent and able to retrain up
    to three times; one extra training run per nightly is a cheap price for removing that.
    """
    feature_cols, train_df, test_df = _prepare_synthetic_data(spark)
    model_name, registry_table = _new_model_names(make_schema, make_random)
    request.addfinalizer(lambda: _cleanup_anomaly_mlflow(model_name, registry_table, spark))

    engine = AnomalyEngine(workspace_client=ws, spark=spark)
    engine.train(
        df=train_df,
        model_name=model_name,
        registry_table=registry_table,
        columns=feature_cols,
    )

    # The third element is the name of the struct column the check writes. `_dq_info` is assembled a
    # layer up by DQEngine from that column, so reading `_dq_info` here resolves against nothing.
    _, apply_fn, info_col = has_no_row_anomalies(
        model_name=model_name,
        registry_table=registry_table,
        enable_contributions=False,
        enable_ai_explanation=False,
    )

    def run_score():
        scored_df = apply_fn(test_df)
        anomaly = F.col(info_col).getField("anomaly")
        return scored_df.select(
            F.col("is_anomaly").cast("double").alias("label"),
            anomaly.getField("score").alias("score"),
            anomaly.getField("is_anomaly").cast("double").alias("pred"),
        )

    scored = benchmark.pedantic(run_score, rounds=1, iterations=1, warmup_rounds=0)

    _record_provenance(benchmark, train_df.count(), test_df.count())
    for name, value in _detection_quality(scored).items():
        benchmark.extra_info[name] = value


def _detection_quality(scored: DataFrame) -> dict[str, float]:
    """Compute indicative detection quality from a scored frame.

    Deliberately unadjusted: no point-adjustment of any kind. Kim et al. (AAAI 2022) showed that
    point-adjusted F1 lets random scores beat published state of the art, so it is not a metric worth
    reporting even as an indicator.
    """
    metrics = cast(pd.DataFrame, scored.select("label", "score", "pred").toPandas())

    if metrics["label"].nunique() > 1:
        from sklearn.metrics import roc_auc_score  # type: ignore[import-untyped]

        roc_auc = float(roc_auc_score(metrics["label"], metrics["score"]))
    else:
        roc_auc = float("nan")

    true_pos = float(((metrics["label"] == 1.0) & (metrics["pred"] == 1.0)).sum())
    false_pos = float(((metrics["label"] == 0.0) & (metrics["pred"] == 1.0)).sum())
    false_neg = float(((metrics["label"] == 1.0) & (metrics["pred"] == 0.0)).sum())
    precision = true_pos / (true_pos + false_pos) if (true_pos + false_pos) > 0 else 0.0
    recall = true_pos / (true_pos + false_neg) if (true_pos + false_neg) > 0 else 0.0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    n_anomalies = int(metrics["label"].sum())
    if n_anomalies > 0:
        top_n = metrics.sort_values("score", ascending=False).head(n_anomalies)
        precision_at_n = float(top_n["label"].mean())
    else:
        precision_at_n = 0.0

    return {
        "roc_auc": roc_auc,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
        "precision_at_n": precision_at_n,
    }
