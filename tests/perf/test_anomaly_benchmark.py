import numpy as np
import pytest
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.errors import InvalidParameterError
from tests.constants import TEST_CATALOG

_TRAINED_MODEL: dict[str, str] = {}


def _prepare_synthetic_data(
    spark,
    *,
    seed: int = 42,
    n_samples: int = 2000,
    n_features: int = 20,
    anomaly_frac: float = 0.02,
):
    rng = np.random.default_rng(seed)
    n_anomalies = max(1, int(n_samples * anomaly_frac))
    n_normals = n_samples - n_anomalies

    normal = rng.normal(loc=0.0, scale=1.0, size=(n_normals, n_features))
    anomalies = rng.normal(loc=6.0, scale=1.5, size=(n_anomalies, n_features))
    X = np.vstack([normal, anomalies])
    y = np.concatenate([np.zeros(n_normals, dtype=float), np.ones(n_anomalies, dtype=float)])

    order = rng.permutation(len(X))
    X = X[order]
    y = y[order]

    n_train = int(0.6 * len(X))
    X_train, X_test = X[:n_train], X[n_train:]
    y_train, y_test = y[:n_train], y[n_train:]

    feature_cols = [f"feature_{i}" for i in range(X.shape[1])]
    train_pdf = np.hstack([X_train, y_train.reshape(-1, 1)])
    test_pdf = np.hstack([X_test, y_test.reshape(-1, 1)])
    train_df = spark.createDataFrame(train_pdf.tolist(), feature_cols + ["is_anomaly"])
    test_df = spark.createDataFrame(test_pdf.tolist(), feature_cols + ["is_anomaly"])
    return feature_cols, train_df, test_df


@pytest.mark.benchmark(group="anomaly_synthetic")
def test_benchmark_anomaly_arrhythmia_train(benchmark, spark, ws, make_schema, make_random):
    feature_cols, train_df, _ = _prepare_synthetic_data(spark)

    schema = make_schema(catalog_name=TEST_CATALOG).name
    model_name = f"{TEST_CATALOG}.{schema}.bench_model_{make_random(6).lower()}"
    registry_table = f"{TEST_CATALOG}.{schema}.bench_registry_{make_random(6).lower()}"

    engine = AnomalyEngine(workspace_client=ws, spark=spark)

    def run_train():
        engine.train(
            df=train_df,
            model_name=model_name,
            registry_table=registry_table,
            columns=feature_cols,
        )

    benchmark(run_train)
    _TRAINED_MODEL["model_name"] = model_name
    _TRAINED_MODEL["registry_table"] = registry_table


@pytest.mark.benchmark(group="anomaly_synthetic")
def test_benchmark_anomaly_arrhythmia_score(benchmark, spark, ws, make_schema, make_random):
    feature_cols, train_df, test_df = _prepare_synthetic_data(spark)

    engine = AnomalyEngine(workspace_client=ws, spark=spark)
    model_name = _TRAINED_MODEL.get("model_name")
    registry_table = _TRAINED_MODEL.get("registry_table")
    needs_training = not model_name or not registry_table
    if not needs_training:
        registry_client = AnomalyModelRegistry(spark)
        record = registry_client.get_active_model(registry_table, model_name)
        needs_training = record is None

    if needs_training:
        schema = make_schema(catalog_name=TEST_CATALOG).name
        model_name = f"{TEST_CATALOG}.{schema}.bench_model_{make_random(6).lower()}"
        registry_table = f"{TEST_CATALOG}.{schema}.bench_registry_{make_random(6).lower()}"
        engine.train(
            df=train_df,
            model_name=model_name,
            registry_table=registry_table,
            columns=feature_cols,
        )
        _TRAINED_MODEL["model_name"] = model_name
        _TRAINED_MODEL["registry_table"] = registry_table

    try:
        _, apply_fn = has_no_anomalies(
            model=model_name,
            registry_table=registry_table,
            include_contributions=False,
        )
    except InvalidParameterError:
        schema = make_schema(catalog_name=TEST_CATALOG).name
        model_name = f"{TEST_CATALOG}.{schema}.bench_model_{make_random(6).lower()}"
        registry_table = f"{TEST_CATALOG}.{schema}.bench_registry_{make_random(6).lower()}"
        engine.train(
            df=train_df,
            model_name=model_name,
            registry_table=registry_table,
            columns=feature_cols,
        )
        _TRAINED_MODEL["model_name"] = model_name
        _TRAINED_MODEL["registry_table"] = registry_table
        _, apply_fn = has_no_anomalies(
            model=model_name,
            registry_table=registry_table,
            include_contributions=False,
        )

    def run_score():
        scored_df = apply_fn(test_df)
        return scored_df.select(
            F.col("is_anomaly").cast("double").alias("label"),
            F.col("_dq_info.anomaly.score").alias("score"),
            F.col("_dq_info.anomaly.is_anomaly").cast("double").alias("pred"),
        )

    scored = benchmark(run_score)
    metrics = scored.select("label", "score", "pred").toPandas()
    if metrics["label"].nunique() > 1:
        from sklearn.metrics import roc_auc_score  # type: ignore[import-untyped]

        roc_auc = roc_auc_score(metrics["label"], metrics["score"])
    else:
        roc_auc = float("nan")
    tp = float(((metrics["label"] == 1.0) & (metrics["pred"] == 1.0)).sum())
    fp = float(((metrics["label"] == 0.0) & (metrics["pred"] == 1.0)).sum())
    fn = float(((metrics["label"] == 1.0) & (metrics["pred"] == 0.0)).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    n_anomalies = int(metrics["label"].sum())
    if n_anomalies > 0:
        top_n = metrics.sort_values("score", ascending=False).head(n_anomalies)
        precision_at_n = float(top_n["label"].mean())
    else:
        precision_at_n = 0.0
    benchmark.extra_info["roc_auc"] = roc_auc
    benchmark.extra_info["precision"] = precision
    benchmark.extra_info["recall"] = recall
    benchmark.extra_info["f1_score"] = f1
    benchmark.extra_info["precision_at_n"] = precision_at_n
