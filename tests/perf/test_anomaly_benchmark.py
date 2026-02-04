from pathlib import Path

import numpy as np
import pytest
from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from tests.constants import TEST_CATALOG

_TRAINED_MODEL: dict[str, str] = {}


def _load_arrhythmia_dataset(dataset_path: Path) -> tuple[np.ndarray, np.ndarray]:
    try:
        from scipy.io import loadmat  # type: ignore[import-untyped]
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("scipy is required to load .mat benchmark dataset") from exc

    mat = loadmat(dataset_path)
    if "X" not in mat or "y" not in mat:
        raise ValueError("Expected keys 'X' and 'y' in arrhythmia .mat file")
    X = mat["X"]
    y = mat["y"].ravel()
    return X, y


def _arrhythmia_path() -> Path | None:
    repo_root = Path(__file__).resolve().parents[2]
    candidate = repo_root / "tests" / "resources" / "arrhythmia.mat"
    return candidate if candidate.exists() else None


def _prepare_arrhythmia_data(spark, dataset_path: Path):
    X, y = _load_arrhythmia_dataset(dataset_path)
    n_train = int(0.6 * len(X))
    X_train, X_test = X[:n_train], X[n_train:]
    y_train, y_test = y[:n_train], y[n_train:]

    feature_cols = [f"feature_{i}" for i in range(X.shape[1])]
    train_pdf = np.hstack([X_train, y_train.reshape(-1, 1)])
    test_pdf = np.hstack([X_test, y_test.reshape(-1, 1)])
    train_df = spark.createDataFrame(train_pdf.tolist(), feature_cols + ["is_anomaly"])
    test_df = spark.createDataFrame(test_pdf.tolist(), feature_cols + ["is_anomaly"])
    return feature_cols, train_df, test_df


@pytest.mark.benchmark(group="anomaly_arrhythmia")
def test_benchmark_anomaly_arrhythmia_train(benchmark, spark, ws, make_schema, make_random):
    dataset_path = _arrhythmia_path()
    if dataset_path is None:
        pytest.skip("arrhythmia.mat not available in tests/resources")

    try:
        feature_cols, train_df, _ = _prepare_arrhythmia_data(spark, dataset_path)
    except RuntimeError:
        pytest.skip("scipy not installed (required to load arrhythmia.mat)")

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

    benchmark.pedantic(run_train, rounds=1, iterations=1)
    _TRAINED_MODEL["model_name"] = model_name
    _TRAINED_MODEL["registry_table"] = registry_table


@pytest.mark.benchmark(group="anomaly_arrhythmia")
def test_benchmark_anomaly_arrhythmia_score(benchmark, spark, ws, make_schema, make_random):
    dataset_path = _arrhythmia_path()
    if dataset_path is None:
        pytest.skip("arrhythmia.mat not available in tests/resources")

    try:
        feature_cols, train_df, test_df = _prepare_arrhythmia_data(spark, dataset_path)
    except RuntimeError:
        pytest.skip("scipy not installed (required to load arrhythmia.mat)")

    engine = AnomalyEngine(workspace_client=ws, spark=spark)
    model_name = _TRAINED_MODEL.get("model_name")
    registry_table = _TRAINED_MODEL.get("registry_table")
    if not model_name or not registry_table:
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

    scored = benchmark.pedantic(run_score, rounds=1, iterations=1)
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
