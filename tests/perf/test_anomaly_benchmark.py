from pyspark.sql import functions as F

from databricks.labs.dqx.anomaly import AnomalyEngine, has_no_anomalies
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.errors import InvalidParameterError
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.synthetic_generators import (
    generate_heavy_tail_data,
    generate_overlapping_gaussian_data,
)

_TRAINED_MODEL: dict[str, str] = {}


def _prepare_synthetic_data(
    spark,
    *,
    seed: int = 42,
    n_samples: int = 3000,
    n_features: int = 16,
    anomaly_frac: float = 0.03,
):
    # Blend overlapping and heavy-tail scenarios for less optimistic quality estimates.
    overlap_cols, overlap_train, overlap_test = generate_overlapping_gaussian_data(
        spark,
        seed=seed,
        n_samples=n_samples,
        n_features=n_features,
        anomaly_frac=anomaly_frac,
        anomaly_shift=2.0,
    )
    _heavy_cols, heavy_train, heavy_test = generate_heavy_tail_data(
        spark,
        seed=seed + 7,
        n_samples=n_samples,
        n_features=n_features,
        anomaly_frac=anomaly_frac,
    )

    train_df = overlap_train.unionByName(heavy_train)
    test_df = overlap_test.unionByName(heavy_test)
    return overlap_cols, train_df, test_df


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
