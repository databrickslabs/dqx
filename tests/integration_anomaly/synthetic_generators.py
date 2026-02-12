"""Reusable synthetic data generators for anomaly detection tests."""

from __future__ import annotations

import numpy as np
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def _to_train_test_frames(
    spark,
    x: np.ndarray,
    y: np.ndarray,
    *,
    train_ratio: float = 0.6,
) -> tuple[list[str], DataFrame, DataFrame]:
    order = np.random.default_rng(42).permutation(len(x))
    x = x[order]
    y = y[order]

    n_train = int(train_ratio * len(x))
    x_train, x_test = x[:n_train], x[n_train:]
    y_train, y_test = y[:n_train], y[n_train:]

    feature_cols = [f"feature_{i}" for i in range(x.shape[1])]
    train_rows = np.hstack([x_train, y_train.reshape(-1, 1)]).tolist()
    test_rows = np.hstack([x_test, y_test.reshape(-1, 1)]).tolist()

    train_df = spark.createDataFrame(train_rows, feature_cols + ["is_anomaly"])
    test_df = spark.createDataFrame(test_rows, feature_cols + ["is_anomaly"])
    return feature_cols, train_df, test_df


def generate_overlapping_gaussian_data(
    spark,
    *,
    seed: int = 42,
    n_samples: int = 3000,
    n_features: int = 12,
    anomaly_frac: float = 0.03,
    anomaly_shift: float = 2.0,
) -> tuple[list[str], DataFrame, DataFrame]:
    """Generate moderately hard anomaly data with overlapping Gaussians."""
    rng = np.random.default_rng(seed)
    n_anomalies = max(1, int(n_samples * anomaly_frac))
    n_normals = n_samples - n_anomalies

    normal = rng.normal(loc=0.0, scale=1.0, size=(n_normals, n_features))
    anomalies = rng.normal(loc=anomaly_shift, scale=1.25, size=(n_anomalies, n_features))

    x = np.vstack([normal, anomalies])
    y = np.concatenate([np.zeros(n_normals, dtype=float), np.ones(n_anomalies, dtype=float)])
    return _to_train_test_frames(spark, x, y)


def generate_heavy_tail_data(
    spark,
    *,
    seed: int = 42,
    n_samples: int = 3000,
    n_features: int = 10,
    anomaly_frac: float = 0.03,
) -> tuple[list[str], DataFrame, DataFrame]:
    """Generate data with heavy-tailed normal behavior and extreme anomalies."""
    rng = np.random.default_rng(seed)
    n_anomalies = max(1, int(n_samples * anomaly_frac))
    n_normals = n_samples - n_anomalies

    normal = rng.standard_t(df=3, size=(n_normals, n_features))
    anomalies = rng.standard_t(df=2, size=(n_anomalies, n_features)) * 4.0

    x = np.vstack([normal, anomalies])
    y = np.concatenate([np.zeros(n_normals, dtype=float), np.ones(n_anomalies, dtype=float)])
    return _to_train_test_frames(spark, x, y)


def generate_segment_conditional_data(
    spark,
    *,
    seed: int = 42,
    n_per_segment: int = 1200,
) -> tuple[list[str], DataFrame, DataFrame]:
    """Generate data with segment-specific anomaly patterns."""
    rng = np.random.default_rng(seed)
    rows: list[tuple[float, float, str, float]] = []

    for region, mean_a, mean_b in [("US", 100.0, 20.0), ("EU", 80.0, 15.0), ("APAC", 120.0, 25.0)]:
        normal_count = int(n_per_segment * 0.96)
        anomaly_count = n_per_segment - normal_count

        normal_amount = rng.normal(mean_a, 8.0, normal_count)
        normal_qty = rng.normal(mean_b, 2.0, normal_count)
        for amount, qty in zip(normal_amount, normal_qty, strict=True):
            rows.append((float(amount), float(qty), region, 0.0))

        # Region-specific anomaly style
        anomaly_amount = rng.normal(mean_a * 1.8, 12.0, anomaly_count)
        anomaly_qty = rng.normal(mean_b * 0.4, 3.0, anomaly_count)
        for amount, qty in zip(anomaly_amount, anomaly_qty, strict=True):
            rows.append((float(amount), float(qty), region, 1.0))

    df = spark.createDataFrame(rows, "amount double, quantity double, region string, is_anomaly double")
    train_df, test_df = df.randomSplit([0.6, 0.4], seed=seed)
    return ["amount", "quantity"], train_df, test_df


def inject_missingness_spike(
    df: DataFrame,
    *,
    columns: list[str],
    missing_fraction: float = 0.2,
    seed: int = 42,
) -> DataFrame:
    """Inject deterministic null spikes into selected columns."""
    result = df
    for idx, col_name in enumerate(columns):
        rand_col = F.rand(seed + idx)
        result = result.withColumn(col_name, F.when(rand_col < missing_fraction, F.lit(None)).otherwise(F.col(col_name)))
    return result


def apply_distribution_shift(df: DataFrame, *, columns: list[str], shift_factor: float = 1.25) -> DataFrame:
    """Apply multiplicative distribution shift for drift scenarios."""
    shifted = df
    for col_name in columns:
        shifted = shifted.withColumn(col_name, F.col(col_name) * F.lit(shift_factor))
    return shifted
