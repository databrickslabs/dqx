"""Reusable synthetic data generators for row anomaly detection tests."""

from dataclasses import dataclass

import numpy as np
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from databricks.labs.dqx.anomaly.segment_utils import build_baseline_key


def _to_train_test_frames(
    spark,
    features: np.ndarray,
    labels: np.ndarray,
    *,
    train_ratio: float = 0.6,
) -> tuple[list[str], DataFrame, DataFrame]:
    order = np.random.default_rng(42).permutation(len(features))
    features = features[order]
    labels = labels[order]

    n_train = int(train_ratio * len(features))
    features_train, features_test = features[:n_train], features[n_train:]
    labels_train, labels_test = labels[:n_train], labels[n_train:]

    feature_cols = [f"feature_{i}" for i in range(features.shape[1])]
    train_rows = np.hstack([features_train, labels_train.reshape(-1, 1)]).tolist()
    test_rows = np.hstack([features_test, labels_test.reshape(-1, 1)]).tolist()

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

    features = np.vstack([normal, anomalies])
    labels = np.concatenate([np.zeros(n_normals, dtype=float), np.ones(n_anomalies, dtype=float)])
    return _to_train_test_frames(spark, features, labels)


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

    features = np.vstack([normal, anomalies])
    labels = np.concatenate([np.zeros(n_normals, dtype=float), np.ones(n_anomalies, dtype=float)])
    return _to_train_test_frames(spark, features, labels)


def generate_segment_conditional_data(
    spark,
    *,
    seed: int = 42,
    n_per_segment: int = 1200,
) -> tuple[list[str], DataFrame, DataFrame]:
    """Generate data with segment-specific anomaly patterns."""
    rng = np.random.default_rng(seed)
    rows: list[tuple[float, float, str, float]] = []

    for region, mean_a, mean_b in (("US", 100.0, 20.0), ("EU", 80.0, 15.0), ("APAC", 120.0, 25.0)):
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


@dataclass
class _GroupWorld:
    """The per-group baseline levels a contextual incident is generated against.

    Holds what every simulated day needs, so the generator itself stays a short sequence of
    "build the world, emit control days, emit incident days" rather than carrying a dozen
    loop variables alongside its Spark plumbing.
    """

    rng: np.random.Generator
    groups: list[tuple[str, str, str]]
    levels: dict[tuple[str, str, str], float]
    incident_groups: list[tuple[str, str, str]]
    collapse_factor: float

    @classmethod
    def build(
        cls,
        *,
        seed: int,
        n_countries: int,
        n_event_types: int,
        n_products: int,
        collapse_factor: float,
        n_incident_groups: int,
    ) -> "_GroupWorld":
        rng = np.random.default_rng(seed)
        groups = [
            (f"C{c}", f"E{e}", f"P{p}")
            for c in range(n_countries)
            for e in range(n_event_types)
            for p in range(n_products)
        ]
        # Each group gets its own baseline level, spanning an order of magnitude. This
        # heterogeneity is the point: it is what makes one group's normal look like another
        # group's anomaly.
        levels = {group: float(rng.uniform(200.0, 4000.0)) for group in groups}
        # Spread the affected groups across the level range rather than taking a contiguous
        # slice, so the result does not depend on whether high- or low-volume groups happen to
        # be easier to flag.
        stride = max(1, len(groups) // max(1, n_incident_groups))
        incident_groups = [groups[i * stride] for i in range(n_incident_groups)]
        return cls(
            rng=rng,
            groups=groups,
            levels=levels,
            incident_groups=incident_groups,
            collapse_factor=collapse_factor,
        )

    def day_rows(self, day: int, *, collapse: bool) -> list[tuple]:
        """Emit one row per group for *day*, collapsing the incident groups when asked."""
        # Seasonality shared by every group, so it cannot be what distinguishes the incident.
        seasonal = 1.0 + 0.15 * np.sin(2 * np.pi * (day % 7) / 7.0)
        rows = []
        lost_volume = 0.0
        for group in self.groups:
            level = self.levels[group] * seasonal
            value = float(self.rng.normal(level, level * 0.05))
            label = 0.0
            if collapse and group in self.incident_groups:
                collapsed = value * self.collapse_factor
                lost_volume += value - collapsed
                value = collapsed
                label = 1.0
            rows.append((day, *group, value, label))
        if not lost_volume:
            return rows
        # Redistribute the lost volume so the daily total is unchanged. Without this the
        # incident would be detectable from the total alone and would prove nothing.
        share = lost_volume / (len(self.groups) - len(self.incident_groups))
        return [
            (d, c, e, p, v + share, lab) if (c, e, p) not in self.incident_groups else (d, c, e, p, v, lab)
            for d, c, e, p, v, lab in rows
        ]


def generate_group_conditional_data(
    spark,
    *,
    seed: int = 42,
    n_days: int = 120,
    n_countries: int = 6,
    n_event_types: int = 5,
    n_products: int = 3,
    collapse_factor: float = 0.2,
    n_incident_days: int = 1,
    n_incident_groups: int = 1,
    n_control_days: int = 1,
) -> tuple[list[str], DataFrame, DataFrame, str]:
    """Generate the *contextual* anomaly that motivates group conditioning.

    One group's daily volume collapses to ``collapse_factor`` of its usual level, while the
    **daily total across all groups is held flat** by redistributing the lost volume over the
    other groups. So the collapse is invisible in any aggregate: only a comparison against that
    group's own history reveals it.

    The collapsed value is deliberately *ordinary* in the global distribution — it sits inside the
    range other groups occupy normally — which is what makes a pooled model miss it. See
    databrickslabs/dqx#1484 for the measured behaviour this reproduces.

    Both frames carry an ``is_anomaly`` label column (all zero in training), so the same fixture
    serves a severity assertion and a PR-AUC measurement. Raise *n_incident_days* and
    *n_incident_groups* for the latter: average precision over a single positive row is a coin
    flip, not a measurement.

    Raise *n_control_days* for anything computed **per group**. With one control day each group has
    a handful of test rows, and a per-group rate over a handful of rows can only land on a few
    coarse values — so the maximum across many groups reaches 1.0 by chance and measures the
    fixture rather than the model.

    Returns ``(feature_columns, train_df, test_df, incident_group_key)`` where *incident_group_key*
    is the baseline key of the first affected group.
    """
    world = _GroupWorld.build(
        seed=seed,
        n_countries=n_countries,
        n_event_types=n_event_types,
        n_products=n_products,
        collapse_factor=collapse_factor,
        n_incident_groups=n_incident_groups,
    )
    incident_groups = world.incident_groups

    train_rows: list[tuple] = []
    for day in range(n_days):
        train_rows.extend(world.day_rows(day, collapse=False))

    test_rows: list[tuple] = []
    for offset in range(n_control_days):  # control days, no incident
        test_rows.extend(world.day_rows(n_days + offset, collapse=False))
    # Incident days follow every control day, so the two never land on the same day number and the
    # latest day in the frame is always an incident day (which is what the severity assertion in
    # test_anomaly_groups.py reads off).
    for offset in range(n_incident_days):
        test_rows.extend(world.day_rows(n_days + n_control_days + offset, collapse=True))

    schema = "day int, country string, event_type string, product string, event_count double, is_anomaly double"
    train_df = spark.createDataFrame(train_rows, schema)
    test_df = spark.createDataFrame(test_rows, schema)

    first = incident_groups[0]
    incident_key = build_baseline_key({"country": first[0], "event_type": first[1], "product": first[2]})
    return ["event_count"], train_df, test_df, incident_key


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
        result = result.withColumn(
            col_name, F.when(rand_col < missing_fraction, F.lit(None)).otherwise(F.col(col_name))
        )
    return result


def apply_distribution_shift(df: DataFrame, *, columns: list[str], shift_factor: float = 1.25) -> DataFrame:
    """Apply multiplicative distribution shift for drift scenarios."""
    shifted = df
    for col_name in columns:
        shifted = shifted.withColumn(col_name, F.col(col_name) * F.lit(shift_factor))
    return shifted


def test_missingness_spike_and_shift_generators(spark):
    """Missingness and shift helpers should deterministically modify data characteristics."""
    _feature_cols, _train_df, test_df = generate_overlapping_gaussian_data(
        spark,
        seed=321,
        n_samples=1200,
        n_features=4,
        anomaly_frac=0.02,
    )

    base_nulls = test_df.filter(F.col("feature_0").isNull()).count()
    spiked = inject_missingness_spike(test_df, columns=["feature_0"], missing_fraction=0.35, seed=11)
    spiked_nulls = spiked.filter(F.col("feature_0").isNull()).count()
    assert spiked_nulls > base_nulls

    shifted = apply_distribution_shift(test_df, columns=["feature_1"], shift_factor=1.5)
    base_mean = test_df.select(F.mean("feature_1")).first()[0]
    shifted_mean = shifted.select(F.mean("feature_1")).first()[0]
    assert shifted_mean > base_mean


def test_segment_conditional_generator_has_region_column(spark):
    """Segment-conditioned synthetic generator should preserve region segmentation column."""
    feature_cols, train_df, _test_df = generate_segment_conditional_data(spark, seed=77, n_per_segment=300)

    assert feature_cols == ["amount", "quantity"]
    assert "region" in train_df.columns
    regions = {row[0] for row in train_df.select("region").distinct().collect()}
    assert regions == {"US", "EU", "APAC"}
