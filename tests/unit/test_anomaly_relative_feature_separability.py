"""Does the group-relative feature actually make a contextual anomaly separable?

This guards the **mechanism**, not the pipeline. It reimplements the transform in numpy and fits
scikit-learn directly, so it runs offline in the unit suite — no Spark session, no workspace, no
MLflow. The pipeline that carries the same transform through Spark is covered by
``tests/integration_anomaly/``.

Two scenarios, because a feature that helps one case and silently hurts the other is not an
improvement:

* **contextual** — a group's metric collapses to a value that is entirely ordinary *globally*. This
  is the case from databrickslabs/dqx#1484 that a pooled model scored at the 45th percentile. The
  relative feature must make it separable.
* **global magnitude** — the anomaly is extreme against every group. A pooled model already sees
  this, so the relative feature must not make it materially worse. Adding features to an Isolation
  Forest is not free: it picks split dimensions at random, so extra columns dilute the informative
  ones.
"""

import numpy as np
import pytest
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score

SEED = 20260825
N_GROUPS = 12
N_PER_GROUP = 160
NOISE_FEATURES = 2

# Detection floors. Deliberately loose: the point is the *direction and size* of the gap, not a
# precise number that would make this test a tripwire for scikit-learn's RNG.
MIN_CONTEXTUAL_GAIN = 0.15
MAX_GLOBAL_REGRESSION = 0.05


def _signed_log1p(values: np.ndarray) -> np.ndarray:
    """``signum(x) * log1p(|x|)`` — mirrors ``anomaly/transformers.py::_signed_log1p``.

    Defined over all reals, unlike plain ``log1p``, which is NaN for ``x <= -1``.
    """
    return np.sign(values) * np.log1p(np.abs(values))


def _relative_to_group_median(metric: np.ndarray, groups: np.ndarray) -> np.ndarray:
    """Deviation of each value from its own group's median, in signed-log space.

    The training-time half of the transform. Scoring uses persisted medians instead, but the
    arithmetic being checked here is identical.
    """
    relative = np.empty_like(metric, dtype=float)
    for group in np.unique(groups):
        mask = groups == group
        median = float(np.median(metric[mask]))
        relative[mask] = _signed_log1p(metric[mask]) - _signed_log1p(np.full(mask.sum(), median))
    return relative


def _make_grouped_data(*, contextual: bool, seed: int = SEED) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build a grouped metric with a planted anomaly.

    Group levels span an order of magnitude, which is what makes one group's normal look like
    another group's anomaly — the heterogeneity the relative feature exists to remove.

    Returns ``(metric, groups, labels)``.
    """
    rng = np.random.default_rng(seed)
    levels = np.geomspace(200.0, 4000.0, N_GROUPS)

    metric_parts, group_parts, label_parts = [], [], []
    for index, level in enumerate(levels):
        values = rng.normal(level, level * 0.05, N_PER_GROUP)
        labels = np.zeros(N_PER_GROUP)

        # Plant the anomaly in a mid-level group, so a contextual collapse lands squarely inside
        # the range other groups occupy normally.
        if index == N_GROUPS // 2:
            if contextual:
                # 80% collapse. Ordinary globally: 0.2 * mid-level sits among the low groups.
                values[:5] = level * 0.2
            else:
                # Extreme against every group, so pooling can already see it.
                values[:5] = levels[-1] * 4.0
            labels[:5] = 1.0

        metric_parts.append(values)
        group_parts.append(np.full(N_PER_GROUP, index))
        label_parts.append(labels)

    return np.concatenate(metric_parts), np.concatenate(group_parts), np.concatenate(label_parts)


def _average_precision(features: np.ndarray, labels: np.ndarray) -> float:
    """PR-AUC of an Isolation Forest on *features*.

    Scores are negated ``score_samples`` so that higher means more anomalous, matching
    ``anomaly/core.py``. PR-AUC rather than ROC-AUC because the positive class is ~0.3%.
    """
    forest = IsolationForest(n_estimators=200, random_state=42, n_jobs=1)
    forest.fit(features)
    scores = -forest.score_samples(features)
    return float(average_precision_score(labels, scores))


def _pooled_and_relative(*, contextual: bool) -> tuple[float, float]:
    """PR-AUC without and with the relative feature, on identical data and noise."""
    metric, groups, labels = _make_grouped_data(contextual=contextual)
    rng = np.random.default_rng(SEED + 1)
    noise = rng.normal(0.0, 1.0, (metric.size, NOISE_FEATURES))

    pooled = np.column_stack([metric, noise])
    relative = np.column_stack([metric, noise, _relative_to_group_median(metric, groups)])

    return _average_precision(pooled, labels), _average_precision(relative, labels)


def test_relative_feature_makes_a_contextual_anomaly_separable():
    """The claim the feature exists for: conditioning finds what pooling cannot."""
    pooled, relative = _pooled_and_relative(contextual=True)

    assert relative > pooled + MIN_CONTEXTUAL_GAIN, (
        f"relative feature should separate a contextual anomaly that pooling misses "
        f"(pooled PR-AUC {pooled:.3f}, relative {relative:.3f})"
    )


def test_pooling_alone_misses_the_contextual_anomaly():
    """Pins the negative half.

    If pooling ever starts catching this, the fixture has stopped isolating a purely contextual
    anomaly and the positive result above no longer means what it claims.
    """
    pooled, _ = _pooled_and_relative(contextual=True)

    assert pooled < 0.5, f"fixture no longer isolates a contextual anomaly: pooled PR-AUC {pooled:.3f}"


def test_relative_feature_does_not_hurt_a_global_anomaly():
    """The cost side. An extra Isolation Forest dimension dilutes split selection, so a feature
    that is useless for a given anomaly must still not meaningfully degrade detection."""
    pooled, relative = _pooled_and_relative(contextual=False)

    assert relative > pooled - MAX_GLOBAL_REGRESSION, (
        f"relative feature degraded a globally-extreme anomaly too far "
        f"(pooled PR-AUC {pooled:.3f}, relative {relative:.3f})"
    )


# ============================================================================
# The transform itself
# ============================================================================


def test_signed_log1p_matches_log1p_for_non_negative_input():
    values = np.array([0.0, 0.5, 1.0, 100.0, 3284.0])
    np.testing.assert_allclose(_signed_log1p(values), np.log1p(values))


@pytest.mark.parametrize("value", [-0.5, -1.0, -2.0, -500.0, -1e6])
def test_signed_log1p_is_finite_where_plain_log1p_is_not(value):
    """``log1p(x)`` is NaN for ``x <= -1``; the signed form must survive signed metrics."""
    result = _signed_log1p(np.array([value]))

    assert np.isfinite(result).all(), f"signed_log1p({value}) was not finite"
    assert result[0] < 0.0, "a negative input must map to a negative output"


def test_signed_log1p_is_monotone_over_the_whole_real_line():
    """Monotonicity is what lets the transform preserve Isolation Forest's axis-aligned splits."""
    values = np.linspace(-1000.0, 1000.0, 2001)

    assert np.all(np.diff(_signed_log1p(values)) > 0)


def test_relative_deviation_is_zero_at_the_group_median():
    """Two groups an order of magnitude apart both centre on zero — the entire point."""
    metric = np.array([100.0, 100.0, 100.0, 10.0, 10.0, 10.0])
    groups = np.array([0, 0, 0, 1, 1, 1])

    np.testing.assert_allclose(_relative_to_group_median(metric, groups), np.zeros(6), atol=1e-12)
