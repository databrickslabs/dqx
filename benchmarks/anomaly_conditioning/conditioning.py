"""The three ways to condition an anomaly model on a group, reimplemented offline.

Mirrors ``databricks.labs.dqx.anomaly.transformers`` closely enough to measure the *mechanism*,
while running in numpy and sklearn so a sweep of a few thousand fits needs no Spark session, no
Databricks workspace, and no MLflow registry. Pipeline equivalence is a separate question and is
asserted in ``tests/integration_anomaly/test_anomaly_quality.py``; this harness is about which
mechanism detects better, not about whether DQX implements it faithfully.

The three configurations:

``pooled``
    One model over the raw metrics. No notion of a group at all. This is DQX before #1484.

Note the estimator is configured to match the shipped defaults (see :data:`N_TREES`), so the absolute
figures are comparable with the product rather than with a lighter stand-in.

``relative``
    One model over the raw metrics *plus* each metric's deviation from its own group's baseline.
    This is what ``baseline_by`` does.

``per_group``
    One model per group, each trained only on that group's rows. This is what the legacy
    ``segment_by`` does, and what auto-discovery used to select.
"""

import time
from dataclasses import dataclass

import numpy as np
from sklearn.ensemble import IsolationForest


def signed_log1p(values: np.ndarray) -> np.ndarray:
    """``signum(x) * log1p(|x|)``, matching ``transformers._signed_log1p``.

    Defined for negative input, unlike a bare log, and symmetric about zero so halving and
    doubling move the same distance in opposite directions.
    """
    return np.sign(values) * np.log1p(np.abs(values))


def relative_to_group_baseline(values: np.ndarray, groups: np.ndarray) -> np.ndarray:
    """Each column's deviation from its own group's median, in signed-log space.

    The medians come from the data being transformed, which is correct here because every
    configuration is fitted and scored on the same split; DQX instead persists the training
    medians and reuses them at scoring, falling back to a global median for unseen groups.
    """
    out = np.zeros_like(values, dtype=float)
    for group in np.unique(groups):
        mask = groups == group
        medians = np.median(values[mask], axis=0)
        out[mask] = signed_log1p(values[mask]) - signed_log1p(medians)
    return out


def eta_squared(values: np.ndarray, groups: np.ndarray) -> float:
    """Share of total variance that lies *between* groups: ``SS_between / SS_total``.

    Removed from DQX itself — it gated whether conditioning was applied at all, and cost a full
    Spark aggregation to compute a number whose predictive value had never been measured. It lives
    here because measuring that value is precisely this harness's job. Averaged over columns so a
    multi-metric dataset gets one number.
    """
    per_column = []
    for column in range(values.shape[1]):
        series = values[:, column]
        grand_mean = series.mean()
        ss_total = float(((series - grand_mean) ** 2).sum())
        if ss_total == 0:
            continue
        ss_between = 0.0
        for group in np.unique(groups):
            member = series[groups == group]
            ss_between += len(member) * (member.mean() - grand_mean) ** 2
        per_column.append(float(ss_between) / ss_total)
    return float(np.mean(per_column)) if per_column else 0.0


@dataclass
class FitResult:
    """Scores for every row, plus what it cost to produce them."""

    scores: np.ndarray
    n_models: int
    seconds: float


# Mirrors what DQX ships: IsolationForestConfig(num_trees=200), and contamination taken from
# expected_anomaly_rate (default 0.02) rather than sklearn's "auto". An earlier version of this
# harness used 100 trees and "auto", which understated the product -- contamination only moves the
# predict/offset_ threshold and cannot change score_samples ranking, so PR-AUC was unaffected by that
# half, but tree count is not neutral.
N_TREES = 200
CONTAMINATION = 0.02


def _forest(seed: int) -> IsolationForest:
    """One estimator configuration for every cell, so comparisons are not confounded by tuning."""
    return IsolationForest(n_estimators=N_TREES, contamination=CONTAMINATION, random_state=seed, n_jobs=-1)


def fit_pooled(values: np.ndarray, groups: np.ndarray, seed: int) -> FitResult:
    """One model over raw metrics. *groups* is accepted and ignored, to keep one call signature."""
    del groups
    started = time.perf_counter()
    model = _forest(seed).fit(values)
    scores = -model.score_samples(values)
    return FitResult(scores=scores, n_models=1, seconds=time.perf_counter() - started)


def fit_relative(values: np.ndarray, groups: np.ndarray, seed: int) -> FitResult:
    """One model over raw metrics plus baseline-relative ones."""
    started = time.perf_counter()
    features = np.hstack([values, relative_to_group_baseline(values, groups)])
    model = _forest(seed).fit(features)
    scores = -model.score_samples(features)
    return FitResult(scores=scores, n_models=1, seconds=time.perf_counter() - started)


def fit_per_group(values: np.ndarray, groups: np.ndarray, seed: int) -> FitResult:
    """One model per group.

    A group too small to fit falls back to a score of zero rather than being dropped, so every
    configuration returns a score for every row and the metrics stay comparable. Note what this
    costs even when it works: each model calibrates its own contamination on its own group, so a
    group whose rows are all normal still has its most-unusual few percent scored as extreme. That
    is the mechanism behind the 56% false-alarm entity in the SMD result.
    """
    started = time.perf_counter()
    scores = np.zeros(len(values), dtype=float)
    n_models = 0
    for group in np.unique(groups):
        mask = groups == group
        if mask.sum() < 10:
            continue
        model = _forest(seed).fit(values[mask])
        scores[mask] = -model.score_samples(values[mask])
        n_models += 1
    return FitResult(scores=scores, n_models=n_models, seconds=time.perf_counter() - started)


CONFIGS = {
    "pooled": fit_pooled,
    "relative": fit_relative,
    "per_group": fit_per_group,
}
