"""Detection-quality metrics and the paired statistics used to compare configurations.

Deliberately **no point-adjustment**, for the reason given at length in
``tests/integration_anomaly/quality_metrics.py``: under the point-adjust protocol a random score
reaches state-of-the-art F1 (Kim et al., AAAI 2022), so any number produced that way is
uninterpretable. This module and that one are kept separate rather than shared because the test
tree is not importable from a top-level script directory, and because the paired statistics below
have no place in a test.
"""

from dataclasses import asdict, dataclass, field

import numpy as np
from scipy import stats
from sklearn.metrics import average_precision_score, roc_auc_score


@dataclass
class CellMetrics:
    """Every number recorded for one (dataset, grouping, config, seed) cell."""

    pr_auc: float
    roc_auc: float
    precision_at_n: float
    macro_pr_auc: float
    worst_group_fpr: float
    n_models: int
    seconds: float
    eta_squared: float = 0.0
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)


def pr_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """Average precision. NaN when a split holds only one class."""
    if len(np.unique(labels)) < 2:
        return float("nan")
    return float(average_precision_score(labels, scores))


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """Reported alongside PR-AUC, never instead of it: these datasets are heavily imbalanced."""
    if len(np.unique(labels)) < 2:
        return float("nan")
    return float(roc_auc_score(labels, scores))


def precision_at_n(labels: np.ndarray, scores: np.ndarray) -> float:
    """Precision over the top-*n* scored rows, where *n* is the true number of anomalies.

    The operational metric: an analyst reviews a fixed-size queue, so what matters is how much of
    that queue is worth reviewing.
    """
    n_anomalies = int(labels.sum())
    if n_anomalies == 0:
        return float("nan")
    top = np.argsort(scores)[::-1][:n_anomalies]
    return float(labels[top].sum() / n_anomalies)


def per_group_pr_auc(labels: np.ndarray, scores: np.ndarray, groups: np.ndarray) -> dict[str, float]:
    """PR-AUC within each group, skipping groups that hold only one class."""
    out = {}
    for group in np.unique(groups):
        mask = groups == group
        if len(np.unique(labels[mask])) < 2:
            continue
        out[str(group)] = pr_auc(labels[mask], scores[mask])
    return out


def worst_group_false_positive_rate(
    labels: np.ndarray, scores: np.ndarray, groups: np.ndarray, *, quantile: float = 0.95
) -> float:
    """Highest per-group false-positive rate at a global score threshold.

    The threshold is the *global* score quantile, which is the point: a per-group model's scores
    are calibrated within its own group, so a group of entirely normal rows still emits a full
    complement of extreme scores. Averaging over groups hides that; taking the worst group does
    not, and it is the statistic that exposed the SMD failure.
    """
    if len(np.unique(labels)) < 2:
        return float("nan")
    threshold = float(np.quantile(scores, quantile))
    rates = []
    for group in np.unique(groups):
        mask = (groups == group) & (labels == 0)
        if mask.sum() == 0:
            continue
        rates.append(float((scores[mask] > threshold).mean()))
    return max(rates) if rates else float("nan")


def macro_average(values: dict[str, float]) -> float:
    """Unweighted mean across groups.

    Unweighted on purpose: weighting by group size reproduces the aggregate metric and re-hides
    the small-group failures this exists to surface.
    """
    finite = [v for v in values.values() if np.isfinite(v)]
    return float(np.mean(finite)) if finite else float("nan")


def wilcoxon_paired(deltas: list[float]) -> tuple[float, float]:
    """Wilcoxon signed-rank on per-seed differences: returns ``(statistic, p_value)``.

    Paired by seed rather than comparing unpaired means, because the seed controls both the data
    draw and the forest, and is by far the largest source of variance between cells.
    """
    finite = [d for d in deltas if np.isfinite(d)]
    if len(finite) < 2 or all(d == 0 for d in finite):
        return float("nan"), float("nan")
    result = stats.wilcoxon(finite)
    return float(result.statistic), float(result.pvalue)


def spearman_with_bootstrap_ci(
    xs: list[float], ys: list[float], *, n_boot: int = 2000, seed: int = 0
) -> tuple[float, float, float]:
    """Spearman ρ plus a percentile bootstrap 95% CI: ``(rho, lo, hi)``.

    A point correlation over a few dozen configurations is not evidence on its own; the interval is
    what says whether the sign is even determined.
    """
    x_arr, y_arr = np.asarray(xs, dtype=float), np.asarray(ys, dtype=float)
    keep = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_arr, y_arr = x_arr[keep], y_arr[keep]
    if len(x_arr) < 4:
        return float("nan"), float("nan"), float("nan")

    rho = float(stats.spearmanr(x_arr, y_arr).statistic)
    rng = np.random.default_rng(seed)
    boots = []
    for _ in range(n_boot):
        idx = rng.integers(0, len(x_arr), len(x_arr))
        if len(np.unique(x_arr[idx])) < 3:
            continue
        boots.append(stats.spearmanr(x_arr[idx], y_arr[idx]).statistic)
    if not boots:
        return rho, float("nan"), float("nan")
    return rho, float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))
