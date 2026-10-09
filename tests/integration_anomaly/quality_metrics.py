"""Detection-quality metrics for anomaly integration tests.

Deliberately **no point-adjustment of any kind.** Kim et al., "Towards a Rigorous Evaluation of
Time-series Anomaly Detection" (AAAI 2022) showed that under the point-adjust protocol — crediting an
entire labelled anomaly segment when any single point inside it is detected — a *random* anomaly score
achieves state-of-the-art F1. Any number produced that way is uninterpretable, so none is computed
here. If you are tempted to add one, read that paper first.

What is here instead:

* **PR-AUC** as the primary metric (``average_precision_score``), because these datasets are heavily
  imbalanced and ROC-AUC flatters a detector that ranks the majority class well.
* **Per-group and macro-averaged** metrics, not just aggregate. An aggregate average is dominated by
  the largest groups, which is exactly how a catastrophic failure in one group stays invisible — the
  SMD result that motivated this work had one entity at a 56% false-alarm rate while the aggregate
  looked merely mediocre.
* **Worst-group false-positive rate**, which is the statistic that exposed that failure.
* **Trivial baselines**, so a result can be compared against doing almost nothing. Kim et al.'s other
  recommendation: report improvement over a baseline, not an absolute number.
"""

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


def pr_auc(labels: pd.Series, scores: pd.Series) -> float:
    """Area under the precision-recall curve. NaN when only one class is present."""
    if labels.nunique() < 2:
        return float("nan")
    return float(average_precision_score(labels, scores))


def roc_auc(labels: pd.Series, scores: pd.Series) -> float:
    """Area under the ROC curve. Reported alongside PR-AUC, never instead of it."""
    if labels.nunique() < 2:
        return float("nan")
    return float(roc_auc_score(labels, scores))


def false_positive_rate(labels: pd.Series, flagged: pd.Series) -> float:
    """Share of genuinely normal rows that were flagged."""
    normal = labels == 0
    if not normal.any():
        return float("nan")
    return float(flagged[normal].mean())


def per_group_metrics(
    frame: pd.DataFrame,
    *,
    group_col: str,
    label_col: str = "label",
    score_col: str = "score",
    flag_col: str = "flagged",
) -> pd.DataFrame:
    """One row of metrics per group.

    Groups with a single class present yield NaN PR-AUC rather than a misleading 0 or 1; callers
    should drop those before averaging rather than have this function guess.
    """
    rows = []
    for group, chunk in frame.groupby(group_col):
        rows.append(
            {
                group_col: group,
                "n": len(chunk),
                "n_normal": int((chunk[label_col] == 0).sum()),
                "n_anomalies": int(chunk[label_col].sum()),
                "pr_auc": pr_auc(chunk[label_col], chunk[score_col]),
                "false_positive_rate": false_positive_rate(chunk[label_col], chunk[flag_col]),
            }
        )
    return pd.DataFrame(rows)


def macro_average(group_metrics: pd.DataFrame, column: str) -> float:
    """Unweighted mean across groups, ignoring groups where the metric is undefined.

    Unweighted on purpose: weighting by group size reproduces the aggregate metric and re-hides the
    small-group failures this exists to surface.
    """
    values = group_metrics[column].dropna()
    if values.empty:
        return float("nan")
    return float(values.mean())


def worst_group_false_positive_rate(group_metrics: pd.DataFrame, *, min_normal_rows: int = 10) -> float:
    """Highest per-group false-positive rate, over groups large enough to have one.

    The single most useful number for judging per-group models: their failure mode is one group's
    threshold collapsing while the rest look fine.

    *min_normal_rows* is not a convenience. A rate over four normal rows can only take the values
    0, 0.25, 0.5, 0.75 or 1 — so on a small-group fixture the maximum across many groups is
    approximately guaranteed to hit 1.0 by chance, and an assertion on it measures the fixture
    rather than the model. The failure this statistic exists to catch was 56% over 28,392 rows;
    groups that cannot express such a rate are excluded rather than allowed to fabricate one.

    Returns NaN when no group is large enough, which callers must treat as "not measured" rather
    than as a pass.
    """
    eligible = group_metrics
    if "n_normal" in group_metrics.columns:
        eligible = group_metrics[group_metrics["n_normal"] >= min_normal_rows]
    values = eligible["false_positive_rate"].dropna()
    if values.empty:
        return float("nan")
    return float(values.max())


def trivial_baselines(frame: pd.DataFrame, metric_cols: list[str], *, seed: int = 42) -> dict[str, float]:
    """PR-AUC of scores that required no model, as a floor to beat.

    ``random`` is the sanity check Kim et al. recommend. ``max_abs_z`` is the cheapest defensible
    detector — the largest absolute z-score across metrics — and is a genuinely competitive baseline
    on data whose anomalies are simply extreme, which makes it the honest thing to compare against.
    """
    rng = np.random.default_rng(seed)
    labels = frame["label"]

    values = frame[metric_cols].to_numpy(dtype=float)
    means = np.nanmean(values, axis=0)
    stds = np.nanstd(values, axis=0)
    stds[stds == 0] = 1.0
    max_abs_z = np.nanmax(np.abs((values - means) / stds), axis=1)

    return {
        "random": pr_auc(labels, pd.Series(rng.random(len(frame)), index=frame.index)),
        "max_abs_z": pr_auc(labels, pd.Series(max_abs_z, index=frame.index)),
    }
