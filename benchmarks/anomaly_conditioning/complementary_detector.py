"""Does a second detector alongside Isolation Forest help? Measured answer: not as a default.

Isolation Forest loses to a four-line z-score on two of ten classical benchmarks, and loses under
every hyperparameter setting tried -- more trees, more samples per tree, all samples. So the gap is
inductive bias, not tuning: axis-parallel random splits dilute when an anomaly is one extreme feature
among few dimensions, and degrade in high dimension.

The obvious remedy is to add a cheap complementary scorer to the ensemble. This module exists so that
proposal stays measured rather than re-argued. **It is not wired into DQX**, and the numbers below are
why.

## What was measured

Held-out splits (70/30), 5 seeds, the shipped forest configuration. Each member's scores are mapped to
a percentile against its own *training* distribution before combining -- the calibration a real
implementation would have to persist, since DQX averages member scores raw and the two scales are
incomparable (Isolation Forest sits around 0.4-0.7, max-abs-z is unbounded). Ranking inside the
scoring UDF would be wrong: the UDF sees a pandas batch, not the frame, so ranks would depend on
partitioning.

## Result

    mean-of-percentiles   wins 5/10   median -0.0028   worst -0.1409 (thyroid)
    max-of-percentiles    wins 3/10   median -0.0253   worst -0.0690

Per dataset, where the two disagree most:

    cover      IF 0.0509   z 0.1009   mean +0.0121   max +0.0295
    mnist      IF 0.2609   z 0.3641   mean +0.0475   max +0.0724
    thyroid    IF 0.5446   z 0.2939   mean -0.1409   max -0.0690
    fraud      IF 0.2607   z 0.1249   mean -0.0371   max -0.0598
    shuttle    IF 0.9764   z 0.8948   mean -0.0176   max -0.0556

It helps exactly where predicted and hurts where Isolation Forest is genuinely stronger. A coin flip
on average, with a 0.14 worst case, is not a default.

## Why this cannot be fixed by choosing per dataset

Picking the better detector requires knowing which regime you are in, and that means labels. DQX is
unsupervised: there are none. So the honest options are to expose the choice to a user who does know
their anomalies are single-feature extremes, or to document the limitation. Guessing is not among
them.

Run it with:

    uv run python benchmarks/anomaly_conditioning/complementary_detector.py
"""

import json
import pathlib
import sys

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from conditioning import CONTAMINATION, N_TREES  # noqa: E402
from datasets import tabular  # noqa: E402

SEEDS = 5
TRAIN_FRACTION = 0.7
RESULTS = pathlib.Path(__file__).resolve().parent / "results"


def percentile_of(reference: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Map *values* onto their percentile within a *reference* distribution.

    This is the calibration a real implementation would persist: the reference is the member's
    training scores, computed once at training. Partition-independent by construction.
    """
    order = np.sort(reference)
    return np.searchsorted(order, values, side="right") / max(1, len(order))


def max_abs_z(train: np.ndarray, values: np.ndarray) -> np.ndarray:
    """Largest absolute z-score across features, standardised on the training split."""
    means, stds = np.nanmean(train, axis=0), np.nanstd(train, axis=0)
    stds = np.where(stds == 0, 1.0, stds)
    return np.nanmax(np.abs((values - means) / stds), axis=1)


def measure_dataset(values: np.ndarray, labels: np.ndarray) -> dict[str, float] | None:
    """Median PR-AUC per strategy over *SEEDS* held-out splits, or None if unusable."""
    per_strategy: dict[str, list[float]] = {k: [] for k in ("iforest", "zscore", "mean_pct", "max_pct")}
    for seed in range(SEEDS):
        rng = np.random.default_rng(seed)
        idx = rng.permutation(len(values))
        cut = int(TRAIN_FRACTION * len(values))
        train_idx, test_idx = idx[:cut], idx[cut:]
        if len(np.unique(labels[test_idx])) < 2:
            continue

        forest = IsolationForest(n_estimators=N_TREES, contamination=CONTAMINATION, random_state=seed, n_jobs=-1).fit(
            values[train_idx]
        )
        if_train = -forest.score_samples(values[train_idx])
        if_test = -forest.score_samples(values[test_idx])
        z_train = max_abs_z(values[train_idx], values[train_idx])
        z_test = max_abs_z(values[train_idx], values[test_idx])

        if_pct, z_pct = percentile_of(if_train, if_test), percentile_of(z_train, z_test)
        held_out = labels[test_idx]
        per_strategy["iforest"].append(average_precision_score(held_out, if_test))
        per_strategy["zscore"].append(average_precision_score(held_out, z_test))
        per_strategy["mean_pct"].append(average_precision_score(held_out, (if_pct + z_pct) / 2))
        per_strategy["max_pct"].append(average_precision_score(held_out, np.maximum(if_pct, z_pct)))

    if not per_strategy["iforest"]:
        return None
    out = {k: float(np.median(v)) for k, v in per_strategy.items()}
    out["mean_vs_iforest"] = out["mean_pct"] - out["iforest"]
    out["max_vs_iforest"] = out["max_pct"] - out["iforest"]
    return out


def main() -> int:
    results: list[dict[str, object]] = []
    # Deltas are accumulated as floats alongside the rows rather than read back out of them: the rows
    # mix a dataset name with numbers, so indexing them yields object and the statistics below would
    # need casts to satisfy the type checker.
    deltas: dict[str, list[float]] = {"mean": [], "max": []}
    for name, values, labels in tabular.iter_datasets():
        measured = measure_dataset(values, labels)
        if measured is None:
            continue
        row: dict[str, object] = {"dataset": name, "base_rate": float(labels.mean()), **measured}
        results.append(row)
        deltas["mean"].append(measured["mean_vs_iforest"])
        deltas["max"].append(measured["max_vs_iforest"])
        print(
            f"{name:<12} base {float(labels.mean()):>7.3%}  IF {measured['iforest']:.4f}  "
            f"z {measured['zscore']:.4f}  "
            f"mean {measured['mean_pct']:.4f} ({measured['mean_vs_iforest']:+.4f})  "
            f"max {measured['max_pct']:.4f} ({measured['max_vs_iforest']:+.4f})",
            flush=True,
        )

    print("\n--- verdict ---")
    for strategy, values_for_strategy in deltas.items():
        print(
            f"{strategy}-of-percentiles: "
            f"wins {sum(d > 0 for d in values_for_strategy)}/{len(values_for_strategy)}, "
            f"median {np.median(values_for_strategy):+.4f}, "
            f"worst {min(values_for_strategy):+.4f}, best {max(values_for_strategy):+.4f}"
        )
    print("\nNot a default. See the module docstring.")

    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / "complementary-detector.json"
    path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
