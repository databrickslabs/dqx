"""Why trend is a documented limitation, and why `drift_threshold` is not the safety net for it.

Row anomaly detection learns "normal" from a window and compares later rows against it. A steadily
growing metric therefore leaves that window, and ordinary rows start being flagged. This module measures
how quickly that happens, and tests the two fixes that suggest themselves -- both of which fail. **It is
not wired into DQX**; it exists so the documented limitation stays measured rather than re-argued.

## Fix 1: give the model an elapsed-time feature so it can learn the slope

Measured: it makes false flagging *worse*, not better -- 95.8% -> 100.0% on a batch that simply continues
the trend. New rows carry time values outside the training range, and isolating an out-of-range value
takes very few splits, which is precisely what Isolation Forest scores as anomalous. The feature intended
to explain the growth away becomes the strongest evidence of anomaly.

## Fix 2: condition on a time bucket with `baseline_by`

Cannot work, for a structural reason rather than a numerical one. `baseline_by` persists a median per
group at training time and looks it up while scoring; a row whose group was absent from training is
reported via ``is_new_baseline`` and has its score, severity and contributions **nulled**
(``scoring_utils._null_unseen_group_scores``). Every future time bucket is by definition absent from
training, so a model conditioned on one would score nothing at all. The null-on-unseen behaviour is
correct for categorical groups -- an unseen category cannot be judged honestly -- which is exactly why a
time bucket is the wrong thing to condition on.

## So the remaining question: does drift detection warn before scoring degrades?

No, and it cannot. `drift_threshold=3.0` is the documented setting; the drift score saturates below 1.8
however steep the trend, because ``_compute_column_drift_score`` divides the batch's mean shift by the
*training window's* standard deviation, and a linear ramp inflates that standard deviation in proportion
to the growth. Both numerator and denominator scale together, so the ratio is bounded:

    trend over window   false flags   drift score   warns at 3.0?
                    0%          3.0%          0.04   no (correct)
                    4%          5.8%          0.60   NO
                   10%         19.6%          1.17   NO
                   20%         45.4%          1.53   NO
                   40%         80.2%          1.68   NO
                  100%         95.8%          1.74   NO
                  200%         99.2%          1.74   NO

At 200% growth, 99.2% of a batch in which nothing is wrong is flagged, and drift detection stays silent.

## What the documentation says because of this

Not "enable drift_threshold and you will be warned" -- that would be a false safety guarantee, and a user
relying on it gets burned without notice. Instead: model a quantity that does not trend (a rate or a
ratio, not a running level), and where the level itself matters, retrain on a schedule.

Fixing drift detection for this case is a separate change -- it needs a trend-aware statistic rather than
a z-score against an inflated baseline -- and is deliberately not attempted here.

Run:  uv run python benchmarks/anomaly_conditioning/trend_limits.py
"""

import numpy as np
from sklearn.ensemble import IsolationForest

N_TRAIN = 2000
N_SCORE = 500
CONTAMINATION = 0.02
NOISE = 3.0
# The setting the user guide documents, and what DQX compares its max-across-columns score against.
DRIFT_THRESHOLD = 3.0
# Trend slopes per row, spanning "flat" to "the metric tripled across the training window".
SLOPES = (0.0, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1)


def _series(slope: float, start: int, count: int, rng: np.random.Generator) -> np.ndarray:
    """A metric growing at *slope* per row, with constant noise."""
    t = np.arange(start, start + count, dtype=float)
    return 100.0 + slope * t + rng.normal(0, NOISE, count)


def false_flag_rate(train: np.ndarray, score: np.ndarray) -> float:
    """Share of a nothing-is-wrong batch that gets flagged. The correct answer is ~CONTAMINATION."""
    model = IsolationForest(contamination=CONTAMINATION, random_state=42).fit(train.reshape(-1, 1))
    return float((model.predict(score.reshape(-1, 1)) == -1).mean())


def drift_score(train: np.ndarray, score: np.ndarray) -> float:
    """DQX's per-column drift score, mirroring ``drift._compute_column_drift_score``."""
    baseline_mean, baseline_std = float(train.mean()), float(train.std())
    current_mean, current_std = float(score.mean()), float(score.std())
    if baseline_std == 0:
        return abs(current_mean - baseline_mean)
    z_score = abs(current_mean - baseline_mean) / baseline_std
    std_change = abs(current_std - baseline_std) / baseline_std
    return (z_score * 0.7) + (std_change * 0.3)


def elapsed_time_feature_makes_it_worse() -> tuple[float, float]:
    """Fix 1, measured: adding a monotonic time feature raises the false-flag rate."""
    rng = np.random.default_rng(42)
    t_train = np.arange(N_TRAIN, dtype=float)
    metric_train = _series(0.05, 0, N_TRAIN, rng)
    t_score = np.arange(N_TRAIN, N_TRAIN + N_SCORE, dtype=float)
    metric_score = _series(0.05, N_TRAIN, N_SCORE, rng)

    without = false_flag_rate(metric_train, metric_score)

    model = IsolationForest(contamination=CONTAMINATION, random_state=42).fit(np.column_stack([metric_train, t_train]))
    with_time = float((model.predict(np.column_stack([metric_score, t_score])) == -1).mean())
    return without, with_time


def main() -> None:
    print("Does a trend break scoring, and does drift detection warn?\n")
    print(f"{'trend over window':>18} {'false flags':>12} {'drift score':>12}  warns?")
    print("-" * 60)
    for slope in SLOPES:
        rng = np.random.default_rng(42)
        train = _series(slope, 0, N_TRAIN, rng)
        score = _series(slope, N_TRAIN, N_SCORE, rng)
        flags = false_flag_rate(train, score)
        drift = drift_score(train, score)
        # A flat series *should* stay silent, so only a missed warning on a real trend is a failure.
        if drift >= DRIFT_THRESHOLD:
            warns = "yes"
        else:
            warns = "NO" if slope > 0 else "no (correct)"
        print(f"{slope * N_TRAIN:>17.0f}% {flags:>11.1%} {drift:>12.2f}  {warns}")

    print("\nfalse flags = share of a batch that merely continues the trend and is flagged anomalous")
    print(f"              (the correct answer is ~{CONTAMINATION:.0%}, the contamination rate)")

    without, with_time = elapsed_time_feature_makes_it_worse()
    print("\nFix 1 -- add an elapsed-time feature so the model can learn the slope:")
    print(f"  metric only            {without:6.1%} falsely flagged")
    print(f"  metric + elapsed time  {with_time:6.1%} falsely flagged  <- worse, not better")

    print("\nFix 2 -- condition on a time bucket via baseline_by:")
    print("  Not measurable, and not viable: every future bucket is an unseen group, whose score,")
    print("  severity and contributions are nulled. Such a model would score nothing. See the")
    print("  module docstring.")


if __name__ == "__main__":
    main()
