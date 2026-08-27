"""Should DQX warn a user that their data looks temporal? Measured answer: not from this signal.

`profile="timeseries"` selects a correlation-aware detector that measured far better on multivariate
metrics (incident coverage 0.82 against 0.36 on SMD). The obvious next step is for DQX to notice when a
table looks temporal and say so. This module exists so that proposal stays measured rather than
re-argued each time. **It is not wired into DQX**, and the numbers below are why.

## What was rejected first, and why it was never viable

Detecting a timestamp column. Nearly every Delta table has `created_at`, `updated_at` or `ingested_at`,
so "a timestamp exists, therefore this is a time series" fires on almost everything. Presence of a
column is not evidence of intent.

## What was actually measured

Mean absolute lag-1 autocorrelation of the numeric columns, under the row ordering given. The
hypothesis: genuine time series have serially correlated metrics, i.i.d. tabular rows do not, so the
statistic should separate the two even though timestamp presence cannot.

    TEMPORAL   SMD, 28 entities in time order   mean 0.660   min 0.426   max 0.791

    TABULAR    satellite    0.823
               cardio       0.728
               covertype    0.573
               spambase     0.104
               mnist        0.086
               thyroid      0.068
               fraud        0.065
               mammography  0.056
               shuttle      0.005
               campaign     0.004

## Result: it does not separate

Three of ten tabular datasets score above the weakest SMD entity, and two score above SMD's *mean*.
There is no threshold that admits every time series and rejects every tabular table, so a warning built
on this would fire on ordinary tabular data.

## Why it fails, which is the part worth remembering

The statistic is confounded by **any ordering correlated with the values**, not just a temporal one.
These datasets are stored sorted -- `satellite` at 0.823 is almost certainly ordered by class -- and
sorted storage produces serial correlation without any time series underneath.

That is not an artefact of the benchmark. It is the common case in a warehouse: batch loads, sorted ETL
output and backfills all leave `created_at` correlated with the values beside it. So the failure mode
transfers directly to the data a user would actually run this on, and it fails in the expensive
direction -- advising a change of algorithm on data that does not need one.

## What DQX does instead

Nothing automatic, and nothing silent. The resolved profile is logged at INFO on every training run, so
`auto` is visible rather than invisible, and the documentation states the choice and the measured
difference so a user who knows their data can make it. Choosing the algorithm for them would need to be
verified, and verifying it needs labels DQX does not have.

A better signal may exist -- regular cadence per entity, or autocorrelation compared against a
permutation baseline that controls for the ordering itself. Neither is attempted here: the point of this
module is that the cheap version was tried, measured, and declined.

Run:  uv run python benchmarks/anomaly_conditioning/profile_advisory_gate.py
"""

import numpy as np

from datasets.real import SMD_ENTITIES, load_smd_split
from datasets.tabular import DATASETS, load


def mean_abs_lag1_autocorr(values: np.ndarray) -> float:
    """Mean absolute lag-1 autocorrelation across columns, in the row order given.

    Constant columns are skipped: they have no correlation to measure, and including them as zeros
    would dilute the statistic by however many one-hot or indicator columns a frame happens to carry.
    """
    scores = []
    for column in range(values.shape[1]):
        series = values[:, column]
        if series.std() <= 1e-12 or len(series) < 3:
            continue
        current, previous = series[1:], series[:-1]
        if current.std() <= 1e-12 or previous.std() <= 1e-12:
            continue
        scores.append(abs(float(np.corrcoef(current, previous)[0, 1])))
    return float(np.mean(scores)) if scores else 0.0


def main() -> None:
    print("Temporal reference: SMD, per entity, in file (time) order")
    train, _, _ = load_smd_split()
    temporal = [mean_abs_lag1_autocorr(train[entity]) for entity in SMD_ENTITIES]
    print(
        f"  {len(SMD_ENTITIES)} entities | mean {np.mean(temporal):.3f} "
        f"| min {min(temporal):.3f} | max {max(temporal):.3f}\n"
    )

    print("Tabular comparison: classical benchmarks, in stored order")
    tabular: dict[str, float] = {}
    for name in DATASETS:
        values, _ = load(name)
        tabular[name] = mean_abs_lag1_autocorr(values)
    for name, score in sorted(tabular.items(), key=lambda item: -item[1]):
        print(f"  {name:14s} {score:.3f}")

    weakest_temporal = min(temporal)
    strongest_tabular = max(tabular.values())
    print("\nSeparation")
    print(f"  weakest temporal   {weakest_temporal:.3f}")
    print(f"  strongest tabular  {strongest_tabular:.3f}  ({max(tabular, key=lambda k: tabular[k])})")
    if weakest_temporal > strongest_tabular:
        print(f"  SEPARATES: any threshold in ({strongest_tabular:.3f}, {weakest_temporal:.3f}) works")
    else:
        above = sorted(name for name, score in tabular.items() if score > weakest_temporal)
        print(f"  DOES NOT SEPARATE: {len(above)} tabular datasets score above the weakest time series")
        print(f"  overlapping: {', '.join(above)}")


if __name__ == "__main__":
    main()
