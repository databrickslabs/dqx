"""The two-factor synthetic sweep that decides whether a heterogeneity gate is needed.

Real datasets contribute roughly ten grouping candidates in total, which is far too few to
establish a correlation between heterogeneity and the benefit of conditioning. Synthetic data has
no licensing constraints and lets both factors be set directly, so the sweep carries the breadth
and the real datasets check that its conclusion survives contact with real data.

**Factor 1 — level spread.** How far apart the per-group baseline levels sit. At spread 0 every
group has the same level and eta-squared is ~0; at the top of the range levels differ by an order
of magnitude and eta-squared approaches 0.9. This moves the gate's input continuously across its
whole range, including the region below the 0.10 threshold that was removed.

**Factor 2 — anomaly mechanism.** What actually makes a row anomalous:

``global``
    The value is extreme for the table as a whole. A pooled model should find these, and the
    baseline-relative feature should be redundant rather than harmful — the prediction that
    justified removing the gate rather than keeping it pending.

``contextual``
    The value is ordinary for the table but wrong for its own group. Only a comparison against the
    group's own baseline can see these. This is #1484.

The two mechanisms are the confound that matters: without splitting on it, any correlation between
eta-squared and the benefit of conditioning could be entirely an artefact of contextual anomalies
being more common at high spread.
"""

import numpy as np

MECHANISMS = ("global", "contextual")


def generate(
    *,
    seed: int,
    level_spread: float,
    mechanism: str,
    n_groups: int = 12,
    n_rows_per_group: int = 400,
    anomaly_rate: float = 0.02,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return ``(values, labels, groups)`` for one cell of the sweep.

    Args:
        seed: Controls the data draw. Paired with the forest seed by the caller.
        level_spread: 0.0 gives every group the same baseline level; 1.0 spans an order of
            magnitude. This is the axis eta-squared tracks.
        mechanism: ``"global"`` or ``"contextual"`` — see the module docstring.
        n_groups: Number of groups.
        n_rows_per_group: Rows per group.
        anomaly_rate: Share of rows labelled anomalous.
    """
    if mechanism not in MECHANISMS:
        raise ValueError(f"mechanism must be one of {MECHANISMS}, got {mechanism!r}")

    rng = np.random.default_rng(seed)
    base_level = 1000.0
    # Levels are spread multiplicatively and symmetrically about base_level, so the *mean* level is
    # roughly constant across the sweep. Otherwise raising the spread would also raise the overall
    # scale, and the two effects would be inseparable.
    factors = np.linspace(1.0 - 0.9 * level_spread, 1.0 + 0.9 * level_spread, n_groups)
    levels = base_level * np.clip(factors, 0.05, None)

    values_list, labels_list, groups_list = [], [], []
    for index in range(n_groups):
        level = levels[index]
        rows = rng.normal(level, level * 0.08, size=(n_rows_per_group, 2))
        rows[:, 1] = rng.normal(level * 0.5, level * 0.05, size=n_rows_per_group)
        labels = np.zeros(n_rows_per_group)

        n_anomalies = max(1, int(n_rows_per_group * anomaly_rate))
        picks = rng.choice(n_rows_per_group, n_anomalies, replace=False)
        if mechanism == "global":
            # Extreme against the whole table: far above the largest group's normal range.
            rows[picks, 0] = base_level * (1.0 + 0.9 * level_spread) * rng.uniform(4.0, 6.0, n_anomalies)
        else:
            # Ordinary globally, wrong for this group: collapse to a fifth of the group's own
            # level, which lands inside the range other groups occupy normally.
            rows[picks, 0] = level * 0.2
        labels[picks] = 1.0

        values_list.append(rows)
        labels_list.append(labels)
        groups_list.append(np.full(n_rows_per_group, f"g{index:02d}"))

    return (
        np.vstack(values_list),
        np.concatenate(labels_list),
        np.concatenate(groups_list),
    )


def sweep_points(n_spreads: int = 9) -> list[tuple[float, str]]:
    """The (level_spread, mechanism) grid.

    Spreads are dense at the low end because that is where the removed gate would have fired, and
    where the "monotone duplicate rather than noise" prediction has to hold for its removal to be
    defensible.
    """
    spreads = sorted(
        {round(v, 3) for v in np.concatenate([np.linspace(0.0, 0.2, 5), np.linspace(0.2, 1.0, n_spreads)])}
    )
    return [(spread, mechanism) for spread in spreads for mechanism in MECHANISMS]
