"""Unit pins for the severity tail above p95 (no Spark).

`threshold` is documented as an alert budget, and above p95 it did not deliver one: the quantile grid has
knots at 95, 99 and 100 and nothing between, so linear interpolation in *score* space put the cut for
"severity 98" above the true 98th percentile. Measured against a training distribution scored with itself,
so with the grid as the only variable, threshold 98 flagged 1.4-1.6% of rows against the 2% it promises and
threshold 99.9 flagged nothing at all.

The tests here pin the three properties the fix rests on: the two anchors are exact, so nobody's current
alerts move; severity is strictly increasing past p99, so the rows that most need ordering can be ordered;
and the realised alert rate is close to the requested one, which is the regression guard.

The numpy implementation is tested rather than the Spark one because the two are required to agree and only
one of them runs without a session. `tests/integration_anomaly/` covers the Spark side and their parity.
"""

import numpy as np
import pytest

from databricks.labs.dqx.anomaly.explainability import severity_from_scores
from databricks.labs.dqx.anomaly.scoring_config import (
    SEVERITY_QUANTILE_KEYS,
    TAIL_ANCHOR_PERCENTILE,
    TAIL_RATE_PERCENTILE,
)


def _quantile_points(scores: np.ndarray) -> list[tuple[float, float]]:
    """The grid a trained model persists: one score per key in SEVERITY_QUANTILE_KEYS."""
    return [(percentile, float(np.quantile(scores, percentile / 100.0))) for percentile, _ in SEVERITY_QUANTILE_KEYS]


def _realised_alert_rate(scores: np.ndarray, points: list[tuple[float, float]], threshold: float) -> float:
    """The share of rows a given threshold actually flags, which is what the docs promise."""
    severity = severity_from_scores(scores, points)
    return float((severity >= threshold).mean())


# A score distribution per shape a real detector produces. Mahalanobis distances are chi-square-like;
# Isolation Forest path lengths are closer to normal; the heavy-tailed cases are the adversarial ones.
_DISTRIBUTIONS = {
    "normal": lambda rng: rng.normal(0.0, 1.0, 200_000),
    "chi_square_38": lambda rng: rng.chisquare(38, 200_000),
    "gamma": lambda rng: rng.gamma(2.0, 2.0, 200_000),
    "lognormal": lambda rng: rng.lognormal(0.0, 1.0, 200_000),
}


@pytest.mark.parametrize("shape", sorted(_DISTRIBUTIONS))
def test_the_two_anchors_are_exact(shape: str):
    """95 and 99 are fixed points, which is what makes the default configuration byte-identical.

    The alternative fix, adding knots at 96/97/98/99.5, would have moved severity everywhere above 95 and
    needed new persisted keys that pre-existing models do not carry.
    """
    scores = _DISTRIBUTIONS[shape](np.random.default_rng(7))
    points = _quantile_points(scores)
    by_percentile = dict(points)

    at_anchor = severity_from_scores(np.array([by_percentile[TAIL_ANCHOR_PERCENTILE]]), points)
    at_rate = severity_from_scores(np.array([by_percentile[TAIL_RATE_PERCENTILE]]), points)

    assert at_anchor[0] == pytest.approx(TAIL_ANCHOR_PERCENTILE, abs=1e-9)
    assert at_rate[0] == pytest.approx(TAIL_RATE_PERCENTILE, abs=1e-9)


@pytest.mark.parametrize("shape", sorted(_DISTRIBUTIONS))
@pytest.mark.parametrize("threshold", [96.0, 97.0, 98.0, 99.5])
def test_a_threshold_between_the_knots_flags_close_to_what_it_promises(shape: str, threshold: float):
    """The regression guard, and the reason the tail exists.

    Before this, 98 flagged 1.4-1.6% rather than 2%, and 99.5 flagged 0.00-0.03% rather than 0.5%. A 25%
    relative tolerance is loose on purpose: the tail assumes an exponential shape, which is right for these
    detectors' scores and not exact for any of them. The old behaviour missed by 20 to 100%, so this
    separates the two comfortably without pretending to a precision the model does not have.
    """
    scores = _DISTRIBUTIONS[shape](np.random.default_rng(11))
    points = _quantile_points(scores)

    promised = (100.0 - threshold) / 100.0
    realised = _realised_alert_rate(scores, points, threshold)

    assert realised == pytest.approx(
        promised, rel=0.25
    ), f"{shape} at threshold {threshold}: flagged {realised:.4%}, promised {promised:.4%}"


@pytest.mark.parametrize("threshold", [90.0, 95.0, 99.0])
def test_the_documented_thresholds_were_already_exact_and_stay_exact(threshold: float):
    """90, 95 and 99 land on knots, so they were never affected and must not become so."""
    scores = _DISTRIBUTIONS["chi_square_38"](np.random.default_rng(3))
    points = _quantile_points(scores)

    promised = (100.0 - threshold) / 100.0
    realised = _realised_alert_rate(scores, points, threshold)

    assert realised == pytest.approx(promised, rel=0.02)


def test_severity_keeps_increasing_past_the_last_knot():
    """The old expression clamped every score past the training maximum to exactly 100.

    Measured on real telemetry, that was 20% of a scored batch on average for the correlation-aware
    detector, and for one entity all of it. Those rows are the ones the docs tell you to rank by severity.

    The tail orders them instead, though not forever: it asymptotes to 100, so at some distance the
    difference falls below float64 resolution and ties resume. Measured, that is around 21 times the
    p95-to-p99 span past p95, while the training maximum sits about 4.6 spans out. So the range over which
    rows can be ordered extends roughly four times further than the training data itself reaches, which is
    the useful claim; "no ties ever" would not be true.
    """
    scores = _DISTRIBUTIONS["chi_square_38"](np.random.default_rng(5))
    points = _quantile_points(scores)
    by_percentile = dict(points)
    anchor = by_percentile[TAIL_ANCHOR_PERCENTILE]
    span = by_percentile[TAIL_RATE_PERCENTILE] - anchor
    training_max = float(max(q for _, q in points))

    # From the old clamp point outward, in span units, stopping short of the float64 asymptote.
    probes = np.array([training_max + multiple * span for multiple in (0.0, 1.0, 2.0, 5.0, 10.0)])
    severity = severity_from_scores(probes, points)

    assert np.all(np.diff(severity) > 0), f"ties where the old map clamped: {severity}"
    assert np.all(severity < 100.0), "severity asymptotes to 100 rather than reaching it here"
    assert np.all(severity > TAIL_RATE_PERCENTILE)


def test_severity_stays_monotonic_across_the_whole_range():
    """A severity map that is not monotonic in score would make any threshold meaningless."""
    scores = _DISTRIBUTIONS["gamma"](np.random.default_rng(13))
    points = _quantile_points(scores)

    probes = np.linspace(float(min(q for _, q in points)) - 1.0, float(max(q for _, q in points)) * 3.0, 4000)
    severity = severity_from_scores(probes, points)

    assert np.all(np.diff(severity) >= 0.0)


def test_a_degenerate_tail_falls_back_to_the_anchor():
    """A constant score above p95 leaves no width to interpolate over.

    Real: a table where almost every row scores identically, so p95 and p99 coincide. The anchor percentile
    is then the answer outright, which is how a zero-width segment lower down is already handled.
    """
    points = [(percentile, 0.0 if percentile <= 95.0 else 0.0) for percentile, _ in SEVERITY_QUANTILE_KEYS]

    severity = severity_from_scores(np.array([0.0, 5.0]), points)

    assert np.all(np.isfinite(severity))


def test_points_without_the_anchors_keep_the_plain_linear_behaviour():
    """A caller supplying its own grid need not include p95 and p99, and must not be silently reshaped."""
    points = [(0.0, 0.0), (50.0, 10.0), (100.0, 20.0)]

    severity = severity_from_scores(np.array([0.0, 5.0, 10.0, 15.0, 20.0, 40.0]), points)

    assert severity.tolist() == [0.0, 25.0, 50.0, 75.0, 100.0, 100.0]


def test_scores_below_the_anchor_are_untouched_by_the_tail():
    """Everything at or below p95 must map exactly as it did before, or existing alerts move."""
    scores = _DISTRIBUTIONS["normal"](np.random.default_rng(17))
    points = _quantile_points(scores)
    body = [(p, q) for p, q in points if p <= TAIL_ANCHOR_PERCENTILE]

    probes = np.linspace(float(body[0][1]), float(body[-1][1]), 500)
    with_tail = severity_from_scores(probes, points)
    plain_linear = np.interp(probes, [q for _, q in body], [p for p, _ in body])

    assert np.allclose(with_tail, plain_linear, atol=1e-9)
