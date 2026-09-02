"""The temporal baseline fit: what it does, and the measurements that chose how it does it.

Every design rule in :mod:`databricks.labs.dqx.anomaly.temporal` was picked from a measurement rather
than from taste, so each test here reproduces the measurement rather than merely asserting the rule. A
future reader who wants to change one of these defaults will find the number that argued against them.

No Spark and no workspace: the module is pure numpy, which is what makes it unit-testable at all.
"""

import logging

import numpy as np
import pytest
from sklearn.linear_model import Ridge

from databricks.labs.dqx.anomaly.temporal import (
    CANDIDATE_PERIODS_SECONDS,
    MIN_SEASONAL_CYCLES,
    TemporalBasis,
    candidate_periods,
    design_matrix,
    expected,
    fit_temporal,
    select_basis,
    trend_strength,
)

HOUR = 3600.0
DAY = 86400.0
WEEK = 604800.0
TRUE_SLOPE_PER_SECOND = 0.05 / HOUR  # 0.05 per hourly step, the slope the contamination tests recover


def _hourly_axis(count: int) -> np.ndarray:
    return np.arange(count, dtype=float) * HOUR


def _linear_metric(seconds: np.ndarray, rng: np.random.Generator, noise: float = 3.0) -> np.ndarray:
    return 100.0 + TRUE_SLOPE_PER_SECOND * seconds + rng.normal(0, noise, seconds.size)


def _recovered_slope_per_hour(basis: TemporalBasis, coefficients: list[float]) -> float:
    """Convert the fitted trend coefficient back into units of "per hourly step".

    The design scales the trend column by the span, so the raw coefficient is per span rather than per
    second. Reading it back through :func:`expected` avoids duplicating that convention here.
    """
    probe = np.array([0.0, HOUR])
    values = expected(probe, basis, coefficients)
    return float(values[1] - values[0])


# ── the cycle guard ────────────────────────────────────────────────────────────────────────────────


def test_a_period_with_too_few_cycles_is_rejected():
    """Measured on the Server Machine Dataset: a daily period over 2.8 days of history cost Isolation
    Forest 15 points of event coverage, 94.7% down to 79.4%. Over 5.6 cycles it cost nothing."""
    # 2.8 days of hourly data. Enough resolution for a daily shape, nowhere near enough repetitions.
    seconds = _hourly_axis(int(2.8 * 24))

    admitted, rejected = candidate_periods(seconds)

    assert DAY not in admitted
    assert "2.8 cycles" in rejected[DAY]
    assert str(MIN_SEASONAL_CYCLES) in rejected[DAY]


def test_a_period_with_enough_cycles_is_admitted():
    """Six complete cycles is the threshold, and the first safe integer above the measured harm at 2.8."""
    seconds = _hourly_axis(MIN_SEASONAL_CYCLES * 24 + 24)

    admitted, rejected = candidate_periods(seconds)

    assert DAY in admitted
    assert DAY not in rejected


def test_a_period_the_cadence_cannot_resolve_is_rejected():
    """Daily samples cannot carry an hourly cycle however long the window is.

    The cycle guard alone would admit an hourly period here, because a long window trivially contains
    thousands of hours. Resolution is a separate question from repetition and needs its own guard.
    """
    seconds = np.arange(400, dtype=float) * DAY  # 400 days, sampled once a day

    admitted, rejected = candidate_periods(seconds)

    assert HOUR not in admitted
    assert "samples per cycle" in rejected[HOUR]
    assert WEEK in admitted  # 400 days is ~57 weeks, comfortably resolvable


def test_rejected_periods_are_reported_so_a_user_can_be_told():
    """Silence is the defect. A caller who expected a daily cycle must learn their window held 2.8 days."""
    admitted, rejected = candidate_periods(_hourly_axis(48))

    assert not admitted
    assert set(rejected) == set(CANDIDATE_PERIODS_SECONDS)
    assert all(reason for reason in rejected.values())


def test_a_single_row_admits_nothing_rather_than_dividing_by_zero():
    admitted, rejected = candidate_periods(np.array([0.0]))

    assert not admitted
    assert set(rejected) == set(CANDIDATE_PERIODS_SECONDS)


# ── robustness of the fit ──────────────────────────────────────────────────────────────────────────


def test_huber_recovers_the_slope_through_contaminated_training_data():
    """The measurement that rules out least squares.

    DQX fits a sample of the user's table with anomalies still in it, deliberately. With 5% of rows at six
    times normal, a ridge fit recovered a slope of 0.0694 against a true 0.0500, a 39% overestimate, while
    Huber recovered 0.0502. A biased slope tilts every residual downstream.
    """
    rng = np.random.default_rng(5)
    seconds = _hourly_axis(2000)
    values = _linear_metric(seconds, rng)
    contaminated = values.copy()
    contaminated[rng.choice(seconds.size, size=int(0.05 * seconds.size), replace=False)] *= 6.0

    basis = TemporalBasis(trend=True, span=float(seconds[-1]))
    coefficients = fit_temporal(seconds, {"m": contaminated}, basis)["m"]

    # Within 5% of the true slope despite one row in twenty being six times too large.
    assert _recovered_slope_per_hour(basis, coefficients) == pytest.approx(0.05, rel=0.05)


def test_least_squares_on_the_same_data_would_be_badly_biased():
    """Executable evidence for the previous test's choice, so the reasoning cannot be lost.

    Without this, a later contributor swapping Huber for the faster Ridge sees only a passing suite.
    """
    rng = np.random.default_rng(5)
    seconds = _hourly_axis(2000)
    values = _linear_metric(seconds, rng)
    values[rng.choice(seconds.size, size=int(0.05 * seconds.size), replace=False)] *= 6.0

    basis = TemporalBasis(trend=True, span=float(seconds[-1]))
    design = design_matrix(seconds, basis)
    ridge = Ridge(alpha=1e-3).fit(design[:, 1:], values)
    ridge_slope = _recovered_slope_per_hour(basis, [float(ridge.intercept_), *ridge.coef_])

    # Overestimates by more than a third, which is what the Huber test above avoids.
    assert ridge_slope > 0.05 * 1.25


def test_a_constant_metric_is_fitted_as_its_own_level():
    """No expectation to learn, and no division by a zero standard deviation either."""
    seconds = _hourly_axis(500)
    coefficients = fit_temporal(seconds, {"flat": np.full(seconds.size, 7.0)}, TemporalBasis(span=float(seconds[-1])))

    assert expected(seconds, TemporalBasis(span=float(seconds[-1])), coefficients["flat"]) == pytest.approx(7.0)


def test_a_metric_with_too_few_rows_is_omitted_rather_than_guessed():
    """Omitted, not zeroed, so the caller can distinguish "flat expectation" from "none learned"."""
    seconds = _hourly_axis(3)

    assert not fit_temporal(seconds, {"m": np.array([1.0, 2.0, 3.0])}, TemporalBasis(span=float(seconds[-1])))


# ── changepoints ───────────────────────────────────────────────────────────────────────────────────


def test_a_straight_series_earns_no_changepoints():
    """Flexibility has to be earned. In-sample fit is identical from 0 to 25 changepoints (R-squared
    0.9913 to 0.9914) while false flags one window out range 1.2% to 100%, so a tie must leave the
    simpler basis in place."""
    rng = np.random.default_rng(11)
    seconds = _hourly_axis(2000)

    basis, _ = select_basis(seconds, _linear_metric(seconds, rng))

    assert not basis.changepoints


def test_a_series_whose_slope_doubles_earns_changepoints():
    """The case changepoints exist for. A straight line through a bent trend leaves a residual that is
    not stationary, and everything downstream inherits it."""
    rng = np.random.default_rng(11)
    seconds = _hourly_axis(2000)
    knee = seconds[seconds.size // 2]
    values = 100.0 + TRUE_SLOPE_PER_SECOND * np.minimum(seconds, knee)
    values = values + 3.0 * TRUE_SLOPE_PER_SECOND * np.maximum(0.0, seconds - knee) + rng.normal(0, 3.0, seconds.size)

    basis, _ = select_basis(seconds, values)

    assert basis.changepoints


def test_changepoints_are_never_placed_in_the_recent_tail():
    """The extrapolating segment needs data behind it.

    A changepoint near the end of history leaves the final slope estimated from a handful of rows, which
    is precisely the mechanism that took out-of-window false flags to 100%.
    """
    rng = np.random.default_rng(3)
    seconds = _hourly_axis(2000)
    knee = seconds[seconds.size // 2]
    values = 100.0 + TRUE_SLOPE_PER_SECOND * np.minimum(seconds, knee)
    values = values + 4.0 * TRUE_SLOPE_PER_SECOND * np.maximum(0.0, seconds - knee) + rng.normal(0, 2.0, seconds.size)

    basis, _ = select_basis(seconds, values)

    assert all(changepoint <= 0.8 for changepoint in basis.changepoints)


# ── extrapolation, which is the property the design rests on ───────────────────────────────────────


def test_the_expectation_extrapolates_past_the_training_window():
    """A lookup table cannot serve a future timestamp; a function of seconds can.

    This is why the temporal baseline is a fitted function rather than a per-time-bucket median, and it
    is the difference between this approach and the time-bucket grouping that was measured and rejected.
    """
    rng = np.random.default_rng(1)
    seconds = _hourly_axis(1000)
    values = _linear_metric(seconds, rng, noise=0.5)
    basis = TemporalBasis(trend=True, span=float(seconds[-1]))
    coefficients = fit_temporal(seconds, {"m": values}, basis)["m"]

    # One full window beyond training, where no row was ever seen.
    future = seconds + float(seconds[-1])
    predicted = expected(future, basis, coefficients)

    assert np.all(np.isfinite(predicted))
    assert predicted[-1] == pytest.approx(100.0 + TRUE_SLOPE_PER_SECOND * future[-1], rel=0.02)


def test_coefficients_that_do_not_match_the_basis_raise():
    """A model and its basis drifting apart must fail loudly, not silently score a different feature."""
    basis = TemporalBasis(trend=True, periods=(DAY,), span=DAY * 10)

    with pytest.raises(ValueError, match="out of step"):
        expected(_hourly_axis(10), basis, [1.0, 2.0])


def test_the_design_column_count_matches_the_declared_basis():
    """Column order is a persisted contract between training and scoring."""
    basis = TemporalBasis(trend=True, periods=(DAY, WEEK), harmonics=2, changepoints=(0.3, 0.6), span=WEEK * 8)

    assert design_matrix(_hourly_axis(50), basis).shape[1] == basis.n_terms
    # intercept + trend + 2 changepoints + 2 periods x 2 harmonics x sin/cos
    assert basis.n_terms == 1 + 1 + 2 + 8


# ── the advisory statistic ─────────────────────────────────────────────────────────────────────────


def test_trend_strength_separates_a_trending_table_from_a_stationary_one():
    """The statistic behind the advisory, and the reason it is worth warning on.

    Measured, the Server Machine Dataset sits at a median 0.016 while synthetic trending data reaches
    0.999. That separation is what lets DQX tell a caller the parameter will not help them, instead of
    silently costing them 20 points of event coverage.
    """
    rng = np.random.default_rng(7)
    seconds = _hourly_axis(1500)
    basis = TemporalBasis(trend=True, span=float(seconds[-1]))

    trending = trend_strength(seconds, {"m": _linear_metric(seconds, rng, noise=1.0)}, basis)
    stationary = trend_strength(seconds, {"m": 100.0 + rng.normal(0, 3.0, seconds.size)}, basis)

    assert trending > 0.8
    assert stationary < 0.1


def test_trend_strength_ignores_constant_metrics_rather_than_scoring_them_zero():
    """A constant column has no variance to explain, so including it would drag the median down and
    advise against the parameter on a table that genuinely trends."""
    rng = np.random.default_rng(2)
    seconds = _hourly_axis(1000)
    basis = TemporalBasis(trend=True, span=float(seconds[-1]))
    metrics = {"trending": _linear_metric(seconds, rng, noise=1.0), "flat": np.full(seconds.size, 4.0)}

    assert trend_strength(seconds, metrics, basis) > 0.8


# ── persistence ────────────────────────────────────────────────────────────────────────────────────


def test_the_basis_round_trips_through_a_dict():
    """It is persisted in the model's feature metadata, so it has to survive JSON."""
    basis = TemporalBasis(trend=True, periods=(DAY, WEEK), harmonics=3, changepoints=(0.2, 0.5), span=12345.0)

    assert TemporalBasis.from_dict(basis.to_dict()) == basis


def test_an_empty_dict_deserializes_to_a_usable_default():
    """Models trained before the temporal fields existed carry an empty dict here."""
    basis = TemporalBasis.from_dict({})

    assert not basis.periods
    assert not basis.changepoints
    assert basis.trend is True


def test_the_basis_describes_itself_for_logs_and_the_registry():
    basis = TemporalBasis(trend=True, periods=(DAY,), changepoints=(0.4,), span=DAY * 20)

    described = basis.describe()

    assert "trend" in described
    assert "daily" in described
    assert "1 changepoints" in described


def test_select_basis_reports_what_it_skipped(caplog):
    """The reasons travel with the basis so the caller can warn; they are not logged and forgotten."""
    rng = np.random.default_rng(4)
    seconds = _hourly_axis(48)  # two days: nothing is admissible

    with caplog.at_level(logging.DEBUG):
        basis, rejected = select_basis(seconds, _linear_metric(seconds, rng))

    assert not basis.periods
    assert set(rejected) == set(CANDIDATE_PERIODS_SECONDS)
