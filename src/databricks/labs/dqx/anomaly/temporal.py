"""Fit an expected level over time, so a metric can be judged against its own history.

This is the third basis a metric can be compared against, beside the whole table and its own group. The
shape is deliberately the same as the group-relative feature in :mod:`transformers`: observed value minus
an expected value. Only the source of the expectation changes, from a per-group median to a function of
time, which is what lets it extrapolate to rows the training window never saw.

**Nothing here touches Spark.** These are pure functions over numpy arrays, called once per metric at
training time and evaluated per row at scoring time. Scoring runs inside a scalar pandas UDF that sees an
arbitrary chunk of rows with no ordering and no access to neighbours, so an expectation that is a *function
of time* is evaluable per row while anything needing the previous row is not. That is what keeps
"train on batch, score on streaming" working.

The time axis is **seconds relative to the start of the training window**. Callers subtract ``t_min``
before calling in, which keeps the numbers small and makes the period arithmetic below read in real units.

Three rules are measured rather than chosen, and each has a test that cites its number:

**A seasonal period needs enough complete cycles to be identifiable.** Fitting a daily cycle to 2.8 days of
history does not recover a daily shape; the Fourier terms absorb slow drift instead, and subtracting them
removes signal that was informative. Measured on the Server Machine Dataset, a period with 2.8 cycles in
the window cost Isolation Forest 15 points of event coverage (94.7% to 79.4%), 5.6 cycles cost nothing
(94.3%), and 66.7 cycles helped (96.1%). Note also that *variance explained is the wrong gate*: the
apparent seasonal strength rose from 0.00 to 0.39 as the cycle count fell, so a high figure on a short
window is overfitting rather than signal.

**The fit must be robust.** DQX trains on a sample that still contains anomalies, deliberately. Least
squares is not robust to them: with 5% of rows at six times their normal value, a ridge fit recovered a
slope of 0.0694 against a true 0.0500, a 39% overestimate, while Huber recovered 0.0502.

**Changepoints must be selected on held-out data, never on fit.** A piecewise-linear trend extrapolates the
*last* segment's slope, which with many changepoints is estimated from a short tail and is therefore noisy.
In-sample R-squared cannot see this at all: across 0 to 25 changepoints it stayed at 0.9913 to 0.9914 while
false flags one window past training ranged from 1.2% to 100%. So the count is chosen by measuring the fit
on a tail the fit never saw.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from sklearn.linear_model import HuberRegressor

logger = logging.getLogger(__name__)

# Measured: harm at 2.8 cycles, no cost at 5.6. Six is the first safe integer.
MIN_SEASONAL_CYCLES = 6
# A period cannot be resolved from samples spaced comparably to it. Four samples per cycle is the
# coarsest that can carry a sine at all, and this guard is what stops a daily period being offered for
# daily data.
MIN_SAMPLES_PER_CYCLE = 4
# Two harmonics per period. Deliberately few: every extra term is another chance to fit noise, and the
# cycle-count finding above is a warning about exactly that.
SEASONAL_HARMONICS = 2
# Candidate periods in seconds. Hour, day and week are the cycles business and telemetry data actually
# carry; anything else is a domain fact DQX has no way to know.
CANDIDATE_PERIODS_SECONDS: tuple[float, ...] = (3600.0, 86400.0, 604800.0)
_PERIOD_NAMES = {3600.0: "hourly", 86400.0: "daily", 604800.0: "weekly"}

# Changepoint search. Placed only in the first 80% of history so the extrapolating segment has data
# behind it, which is the same reason Prophet defaults to a changepoint range rather than the full span.
CHANGEPOINT_RANGE = 0.8
CHANGEPOINT_CANDIDATES: tuple[int, ...] = (0, 1, 3, 6)
HOLDOUT_FRACTION = 0.2

# Huber's transition point between squared and linear loss, in units of the residual scale it estimates.
HUBER_EPSILON = 1.35
HUBER_ALPHA = 1e-4
HUBER_MAX_ITER = 400

# MAD trimming. 1.4826 scales the median absolute deviation to a standard deviation for Gaussian data.
MAD_TO_SIGMA = 1.4826
MAD_TRIM_SIGMA = 3.0


@dataclass(frozen=True)
class TemporalBasis:
    """The design a metric's expected level is fitted against.

    Persisted with the model, because scoring has to rebuild exactly the same columns in exactly the same
    order. A basis plus a coefficient vector is the whole expectation.
    """

    trend: bool = True
    periods: tuple[float, ...] = ()
    harmonics: int = SEASONAL_HARMONICS
    changepoints: tuple[float, ...] = ()  # fractions of the span, in (0, 1)
    span: float = 1.0  # seconds covered by the training window, for scaling the trend term

    @property
    def n_terms(self) -> int:
        """Columns in the design matrix, intercept included."""
        return 1 + int(self.trend) + len(self.changepoints) + 2 * self.harmonics * len(self.periods)

    def describe(self) -> str:
        """Human-readable summary, for logs and the registry's queryable temporal_config column."""
        parts = ["trend"] if self.trend else []
        parts += [_PERIOD_NAMES.get(p, f"{p:g}s") for p in self.periods]
        if self.changepoints:
            parts.append(f"{len(self.changepoints)} changepoints")
        return "+".join(parts) if parts else "none"

    def to_dict(self) -> dict[str, Any]:
        return {
            "trend": self.trend,
            "periods": list(self.periods),
            "harmonics": self.harmonics,
            "changepoints": list(self.changepoints),
            "span": self.span,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TemporalBasis":
        return cls(
            trend=bool(data.get("trend", True)),
            periods=tuple(float(p) for p in data.get("periods", ())),
            harmonics=int(data.get("harmonics", SEASONAL_HARMONICS)),
            changepoints=tuple(float(c) for c in data.get("changepoints", ())),
            span=float(data.get("span", 1.0)),
        )


@dataclass
class TemporalFit:
    """A fitted basis and its per-metric coefficients, ready to persist."""

    basis: TemporalBasis
    coefficients: dict[str, list[float]] = field(default_factory=dict)
    skipped_periods: dict[float, str] = field(default_factory=dict)  # period -> why it was not fitted


def candidate_periods(seconds: np.ndarray) -> tuple[tuple[float, ...], dict[float, str]]:
    """Which seasonal periods the training window can actually support.

    Returns the admitted periods and, for every rejected one, the reason. The reasons are surfaced to the
    caller rather than swallowed: a user who expected a daily cycle and did not get one needs to be told
    that their window held 2.8 days, not left to wonder.

    Args:
        seconds: Time axis in seconds, relative to the window start. Need not be sorted.

    Returns:
        ``(admitted, rejected)`` where *rejected* maps a period in seconds to a human-readable reason.
    """
    admitted: list[float] = []
    rejected: dict[float, str] = {}
    if seconds.size < 2:
        return (), {p: "fewer than two rows" for p in CANDIDATE_PERIODS_SECONDS}

    span = float(np.max(seconds) - np.min(seconds))
    # Median gap rather than mean: a single large hole in the data should not make the cadence look coarse.
    gaps = np.diff(np.sort(seconds))
    positive = gaps[gaps > 0]
    cadence = float(np.median(positive)) if positive.size else 0.0

    for period in CANDIDATE_PERIODS_SECONDS:
        cycles = span / period if period > 0 else 0.0
        if cycles < MIN_SEASONAL_CYCLES:
            rejected[period] = f"window covers {cycles:.1f} cycles, needs {MIN_SEASONAL_CYCLES}"
            continue
        if cadence > 0 and period / cadence < MIN_SAMPLES_PER_CYCLE:
            rejected[period] = (
                f"sampled every {cadence:.0f}s, which is {period / cadence:.1f} samples per cycle "
                f"and cannot resolve the shape"
            )
            continue
        admitted.append(period)
    return tuple(admitted), rejected


def design_matrix(seconds: np.ndarray, basis: TemporalBasis) -> np.ndarray:
    """Build the design columns for *seconds* under *basis*.

    Column order is fixed by the basis and must not change between training and scoring, which is why the
    basis is persisted rather than recomputed.
    """
    scaled = seconds / basis.span if basis.span > 0 else np.zeros_like(seconds, dtype=float)
    columns: list[np.ndarray] = [np.ones_like(scaled, dtype=float)]
    if basis.trend:
        columns.append(scaled)
    for changepoint in basis.changepoints:
        # max(0, seconds - cp): lets the slope change at the changepoint without fitting separate segments.
        columns.append(np.maximum(0.0, scaled - changepoint))
    for period in basis.periods:
        for k in range(1, basis.harmonics + 1):
            angle = 2.0 * math.pi * k * seconds / period
            columns.append(np.sin(angle))
            columns.append(np.cos(angle))
    return np.column_stack(columns)


def _fit_one(design: np.ndarray, values: np.ndarray) -> list[float] | None:
    """Huber-fit one metric, returning ``[intercept, *coefficients]`` or None if it could not be fitted.

    The intercept is folded into the returned vector and the design's own constant column is dropped for
    the fit, so that scoring can evaluate a plain dot product without needing to know which convention
    was used.
    """
    if values.size <= design.shape[1] + 1:
        return None
    if float(np.std(values)) == 0.0:
        # A constant metric has no expectation to learn beyond its own level.
        return [float(values[0])] + [0.0] * (design.shape[1] - 1)
    try:
        model = HuberRegressor(epsilon=HUBER_EPSILON, alpha=HUBER_ALPHA, max_iter=HUBER_MAX_ITER)
        model.fit(design[:, 1:], values)
    except (ValueError, FloatingPointError) as exc:
        logger.debug(f"Temporal fit failed, falling back to no temporal feature for this metric: {exc}")
        return None
    return [float(model.intercept_), *(float(c) for c in model.coef_)]


def _holdout_residual_scale(seconds: np.ndarray, values: np.ndarray, basis: TemporalBasis) -> float:
    """Robust residual scale on a tail the fit never saw.

    This is the statistic changepoint counts are chosen by. It has to be measured *after* the fit window
    because that is where over-flexible trends go wrong: an in-sample criterion is blind to it.
    """
    order = np.argsort(seconds)
    t_sorted, v_sorted = seconds[order], values[order]
    cut = int(len(t_sorted) * (1.0 - HOLDOUT_FRACTION))
    if cut < basis.n_terms + 2 or cut >= len(t_sorted):
        return float("inf")
    coefficients = _fit_one(design_matrix(t_sorted[:cut], basis), v_sorted[:cut])
    if coefficients is None:
        return float("inf")
    residual = v_sorted[cut:] - design_matrix(t_sorted[cut:], basis) @ np.asarray(coefficients)
    # Median absolute deviation rather than a standard deviation: one bad row in the holdout should not
    # decide the changepoint count.
    return float(np.median(np.abs(residual - np.median(residual))) * MAD_TO_SIGMA)


def select_basis(seconds: np.ndarray, values: np.ndarray) -> tuple[TemporalBasis, dict[float, str]]:
    """Choose the basis for a table: which periods, and how many changepoints.

    *values* is a representative metric, used only to score changepoint counts. The periods depend on the
    time axis alone, so they are the same for every metric in the table, which is what keeps one basis and
    one column order for the whole model.

    Returns the basis and the rejected periods with their reasons.
    """
    seconds = np.asarray(seconds, dtype=float)
    span = float(np.max(seconds) - np.min(seconds)) if seconds.size > 1 else 1.0
    periods, rejected = candidate_periods(seconds)
    base = TemporalBasis(trend=True, periods=periods, harmonics=SEASONAL_HARMONICS, span=max(span, 1.0))

    values = np.asarray(values, dtype=float)
    best_basis, best_scale = base, _holdout_residual_scale(seconds, values, base)
    for count in CHANGEPOINT_CANDIDATES:
        if count == 0:
            continue
        changepoints = tuple(np.linspace(CHANGEPOINT_RANGE / (count + 1), CHANGEPOINT_RANGE, count))
        candidate = TemporalBasis(
            trend=True, periods=periods, harmonics=SEASONAL_HARMONICS, changepoints=changepoints, span=base.span
        )
        scale = _holdout_residual_scale(seconds, values, candidate)
        # Strictly better, so a tie leaves the simpler basis in place. Flexibility has to earn its keep:
        # in-sample fit was identical across 0 to 25 changepoints while extrapolation ranged 1.2% to 100%.
        if scale < best_scale:
            best_basis, best_scale = candidate, scale

    return best_basis, rejected


def fit_temporal(seconds: np.ndarray, metrics: dict[str, np.ndarray], basis: TemporalBasis) -> dict[str, list[float]]:
    """Fit *basis* to every metric, returning ``metric -> [intercept, *coefficients]``.

    A metric that cannot be fitted is omitted rather than given zeros, so the caller can tell the
    difference between "expected level is flat" and "no expectation was learned".
    """
    design = design_matrix(np.asarray(seconds, dtype=float), basis)
    fitted: dict[str, list[float]] = {}
    for name, values in metrics.items():
        coefficients = _fit_one(design, np.asarray(values, dtype=float))
        if coefficients is None:
            logger.debug(f"No temporal expectation fitted for metric {name!r}")
            continue
        fitted[name] = coefficients
    return fitted


def expected(seconds: np.ndarray, basis: TemporalBasis, coefficients: list[float]) -> np.ndarray:
    """The expected level at each timestamp.

    A function of time, so it extrapolates to any time, including rows past the training window. That is
    the property the whole approach rests on, and also why the caller must enforce a staleness horizon:
    extrapolation stays finite but its accuracy decays with distance.
    """
    design = design_matrix(np.asarray(seconds, dtype=float), basis)
    coefficient_array = np.asarray(coefficients, dtype=float)
    if coefficient_array.size != design.shape[1]:
        raise ValueError(
            f"temporal coefficients have length {coefficient_array.size} but the persisted basis needs "
            f"{design.shape[1]}; the model and its basis are out of step"
        )
    return design @ coefficient_array


def robust_scale_mask(residuals: np.ndarray, sigma: float = MAD_TRIM_SIGMA) -> np.ndarray:
    """Rows within *sigma* robust deviations on every column.

    A robust *fit* is not enough on its own. With 5% of training rows at six times normal, both a robust
    and a least-squares fit produced a learned residual spread near 170 and both then missed a 1.5x spike
    entirely, because the detector takes its notion of normal from residuals computed over the same
    contaminated rows. Trimming before the detector fits is what closes that.
    """
    residuals = np.atleast_2d(np.asarray(residuals, dtype=float))
    if residuals.shape[0] == 1 and residuals.size > 1:
        residuals = residuals.T
    centre = np.median(residuals, axis=0)
    deviation = np.median(np.abs(residuals - centre), axis=0) * MAD_TO_SIGMA
    # A column with no spread cannot exclude anything, so it must not divide by zero either.
    deviation = np.where(deviation <= 0, np.inf, deviation)
    return (np.abs((residuals - centre) / deviation) <= sigma).all(axis=1)


def trend_strength(seconds: np.ndarray, metrics: dict[str, np.ndarray], basis: TemporalBasis) -> float:
    """Share of variance the basis removes, as the median across metrics.

    Used to advise rather than to decide: where this is near zero the data has no temporal structure to
    subtract, and the caller is told the parameter is unlikely to help. Median rather than mean, so one
    wildly trending column in a table of forty does not carry the recommendation for the rest.
    """
    design = design_matrix(np.asarray(seconds, dtype=float), basis)
    shares: list[float] = []
    for values in metrics.values():
        values = np.asarray(values, dtype=float)
        variance = float(np.var(values))
        if variance <= 0:
            continue
        coefficients = _fit_one(design, values)
        if coefficients is None:
            continue
        residual = values - design @ np.asarray(coefficients)
        shares.append(max(0.0, 1.0 - float(np.var(residual)) / variance))
    return float(np.median(shares)) if shares else 0.0
