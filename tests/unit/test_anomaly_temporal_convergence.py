"""Convergence handling and future-window accuracy of temporal expectations."""

import warnings
import numpy as np
import pytest
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import HuberRegressor

from databricks.labs.dqx.anomaly.temporal import TemporalBasis, expected, fit_temporal, select_basis


@pytest.mark.parametrize("split_seed", [3, 7, 13])
def test_sampled_daily_volume_selects_a_converged_fit(split_seed: int) -> None:
    days = np.arange(540)
    rng = np.random.default_rng(21)
    count = 4000 + 9 * days + 300 * np.sin(2 * np.pi * days / 7) + rng.normal(0, 120, len(days))
    settled = count * (26 + 0.004 * days) + rng.normal(0, 4000, len(days))
    retained = np.random.default_rng(split_seed).random(len(days)) < 0.8
    seconds = days[retained] * 86400.0
    start = seconds.min()
    seconds -= start
    metrics = {"count": count[retained], "settled": settled[retained]}
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        basis, _ = select_basis(seconds, metrics)
        fitted = fit_temporal(seconds, metrics, basis)
    future_days = np.arange(540, 720)
    truth = 4000 + 9 * future_days + 300 * np.sin(2 * np.pi * future_days / 7)
    forecast = expected(future_days * 86400.0 - start, basis, fitted["count"])
    # Noise in the generated metric has standard deviation 120. The expected-level error
    # must remain below that on a separate future window, not just on the training rows.
    assert np.sqrt(np.mean((forecast - truth) ** 2)) < 120
    assert set(fitted) == {"count", "settled"}


def test_persistent_nonconvergence_omits_the_expectation(monkeypatch: pytest.MonkeyPatch) -> None:
    fit = HuberRegressor.fit

    def nonconverged(self: HuberRegressor, X: np.ndarray, y: np.ndarray) -> HuberRegressor:
        result = fit(self, X, y)
        warnings.warn("Synthetic optimizer limit", ConvergenceWarning)
        return result

    monkeypatch.setattr(HuberRegressor, "fit", nonconverged)
    seconds = np.arange(80, dtype=float)
    values = 5 + seconds + np.sin(seconds)
    assert not fit_temporal(seconds, {"metric": values}, TemporalBasis(span=79))


def test_unrelated_fit_warnings_remain_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    fit = HuberRegressor.fit

    def warned(self: HuberRegressor, X: np.ndarray, y: np.ndarray) -> HuberRegressor:
        result = fit(self, X, y)
        warnings.warn("Synthetic unrelated warning", UserWarning)
        return result

    monkeypatch.setattr(HuberRegressor, "fit", warned)
    seconds = np.arange(80, dtype=float)
    with pytest.warns(UserWarning, match="Synthetic unrelated warning"):
        fitted = fit_temporal(seconds, {"metric": 5 + seconds + np.sin(seconds)}, TemporalBasis(span=79))
    assert "metric" in fitted


@pytest.mark.parametrize("failure", [ValueError, FloatingPointError])
def test_unrelated_warning_survives_a_failed_fit(monkeypatch: pytest.MonkeyPatch, failure: type[Exception]) -> None:
    def failed(_self: HuberRegressor, _features: np.ndarray, _values: np.ndarray) -> HuberRegressor:
        warnings.warn("Synthetic warning before failure", UserWarning)
        raise failure("Synthetic fit failure")

    monkeypatch.setattr(HuberRegressor, "fit", failed)
    seconds = np.arange(80, dtype=float)
    with pytest.warns(UserWarning, match="Synthetic warning before failure"):
        fitted = fit_temporal(seconds, {"metric": 5 + seconds + np.sin(seconds)}, TemporalBasis(span=79))
    assert not fitted
