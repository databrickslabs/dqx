"""A correlation-aware detector for multivariate metrics, and its exact feature attribution.

IsolationForest splits on one randomly chosen feature at a time, which is why it is strong on tabular
data and weak on multivariate metrics whose anomalies are *broken correlations* rather than extreme
single values. Measured on SMD (28 machines, 38 metrics, fit on the train split and scored on the test
split, no point adjustment) it catches 36% of incidents inside a 1%-of-rows alert budget; the
Mahalanobis detector here catches 82%. Refitting on training data that still contains anomalies -- what
DQX actually does -- costs both of them a few points and does not change the conclusion: 33% against
79%. That was the result that could have sunk the approach, because sample covariance is not robust and
a few extreme rows inflate it along the very direction that needs to stay tight. Detection quality on
DQX's own synthetic fixtures is published in the benchmarks report and measured by
``tests/perf/test_anomaly_benchmark.py``.

The distance is the ordinary squared Mahalanobis distance from the training centre,
``d² = (x−μ)ᵀ Σ⁻¹ (x−μ)``, with three deliberate choices.

**Standardisation is internal, not a pipeline step.** Mahalanobis distance is invariant under any
invertible linear map: standardising *x* and using the standardised covariance gives bit-identical
distances to using the raw covariance. So a scaler cannot change the answer directly — it changes it
only *through the regulariser*, whose shrinkage target is not affine-equivariant. A fixed ridge is
negligible against a column of variance 1e6 and dominant against one of variance 1e-3, so the ridge is
expressed relative to the average variance and the standardisation is kept inside this estimator, where
it belongs. Keeping it internal also leaves the sklearn pipeline single-step and therefore identical in
shape to the IsolationForest one, so nothing downstream has to know a scaler exists.

**Regularisation adapts to the sample.** A plain ridge floor measured better than Ledoit-Wolf on SMD
(incident coverage 0.821 vs 0.769) — but SMD has 38 features and thousands of rows per entity, which is
exactly the well-conditioned regime where shrinkage has nothing to add. Ledoit-Wolf earns its place in
the small-sample regime and against the collinear one-hot columns DQX's feature engineering produces,
neither of which SMD exercises. So the ridge floor is the default and Ledoit-Wolf is used when the
sample is too small for a stable empirical covariance.

**The estimate is not robust to gross contamination, and trimming was measured and rejected.** The mean,
the standard deviation and the sample covariance all move with the rows they are computed over, so a
training sample containing a few wildly wrong rows produces an inflated scale, and a real but moderate
anomaly then scores *inside* it. Measured on synthetic data: with 5% of rows at six times normal, a 1.5x
spike went entirely undetected. The cost is a miss rate rather than a false-alarm rate, which is what
makes it easy to overlook.

A median-and-MAD trim was implemented to fix that and then removed, because it cost more than it bought
on real data: on SMD it excluded 4.6% of training rows and took event coverage from 91.9% to 86.4% and
ROC-AUC from 0.784 to 0.763. Those rows were legitimate tail observations -- SMD's train split is clean
by construction -- and a covariance estimated from the narrower core scored ordinary rows as anomalous.

A distortion statistic was then tried, to fire the trim only on gross contamination and leave heavy tails
alone. It does not separate them: a t-distribution with df=1.5 gives a scale-distortion ratio of 4.64 and
the worst clean SMD entity 3.48, while the 6x-contaminated sample gives 1.62. The ordering is inverted,
so there is no cheap in-sample signal to gate on.

What this means for a caller: if the training sample is known to contain grossly wrong rows, filter them
before training or narrow the input, because the detector will not do it for you. Ordinary tails need no
action. Recorded in ``robust_gate.py`` and ``robust_gate2.py`` alongside the benchmark harness.

**Attribution is leave-one-out, and non-negative by construction.** See
:meth:`MahalanobisDetector.feature_contributions`.
"""

import logging
import sys
from typing import Any

import cloudpickle
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, OutlierMixin
from sklearn.covariance import LedoitWolf
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig

logger = logging.getLogger(__name__)

# Below this many rows per feature the empirical covariance is too noisy to invert meaningfully, so
# Ledoit-Wolf shrinkage is used instead. Ten is the usual rule of thumb for a stable covariance.
SMALL_SAMPLE_ROWS_PER_FEATURE = 10
# A feature whose training standard deviation is at or below this is treated as constant and excluded
# from the distance. Null indicators for columns that had no nulls, and single-category one-hot columns,
# are exactly this: constant, and therefore contributing a zero row and column to the covariance.
CONSTANT_FEATURE_TOLERANCE = 1e-12
# Ridge added to the covariance diagonal, as a fraction of the average variance so it is scale-free.
DEFAULT_RIDGE = 1e-6
# Fallback expected anomaly rate, matching AnomalyEngine.train's expected_anomaly_rate default. Only
# used if contamination somehow reached this point unset; production fills it in before training.
DEFAULT_CONTAMINATION = 0.02


class MahalanobisDetector(BaseEstimator, OutlierMixin):
    """Squared-Mahalanobis outlier detector with exact per-feature attribution.

    Implements the scikit-learn outlier-detector contract in full — ``fit``, ``score_samples``,
    ``decision_function``, ``predict`` — because DQX depends on all of it: scoring calls
    ``-model.score_samples(X)`` (so **higher must mean more normal**, matching IsolationForest), and
    ``core.score_with_model`` calls ``predict`` on the training path, not only MLflow's signature
    inference.

    Args:
        contamination: Expected fraction of anomalies, used only to place ``offset_`` so that
            ``predict`` labels roughly that fraction as outliers. Does not affect ``score_samples``
            and therefore does not affect ranking or DQX's severity calibration.
        ridge: Ridge added to the covariance diagonal as a fraction of the average variance.
    """

    def __init__(self, contamination: float = 0.02, ridge: float = DEFAULT_RIDGE) -> None:
        # sklearn convention: __init__ only stores arguments, never validates or transforms them, so
        # that get_params/set_params/clone round-trip exactly.
        self.contamination = contamination
        self.ridge = ridge

    def fit(self, X: np.ndarray, y: object = None) -> "MahalanobisDetector":
        """Estimate the centre, the regularised covariance and its Cholesky factor.

        Args:
            X: Training features, shape ``(n_samples, n_features)``.
            y: Ignored; present for scikit-learn compatibility.
        """
        del y
        data = np.asarray(X, dtype=float)
        if data.ndim != 2:
            raise ValueError(f"expected a 2-D feature matrix, got shape {data.shape}")
        n_samples, n_features = data.shape

        self.location_ = data.mean(axis=0)
        spread = data.std(axis=0)
        # Constant features are excluded rather than divided by: dividing by ~0 would turn any change
        # at scoring time into an astronomical distance. Their contribution is reported as 0.0 so that
        # the emitted feature list still matches engineered_feature_names exactly.
        self.active_ = spread > CONSTANT_FEATURE_TOLERANCE
        self.scale_ = np.where(self.active_, spread, 1.0)
        n_active = int(self.active_.sum())
        if n_active == 0:
            raise ValueError("every feature is constant in the training data; nothing to model")

        standardised = self._standardise(data)
        self.n_features_in_ = n_features

        covariance = self._covariance(standardised, n_samples, n_active)
        # Ledoit-Wolf minimises the error of the covariance, not the conditioning of its inverse, so a
        # floor is applied either way. Scale-free: a fraction of the average variance.
        average_variance = float(np.trace(covariance)) / n_active
        covariance = covariance + np.eye(n_active) * self.ridge * average_variance

        self.cholesky_ = np.linalg.cholesky(covariance)
        # diag(Σ⁻¹) without forming Σ⁻¹: with Σ = L Lᵀ, (Σ⁻¹)ᵢᵢ is the squared norm of column i of L⁻¹.
        inverse_factor = np.linalg.solve(self.cholesky_, np.eye(n_active))
        self.precision_diagonal_ = np.sum(inverse_factor**2, axis=0)

        # offset_ places the predict/decision_function boundary, mirroring IsolationForest: the
        # contamination-th percentile of the training scores.
        training_scores = self.score_samples(data)
        self.offset_ = float(np.percentile(training_scores, 100.0 * self.contamination))
        return self

    def _covariance(self, standardised: np.ndarray, n_samples: int, n_active: int) -> np.ndarray:
        """Empirical covariance, or Ledoit-Wolf shrinkage when the sample is too small to trust one."""
        if n_samples <= n_active:
            raise ValueError(
                f"cannot fit a correlation-aware detector on {n_samples} rows with {n_active} "
                "informative features: the covariance is singular. Provide more training rows, or "
                "reduce the feature count."
            )
        if n_samples < SMALL_SAMPLE_ROWS_PER_FEATURE * n_active:
            logger.warning(
                f"Only {n_samples} training rows for {n_active} features "
                f"(<{SMALL_SAMPLE_ROWS_PER_FEATURE} per feature): using Ledoit-Wolf shrinkage, "
                "which is more stable but less sharp. More training data would detect better."
            )
            self.used_shrinkage_ = True
            return np.atleast_2d(LedoitWolf(assume_centered=False).fit(standardised).covariance_)
        self.used_shrinkage_ = False
        centred = standardised - standardised.mean(axis=0)
        return np.atleast_2d(np.cov(centred, rowvar=False))

    def _standardise(self, data: np.ndarray) -> np.ndarray:
        """Centre and scale, keeping only the features that varied during training."""
        return ((data - self.location_) / self.scale_)[:, self.active_]

    def _whitened(self, X: np.ndarray) -> np.ndarray:
        """``L⁻¹ z`` for each row, whose squared norm is the squared Mahalanobis distance."""
        standardised = self._standardise(np.asarray(X, dtype=float))
        # numpy's general solve rather than a triangular one: scipy is not a declared dependency of
        # DQX, and at the feature counts involved (p <= 50) the difference is unmeasurable.
        return np.linalg.solve(self.cholesky_, standardised.T).T

    def mahalanobis_squared(self, X: np.ndarray) -> np.ndarray:
        """Squared Mahalanobis distance per row. Higher means more unusual."""
        return np.sum(self._whitened(X) ** 2, axis=1)

    def score_samples(self, X: np.ndarray) -> np.ndarray:
        """Anomaly score per row, **higher meaning more normal**.

        The sign matters: DQX negates this (``-model.score_samples(X)``) exactly as it does for
        IsolationForest, so returning the negated distance is what makes the two interchangeable
        everywhere downstream, including severity calibration.
        """
        return -self.mahalanobis_squared(X)

    def decision_function(self, X: np.ndarray) -> np.ndarray:
        """``score_samples`` shifted so that negative means outlier, as scikit-learn expects."""
        return self.score_samples(X) - self.offset_

    def predict(self, X: np.ndarray) -> np.ndarray:
        """``-1`` for outliers and ``1`` for inliers.

        Required beyond MLflow's signature inference: ``core.score_with_model`` calls this and maps
        ``-1`` to a flag, so anything other than the sklearn convention would silently mislabel rows.
        """
        return np.where(self.decision_function(X) < 0, -1, 1)

    def feature_contributions(self, X: np.ndarray) -> np.ndarray:
        """Per-feature contribution to each row's distance: **non-negative**, leave-one-out.

        ``aᵢ = zᵢ² / (Σ⁻¹)ᵢᵢ`` where ``z = Σ⁻¹(x−μ)``. By the Schur-complement identity this is exactly
        the drop in squared distance from marginalising feature *i* out — "how much of the anomaly
        disappears if we stop looking at this feature" — so it is always ``>= 0`` and independent of the
        order features are considered in.

        The tempting alternative, the exactly-additive ``cᵢ = (x−μ)ᵢ·zᵢ`` with ``Σᵢ cᵢ = d²``, is
        **wrong for this pipeline**: its terms can be negative when features are correlated. With
        ``Σ = [[1, 0.9], [0.9, 1]]`` and ``x−μ = (1.0, 0.5)`` it gives ``(2.895, −1.053)`` — the second
        feature *reduced* the distance. Every consumer downstream takes ``abs()`` and renormalises
        (``explainability.format_shap_contributions``, ``_pattern_spark_expr``,
        ``_format_contributions_sql``), so that term would be presented to an LLM as a 27% *driver* of
        the anomaly and written into a narrative. Additivity buys nothing here, because nothing
        downstream consumes it; non-negativity is what correctness requires.

        Constant-in-training features are excluded from the distance and reported as ``0.0``, so the
        returned width always matches the trained feature count and therefore
        ``engineered_feature_names``.

        Returns:
            Array of shape ``(n_samples, n_features_in_)``, non-negative.
        """
        whitened = self._whitened(X)
        # z = Σ⁻¹(x−μ) = L⁻ᵀ(L⁻¹ z̃), reusing the whitened rows rather than forming Σ⁻¹.
        precision_delta = np.linalg.solve(self.cholesky_.T, whitened.T).T
        active_contributions = precision_delta**2 / self.precision_diagonal_

        contributions = np.zeros((active_contributions.shape[0], self.n_features_in_), dtype=float)
        contributions[:, self.active_] = active_contributions
        return contributions


# cloudpickle serialises classes **by reference** by default, which would make any pickled payload
# containing this estimator require ``databricks.labs.dqx`` to be importable on every executor.
# Contributions-disabled scoring deliberately needs nothing but sklearn and numpy on the workers today
# — the ai_query explainer's docstring records that as a design goal — so this module is registered by
# value and the class travels inside the pickle instead.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def fit_mahalanobis_model(train_pandas: pd.DataFrame, params: AnomalyParams) -> tuple[Pipeline, dict[str, Any]]:
    """Fit the detector on pre-engineered pandas features, wrapped as DQX wraps every model.

    Deliberately a sibling of ``core.fit_sklearn_model`` rather than a branch inside it: that keeps the
    IsolationForest fit function literally untouched, so "the tabular path is unchanged" is a fact a
    reviewer reads off the diff rather than a claim to verify.

    The pipeline is single-step, matching the IsolationForest one, because standardisation lives inside
    the estimator — so ``named_steps["model"]`` resolves identically for both algorithms.

    *contamination* is read from ``algorithm_config``, which is where
    ``training_service.apply_expected_anomaly_rate_if_default_contamination`` puts
    *expected_anomaly_rate*. Reusing that field rather than adding a parallel one keeps one source of
    truth for "how many anomalies do we expect"; the genuinely IsolationForest-specific fields beside it
    (tree count, subsampling) are simply not read here.
    """
    algo_cfg = params.algorithm_config or IsolationForestConfig()
    contamination = algo_cfg.contamination if algo_cfg.contamination else DEFAULT_CONTAMINATION

    detector = MahalanobisDetector(contamination=contamination, ridge=DEFAULT_RIDGE)
    pipeline = Pipeline([("model", detector)])
    pipeline.fit(train_pandas)

    hyperparams: dict[str, Any] = {
        "contamination": contamination,
        "ridge": DEFAULT_RIDGE,
        "covariance": "ledoit_wolf" if detector.used_shrinkage_ else "empirical",
        "informative_features": int(detector.active_.sum()),
        "feature_scaling": "standardised_internally",
    }
    return pipeline, hyperparams
