"""Unit tests for the correlation-aware detector and its attribution (no Spark, no workspace).

The attribution tests are the most valuable ones here. A distance decomposition that is merely
*additive* would pass a naive "sums to the total" check and still feed a false claim into an LLM
narrative, so the rejected alternative is asserted explicitly rather than described in a comment.
"""

import numpy as np
import pytest
from sklearn.base import clone
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.timeseries_detector import MahalanobisDetector

# The correlated 2x2 case used throughout: rho = 0.9, so the off-diagonal precision terms are large
# enough that the signed decomposition goes negative.
_CORRELATED = np.array([[1.0, 0.9], [0.9, 1.0]])


def _sample_from(covariance: np.ndarray, n_samples: int = 4000, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.multivariate_normal(np.zeros(len(covariance)), covariance, size=n_samples)


@pytest.fixture
def fitted() -> MahalanobisDetector:
    """Fitted on a well-conditioned correlated sample, so the empirical covariance path is used."""
    return MahalanobisDetector(ridge=0.0).fit(_sample_from(_CORRELATED))


# ---------------------------------------------------------------------------
# The sklearn outlier contract, which DQX depends on in full
# ---------------------------------------------------------------------------


def test_score_samples_is_higher_for_more_normal_rows(fitted: MahalanobisDetector):
    """The sign convention is load-bearing: DQX computes ``-model.score_samples(X)``, so this must
    match IsolationForest's orientation or every score downstream is inverted."""
    normal, extreme = np.array([[0.0, 0.0]]), np.array([[6.0, -6.0]])

    assert fitted.score_samples(normal)[0] > fitted.score_samples(extreme)[0]
    assert fitted.score_samples(normal)[0] <= 0.0  # negated distance, so never positive


def test_predict_returns_the_sklearn_outlier_labels(fitted: MahalanobisDetector):
    """``core.score_with_model`` maps -1 to a flag, so any other encoding silently mislabels rows."""
    labels = fitted.predict(np.array([[0.0, 0.0], [6.0, -6.0]]))

    assert set(np.unique(labels)).issubset({-1, 1})
    assert labels[0] == 1 and labels[1] == -1


def test_decision_function_is_negative_exactly_for_outliers(fitted: MahalanobisDetector):
    points = np.array([[0.0, 0.0], [6.0, -6.0]])

    decisions = fitted.decision_function(points)
    assert np.array_equal(np.where(decisions < 0, -1, 1), fitted.predict(points))


def test_get_params_round_trips_so_clone_and_pipeline_work(fitted: MahalanobisDetector):
    """Needed by sklearn ``Pipeline`` and by ``mlflow.sklearn``."""
    detector = MahalanobisDetector(contamination=0.07, ridge=1e-5)

    assert clone(detector).get_params() == {"contamination": 0.07, "ridge": 1e-5}


def test_works_as_the_model_step_of_a_single_step_pipeline():
    """DQX wraps the estimator as ``Pipeline([('model', ...)])`` and reaches it via
    ``named_steps['model']``; keeping the shape single-step is what makes this detector a drop-in."""
    pipeline = Pipeline([("model", MahalanobisDetector())]).fit(_sample_from(_CORRELATED))

    assert list(pipeline.named_steps) == ["model"]
    assert pipeline.score_samples(np.array([[0.0, 0.0]])).shape == (1,)


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------


def test_contributions_are_never_negative(fitted: MahalanobisDetector):
    """The property the whole choice of formula rests on: every consumer downstream takes abs() and
    renders the result as a percentage driver, so a negative term would become a false claim."""
    rng = np.random.default_rng(7)
    points = rng.normal(size=(500, 2)) * 3.0

    assert (fitted.feature_contributions(points) >= 0.0).all()


def test_contribution_equals_the_drop_from_marginalising_that_feature_out():
    """Pins the leave-one-out identity ``aᵢ = d²(all) − d²(all but i)`` against a hand-built covariance.

    Computed directly from the 2x2 case rather than through the estimator's internals, so the test
    would still fail if the implementation and the intended identity drifted apart.
    """
    detector = MahalanobisDetector(ridge=0.0).fit(_sample_from(_CORRELATED, n_samples=20000, seed=3))
    delta = np.array([[1.0, 0.5]])
    standardised = (delta - detector.location_) / detector.scale_

    contributions = detector.feature_contributions(delta)[0]

    covariance = detector.cholesky_ @ detector.cholesky_.T
    full = float(standardised @ np.linalg.inv(covariance) @ standardised.T)
    for index in (0, 1):
        kept = [j for j in range(2) if j != index]
        sub = covariance[np.ix_(kept, kept)]
        without = float(standardised[:, kept] @ np.linalg.inv(sub) @ standardised[:, kept].T)
        assert contributions[index] == pytest.approx(full - without, rel=1e-6)


def test_the_signed_additive_decomposition_would_go_negative():
    """The rejected alternative, asserted so the rejection is executable rather than a comment.

    ``cᵢ = (x−μ)ᵢ·zᵢ`` sums exactly to ``d²`` and is therefore tempting. On correlated features one
    term is negative — the feature *reduced* the distance — and since every downstream consumer takes
    ``abs()`` and renormalises, it would be reported as a substantial positive driver of the anomaly.
    """
    precision = np.linalg.inv(_CORRELATED)
    delta = np.array([1.0, 0.5])
    signed = delta * (precision @ delta)

    assert signed.sum() == pytest.approx(delta @ precision @ delta)  # exactly additive...
    assert signed.min() < 0.0  # ...and still unusable
    # The leave-one-out form on the same input is strictly positive.
    leave_one_out = (precision @ delta) ** 2 / np.diag(precision)
    assert (leave_one_out > 0.0).all()


def test_the_deviating_feature_is_ranked_first(fitted: MahalanobisDetector):
    """A single feature pushed far from its correlated partner must dominate the attribution."""
    contributions = fitted.feature_contributions(np.array([[5.0, 0.0]]))[0]

    assert contributions.argmax() == 0


def test_constant_features_are_excluded_but_still_reported():
    """Null indicators for columns that had no nulls, and single-category one-hots, are constant.

    They must not be divided by a ~zero spread, and they must still appear in the output so the width
    keeps matching ``engineered_feature_names`` — the persisted scoring contract.
    """
    varying = _sample_from(_CORRELATED)
    train = np.hstack([varying, np.full((len(varying), 1), 3.0)])

    detector = MahalanobisDetector().fit(train)
    contributions = detector.feature_contributions(np.array([[1.0, 0.5, 3.0]]))

    assert contributions.shape == (1, 3)
    assert contributions[0, 2] == 0.0
    assert np.isfinite(detector.score_samples(np.array([[1.0, 0.5, 999.0]]))).all()


def test_all_constant_training_data_is_refused():
    with pytest.raises(ValueError, match="every feature is constant"):
        MahalanobisDetector().fit(np.ones((50, 3)))


def test_more_features_than_rows_is_refused_with_an_actionable_message():
    """Silent failure here would produce a model that scores everything as wildly anomalous."""
    rng = np.random.default_rng(0)

    with pytest.raises(ValueError, match="covariance is singular"):
        MahalanobisDetector().fit(rng.normal(size=(8, 20)))


def test_small_samples_fall_back_to_shrinkage(caplog):
    """A plain ridge floor measured better on SMD, but SMD is well conditioned. Shrinkage is what
    keeps a near-square sample usable, so the fallback must actually engage and say so."""
    rng = np.random.default_rng(1)

    with caplog.at_level("WARNING"):
        detector = MahalanobisDetector().fit(rng.normal(size=(40, 8)))

    assert "Ledoit-Wolf" in caplog.text
    assert np.isfinite(detector.score_samples(rng.normal(size=(5, 8)))).all()


# ── contamination reaches predict, and nothing DQX scores with ──────────────────────────────────────


def test_contamination_moves_the_predict_boundary_but_not_the_scores():
    """Pinned because the docstrings now promise exactly this, and a plausible "fix" would break it.

    ``expected_anomaly_rate`` flows into ``contamination``, which for both shipping detectors places
    ``offset_`` and therefore only ``predict`` / ``decision_function``. Every DQX scoring path reads
    ``-score_samples`` and ranks it against the training score quantiles, so the parameter cannot change
    which rows are flagged -- the check's ``threshold`` does that.

    Asserting both halves matters. Dropping the parameter would break someone who loads the registered
    model and calls ``predict``; treating it as a detection knob is what the documentation used to imply.
    """
    rng = np.random.default_rng(7)
    data = np.vstack([rng.normal(0, 1, (200, 3)), rng.normal(6, 1, (10, 3))])

    timid = MahalanobisDetector(contamination=0.01).fit(data)
    liberal = MahalanobisDetector(contamination=0.20).fit(data)

    np.testing.assert_allclose(timid.score_samples(data), liberal.score_samples(data))
    assert timid.offset_ != liberal.offset_
    assert (liberal.predict(data) == -1).sum() > (timid.predict(data) == -1).sum()


def test_isolation_forest_scores_are_also_independent_of_contamination():
    """The default profile, for the same reason. sklearn documents it; DQX's docs now rely on it."""
    rng = np.random.default_rng(8)
    data = np.vstack([rng.normal(0, 1, (200, 3)), rng.normal(6, 1, (10, 3))])

    timid = IsolationForest(contamination=0.01, random_state=0, n_estimators=50).fit(data)
    liberal = IsolationForest(contamination=0.20, random_state=0, n_estimators=50).fit(data)

    np.testing.assert_allclose(timid.score_samples(data), liberal.score_samples(data))


# ── retaining every one-hot category, and what that means for a singular direction ───────────────────


def test_a_redundant_dummy_costs_nothing_and_an_unseen_category_scores_high():
    """Retaining every one-hot category induces an exactly collinear pair. Both halves are checked.

    A binary column encoded as both indicators satisfies ``d_a + d_b == 1`` on every trained row, so the
    covariance is singular along that direction. Two things follow, and only one of them is obvious:

    - for rows that *do* satisfy the constraint, the redundant dummy changes nothing: the score is
      identical to the same data encoded with one dummy, because the ridged pseudo-inverse gives the
      zero-variance direction no weight
    - a row that violates it -- an unseen category, encoded all-zeros -- sits off the surface every
      training row lay on, and scores enormously

    The second is deliberate rather than accidental, and it is not new: columns with three or more
    categories always retained all of them, so an unseen value has always scored this way. Truncating
    the binary case was the inconsistency, and it is what made an unexpected value in a binary column
    invisible instead.
    """
    rng = np.random.default_rng(3)
    metric = rng.normal(10.0, 1.0, 400)
    indicator = (rng.random(400) < 0.5).astype(float)

    both = MahalanobisDetector().fit(np.column_stack([metric, indicator, 1.0 - indicator]))
    one = MahalanobisDetector().fit(np.column_stack([metric, indicator]))

    on_surface = np.array([[40.0, 1.0, 0.0], [10.0, 1.0, 0.0]])
    scores_both = -both.score_samples(on_surface)
    scores_one = -one.score_samples(np.array([[40.0, 1.0], [10.0, 1.0]]))

    assert np.all(np.isfinite(scores_both))
    np.testing.assert_allclose(scores_both, scores_one, rtol=1e-6)
    assert scores_both[0] > scores_both[1]  # the extreme metric still ranks above the ordinary one

    unseen = -both.score_samples(np.array([[10.0, 0.0, 0.0]]))[0]
    known = -both.score_samples(np.array([[10.0, 1.0, 0.0]]))[0]
    assert np.isfinite(unseen)
    assert unseen > known
