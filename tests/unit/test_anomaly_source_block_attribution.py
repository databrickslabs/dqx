"""Attribution keyed by the column a reader passed, not by the views feature engineering made of it.

DQX gives one numeric column up to three engineered views: the metric, its deviation from its group's
baseline, and its deviation from its expected level at that time. Explaining views one at a time answers
a question nobody asked -- how much did *this view* matter -- and the answer does not add up to how much
the column mattered.

Both detectors need blocking and both get it, by different arithmetic: an exact joint marginalisation for
the correlation-aware detector, whose per-view drops are each almost nothing, and a plain sum for a tree
model, which is exact because SHAP is additive. This file covers the tree path; the correlation-aware
path is covered in test_anomaly_mahalanobis_detector.py.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest

from databricks.labs.dqx.anomaly.explainability import compute_row_attributions, format_shap_contributions
from databricks.labs.dqx.anomaly.correlation_detector import MahalanobisDetector
from databricks.labs.dqx.errors import InvalidParameterError


def _shares(model: IsolationForest, row: pd.DataFrame, columns: list[str], blocks=None) -> dict[str, float]:
    attribution, valid, keys = compute_row_attributions(model, row, columns, blocks)
    emitted = format_shap_contributions(attribution, valid, 1, keys)[0]
    return {key: value for key, value in emitted.items() if value is not None}


def test_a_single_view_per_column_is_left_exactly_as_the_unblocked_map():
    """Blocking generalises the per-feature map; it must not redefine it.

    With one view per column every block is a singleton, so the sum over each block is the value itself.
    Asserted rather than argued, because otherwise every model without derived features would silently
    change its explanations.
    """
    rng = np.random.default_rng(5)
    columns = ["amount", "quantity"]
    train = pd.DataFrame(rng.normal(0, 1, (500, 2)), columns=columns)
    model = IsolationForest(n_estimators=150, random_state=0).fit(train)
    row = pd.DataFrame([[6.0, 0.3]], columns=columns)

    unblocked = _shares(model, row, columns)
    blocked = _shares(model, row, columns, {"amount": [0], "quantity": [1]})

    assert blocked == unblocked


def test_the_views_of_one_column_are_summed_into_that_column():
    """The defect: one column's evidence split across its views, so the column understates itself.

    A constant temporal expectation makes ``amount_rel_time`` an affine duplicate of *amount*. The tree
    model then splits the evidence roughly in half between them -- measured at 49.3% and 50.7% where the
    same data with a single view attributes 100% to the column. Summing the block recovers it exactly.
    """
    rng = np.random.default_rng(0)
    rows = 3000
    amount = rng.normal(0, 1, rows)
    quantity = rng.normal(0, 1, rows)

    single_view = pd.DataFrame({"amount": amount, "quantity": quantity})
    undisturbed = IsolationForest(n_estimators=200, random_state=0).fit(single_view)
    truth = _shares(undisturbed, pd.DataFrame([[8.0, 0.5]], columns=["amount", "quantity"]), ["amount", "quantity"])

    columns = ["amount", "amount_rel_time", "quantity"]
    expanded = pd.DataFrame({"amount": amount, "amount_rel_time": amount - 3.0, "quantity": quantity})
    model = IsolationForest(n_estimators=200, random_state=0).fit(expanded)
    row = pd.DataFrame([[8.0, 5.0, 0.5]], columns=columns)

    per_view = _shares(model, row, columns)
    # Pinned so the fix's absence is not mistaken for the test being vacuous.
    assert per_view["amount"] < 70.0, f"expected the per-view form to understate amount, got {per_view}"

    blocked = _shares(model, row, columns, {"amount": [0, 1], "quantity": [2]})

    assert blocked["amount"] == pytest.approx(truth["amount"], abs=1.0)
    assert set(blocked) == {"amount", "quantity"}


def test_splitting_a_columns_evidence_can_hand_the_top_spot_to_a_derived_view():
    """Why this is material rather than cosmetic, since a reader only ever reads the top entry.

    Three views divide a column's share three ways. Measured on a column whose true share was 74.5%, each
    view landed near 25% -- so an unrelated single-view column sat level with them, and the name the map
    put first was a *derived view* of the real driver rather than the column itself. Blocking names the
    column.
    """
    rng = np.random.default_rng(1)
    rows = 3000
    driver = rng.normal(0, 1, rows)
    other = rng.normal(0, 1, rows)
    columns = ["driver", "driver_rel_baseline", "driver_rel_time", "other"]
    train = pd.DataFrame(
        {
            "driver": driver,
            "driver_rel_baseline": driver - 3.0,
            "driver_rel_time": driver * 0.5 + 1.0,
            "other": other,
        }
    )
    model = IsolationForest(n_estimators=300, random_state=0).fit(train)
    row = pd.DataFrame([[5.0, 2.0, 3.5, 3.0]], columns=columns)

    per_view = _shares(model, row, columns)
    named_per_view = max(per_view, key=lambda k: per_view[k])

    blocks = {"driver": [0, 1, 2], "other": [3]}
    blocked = _shares(model, row, columns, blocks)
    named_blocked = max(blocked, key=lambda k: blocked[k])

    assert named_per_view != "driver", f"fixture must reproduce the dilution, got {per_view}"
    assert named_blocked == "driver", f"blocking should name the source column, got {blocked}"


def test_a_block_sums_signed_values_so_a_normalising_view_cancels():
    """The reason the orientation must not clip before blocking.

    A block's value is the net effect of its views on the path length. If one view drove the anomaly and
    another argued the row was ordinary, the second has to reduce the first. Clipping the views before
    summing would report the block as more responsible than the model found it -- two views at +3 and -1
    net to 2, but clipping first sums to 3.
    """
    rng = np.random.default_rng(9)
    rows = 2000
    columns = ["metric", "metric_rel_time", "other"]
    train = pd.DataFrame(rng.normal(0, 1, (rows, 3)), columns=columns)
    model = IsolationForest(n_estimators=200, random_state=0).fit(train)
    row = pd.DataFrame([[4.0, -3.0, 0.2]], columns=columns)

    per_view, _, _ = compute_row_attributions(model, row, columns)
    blocked, _, keys = compute_row_attributions(model, row, columns, {"metric": [0, 1], "other": [2]})

    assert keys == ["metric", "other"]
    # The block is the signed sum, not the sum of the clipped parts.
    np.testing.assert_allclose(blocked[0, 0], per_view[0, 0] + per_view[0, 1], rtol=1e-12)
    clipped_sum = float(np.maximum(per_view[0, :2], 0.0).sum())
    if per_view[0, :2].min() < 0:
        assert blocked[0, 0] < clipped_sum


# ── the attribution contract: shape must match the names it will be reported under ────────────────────


class _PerFeatureOnly:
    """An estimator that can attribute per feature but cannot group by source column.

    Reachable for two reasons, neither hypothetical. ``correlation_detector`` is registered with cloudpickle
    *by value*, so an older class definition travels inside a persisted model and is restored without
    methods added since; and the config hash covers only columns and the comparison bases, so such a model
    passes scoring-time validation unchanged. A third-party estimator supplied through the documented
    training-strategy extension point is the other.
    """

    def feature_contributions(self, rows: np.ndarray) -> np.ndarray:
        return np.tile(np.array([1e-5, 1e-5, 0.13]), (len(rows), 1))


_BLOCKED_COLUMNS = ["x", "x_rel_time", "y"]
_BLOCKS = {"x": [0, 1], "y": [2]}


def test_an_estimator_that_cannot_group_by_source_is_refused_rather_than_misreported():
    """The defect: per-feature width reported under source-column names, silently.

    Left alone this published ``{'x': 0.0, 'y': 0.0}`` -- nothing identified as a driver -- on a row whose
    score was identical to a correctly explained one. Both halves of that are bad: the substantial value
    landed in the normalising denominator and was then never emitted, and the two names it did emit belonged
    to different columns than the numbers behind them.

    Summing the drops instead is not an option: they are not additive, which is the whole reason source
    blocking exists for this detector.
    """
    frame = pd.DataFrame([[8.0, 5.0, 0.5]], columns=_BLOCKED_COLUMNS)

    with pytest.raises(InvalidParameterError, match="cannot group it by source column"):
        compute_row_attributions(_PerFeatureOnly(), frame, _BLOCKED_COLUMNS, _BLOCKS)


def test_the_same_estimator_is_still_attributed_when_no_blocks_are_asked_for():
    """The refusal is about the combination, not about the estimator.

    Without this the guard could be satisfied by rejecting the estimator outright, which would break a
    caller who never wanted source blocking in the first place.
    """
    frame = pd.DataFrame([[8.0, 5.0, 0.5]], columns=_BLOCKED_COLUMNS)

    attribution, valid, keys = compute_row_attributions(_PerFeatureOnly(), frame, _BLOCKED_COLUMNS)

    assert keys == _BLOCKED_COLUMNS
    assert attribution.shape == (1, 3)
    assert valid.all()


def test_an_estimator_that_can_group_by_source_is_unaffected():
    """The guard must not be satisfiable by refusing everything, so the working path is pinned beside it."""
    rng = np.random.default_rng(0)
    metric = rng.normal(0, 1, 2000)
    train = np.column_stack([metric, metric - 3.0, rng.normal(0, 1, 2000)])
    detector = MahalanobisDetector().fit(train)
    frame = pd.DataFrame([[8.0, 5.0, 0.5]], columns=_BLOCKED_COLUMNS)

    attribution, _, keys = compute_row_attributions(detector, frame, _BLOCKED_COLUMNS, _BLOCKS)

    assert keys == ["x", "y"]
    assert attribution.shape == (1, 2)


def test_the_formatter_refuses_a_matrix_that_does_not_match_its_keys():
    """Guarded twice on purpose, because this function is reachable with a hand-built matrix.

    ``compute_row_attributions`` catches the case it can diagnose, with a remedy. This catches any producer
    -- a future branch, a caller assembling values itself -- at the point where a column acquires a name.
    """
    three_wide = np.array([[1e-5, 1e-5, 0.13]])

    with pytest.raises(InvalidParameterError, match="cannot be reported under 2 keys"):
        format_shap_contributions(three_wide, np.array([True]), 1, ["x", "y"])
