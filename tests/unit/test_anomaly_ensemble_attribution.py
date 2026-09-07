"""An ensemble's explanation must describe the aggregate it scored with (no Spark, no workspace).

The score an ensemble reports is the mean over its members, and so is *anomaly_score_std*. The
explanation used to come from ``models[0]``, which is not a representative member -- members differ only
by random seed, so member zero is just the one that trained first. They also disagree far more than that
framing suggests: measured on 200 rows with the default three members, the least-agreeing pair named a
different top driver on 130 of them and the closest pair still differed on 92.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest

from databricks.labs.dqx.anomaly.explainability import (
    compute_gated_shap_contributions,
    compute_row_attributions,
    format_shap_contributions,
    mean_row_attributions,
)
from databricks.labs.dqx.errors import InvalidParameterError

_COLUMNS = ["amount", "quantity", "discount"]
_QUANTILE_POINTS = [(50.0, 0.4), (90.0, 0.5), (95.0, 0.55), (99.0, 0.6)]


@pytest.fixture
def members() -> list[IsolationForest]:
    """Three members differing only by seed, mirroring how *ensemble_training* builds them."""
    rng = np.random.default_rng(11)
    train = pd.DataFrame(rng.normal(0, 1, (500, 3)), columns=_COLUMNS)
    return [IsolationForest(n_estimators=120, random_state=42 + i).fit(train) for i in range(3)]


@pytest.fixture
def probe() -> pd.DataFrame:
    """Rows spanning ordinary to clearly anomalous, so the gate has something to include and exclude."""
    return pd.DataFrame(
        [[0.2, -0.1, 0.3], [7.0, 0.2, -0.4], [-6.5, 5.0, 0.1]],
        columns=_COLUMNS,
    )


def _map_for(attribution: np.ndarray, valid: np.ndarray, keys: list[str]) -> list[dict[str, float | None]]:
    return list(format_shap_contributions(attribution, valid, len(attribution), keys))


def test_the_attribution_is_the_mean_over_members_not_the_first_one(
    members: list[IsolationForest], probe: pd.DataFrame
):
    """The defect: scored by a committee, explained by whichever member trained first.

    Built by averaging the per-member attributions independently, so this asserts the aggregate rather
    than re-deriving whatever the implementation happens to do.
    """
    per_member = [compute_row_attributions(model, probe, _COLUMNS)[0] for model in members]
    expected = np.mean(np.stack(per_member), axis=0)

    attribution, _, keys = mean_row_attributions(members, probe, _COLUMNS)

    assert keys == _COLUMNS
    np.testing.assert_allclose(attribution, expected, rtol=1e-12)
    # And it is genuinely not member zero's, or the test would pass without the fix.
    assert not np.allclose(attribution, per_member[0])


def test_averaging_happens_before_the_clip_so_a_disagreeing_member_can_cancel(
    members: list[IsolationForest], probe: pd.DataFrame
):
    """Why the orientation must not clip: clip-then-mean and mean-then-clip are different numbers.

    Averaging is only an exact decomposition of the mean path length while the negatives survive. If a
    member decides a feature made the row look *normal*, that has to pull the mean down rather than be
    read as zero -- two members at +4 and -2 average to 1, but clipping first averages to 2.

    Pinned by asserting the intermediate is still signed, which is the property clip-then-mean destroys.
    """
    attribution, _, _ = mean_row_attributions(members, probe, _COLUMNS)

    assert (attribution < 0).any(), "the averaged attribution should still carry the normalising side"


def test_member_order_does_not_change_the_explanation(members: list[IsolationForest], probe: pd.DataFrame):
    """Member order is an accident of training, so it must not reach the output.

    Compared with a tolerance, not exactly: floating-point addition is not associative, so summing three
    members in a different order gives a bit-different answer. Asserting bit equality here would be a
    statement about float arithmetic rather than about the aggregate, and it would fail.
    """
    original, _, keys = mean_row_attributions(members, probe, _COLUMNS)
    reordered, _, reordered_keys = mean_row_attributions([members[2], members[0], members[1]], probe, _COLUMNS)

    assert reordered_keys == keys
    np.testing.assert_allclose(reordered, original, rtol=1e-9)


def test_every_member_influences_the_result(probe: pd.DataFrame):
    """The behavioural statement of the fix, which survives a refactor that the arithmetic test would not.

    Two members are fitted so that each one attributes the same row predominantly to a *different*
    feature. Explaining either alone names only that member's favourite; the aggregate has to reflect
    both, because both took part in the score.
    """
    rng = np.random.default_rng(3)
    rows = 500
    tight_amount = pd.DataFrame(
        np.column_stack([rng.normal(0, 0.05, rows), rng.normal(0, 3.0, rows), rng.normal(0, 1, rows)]), columns=_COLUMNS
    )
    tight_quantity = pd.DataFrame(
        np.column_stack([rng.normal(0, 3.0, rows), rng.normal(0, 0.05, rows), rng.normal(0, 1, rows)]), columns=_COLUMNS
    )
    one = IsolationForest(n_estimators=200, random_state=0).fit(tight_amount)
    two = IsolationForest(n_estimators=200, random_state=0).fit(tight_quantity)
    row = pd.DataFrame([[1.0, 1.0, 0.0]], columns=_COLUMNS)

    top_one = _map_for(*compute_row_attributions(one, row, _COLUMNS)[:2], _COLUMNS)[0]
    top_two = _map_for(*compute_row_attributions(two, row, _COLUMNS)[:2], _COLUMNS)[0]
    combined = _map_for(*mean_row_attributions([one, two], row, _COLUMNS)[:2], _COLUMNS)[0]

    named_one = max(top_one, key=lambda k: top_one[k] or 0.0)
    named_two = max(top_two, key=lambda k: top_two[k] or 0.0)
    assert named_one != named_two, "fixture must produce members that disagree, or this proves nothing"

    # The aggregate credits both members' features, rather than only one of them.
    assert (combined[named_one] or 0.0) > 5.0
    assert (combined[named_two] or 0.0) > 5.0


def test_a_single_member_is_left_exactly_as_the_single_model_path(members: list[IsolationForest], probe: pd.DataFrame):
    """The single-model path must be bit-identical: no averaging, no division by one, no stacking."""
    expected, expected_valid, expected_keys = compute_row_attributions(members[0], probe, _COLUMNS)
    actual, actual_valid, actual_keys = mean_row_attributions([members[0]], probe, _COLUMNS)

    assert actual_keys == expected_keys
    assert np.array_equal(actual_valid, expected_valid)
    assert np.array_equal(actual, expected)


def test_the_gated_entry_point_takes_every_member(members: list[IsolationForest], probe: pd.DataFrame):
    """The seam the scorers actually call, so the fix is wired and not merely available.

    Checks the plumbing and the gate, not the aggregate: whether the numbers are the mean is asserted
    directly on the attribution above, which is the level where it cannot be masked by clipping,
    normalising and rounding. On a row with one overwhelming driver every member agrees anyway, so a map
    comparison here would prove nothing and would fail for the wrong reason.
    """
    scores = np.array([0.40, 0.62, 0.61])

    contributions = compute_gated_shap_contributions(members, probe, _COLUMNS, scores, _QUANTILE_POINTS, threshold=95.0)

    assert contributions[0] is None, "an ordinary row should not be attributed at all"
    for anomalous in contributions[1:]:
        assert anomalous is not None
        assert sum(v for v in anomalous.values() if v is not None) == pytest.approx(100.0, abs=0.5)
        assert min(v for v in anomalous.values() if v is not None) >= 0.0


# ── failures that must be loud ───────────────────────────────────────────────────────────────────────


class _WrongWidthEstimator:
    """An estimator whose attribution has the wrong number of columns.

    A real second IsolationForest cannot produce this -- sklearn would raise inside ``score_samples``
    first -- so a stand-in is the only way to reach the guard.
    """

    def feature_contributions(self, rows: np.ndarray) -> np.ndarray:
        return np.ones((len(rows), 1))


def test_members_whose_attributions_cannot_be_aligned_are_rejected(members: list[IsolationForest], probe: pd.DataFrame):
    """Averaging misaligned columns would blend different features into one confident, wrong answer.

    That is indistinguishable from correct output downstream -- exactly the failure mode this whole
    function exists to remove -- so it raises rather than quietly falling back to one member.
    """
    with pytest.raises(InvalidParameterError, match="cannot be averaged"):
        mean_row_attributions([members[0], _WrongWidthEstimator()], probe, _COLUMNS)


def test_no_models_is_rejected(probe: pd.DataFrame):
    """No silent empty map and no division by zero."""
    with pytest.raises(InvalidParameterError, match="At least one model"):
        mean_row_attributions([], probe, _COLUMNS)
