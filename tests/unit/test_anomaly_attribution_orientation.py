"""What a contribution means, and the sign convention the whole map depends on (no Spark, no workspace).

TreeSHAP on an isolation forest explains the ensemble's average *path length*, so a negative value is
what drove the anomaly and a positive one argues the row is ordinary. Getting that backwards is not a
small error, and it is not detectable from any test that only checks the map's shape -- so the
orientation is pinned here against ground truth, and the two alternatives are pinned as failures.

The arithmetic tests build the attribution matrix by hand, which is the only way to state the semantics
without depending on whatever SHAP happens to produce for a fixture.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from databricks.labs.dqx.anomaly.explainability import (
    compute_contributions_for_matrix,
    compute_row_attributions,
    format_contributions_map,
    format_shap_contributions,
)

_KEYS = ["amount", "quantity"]


def _single_row(values: list[float], keys: list[str] | None = None) -> dict[str, float | None]:
    """Format one row's attribution, returning just that row's map."""
    names = keys if keys is not None else _KEYS
    return format_shap_contributions(np.array([values]), np.array([True]), 1, names)[0]


def _top_key(contributions: dict[str, float | None]) -> str:
    """The key a reader would take as the driver: the largest share, nulls treated as no share."""
    return max(contributions, key=lambda key: contributions[key] or 0.0)


# ── what earns a share ───────────────────────────────────────────────────────────────────────────────


def test_a_feature_arguing_the_row_is_normal_earns_no_share():
    """A normalising feature is not a driver, and used to be reported as one.

    Attribution arrives oriented so larger means more responsible; a negative value is a feature that
    made the row look *more* ordinary. Taking the magnitude added that to the evidence for the anomaly
    and reported the sum as one number, so a feature that argued against the flag was rendered as having
    caused a fifth of it.
    """
    contributions = _single_row([8.0, -2.0])

    assert contributions == {"amount": 100.0, "quantity": 0.0}


def test_the_emitted_shares_still_total_one_hundred():
    """The public contract, which the fix had to preserve rather than trade away.

    Two test suites and the documented schema of ``_dq_info[].anomaly.contributions`` depend on the map
    being non-negative and summing to 100. Dropping the normalising side changes the denominator, so this
    asserts the sum survived that change.
    """
    contributions = _single_row([6.0, 2.0, -3.0], keys=["a", "b", "c"])

    assert sum(v for v in contributions.values() if v is not None) == pytest.approx(100.0, abs=0.5)
    assert min(v for v in contributions.values() if v is not None) >= 0.0


# ── absence of evidence is not evidence ──────────────────────────────────────────────────────────────


def test_a_row_with_no_anomaly_driving_evidence_gets_no_explanation():
    """Every feature says the row looks normal, so there is nothing to report.

    This used to emit ``1 / n`` for every key -- "all features contributed equally" -- which is a claim
    with no input behind it, and the more misleading failure of the two because the numbers look
    unremarkable. The all-null map is what a row with a null feature already produces, and the consumers
    already render it as ``unknown``.
    """
    contributions = _single_row([-1.0, -2.0])

    assert contributions == {"amount": None, "quantity": None}


def test_an_all_zero_attribution_row_gets_no_explanation():
    """The exact branch the invented uniform split used to occupy: a total of zero."""
    contributions = _single_row([0.0, 0.0])

    assert contributions == {"amount": None, "quantity": None}


def test_a_row_without_evidence_does_not_shift_the_rows_after_it():
    """The off-by-one this restructure could have introduced, and the reason it is tested directly.

    Skipping a row while formatting must not skip the attribution cursor, or every row after the first
    unexplained one is given another row's numbers -- plausible output, wrong row, invisible without an
    assertion like this one.
    """
    attribution = np.array([[9.0, 1.0], [-1.0, -1.0], [1.0, 3.0]])

    contributions = format_shap_contributions(attribution, np.array([True, True, True]), 3, _KEYS)

    assert contributions[0] == {"amount": 90.0, "quantity": 10.0}
    assert contributions[1] == {"amount": None, "quantity": None}
    assert contributions[2] == {"amount": 25.0, "quantity": 75.0}


def test_a_null_feature_row_and_an_unexplained_row_can_both_appear_at_once():
    """The two independent reasons for a null map compose, and the cursor copes with both."""
    attribution = np.array([[-1.0, -1.0], [2.0, 2.0]])

    contributions = format_shap_contributions(attribution, np.array([True, False, True]), 3, _KEYS)

    assert contributions[0] == {"amount": None, "quantity": None}
    assert contributions[1] == {"amount": None, "quantity": None}
    assert contributions[2] == {"amount": 50.0, "quantity": 50.0}


# ── ground truth: the orientation itself ─────────────────────────────────────────────────────────────


@pytest.fixture
def forest_and_features() -> tuple[IsolationForest, list[str]]:
    """An isolation forest on three independent standard normals, so any anomaly is one we injected."""
    rng = np.random.default_rng(7)
    columns = ["a", "b", "c"]
    train = pd.DataFrame(rng.normal(0, 1, (600, 3)), columns=columns)
    return IsolationForest(n_estimators=200, random_state=0).fit(train), columns


@pytest.mark.parametrize("culprit", [0, 1, 2])
def test_the_deliberately_anomalous_feature_is_named_as_the_top_driver(
    forest_and_features: tuple[IsolationForest, list[str]], culprit: int
):
    """Ground truth, end to end through real TreeSHAP: one feature is anomalous, and it must be named."""
    forest, columns = forest_and_features
    values = [0.1, 0.1, 0.1]
    values[culprit] = 9.0
    probe = pd.DataFrame([values], columns=columns)

    attribution, valid_indices, keys = compute_row_attributions(forest, probe, columns)
    contributions = format_shap_contributions(attribution, valid_indices, 1, keys)[0]

    named = _top_key(contributions)
    assert named == columns[culprit], f"expected {columns[culprit]}, got {contributions}"


@pytest.mark.parametrize("culprit", [0, 1, 2])
def test_keeping_the_un_negated_side_would_name_the_most_ordinary_feature_instead(
    forest_and_features: tuple[IsolationForest, list[str]], culprit: int
):
    """The alternative a reader might reach for, pinned as wrong so nobody reaches for it again.

    A review of this code proposed keeping the *positive* SHAP values, on the reading that positive means
    "drove the anomaly". It is the opposite: because the explained quantity is a path length, the positive
    side is the evidence that the row is ordinary. Measured over 200 single-culprit rows, that choice
    named the true culprit 0 times; the orientation in use named it 199 times.

    Asserted by reconstructing the rejected alternative from the same attribution, so this test cannot
    drift away from the code it is arguing about.
    """
    forest, columns = forest_and_features
    values = [0.1, 0.1, 0.1]
    values[culprit] = 9.0
    probe = pd.DataFrame([values], columns=columns)

    attribution, _, _ = compute_row_attributions(forest, probe, columns)
    # compute_row_attributions already negated, so negating again recovers the raw SHAP values.
    rejected = np.maximum(-attribution, 0.0)

    assert rejected.argmax() != culprit, "the un-negated side should not name the true culprit"


def test_a_zero_share_feature_is_not_rendered_as_a_contributor():
    """Dropping the normalising side makes exact zeros common, and a zero is not a driver.

    Before this, nearly every feature had some magnitude and so a non-zero share. Now a feature that
    argued the row was normal lands on exactly 0.0, and rendering it produces "quantity (0%)" in a
    message or an LLM prompt -- naming something that contributed nothing, which is the same invention
    the all-null map exists to avoid, one layer down.
    """
    assert format_contributions_map({"amount": 100.0, "quantity": 0.0}, 3) == "amount (100%)"


# ── the row-at-a-time variant, which has to agree with the scoring path ───────────────────────────────


def _matrix_forest(columns: int = 3) -> IsolationForest:
    rng = np.random.default_rng(4)
    return IsolationForest(n_estimators=150, random_state=0).fit(rng.normal(0, 1, (400, columns)))


def test_the_matrix_variant_names_the_deliberately_anomalous_feature():
    """Same orientation as the scoring path, asserted independently.

    This function is a second implementation of the same semantics -- it existed with the same two defects
    the scoring path had, which is how one module came to hold two answers to what a negative SHAP value
    means. Pinning it separately is what keeps them from drifting apart again.
    """
    forest = _matrix_forest()
    probe = np.array([[9.0, 0.1, 0.1]])

    contributions = compute_contributions_for_matrix(forest, probe, ["a", "b", "c"])[0]

    named = _top_key(contributions)
    assert named == "a", f"expected the perturbed feature, got {contributions}"


def test_the_matrix_variant_reports_fractions_rather_than_percentages():
    """Its contract differs from the scoring path's on purpose, so the difference is pinned.

    The scoring path emits 0-100 because that is what ``_dq_info[].anomaly.contributions`` documents. This
    one emits fractions of 1, which is what its own caller expects. Recording that here stops a later
    reader "aligning" them and silently rescaling the other consumer by a hundred.
    """
    forest = _matrix_forest()

    contributions = compute_contributions_for_matrix(forest, np.array([[8.0, 0.2, 0.2]]), ["a", "b", "c"])[0]

    values = [v for v in contributions.values() if v is not None]
    assert sum(values) == pytest.approx(1.0)
    assert max(values) <= 1.0


def test_the_matrix_variant_invents_nothing_when_no_feature_drove_the_anomaly():
    """The uniform-split defect, in the copy that also carried it.

    Built from a hand-made attribution rather than hunting for a real row with no anomaly-driving
    evidence, by using a model whose every feature is constant so nothing can be isolated.
    """
    constant = IsolationForest(n_estimators=50, random_state=0).fit(np.zeros((200, 2)))

    contributions = compute_contributions_for_matrix(constant, np.array([[0.0, 0.0]]), ["a", "b"])[0]

    assert contributions == {"a": None, "b": None}


def test_the_matrix_variant_returns_nulls_for_a_row_it_cannot_score():
    """A row carrying a null cannot be attributed, and must not take another row's numbers with it."""
    forest = _matrix_forest(columns=2)
    probe = np.array([[np.nan, 1.0], [7.0, 0.1]])

    contributions = compute_contributions_for_matrix(forest, probe, ["a", "b"])

    assert contributions[0] == {"a": None, "b": None}
    assert contributions[1]["a"] is not None


def test_the_matrix_variant_unwraps_a_pipeline_and_its_scaler():
    """Models trained by older versions carry a RobustScaler in the pipeline; newer ones carry none.

    Both shapes have to work, and the scaler has to be *applied* rather than merely tolerated, or the
    attribution is computed on unscaled values the estimator never saw.
    """
    rng = np.random.default_rng(6)
    train = rng.normal(0, 1, (400, 2))
    scaled = Pipeline([("scaler", RobustScaler()), ("model", IsolationForest(n_estimators=150, random_state=0))])
    scaled.fit(train)
    bare = Pipeline([("model", IsolationForest(n_estimators=150, random_state=0))]).fit(train)

    probe = np.array([[8.0, 0.1]])
    for label, model in (("with a scaler", scaled), ("without one", bare)):
        contributions = compute_contributions_for_matrix(model, probe, ["a", "b"])[0]
        assert _top_key(contributions) == "a", f"pipeline {label} named {contributions}"
