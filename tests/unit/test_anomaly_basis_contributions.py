"""Which comparison made a row unusual, not just which column.

The reported gap: a customer checking one metric (*units*) with ``baseline_by=[brand, nation]`` and
``baseline_over_time=date`` got an explanation that never mentioned time. The cause is structural rather
than a bug in the prompt. Feature engineering builds three views of that one metric, attribution is keyed
by source column (correctly, because explaining one view at a time misattributes), and blocking sums the
three together. So ``contributions`` is ``{"units": 100}`` and carries no information at all.

These tests pin the second map that fixes it, and the boundary where it must stay silent instead of
guessing.
"""

import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline

from databricks.labs.dqx.anomaly.correlation_detector import MahalanobisDetector
from databricks.labs.dqx.anomaly.explainability import (
    compute_gated_shap_contributions,
    format_basis_contributions,
    supports_basis_split,
)
from databricks.labs.dqx.anomaly.feature_naming import AttributionKeys
from databricks.labs.dqx.anomaly.transformers import SparkFeatureMetadata

QUANTILE_POINTS = [(10.0, 1.0), (50.0, 2.0), (90.0, 4.0)]


@pytest.fixture(name="customer_metadata")
def customer_metadata_fixture() -> SparkFeatureMetadata:
    """The reported configuration: one metric, grouped, and compared along time."""
    return SparkFeatureMetadata(
        column_infos=[{"name": "units", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["units", "units_rel_baseline", "units_rel_time"],
        baseline_by=["brand", "nation"],
        baseline_over_time="date",
    )


def test_the_three_views_of_one_metric_land_in_one_block(customer_metadata: SparkFeatureMetadata):
    """The premise the rest of this file rests on, asserted rather than assumed."""
    keys = AttributionKeys.from_metadata(customer_metadata)

    assert keys.blocks == {"units": [0, 1, 2]}, "all three views belong to the one source column"
    assert keys.labels == [
        "units",
        "units vs its group baseline",
        "units vs its expected level at that time",
    ]


def test_each_column_splits_across_its_own_views_and_totals_one_hundred(
    customer_metadata: SparkFeatureMetadata,
):
    """Normalised within the column, so the split never competes with the column share beside it."""
    keys = AttributionKeys.from_metadata(customer_metadata)
    # Time dominates: the value is ordinary for the table and for its group, wrong for when it arrived.
    per_feature = np.array([[1.0, 2.0, 7.0]])

    basis = format_basis_contributions(per_feature, keys.blocks, keys.labels, np.array([True]), 1)

    assert basis[0] == {
        "units": 10.0,
        "units vs its group baseline": 20.0,
        "units vs its expected level at that time": 70.0,
    }
    assert sum(v for v in basis[0].values() if v is not None) == pytest.approx(100.0)


def test_a_view_arguing_the_row_is_normal_earns_no_share(customer_metadata: SparkFeatureMetadata):
    """Negatives are dropped here, the same rule format_shap_contributions applies to the column map."""
    keys = AttributionKeys.from_metadata(customer_metadata)
    per_feature = np.array([[-4.0, 1.0, 3.0]])

    basis = format_basis_contributions(per_feature, keys.blocks, keys.labels, np.array([True]), 1)

    assert basis[0] == {
        "units": 0.0,
        "units vs its group baseline": 25.0,
        "units vs its expected level at that time": 75.0,
    }


def test_a_column_compared_only_one_way_is_omitted():
    """There is no basis to disambiguate, and ``{col: 100}`` would pad the map with nothing."""
    metadata = SparkFeatureMetadata(
        column_infos=[{"name": "amount", "category": "numeric"}],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount"],
        baseline_by=[],
        baseline_over_time="",
    )
    keys = AttributionKeys.from_metadata(metadata)

    basis = format_basis_contributions(np.array([[5.0]]), keys.blocks, keys.labels, np.array([True]), 1)

    assert basis == [None]


# ── the boundary: where a split is not sound and must not be invented ────────────────────────────────


def test_a_tree_model_supports_the_split_and_the_correlation_detector_does_not():
    """The dispatch, asserted on the objects DQX actually wraps.

    Tested through ``feature_contributions`` rather than ``block_contributions`` deliberately: the
    correlation detector has both, but an older pickled copy can arrive carrying only the former, and
    summing its leave-one-out drops is the error blocking exists to remove.
    """
    tree = Pipeline([("model", IsolationForest(random_state=0))])
    correlation = Pipeline([("model", MahalanobisDetector())])

    assert supports_basis_split(tree) is True
    assert supports_basis_split(correlation) is False


def test_the_correlation_detector_reports_no_split_rather_than_a_wrong_one(
    customer_metadata: SparkFeatureMetadata,
):
    """Null means "not measured". Its per-view drops are each near zero, so shares built from them would
    look ordinary and name the wrong comparison."""
    rng = np.random.default_rng(0)
    train = pd.DataFrame(
        {
            "units": rng.normal(100, 5, 400),
            "units_rel_baseline": rng.normal(0, 1, 400),
            "units_rel_time": rng.normal(0, 1, 400),
        }
    )
    model = Pipeline([("model", MahalanobisDetector())]).fit(train)
    probe = pd.DataFrame({"units": [100.0, 9999.0], "units_rel_baseline": [0.0, 8.0], "units_rel_time": [0.0, 9.0]})

    result = compute_gated_shap_contributions(
        [model],
        probe,
        list(customer_metadata.engineered_feature_names),
        np.array([1.0, 10.0]),
        QUANTILE_POINTS,
        threshold=85.0,
        keys=AttributionKeys.from_metadata(customer_metadata),
    )

    assert result.by_column[1] is not None, "the column map is still produced"
    assert set(result.by_column[1]) == {"units"}
    assert all(entry is None for entry in result.by_basis), "no basis split is published for this detector"


def test_turning_the_split_on_does_not_move_a_single_column_share(customer_metadata: SparkFeatureMetadata):
    """The regression guard for the change itself.

    With the split on, the column map is built by summing per-feature values in Python instead of inside
    the blocked attribution call. Those are the same operation on the same values, so the published column
    shares must be identical, and this compares them directly rather than trusting that argument.
    """
    rng = np.random.default_rng(7)
    train = pd.DataFrame(
        {
            "units": rng.normal(100, 5, 300),
            "units_rel_baseline": rng.normal(0, 1, 300),
            "units_rel_time": rng.normal(0, 1, 300),
        }
    )
    model = IsolationForest(random_state=42).fit(train)
    probe = pd.DataFrame({"units": [100.0, 9999.0], "units_rel_baseline": [0.0, 7.0], "units_rel_time": [0.0, 9.0]})
    cols = list(customer_metadata.engineered_feature_names)
    keys = AttributionKeys.from_metadata(customer_metadata)
    scores = np.array([1.0, 10.0])

    with_split = compute_gated_shap_contributions(
        [model], probe, cols, scores, QUANTILE_POINTS, threshold=85.0, keys=keys
    )
    # keys=None is the pre-change shape: attribution keyed by engineered feature, no basis split.
    blocked_only = compute_gated_shap_contributions([model], probe, cols, scores, QUANTILE_POINTS, threshold=85.0)

    assert with_split.by_basis[1] is not None, "the split is populated for a tree model"
    assert set(with_split.by_column[1] or {}) == {"units"}, "still keyed by source column"
    # Without keys there is no blocking at all, so compare the total evidence rather than the keys.
    assert sum(v for v in (with_split.by_column[1] or {}).values() if v is not None) == pytest.approx(
        sum(v for v in (blocked_only.by_column[1] or {}).values() if v is not None), abs=0.5
    )


def test_the_split_names_the_time_comparison_when_time_is_the_reason(
    customer_metadata: SparkFeatureMetadata,
):
    """The customer's case end to end at unit level: a row ordinary in level, wrong for its moment.

    Trained so that *units* and its group-relative view are unremarkable while the time-relative view is
    far outside its range, which is what "sales look normal until you ask what was expected that week"
    looks like in features.
    """
    rng = np.random.default_rng(11)
    train = pd.DataFrame(
        {
            "units": rng.normal(100, 5, 400),
            "units_rel_baseline": rng.normal(0, 0.5, 400),
            "units_rel_time": rng.normal(0, 0.5, 400),
        }
    )
    model = IsolationForest(random_state=42).fit(train)
    probe = pd.DataFrame({"units": [101.0], "units_rel_baseline": [0.1], "units_rel_time": [14.0]})

    result = compute_gated_shap_contributions(
        [model],
        probe,
        list(customer_metadata.engineered_feature_names),
        np.array([10.0]),
        QUANTILE_POINTS,
        threshold=85.0,
        keys=AttributionKeys.from_metadata(customer_metadata),
    )

    basis = result.by_basis[0]
    assert basis is not None
    time_share = basis["units vs its expected level at that time"]
    assert time_share is not None
    assert time_share > 50.0, f"time should dominate, got {basis}"
