"""The policy that chooses a baseline grouping from candidate dimension columns.

Pure selection logic, so it is unit-tested directly rather than through a Spark session. It exists
as a separate policy because the segmented one is wrong here: a per-segment model must train a forest
on its own rows, so it wants few groups with many rows each, while a baseline group only has to yield
a median and costs nothing extra because there is one model regardless of group count.

Applying the segmented policy to ``baseline_by`` was a measured defect. On a dataset grouped by
country x event_type x product -- 90 groups -- it selected the single lowest-cardinality column,
giving 3 groups, and conditioning barely engaged: PR-AUC 0.1302 against 0.1176 unconditioned. With
this policy the same data yields all 90 groups. See databrickslabs/dqx#1484.
"""

import pytest

from databricks.labs.dqx.anomaly.group_config import (
    MAX_BASELINE_COLUMN_CARDINALITY,
    MAX_BASELINE_GROUPS,
    MIN_ROWS_PER_BASELINE_GROUP,
)
from databricks.labs.dqx.anomaly.core import DEFAULT_SAMPLE_FRACTION, DEFAULT_TRAIN_RATIO
from databricks.labs.dqx.anomaly.profiler import effective_min_rows_per_group, select_baseline_columns


def _candidate(name: str, distinct: int, total: int) -> tuple[str, int, float]:
    """Candidates arrive as (name, distinct_count, rows_per_group), ordered by cardinality."""
    return (name, distinct, total / distinct)


def test_combines_dimensions_while_groups_stay_estimable():
    """The case the old policy got wrong: take all three, not just the cheapest."""
    total = 13500
    candidates = [
        _candidate("product", 3, total),
        _candidate("event_type", 5, total),
        _candidate("country", 6, total),
    ]

    selected = select_baseline_columns(candidates, total)

    assert selected == ["product", "event_type", "country"]
    # 3 * 5 * 6 = 90 groups, 150 rows each on the full table. Sized against the *effective* floor, not the
    # nominal one: discovery runs before sampling, so what matters is the ~36 rows per group that survive
    # sampling and the train split. This fixture held 10,800 rows when it was written, which looked like a
    # comfortable 120 rows per group and was 29 by the time anything was fitted.
    assert total / 90 >= effective_min_rows_per_group()
    assert (total / 90) * DEFAULT_SAMPLE_FRACTION * DEFAULT_TRAIN_RATIO >= MIN_ROWS_PER_BASELINE_GROUP


def test_stops_before_groups_get_too_thin_to_median():
    """A dimension that would starve every group is skipped, not accepted."""
    total = 400
    candidates = [
        _candidate("region", 2, total),  # 200 rows/group, ~48 after sampling -- fine
        _candidate("sku", 40, total),  # 80 groups over 400 rows -- 5 rows each, ~1 after sampling
    ]

    selected = select_baseline_columns(candidates, total)

    assert selected == ["region"]


def test_the_rows_per_group_floor_accounts_for_sampling():
    """Discovery runs on the full table; the fit runs on a sample of a split of it.

    Checking the nominal floor against the full table overstated what the model would get by about four
    times at the defaults, so a group that just cleared 30 contributed roughly 7 rows to its own median.
    This pins the relationship rather than leaving it implicit in two call sites.
    """
    retained = DEFAULT_SAMPLE_FRACTION * DEFAULT_TRAIN_RATIO

    assert effective_min_rows_per_group() > MIN_ROWS_PER_BASELINE_GROUP
    assert effective_min_rows_per_group() * retained >= MIN_ROWS_PER_BASELINE_GROUP
    # Nothing is sampled away, so the two coincide.
    assert effective_min_rows_per_group(1.0, 1.0) == MIN_ROWS_PER_BASELINE_GROUP


def test_skips_columns_too_wide_to_be_a_dimension():
    """Above the per-column ceiling a column is an identifier, not a peer group."""
    total = 1_000_000
    candidates = [
        _candidate("region", 4, total),
        _candidate("user_bucket", MAX_BASELINE_COLUMN_CARDINALITY + 1, total),
    ]

    selected = select_baseline_columns(candidates, total)

    assert selected == ["region"]


def test_respects_the_total_group_ceiling():
    """The ceiling bounds what is persisted and broadcast, not what is affordable to train."""
    total = 100_000_000  # rows are not the binding constraint here
    candidates = [
        _candidate("a", 50, total),
        _candidate("b", 50, total),
        _candidate("c", 50, total),  # 50^3 = 125,000 groups, over the ceiling
    ]

    selected = select_baseline_columns(candidates, total)

    assert selected == ["a", "b"]  # 2,500 groups
    assert 50 * 50 <= MAX_BASELINE_GROUPS
    assert 50 * 50 * 50 > MAX_BASELINE_GROUPS


def test_no_candidates_means_no_grouping():
    """Returning an empty list is how the caller learns to train unconditioned."""
    assert not select_baseline_columns([], 5000)


def test_selection_is_deterministic_in_candidate_order():
    """Two runs over the same table must produce the same grouping, or the key changes underneath a
    persisted model."""
    total = 9000
    candidates = [_candidate("a", 3, total), _candidate("b", 5, total), _candidate("c", 6, total)]

    original = list(candidates)
    assert select_baseline_columns(candidates, total) == ["a", "b"]
    assert candidates == original
    assert select_baseline_columns(candidates, total) == ["a", "b"]


@pytest.mark.parametrize("smallest, expected", [(29, []), (30, ["region"]), (31, ["region"])])
def test_measured_support_controls_selection_at_the_floor(smallest, expected):
    assert (
        select_baseline_columns(
            [_candidate("region", 2, 1000)],
            1000,
            sample_fraction=1.0,
            train_ratio=1.0,
            group_support=lambda names: (2, smallest),
        )
        == expected
    )


def test_joint_support_rejects_skew_hidden_by_the_average():
    assert not select_baseline_columns(
        [_candidate("region", 2, 1000)],
        1000,
        group_support=lambda names: (2, 1),
    )


def test_observed_joint_groups_do_not_assume_independent_dimensions():
    # Two perfectly correlated dimensions have two groups, not their Cartesian product of four.
    assert select_baseline_columns(
        [_candidate("region", 2, 300), _candidate("channel", 2, 300)],
        300,
        group_support=lambda names: (2, 150),
    ) == ["region", "channel"]


@pytest.mark.parametrize(
    "sample_fraction, train_ratio, max_rows, expected",
    [(1.0, 0.5, None, ["region"]), (0.1, 0.5, None, []), (1.0, 0.5, 100, []), (1.0, 0.5, 120, ["region"])],
)
def test_resolved_sampling_and_cap_control_projected_support(sample_fraction, train_ratio, max_rows, expected):
    assert (
        select_baseline_columns(
            [_candidate("region", 2, 400)],
            400,
            sample_fraction=sample_fraction,
            train_ratio=train_ratio,
            max_rows=max_rows,
            group_support=lambda names: (2, 200),
        )
        == expected
    )
