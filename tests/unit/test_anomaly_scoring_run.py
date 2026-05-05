"""Unit tests for the budget-allocation helper used by *score_segmented*.

Verifies that *_split_max_groups_budget* keeps the *total* LLM-call cap (per-segment
budget × num_segments) at or below the user-facing *max_groups* whenever the budget
is at least the segment count, and falls back to a documented finite bound (one call
per segment) when the budget is under-provisioned.
"""

from __future__ import annotations

import pytest

from databricks.labs.dqx.anomaly.scoring_run import _split_max_groups_budget
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.mark.parametrize(
    "max_groups, num_segments, expected_per_segment",
    [
        # Even split — total = 500, exactly at the cap.
        pytest.param(500, 5, 100, id="even_split"),
        # Uneven split — floor division yields 142, total = 994 < 1000.
        pytest.param(1000, 7, 142, id="floor_divides_below_cap"),
        # Single segment — gets the full budget.
        pytest.param(500, 1, 500, id="single_segment_gets_full_budget"),
        # Budget exactly equals segment count — each segment gets 1.
        pytest.param(10, 10, 1, id="budget_equals_segments"),
    ],
)
def test_split_keeps_total_at_or_below_cap(max_groups, num_segments, expected_per_segment):
    per_segment = _split_max_groups_budget(max_groups, num_segments)
    assert per_segment == expected_per_segment
    # The whole point of the helper: total LLM calls across segments stays bounded.
    assert per_segment * num_segments <= max_groups


def test_split_floor_of_one_when_budget_under_provisioned():
    """When *max_groups* < num_segments the floor of 1 kicks in.

    Documented tradeoff: total calls become ``num_segments`` (> *max_groups*) but the
    cap is still finite and proportional to the input — every segment gets a chance to
    produce at least one explanation. Locks the documented behaviour.
    """
    per_segment = _split_max_groups_budget(max_groups=3, num_eligible_segments=10)
    assert per_segment == 1
    # Total = 10 > max_groups=3, by design.
    assert per_segment * 10 == 10


def test_split_rejects_zero_segments():
    """Calling with no eligible segments is a programming error — *score_segmented*
    skips the call entirely when *eligible* is empty. Surface it loudly here so a
    refactor that drops the guard fails fast."""
    with pytest.raises(InvalidParameterError, match="num_eligible_segments must be positive"):
        _split_max_groups_budget(max_groups=500, num_eligible_segments=0)


def test_split_rejects_negative_segments():
    with pytest.raises(InvalidParameterError, match="num_eligible_segments must be positive"):
        _split_max_groups_budget(max_groups=500, num_eligible_segments=-1)
