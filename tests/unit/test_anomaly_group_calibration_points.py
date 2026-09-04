"""Unit tests for the SHAP gate under per-group score calibration.

`permissive_quantile_points` collapses many per-group calibrations into the single set of bounds
the scorers use to decide which rows are worth a SHAP call. Pure dictionary arithmetic, so it is
testable without Spark.
"""

from databricks.labs.dqx.anomaly.scoring_utils import permissive_quantile_points

GLOBAL_POINTS = [(0.0, 0.10), (0.5, 0.50), (0.95, 0.90), (1.0, 1.00)]


def test_falls_back_to_global_when_no_group_calibration():
    """An ungrouped model must gate exactly as it did before."""
    assert permissive_quantile_points({}, GLOBAL_POINTS) == GLOBAL_POINTS


def test_takes_the_minimum_bound_across_groups():
    """The gate must admit a row that *any* group would consider anomalous.

    A per-group maximum, or the global bound, would silently drop contributions from anomalous
    rows in the groups whose scores run low.
    """
    group_points = {
        "quiet": [(0.0, 0.01), (0.5, 0.05), (0.95, 0.20), (1.0, 0.30)],
        "busy": [(0.0, 0.40), (0.5, 0.60), (0.95, 0.95), (1.0, 1.20)],
    }

    result = dict(permissive_quantile_points(group_points, GLOBAL_POINTS))

    assert result[0.95] == 0.20
    assert result[0.5] == 0.05
    assert result[1.0] == 0.30


def test_gate_is_never_stricter_than_the_strictest_group():
    """Restates the safety property directly: the bound never exceeds any group's own bound."""
    group_points = {
        "a": [(0.95, 0.70)],
        "b": [(0.95, 0.30)],
        "c": [(0.95, 0.55)],
    }

    result = dict(permissive_quantile_points(group_points, GLOBAL_POINTS))

    assert result[0.95] <= min(0.70, 0.30, 0.55)


def test_percentiles_only_the_global_calibration_has_are_kept():
    """A partial group calibration must not shrink the set of interpolation points."""
    group_points = {"a": [(0.95, 0.30)]}

    result = dict(permissive_quantile_points(group_points, GLOBAL_POINTS))

    assert set(result) == {0.0, 0.5, 0.95, 1.0}
    assert result[0.0] == 0.10
    assert result[0.95] == 0.30


def test_result_is_ordered_by_percentile():
    """The interpolation walks these in order, so ordering is part of the contract."""
    group_points = {"a": [(1.0, 0.9), (0.0, 0.1), (0.5, 0.4)]}

    result = permissive_quantile_points(group_points, GLOBAL_POINTS)

    assert [p for p, _ in result] == sorted(p for p, _ in result)
