"""The attribution gate must admit every row flagged by any real calibration."""

import numpy as np
import pytest

from databricks.labs.dqx.anomaly.explainability import AttributionGate, severity_from_scores

GLOBAL_POINTS = [(0.0, 0.0), (95.0, 20.0), (99.0, 21.0), (100.0, 30.0)]
GROUP_POINTS = dict(
    (
        ("wide", [(0.0, 0.0), (95.0, 1.0), (99.0, 10.0), (100.0, 12.0)]),
        ("narrow", [(0.0, 0.0), (95.0, 8.0), (99.0, 9.0), (100.0, 12.0)]),
    )
)


def test_ungrouped_gate_uses_global_curve():
    scores = np.array([0.0, 9.45, 20.0, 22.0])
    gate = AttributionGate.from_calibrations({}, GLOBAL_POINTS)
    np.testing.assert_array_equal(gate.maximum_severity(scores), severity_from_scores(scores, GLOBAL_POINTS))


def test_crossing_tail_curves_are_not_combined_knot_by_knot():
    score = np.array([9.45])
    gate = AttributionGate.from_calibrations(GROUP_POINTS, GLOBAL_POINTS)
    assert severity_from_scores(score, GROUP_POINTS["narrow"])[0] > 99.5
    assert gate.maximum_severity(score)[0] > 99.5
    synthetic_minima = [(0.0, 0.0), (95.0, 1.0), (99.0, 9.0), (100.0, 12.0)]
    assert severity_from_scores(score, synthetic_minima)[0] < 99.5


@pytest.mark.parametrize("threshold", [95.0, 99.0, 99.5, 99.9])
def test_gate_includes_every_group_and_global_fallback(threshold):
    scores = np.linspace(0.0, 35.0, 1001)
    gate = AttributionGate.from_calibrations(GROUP_POINTS, GLOBAL_POINTS)
    admitted = gate.maximum_severity(scores) >= threshold
    for curve in (GLOBAL_POINTS, *GROUP_POINTS.values()):
        flagged = severity_from_scores(scores, curve) >= threshold
        assert np.all(admitted[flagged])


def test_global_fallback_remains_included_when_its_scores_run_lower():
    gate = AttributionGate.from_calibrations({"high": GLOBAL_POINTS}, GROUP_POINTS["narrow"])
    assert gate.maximum_severity(np.array([9.45]))[0] > 99.5


def test_incomplete_group_uses_global_fallback():
    scores = np.array([0.0, 1.0, 21.0])
    gate = AttributionGate.from_calibrations({"partial": [(95.0, 0.1)]}, GLOBAL_POINTS)
    np.testing.assert_array_equal(gate.maximum_severity(scores), severity_from_scores(scores, GLOBAL_POINTS))


@pytest.mark.parametrize(
    "points, expected",
    [
        ([(0.0, 0.0), (90.0, 1.0), (95.0, 1.0), (99.0, 2.0), (100.0, 3.0)], 90.0),
        ([(0.0, 1.0), (95.0, 1.0), (99.0, 1.0), (100.0, 1.0)], 0.0),
        ([(0.0, 0.0), (90.0, 1.0), (95.0, 1.0)], 90.0),
    ],
)
def test_equal_score_knots_use_first_matching_bound(points, expected):
    assert severity_from_scores(np.array([1.0]), points)[0] == expected


def test_degenerate_tail_uses_remaining_knots_above_anchor():
    points = [(0.0, 0.0), (95.0, 1.0), (99.0, 1.0), (100.0, 2.0)]
    np.testing.assert_allclose(severity_from_scores(np.array([1.0, 1.5, 3.0]), points), [95.0, 99.5, 100.0])
