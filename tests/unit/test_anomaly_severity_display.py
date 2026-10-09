"""The published severity must lead a reader to the same verdict as the flag (no Spark, no workspace).

The flag is decided on the full-precision severity. The published number used to be *rounded* to one
decimal, so a row at 94.96 was shown as 95.0 and not flagged at a threshold of 95: nothing wrong with the
decision, but a reader comparing the two numbers disagreed with it. A black-box evaluation found that in 477
of 912 scoring cells, every audited disagreement sitting exactly at displayed threshold equality.

Flooring at the threshold's own precision closes it without touching a single flag. These tests pin the
equivalence itself rather than the formula, so a different implementation of the same guarantee passes.
"""

import math
from decimal import ROUND_FLOOR, Decimal

import pytest

from databricks.labs.dqx.anomaly.scoring_utils import displayed_severity_decimals

# The commonly used thresholds, plus finer ones, plus three that a first version of this fix got WRONG.
# 0.9, 90.01 and 99.01 are here because scaling-then-flooring disagrees with the flag on the double one ulp
# below each of them, and none of the ordinary thresholds show it. An earlier version of this file swept
# values densely at seven clean thresholds and passed against a defective implementation.
_THRESHOLDS = [90.0, 95.0, 99.0, 99.5, 99.9, 99.95, 99.995, 0.9, 90.01, 99.01, 99.058]


def _displayed(severity: float, threshold: float) -> float:
    """The published value, in Python, mirroring *displayed_severity_expr*.

    Flooring in decimal space on the shortest representation, which is what Spark's ``floor(expr, scale)``
    does for a double: it routes through ``BigDecimal.valueOf``, i.e. ``Double.toString``. Deliberately not
    ``floor(severity * scale) / scale`` -- that is the form this fix replaced, and
    :func:`test_scaling_then_flooring_would_reintroduce_the_defect` pins why.

    A mirror, so it proves a property of the arithmetic and not of the product. The integration suite is
    what checks Spark agrees with it.
    """
    quantum = Decimal(1).scaleb(-displayed_severity_decimals(threshold))
    return float(Decimal(repr(severity)).quantize(quantum, rounding=ROUND_FLOOR))


def _scaled_then_floored(severity: float, threshold: float) -> float:
    """The rejected form, kept so its failure is executable rather than described."""
    scale = float(10 ** displayed_severity_decimals(threshold))
    return math.floor(severity * scale) / scale


def _candidates(threshold: float) -> list[float]:
    """Values around a threshold, including the float neighbours of each gridpoint.

    The dense sweep covers ordinary cases; the neighbours are where a scaling-then-flooring implementation
    would break, because ``10 * x`` need not be exact.
    """
    values = [threshold + step * 1e-5 for step in range(-500, 501)]
    for base in (threshold - 0.1, threshold, threshold + 0.1):
        for direction in (math.inf, -math.inf):
            value = base
            for _ in range(50):
                value = math.nextafter(value, direction)
                values.append(value)
    return values


@pytest.mark.parametrize("threshold", _THRESHOLDS)
def test_a_reader_comparing_the_published_severity_reaches_the_flags_verdict(threshold: float):
    """The contract, stated as the equivalence it is.

    Swept either side of the threshold and across the float neighbours of every gridpoint, because the
    implementation multiplies before flooring and that is exactly where such an implementation would drift.
    """
    disagreements = [
        value for value in _candidates(threshold) if (_displayed(value, threshold) >= threshold) != (value >= threshold)
    ]

    assert not disagreements, f"{len(disagreements)} values disagree at {threshold}, e.g. {disagreements[:3]}"


@pytest.mark.parametrize("threshold", [90.0, 95.0, 99.0, 99.5, 99.9])
def test_rounding_to_one_decimal_would_break_that_equivalence(threshold: float):
    """The defect, pinned so the fix cannot be reverted quietly.

    Without this a later contributor could restore ``round(severity, 1)`` -- which is shorter, looks tidier,
    and passes every other test in the suite -- and reintroduce the disagreement.

    Parametrised over the one-decimal thresholds only, which is where the defect was observed and where
    rounding is guaranteed to break: a value in ``[t - 0.05, t)`` rounds up onto the threshold while the
    flag, decided on the true value, says no. At a finer threshold the relationship between the two
    precisions changes and rounding need not disagree on any sampled value, so claiming it there would be
    asserting more than the arithmetic supports.
    """
    disagreements = [
        value for value in _candidates(threshold) if (round(value, 1) >= threshold) != (value >= threshold)
    ]

    assert disagreements, f"expected rounding to disagree somewhere near {threshold}"


def test_the_published_severity_never_reads_higher_than_the_decided_one():
    """The one-sided half of the guarantee, and the reason flooring was chosen over rounding.

    Overstating is the harmful direction: it invites a reader to believe a row crossed the threshold when
    the flag says it did not. Understating cannot mislead that way, and the equivalence above bounds how
    far it may understate.
    """
    for threshold in _THRESHOLDS:
        for value in _candidates(threshold):
            assert _displayed(value, threshold) <= value


@pytest.mark.parametrize(
    "threshold, expected",
    [(95.0, 1), (99.0, 1), (99.5, 1), (99.9, 1), (99.95, 2), (99.995, 3), (95, 1)],
)
def test_the_display_precision_follows_the_threshold(threshold: float, expected: int):
    """Derived rather than fixed, because a fixed precision is right only for thresholds that match it.

    Measured: flooring at one decimal disagrees on none of the one-decimal thresholds but on 560 sampled
    values at a threshold of 99.95. Deriving the precision disagrees on none at any of them.
    """
    assert displayed_severity_decimals(threshold) == expected


def test_a_whole_number_threshold_still_shows_a_decimal():
    """Readability: a severity of 95 should not print as an integer just because the threshold is one."""
    assert displayed_severity_decimals(95.0) == 1
    assert _displayed(95.04, 95.0) == 95.0


def test_a_saturated_severity_is_published_unchanged():
    """The tail expression reaches 100 exactly; flooring must not shave it to 99.9."""
    for threshold in _THRESHOLDS:
        assert _displayed(100.0, threshold) == 100.0


def _threshold_grid(start: float, stop: float, step: float, places: int) -> list[float]:
    """Every legal threshold at one precision, so the sweep covers thresholds and not only values."""
    count = int(round((stop - start) / step))
    return [round(start + step * k, places) for k in range(count + 1)]


@pytest.mark.parametrize(
    "grid",
    [
        pytest.param(_threshold_grid(0.1, 100.0, 0.1, 1), id="every_one_decimal_threshold"),
        pytest.param(_threshold_grid(90.0, 100.0, 0.01, 2), id="every_two_decimal_threshold_from_90"),
        pytest.param(_threshold_grid(99.0, 100.0, 0.001, 3), id="every_three_decimal_threshold_from_99"),
    ],
)
def test_the_equivalence_holds_at_every_legal_threshold_not_just_the_common_ones(grid: list[float]):
    """Swept over thresholds, which is the axis the first version of this fix left untested.

    Its predecessor swept values densely at seven thresholds that all happen to be clean, so it passed
    against an implementation that disagreed with the flag at 268 other legal thresholds. The failure was
    always one ulp below the threshold and always in the harmful direction: the display read at the
    threshold while the row was not flagged.
    """
    failures = []
    for threshold in grid:
        probe = threshold
        for _ in range(4):
            probe = math.nextafter(probe, -math.inf)
            if (_displayed(probe, threshold) >= threshold) != (probe >= threshold):
                failures.append((threshold, probe))
                break

    assert not failures, f"{len(failures)} thresholds disagree, e.g. {failures[:3]}"


@pytest.mark.parametrize("threshold, probe", [(0.9, 0.8999999999999999), (90.01, 90.00999999999999)])
def test_scaling_then_flooring_would_reintroduce_the_defect(threshold: float, probe: float):
    """The rejected implementation, pinned at two of the thresholds where it fails.

    ``severity * 10**n`` is a double multiply and can round up across an integer, after which the floor
    lands on the threshold for a row below it. Without this test the shorter form looks equivalent, reads
    more simply, and passes every threshold anyone would think to try.
    """
    assert probe < threshold
    assert _scaled_then_floored(probe, threshold) >= threshold, "expected the rejected form to overstate"
    assert _displayed(probe, threshold) < threshold, "the shipped form must not"
