"""Unit pins for the feature-width arithmetic (no Spark).

The width warning is the only thing standing between a caller and a model three times wider than they
asked for, and it counted one feature per numeric column while both comparison bases were quietly adding
one more each. Twenty numeric columns with both bases build sixty features; the estimate said twenty and
stayed silent under the default recommended maximum of fifty.
"""

import pytest

from databricks.labs.dqx.anomaly.transformers import features_per_numeric_column


@pytest.mark.parametrize(
    "baseline_by, baseline_over_time, expected",
    [
        (None, None, 1),
        (["region"], None, 2),
        (None, "event_ts", 2),
        (["region"], "event_ts", 3),
        ([], "", 1),  # empty rather than None: the shapes a resolved-but-unset basis actually arrives as
    ],
)
def test_each_comparison_basis_adds_one_feature_per_metric(baseline_by, baseline_over_time, expected):
    assert features_per_numeric_column(baseline_by, baseline_over_time) == expected


def test_twenty_metrics_with_both_bases_reach_sixty_features():
    """The review's own example, as arithmetic. Sixty is over the default limit of fifty; twenty is not,
    which is why the warning never fired."""
    assert 20 * features_per_numeric_column(["region"], "event_ts") == 60
