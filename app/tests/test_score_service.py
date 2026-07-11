"""Unit tests for ``backend.services.score_service``.

``ScoreService`` is a pure computation service — no I/O — implementing
the row-weighted DQ score:

    score = 1 - (sum(failed_tests) / sum(total_tests))

where, per rule in a run: total_tests = input_row_count (table-wide),
failed_tests = error_count + warning_count for that rule.
"""

from __future__ import annotations

import pytest

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown
from databricks_labs_dqx_app.backend.services.score_service import ScoreService


class TestComputeTableScore:
    def test_faithful_to_row_weighted_formula(self):
        # 100 rows, rule A fails 10, rule B fails 30 -> failed_tests=40, total_tests=200
        check_metrics = [
            CheckMetricBreakdown(check_name="rule_a", error_count=10, warning_count=0),
            CheckMetricBreakdown(check_name="rule_b", error_count=20, warning_count=10),
        ]
        score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
        assert score == pytest.approx(1 - (40 / 200))

    def test_returns_none_when_no_rows(self):
        assert ScoreService.compute_table_score([], input_row_count=0) is None

    def test_returns_none_when_no_check_metrics(self):
        assert ScoreService.compute_table_score([], input_row_count=100) is None

    def test_returns_none_for_negative_row_count(self):
        check_metrics = [CheckMetricBreakdown(check_name="rule_a", error_count=1, warning_count=0)]
        assert ScoreService.compute_table_score(check_metrics, input_row_count=-5) is None

    def test_perfect_when_no_failures(self):
        check_metrics = [CheckMetricBreakdown(check_name="rule_a", error_count=0, warning_count=0)]
        assert ScoreService.compute_table_score(check_metrics, input_row_count=50) == 1.0

    def test_zero_when_every_test_fails(self):
        check_metrics = [
            CheckMetricBreakdown(check_name="rule_a", error_count=50, warning_count=0),
            CheckMetricBreakdown(check_name="rule_b", error_count=0, warning_count=50),
        ]
        assert ScoreService.compute_table_score(check_metrics, input_row_count=50) == 0.0

    def test_warnings_count_as_failures(self):
        check_metrics = [CheckMetricBreakdown(check_name="rule_a", error_count=0, warning_count=10)]
        score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
        assert score == pytest.approx(0.9)


class TestComputeProductScore:
    def test_unweighted_mean_of_member_scores(self):
        assert ScoreService.compute_product_score([1.0, 0.5]) == pytest.approx(0.75)

    def test_returns_none_for_empty_membership(self):
        assert ScoreService.compute_product_score([]) is None

    def test_single_table_passthrough(self):
        assert ScoreService.compute_product_score([0.8]) == pytest.approx(0.8)
