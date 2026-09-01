"""Unit tests for ``backend.services.score_service``.

``ScoreService`` is a pure computation service — no I/O — implementing
the equal-rule-weight DQ score. Row checks contribute their row pass
rate; dataset checks contribute one binary verdict; checks produced by
the same reusable rule first roll up to one rule score.
"""

import pytest

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown
from databricks_labs_dqx_app.backend.services.score_service import ScoreService


class TestComputeTableScore:
    def test_equal_mean_of_row_check_pass_rates(self):
        check_metrics = [
            CheckMetricBreakdown(check_name="rule_a", error_count=10, warning_count=0),
            CheckMetricBreakdown(check_name="rule_b", error_count=20, warning_count=10),
        ]
        score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
        assert score == pytest.approx((0.9 + 0.7) / 2)

    def test_dataset_failure_is_one_binary_failed_check(self):
        check_metrics = [
            CheckMetricBreakdown(check_name="row_rule", error_count=10, warning_count=0),
            # DQX broadcasts this verdict, so its raw count is 100. The score
            # must still treat it as one failed table gate.
            CheckMetricBreakdown(check_name="table_rule", error_count=100, warning_count=0),
        ]
        score = ScoreService.compute_table_score(
            check_metrics,
            input_row_count=100,
            dataset_check_names={"table_rule"},
        )
        assert score == pytest.approx((0.9 + 0.0) / 2)

    def test_dataset_pass_is_one_binary_passing_check(self):
        check_metrics = [
            CheckMetricBreakdown(check_name="row_rule", error_count=50, warning_count=0),
            CheckMetricBreakdown(check_name="table_rule", error_count=0, warning_count=0),
        ]
        score = ScoreService.compute_table_score(
            check_metrics,
            input_row_count=100,
            dataset_check_names={"table_rule"},
        )
        assert score == pytest.approx((0.5 + 1.0) / 2)

    def test_rule_applied_to_many_columns_still_has_one_rule_weight(self):
        check_metrics = [
            CheckMetricBreakdown(check_name="a_col_1", error_count=0, warning_count=0),
            CheckMetricBreakdown(check_name="a_col_2", error_count=0, warning_count=0),
            CheckMetricBreakdown(check_name="b", error_count=100, warning_count=0),
        ]
        score = ScoreService.compute_table_score(
            check_metrics,
            input_row_count=100,
            check_rule_ids={"a_col_1": "a", "a_col_2": "a", "b": "b"},
        )
        assert score == pytest.approx((1.0 + 0.0) / 2)

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

    def test_score_clamped_when_failures_exceed_row_count(self):
        # When failed count exceeds input_row_count (e.g. from two checks collapsing
        # or a filtered run), the score is clamped to [0, 1] to avoid negative values.
        check_metrics = [
            CheckMetricBreakdown(check_name="rule_a", error_count=150, warning_count=0),
        ]
        score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
        assert score == pytest.approx(0.0)  # Clamped to 0, not negative

    def test_multiple_checks_with_excess_failures_table_score_stays_in_range(self):
        # Multiple checks with one having excess failures should still yield
        # a table score in [0, 1] — the clamping prevents negative rule means.
        check_metrics = [
            CheckMetricBreakdown(check_name="rule_a", error_count=0, warning_count=0),
            CheckMetricBreakdown(check_name="rule_b", error_count=200, warning_count=0),
        ]
        score = ScoreService.compute_table_score(check_metrics, input_row_count=100)
        # rule_a score = 1.0, rule_b score = max(0, 1 - 200/100) = 0, mean = 0.5
        assert score == pytest.approx(0.5)
        assert 0.0 <= score <= 1.0


class TestComputeProductScore:
    def test_unweighted_mean_of_member_scores(self):
        assert ScoreService.compute_product_score([1.0, 0.5]) == pytest.approx(0.75)

    def test_returns_none_for_empty_membership(self):
        assert ScoreService.compute_product_score([]) is None

    def test_single_table_passthrough(self):
        assert ScoreService.compute_product_score([0.8]) == pytest.approx(0.8)
