"""Computes DQ scores from the existing dq_metrics data.

Score formula (row-weighted, faithful port of dqlake's approach, minus
per-rule *filter* scoping — an accepted approximation, see
docs/superpowers/specs/2026-07-10-dq-score-results-design.md §2):

    score = 1 - (sum(failed_tests) / sum(total_tests))

where, per rule in a run: total_tests = input_row_count (table-wide,
not filter-scoped), failed_tests = error_count + warning_count for
that rule.
"""

from __future__ import annotations

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown


class ScoreService:
    """Pure score computations — no I/O, deterministic, easily unit-testable."""

    @staticmethod
    def compute_table_score(
        check_metrics: list[CheckMetricBreakdown],
        input_row_count: int,
    ) -> float | None:
        """Return the row-weighted DQ score for one run, or None if undefined.

        Args:
            check_metrics: Per-check error/warning breakdown for the run.
            input_row_count: The run's table-wide input row count; every
                rule is treated as having been evaluated against all rows.
        """
        if input_row_count <= 0 or not check_metrics:
            return None
        total_tests = input_row_count * len(check_metrics)
        failed_tests = sum(m.error_count + m.warning_count for m in check_metrics)
        return 1.0 - (failed_tests / total_tests)

    @staticmethod
    def compute_product_score(table_scores: list[float]) -> float | None:
        """Unweighted mean of member tables' latest scores."""
        if not table_scores:
            return None
        return sum(table_scores) / len(table_scores)
