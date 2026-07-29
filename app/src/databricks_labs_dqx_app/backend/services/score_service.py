"""Pure computations for the DQ quality score.

Each rule has equal weight. Its row-level checks contribute row pass rates;
dataset-level checks contribute one binary pass/fail verdict:

    row check score = 1 - failed_rows / input_rows
    dataset check score = 1 if no rows carry its failure, else 0
    rule score = mean(the rule's check scores)
    table score = mean(rule scores)

The row-level denominator remains table-wide rather than filter-scoped,
which preserves the accepted approximation documented in
docs/superpowers/specs/2026-07-10-dq-score-results-design.md §2.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Mapping

from databricks_labs_dqx_app.backend.models import CheckMetricBreakdown


class ScoreService:
    """Pure score computations — no I/O, deterministic, easily unit-testable."""

    @staticmethod
    def compute_table_score(
        check_metrics: list[CheckMetricBreakdown],
        input_row_count: int,
        dataset_check_names: Collection[str] = (),
        check_rule_ids: Mapping[str, str] | None = None,
    ) -> float | None:
        """Return the equal-rule-weight DQ score, or ``None`` if undefined.

        Args:
            check_metrics: Per-check error/warning breakdown for the run.
            input_row_count: The run's table-wide input row count; every
                row-level check is treated as evaluated against all rows.
            dataset_check_names: Check names whose dataset-wide verdict must
                count once, rather than once per input row.
            check_rule_ids: Optional check-name to stable rule-id mapping.
                Checks sharing a rule id are averaged into one rule score.
                Unmapped checks use their check name as their rule identity.
        """
        if input_row_count <= 0 or not check_metrics:
            return None
        dataset_names = set(dataset_check_names)
        rule_scores: dict[str, list[float]] = defaultdict(list)
        for metric in check_metrics:
            failed = metric.error_count + metric.warning_count
            if metric.check_name in dataset_names:
                check_score = 0.0 if failed > 0 else 1.0
            else:
                check_score = 1.0 - failed / input_row_count
            rule_key = (check_rule_ids or {}).get(metric.check_name, metric.check_name)
            rule_scores[rule_key].append(check_score)
        per_rule = [sum(scores) / len(scores) for scores in rule_scores.values()]
        return sum(per_rule) / len(per_rule)

    @staticmethod
    def compute_product_score(table_scores: list[float]) -> float | None:
        """Unweighted mean of member tables' latest scores."""
        if not table_scores:
            return None
        return sum(table_scores) / len(table_scores)
