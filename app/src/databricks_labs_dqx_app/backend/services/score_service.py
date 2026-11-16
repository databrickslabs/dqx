"""Pure computations for the DQ quality score.

Each rule has equal weight. Its row-level checks contribute row pass rates;
dataset-level checks contribute one binary pass/fail verdict:

    row check score = 1 - failed_rows / evaluated_rows
    dataset check score = 1 if no rows carry its failure, else 0
    rule score = mean(the rule's scorable check scores)
    table score = mean(scorable rule scores)

A filtered row check's evaluated rows are the rows its filter selected
(the run's *check_evaluated_rows* metric); unfiltered checks and runs
that predate the metric use the table's input rows. A row check that
evaluated no rows is unscorable and leaves its rule's average; a rule
with no scorable checks leaves the table score.
"""

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
                A row check's *evaluated_count*, when set, is its denominator.
            input_row_count: The run's table-wide input row count; the
                denominator of every row check without an *evaluated_count*.
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
                evaluated = metric.evaluated_count if metric.evaluated_count is not None else input_row_count
                if evaluated <= 0:
                    continue  # the filter matched no rows: unscorable
                check_score = max(0.0, 1.0 - failed / evaluated)
            rule_key = (check_rule_ids or {}).get(metric.check_name, metric.check_name)
            rule_scores[rule_key].append(check_score)
        if not rule_scores:
            return None
        per_rule = [sum(scores) / len(scores) for scores in rule_scores.values()]
        return sum(per_rule) / len(per_rule)

    @staticmethod
    def compute_product_score(table_scores: list[float]) -> float | None:
        """Unweighted mean of member tables' latest scores."""
        if not table_scores:
            return None
        return sum(table_scores) / len(table_scores)
