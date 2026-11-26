"""Pure per-rule summary helpers for the DQX MCP runner.

Intentionally dependency-free (stdlib only): no pyspark, no databricks-sdk, no dqx. *runner.py*
does the Spark aggregation (explode *_errors* / *_warnings* and count per rule) and hands the
grouped counts to *summarize_rule_counts*, which builds the *rule_summary* returned by
*run_checks*. Keeping the merge logic here lets it be unit-tested without a Spark session.

Rules are keyed by *rule_fingerprint*, not by name: DQX allows several rules to share a name
(e.g. *tags* and *tags[*]* both default to *tags_is_null*) and tells them apart by fingerprint,
which is also how summary metrics match results. Grouping by name would merge such rules into one
entry with combined counts.
"""

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class RuleCount:
    """Number of result entries one rule produced in a single result column (*_errors* or *_warnings*)."""

    rule_fingerprint: str
    rule_name: str
    count: int


@dataclass
class _RuleTotals:
    rule_name: str
    rule_fingerprint: str
    error_count: int = 0
    warning_count: int = 0


def summarize_rule_counts(
    error_counts: Iterable[RuleCount], warning_counts: Iterable[RuleCount]
) -> list[dict[str, str | int]]:
    """Build one summary entry per rule from grouped error and warning counts.

    Entries are keyed by *rule_fingerprint*, so rules that share a name stay separate. DQX gives
    every result entry a fingerprint and a name, so no fallback is needed. The result is sorted by
    rule name, then fingerprint, so the output is deterministic regardless of the order Spark returns
    groups in.

    Args:
        error_counts: Per-rule entry counts from the *_errors* column.
        warning_counts: Per-rule entry counts from the *_warnings* column.

    Returns:
        A list of dicts with *rule_name*, *rule_fingerprint*, *error_count* and *warning_count*.
    """
    totals: dict[str, _RuleTotals] = {}

    def entry_for(rule_count: RuleCount) -> _RuleTotals:
        return totals.setdefault(
            rule_count.rule_fingerprint,
            _RuleTotals(rule_name=rule_count.rule_name, rule_fingerprint=rule_count.rule_fingerprint),
        )

    for rule_count in error_counts:
        entry_for(rule_count).error_count += rule_count.count
    for rule_count in warning_counts:
        entry_for(rule_count).warning_count += rule_count.count

    ordered = sorted(totals.values(), key=lambda t: (t.rule_name, t.rule_fingerprint))
    return [
        {
            "rule_name": t.rule_name,
            "rule_fingerprint": t.rule_fingerprint,
            "error_count": t.error_count,
            "warning_count": t.warning_count,
        }
        for t in ordered
    ]
