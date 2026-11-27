"""Unit tests for the runner's per-rule summary helper.

``dqx_mcp_runner.rule_summary`` is stdlib-only (no pyspark / DQX import), so it runs in the
mcp-server test environment. The runner does the Spark grouping and passes the grouped counts to
``summarize_rule_counts``; these tests cover how those counts are merged into ``rule_summary``,
in particular that rules sharing a name are kept apart by fingerprint.
"""

from dqx_mcp_runner.rule_summary import RuleCount, summarize_rule_counts


def _entry(name: str, fingerprint: str | None, errors: int = 0, warnings: int = 0) -> dict:
    return {"rule_name": name, "rule_fingerprint": fingerprint, "error_count": errors, "warning_count": warnings}


class TestRulesSharingAName:
    def test_rules_with_same_name_are_reported_separately(self):
        # Regression for the MCP summary merging two different rules that share a name: is_not_null
        # on `a` and on `b`, both named col_is_null, fail on 2 and 3 rows respectively.
        summary = summarize_rule_counts(
            error_counts=[RuleCount("fp_a", "col_is_null", 2), RuleCount("fp_b", "col_is_null", 3)],
            warning_counts=[],
        )
        assert summary == [_entry("col_is_null", "fp_a", errors=2), _entry("col_is_null", "fp_b", errors=3)]

    def test_array_path_rules_with_same_default_name_are_reported_separately(self):
        # `tags` and `tags[*]` both default to tags_is_null; their counts must not be combined.
        summary = summarize_rule_counts(
            error_counts=[RuleCount("fp_tags", "tags_is_null", 1), RuleCount("fp_tags_elems", "tags_is_null", 4)],
            warning_counts=[],
        )
        assert len(summary) == 2
        assert sum(e["error_count"] for e in summary) == 5
        assert {e["rule_fingerprint"]: e["error_count"] for e in summary} == {"fp_tags": 1, "fp_tags_elems": 4}

    def test_error_and_warning_rules_sharing_a_name_are_reported_separately(self):
        # Criticality is part of the fingerprint, so an error rule and a warn rule with the same name
        # are different rules and must each keep their own count.
        summary = summarize_rule_counts(
            error_counts=[RuleCount("fp_err", "id_is_null", 2)],
            warning_counts=[RuleCount("fp_warn", "id_is_null", 7)],
        )
        assert summary == [_entry("id_is_null", "fp_err", errors=2), _entry("id_is_null", "fp_warn", warnings=7)]


class TestCounting:
    def test_rules_with_unique_names_keep_their_counts(self):
        summary = summarize_rule_counts(
            error_counts=[RuleCount("fp_id", "id_is_null", 3), RuleCount("fp_age", "age_out_of_range", 1)],
            warning_counts=[RuleCount("fp_name", "name_is_empty", 2)],
        )
        assert summary == [
            _entry("age_out_of_range", "fp_age", errors=1),
            _entry("id_is_null", "fp_id", errors=3),
            _entry("name_is_empty", "fp_name", warnings=2),
        ]

    def test_no_failures_gives_empty_summary(self):
        assert not summarize_rule_counts(error_counts=[], warning_counts=[])


class TestOutput:
    def test_order_is_deterministic_regardless_of_input_order(self):
        # Spark returns groups in no particular order; the summary must not depend on it.
        counts = [
            RuleCount("fp_2", "same_name", 1),
            RuleCount("fp_z", "b_rule", 1),
            RuleCount("fp_1", "same_name", 1),
            RuleCount("fp_y", "a_rule", 1),
        ]
        forward = summarize_rule_counts(error_counts=counts, warning_counts=[])
        backward = summarize_rule_counts(error_counts=list(reversed(counts)), warning_counts=[])
        assert forward == backward
        assert [(e["rule_name"], e["rule_fingerprint"]) for e in forward] == [
            ("a_rule", "fp_y"),
            ("b_rule", "fp_z"),
            ("same_name", "fp_1"),
            ("same_name", "fp_2"),
        ]

    def test_entries_keep_rule_name_and_add_fingerprint(self):
        # rule_name/error_count/warning_count keep their existing keys for callers already reading
        # the summary; rule_fingerprint is added so entries can be told apart and joined to results.
        [entry] = summarize_rule_counts(error_counts=[RuleCount("fp_a", "a_is_null", 1)], warning_counts=[])
        assert set(entry) == {"rule_name", "rule_fingerprint", "error_count", "warning_count"}
