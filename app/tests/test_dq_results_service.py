"""Unit tests for ``services.dq_results_service`` — the pure Python
aggregation that reproduces dqlake's SQL breakdown/trend semantics over
``v_dq_check_results`` rows.

Attribution (severity tag, quality dimension, mapped columns, registry
rule id) is VERSION-ACCURATE: it arrives ON each view row, parsed from
the run's own frozen ``dq_validation_runs.checks_json`` rendered rule
set — never joined live to the binding's current metadata. Editing or
renaming a tag today must not change what these functions compute for
historical rows.

No SQL, no workspace: rows are built in-memory, so these tests pin the
aggregation math (pass rates, distinct-rule counts, explosions, facet
filtering, axes slicing) independent of the routes.
"""

from __future__ import annotations

import json

import pytest

from databricks_labs_dqx_app.backend.services.dq_results_service import (
    CheckResultRow,
    ResultFacets,
    compute_entity_results,
    parse_check_rows,
)

FQN = "main.sales.orders"


def make_row(
    check: str = "c1",
    failed: int = 0,
    total: int | None = 100,
    run_id: str = "r1",
    run_date: str = "2026-07-01 00:00:00",
    fqn: str = FQN,
    severity: str | None = None,
    dimension: str | None = None,
    columns: tuple[str, ...] = (),
    rule_id: str | None = None,
) -> CheckResultRow:
    return CheckResultRow(
        table_fqn=fqn,
        run_id=run_id,
        run_date=run_date,
        check_name=check,
        failed=failed,
        total=total,
        severity=severity,
        dimension=dimension,
        columns=columns,
        rule_id=rule_id,
    )


class TestParseCheckRows:
    def test_parses_statement_shaped_strings_with_attribution(self):
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "2026-07-01 00:00:00",
                    "check_name": "c1",
                    "error_count": "3",
                    "warning_count": "2",
                    "input_row_count": "100",
                    "severity": "High",
                    "dimension": "Completeness",
                    "registry_rule_id": "rule-1",
                    "columns_json": json.dumps(["id", "amount"]),
                }
            ]
        )
        assert len(rows) == 1
        assert rows[0].failed == 5  # errors + warnings
        assert rows[0].total == 100
        assert rows[0].severity == "High"
        assert rows[0].dimension == "Completeness"
        assert rows[0].rule_id == "rule-1"
        assert rows[0].columns == ("id", "amount")

    def test_placeholder_rows_are_dropped(self):
        # A run with no per-check breakdown yields a check_name NULL
        # placeholder row in the shaping view — no axis can use it.
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "2026-07-01 00:00:00",
                    "check_name": None,
                    "error_count": None,
                    "warning_count": None,
                    "input_row_count": None,
                }
            ]
        )
        assert rows == []

    def test_null_counts_default_to_zero_failed_and_none_total(self):
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "d",
                    "check_name": "c1",
                    "error_count": None,
                    "warning_count": None,
                    "input_row_count": None,
                }
            ]
        )
        assert rows[0].failed == 0
        assert rows[0].total is None

    def test_null_attribution_yields_untagged_row(self):
        # Legacy run (NULL checks_json) or a synthesized/hand-authored
        # check without tags: attribution columns arrive NULL from the
        # view's LEFT JOIN — the row lands in the untagged bucket.
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "d",
                    "check_name": "c1",
                    "error_count": "1",
                    "warning_count": "0",
                    "input_row_count": "10",
                    "severity": None,
                    "dimension": None,
                    "registry_rule_id": None,
                    "columns_json": None,
                }
            ]
        )
        assert rows[0].severity is None
        assert rows[0].dimension is None
        assert rows[0].rule_id is None
        assert rows[0].columns == ()

    def test_malformed_columns_json_degrades_to_empty(self):
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "d",
                    "check_name": "c1",
                    "error_count": "0",
                    "warning_count": "0",
                    "input_row_count": "10",
                    "columns_json": "{not json",
                },
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "d",
                    "check_name": "c2",
                    "error_count": "0",
                    "warning_count": "0",
                    "input_row_count": "10",
                    "columns_json": json.dumps({"not": "a list"}),
                },
            ]
        )
        assert rows[0].columns == ()
        assert rows[1].columns == ()


class TestBreakdowns:
    def test_by_dimension_groups_and_computes_pass_rate(self):
        rows = [
            make_row("c1", failed=10, total=100, dimension="Completeness", rule_id="rule-1"),
            make_row("c2", failed=30, total=100, dimension="Validity", rule_id="rule-2"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        by_dim = {g.label: g for g in out.by_dimension}
        assert by_dim["Completeness"].pass_rate == pytest.approx(0.9)
        assert by_dim["Completeness"].failed_tests == 10
        assert by_dim["Completeness"].rule_count == 1
        assert by_dim["Completeness"].check_count == 1
        assert by_dim["Completeness"].total_tests == 100
        assert by_dim["Validity"].pass_rate == pytest.approx(0.7)
        # dqlake: ORDER BY failed_tests DESC.
        assert [g.label for g in out.by_dimension] == ["Validity", "Completeness"]

    def test_untagged_checks_land_in_null_label_bucket(self):
        # dqlake parity: a rule without a dimension tag groups under a NULL
        # label (the UI renders an em-dash), not a synthetic string.
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets())
        assert [g.label for g in out.by_dimension] == [None]
        assert [g.label for g in out.by_severity] == [None]

    def test_by_rule_labels_by_check_name(self):
        # The materialized check's name IS the registry rule's name tag,
        # frozen at run time — so the rule label is simply the check name.
        rows = [make_row("unattributed_check", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets())
        assert [g.label for g in out.by_rule] == ["unattributed_check"]

    def test_pass_rate_none_when_no_tests(self):
        rows = [make_row("c1", failed=0, total=None)]
        out = compute_entity_results(rows, ResultFacets())
        assert out.by_rule[0].pass_rate is None
        assert out.by_rule[0].total_tests is None

    def test_rule_count_is_distinct_across_runs(self):
        rows = [
            make_row("c1", failed=0, total=100, run_id="r1", run_date="d1", rule_id="rule-1"),
            make_row("c1", failed=0, total=100, run_id="r2", run_date="d2", rule_id="rule-1"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert out.by_dimension[0].rule_count == 1
        assert out.by_dimension[0].check_count == 2  # one check row per run

    def test_rule_count_falls_back_to_check_name_when_unattributed(self):
        rows = [
            make_row("c1", failed=0, total=100, run_id="r1"),
            make_row("c2", failed=0, total=100, run_id="r1"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert out.by_dimension[0].rule_count == 2

    def test_by_column_explodes_multi_column_checks(self):
        # A check spanning two columns attributes to EACH of them (rows can
        # sum above the total — dqlake's intended involvement view); its
        # check_count is None (dqlake's by_column computes no check_count).
        rows = [make_row("c1", failed=10, total=100, columns=("id", "amount"), rule_id="rule-1")]
        out = compute_entity_results(rows, ResultFacets())
        by_col = {g.label: g for g in out.by_column}
        assert set(by_col) == {"id", "amount"}
        assert by_col["id"].failed_tests == 10
        assert by_col["id"].check_count is None

    def test_checks_without_columns_absent_from_by_column(self):
        rows = [make_row("c1", failed=10, total=100)]
        out = compute_entity_results(rows, ResultFacets())
        assert out.by_column == []

    def test_table_axis_selects_tables_vs_by_table(self):
        rows = [make_row("c1", failed=1, total=10)]
        table_out = compute_entity_results(rows, ResultFacets(), table_axis="tables")
        assert [g.label for g in table_out.tables] == [FQN]
        assert table_out.by_table == []
        product_out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        assert [g.label for g in product_out.by_table] == [FQN]
        assert product_out.tables == []

    def test_by_table_rows_carry_binding_ids_from_the_map(self):
        rows = [
            make_row("c1", failed=1, total=10, fqn="main.a.t1"),
            make_row("c2", failed=2, total=10, fqn="main.a.t2"),
        ]
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="by_table",
            binding_ids_by_table={"main.a.t1": "b1"},
        )
        by_table = {g.label: g.binding_id for g in out.by_table}
        # Mapped tables link to their binding; unmonitored tables keep None.
        assert by_table == {"main.a.t1": "b1", "main.a.t2": None}

    def test_tables_axis_is_never_binding_enriched(self):
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="tables",
            binding_ids_by_table={FQN: "b1"},
        )
        assert out.tables[0].binding_id is None

    def test_by_table_binding_id_defaults_to_none_without_a_map(self):
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        assert out.by_table[0].binding_id is None


class TestRuleIdentityGrouping:
    """P5.2: by_rule groups by RULE IDENTITY (registry_rule_id, else the
    check name) and labels each group with the name from the NEWEST run
    in scope — a rule renamed between versions is ONE row showing its
    current display name, never two rows split by name."""

    def _renamed(self) -> list[CheckResultRow]:
        return [
            make_row("old_name", failed=10, total=100, run_id="r1", run_date="2026-07-01 00:00:00", rule_id="rule-1"),
            make_row("new_name", failed=5, total=100, run_id="r2", run_date="2026-07-02 00:00:00", rule_id="rule-1"),
        ]

    def test_renamed_rule_collapses_to_one_row_labeled_with_newest_name(self):
        out = compute_entity_results(self._renamed(), ResultFacets())
        assert len(out.by_rule) == 1
        group = out.by_rule[0]
        assert group.label == "new_name"
        assert group.rule_id == "rule-1"
        assert group.failed_tests == 15
        assert group.total_tests == 200
        assert group.check_count == 2
        assert group.rule_count == 1

    def test_newest_label_is_order_independent(self):
        out = compute_entity_results(list(reversed(self._renamed())), ResultFacets())
        assert [g.label for g in out.by_rule] == ["new_name"]

    def test_dateless_rows_order_oldest_for_labeling(self):
        rows = [
            make_row("dateless_name", failed=1, total=10, run_id="r0", run_date=None, rule_id="rule-1"),
            make_row("dated_name", failed=1, total=10, run_id="r1", run_date="2026-07-01 00:00:00", rule_id="rule-1"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert [g.label for g in out.by_rule] == ["dated_name"]

    def test_legacy_null_rule_id_rows_still_group_by_name(self):
        rows = [
            make_row("legacy_check", failed=1, total=10, run_id="r1", run_date="d1"),
            make_row("legacy_check", failed=2, total=10, run_id="r2", run_date="d2"),
            make_row("other_check", failed=0, total=10, run_id="r2", run_date="d2"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        by_rule = {g.label: g for g in out.by_rule}
        assert set(by_rule) == {"legacy_check", "other_check"}
        assert by_rule["legacy_check"].failed_tests == 3
        assert by_rule["legacy_check"].rule_id is None

    def test_same_name_under_different_rule_ids_stays_two_rows(self):
        # Two DIFFERENT registry rules that happen to share a display name
        # must not merge — identity wins over label.
        rows = [
            make_row("same_name", failed=1, total=10, run_id="r1", rule_id="rule-1"),
            make_row("same_name", failed=2, total=10, run_id="r1", rule_id="rule-2"),
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert [g.label for g in out.by_rule] == ["same_name", "same_name"]
        assert {g.rule_id for g in out.by_rule} == {"rule-1", "rule-2"}

    def test_rule_id_absent_on_other_axes(self):
        out = compute_entity_results(self._renamed(), ResultFacets(), table_axis="by_table")
        assert out.by_table[0].rule_id is None
        assert out.by_dimension[0].rule_id is None

    def test_rule_facet_by_rule_id_selects_all_runs_including_old_names(self):
        # Clicking the labeled by_rule row sends the rule_id — every run of
        # that identity matches, old-name runs included.
        out = compute_entity_results(self._renamed(), ResultFacets(rules=("rule-1",)))
        assert len(out.by_rule) == 1
        assert out.by_rule[0].failed_tests == 15

    def test_rule_facet_by_label_still_works_but_stays_name_scoped(self):
        # Backward compat: a label-only value keeps matching by check name
        # (and only the runs that carried that name).
        out = compute_entity_results(self._renamed(), ResultFacets(rules=("old_name",)))
        assert len(out.by_rule) == 1
        assert out.by_rule[0].failed_tests == 10
        # The surviving group is still labeled by ITS newest matched run.
        assert out.by_rule[0].label == "old_name"


class TestVersionAccuracy:
    def test_same_check_attributes_per_run_not_per_current_rule(self):
        # THE POINT of the as-of-run design: the same check name carries
        # different tags in different runs (the rule was re-tagged between
        # them) and each run groups under what it RAN with.
        rows = [
            make_row("c1", failed=10, total=100, run_id="r1", run_date="d1", severity="Low", dimension="Validity"),
            make_row(
                "c1", failed=10, total=100, run_id="r2", run_date="d2", severity="Critical", dimension="Accuracy"
            ),
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert {g.label for g in out.by_severity} == {"Low", "Critical"}
        assert {g.label for g in out.by_dimension} == {"Validity", "Accuracy"}


class TestFacets:
    def _rows(self):
        return [
            make_row(
                "c1", failed=10, total=100, severity="High", dimension="Completeness", columns=("id",), rule_id="rule-1"
            ),
            make_row(
                "c2", failed=20, total=100, severity="Low", dimension="Validity", columns=("amount",), rule_id="rule-2"
            ),
        ]

    def test_dimension_facet_restricts_breakdowns_and_trend(self):
        out = compute_entity_results(self._rows(), ResultFacets(dimensions=("Completeness",)))
        assert [g.label for g in out.by_rule] == ["c1"]
        assert out.trend[0].pass_rate == pytest.approx(0.9)  # only c1 counted

    def test_or_within_a_facet(self):
        out = compute_entity_results(self._rows(), ResultFacets(dimensions=("Completeness", "Validity")))
        assert {g.label for g in out.by_rule} == {"c1", "c2"}

    def test_and_across_facets(self):
        out = compute_entity_results(self._rows(), ResultFacets(dimensions=("Completeness",), severities=("Low",)))
        assert out.by_rule == []

    def test_column_facet_is_membership_test(self):
        out = compute_entity_results(self._rows(), ResultFacets(columns=("id",)))
        assert [g.label for g in out.by_rule] == ["c1"]

    def test_rule_facet_matches_check_name(self):
        out = compute_entity_results(self._rows(), ResultFacets(rules=("c2",)))
        assert [g.label for g in out.by_rule] == ["c2"]

    def test_untagged_checks_never_match_active_dimension_facet(self):
        # SQL analogue: `dimension = 'X'` on a NULL column is never true.
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets(dimensions=("Completeness",)))
        assert out.by_rule == []


class TestTrends:
    def _multi_run(self):
        return [
            make_row(
                "c1",
                failed=10,
                total=100,
                run_id="r1",
                run_date="2026-07-01 00:00:00",
                severity="High",
                dimension="Completeness",
                rule_id="rule-1",
            ),
            make_row(
                "c2",
                failed=0,
                total=100,
                run_id="r1",
                run_date="2026-07-01 00:00:00",
                severity="Low",
                dimension="Validity",
                rule_id="rule-2",
            ),
            make_row(
                "c1",
                failed=50,
                total=100,
                run_id="r2",
                run_date="2026-07-02 00:00:00",
                severity="High",
                dimension="Completeness",
                rule_id="rule-1",
            ),
        ]

    def test_trend_orders_ascending_by_run_date(self):
        out = compute_entity_results(self._multi_run(), ResultFacets())
        assert [p.run_date for p in out.trend] == ["2026-07-01 00:00:00", "2026-07-02 00:00:00"]
        assert out.trend[0].pass_rate == pytest.approx(1 - 10 / 200)
        assert out.trend[1].pass_rate == pytest.approx(0.5)

    def test_trend_by_dimension_carries_series_and_counts(self):
        out = compute_entity_results(self._multi_run(), ResultFacets())
        first_day = [p for p in out.trend_by_dimension if p.run_date == "2026-07-01 00:00:00"]
        assert {p.series for p in first_day} == {"Completeness", "Validity"}
        completeness = next(p for p in first_day if p.series == "Completeness")
        assert completeness.pass_rate == pytest.approx(0.9)
        assert completeness.rule_count == 1
        assert completeness.total_tests == 100

    def test_trend_counts(self):
        out = compute_entity_results(self._multi_run(), ResultFacets())
        assert out.trend_counts[0].rule_count == 2
        assert out.trend_counts[0].check_count == 2
        assert out.trend_counts[0].test_count == 200
        assert out.trend_counts[1].rule_count == 1

    def test_trend_failures_counts_failed_rules_checks_tests(self):
        out = compute_entity_results(self._multi_run(), ResultFacets())
        day1 = out.trend_failures[0]
        assert day1.failed_rule_count == 1  # only rule-1 failed
        assert day1.failed_check_count == 1  # one check row with failures
        assert day1.failed_test_count == 10
        assert day1.failed_records is None  # no per-run row counts supplied

    def test_trend_failures_sums_failed_records_per_run(self):
        out = compute_entity_results(
            self._multi_run(),
            ResultFacets(),
            failed_records_by_run={(FQN, "r1"): 7, (FQN, "r2"): 3},
        )
        assert out.trend_failures[0].failed_records == 7
        assert out.trend_failures[1].failed_records == 3

    def test_trend_failures_can_ignore_facets(self):
        # dqlake parity: the table reader's trend_failures query filters on
        # binding/run only — never on the drilldown chips.
        facets = ResultFacets(dimensions=("Validity",))
        honoured = compute_entity_results(self._multi_run(), facets)
        ignored = compute_entity_results(self._multi_run(), facets, failures_ignore_facets=True)
        assert honoured.trend_failures[0].failed_test_count == 0  # c1 filtered out
        assert ignored.trend_failures[0].failed_test_count == 10

    def test_trend_by_table_only_on_by_table_axis(self):
        table_out = compute_entity_results(self._multi_run(), ResultFacets(), table_axis="tables")
        assert table_out.trend_by_table == []
        product_out = compute_entity_results(self._multi_run(), ResultFacets(), table_axis="by_table")
        assert {p.series for p in product_out.trend_by_table} == {FQN}


class TestAsOfAverageTrend:
    """The overall ``trend`` is the AS-OF carry-forward average (dqlake's
    mv_product_results / _product_trend semantics): one point per distinct
    run instant across the scope; at each instant every member table
    contributes the pass rate of its most recent run at-or-before that
    instant, and the point is the EQUAL-WEIGHT mean of those carried
    values. dqlake's exclusion choices, pinned here:

    - a table with no run yet at an instant is excluded until its first
      run (inner-join semantics in dqlake's asof CTE);
    - a table whose as-of run has a NULL pass rate (zero tests) is
      excluded at that instant — never substituted with an older run.
    """

    A = "main.sales.orders"
    B = "main.sales.customers"
    T1 = "2026-07-01 00:00:00"
    T2 = "2026-07-02 00:00:00"
    T3 = "2026-07-03 00:00:00"

    def _misaligned(self) -> list[CheckResultRow]:
        # A runs at t1 (0.9) and t3 (1.0); B runs at t2 only (0.5).
        return [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B),
            make_row("c1", failed=0, total=100, run_id="a2", run_date=self.T3, fqn=self.A),
        ]

    def test_average_carries_each_table_forward_at_every_instant(self):
        out = compute_entity_results(self._misaligned(), ResultFacets(), table_axis="by_table")
        assert [p.run_date for p in out.trend] == [self.T1, self.T2, self.T3]
        # t1: only A has run (B excluded until its first run).
        assert out.trend[0].pass_rate == pytest.approx(0.9)
        # t2: A carried forward from t1 (0.9), B's own run (0.5).
        assert out.trend[1].pass_rate == pytest.approx((0.9 + 0.5) / 2)
        # t3: A's t3 run (1.0), B carried forward from t2 (0.5).
        assert out.trend[2].pass_rate == pytest.approx((1.0 + 0.5) / 2)

    def test_average_is_equal_weight_across_tables_not_test_weighted(self):
        rows = [
            make_row("c1", failed=0, total=1000, run_id="a1", run_date=self.T1, fqn=self.A),
            make_row("c1", failed=5, total=10, run_id="b1", run_date=self.T1, fqn=self.B),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        # Mean of the table rates (1.0 and 0.5) — not the pooled 1 - 5/1010.
        assert out.trend[0].pass_rate == pytest.approx(0.75)

    def test_null_rate_asof_run_is_excluded_not_substituted_with_older_run(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A),
            # A's latest run at t2 has no derivable rate (zero/absent totals).
            make_row("c1", failed=0, total=None, run_id="a2", run_date=self.T2, fqn=self.A),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        # t2: A's as-of run is the NULL-rate one — A drops out (dqlake filters
        # NULL AFTER picking the as-of run); only B contributes.
        assert out.trend[1].pass_rate == pytest.approx(0.5)

    def test_point_is_null_when_no_table_has_a_rate_at_the_instant(self):
        rows = [make_row("c1", failed=0, total=None, run_id="a1", run_date=self.T1, fqn=self.A)]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        assert [p.run_date for p in out.trend] == [self.T1]
        assert out.trend[0].pass_rate is None

    def test_facets_scope_the_carried_forward_rates(self):
        rows = [
            make_row(
                "c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Completeness"
            ),
            make_row("c2", failed=90, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Validity"),
            make_row(
                "c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B, dimension="Completeness"
            ),
        ]
        out = compute_entity_results(rows, ResultFacets(dimensions=("Completeness",)), table_axis="by_table")
        # A's carried-forward rate at t2 is its Completeness-only rate (0.9),
        # not the all-checks rate — carry-forward runs over the matched rows.
        assert out.trend[1].pass_rate == pytest.approx((0.9 + 0.5) / 2)

    def test_single_table_scope_degenerates_to_per_run_rates(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="r1", run_date=self.T1),
            make_row("c1", failed=50, total=100, run_id="r2", run_date=self.T2),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="tables")
        assert [(p.run_date, p.pass_rate) for p in out.trend] == [
            (self.T1, pytest.approx(0.9)),
            (self.T2, pytest.approx(0.5)),
        ]

    def test_dateless_rows_form_one_isolated_leading_point(self):
        rows = [
            make_row("c1", failed=50, total=100, run_id="r0", run_date=None, fqn=self.A),
            make_row("c1", failed=0, total=100, run_id="b1", run_date=self.T1, fqn=self.B),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        assert [p.run_date for p in out.trend] == [None, self.T1]
        assert out.trend[0].pass_rate == pytest.approx(0.5)
        # The dateless run cannot be ordered, so it never carries forward.
        assert out.trend[1].pass_rate == pytest.approx(1.0)

    def test_trend_by_table_stays_per_run_instant(self):
        # The dull per-table lines keep their raw per-run points — no
        # carry-forward there (dqlake's trend_by_table is each table's own
        # runs).
        out = compute_entity_results(self._misaligned(), ResultFacets(), table_axis="by_table")
        assert [(p.run_date, p.series) for p in out.trend_by_table] == [
            (self.T1, self.A),
            (self.T2, self.B),
            (self.T3, self.A),
        ]


class TestAxesSlicing:
    def test_trend_axes_returns_empty_breakdowns(self):
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets(), axes="trend")
        assert out.by_dimension == [] and out.by_rule == [] and out.tables == []
        assert out.trend != [] and out.trend_counts != []

    def test_breakdown_axes_returns_empty_trends(self):
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets(), axes="breakdown")
        assert out.trend == [] and out.trend_counts == [] and out.trend_failures == []
        assert out.by_rule != []

    def test_unknown_axes_behaves_as_all(self):
        # dqlake parity: anything other than trend/breakdown selects all.
        rows = [make_row("c1", failed=1, total=10)]
        out = compute_entity_results(rows, ResultFacets(), axes="bogus")
        assert out.by_rule != [] and out.trend != []
