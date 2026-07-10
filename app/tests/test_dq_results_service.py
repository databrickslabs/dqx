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
