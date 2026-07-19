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
from collections import defaultdict
from dataclasses import replace
from datetime import datetime

import pytest

from databricks_labs_dqx_app.backend.models import TrendPointOut
from databricks_labs_dqx_app.backend.services.dq_results_service import (
    CheckResultRow,
    ResultFacets,
    _by_rule_rows,
    annotate_trend_versions,
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
    run_mode: str | None = None,
    error_count: int = 0,
    warning_count: int = 0,
    criticality: str | None = None,
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
        run_mode=run_mode,
        error_count=error_count,
        warning_count=warning_count,
        criticality=criticality,
    )


def expand_asof(rows: list[CheckResultRow]) -> list[CheckResultRow]:
    """Reference AS-OF expansion mirroring ``v_dq_check_results_asof``.

    The carry-forward consolidation now lives in the UC view (one scope
    partition per include_drafts value — these fixtures model the one
    partition a route ever reads); this helper reproduces its semantics
    over in-memory rows so the misaligned fixtures keep pinning the
    NUMERIC truth of the full SQL-then-service path: for every distinct
    run instant, each table with a run at-or-before that instant
    contributes the rows of its LATEST such run, stamped with the
    instant (``run_date`` = the view's ``as_of_time``). Rows with no
    run_date never enter (the view excludes NULL run_time).
    """
    dated = [row for row in rows if row.run_date is not None]
    runs_by_table: dict[str, dict[str, list[CheckResultRow]]] = defaultdict(dict)
    for row in dated:
        assert row.run_date is not None
        runs_by_table[row.table_fqn].setdefault(row.run_date, []).append(row)
    out: list[CheckResultRow] = []
    for instant in sorted({row.run_date for row in dated if row.run_date is not None}):
        for runs in runs_by_table.values():
            eligible = [run_date for run_date in runs if run_date <= instant]
            if not eligible:
                continue
            out.extend(replace(row, run_date=instant) for row in runs[max(eligible)])
    return out


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
            make_row("c1", failed=10, total=100, run_id="r2", run_date="d2", severity="Critical", dimension="Accuracy"),
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


class TestTableFacet:
    """P7.2: the table facet (clicking a By table row cross-filters the
    OTHER drilldown boxes to that table) — same OR-within / AND-across
    semantics as the other four facets, except the by_table box itself
    SELF-EXCLUDES it (its rows must not vanish when one is clicked)."""

    OTHER = "main.sales.customers"

    def _rows(self):
        return [
            make_row(
                "c1", failed=10, total=100, severity="High", dimension="Completeness", columns=("id",), rule_id="rule-1"
            ),
            make_row(
                "c2",
                failed=20,
                total=100,
                fqn=self.OTHER,
                severity="Low",
                dimension="Validity",
                columns=("amount",),
                rule_id="rule-2",
            ),
        ]

    def test_table_facet_restricts_breakdowns_and_trend(self):
        out = compute_entity_results(self._rows(), ResultFacets(tables=(FQN,)), table_axis="by_table")
        assert [g.label for g in out.by_rule] == ["c1"]
        assert [g.label for g in out.by_dimension] == ["Completeness"]
        assert out.trend[0].pass_rate == pytest.approx(0.9)  # only c1's table counted

    def test_or_within_the_table_facet(self):
        out = compute_entity_results(self._rows(), ResultFacets(tables=(FQN, self.OTHER)), table_axis="by_table")
        assert {g.label for g in out.by_rule} == {"c1", "c2"}

    def test_and_across_with_the_other_facets(self):
        out = compute_entity_results(
            self._rows(), ResultFacets(tables=(FQN,), dimensions=("Validity",)), table_axis="by_table"
        )
        assert out.by_rule == []

    def test_by_table_box_self_excludes_the_table_facet(self):
        # Clicking a By table row must not make the OTHER table rows vanish
        # from the By table box itself — the box stays unfiltered by its own
        # facet (the selection highlight needs the full row set).
        out = compute_entity_results(self._rows(), ResultFacets(tables=(FQN,)), table_axis="by_table")
        assert {g.label for g in out.by_table} == {FQN, self.OTHER}

    def test_by_table_self_exclusion_still_honours_the_other_facets(self):
        # The self-exclusion drops ONLY the table facet: an active dimension
        # chip still cross-filters the By table box as before.
        out = compute_entity_results(
            self._rows(), ResultFacets(tables=(FQN,), dimensions=("Validity",)), table_axis="by_table"
        )
        assert [g.label for g in out.by_table] == [self.OTHER]

    def test_trend_by_table_is_scoped_by_the_table_facet(self):
        # The per-table trend lines are a chart, not the By table box — they
        # cross-filter like every other trend series.
        out = compute_entity_results(self._rows(), ResultFacets(tables=(FQN,)), table_axis="by_table")
        assert {p.series for p in out.trend_by_table} == {FQN}

    def test_table_facet_scopes_the_carried_forward_series(self):
        rows = self._rows()
        out = compute_entity_results(
            rows, ResultFacets(tables=(self.OTHER,)), table_axis="by_table", asof_rows=expand_asof(rows)
        )
        assert out.trend[0].pass_rate == pytest.approx(0.8)  # only c2's table carried

    def test_table_facet_counts_as_active(self):
        assert ResultFacets(tables=(FQN,)).any_active()
        assert not ResultFacets().any_active()


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
    """The overall ``trend`` is the AS-OF carry-forward average. The
    expansion itself is the UC view ``v_dq_check_results_asof`` (modelled
    here by ``expand_asof`` — the misaligned fixtures pin the numeric truth
    of the SQL-then-service path); the service pools each table's carried
    rows per instant and takes the EQUAL-WEIGHT mean across tables
    (dqlake's ``_product_trend``):

    - a table with no run yet at an instant is excluded until its first
      run (inner-join semantics in the view's asof CTE);
    - a table whose as-of rows pool to a NULL pass rate (zero tests) is
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

    def _compute(self, rows: list[CheckResultRow], facets: ResultFacets | None = None):
        return compute_entity_results(
            rows, facets or ResultFacets(), table_axis="by_table", asof_rows=expand_asof(rows)
        )

    def test_average_carries_each_table_forward_at_every_instant(self):
        out = self._compute(self._misaligned())
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
        out = self._compute(rows)
        # Mean of the table rates (1.0 and 0.5) — not the pooled 1 - 5/1010.
        assert out.trend[0].pass_rate == pytest.approx(0.75)

    def test_null_rate_asof_run_is_excluded_not_substituted_with_older_run(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A),
            # A's latest run at t2 has no derivable rate (zero/absent totals).
            make_row("c1", failed=0, total=None, run_id="a2", run_date=self.T2, fqn=self.A),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B),
        ]
        out = self._compute(rows)
        # t2: A's as-of run is the NULL-rate one — A drops out (NULL is
        # filtered AFTER the as-of pick); only B contributes.
        assert out.trend[1].pass_rate == pytest.approx(0.5)

    def test_point_is_null_when_no_table_has_a_rate_at_the_instant(self):
        rows = [make_row("c1", failed=0, total=None, run_id="a1", run_date=self.T1, fqn=self.A)]
        out = self._compute(rows)
        assert [p.run_date for p in out.trend] == [self.T1]
        assert out.trend[0].pass_rate is None

    def test_facets_scope_the_carried_forward_rates(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Completeness"),
            make_row("c2", failed=90, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Validity"),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B, dimension="Completeness"),
        ]
        out = self._compute(rows, ResultFacets(dimensions=("Completeness",)))
        # A's carried-forward rate at t2 is its Completeness-only rate (0.9),
        # not the all-checks rate — facets filter the expansion's rows.
        assert out.trend[1].pass_rate == pytest.approx((0.9 + 0.5) / 2)

    def test_foreign_instants_in_the_expansion_are_dropped(self):
        # The UC view is table-agnostic: its instants span EVERY table, so
        # the expansion can carry this scope's tables at instants where the
        # scope never ran. Those rows must not create points — the series'
        # instants are the scope's own run instants (from the raw rows).
        rows = [make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A)]
        foreign = replace(rows[0], run_date=self.T2)  # carried at another table's instant
        out = compute_entity_results(
            rows, ResultFacets(), table_axis="by_table", asof_rows=[*expand_asof(rows), foreign]
        )
        assert [p.run_date for p in out.trend] == [self.T1]

    def test_single_table_scope_degenerates_to_per_run_rates(self):
        # The table endpoint fetches no expansion (asof_rows=None): its own
        # per-run rows ARE the degenerate expansion.
        rows = [
            make_row("c1", failed=10, total=100, run_id="r1", run_date=self.T1),
            make_row("c1", failed=50, total=100, run_id="r2", run_date=self.T2),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="tables")
        assert [(p.run_date, p.pass_rate) for p in out.trend] == [
            (self.T1, pytest.approx(0.9)),
            (self.T2, pytest.approx(0.5)),
        ]

    def test_dateless_rows_never_enter_the_expansion(self):
        # The view excludes NULL run_time (it cannot be ordered), so a
        # legacy dateless run contributes no as-of point.
        rows = [
            make_row("c1", failed=50, total=100, run_id="r0", run_date=None, fqn=self.A),
            make_row("c1", failed=0, total=100, run_id="b1", run_date=self.T1, fqn=self.B),
        ]
        out = self._compute(rows)
        assert [p.run_date for p in out.trend] == [self.T1]
        assert out.trend[0].pass_rate == pytest.approx(1.0)

    def test_trend_by_table_stays_per_run_instant(self):
        # The dull per-table lines keep their raw per-run points — no
        # carry-forward there (dqlake's trend_by_table is each table's own
        # runs).
        out = self._compute(self._misaligned())
        assert [(p.run_date, p.series) for p in out.trend_by_table] == [
            (self.T1, self.A),
            (self.T2, self.B),
            (self.T3, self.A),
        ]


class TestAsOfGroupedTrends:
    """``trend_by_dimension`` / ``trend_by_severity`` pool the SAME as-of
    expansion per (instant, series) — dqlake's ``_product_trend_grouped``
    over its consolidated view: at each instant every member table
    contributes its latest run's rows, and the series value per group is
    the POOLED rate over those carried rows (1 - SUM(failed)/SUM(total)) —
    NOT the equal-weight mean the overall Average uses (dqlake's grouped
    SQL pools; only its Average AVGs per-table rates).
    """

    A = "main.sales.orders"
    B = "main.sales.customers"
    T1 = "2026-07-01 00:00:00"
    T2 = "2026-07-02 00:00:00"
    T3 = "2026-07-03 00:00:00"

    def _misaligned_dimensions(self) -> list[CheckResultRow]:
        # A runs at t1 (Completeness 0.9, Validity 1.0) and t3 (Completeness
        # 1.0 — Validity dropped from the run); B runs at t2 only
        # (Completeness 0.5).
        return [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Completeness"),
            make_row("c2", failed=0, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Validity"),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B, dimension="Completeness"),
            make_row("c1", failed=0, total=100, run_id="a2", run_date=self.T3, fqn=self.A, dimension="Completeness"),
        ]

    def _compute(self, rows: list[CheckResultRow], facets: ResultFacets | None = None):
        return compute_entity_results(
            rows, facets or ResultFacets(), table_axis="by_table", asof_rows=expand_asof(rows)
        )

    def test_dimension_series_carries_each_table_forward_at_every_instant(self):
        out = self._compute(self._misaligned_dimensions())
        completeness = [p for p in out.trend_by_dimension if p.series == "Completeness"]
        assert [p.run_date for p in completeness] == [self.T1, self.T2, self.T3]
        # t1: only A has run.
        assert completeness[0].pass_rate == pytest.approx(0.9)
        # t2: A carried forward from t1 (10/100) pooled with B's run (50/100).
        assert completeness[1].pass_rate == pytest.approx(1 - 60 / 200)
        # t3: A's t3 run (0/100) pooled with B carried from t2 (50/100).
        assert completeness[2].pass_rate == pytest.approx(1 - 50 / 200)

    def test_grouped_series_pools_counts_not_mean_of_table_rates(self):
        # dqlake's grouped trend is SUM/SUM over the carried rows — with very
        # different test volumes the pooled rate diverges from the mean.
        rows = [
            make_row("c1", failed=0, total=1000, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Completeness"),
            make_row("c1", failed=5, total=10, run_id="b1", run_date=self.T1, fqn=self.B, dimension="Completeness"),
        ]
        out = self._compute(rows)
        point = next(p for p in out.trend_by_dimension if p.series == "Completeness")
        assert point.pass_rate == pytest.approx(1 - 5 / 1010)  # pooled, not (1.0 + 0.5) / 2
        assert point.total_tests == 1010

    def test_carried_rows_pool_rule_counts_across_tables(self):
        out = self._compute(self._misaligned_dimensions())
        completeness = [p for p in out.trend_by_dimension if p.series == "Completeness"]
        # t2 pools A's carried c1 and B's c1 — same check name, so one
        # distinct rule key; total_tests pools both runs.
        assert completeness[1].rule_count == 1
        assert completeness[1].total_tests == 200

    def test_series_absent_from_the_carried_run_does_not_persist(self):
        # The expansion carries each table's WHOLE latest run: A's t3 run has
        # no Validity check, so Validity gets no t3 point from A's older run
        # (the view replaces the table's contribution wholesale).
        out = self._compute(self._misaligned_dimensions())
        validity = [p for p in out.trend_by_dimension if p.series == "Validity"]
        assert [p.run_date for p in validity] == [self.T1, self.T2]

    def test_severity_series_uses_the_same_carry_forward(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, severity="High"),
            make_row("c1", failed=50, total=100, run_id="b1", run_date=self.T2, fqn=self.B, severity="High"),
        ]
        out = self._compute(rows)
        high = [p for p in out.trend_by_severity if p.series == "High"]
        assert [p.run_date for p in high] == [self.T1, self.T2]
        assert high[1].pass_rate == pytest.approx(1 - 60 / 200)

    def test_single_table_scope_degenerates_to_per_run_grouping(self):
        # PINNED: the table endpoint fetches no expansion; its per-run rows
        # group exactly as before.
        rows = [
            make_row("c1", failed=10, total=100, run_id="r1", run_date=self.T1, dimension="Completeness"),
            make_row("c2", failed=0, total=50, run_id="r1", run_date=self.T1, dimension="Validity"),
            make_row("c1", failed=50, total=100, run_id="r2", run_date=self.T2, dimension="Completeness"),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="tables")
        assert [(p.run_date, p.series, p.pass_rate, p.rule_count, p.total_tests) for p in out.trend_by_dimension] == [
            (self.T1, "Completeness", pytest.approx(0.9), 1, 100),
            (self.T1, "Validity", pytest.approx(1.0), 1, 50),
            (self.T2, "Completeness", pytest.approx(0.5), 1, 100),
        ]

    def test_facets_scope_the_carried_forward_groups(self):
        out = self._compute(self._misaligned_dimensions(), ResultFacets(dimensions=("Completeness",)))
        assert {p.series for p in out.trend_by_dimension} == {"Completeness"}
        completeness = [p for p in out.trend_by_dimension if p.series == "Completeness"]
        assert completeness[1].pass_rate == pytest.approx(1 - 60 / 200)


class TestLatestRunBreakdowns:
    """Multi-table (``by_table`` axis) breakdown TABLES reflect exactly each
    member table's LATEST run — dqlake's ``latest_only=True``
    (``v_table_scores.is_latest_for_table``) on every product breakdown (By
    dimension/severity/rule/column/table) — so the numbers match the
    Invalid-samples view (single latest run) instead of stacking every run
    in history. Single-table scope (``tables`` axis) keeps the caller's
    scoping (the monitored-table tab pins a run_id itself).
    """

    A = "main.sales.orders"
    B = "main.sales.customers"
    T1 = "2026-07-01 00:00:00"
    T2 = "2026-07-02 00:00:00"

    def _two_runs_each(self) -> list[CheckResultRow]:
        return [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Completeness"),
            make_row("c1", failed=20, total=100, run_id="a2", run_date=self.T2, fqn=self.A, dimension="Completeness"),
            make_row("c2", failed=5, total=50, run_id="b1", run_date=self.T1, fqn=self.B, severity="High"),
            make_row("c2", failed=0, total=50, run_id="b2", run_date=self.T2, fqn=self.B, severity="High"),
        ]

    def test_by_table_reflects_each_members_latest_run_only(self):
        out = compute_entity_results(self._two_runs_each(), ResultFacets(), table_axis="by_table")
        by_table = {g.label: g for g in out.by_table}
        # A's latest run only (20/100) — not 30/200 stacked across both runs.
        assert by_table[self.A].failed_tests == 20
        assert by_table[self.A].total_tests == 100
        assert by_table[self.B].failed_tests == 0
        assert by_table[self.B].total_tests == 50

    def test_dimension_and_severity_breakdowns_are_latest_run_scoped(self):
        out = compute_entity_results(self._two_runs_each(), ResultFacets(), table_axis="by_table")
        completeness = next(g for g in out.by_dimension if g.label == "Completeness")
        assert completeness.failed_tests == 20
        assert completeness.total_tests == 100
        high = next(g for g in out.by_severity if g.label == "High")
        assert high.failed_tests == 0

    def test_members_on_misaligned_instants_each_contribute_their_own_latest(self):
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T2, fqn=self.A),
            # B's only (and therefore latest) run is OLDER than A's.
            make_row("c2", failed=5, total=50, run_id="b1", run_date=self.T1, fqn=self.B),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        by_table = {g.label: g for g in out.by_table}
        assert by_table[self.A].failed_tests == 10
        assert by_table[self.B].failed_tests == 5

    def test_facet_never_resurrects_an_older_run(self):
        # dqlake's is_latest_for_table is computed BEFORE the facet WHERE: a
        # facet excluding every row of a table's latest run leaves that table
        # empty — it never falls back to an older run that had the facet.
        rows = [
            make_row("c1", failed=10, total=100, run_id="a1", run_date=self.T1, fqn=self.A, dimension="Validity"),
            make_row("c1", failed=0, total=100, run_id="a2", run_date=self.T2, fqn=self.A, dimension="Completeness"),
        ]
        out = compute_entity_results(rows, ResultFacets(dimensions=("Validity",)), table_axis="by_table")
        assert out.by_table == []
        assert out.by_dimension == []

    def test_trends_keep_every_run_in_history(self):
        # dqlake: latest_only applies to the breakdown TABLES only — the
        # over-time series keep the full history.
        out = compute_entity_results(self._two_runs_each(), ResultFacets(), table_axis="by_table")
        assert [p.run_date for p in out.trend] == [self.T1, self.T2]
        assert {p.run_date for p in out.trend_counts} == {self.T1, self.T2}
        assert {p.run_date for p in out.trend_failures} == {self.T1, self.T2}

    def test_single_table_axis_keeps_all_runs_pooled(self):
        # PINNED: the table endpoint's breakdowns are run-scoped by the
        # caller (run_id param); without one they pool history as before.
        rows = [
            make_row("c1", failed=10, total=100, run_id="r1", run_date=self.T1),
            make_row("c1", failed=20, total=100, run_id="r2", run_date=self.T2),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="tables")
        assert out.tables[0].failed_tests == 30
        assert out.tables[0].total_tests == 200

    def test_dateless_only_table_still_appears(self):
        rows = [
            make_row("c1", failed=5, total=50, run_id="r0", run_date=None, fqn=self.A),
            make_row("c1", failed=10, total=100, run_id="b1", run_date=self.T1, fqn=self.B),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table")
        by_table = {g.label: g for g in out.by_table}
        assert by_table[self.A].failed_tests == 5
        assert by_table[self.B].failed_tests == 10


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


class TestAnnotateTrendVersions:
    """`annotate_trend_versions` stamps each overall-trend point with the
    binding version active at its run instant (#65)."""

    @staticmethod
    def _pts(*run_dates: str) -> list[TrendPointOut]:
        return [TrendPointOut(run_date=d, pass_rate=1.0) for d in run_dates]

    def test_stamps_active_version_per_run(self):
        # v1 frozen 06-10, v2 frozen 06-20.
        freezes = [
            (1, datetime.fromisoformat("2026-06-10T00:00:00+00:00")),
            (2, datetime.fromisoformat("2026-06-20T00:00:00+00:00")),
        ]
        pts = self._pts(
            "2026-06-05 00:00:00",  # before any approval -> 0
            "2026-06-15 00:00:00",  # under v1
            "2026-06-25 00:00:00",  # under v2
        )
        annotate_trend_versions(pts, freezes)
        assert [p.version for p in pts] == [0, 1, 2]

    def test_run_exactly_at_freeze_is_inclusive(self):
        freezes = [(1, datetime.fromisoformat("2026-06-10T00:00:00+00:00"))]
        pts = self._pts("2026-06-10 00:00:00")
        annotate_trend_versions(pts, freezes)
        assert pts[0].version == 1

    def test_iso_and_z_suffixed_run_dates_parse(self):
        freezes = [(3, datetime.fromisoformat("2026-06-01T00:00:00+00:00"))]
        pts = [
            TrendPointOut(run_date="2026-06-02T12:00:00Z", pass_rate=1.0),
            TrendPointOut(run_date="2026-06-02T12:00:00+00:00", pass_rate=1.0),
        ]
        annotate_trend_versions(pts, freezes)
        assert [p.version for p in pts] == [3, 3]

    def test_freeze_order_independent(self):
        freezes = [
            (2, datetime.fromisoformat("2026-06-20T00:00:00+00:00")),
            (1, datetime.fromisoformat("2026-06-10T00:00:00+00:00")),
        ]
        pts = self._pts("2026-06-25 00:00:00")
        annotate_trend_versions(pts, freezes)
        assert pts[0].version == 2

    def test_no_dated_freezes_is_noop(self):
        pts = self._pts("2026-06-25 00:00:00")
        annotate_trend_versions(pts, [(1, None)])
        assert pts[0].version is None

    def test_unparseable_run_date_left_untouched(self):
        freezes = [(1, datetime.fromisoformat("2026-06-10T00:00:00+00:00"))]
        pts = [TrendPointOut(run_date="not-a-date", pass_rate=1.0)]
        annotate_trend_versions(pts, freezes)
        assert pts[0].version is None


# ---------------------------------------------------------------------------
# Run-batch consolidation (B2-18 + B2-5): concurrent member runs of one
# Table-Space "Run now" collapse onto a single batch instant. The batch key
# is COALESCE(run_set_id, run_id); the batch instant is the batch's last
# run_date (MAX run_time). Only the by_table axis consolidates (dqlake).
# ---------------------------------------------------------------------------


class TestBatchConsolidation:
    A = "main.sales.orders"
    B = "main.sales.customers"

    def test_trend_by_table_collapses_two_concurrent_members_to_one_instant(self):
        # Two member tables ran in one batch (shared run set) but landed at
        # DISTINCT run_times: their markers must share the batch instant
        # (the batch's MAX run_time) so the trend tooltip lists both (B2-5).
        rows = [
            make_row(fqn=self.A, run_id="ra", run_date="2026-07-01 10:00:00", failed=0, total=100),
            make_row(fqn=self.B, run_id="rb", run_date="2026-07-01 10:05:00", failed=10, total=100),
        ]
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="by_table",
            run_set_by_run_id={"ra": "set1", "rb": "set1"},
            asof_rows=rows,
        )
        instants = {p.run_date for p in out.trend_by_table}
        assert instants == {"2026-07-01 10:05:00"}  # single batch instant = MAX(run_time)
        assert {p.series for p in out.trend_by_table} == {self.A, self.B}

    def test_coalesce_fallback_keeps_unsetted_runs_on_their_own_instant(self):
        # No run-set membership -> COALESCE(run_set_id, run_id) degenerates to
        # the bare run_id, so each run stays its own batch (unconsolidated).
        rows = [
            make_row(fqn=self.A, run_id="ra", run_date="2026-07-01 10:00:00"),
            make_row(fqn=self.B, run_id="rb", run_date="2026-07-01 10:05:00"),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table", asof_rows=rows)
        assert {p.run_date for p in out.trend_by_table} == {
            "2026-07-01 10:00:00",
            "2026-07-01 10:05:00",
        }

    def test_by_table_snapshot_uses_batch_instant_for_latest(self):
        # Table A's latest run and table B's only run share one batch; an
        # earlier A run must be excluded from the by_table snapshot (each
        # table's latest run WITHIN the latest batch).
        rows = [
            make_row(fqn=self.A, run_id="ra0", run_date="2026-07-01 09:00:00", failed=99, total=100),
            make_row(fqn=self.A, run_id="ra1", run_date="2026-07-02 10:00:00", failed=0, total=100),
            make_row(fqn=self.B, run_id="rb1", run_date="2026-07-02 10:05:00", failed=10, total=100),
        ]
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="by_table",
            run_set_by_run_id={"ra1": "set2", "rb1": "set2"},
            asof_rows=rows,
        )
        by_table = {g.label: g.pass_rate for g in out.by_table}
        assert by_table[self.A] == pytest.approx(1.0)  # the batch run, not the 99-fail earlier one
        assert by_table[self.B] == pytest.approx(1 - 10 / 100)

    def test_single_table_axis_is_unaffected_by_run_sets(self):
        # The single-table endpoint (table_axis="tables") never consolidates:
        # a run-set map must not change its `tables` grouping or resurrect an
        # older run. MT single-table behaviour stays exactly as before.
        rows = [
            make_row(run_id="r1", run_date="2026-07-01 00:00:00", failed=5, total=100),
            make_row(run_id="r2", run_date="2026-07-02 00:00:00", failed=10, total=100),
        ]
        baseline = compute_entity_results(rows, ResultFacets(), table_axis="tables", asof_rows=rows)
        with_sets = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="tables",
            run_set_by_run_id={"r1": "s1", "r2": "s1"},
            asof_rows=rows,
        )
        assert [t.model_dump() for t in with_sets.tables] == [t.model_dump() for t in baseline.tables]
        assert with_sets.trend_by_table == baseline.trend_by_table == []
        assert [p.model_dump() for p in with_sets.trend] == [p.model_dump() for p in baseline.trend]

    def test_as_of_batch_caps_trend_to_chosen_batch_instant(self):
        # as_of_batch (a run_id) caps the series to batches at/-before that
        # batch's instant — the newest run is truncated.
        rows = [
            make_row(run_id="r1", run_date="2026-07-01 00:00:00", failed=0, total=100),
            make_row(run_id="r2", run_date="2026-07-02 00:00:00", failed=0, total=100),
            make_row(run_id="r3", run_date="2026-07-03 00:00:00", failed=0, total=100),
        ]
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="by_table",
            as_of_batch="r2",
            asof_rows=rows,
        )
        assert {p.run_date for p in out.trend_by_table} == {
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
        }
        # Overall Average also truncates to the chosen batch.
        assert {p.run_date for p in out.trend} == {
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
        }

    def test_as_of_batch_resolves_a_member_run_id_to_its_batch(self):
        # The chosen run_id can be ANY member of the batch; capping uses the
        # batch instant (its last run_time), not the member's own run_time.
        rows = [
            make_row(fqn=self.A, run_id="ra", run_date="2026-07-02 10:00:00", failed=0, total=100),
            make_row(fqn=self.B, run_id="rb", run_date="2026-07-02 10:05:00", failed=0, total=100),
            make_row(fqn=self.A, run_id="ra2", run_date="2026-07-03 00:00:00", failed=0, total=100),
        ]
        # as_of_batch = "ra" (the EARLIER member); its batch instant is 10:05
        # (rb), so rb stays in scope even though its own run_time is later.
        out = compute_entity_results(
            rows,
            ResultFacets(),
            table_axis="by_table",
            run_set_by_run_id={"ra": "set3", "rb": "set3"},
            as_of_batch="ra",
            asof_rows=rows,
        )
        assert {p.run_date for p in out.trend_by_table} == {"2026-07-02 10:05:00"}
        assert {p.series for p in out.trend_by_table} == {self.A, self.B}


class TestTrendDraftFlag:
    """B2-136: trend points carry an is_draft flag from their run(s)' provenance."""

    def test_parse_check_rows_reads_run_mode(self):
        rows = parse_check_rows(
            [
                {
                    "input_location": FQN,
                    "run_id": "r1",
                    "run_date": "2026-07-01 00:00:00",
                    "check_name": "c1",
                    "error_count": "0",
                    "warning_count": "0",
                    "input_row_count": "100",
                    "run_mode": "draft",
                }
            ]
        )
        assert rows[0].run_mode == "draft"

    def test_published_points_are_not_draft(self):
        rows = [make_row(run_id="r1", run_date="2026-07-01 00:00:00", run_mode="published")]
        out = compute_entity_results(rows, ResultFacets())
        assert out.trend[0].is_draft is False

    def test_draft_run_marks_overall_and_grouped_points(self):
        rows = [
            make_row(
                run_id="r1",
                run_date="2026-07-02 00:00:00",
                run_mode="draft",
                dimension="Completeness",
            )
        ]
        out = compute_entity_results(rows, ResultFacets())
        assert out.trend[0].is_draft is True
        assert all(p.is_draft for p in out.trend_by_dimension)

    def test_mixed_instant_is_draft_when_any_contributing_run_is_draft(self):
        # Two tables run at the same instant; one draft, one published. The
        # pooled overall point is conservatively marked draft.
        rows = [
            make_row(fqn="c.s.a", run_id="ra", run_date="2026-07-02 00:00:00", run_mode="published"),
            make_row(fqn="c.s.b", run_id="rb", run_date="2026-07-02 00:00:00", run_mode="draft"),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="by_table", asof_rows=rows)
        assert out.trend[0].is_draft is True

    def test_untagged_legacy_run_is_not_draft(self):
        # The shaping view resolves untagged runs to 'published', so a None
        # run_mode must not mark a point draft.
        rows = [make_row(run_id="r1", run_date="2026-07-01 00:00:00", run_mode=None)]
        out = compute_entity_results(rows, ResultFacets())
        assert out.trend[0].is_draft is False


class TestByRuleRowsRuleName:
    def test_by_rule_labels_with_rule_name_not_suffixed_check_name(self):
        # Two per-column checks of ONE rule: distinct suffixed check_names, same
        # registry_rule_id, same underlying rule_name.
        rows = [
            CheckResultRow(
                table_fqn="c.s.t",
                run_id="r1",
                run_date="2026-07-15",
                check_name="Column is not null (a)",
                failed=2,
                total=100,
                severity="High",
                dimension="Completeness",
                columns=("a",),
                rule_id="rid1",
                run_mode="published",
                rule_name="Column is not null",
            ),
            CheckResultRow(
                table_fqn="c.s.t",
                run_id="r1",
                run_date="2026-07-15",
                check_name="Column is not null (b)",
                failed=0,
                total=100,
                severity="High",
                dimension="Completeness",
                columns=("b",),
                rule_id="rid1",
                run_mode="published",
                rule_name="Column is not null",
            ),
        ]
        groups = _by_rule_rows(rows)
        assert len(groups) == 1
        assert groups[0].label == "Column is not null"
        assert groups[0].rule_id == "rid1"

    def test_by_rule_falls_back_to_check_name_when_rule_name_absent(self):
        rows = [
            CheckResultRow(
                table_fqn="c.s.t",
                run_id="r1",
                run_date="2026-07-15",
                check_name="a_is_null",
                failed=1,
                total=10,
                severity=None,
                dimension=None,
                columns=("a",),
                rule_id=None,
                run_mode="published",
                rule_name=None,
            )
        ]
        groups = _by_rule_rows(rows)
        assert groups[0].label == "a_is_null"


class TestBreach:
    """Per-check breach evaluation + criticality-aware group/run/trend roll-ups.

    A check breaches when its pass rate (``1 - failed/total``) falls below
    the resolved threshold. Breaches roll up into every drill-down group
    ("any child breached"), carrying the worst breaching child's DQX
    criticality (error beats warn). A fixed resolver keeps these tests
    DB-free.
    """

    @staticmethod
    def _resolver(threshold: int):
        return lambda _row: threshold

    def test_check_below_threshold_breaches_group(self):
        # 20 failed / 100 -> 80% pass < 90 threshold -> breach.
        rows = [make_row(check="c1", failed=20, total=100, error_count=20, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is True
        assert out.by_rule[0].breach_criticality == "error"

    def test_check_at_or_above_threshold_does_not_breach(self):
        # 10 failed / 100 -> 90% pass, threshold 90 -> pass_rate*100 == threshold, NOT < -> no breach.
        rows = [make_row(check="c1", failed=10, total=100, error_count=10, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_exact_tie_does_not_breach_small_counts(self):
        # 8 failed / 10 -> 20% pass EXACTLY meets a 20 threshold; strict-< means
        # NOT a breach. Float math ((1 - 8/10)*100 = 19.999...) would wrongly
        # breach — this pins the integer-arithmetic fix on the small counts
        # sample-limited draft runs produce.
        rows = [make_row(check="c1", failed=8, total=10, error_count=8, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(20))
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_just_below_tie_breaches_small_counts(self):
        # 9 failed / 10 -> 10% pass < 20 threshold -> breach.
        rows = [make_row(check="c1", failed=9, total=10, error_count=9, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(20))
        assert out.by_rule[0].breached is True
        assert out.by_rule[0].breach_criticality == "error"

    def test_warn_criticality_breach_yields_warn(self):
        rows = [make_row(check="c1", failed=20, total=100, warning_count=20, criticality="warn")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is True
        assert out.by_rule[0].breach_criticality == "warn"

    def test_none_criticality_defaults_to_error(self):
        # A check with no criticality attribution defaults to DQX's "error".
        rows = [make_row(check="c1", failed=20, total=100, error_count=20, criticality=None)]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is True
        assert out.by_rule[0].breach_criticality == "error"

    def test_mixed_error_and_warn_breaches_yield_error(self):
        # Two checks share a dimension; one breaches as warn, one as error.
        rows = [
            make_row(check="c1", failed=20, total=100, warning_count=20, criticality="warn", dimension="Completeness"),
            make_row(check="c2", failed=20, total=100, error_count=20, criticality="error", dimension="Completeness"),
        ]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_dimension[0].breached is True
        assert out.by_dimension[0].breach_criticality == "error"

    def test_clean_group_not_breached(self):
        rows = [
            make_row(check="c1", failed=0, total=100, criticality="error"),
            make_row(check="c2", failed=5, total=100, criticality="error"),  # 95% >= 90
        ]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_total_zero_never_breaches(self):
        rows = [make_row(check="c1", failed=0, total=0, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_total_none_never_breaches(self):
        rows = [make_row(check="c1", failed=0, total=None, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=self._resolver(90))
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_by_column_breach_uses_per_column_threshold_after_explosion(self):
        # One check maps two columns; on the by-column path the resolver keys
        # off the EXPLODED single-column row so each column is evaluated with
        # its own threshold. (Other axes call the resolver with the full row,
        # so the resolver must tolerate a multi-column row too.)
        seen_by_column: list[int] = []

        def resolver(row: CheckResultRow) -> int:
            if len(row.columns) == 1:
                seen_by_column.append(len(row.columns))
                return 95 if row.columns[0] == "strict_col" else 50
            return 0  # multi-column rows (other axes) — never breach here

        rows = [
            make_row(
                check="c1", failed=20, total=100, error_count=20, criticality="error", columns=("strict_col", "lax_col")
            ),
        ]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=resolver)
        by_col = {g.label: g for g in out.by_column}
        # 80% pass: breaches under 95 (strict_col) but not under 50 (lax_col).
        assert by_col["strict_col"].breached is True
        assert by_col["strict_col"].breach_criticality == "error"
        assert by_col["lax_col"].breached is False
        assert seen_by_column  # the exploded single-column path was exercised

    def test_run_and_trend_points_carry_breach(self):
        rows = [
            make_row(check="c1", failed=20, total=100, error_count=20, criticality="error", run_date="2026-07-01 00:00:00"),
        ]
        out = compute_entity_results(rows, ResultFacets(), table_axis="tables", resolve_threshold=self._resolver(90))
        assert out.trend[0].breached is True
        assert out.trend[0].breach_criticality == "error"

    def test_default_resolver_never_breaches(self):
        # No resolver passed -> backward-compatible: breach fields stay falsy.
        rows = [make_row(check="c1", failed=99, total=100, error_count=99, criticality="error")]
        out = compute_entity_results(rows, ResultFacets())
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None

    def test_disabled_resolver_none_no_breach_even_with_failing_checks(self):
        # Simulates the server-side disable path: dq_results.py passes
        # resolve_threshold=None when pass_threshold_enabled is False.
        # Even a 99%-failed check must produce no breach fields.
        rows = [make_row(check="c1", failed=99, total=100, error_count=99, criticality="error")]
        out = compute_entity_results(rows, ResultFacets(), resolve_threshold=None)
        assert out.by_rule[0].breached is False
        assert out.by_rule[0].breach_criticality is None
