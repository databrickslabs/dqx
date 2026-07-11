"""Pure aggregation logic for the dq-results endpoints (dqlake-shape port).

The dqlake original computes its breakdowns/trends in SQL over an enriched
fact table (``run_check_totals`` joined to versioned rule/mapping dims).
DQX Studio's per-check facts live in the UC shaping view
``v_dq_check_results`` (one row per run x table x check), which carries the
AS-OF-THE-RUN attribution on every row: severity tag, quality dimension,
mapped columns, and registry rule id parsed from the run's own frozen
``dq_validation_runs.checks_json`` rendered rule set (see
``services.score_view_service``). Attribution is therefore VERSION-ACCURATE
by construction — editing or renaming a rule's tags today never rewrites
historical results — and this module needs no live join to the binding's
current applied-rule metadata:

1. The route fetches the raw check rows via SQL (``parse_check_rows``).
2. ``compute_entity_results`` filters by the active facets and groups
   every axis, mirroring dqlake's SQL semantics (documented per helper).

Rows from runs without a frozen rule set (legacy pre-checks_json runs) or
from checks that carry no tags (hand-authored checks, synthesized SQL-check
payloads) arrive with NULL attribution and land in the untagged (NULL
label) bucket.

Everything in this module is pure (no I/O) so the aggregation semantics
are unit-testable without a warehouse.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from databricks_labs_dqx_app.backend.metrics_utils import safe_int
from databricks_labs_dqx_app.backend.models import (
    EntityResultsOut,
    GroupRowOut,
    TrendCountPointOut,
    TrendFailurePointOut,
    TrendPointOut,
)

VALID_AXES = ("all", "trend", "breakdown")


@dataclass(frozen=True)
class CheckResultRow:
    """One ``v_dq_check_results`` row: a check's outcome in one run.

    *severity* / *dimension* / *columns* / *rule_id* are the check's
    as-of-run attribution (frozen into the run's ``checks_json`` at
    materialization time); all-None/empty for untagged checks.
    """

    table_fqn: str
    run_id: str | None
    run_date: str | None
    check_name: str
    failed: int
    total: int | None
    severity: str | None = None
    dimension: str | None = None
    columns: tuple[str, ...] = ()
    rule_id: str | None = None


@dataclass(frozen=True)
class ResultFacets:
    """Active drilldown filters: OR within a facet, AND across facets."""

    dimensions: tuple[str, ...] = ()
    severities: tuple[str, ...] = ()
    rules: tuple[str, ...] = ()
    columns: tuple[str, ...] = ()

    def any_active(self) -> bool:
        return bool(self.dimensions or self.severities or self.rules or self.columns)


def _parse_columns_json(raw: str | None) -> tuple[str, ...]:
    """Parse the view's ``to_json(columns)`` array; empty on absent/corrupt."""
    if not raw or raw == "null":
        return ()
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(c) for c in parsed if c is not None)


def parse_check_rows(raw_rows: list[dict[str, str | None]]) -> list[CheckResultRow]:
    """Parse Statement-Execution-shaped ``v_dq_check_results`` rows.

    Placeholder rows (a run with no per-check breakdown — ``check_name``
    NULL) are dropped: they carry no test counts and belong to no axis.
    The attribution columns are optional: NULLs (legacy runs, untagged
    checks) parse to None/empty and land in the untagged bucket.
    """
    out: list[CheckResultRow] = []
    for row in raw_rows:
        check_name = row.get("check_name")
        fqn = row.get("input_location")
        if not check_name or not fqn:
            continue
        failed = (safe_int(row.get("error_count")) or 0) + (safe_int(row.get("warning_count")) or 0)
        out.append(
            CheckResultRow(
                table_fqn=fqn,
                run_id=row.get("run_id"),
                run_date=row.get("run_date"),
                check_name=check_name,
                failed=failed,
                total=safe_int(row.get("input_row_count")),
                severity=row.get("severity"),
                dimension=row.get("dimension"),
                columns=_parse_columns_json(row.get("columns_json")),
                rule_id=row.get("registry_rule_id"),
            )
        )
    return out


def _rule_key(row: CheckResultRow) -> str:
    """Distinct-rule counting key: the frozen registry rule id when the run
    carried one, else the check name (mirrors dqlake's
    COUNT(DISTINCT rule_id))."""
    return row.rule_id or row.check_name


def row_matches_facets(row: CheckResultRow, facets: ResultFacets) -> bool:
    """dqlake facet semantics: OR within a facet, AND across facets.

    Untagged checks (attribution field None) never match an active
    dimension/severity facet — the SQL analogue is ``col = 'v'`` on a
    NULL column. The column facet is a membership test over the check's
    as-of-run mapped columns. The rule facet is keyed on the check name
    (the registry rule's name tag, frozen at materialization time).
    """
    if facets.dimensions and row.dimension not in facets.dimensions:
        return False
    if facets.severities and row.severity not in facets.severities:
        return False
    if facets.rules and row.check_name not in facets.rules:
        return False
    if facets.columns and not any(c in facets.columns for c in row.columns):
        return False
    return True


@dataclass
class _GroupAcc:
    failed: int = 0
    total: int | None = None
    rule_keys: set[str] = field(default_factory=set)
    check_rows: int = 0

    def add(self, row: CheckResultRow) -> None:
        self.failed += row.failed
        if row.total is not None:
            self.total = (self.total or 0) + row.total
        self.rule_keys.add(_rule_key(row))
        self.check_rows += 1

    @property
    def pass_rate(self) -> float | None:
        # SQL analogue: 1 - SUM(failed) / NULLIF(SUM(total), 0).
        if not self.total:
            return None
        return 1 - self.failed / self.total


def _group_rows(
    rows: list[CheckResultRow],
    key_of: Callable[[CheckResultRow], str | None],
    *,
    with_check_count: bool = True,
) -> list[GroupRowOut]:
    groups: dict[str | None, _GroupAcc] = defaultdict(_GroupAcc)
    for row in rows:
        groups[key_of(row)].add(row)
    out = [
        GroupRowOut(
            label=label,
            pass_rate=acc.pass_rate,
            failed_tests=acc.failed,
            rule_count=len(acc.rule_keys),
            check_count=acc.check_rows if with_check_count else None,
            total_tests=acc.total,
        )
        for label, acc in groups.items()
    ]
    # dqlake: ORDER BY failed_tests DESC.
    out.sort(key=lambda g: (g.failed_tests or 0), reverse=True)
    return out


def _by_column_rows(rows: list[CheckResultRow]) -> list[GroupRowOut]:
    """By-column breakdown EXPLODES the mapped columns: a check spanning N
    columns attributes to each of them (rows can sum above the total —
    dqlake's intended "involvement" view). Checks with no mapped columns
    don't appear (SQL analogue: explode of a NULL array yields no rows).
    dqlake's by_column query computes no check_count."""
    exploded = [replace(row, columns=(column,)) for row in rows for column in row.columns]
    return _group_rows(exploded, lambda row: row.columns[0], with_check_count=False)


def _trend(rows: list[CheckResultRow]) -> list[TrendPointOut]:
    """AS-OF carry-forward average — the overall "Average" series.

    dqlake's product-trend semantics (its ``_product_trend`` reader /
    ``v_product_check_consolidated`` view): one point per distinct run
    instant across the scope; at each instant every member table
    contributes the pass rate of its most recent run at-or-before that
    instant, and the point is the EQUAL-WEIGHT mean of those carried
    values. Exclusion choices follow dqlake exactly:

    - a table with no run yet at an instant is excluded until its first
      run (the inner join in dqlake's asof CTE yields it no row);
    - a table whose as-of run has a NULL pass rate (zero tests) is
      excluded at that instant — never substituted with an older run
      (dqlake filters NULL AFTER picking the as-of run, rn = 1).

    For a single-table scope this degenerates to the table's own per-run
    rates (its as-of run at each of its instants is that run). Computed
    in Python over the already-fetched rows rather than as warehouse SQL:
    every axis shares one fetch, the row counts are page-sized, and the
    pure function keeps the carry-forward math unit-testable (the SQL
    analogue is documented in the Genie space's curated as-of example).

    Rows with no run_date (defensive — legacy/corrupt) cannot be ordered,
    so they form one isolated leading point and never carry forward.
    """
    # (table -> run instant -> pooled accumulator) at the run grain.
    per_table: dict[str, dict[str | None, _GroupAcc]] = defaultdict(lambda: defaultdict(_GroupAcc))
    for row in rows:
        per_table[row.table_fqn][row.run_date].add(row)

    out: list[TrendPointOut] = []
    dateless = [runs[None].pass_rate for runs in per_table.values() if None in runs]
    if dateless:
        rated = [rate for rate in dateless if rate is not None]
        out.append(TrendPointOut(run_date=None, pass_rate=sum(rated) / len(rated) if rated else None))

    # Each table's dated runs, ascending — the carry-forward walk below
    # advances a per-table cursor instead of re-scanning per instant.
    series: dict[str, list[tuple[str, float | None]]] = {
        fqn: sorted((run_date, acc.pass_rate) for run_date, acc in runs.items() if run_date is not None)
        for fqn, runs in per_table.items()
    }
    instants = sorted({run_date for table_runs in series.values() for run_date, _ in table_runs})
    positions: dict[str, int] = dict.fromkeys(series, 0)
    carried: dict[str, float | None] = {}
    for instant in instants:
        for fqn, table_runs in series.items():
            i = positions[fqn]
            while i < len(table_runs) and table_runs[i][0] <= instant:
                carried[fqn] = table_runs[i][1]
                i += 1
            positions[fqn] = i
        values = [rate for rate in carried.values() if rate is not None]
        out.append(TrendPointOut(run_date=instant, pass_rate=sum(values) / len(values) if values else None))
    return out


def _trend_grouped(
    rows: list[CheckResultRow],
    series_of: Callable[[CheckResultRow], str | None],
) -> list[TrendPointOut]:
    groups: dict[tuple[str | None, str | None], _GroupAcc] = defaultdict(_GroupAcc)
    for row in rows:
        groups[(row.run_date, series_of(row))].add(row)
    return [
        TrendPointOut(
            run_date=run_date,
            series=series,
            pass_rate=acc.pass_rate,
            rule_count=len(acc.rule_keys),
            total_tests=acc.total,
        )
        for (run_date, series), acc in sorted(groups.items(), key=lambda kv: (kv[0][0] or "", kv[0][1] or ""))
    ]


def _trend_counts(rows: list[CheckResultRow]) -> list[TrendCountPointOut]:
    groups: dict[str | None, _GroupAcc] = defaultdict(_GroupAcc)
    for row in rows:
        groups[row.run_date].add(row)
    return [
        TrendCountPointOut(
            run_date=run_date,
            rule_count=len(acc.rule_keys),
            check_count=acc.check_rows,
            test_count=acc.total,
        )
        for run_date, acc in sorted(groups.items(), key=lambda kv: kv[0] or "")
    ]


def _trend_failures(
    rows: list[CheckResultRow],
    failed_records_by_run: dict[tuple[str, str], int | None],
) -> list[TrendFailurePointOut]:
    """Per run instant: failed rules (distinct), failed checks (rows with
    >=1 failed test), failed tests (sum), and failed records.

    *failed_records_by_run* maps ``(table_fqn, run_id)`` to the run's
    distinct failing-row count (``input_row_count - valid_row_count``
    from dq_metrics — rows carrying any error or warning). Summed across
    the distinct runs at one instant; None when no run at the instant has
    a derivable count (dqlake reads a persisted ``failed_records`` column
    we don't have)."""
    by_date: dict[str | None, list[CheckResultRow]] = defaultdict(list)
    for row in rows:
        by_date[row.run_date].append(row)
    out: list[TrendFailurePointOut] = []
    for run_date in sorted(by_date, key=lambda d: d or ""):
        rows_here = by_date[run_date]
        failed_rules = {_rule_key(row) for row in rows_here if row.failed > 0}
        runs_here = {(row.table_fqn, row.run_id) for row in rows_here if row.run_id}
        record_counts = [count for key in runs_here if (count := failed_records_by_run.get(key)) is not None]
        out.append(
            TrendFailurePointOut(
                run_date=run_date,
                failed_rule_count=len(failed_rules),
                failed_check_count=sum(1 for row in rows_here if row.failed > 0),
                failed_test_count=sum(row.failed for row in rows_here),
                failed_records=sum(record_counts) if record_counts else None,
            )
        )
    return out


def compute_entity_results(
    rows: list[CheckResultRow],
    facets: ResultFacets,
    *,
    axes: str = "all",
    table_axis: str = "tables",
    failed_records_by_run: dict[tuple[str, str], int | None] | None = None,
    failures_ignore_facets: bool = False,
    binding_ids_by_table: dict[str, str] | None = None,
) -> EntityResultsOut:
    """Assemble the full EntityResultsOut from raw check rows.

    *table_axis* selects where the per-table grouping lands: ``"tables"``
    for the table endpoint, ``"by_table"`` for product/global/rule
    (dqlake parity — its table reader fills ``tables``, its product
    reader fills ``by_table``/``trend_by_table``). *axes* mirrors
    dqlake's slice selection: ``"trend"`` computes only the over-time
    series, ``"breakdown"`` only the groupings; unrequested keys stay
    empty so the shape is stable.

    *failures_ignore_facets* mirrors dqlake's table reader, whose
    trend_failures query filters on binding/run only — never on the
    dimension/severity/rule/column chips (the product reader does honour
    them).

    *binding_ids_by_table* (table_fqn -> monitored-table binding id)
    enriches the ``by_table`` rows with an additive *binding_id* so the
    UI can link each row to its monitored-table page. Tables absent from
    the map keep None. Only the ``by_table`` axis is enriched — the
    single-table endpoint's ``tables`` axis has no linking use case.
    """
    if axes not in VALID_AXES:
        axes = "all"  # dqlake parity: anything else selects every slice
    matched = [row for row in rows if row_matches_facets(row, facets)]
    failure_rows = rows if failures_ignore_facets else matched

    result = EntityResultsOut()
    records_by_run = failed_records_by_run or {}
    if axes in ("all", "breakdown"):
        result.by_dimension = _group_rows(matched, lambda row: row.dimension)
        result.by_severity = _group_rows(matched, lambda row: row.severity)
        result.by_rule = _group_rows(matched, lambda row: row.check_name)
        result.by_column = _by_column_rows(matched)
        table_groups = _group_rows(matched, lambda row: row.table_fqn)
        if table_axis == "by_table":
            if binding_ids_by_table:
                for group in table_groups:
                    if group.label is not None:
                        group.binding_id = binding_ids_by_table.get(group.label)
            result.by_table = table_groups
        else:
            result.tables = table_groups
    if axes in ("all", "trend"):
        result.trend = _trend(matched)
        result.trend_by_dimension = _trend_grouped(matched, lambda row: row.dimension)
        result.trend_by_severity = _trend_grouped(matched, lambda row: row.severity)
        if table_axis == "by_table":
            result.trend_by_table = _trend_grouped(matched, lambda row: row.table_fqn)
        result.trend_counts = _trend_counts(matched)
        result.trend_failures = _trend_failures(failure_rows, records_by_run)
    return result
