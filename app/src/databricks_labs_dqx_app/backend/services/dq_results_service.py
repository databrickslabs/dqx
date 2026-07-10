"""Pure aggregation logic for the dq-results endpoints (dqlake-shape port).

The dqlake original computes its breakdowns/trends in SQL over an enriched
fact table (``run_check_totals`` joined to versioned rule/mapping dims).
DQX Studio's per-check facts live in the UC shaping view
``v_dq_check_results`` (one row per run x table x check, counts only), and
the rule metadata (severity tag, quality dimension, column mapping) lives
in the app's OLTP store — so the join happens here, in Python:

1. The route fetches the raw check rows via SQL (``parse_check_rows``).
2. Each check row is attributed to its applied-rule metadata by CHECK
   NAME — the materializer writes the registry rule's ``name`` tag as the
   materialized check's ``name``, which the metrics observer echoes back
   as ``check_name`` (``build_attribution_map``).
3. ``compute_entity_results`` filters by the active facets and groups
   every axis, mirroring dqlake's SQL semantics (documented per helper).

Everything in this module is pure (no I/O) so the aggregation semantics
are unit-testable without a warehouse.
"""

from __future__ import annotations

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
from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableDetail

VALID_AXES = ("all", "trend", "breakdown")


@dataclass(frozen=True)
class CheckAttribution:
    """Metadata attributed to one materialized check via its check name."""

    rule_id: str | None = None
    rule_name: str | None = None
    severity: str | None = None
    dimension: str | None = None
    columns: tuple[str, ...] = ()


_UNATTRIBUTED = CheckAttribution()


@dataclass(frozen=True)
class CheckResultRow:
    """One ``v_dq_check_results`` row: a check's outcome in one run."""

    table_fqn: str
    run_id: str | None
    run_date: str | None
    check_name: str
    failed: int
    total: int | None


@dataclass(frozen=True)
class ResultFacets:
    """Active drilldown filters: OR within a facet, AND across facets."""

    dimensions: tuple[str, ...] = ()
    severities: tuple[str, ...] = ()
    rules: tuple[str, ...] = ()
    columns: tuple[str, ...] = ()

    def any_active(self) -> bool:
        return bool(self.dimensions or self.severities or self.rules or self.columns)


def build_attribution_map(detail: MonitoredTableDetail) -> dict[str, CheckAttribution]:
    """Map check name -> applied-rule metadata for one monitored table.

    The materialized check's name is the registry rule's ``name`` tag
    (see ``services.materializer.render_check``), so the join key is the
    rule name. The effective severity honours the per-application
    ``severity_override``. When the same rule is applied twice to one
    binding (different mapping groups), the mapped columns are merged —
    the run's metrics carry a single check name either way.
    """
    out: dict[str, CheckAttribution] = {}
    for summary in detail.applied_rules:
        applied = summary.applied_rule
        name = summary.rule_name
        if not name:
            continue  # unnamed rule -> its check name is DQX-generated; not attributable
        columns: list[str] = []
        for group in applied.column_mapping:
            for value in group.values():
                if value and value not in columns:
                    columns.append(value)
        existing = out.get(name)
        if existing is not None:
            merged = list(existing.columns) + [c for c in columns if c not in existing.columns]
            out[name] = replace(existing, columns=tuple(merged))
            continue
        out[name] = CheckAttribution(
            rule_id=applied.rule_id,
            rule_name=name,
            severity=applied.severity_override or summary.rule_severity,
            dimension=summary.rule_dimension,
            columns=tuple(columns),
        )
    return out


def parse_check_rows(raw_rows: list[dict[str, str | None]]) -> list[CheckResultRow]:
    """Parse Statement-Execution-shaped ``v_dq_check_results`` rows.

    Placeholder rows (a run with no per-check breakdown — ``check_name``
    NULL) are dropped: they carry no test counts and belong to no axis.
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
            )
        )
    return out


def _attr_of(row: CheckResultRow, attributions: dict[str, dict[str, CheckAttribution]]) -> CheckAttribution:
    return attributions.get(row.table_fqn, {}).get(row.check_name, _UNATTRIBUTED)


def _rule_label(row: CheckResultRow, attr: CheckAttribution) -> str:
    return attr.rule_name or row.check_name


def _rule_key(row: CheckResultRow, attr: CheckAttribution) -> str:
    """Distinct-rule counting key: the registry rule id when attributed,
    else the check name (mirrors dqlake's COUNT(DISTINCT rule_id))."""
    return attr.rule_id or row.check_name


def row_matches_facets(
    row: CheckResultRow,
    attr: CheckAttribution,
    facets: ResultFacets,
) -> bool:
    """dqlake facet semantics: OR within a facet, AND across facets.

    Untagged checks (attribution field None) never match an active
    dimension/severity facet — the SQL analogue is ``col = 'v'`` on a
    NULL column. The column facet is a membership test over the check's
    mapped columns.
    """
    if facets.dimensions and attr.dimension not in facets.dimensions:
        return False
    if facets.severities and attr.severity not in facets.severities:
        return False
    if facets.rules and _rule_label(row, attr) not in facets.rules:
        return False
    if facets.columns and not any(c in facets.columns for c in attr.columns):
        return False
    return True


@dataclass
class _GroupAcc:
    failed: int = 0
    total: int | None = None
    rule_keys: set[str] = field(default_factory=set)
    check_rows: int = 0

    def add(self, row: CheckResultRow, rule_key: str) -> None:
        self.failed += row.failed
        if row.total is not None:
            self.total = (self.total or 0) + row.total
        self.rule_keys.add(rule_key)
        self.check_rows += 1

    @property
    def pass_rate(self) -> float | None:
        # SQL analogue: 1 - SUM(failed) / NULLIF(SUM(total), 0).
        if not self.total:
            return None
        return 1 - self.failed / self.total


def _group_rows(
    pairs: list[tuple[CheckResultRow, CheckAttribution]],
    key_of: Callable[[CheckResultRow, CheckAttribution], str | None],
    *,
    with_check_count: bool = True,
) -> list[GroupRowOut]:
    groups: dict[str | None, _GroupAcc] = defaultdict(_GroupAcc)
    for row, attr in pairs:
        groups[key_of(row, attr)].add(row, _rule_key(row, attr))
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


def _by_column_rows(pairs: list[tuple[CheckResultRow, CheckAttribution]]) -> list[GroupRowOut]:
    """By-column breakdown EXPLODES the mapped columns: a check spanning N
    columns attributes to each of them (rows can sum above the total —
    dqlake's intended "involvement" view). Checks with no mapped columns
    don't appear (SQL analogue: explode of a NULL array yields no rows).
    dqlake's by_column query computes no check_count."""
    exploded: list[tuple[CheckResultRow, CheckAttribution]] = []
    for row, attr in pairs:
        exploded.extend((row, replace(attr, columns=(column,))) for column in attr.columns)
    return _group_rows(exploded, lambda _row, attr: attr.columns[0], with_check_count=False)


def _trend(pairs: list[tuple[CheckResultRow, CheckAttribution]]) -> list[TrendPointOut]:
    groups: dict[str | None, _GroupAcc] = defaultdict(_GroupAcc)
    for row, attr in pairs:
        groups[row.run_date].add(row, _rule_key(row, attr))
    return [
        TrendPointOut(run_date=run_date, pass_rate=groups[run_date].pass_rate)
        for run_date in sorted(groups, key=lambda d: d or "")
    ]


def _trend_grouped(
    pairs: list[tuple[CheckResultRow, CheckAttribution]],
    series_of: Callable[[CheckResultRow, CheckAttribution], str | None],
) -> list[TrendPointOut]:
    groups: dict[tuple[str | None, str | None], _GroupAcc] = defaultdict(_GroupAcc)
    for row, attr in pairs:
        groups[(row.run_date, series_of(row, attr))].add(row, _rule_key(row, attr))
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


def _trend_counts(pairs: list[tuple[CheckResultRow, CheckAttribution]]) -> list[TrendCountPointOut]:
    groups: dict[str | None, _GroupAcc] = defaultdict(_GroupAcc)
    for row, attr in pairs:
        groups[row.run_date].add(row, _rule_key(row, attr))
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
    pairs: list[tuple[CheckResultRow, CheckAttribution]],
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
    by_date: dict[str | None, list[tuple[CheckResultRow, CheckAttribution]]] = defaultdict(list)
    for row, attr in pairs:
        by_date[row.run_date].append((row, attr))
    out: list[TrendFailurePointOut] = []
    for run_date in sorted(by_date, key=lambda d: d or ""):
        rows_here = by_date[run_date]
        failed_rules = {_rule_key(row, attr) for row, attr in rows_here if row.failed > 0}
        runs_here = {(row.table_fqn, row.run_id) for row, _ in rows_here if row.run_id}
        record_counts = [
            count for key in runs_here if (count := failed_records_by_run.get(key)) is not None
        ]
        out.append(
            TrendFailurePointOut(
                run_date=run_date,
                failed_rule_count=len(failed_rules),
                failed_check_count=sum(1 for row, _ in rows_here if row.failed > 0),
                failed_test_count=sum(row.failed for row, _ in rows_here),
                failed_records=sum(record_counts) if record_counts else None,
            )
        )
    return out


def compute_entity_results(
    rows: list[CheckResultRow],
    attributions: dict[str, dict[str, CheckAttribution]],
    facets: ResultFacets,
    *,
    axes: str = "all",
    table_axis: str = "tables",
    failed_records_by_run: dict[tuple[str, str], int | None] | None = None,
    failures_ignore_facets: bool = False,
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
    """
    if axes not in VALID_AXES:
        axes = "all"  # dqlake parity: anything else selects every slice
    all_pairs = [(row, _attr_of(row, attributions)) for row in rows]
    pairs = [(row, attr) for row, attr in all_pairs if row_matches_facets(row, attr, facets)]
    failure_pairs = all_pairs if failures_ignore_facets else pairs

    result = EntityResultsOut()
    records_by_run = failed_records_by_run or {}
    if axes in ("all", "breakdown"):
        result.by_dimension = _group_rows(pairs, lambda _row, attr: attr.dimension)
        result.by_severity = _group_rows(pairs, lambda _row, attr: attr.severity)
        result.by_rule = _group_rows(pairs, _rule_label)
        result.by_column = _by_column_rows(pairs)
        table_groups = _group_rows(pairs, lambda row, _attr: row.table_fqn)
        if table_axis == "by_table":
            result.by_table = table_groups
        else:
            result.tables = table_groups
    if axes in ("all", "trend"):
        result.trend = _trend(pairs)
        result.trend_by_dimension = _trend_grouped(pairs, lambda _row, attr: attr.dimension)
        result.trend_by_severity = _trend_grouped(pairs, lambda _row, attr: attr.severity)
        if table_axis == "by_table":
            result.trend_by_table = _trend_grouped(pairs, lambda row, _attr: row.table_fqn)
        result.trend_counts = _trend_counts(pairs)
        result.trend_failures = _trend_failures(failure_pairs, records_by_run)
    return result
