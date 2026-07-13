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

1. The route fetches the raw check rows via SQL (``parse_check_rows``),
   plus — for the multi-table scopes' trend axes — the scope's slice of
   the UC as-of expansion view ``v_dq_check_results_asof`` (the
   carry-forward consolidation is computed IN the view layer, not here;
   see ``score_view_service.asof_view_ddl``).
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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from databricks_labs_dqx_app.backend.metrics_utils import safe_int
from databricks_labs_dqx_app.backend.models import (
    EntityResultsOut,
    GroupRowOut,
    TrendCountPointOut,
    TrendFailurePointOut,
    TrendPointOut,
)
from databricks_labs_dqx_app.backend.services.score_view_service import RUN_MODE_DRAFT

VALID_AXES = ("all", "trend", "breakdown")


def _rows_have_draft(rows: Iterable[CheckResultRow]) -> bool:
    """True when ANY row came from a draft (non-published) run.

    A trend point that pools multiple runs onto one instant is marked draft
    if any contributing run was a draft — the conservative choice so a mixed
    instant is never silently shown as fully published (B2-136). The shaping
    view resolves untagged legacy runs to 'published', so only an explicit
    ``run_mode == 'draft'`` counts.
    """
    return any(row.run_mode == RUN_MODE_DRAFT for row in rows)


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
    run_mode: str | None = None


@dataclass(frozen=True)
class ResultFacets:
    """Active drilldown filters: OR within a facet, AND across facets.

    *tables* (P7.2) is the multi-table scopes' By-table cross-filter — a
    set of member table FQNs. It participates in the AND like the other
    four facets everywhere EXCEPT the ``by_table`` breakdown itself,
    which self-excludes it (see ``compute_entity_results``).
    """

    dimensions: tuple[str, ...] = ()
    severities: tuple[str, ...] = ()
    rules: tuple[str, ...] = ()
    columns: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()

    def any_active(self) -> bool:
        return bool(self.dimensions or self.severities or self.rules or self.columns or self.tables)


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
                run_mode=row.get("run_mode"),
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
    as-of-run mapped columns. The rule facet matches on rule IDENTITY:
    a value matches the row's frozen registry rule id (preferred — one
    id selects every run of the rule, old names included) or its check
    name (backward compat for label-only callers and the only handle
    legacy NULL-rule_id rows have). The table facet is an equality on the
    row's table FQN — the SQL analogue of dqlake's binding filter.
    """
    if facets.tables and row.table_fqn not in facets.tables:
        return False
    if facets.dimensions and row.dimension not in facets.dimensions:
        return False
    if facets.severities and row.severity not in facets.severities:
        return False
    if facets.rules and row.check_name not in facets.rules and (row.rule_id is None or row.rule_id not in facets.rules):
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


def _by_rule_rows(rows: list[CheckResultRow]) -> list[GroupRowOut]:
    """By-rule breakdown grouped by RULE IDENTITY, not display name.

    The grouping key is ``_rule_key``: the frozen registry rule id when
    the run carried one, else the check name (legacy/untagged rows have
    nothing better) — so a rule renamed between versions stays ONE row
    across runs. Each group is LABELLED with the check name from the
    NEWEST run in scope, collapsing renames into the current display
    name (rows with no run_date order oldest; ties keep the first-seen
    name). The additive *rule_id* is set on identity-keyed groups so the
    UI can facet-filter by identity instead of the version-dependent
    label; name-keyed (legacy) groups keep it None.
    """
    newest_name: dict[str, tuple[str, str]] = {}  # key -> (run_date, check_name)
    identity_ids: dict[str, str] = {}
    for row in rows:
        key = _rule_key(row)
        candidate = (row.run_date or "", row.check_name)
        if key not in newest_name or candidate[0] > newest_name[key][0]:
            newest_name[key] = candidate
        if row.rule_id is not None:
            identity_ids[key] = row.rule_id
    out = _group_rows(rows, _rule_key)
    for group in out:
        if group.label is None:  # defensive: _rule_key never yields None
            continue
        group.rule_id = identity_ids.get(group.label)
        group.label = newest_name[group.label][1]
    return out


def _by_column_rows(rows: list[CheckResultRow]) -> list[GroupRowOut]:
    """By-column breakdown EXPLODES the mapped columns: a check spanning N
    columns attributes to each of them (rows can sum above the total —
    dqlake's intended "involvement" view). Checks with no mapped columns
    don't appear (SQL analogue: explode of a NULL array yields no rows).
    dqlake's by_column query computes no check_count."""
    exploded = [replace(row, columns=(column,)) for row in rows for column in row.columns]
    return _group_rows(exploded, lambda row: row.columns[0], with_check_count=False)


def _trend_asof(rows: list[CheckResultRow]) -> list[TrendPointOut]:
    """The overall "Average" series over AS-OF-EXPANDED rows.

    The carry-forward itself now lives in the UC view
    ``v_dq_check_results_asof`` (see ``score_view_service.asof_view_ddl``):
    *rows* already contain, at every run instant (``run_date`` = the
    expansion's ``as_of_time``), each member table's latest-run check
    rows. This function only finishes the aggregation the way dqlake's
    ``_product_trend`` does: pool each table's rows at the instant into
    its pass rate, then take the EQUAL-WEIGHT mean across tables. A
    table whose as-of rows pool to a NULL rate (zero tests) is excluded
    from that instant's mean — never substituted (dqlake filters NULL
    AFTER the as-of pick); an instant where no table has a rate yields a
    NULL point.

    The series starts at the FIRST member's first run and every member
    joins the line at its own first run — there is no all-members-ran
    display gate (tables get added to and removed from scopes over
    time). LIMITATION: membership history is not stored, so the series
    reflects the CURRENT member set's run history — a table removed from
    the scope today also drops out of the past points.

    For a single-table scope the expansion degenerates to the table's
    own per-run rows, so this is its per-run rate series.
    """
    # instant -> table -> pooled accumulator.
    per_instant: dict[str | None, dict[str, _GroupAcc]] = defaultdict(lambda: defaultdict(_GroupAcc))
    draft_instants: set[str | None] = set()
    for row in rows:
        per_instant[row.run_date][row.table_fqn].add(row)
        if row.run_mode == RUN_MODE_DRAFT:
            draft_instants.add(row.run_date)
    out: list[TrendPointOut] = []
    for run_date in sorted(per_instant, key=lambda d: d or ""):
        rates = [acc.pass_rate for acc in per_instant[run_date].values() if acc.pass_rate is not None]
        out.append(
            TrendPointOut(
                run_date=run_date,
                pass_rate=sum(rates) / len(rates) if rates else None,
                is_draft=run_date in draft_instants,
            )
        )
    return out


def _parse_run_instant(raw: str | None) -> datetime | None:
    """Parse a trend point's ``run_date`` into a UTC-aware datetime.

    Runs surface ``run_date`` as ``CAST(run_time AS STRING)`` —
    ``'YYYY-MM-DD HH:MM:SS[.ffffff]'``, assumed UTC — but an ISO-8601
    string (``'…T…Z'`` / with an offset) parses too. Returns None when
    unparseable so callers can leave the point untouched.
    """
    if not raw:
        return None
    text = raw.strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def annotate_trend_versions(
    trend: list[TrendPointOut],
    version_freezes: list[tuple[int, datetime | None]],
) -> None:
    """Stamp each overall-trend point with the binding version active then.

    *version_freezes* is the monitored-table binding's ``(version,
    frozen_at)`` history (order-independent). Each point gets the highest
    version whose freeze time is at/-before its run instant; points before
    the first approval get version 0. Mutates *trend* in place. A point
    whose ``run_date`` is unparseable is left as-is (version stays None),
    and the whole call is a no-op when no freeze carries a timestamp.
    """
    freezes = sorted(
        (
            (ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc), ver)
            for ver, ts in version_freezes
            if ts is not None
        ),
        key=lambda kv: kv[0],
    )
    if not freezes:
        return
    for point in trend:
        run_dt = _parse_run_instant(point.run_date)
        if run_dt is None:
            continue
        active = 0
        for ts, ver in freezes:
            if ts <= run_dt:
                active = ver
            else:
                break
        point.version = active


def _trend_grouped(
    rows: list[CheckResultRow],
    series_of: Callable[[CheckResultRow], str | None],
    *,
    instant_of: Callable[[CheckResultRow], str | None] | None = None,
) -> list[TrendPointOut]:
    """Per-instant grouped series: one point per (instant, series), each
    pooling the rows AT that instant (1 - SUM(failed)/SUM(total)).

    *instant_of* selects each row's x position; it defaults to the row's
    own ``run_date``. The ``trend_by_table`` caller overrides it with the
    row's RUN-BATCH instant (see ``compute_entity_results``) so every
    concurrent member run of one Table-Space "Run now" collapses onto the
    single batch instant — mirroring dqlake's
    ``_product_trend_by_table_scores`` (all members share the Average
    point's x instead of spreading across the time axis). The COALESCE
    fallback (batch key -> bare run_id for un-setted runs) makes this a
    no-op for single-table scopes.

    Two callers, two row sets:

    - ``trend_by_table`` feeds the RAW per-run rows at the BATCH instant —
      each table's dull line is its own runs, no carry-forward (dqlake
      parity), consolidated per batch;
    - ``trend_by_dimension`` / ``trend_by_severity`` feed the AS-OF
      EXPANSION rows at their own ``run_date`` (default *instant_of*), so
      the same pooling yields dqlake's ``_product_trend_grouped``
      semantics (at each instant every member contributes its latest
      run's rows; the group value is the POOLED rate over the carried
      rows — NOT the mean the overall Average uses; dqlake's grouped SQL
      pools, only its Average AVGs).
    """
    instant = instant_of or (lambda row: row.run_date)
    groups: dict[tuple[str | None, str | None], _GroupAcc] = defaultdict(_GroupAcc)
    draft_groups: set[tuple[str | None, str | None]] = set()
    for row in rows:
        key = (instant(row), series_of(row))
        groups[key].add(row)
        if row.run_mode == RUN_MODE_DRAFT:
            draft_groups.add(key)
    return [
        TrendPointOut(
            run_date=run_date,
            series=series,
            pass_rate=acc.pass_rate,
            rule_count=len(acc.rule_keys),
            total_tests=acc.total,
            is_draft=(run_date, series) in draft_groups,
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


def _latest_instant_by_table(
    rows: list[CheckResultRow],
    instant_of: Callable[[CheckResultRow], str | None] | None = None,
) -> dict[str, str | None]:
    """Each table's latest run instant (max instant; None orders first).

    The Python analogue of dqlake's ``v_table_scores.is_latest_for_table``
    window flag: computed over EVERY run in scope, independent of the
    active facet chips — a facet can hide a latest run's rows but never
    resurrect an older run in its place.

    *instant_of* selects the instant (defaults to the row's ``run_date``);
    the ``by_table`` breakdown overrides it with the row's RUN-BATCH
    instant so a batch counts as ONE instant (all concurrent member runs
    of one Table-Space "Run now" resolve to the same latest instant).
    """
    instant = instant_of or (lambda row: row.run_date)
    latest: dict[str, str | None] = {}
    for row in rows:
        row_instant = instant(row)
        if row.table_fqn not in latest or (row_instant or "") > (latest[row.table_fqn] or ""):
            latest[row.table_fqn] = row_instant
    return latest


def compute_entity_results(
    rows: list[CheckResultRow],
    facets: ResultFacets,
    *,
    axes: str = "all",
    table_axis: str = "tables",
    failed_records_by_run: dict[tuple[str, str], int | None] | None = None,
    failures_ignore_facets: bool = False,
    binding_ids_by_table: dict[str, str] | None = None,
    asof_rows: list[CheckResultRow] | None = None,
    run_set_by_run_id: dict[str, str] | None = None,
    as_of_batch: str | None = None,
) -> EntityResultsOut:
    """Assemble the full EntityResultsOut from raw check rows.

    *table_axis* selects where the per-table grouping lands: ``"tables"``
    for the table endpoint, ``"by_table"`` for product/global/rule
    (dqlake parity — its table reader fills ``tables``, its product
    reader fills ``by_table``/``trend_by_table``). *axes* mirrors
    dqlake's slice selection: ``"trend"`` computes only the over-time
    series, ``"breakdown"`` only the groupings; unrequested keys stay
    empty so the shape is stable.

    *asof_rows* is the scope's slice of the UC as-of expansion view
    ``v_dq_check_results_asof`` (``run_date`` = the expansion's
    ``as_of_time``); it feeds the carry-forward series (overall
    ``trend`` + ``trend_by_dimension`` / ``trend_by_severity``). None —
    the single-table endpoint — falls back to the raw rows, whose
    per-run grouping is that scope's exact as-of degeneration.

    *failures_ignore_facets* mirrors dqlake's table reader, whose
    trend_failures query filters on binding/run only — never on the
    dimension/severity/rule/column chips (the product reader does honour
    them).

    *binding_ids_by_table* (table_fqn -> monitored-table binding id)
    enriches the ``by_table`` rows with an additive *binding_id* so the
    UI can link each row to its monitored-table page. Tables absent from
    the map keep None. Only the ``by_table`` axis is enriched — the
    single-table endpoint's ``tables`` axis has no linking use case.

    *run_set_by_run_id* (run_id -> run_set_id) is the query-time join of
    ``dq_run_set_members`` (see ``RunSetService.run_set_ids_by_run_id``).
    It lets the multi-table axes consolidate CONCURRENT member runs of one
    Table-Space "Run now" onto a single RUN-BATCH instant: the batch key
    is ``COALESCE(run_set_id, run_id)`` (dqlake parity), and the batch
    instant is the batch's LAST ``run_date`` (``MAX(run_time)``). The
    ``trend_by_table`` markers and the ``by_table`` latest-run selection
    are keyed on that instant, so every member of a batch shares the
    Average point's x (fixing the spread-out markers AND the
    single-member trend tooltip). The COALESCE fallback makes it a no-op
    for single-table scopes and un-setted runs, so their behaviour is
    UNCHANGED — the map is None on the single-table endpoint. Only the
    ``by_table`` axis consolidates (mirrors dqlake, whose table reader
    plots raw run instants).

    *as_of_batch* is a run_id identifying a chosen run batch (any member
    run of it, as returned in the batch-keyed runs picker). When set, the
    over-time series and the ``by_table`` snapshot are capped to batches
    whose instant is at/-before that batch's instant — dqlake's
    ``as_of_batch`` truncation. None = newest (no cap). An unknown value
    leaves the scope uncapped (newest) rather than blanking it.
    """
    if axes not in VALID_AXES:
        axes = "all"  # dqlake parity: anything else selects every slice

    # RUN-BATCH consolidation (dqlake parity). The batch key is
    # COALESCE(run_set_id, run_id); the batch instant is the batch's last
    # run_date. Computed over EVERY scope row (facet- and cap-independent,
    # like dqlake's `batches` CTE), so a hidden facet or an as-of cap never
    # shifts a batch's instant.
    run_sets = run_set_by_run_id or {}

    def batch_key_of(row: CheckResultRow) -> str | None:
        if row.run_id is None:
            return None
        return run_sets.get(row.run_id) or row.run_id

    batch_instant: dict[str, str | None] = {}
    for row in rows:
        key = batch_key_of(row)
        if key is None:
            continue
        if key not in batch_instant or (row.run_date or "") > (batch_instant[key] or ""):
            batch_instant[key] = row.run_date

    def instant_of(row: CheckResultRow) -> str | None:
        key = batch_key_of(row)
        return (batch_instant.get(key) if key is not None else None) or row.run_date

    # As-of cap: resolve the chosen run_id to its batch instant. Unknown
    # -> no cap (newest), never an empty scope.
    cap_instant: str | None = None
    if as_of_batch is not None:
        cap_key = run_sets.get(as_of_batch) or as_of_batch
        cap_instant = batch_instant.get(cap_key)

    def within_cap(row: CheckResultRow) -> bool:
        if cap_instant is None:
            return True
        row_instant = instant_of(row)
        return row_instant is not None and row_instant <= cap_instant

    capped_rows = [row for row in rows if within_cap(row)]
    matched = [row for row in capped_rows if row_matches_facets(row, facets)]
    failure_rows = capped_rows if failures_ignore_facets else matched

    result = EntityResultsOut()
    records_by_run = failed_records_by_run or {}
    if axes in ("all", "breakdown"):
        # Multi-table scopes (product/global/rule): the breakdown TABLES
        # reflect exactly each member table's LATEST run — dqlake's
        # ``latest_only=True`` (is_latest_for_table) on every product
        # breakdown — so the numbers match the per-member Invalid-samples
        # view (a single latest run) instead of stacking every run in
        # history. The over-time series below keep the full history
        # (dqlake leaves latest_only False on the trends). The single-table
        # endpoint keeps the caller's scoping: the monitored-table tab pins
        # a run_id itself, and without one it pools history as before.
        breakdown_rows = matched
        table_box_rows = breakdown_rows
        if table_axis == "by_table":
            # Latest run PER TABLE keyed on the BATCH instant (a batch counts
            # as ONE instant), computed over the cap-scoped rows so an as-of
            # selection snapshots each table's latest run at/-before it.
            latest = _latest_instant_by_table(capped_rows, instant_of)
            breakdown_rows = [row for row in matched if instant_of(row) == latest.get(row.table_fqn)]
            # The By table box SELF-EXCLUDES the table facet (P7.2): clicking
            # a table row cross-filters every OTHER box, but the box's own
            # rows must not vanish — the selection highlight needs the full
            # (otherwise-faceted) row set, exactly as the frontend's
            # base-vs-filtered split keeps a clicked row visible in its own
            # box. The other facets still apply to it.
            matched_sans_table = (
                [row for row in capped_rows if row_matches_facets(row, replace(facets, tables=()))]
                if facets.tables
                else matched
            )
            table_box_rows = [row for row in matched_sans_table if instant_of(row) == latest.get(row.table_fqn)]
        result.by_dimension = _group_rows(breakdown_rows, lambda row: row.dimension)
        result.by_severity = _group_rows(breakdown_rows, lambda row: row.severity)
        result.by_rule = _by_rule_rows(breakdown_rows)
        result.by_column = _by_column_rows(breakdown_rows)
        table_groups = _group_rows(table_box_rows, lambda row: row.table_fqn)
        if table_axis == "by_table":
            if binding_ids_by_table:
                for group in table_groups:
                    if group.label is not None:
                        group.binding_id = binding_ids_by_table.get(group.label)
            result.by_table = table_groups
        else:
            result.tables = table_groups
    if axes in ("all", "trend"):
        # The as-of series (overall Average + dimension/severity popovers)
        # aggregate the UC as-of expansion when the caller fetched one.
        # Scope restrictions on the expansion:
        # - instants: the expansion is table-agnostic (every table's run
        #   instants workspace-wide), so its rows are restricted to the
        #   instants where THIS scope actually ran — derived from the raw
        #   rows, pre-facet (dqlake's batch instants are facet-independent);
        #   without it a foreign table's run would inject flat repeat
        #   points into the scope's series.
        # - facets: applied to the carried rows exactly like *matched*
        #   (dqlake filters its facet chips on the consolidated view).
        # Callers without an expansion (the single-table endpoint) fall
        # back to *matched*: a single table's per-run rows ARE its as-of
        # expansion (its latest run at each of its instants is that run).
        if asof_rows is None:
            expansion = matched
        else:
            # The scope's own run instants (facet-independent). Capped to
            # the as-of batch so the carry-forward Average truncates to the
            # chosen batch (dqlake's `<=` HAVING on the batches CTE).
            scope_instants = {row.run_date for row in capped_rows}
            expansion = [
                row for row in asof_rows if row.run_date in scope_instants and row_matches_facets(row, facets)
            ]
        result.trend = _trend_asof(expansion)
        result.trend_by_dimension = _trend_grouped(expansion, lambda row: row.dimension)
        result.trend_by_severity = _trend_grouped(expansion, lambda row: row.severity)
        if table_axis == "by_table":
            # Per-table markers plotted at their RUN-BATCH instant so every
            # member of one Table-Space run shares the Average point's x —
            # this is what makes the trend tooltip list ALL members (B2-5)
            # and stops the markers spreading across the axis (B2-18).
            result.trend_by_table = _trend_grouped(matched, lambda row: row.table_fqn, instant_of=instant_of)
        result.trend_counts = _trend_counts(matched)
        result.trend_failures = _trend_failures(failure_rows, records_by_run)
    return result
