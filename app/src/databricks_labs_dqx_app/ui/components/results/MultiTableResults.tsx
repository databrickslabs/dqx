import { useEffect, useState } from "react";
import type * as React from "react";
import { keepPreviousData } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { ChevronDown, ExternalLink, Loader2 } from "lucide-react";
import {
  useGetTableResults,
  useGetDqResultsFailedRows,
  getDqResultsFailedRows,
  useListResultDimensionsSuspense,
  useListResultSeveritiesSuspense,
  type EntityResultsOut,
  type GetGlobalResultsParams,
  type TrendPointOut,
  type DimensionOut,
  type SeverityOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ScoreBox } from "@/components/results/ScoreBox";
import { CollapsibleSection } from "@/components/results/CollapsibleSection";
import { CollapseRegion } from "@/components/results/CollapseRegion";
import { ScoreTrendChart, type OverallStep } from "@/components/results/ScoreTrendChart";
import { DimensionBreakdown } from "@/components/results/DimensionBreakdown";
import { FilterChips } from "@/components/results/FilterChips";
import { FailingRecordsTable } from "@/components/results/FailingRecordsTable";
import { DownloadFailedRecordsMenu } from "@/components/results/DownloadFailedRecordsMenu";
import {
  EXPORT_ROW_LIMIT,
  toFailingRecords,
} from "@/components/results/failedRecordsExport";
import { toCountSeries } from "@/components/results/countSeries";
import { RunModeSelect, includeDraftsParam } from "@/components/results/RunModeSelect";
import {
  toNum,
  ApplicableToggle,
  COUNT_INFO,
  EMPTY_FILTERS,
  facetQueryParams,
  ruleChipDisplay,
  ruleFacetValue,
  toggleFacet,
  type Facet,
  type MultiFilters,
} from "@/components/monitored-tables/BindingResultsTab";

// The multi-table results composition, extracted from the reviewed
// `components/data-products/ProductResultsTab.tsx` (itself a 1:1 port of
// dqlake's `components/products/ProductResultsTab.tsx` — that file is the
// spec: keep structure/section order/interactions aligned with it). The body
// here is dqlake's product-tab composition parameterized ONLY on the
// entity-specific bits, so the table-space tab (P2.4), the global Results
// page (adaptation #1) and the rule Results view (adaptation #2) share ONE
// composition instead of three near-copies:
//
//  - `useEntityResults`: the entity's trend/breakdown data-source hook
//    (product / global / rule endpoint — all serve `EntityResultsOut`).
//  - `scoreLabel`: the ScoreBox label (product: "Average score"; global: the
//    org-wide label carrying the accessible-table count).
//  - `runPickerSlot`: the optional run-picker node overlapping the ScoreBox.
//    Callers without a coherent run universe omit it entirely.
//
// (A former `requiredFqns` prop gated the Average line on every member
// having run — removed with the allRanSince display gate: the server's
// as-of series is plotted as-is; see computeOverallPoints.)
//
// The sanctioned dqlake deviations live here unchanged: hooks re-pointed to
// `/api/v1/dq-results/*`, all strings via t(), Genie stripped, and NO idle
// polling — queries use RESULTS_QUERY_OPTIONS and refresh comes from the
// run-completion invalidation in `lib/results-invalidation.ts` (dqlake's
// resultsPolling 5s/15s refetch loops are deliberately not ported). The
// RunInProgressBanner is omitted with them: deriving run activity here would
// mean reintroducing an idle poll.

/** Query params accepted by every multi-table results endpoint (the product /
 *  global / rule params are structurally identical; the global shape is the
 *  canonical alias since it has no path parameter). */
export type EntityResultsParams = GetGlobalResultsParams;

/** React Query options the composition passes to its data-source hook. */
export interface EntityResultsQueryOptions {
  readonly staleTime: number;
  readonly refetchOnWindowFocus: boolean;
  placeholderData?: typeof keepPreviousData;
}

/**
 * The entity-specific data-source hook: wraps one of the generated
 * `/api/v1/dq-results/*` hooks, closing over any path parameter (product id,
 * rule id). Called unconditionally exactly three times per render (trend,
 * filtered breakdown, base breakdown), so a wrapper closure re-created each
 * render is safe under the rules of hooks.
 */
export type UseEntityResults = (
  params: EntityResultsParams,
  queryOptions: EntityResultsQueryOptions,
) => {
  data?: { data: EntityResultsOut };
  isPending: boolean;
  isFetching: boolean;
};

/** Breakdown-query params for a multi-table view. Deliberately NEVER includes
 *  a run_id filter — a per-table run_id cannot scope a multi-table view
 *  coherently: it is not a batch across tables, so scoping the breakdowns by
 *  one would collapse them to the single table that produced it (see the
 *  run-picker adaptation note in ProductResultsTab). *includeDrafts* adds
 *  `include_drafts: true` only when the surface's run-mode dropdown says so;
 *  otherwise the param is omitted (backend published-only default). */
export function breakdownParams(
  filters: MultiFilters,
  includeDrafts = false,
): EntityResultsParams {
  return {
    ...facetQueryParams(filters),
    axes: "breakdown",
    include_drafts: includeDraftsParam(includeDrafts),
  };
}

/** Muted, desaturated palette for the per-table contributing lines on the
 *  Average DQ Score chart — deliberately recessive so the foreground Average
 *  step stays the headline. Each member table gets a stable colour by index;
 *  the same colour tints the Average step segment that table contributed. */
export const DULL_TABLE_COLORS = [
  "#6b8ab0", // muted blue
  "#b08a6b", // muted tan
  "#6bb08a", // muted green
  "#a06ba0", // muted purple
  "#b0b06b", // muted olive
  "#6bb0b0", // muted teal
];

/** Friendly table name = last segment of the `catalog.schema.table` FQN. */
export function friendlyTableName(fqn: string | null | undefined): string {
  return (fqn ?? "").split(".").pop() ?? "";
}

/** Distinct per-table series names (first-seen order), each given a stable
 *  dull colour by index (cycling through DULL_TABLE_COLORS). */
export function buildTableColorMap(
  trendByTable: TrendPointOut[] | undefined,
): Record<string, string> {
  const names = Array.from(
    new Set(
      (trendByTable ?? [])
        .map((t) => t.series)
        .filter((s): s is string => Boolean(s)),
    ),
  );
  return Object.fromEntries(
    names.map((name, i) => [name, DULL_TABLE_COLORS[i % DULL_TABLE_COLORS.length]]),
  );
}

/**
 * Average points for the overall-mode trend chart. The server's `trend` IS
 * the as-of carry-forward average (the UC view `v_dq_check_results_asof`:
 * at each run instant every member table contributes its most recent
 * published run at-or-before that instant; equal-weight mean — see backend
 * `dq_results_service._trend_asof`), so this does NO math of its own: it
 * only scales values 0..100 for the chart's OverallStep contract.
 *
 * The Average line starts at the FIRST member's first run and each member
 * joins it at its own first run — the former all-members-ran display gate
 * (allRanSince) was deliberately removed: tables get added to and removed
 * from scopes over time, so the average at each instant is the mean over
 * the members WITH data as of then. LIMITATION: membership history is not
 * stored, so the series reflects the CURRENT member set's run history — a
 * table removed from the scope today also drops out of the past points.
 */
export function computeOverallPoints(
  trend: TrendPointOut[] | undefined,
): OverallStep[] {
  return (trend ?? []).map((t) => {
    const v = toNum(t.pass_rate);
    return { run_date: String(t.run_date ?? ""), value: v == null ? null : v * 100 };
  });
}

/**
 * Failed/total test counts for the ScoreBox subtitle: BOTH sums come from
 * the same (filtered) by_table rows so "X failed of Y tests" is internally
 * consistent. DELIBERATE dqlake deviation — the original hardcodes
 * totalTests={0}, which suppresses the subtitle on the product ScoreBox
 * entirely (its ScoreBox only renders the subtitle when totalTests > 0).
 *
 * *tableFacet* (P7.2): the server self-excludes the table facet from the
 * by_table rows (so the box keeps its full row set), which would leave this
 * subtitle spanning every table while the headline trend is filtered to the
 * selected one — so the facet is re-applied here, client-side, on the rows'
 * FQN labels. Empty facet = all rows, as before.
 */
export function sumTestCounts(
  byTable: EntityResultsOut["by_table"],
  tableFacet: string[] = [],
): {
  failedTests: number;
  totalTests: number;
} {
  const rows = (byTable ?? []).filter(
    (g) => tableFacet.length === 0 || (g.label != null && tableFacet.includes(g.label)),
  );
  return {
    failedTests: rows.reduce((a, g) => a + (g.failed_tests ?? 0), 0),
    totalTests: rows.reduce((a, g) => a + (g.total_tests ?? 0), 0),
  };
}

/**
 * Next (sample selection, table facet) pair for a By table row click (P7.2):
 * clicking a row pins BOTH the invalid-samples selection and the table facet
 * to that table; clicking the selected row again clears both. Single-select
 * semantics — the facet mirrors the selection (switching rows REPLACES the
 * facet value, it never accumulates), and a label that no longer resolves to
 * an FQN clears the facet rather than filtering on a stale value.
 */
export function nextTableSelection(
  current: string | null,
  label: string,
  fqn: string | undefined,
): { selected: string | null; table: string[] } {
  const clearing = current === label;
  return {
    selected: clearing ? null : label,
    table: clearing || !fqn ? [] : [fqn],
  };
}

/** Facet → chip-label i18n key (static keys so the extractor sees them). */
const CHIP_LABEL_KEYS: Record<Facet, string> = {
  dimension: "resultsUi.chipDimension",
  severity: "resultsUi.chipSeverity",
  rule: "resultsUi.chipRule",
  column: "resultsUi.chipColumn",
  table: "resultsUi.chipTable",
};

export interface MultiTableResultsSectionProps {
  /** Entity data-source hook (see UseEntityResults). */
  useEntityResults: UseEntityResults;
  /** ScoreBox label, given the accessible-table count (base by-table rows). */
  scoreLabel: (accessibleTableCount: number) => string;
  /** Optional run-picker node overlapping the ScoreBox's top-right corner.
   *  Omit when the entity has no coherent run universe to pick from. */
  runPickerSlot?: React.ReactNode;
  /** Hides the "By rule" breakdown box. For consumers whose rule facet is
   *  locked server-side (the rule Results view — every row would be the one
   *  rule), where a one-row facet box adds nothing. dqlake has no precedent
   *  for a rule-locked surface, so hiding is the minimal adaptation; the
   *  reviewed product/global consumers don't pass this and are unchanged. */
  hideRuleBreakdown?: boolean;
  /** Run mode ("Published only" vs "Published + Draft"). CONTROLLED by the
   *  caller — each surface owns its own state (never global), and callers
   *  with surface-level dq-results queries of their own (the product tab's
   *  runs list, the rule tab's score) wire the same value into them. The
   *  composition renders the dropdown and threads `include_drafts` into
   *  every query it owns. */
  includeDrafts: boolean;
  onIncludeDraftsChange: (includeDrafts: boolean) => void;
  /** Suppresses the run-mode (Published only / Published + Draft) dropdown and
   *  forces published-only results (`include_drafts` never sent). For surfaces
   *  that are published-only by definition — the global all-tables Results page
   *  and the rule Results view — where a draft toggle would be misleading
   *  (drafts are per-authoring-surface, not a coherent cross-table universe).
   *  The reviewed product/table-space consumers don't pass this and keep the
   *  dropdown. */
  hideRunMode?: boolean;
  /** Called (in an effect) when the BASE (unfiltered) by-table rows land or
   *  change. The composition stays Genie/entitlement-agnostic — the global
   *  Results page uses this to pre-verify row entitlements for its
   *  on-screen tables (P4.3); consumers with an explicit member list (the
   *  product tab) don't need it. Pass a stable (useCallback / module-scope)
   *  function — the effect keys on its identity. */
  onBaseByTable?: (byTable: NonNullable<EntityResultsOut["by_table"]>) => void;
}

/**
 * The multi-table results composition (dqlake's product-tab body): average
 * ScoreBox, over-time trends, count charts, and pass-rate breakdowns. By rule
 * is full width (its own row); By table and By column share the row below it.
 * Filtering to a single table reveals that table's invalid-samples view. Each
 * widget loads independently.
 */
export function MultiTableResultsSection({
  useEntityResults,
  scoreLabel,
  runPickerSlot,
  hideRuleBreakdown,
  includeDrafts,
  onIncludeDraftsChange,
  hideRunMode,
  onBaseByTable,
}: MultiTableResultsSectionProps) {
  const { t } = useTranslation();
  // Published-only surfaces (hideRunMode) never send `include_drafts`,
  // regardless of the controlled prop — the dropdown that would flip it is
  // suppressed, so this just hard-guards the query params too.
  const effectiveIncludeDrafts = hideRunMode ? false : includeDrafts;
  // The active single-table selection (E2). Clicking a By Table row sets it;
  // clicking again clears it. It drives the invalid-samples view AND (P7.2)
  // mirrors into the `table` facet, so the click cross-filters the other
  // drilldown boxes too — see nextTableSelection. The selection stays
  // friendly-name-keyed; the facet carries the FQN.
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  // Multi-select facet filters (dimension/severity/rule/column) plus the
  // single-select table facet. Clicking a breakdown row toggles the matching
  // facet, which re-scopes every breakdown and trend (mirrors the
  // monitored-table tab). The By table box itself is exempt from the table
  // facet — the server self-excludes it so the rows never vanish.
  const [filters, setFilters] = useState<MultiFilters>(EMPTY_FILTERS);
  // No selected-run state: per-table run_ids cannot scope a multi-table view
  // (see breakdownParams), so no run_id ever reaches the queries below.
  // Drilldown scope: "Applicable" (default) cross-filters the breakdowns by the
  // active chips and hides rows excluded by them; "All" keeps the excluded base
  // rows, rendered greyed alongside the matching ones.
  const [applicableOnly, setApplicableOnly] = useState(true);
  // Shared collapse state — each boolean drives BOTH members of a pair.
  const [scoreBreakdownOpen, setScoreBreakdownOpen] = useState(true);
  const [countChartsOpen, setCountChartsOpen] = useState(false);
  const [ruleColOpen, setRuleColOpen] = useState(true);
  const [invalidSamplesOpen, setInvalidSamplesOpen] = useState(true);

  // The FILTERED trends: cross-filtered by the active facet chips (dqlake's
  // product tab re-scopes its trends on the chips, unlike the monitored-table
  // tab). NOT run-scoped — see breakdownParams. NON-suspense so the shell +
  // chart frames render first and each widget shows its own spinner while
  // loading (F1).
  const draftsParam = includeDraftsParam(effectiveIncludeDrafts);
  const trendQuery = useEntityResults(
    {
      ...facetQueryParams(filters),
      axes: "trend",
      include_drafts: draftsParam,
    },
    { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
  );
  const trends = trendQuery.data?.data;
  const loading = trendQuery.isPending;

  // The FILTERED breakdowns: cross-filtered by the active facet chips (never
  // run-scoped — see breakdownParams). This is the live result in both modes
  // — the numbers always reflect the active filter.
  const resultsQuery = useEntityResults(breakdownParams(filters, effectiveIncludeDrafts), {
    placeholderData: keepPreviousData,
    ...RESULTS_QUERY_OPTIONS,
  });
  const results = resultsQuery.data?.data;
  // The BASE (applicable) breakdowns: NO facet filter. Drives the row set in
  // "All" mode — base rows absent from the filtered result render greyed. In
  // "Applicable" mode it's unused beyond being the same set.
  const baseQuery = useEntityResults(breakdownParams(EMPTY_FILTERS, effectiveIncludeDrafts), {
    placeholderData: keepPreviousData,
    ...RESULTS_QUERY_OPTIONS,
  });
  const baseResults = baseQuery.data?.data;

  // Surface the base by-table rows to the caller (see the prop docs). The
  // rows object is referentially stable per fetch (React Query cache), so
  // the effect fires once per data landing, not per render.
  const baseByTable = baseResults?.by_table;
  useEffect(() => {
    if (onBaseByTable && baseByTable) onBaseByTable(baseByTable);
  }, [onBaseByTable, baseByTable]);

  // Background refetch (filter/selection change) vs first load: only the former
  // gets the subtle drilldown spinner — the first load has the ChartFrame and
  // per-widget spinners already.
  const breakdownRefetching = resultsQuery.isFetching && resultsQuery.data != null;

  const hasActiveFilter =
    filters.dimension.length > 0 ||
    filters.severity.length > 0 ||
    filters.rule.length > 0 ||
    filters.column.length > 0 ||
    filters.table.length > 0;

  const accessibleTableCount = (baseResults?.by_table ?? []).length;
  // By table shows the FRIENDLY table name (last FQN segment), so the selection
  // is keyed on that. Map it back to the full FQN to resolve the table.
  const fqnByFriendly = new Map(
    (results?.by_table ?? [])
      .map((g) => g.label)
      .filter((l): l is string => l != null)
      .map((fqn) => [friendlyTableName(fqn), fqn] as const),
  );
  const selectedFqn = selectedTable ? (fqnByFriendly.get(selectedTable) ?? null) : null;
  // By-table rows carry the monitored-table binding_id (additive P3.1
  // enrichment) so each row can deep-link to that table's Results tab.
  const bindingIdByFriendly = new Map(
    (results?.by_table ?? [])
      .filter((g): g is typeof g & { label: string; binding_id: string } =>
        g.label != null && typeof g.binding_id === "string",
      )
      .map((g) => [friendlyTableName(g.label), g.binding_id] as const),
  );

  const { data: dimensions } =
    useListResultDimensionsSuspense<DimensionOut[]>(selector<DimensionOut[]>());
  const { data: severities } =
    useListResultSeveritiesSuspense<SeverityOut[]>(selector<SeverityOut[]>());

  const dimColors = Object.fromEntries(dimensions.map((d) => [d.name, d.color]));
  const sevColors = Object.fromEntries(severities.map((s) => [s.name, s.color]));
  const sevRanks = Object.fromEntries(severities.map((s) => [s.name, s.rank]));

  // Single-table invalid samples (E2). The hook is always called (rules of
  // hooks) but disabled until a single table is selected. Per-table row-level
  // fetch via the Task 7 OBO-gated failed-rows endpoint (FQN-keyed).
  const failedRowsQuery = useGetDqResultsFailedRows(
    selectedFqn ?? "",
    { limit: 200, include_drafts: draftsParam },
    {
      query: {
        enabled: Boolean(selectedFqn),
        placeholderData: keepPreviousData,
        ...RESULTS_QUERY_OPTIONS,
      },
    },
  );
  const failedRows = failedRowsQuery.data?.data;
  const failingRows = toFailingRecords(failedRows?.rows);

  // By column is scoped to the SELECTED table (columns are table-specific, so
  // an entity-wide column list mixes columns from every member). Until a table
  // is picked the box is empty; once picked it shows that table's columns.
  const tableColumnsQuery = useGetTableResults(
    selectedFqn ?? "",
    { axes: "breakdown", include_drafts: draftsParam },
    {
      query: {
        enabled: Boolean(selectedFqn),
        placeholderData: keepPreviousData,
        ...RESULTS_QUERY_OPTIONS,
      },
    },
  );

  // Download fetches the WHOLE undrilled set for the selected table (no facet
  // filters, high limit) — independent of the 200-capped on-screen
  // invalid-samples table.
  const fetchAllFailedRows = async () => {
    if (!selectedFqn) return [];
    const res = await getDqResultsFailedRows(selectedFqn, {
      limit: EXPORT_ROW_LIMIT,
      include_drafts: draftsParam,
    });
    return toFailingRecords(res.data.rows);
  };

  const toRows = (groups: EntityResultsOut["by_dimension"]) =>
    (groups ?? []).map((g) => ({
      label: g.label ?? null,
      // rule_id is a by_rule-only enrichment (null elsewhere), so value
      // degenerates to the label on every other facet box.
      value: ruleFacetValue(g),
      pass_rate: g.pass_rate ?? null,
      failed_tests: g.failed_tests ?? null,
      rule_count: g.rule_count ?? null,
      check_count: g.check_count ?? null,
      total_tests: g.total_tests ?? null,
    }));

  // Registry order for the By dimension / By severity default sort: dimensions
  // in their list order; severities by rank descending. Rows not in the
  // registry sort last. Sets those two tables' DEFAULT order to match the
  // admin/registry order rather than failed-count.
  const dimNames = dimensions.map((d) => d.name);
  const sevNames = [...severities]
    .sort((a, b) => b.rank - a.rank)
    .map((s) => s.name);
  const orderByRegistry = <T extends { label: string | null }>(
    rows: T[],
    order: string[],
  ): T[] => {
    const idx = (label: string | null) => {
      const i = label == null ? -1 : order.indexOf(label);
      return i === -1 ? order.length : i;
    };
    return [...rows].sort((a, b) => idx(a.label) - idx(b.label));
  };

  // Build the rows + muted-label set for one facet box (mirrors the
  // monitored-table tab):
  //  - Applicable mode: just the filtered rows (excluded rows are hidden).
  //  - All mode: the base row set; rows present in the filtered result render
  //    normally (with filtered numbers), base rows absent from it render greyed
  //    (with their base numbers). Nothing is hidden.
  const buildFacet = (
    filteredGroups: EntityResultsOut["by_dimension"],
    baseGroups: EntityResultsOut["by_dimension"],
  ): { rows: ReturnType<typeof toRows>; mutedLabels: string[] } => {
    const filteredRows = toRows(filteredGroups);
    if (applicableOnly || !hasActiveFilter) {
      return { rows: filteredRows, mutedLabels: [] };
    }
    // Keyed on the facet VALUE (rule identity on the By rule box, label
    // elsewhere) so a filtered/base pair whose newest-run labels diverge
    // still pairs up by identity.
    const filteredByValue = new Map(filteredRows.map((r) => [r.value ?? r.label, r]));
    const rows = toRows(baseGroups).map((b) => filteredByValue.get(b.value ?? b.label) ?? b);
    const mutedLabels = rows
      .filter((r) => r.label != null && !filteredByValue.has(r.value ?? r.label))
      .map((r) => r.label as string);
    return { rows, mutedLabels };
  };

  const dimFacet = buildFacet(results?.by_dimension, baseResults?.by_dimension);
  const sevFacet = buildFacet(results?.by_severity, baseResults?.by_severity);
  const ruleFacet = buildFacet(results?.by_rule, baseResults?.by_rule);
  // By column rows come from the selected table (empty until picked).
  const tableColRows = selectedTable
    ? toRows(tableColumnsQuery.data?.data?.by_column)
    : [];

  // Rule chips may carry a registry rule_id as their value — show the
  // matching by_rule row's (newest-run) label instead of the opaque id.
  const ruleChipRows = [...(baseResults?.by_rule ?? []), ...(results?.by_rule ?? [])];
  // Chip display values: rule ids resolve to their newest-run label; the
  // table facet carries FQNs but chips show the friendly table name.
  const chipDisplay = (facet: Facet, value: string) => {
    if (facet === "rule") return ruleChipDisplay(value, ruleChipRows);
    if (facet === "table") return friendlyTableName(value);
    return value;
  };
  const chips = (["dimension", "severity", "rule", "column", "table"] as const).flatMap(
    (facet) =>
      filters[facet].map((value) => ({
        key: `${facet}:${value}`,
        label: t(CHIP_LABEL_KEYS[facet], { value: chipDisplay(facet, value) }),
      })),
  );

  const onRemoveChip = (key: string) => {
    const [facet, value] = key.split(/:(.+)/) as [Facet, string];
    // The table facet mirrors the invalid-samples selection — removing its
    // chip clears both, exactly like re-clicking the selected By table row.
    if (facet === "table") setSelectedTable(null);
    setFilters((f) => toggleFacet(f, facet, value));
  };

  const onRowToggle = (facet: Facet, label: string) =>
    setFilters((f) => toggleFacet(f, facet, label));

  const toTrendSeries = (points: TrendPointOut[] | undefined) =>
    (points ?? []).map((p) => ({
      run_date: String(p.run_date ?? ""),
      series: p.series ?? undefined,
      pass_rate: toNum(p.pass_rate),
    }));

  // Entity average score (dqlake behaviour) = the latest point on the entity
  // trend — the as-of mean across members (every member's latest run
  // contributes there by construction). Failed/total tests both sum the same
  // filtered by_table rows — see sumTestCounts for the documented dqlake
  // deviation (the original's totalTests={0} hid the subtitle).
  const lastTrend = (trends?.trend ?? []).at(-1);
  const passRate = toNum(lastTrend?.pass_rate);
  // The table facet is re-applied client-side here: the server self-excludes
  // it from by_table (the box keeps its rows), but the subtitle must match
  // the (table-filtered) headline trend — see sumTestCounts.
  const { failedTests, totalTests } = sumTestCounts(results?.by_table, filters.table);

  // Over-time chart: a prominent foreground "Average" trendline drawn on top
  // of dull, thin per-table lines. The Average is the SERVER's as-of
  // carry-forward mean of the member tables (the UC as-of view: each member
  // contributes its most recent run at-or-before every instant), plotted
  // from the FIRST member's first run — each member joins the line at its
  // own first run. Each table's latest run links to the latest Average
  // point with a dotted line.
  const perTable = toTrendSeries(trends?.trend_by_table);
  const tableColorMap = buildTableColorMap(trends?.trend_by_table);
  const overallPoints = computeOverallPoints(trends?.trend);

  // Series keys are the ENGLISH canonical names — the chart's ordering/legend
  // logic pins on ["Rules","Checks","Tests","Rows"]; do not translate them here.
  const rulesChecksTestsSeries = toCountSeries(trends?.trend_counts, [
    { key: "rule_count", label: "Rules" },
    { key: "check_count", label: "Checks" },
    { key: "test_count", label: "Tests" },
  ]);
  const failuresSeries = toCountSeries(trends?.trend_failures, [
    { key: "failed_rule_count", label: "Rules" },
    { key: "failed_check_count", label: "Checks" },
    { key: "failed_test_count", label: "Tests" },
    { key: "failed_records", label: "Rows" },
  ]);

  // A By table row click toggles BOTH the invalid-samples selection and the
  // table facet (P7.2) — one gesture, one mental model. Single-select: see
  // nextTableSelection.
  const onTableSelect = (label: string) => {
    const next = nextTableSelection(selectedTable, label, fqnByFriendly.get(label));
    setSelectedTable(next.selected);
    setFilters((f) => ({ ...f, table: next.table }));
  };

  const ChartFrame = ({ children }: { children: React.ReactNode }) =>
    loading ? (
      <div className="flex h-[224px] items-center justify-center rounded-md border">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    ) : (
      <>{children}</>
    );

  const scoreBox = (
    <ScoreBox
      passRate={passRate}
      failedTests={failedTests}
      totalTests={totalTests}
      info={COUNT_INFO}
      label={scoreLabel(accessibleTableCount)}
    />
  );

  return (
    <div className="space-y-6">
      {/* The run-mode dropdown (always) and the run picker (when the caller
          supplies one) overlap the score box's top-right corner; they drop
          below the score on very small screens (mirrors the Tables tab). */}
      <div className="relative">
        <div className="z-10 flex items-center gap-2 sm:absolute sm:right-2 sm:top-2 max-sm:mb-2 max-sm:justify-end">
          {!hideRunMode && (
            <RunModeSelect includeDrafts={includeDrafts} onChange={onIncludeDraftsChange} />
          )}
          {runPickerSlot}
        </div>
        <div className="sm:pr-2">{scoreBox}</div>
      </div>

      <CollapsibleSection title={t("resultsUi.overTimeSection")} defaultOpen>
        <div className="space-y-6">
          <ChartFrame>
            <ScoreTrendChart
              data={perTable}
              colorMap={tableColorMap}
              overall={overallPoints}
              overallLabel={t("resultsUi.averageSeries")}
              title={t("resultsUi.averageDqScoreTitle")}
            />
          </ChartFrame>
          <div className="grid gap-6 md:grid-cols-2">
            <ChartFrame>
              <ScoreTrendChart
                data={toTrendSeries(trends?.trend_by_dimension)}
                colorMap={dimColors}
                title={t("resultsUi.scoreByDimensionTitle")}
                onSeriesClick={(s) => onRowToggle("dimension", s)}
                collapsed={!scoreBreakdownOpen}
                onToggleCollapse={() => setScoreBreakdownOpen((o) => !o)}
              />
            </ChartFrame>
            <ChartFrame>
              <ScoreTrendChart
                data={toTrendSeries(trends?.trend_by_severity)}
                colorMap={sevColors}
                title={t("resultsUi.scoreBySeverityTitle")}
                onSeriesClick={(s) => onRowToggle("severity", s)}
                collapsed={!scoreBreakdownOpen}
                onToggleCollapse={() => setScoreBreakdownOpen((o) => !o)}
              />
            </ChartFrame>
          </div>
          <div className="grid gap-6 md:grid-cols-2">
            <ChartFrame>
              <ScoreTrendChart
                mode="count"
                countData={rulesChecksTestsSeries}
                title={t("resultsUi.appliedCountsTitle")}
                collapsed={!countChartsOpen}
                onToggleCollapse={() => setCountChartsOpen((o) => !o)}
              />
            </ChartFrame>
            <ChartFrame>
              <ScoreTrendChart
                mode="count"
                countData={failuresSeries}
                title={t("resultsUi.failedCountsTitle")}
                collapsed={!countChartsOpen}
                onToggleCollapse={() => setCountChartsOpen((o) => !o)}
              />
            </ChartFrame>
          </div>
        </div>
      </CollapsibleSection>

      <CollapsibleSection
        title={t("resultsUi.drilldownSection")}
        defaultOpen
        headerRight={
          <ApplicableToggle
            applicableOnly={applicableOnly}
            onChange={setApplicableOnly}
          />
        }
      >
        <div className="space-y-6">
          <FilterChips filters={chips} onRemove={onRemoveChip} />

          {/* Row 1: By dimension + By severity. Row 2: By rule full width.
              Row 3: By table + By column share a row (E1). Clicking a
              dimension/severity/rule/column row toggles that facet, re-scoping
              every breakdown. Clicking a By table row (P7.2) toggles the
              invalid-samples selection AND the table facet together — the
              other boxes cross-filter to that table while the By table box
              itself keeps its rows (the server self-excludes the facet). */}
          <div className="grid gap-6 md:grid-cols-2">
            <DimensionBreakdown
              title={t("resultsUi.byDimensionTitle")}
              valueHeader={t("resultsUi.dimensionHeader")}
              rows={orderByRegistry(dimFacet.rows, dimNames)}
              mutedLabels={dimFacet.mutedLabels}
              loading={breakdownRefetching}
              colorMap={dimColors}
              selected={filters.dimension}
              onSelect={(label) => onRowToggle("dimension", label)}
            />
            <DimensionBreakdown
              title={t("resultsUi.bySeverityTitle")}
              valueHeader={t("resultsUi.severityHeader")}
              rows={orderByRegistry(sevFacet.rows, sevNames)}
              mutedLabels={sevFacet.mutedLabels}
              loading={breakdownRefetching}
              colorMap={sevColors}
              selected={filters.severity}
              onSelect={(label) => onRowToggle("severity", label)}
            />
            {!hideRuleBreakdown && (
              <div className="md:col-span-2" data-testid="breakdown-by-rule">
                <DimensionBreakdown
                  title={t("resultsUi.byRuleTitle")}
                  valueHeader={t("resultsUi.ruleHeader")}
                  countMode="checks"
                  rows={ruleFacet.rows}
                  mutedLabels={ruleFacet.mutedLabels}
                  loading={breakdownRefetching}
                  selected={filters.rule}
                  // The row's facet value is its rule identity (rule_id
                  // when present), so the filter spans renames.
                  onSelect={(value) => onRowToggle("rule", value)}
                  collapsed={!ruleColOpen}
                  onToggleCollapse={() => setRuleColOpen((o) => !o)}
                  pageSize={8}
                />
              </div>
            )}
            <div data-testid="breakdown-by-table">
              <DimensionBreakdown
                title={t("resultsUi.byTableTitle")}
                valueHeader={t("resultsUi.tableHeader")}
                rows={toRows(results?.by_table).map((r) => ({
                  ...r,
                  label: r.label == null ? null : friendlyTableName(r.label),
                  // Selection is keyed on the friendly name (see
                  // fqnByFriendly) — null the value so clicks/selection
                  // fall back to the friendly label, not the full FQN.
                  value: null,
                }))}
                loading={breakdownRefetching}
                selected={selectedTable ? [selectedTable] : []}
                onSelect={onTableSelect}
                pageSize={8}
                // Rows with a monitored-table binding deep-link to that
                // table's Results tab; the row click itself still drives the
                // invalid-samples selection (stopPropagation on the link).
                rowLink={(label) => {
                  const bindingId = bindingIdByFriendly.get(label);
                  if (!bindingId) return null;
                  return (
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId }}
                      search={{ tab: "results" }}
                      onClick={(e) => e.stopPropagation()}
                      aria-label={t("resultsUi.openTableResultsAria", { table: label })}
                      className="shrink-0 text-muted-foreground hover:text-foreground"
                    >
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  );
                }}
              />
            </div>
            <div data-testid="breakdown-by-column">
              <DimensionBreakdown
                title={t("resultsUi.byColumnTitle")}
                valueHeader={t("resultsUi.columnHeader")}
                rows={tableColRows}
                defaultSort={{ key: "pass_rate", dir: "asc" }}
                emptyText={
                  selectedTable
                    ? t("resultsUi.noColumnsForTable")
                    : t("resultsUi.pickTableForColumns")
                }
                loading={Boolean(selectedFqn) && tableColumnsQuery.isFetching}
                selected={filters.column}
                onSelect={(label) => onRowToggle("column", label)}
                collapsed={!ruleColOpen}
                onToggleCollapse={() => setRuleColOpen((o) => !o)}
              />
            </div>
          </div>

          {/* E2: filtering to a single table reveals its invalid samples. */}
          {selectedTable && (
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <button
                  type="button"
                  onClick={() => setInvalidSamplesOpen((o) => !o)}
                  aria-expanded={invalidSamplesOpen}
                  className="group flex items-center gap-1.5 text-left"
                >
                  <span className="text-xs uppercase tracking-wide text-muted-foreground">
                    {t("resultsUi.invalidSamplesTitle")}
                  </span>
                  <ChevronDown
                    className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
                      invalidSamplesOpen ? "" : "-rotate-90"
                    }`}
                  />
                </button>
                {selectedFqn && !failedRows?.suppressed && (
                  <DownloadFailedRecordsMenu
                    fetchRows={fetchAllFailedRows}
                    tableName={selectedTable}
                  />
                )}
              </div>
              <CollapseRegion open={invalidSamplesOpen}>
                {!selectedFqn ? (
                  // dqlake's analogue is a member without a binding; ours is a
                  // By-table label that no longer resolves to a table FQN.
                  <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                    {t("resultsUi.invalidSamplesUnavailable")}
                  </div>
                ) : failedRows?.suppressed ? (
                  // Our failed-rows envelope carries an extra `suppressed` flag
                  // (fine-grained access controls on the source table — Task 7
                  // semantics); surface the standing suppression message
                  // instead of an empty table.
                  <p className="text-sm text-muted-foreground">
                    {t("results.suppressedFineGrainedControls")}
                  </p>
                ) : (
                  <FailingRecordsTable
                    rows={failingRows}
                    total={failedRows?.total}
                    loading={failedRowsQuery.isFetching || failedRowsQuery.isPending}
                    severityColors={sevColors}
                    severityRanks={sevRanks}
                    dimensionColors={dimColors}
                  />
                )}
              </CollapseRegion>
            </div>
          )}
        </div>
      </CollapsibleSection>
    </div>
  );
}
