import { useEffect, useState } from "react";
import type * as React from "react";
import { keepPreviousData } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { useTranslation } from "react-i18next";
import { ChevronDown, Loader2 } from "lucide-react";
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
import { FadeIn } from "@/components/anim/FadeIn";
import { CollapsibleSection } from "@/components/results/CollapsibleSection";
import { CollapseRegion } from "@/components/results/CollapseRegion";
import { ScoreTrendChart, type OverallStep } from "@/components/results/ScoreTrendChart";
import { DimensionBreakdown, TruncatedText } from "@/components/results/DimensionBreakdown";
import { FilterChips } from "@/components/results/FilterChips";
import { ResultsFacetFilter, type FacetFilterOption } from "@/components/results/ResultsFacetFilters";
import { FailingRecordsTable } from "@/components/results/FailingRecordsTable";
import { DownloadFailedRecordsMenu } from "@/components/results/DownloadFailedRecordsMenu";
import {
  EXPORT_ROW_LIMIT,
  toFailingRecords,
} from "@/components/results/failedRecordsExport";
import { toCountSeries } from "@/components/results/countSeries";
import { RunModeSelect, includeDraftsParam } from "@/components/results/RunModeSelect";
import { RunReviewStatusPanel } from "@/components/RunReviewStatusPanel";
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
import { usePassThresholdEnabled } from "@/hooks/use-pass-threshold-enabled";

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
  asOfBatch?: string | null,
): EntityResultsParams {
  return {
    ...facetQueryParams(filters),
    axes: "breakdown",
    include_drafts: includeDraftsParam(includeDrafts),
    // The chosen run-batch (a run_id from the batch-keyed picker) caps
    // every axis to that batch's instant. Omitted entirely when no batch
    // is pinned so the query stays the "newest" default.
    ...(asOfBatch ? { as_of_batch: asOfBatch } : {}),
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

/** Catalog part of a `catalog.schema.table` FQN (mirrors backend catalog_of). */
export function catalogOfFqn(fqn: string | null | undefined): string {
  return (fqn ?? "").split(".")[0] ?? "";
}

/** Two-part `catalog.schema` identity of an FQN (mirrors backend schema_of):
 *  unambiguous across catalogs. Empty when the FQN has fewer than two parts. */
export function schemaOfFqn(fqn: string | null | undefined): string {
  const parts = (fqn ?? "").split(".");
  return parts.length >= 2 ? `${parts[0]}.${parts[1]}` : "";
}

/** Display name for a schema identity = its bare schema segment (the part
 *  after the catalog); the catalog is shown by its own chip/filter. */
export function friendlySchemaName(schemaIdentity: string): string {
  const parts = schemaIdentity.split(".");
  return parts.length >= 2 ? parts[1] : schemaIdentity;
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
  catalog: "resultsUi.chipCatalog",
  schema: "resultsUi.chipSchema",
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
  /** B2-8: when set, the selected run's review status renders in its own
   *  full-width INTERACTABLE card (the RunReviewStatusPanel) between the score
   *  and the over-time trend. The table-space tab passes its pinned (latest)
   *  run id; the global and rule surfaces have no coherent single run and omit
   *  it, so no card renders there. */
  reviewStatusRunId?: string | null;
  /** Chosen run batch (a run_id from the batch-keyed runs picker) to read
   *  results AS-OF. Threaded into every axis query (trend + breakdown +
   *  base) so the whole surface truncates to that batch's instant. null /
   *  omitted = newest (no cap). Only the table-space tab supplies it (it
   *  owns the RunPicker); the global and rule surfaces omit it. */
  asOfBatch?: string | null;
  /** Item 35: when true, a row of top-level facet dropdowns (Tables /
   *  Dimensions / Severities / Rules / Columns) renders just below the score
   *  card, letting the viewer filter the whole surface without hunting through
   *  the breakdown boxes. Only the Global Results page passes this — the
   *  per-object surfaces keep their scope implicit. The dropdown options are
   *  derived from the BASE (unfiltered) breakdown rows so the choice list is
   *  the full facet universe regardless of the active filter. */
  showTopLevelFilters?: boolean;
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
  reviewStatusRunId,
  asOfBatch,
  showTopLevelFilters,
}: MultiTableResultsSectionProps) {
  const { t } = useTranslation();
  // Belt-and-suspenders: the backend returns no breaches when the feature is
  // disabled (Task 2), but we also hide all breach UI client-side to avoid
  // any stale-cache flash when the setting was recently changed.
  const thresholdEnabled = usePassThresholdEnabled();
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
      ...(asOfBatch ? { as_of_batch: asOfBatch } : {}),
    },
    { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
  );
  const trends = trendQuery.data?.data;
  // isFetching (not isPending) so the chart frames re-show their spinner while a
  // run-mode switch (or facet change) re-fetches (B2-138) — keepPreviousData
  // otherwise keeps the stale charts up with isPending false, hiding the load.
  const loading = trendQuery.isFetching;

  // The FILTERED breakdowns: cross-filtered by the active facet chips (never
  // run-scoped — see breakdownParams). This is the live result in both modes
  // — the numbers always reflect the active filter.
  const resultsQuery = useEntityResults(breakdownParams(filters, effectiveIncludeDrafts, asOfBatch), {
    placeholderData: keepPreviousData,
    ...RESULTS_QUERY_OPTIONS,
  });
  const results = resultsQuery.data?.data;
  // The BASE (applicable) breakdowns: NO facet filter. Drives the row set in
  // "All" mode — base rows absent from the filtered result render greyed. In
  // "Applicable" mode it's unused beyond being the same set.
  const baseQuery = useEntityResults(breakdownParams(EMPTY_FILTERS, effectiveIncludeDrafts, asOfBatch), {
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
  // Registry rule ids present across the by-rule breakdown (filtered + base) —
  // the By rule names deep-link to the rule's registry detail, but only rows
  // carrying a genuine registry rule_id are linkable (ad-hoc rules have none).
  const registryRuleIds = new Set(
    [...(baseResults?.by_rule ?? []), ...(results?.by_rule ?? [])]
      .map((g) => g.rule_id)
      .filter((id): id is string => typeof id === "string" && id.length > 0),
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
      breached: g.breached ?? false,
      breach_criticality: g.breach_criticality ?? null,
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
  // By table honors the Applicable/All toggle like every other facet box:
  // Applicable hides rows excluded by the active chips; All keeps the base
  // rows and greys the excluded ones (with their base numbers + breach icon),
  // so a breached-but-filtered-out table still surfaces. Its labels are
  // remapped to the friendly (last-segment) name at render, so the muted-label
  // set is remapped the same way to stay matched.
  const tableFacet = buildFacet(results?.by_table, baseResults?.by_table);

  // Rule name → breach criticality for the failing-records cell hover's ⚠.
  // Keyed by the by_rule row label (what failures carry as rule_name); only
  // breached rows contribute; suppressed when the threshold feature is off.
  const breachedRuleCriticality: Record<string, string> = {};
  if (thresholdEnabled) {
    for (const r of [...(baseResults?.by_rule ?? []), ...(results?.by_rule ?? [])]) {
      if (r.breached && r.label && (r.breach_criticality === "error" || r.breach_criticality === "warn")) {
        breachedRuleCriticality[r.label] = r.breach_criticality;
      }
    }
  }
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
    // Schema chips show the bare schema name (the catalog has its own chip).
    if (facet === "schema") return friendlySchemaName(value);
    return value;
  };
  const chips = (
    ["dimension", "severity", "rule", "column", "table", "catalog", "schema"] as const
  ).flatMap((facet) =>
    filters[facet].map((value) => ({
      key: `${facet}:${value}`,
      label: t(CHIP_LABEL_KEYS[facet], { value: chipDisplay(facet, value) }),
    })),
  );

  // Item 35: option lists for the top-level Global Results filter dropdowns.
  // Derived from the BASE (unfiltered) breakdown rows so the choice universe
  // is the full facet set regardless of the active filter. Tables carry the
  // FQN as value but display the friendly (last-segment) name; rules key on
  // their registry rule identity (rule_id when present) and display the label.
  // Deduped + sorted by display label for a stable, scannable list.
  const dedupeSortOptions = (opts: FacetFilterOption[]): FacetFilterOption[] => {
    const byValue = new Map<string, FacetFilterOption>();
    for (const o of opts) {
      if (o.value && !byValue.has(o.value)) byValue.set(o.value, o);
    }
    return [...byValue.values()].sort((a, b) => a.label.localeCompare(b.label));
  };
  // Every accessible table FQN (base by_table row set) — the universe the
  // catalog → schema → table dropdowns are derived and cascaded from.
  const allTableFqns = (baseResults?.by_table ?? [])
    .map((g) => g.label)
    .filter((l): l is string => l != null);
  const selectedCatalogs = new Set(filters.catalog);
  const selectedSchemas = new Set(filters.schema);
  // Catalog options: every distinct catalog. Schema options: narrowed to the
  // selected catalog(s) (all catalogs when none picked). Table options:
  // narrowed to the selected catalog(s) AND schema(s). This is the hierarchical
  // cascade — a narrower level's option list only offers what the wider levels
  // allow, matching the discovery browser's catalog → schema → table flow.
  const catalogFilterOptions = dedupeSortOptions(
    allTableFqns.map((fqn) => ({ value: catalogOfFqn(fqn), label: catalogOfFqn(fqn) })),
  );
  const schemaFilterOptions = dedupeSortOptions(
    allTableFqns
      .filter((fqn) => selectedCatalogs.size === 0 || selectedCatalogs.has(catalogOfFqn(fqn)))
      .map((fqn) => ({ value: schemaOfFqn(fqn), label: friendlySchemaName(schemaOfFqn(fqn)) })),
  );
  const tableFilterOptions = dedupeSortOptions(
    allTableFqns
      .filter((fqn) => selectedCatalogs.size === 0 || selectedCatalogs.has(catalogOfFqn(fqn)))
      .filter((fqn) => selectedSchemas.size === 0 || selectedSchemas.has(schemaOfFqn(fqn)))
      .map((fqn) => ({ value: fqn, label: friendlyTableName(fqn) })),
  );
  const ruleFilterOptions = dedupeSortOptions(
    (baseResults?.by_rule ?? [])
      .map((g) => {
        const value = ruleFacetValue(g);
        if (value == null) return null;
        return { value, label: g.label ?? value };
      })
      .filter((o): o is FacetFilterOption => o != null),
  );

  // Setting the Tables dropdown drives the `table` facet directly (multi-select
  // filter). It intentionally does NOT touch `selectedTable` — that single
  // selection (from a By-table row click) still owns the invalid-samples /
  // By-column drilldown; picking multiple tables here just scopes the shared
  // breakdowns/trends. Clearing the dropdown to empty also clears any lingering
  // single selection so the two never disagree.
  const onTableFilterChange = (values: string[]) => {
    setFilters((f) => ({ ...f, table: values }));
    if (values.length === 0) setSelectedTable(null);
  };
  const onFacetFilterChange = (facet: Facet, values: string[]) =>
    setFilters((f) => ({ ...f, [facet]: values }));

  // Catalog/schema cascade: narrowing a WIDER level prunes any downstream
  // selection it no longer permits, so the active filters can never disagree
  // with the (now-narrowed) option lists. Widening (adding a catalog) leaves
  // downstream picks untouched.
  const onCatalogFilterChange = (values: string[]) => {
    const allow = new Set(values);
    setFilters((f) => {
      const keepSchema = allow.size === 0 ? f.schema : f.schema.filter((s) => allow.has(catalogOfFqn(s)));
      const keepTable = allow.size === 0 ? f.table : f.table.filter((t) => allow.has(catalogOfFqn(t)));
      if (keepTable.length === 0) setSelectedTable(null);
      return { ...f, catalog: values, schema: keepSchema, table: keepTable };
    });
  };
  const onSchemaFilterChange = (values: string[]) => {
    const allow = new Set(values);
    setFilters((f) => {
      const keepTable = allow.size === 0 ? f.table : f.table.filter((t) => allow.has(schemaOfFqn(t)));
      if (keepTable.length === 0) setSelectedTable(null);
      return { ...f, schema: values, table: keepTable };
    });
  };

  const onRemoveChip = (key: string) => {
    const [facet, value] = key.split(/:(.+)/) as [Facet, string];
    // The table facet mirrors the invalid-samples selection — removing its
    // chip clears both, exactly like re-clicking the selected By table row.
    if (facet === "table") setSelectedTable(null);
    // Catalog/schema chips route through the cascade handlers so removing a
    // wider level also prunes the downstream picks it no longer permits.
    if (facet === "catalog") {
      onCatalogFilterChange(filters.catalog.filter((v) => v !== value));
      return;
    }
    if (facet === "schema") {
      onSchemaFilterChange(filters.schema.filter((v) => v !== value));
      return;
    }
    setFilters((f) => toggleFacet(f, facet, value));
  };

  const onRowToggle = (facet: Facet, label: string) =>
    setFilters((f) => toggleFacet(f, facet, label));

  const toTrendSeries = (points: TrendPointOut[] | undefined) =>
    (points ?? []).map((p) => ({
      run_date: String(p.run_date ?? ""),
      series: p.series ?? undefined,
      pass_rate: toNum(p.pass_rate),
      is_draft: p.is_draft ?? false,
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

  // Breach markers for the overall trend chart: one entry per breaching point
  // on the entity-level `trend` (the overall average). Null/non-"error"/"warn"
  // criticalities are filtered out so only real breaches render a marker.
  // Suppressed entirely when the pass-threshold feature is disabled.
  const overallBreachMarkers = thresholdEnabled
    ? (trends?.trend ?? []).flatMap((p) => {
        if (!p.breached) return [];
        const crit = p.breach_criticality;
        if (crit !== "error" && crit !== "warn") return [];
        // score = the point's 0–100 y-value (pass_rate * 100) so the ⚠ icon
        // anchors to the trend line at that run, matching how the series plots.
        const rate = toNum(p.pass_rate);
        return [
          {
            run_date: String(p.run_date ?? ""),
            criticality: crit as "error" | "warn",
            score: rate == null ? null : rate * 100,
          },
        ];
      })
    : [];

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
      breachCriticality={thresholdEnabled ? lastTrend?.breach_criticality : null}
    />
  );

  return (
    <div className="space-y-6">
      {/* Subtle staggered entrance: the score leads, then the review status,
          over-time trends and drilldown fade up in sequence (each FadeIn
          honours prefers-reduced-motion via the shared motion wrapper). */}
      {/* The run-mode dropdown (always) and the run picker (when the caller
          supplies one) overlap the score box's top-right corner; they drop
          below the score on very small screens (mirrors the Tables tab). */}
      <FadeIn delay={0} className="relative">
        <div className="z-10 flex items-center gap-2 sm:absolute sm:right-2 sm:top-2 max-sm:mb-2 max-sm:justify-end">
          {!hideRunMode && (
            <RunModeSelect includeDrafts={includeDrafts} onChange={onIncludeDraftsChange} />
          )}
          {runPickerSlot}
        </div>
        <div className="sm:pr-2">{scoreBox}</div>
      </FadeIn>

      {/* Item 35: top-level facet dropdowns just below the overall score card
          (Global Results only). They set the same facet filters the breakdown
          rows toggle, so they re-scope every trend + breakdown box. Styled to
          match the overview filter bubbles (shared FILTER_TRIGGER_CLASS). The
          active-chip row below the drilldown still mirrors the selection, so a
          filter set here is also removable there.

          Order is the discovery hierarchy — catalog → schema → table → rule.
          Catalog/schema cascade: each narrows the next level's option list.
          Dimension/severity dropdowns were removed here (still reachable by
          clicking a By-dimension / By-severity breakdown row). Column is
          absent — columns are table-specific (only populated once a single
          table is selected), so there is no cross-table column universe. */}
      {showTopLevelFilters && (
        <FadeIn delay={0.04}>
          <div className="flex flex-wrap items-center gap-2">
            <ResultsFacetFilter
              allLabel={t("resultsUi.filterAllCatalogs")}
              options={catalogFilterOptions}
              selected={filters.catalog}
              onChange={onCatalogFilterChange}
              searchPlaceholder={t("resultsUi.filterSearchCatalogs")}
              emptyText={t("resultsUi.filterNoCatalogs")}
              ariaLabel={t("resultsUi.filterAllCatalogs")}
            />
            <ResultsFacetFilter
              allLabel={t("resultsUi.filterAllSchemas")}
              options={schemaFilterOptions}
              selected={filters.schema}
              onChange={onSchemaFilterChange}
              searchPlaceholder={t("resultsUi.filterSearchSchemas")}
              emptyText={t("resultsUi.filterNoSchemas")}
              ariaLabel={t("resultsUi.filterAllSchemas")}
            />
            <ResultsFacetFilter
              allLabel={t("resultsUi.filterAllTables")}
              options={tableFilterOptions}
              selected={filters.table}
              onChange={onTableFilterChange}
              searchPlaceholder={t("resultsUi.filterSearchTables")}
              emptyText={t("resultsUi.filterNoTables")}
              ariaLabel={t("resultsUi.filterAllTables")}
            />
            <ResultsFacetFilter
              allLabel={t("resultsUi.filterAllRules")}
              options={ruleFilterOptions}
              selected={filters.rule}
              onChange={(v) => onFacetFilterChange("rule", v)}
              searchPlaceholder={t("resultsUi.filterSearchRules")}
              emptyText={t("resultsUi.filterNoRules")}
              ariaLabel={t("resultsUi.filterAllRules")}
            />
          </div>
        </FadeIn>
      )}

      {/* B2-8: the pinned run's review status in its own full-width
          INTERACTABLE card between the score and the over-time trend (the
          RunReviewStatusPanel — dropdown + revert-to-default + audit history,
          gated for users without permission). Only surfaces that pass a run id
          (the table-space tab) render it; global/rule omit it. */}
      {reviewStatusRunId && (
        <FadeIn delay={0.06} className="rounded-lg border bg-card p-4">
          <RunReviewStatusPanel runId={reviewStatusRunId} />
        </FadeIn>
      )}

      <FadeIn delay={0.12}>
      <CollapsibleSection title={t("resultsUi.overTimeSection")} defaultOpen>
        <div className="space-y-6">
          <ChartFrame>
            <ScoreTrendChart
              data={perTable}
              colorMap={tableColorMap}
              overall={overallPoints}
              overallLabel={t("resultsUi.averageSeries")}
              title={t("resultsUi.averageDqScoreTitle")}
              breachMarkers={overallBreachMarkers}
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
      </FadeIn>

      <FadeIn delay={0.18}>
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
              breachEnabled={thresholdEnabled}
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
              breachEnabled={thresholdEnabled}
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
                  breachEnabled={thresholdEnabled}
                  // The rule NAME navigates to that registry rule's detail
                  // (only for rows with a genuine registry rule_id); clicking
                  // elsewhere on the row still toggles the rule facet — the
                  // Link stops propagation so the two gestures stay distinct.
                  renderLabel={(label, value) =>
                    value && registryRuleIds.has(value) ? (
                      <Link
                        to="/registry-rules/$ruleId"
                        params={{ ruleId: value }}
                        onClick={(e) => e.stopPropagation()}
                        aria-label={t("resultsUi.openRuleDetailAria", { rule: label })}
                        className="min-w-0 text-foreground hover:underline"
                      >
                        <TruncatedText text={label} className="min-w-0" />
                      </Link>
                    ) : null
                  }
                />
              </div>
            )}
            <div data-testid="breakdown-by-table">
              <DimensionBreakdown
                title={t("resultsUi.byTableTitle")}
                valueHeader={t("resultsUi.tableHeader")}
                rows={tableFacet.rows.map((r) => ({
                  ...r,
                  label: r.label == null ? null : friendlyTableName(r.label),
                  // Selection is keyed on the friendly name (see
                  // fqnByFriendly) — null the value so clicks/selection
                  // fall back to the friendly label, not the full FQN.
                  value: null,
                }))}
                // Muted labels come back as FQNs; remap to the friendly name
                // so they match the remapped row labels above.
                mutedLabels={tableFacet.mutedLabels.map((l) => friendlyTableName(l))}
                loading={breakdownRefetching}
                selected={selectedTable ? [selectedTable] : []}
                onSelect={onTableSelect}
                pageSize={8}
                breachEnabled={thresholdEnabled}
                // The table NAME navigates to that monitored table's Results
                // tab (rows with a binding); clicking elsewhere on the row
                // still drives the invalid-samples selection / table facet —
                // the Link stops propagation so the two gestures stay distinct.
                // (The former "open in new tab" icon is dropped as redundant.)
                renderLabel={(label) => {
                  const bindingId = bindingIdByFriendly.get(label);
                  if (!bindingId) return null;
                  return (
                    <Link
                      to="/monitored-tables/$bindingId"
                      params={{ bindingId }}
                      search={{ tab: "results" }}
                      onClick={(e) => e.stopPropagation()}
                      aria-label={t("resultsUi.openTableResultsAria", { table: label })}
                      className="min-w-0 text-foreground hover:underline"
                    >
                      <TruncatedText text={label} className="min-w-0" />
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
                breachEnabled={thresholdEnabled}
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
                    breachedRuleCriticality={breachedRuleCriticality}
                  />
                )}
              </CollapseRegion>
            </div>
          )}
        </div>
      </CollapsibleSection>
      </FadeIn>
    </div>
  );
}
