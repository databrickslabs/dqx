import { useState } from "react";
import type * as React from "react";
import { QueryErrorResetBoundary, keepPreviousData } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { useTranslation } from "react-i18next";
import { ChevronDown, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  useGetProductResults,
  useGetProductResultsRuns,
  useGetTableResults,
  useGetDataProductSuspense,
  useGetDqResultsFailedRows,
  getDqResultsFailedRows,
  useListResultDimensionsSuspense,
  useListResultSeveritiesSuspense,
  type EntityResultsOut,
  type DataProductOut,
  type TrendPointOut,
  type DimensionOut,
  type SeverityOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { RunPicker, type Run } from "@/components/results/RunPicker";
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
import {
  toNum,
  ApplicableToggle,
  COUNT_INFO,
  EMPTY_FILTERS,
  toggleFacet,
  type Facet,
  type MultiFilters,
} from "@/components/monitored-tables/BindingResultsTab";

// Ported from dqlake's `components/products/ProductResultsTab.tsx` (the spec —
// keep structure/section order/interactions aligned with it). Deviations are
// the sanctioned ones only: hooks re-pointed to `/api/v1/dq-results/*`, all
// strings via t(), Genie stripped, and NO idle polling — queries use
// RESULTS_QUERY_OPTIONS and refresh comes from the run-completion invalidation
// in `lib/results-invalidation.ts` (dqlake's resultsPolling /
// useDataProductRunsActivity / useDataProductMemberActivity 5s/15s refetch
// loops are deliberately not ported). The RunInProgressBanner is omitted with
// them (same as the monitored-table tab): deriving run activity here would
// mean reintroducing an idle poll.
//
// RUN-PICKER SEMANTIC ADAPTATION (contract difference disclosed by P2.1):
// dqlake's product runs endpoint returns batch-keyed ProductRunsOut rows
// (product_run_id) and the picker pins every query "as of" that batch
// (`as_of_batch` carry-forward: trends show history up to the batch, scores
// carry each member's latest run at that instant). Our
// `/dq-results/product/{id}/runs` returns per-member-table run_id rollups
// (RunsOut — one row per underlying run, GROUP BY run_id), and the results
// endpoint's `run_id` param FILTERS rows to that single run rather than
// pinning as-of. So here: picking a run scopes the score + the Drilldown
// breakdowns to that run_id (like the monitored-table tab), while the
// Over-time charts stay all-runs (passing run_id would collapse every trend
// to a single point). "Latest" keeps dqlake's behaviour exactly (score =
// last point of the product trend; breakdowns unscoped).

/** undefined when the array is empty, else the array — so an empty facet is
 *  omitted from the query params (and stays backward-compatible). */
function orUndef(values: string[]): string[] | undefined {
  return values.length ? values : undefined;
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
 * Average points for the overall-mode trend chart: the as-of mean plotted
 * ONLY at run instants where every monitored member table has run as-of then
 * (so a freshly-added table yields one Average point now, growing into a
 * smooth line as runs accrue). Mirrors dqlake's inline computation.
 *
 * *trendByTable* supplies each member's earliest run; *trend* supplies the
 * product-mean points; *requiredFqns* is the member-table universe that must
 * all have run for the Average to exist. Values are scaled 0..100 for the
 * chart's OverallStep contract.
 */
export function computeOverallPoints(
  trendByTable: TrendPointOut[] | undefined,
  trend: TrendPointOut[] | undefined,
  requiredFqns: string[],
): OverallStep[] {
  // Per-table run extent from the table series: earliest run (for the
  // all-members-ran cutoff). run_date values are same-format ISO UTC strings,
  // so string compare orders them.
  const byTable = new Map<string, { first: string }>();
  for (const t of trendByTable ?? []) {
    const s = t.series;
    if (!s) continue;
    const rd = String(t.run_date ?? "");
    const cur = byTable.get(s);
    if (!cur) byTable.set(s, { first: rd });
    else if (rd < cur.first) cur.first = rd;
  }
  const everyRequiredRan =
    requiredFqns.length > 0 && requiredFqns.every((f) => byTable.has(f));
  if (!everyRequiredRan) return [];
  // The instant from which every member has a run: the latest of their first runs.
  const allRanSince = requiredFqns.reduce((mx, f) => {
    const fr = byTable.get(f)?.first ?? "";
    return fr > mx ? fr : mx;
  }, "");
  return (trend ?? [])
    .filter((t) => String(t.run_date ?? "") >= allRanSince)
    .map((t) => {
      const v = toNum(t.pass_rate);
      return { run_date: String(t.run_date ?? ""), value: v == null ? null : v * 100 };
    });
}

/** Facet → chip-label i18n key (static keys so the extractor sees them). */
const CHIP_LABEL_KEYS: Record<Facet, string> = {
  dimension: "resultsUi.chipDimension",
  severity: "resultsUi.chipSeverity",
  rule: "resultsUi.chipRule",
  column: "resultsUi.chipColumn",
};

/**
 * Results tab for a table space (data product). Reads the MV-backed
 * product-scoped DQ results endpoint and renders the average score, over-time
 * trends, count charts, and pass-rate breakdowns. By rule is full width (its
 * own row); By table and By column share the row below it. Filtering to a
 * single table reveals that table's invalid-samples view. The page shell
 * renders immediately and each widget loads independently.
 */
export function ProductResultsTab({ productId }: { productId: string }) {
  const { t } = useTranslation();
  return (
    <div className="space-y-6 pt-4 max-w-5xl">
      <QueryErrorResetBoundary>
        {({ reset }) => (
          <ErrorBoundary
            onReset={reset}
            fallbackRender={({ resetErrorBoundary }) => (
              <div className="rounded-md border border-dashed p-6 text-center space-y-2">
                <p className="text-sm">{t("resultsUi.loadFailed")}</p>
                <Button variant="outline" size="sm" onClick={resetErrorBoundary}>
                  {t("resultsUi.tryAgain")}
                </Button>
              </div>
            )}
          >
            <ResultsBody productId={productId} />
          </ErrorBoundary>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}

function ResultsBody({ productId }: { productId: string }) {
  const { t } = useTranslation();
  // The active single-table selection (E2). Clicking a By Table row sets it;
  // clicking again clears it. This stays SEPARATE from the facet filters — By
  // table is not a facet, it drives the invalid-samples view.
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  // Multi-select facet filters (dimension/severity/rule/column). Clicking a
  // breakdown row toggles the matching facet, which re-scopes every breakdown
  // and trend (mirrors the monitored-table tab).
  const [filters, setFilters] = useState<MultiFilters>(EMPTY_FILTERS);
  // The selected run_id, or null for "Latest" (the default). See the
  // run-picker semantic adaptation note in the module comment: a selected run
  // scopes the score + Drilldown breakdowns; Latest omits it.
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  // Drilldown scope: "Applicable" (default) cross-filters the breakdowns by the
  // active chips and hides rows excluded by them; "All" keeps the excluded base
  // rows, rendered greyed alongside the matching ones.
  const [applicableOnly, setApplicableOnly] = useState(true);
  // Shared collapse state — each boolean drives BOTH members of a pair.
  const [scoreBreakdownOpen, setScoreBreakdownOpen] = useState(true);
  const [countChartsOpen, setCountChartsOpen] = useState(false);
  const [ruleColOpen, setRuleColOpen] = useState(true);
  const [invalidSamplesOpen, setInvalidSamplesOpen] = useState(true);

  // The product run history for the run picker (newest-first). Our rows are
  // already RunPicker-shaped (run_id/run_ts/pass_rate). NON-suspense so the
  // picker fills in once available without blocking the rest of the shell.
  const runsQuery = useGetProductResultsRuns(productId, {
    query: { ...RESULTS_QUERY_OPTIONS },
  });
  const runs: Run[] = (runsQuery.data?.data?.rows ?? []).filter(
    (r): r is typeof r & { run_id: string } => typeof r.run_id === "string",
  );
  const latestRunId = runs[0]?.run_id;
  const isLatest = !selectedRunId || selectedRunId === latestRunId;
  const runIdParam = selectedRunId ?? undefined;

  // The FILTERED product trends: cross-filtered by the active facet chips
  // (dqlake's product tab re-scopes its trends on the chips, unlike the
  // monitored-table tab). NOT run-scoped — see the module comment. NON-suspense
  // so the shell + chart frames render first and each widget shows its own
  // spinner while loading (F1).
  const trendQuery = useGetProductResults(
    productId,
    {
      dimension: orUndef(filters.dimension),
      severity: orUndef(filters.severity),
      rule: orUndef(filters.rule),
      column: orUndef(filters.column),
      axes: "trend",
    },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const trends = trendQuery.data?.data;
  const loading = trendQuery.isPending;

  // The FILTERED product breakdowns: run-scoped AND cross-filtered by the
  // active facet chips. This is the live result in both modes — the numbers
  // always reflect the active filter.
  const resultsQuery = useGetProductResults(
    productId,
    {
      dimension: orUndef(filters.dimension),
      severity: orUndef(filters.severity),
      rule: orUndef(filters.rule),
      column: orUndef(filters.column),
      run_id: runIdParam,
      axes: "breakdown",
    },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const results = resultsQuery.data?.data;
  // The BASE (applicable) product breakdowns: run-scoped only, NO facet
  // filter. Drives the row set in "All" mode — base rows absent from the
  // filtered result render greyed. In "Applicable" mode it's unused beyond
  // being the same set.
  const baseQuery = useGetProductResults(
    productId,
    { run_id: runIdParam, axes: "breakdown" },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const baseResults = baseQuery.data?.data;
  // Background refetch (filter/selection change) vs first load: only the former
  // gets the subtle drilldown spinner — the first load has the ChartFrame and
  // per-widget spinners already.
  const breakdownRefetching = resultsQuery.isFetching && resultsQuery.data != null;

  const hasActiveFilter =
    filters.dimension.length > 0 ||
    filters.severity.length > 0 ||
    filters.rule.length > 0 ||
    filters.column.length > 0;

  // The product (with members) supplies the member-table universe. dqlake
  // built a table-FQN -> binding_id map here because its per-table endpoints
  // keyed on binding_id; ours key directly on the table FQN, so the friendly
  // name -> FQN map below is the whole selection resolution.
  const { data: product } = useGetDataProductSuspense(productId, selector<DataProductOut>());
  const memberFqns = Array.from(
    new Set((product.members ?? []).map((m) => m.table_fqn)),
  );
  // By table shows the FRIENDLY table name (last FQN segment), so the selection
  // is keyed on that. Map it back to the full FQN to resolve the table.
  const fqnByFriendly = new Map(
    (results?.by_table ?? [])
      .map((g) => g.label)
      .filter((l): l is string => l != null)
      .map((fqn) => [friendlyTableName(fqn), fqn] as const),
  );
  const selectedFqn = selectedTable ? (fqnByFriendly.get(selectedTable) ?? null) : null;

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
    { limit: 200 },
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

  // By column is scoped to the SELECTED table (columns are table-specific, so a
  // product-wide column list mixes columns from every member). Until a table is
  // picked the box is empty; once picked it shows that table's columns.
  const tableColumnsQuery = useGetTableResults(
    selectedFqn ?? "",
    { axes: "breakdown" },
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
    });
    return toFailingRecords(res.data.rows);
  };

  const toRows = (groups: EntityResultsOut["by_dimension"]) =>
    (groups ?? []).map((g) => ({
      label: g.label ?? null,
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
    const filteredByLabel = new Map(filteredRows.map((r) => [r.label, r]));
    const rows = toRows(baseGroups).map((b) => filteredByLabel.get(b.label) ?? b);
    const mutedLabels = rows
      .filter((r) => r.label != null && !filteredByLabel.has(r.label))
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

  const chips = (["dimension", "severity", "rule", "column"] as const).flatMap(
    (facet) =>
      filters[facet].map((value) => ({
        key: `${facet}:${value}`,
        label: t(CHIP_LABEL_KEYS[facet], { value }),
      })),
  );

  const onRemoveChip = (key: string) => {
    const [facet, value] = key.split(/:(.+)/) as [Facet, string];
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

  // Product average score. Latest (dqlake behaviour) = the latest point on the
  // product trend (the mean across members). A picked run = that run's rollup
  // from the runs list (our runs are per-member-run, not batches — see the
  // module comment). Failed tests always sum the (run-scoped, filtered)
  // by_table rows, matching dqlake's use of its filtered results.
  const selectedRun = selectedRunId
    ? runs.find((r) => r.run_id === selectedRunId)
    : undefined;
  const lastTrend = (trends?.trend ?? []).at(-1);
  const passRate = selectedRun ? toNum(selectedRun.pass_rate) : toNum(lastTrend?.pass_rate);
  const failedTests = (results?.by_table ?? []).reduce(
    (a, g) => a + (g.failed_tests ?? 0),
    0,
  );

  // Over-time chart: a prominent foreground "Average" trendline drawn on top of
  // dull, thin per-table lines. The Average is the mean of the member tables,
  // plotted ONLY at run instants where every monitored member has run as-of then
  // (so a freshly-added table yields one Average point now, growing into a smooth
  // line as runs accrue). Each table's latest run links to the latest Average
  // point with a dotted line.
  const perTable = toTrendSeries(trends?.trend_by_table);
  const tableColorMap = buildTableColorMap(trends?.trend_by_table);
  const overallPoints = computeOverallPoints(
    trends?.trend_by_table,
    trends?.trend,
    memberFqns,
  );

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

  const onTableSelect = (label: string) =>
    setSelectedTable((cur) => (cur === label ? null : label));

  const ChartFrame = ({ children }: { children: React.ReactNode }) =>
    loading ? (
      <div className="flex h-[224px] items-center justify-center rounded-md border">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    ) : (
      <>{children}</>
    );

  return (
    <div className="space-y-6">
      {/* The run picker overlaps the score box's top-right corner; it drops
          below the score on very small screens (mirrors the Tables tab). */}
      <div className="relative">
        <div className="z-10 sm:absolute sm:right-2 sm:top-2 max-sm:mb-2 max-sm:flex max-sm:justify-end">
          <RunPicker runs={runs} value={selectedRunId} onChange={setSelectedRunId} />
        </div>
        <div className="sm:pr-2">
          <ScoreBox
            passRate={passRate}
            failedTests={failedTests}
            totalTests={0}
            info={COUNT_INFO}
            label={t("resultsUi.averageScoreLabel")}
          />
        </div>
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
              every breakdown. By table is NOT a facet — it drives the
              invalid-samples view. */}
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
            <div className="md:col-span-2" data-testid="breakdown-by-rule">
              <DimensionBreakdown
                title={t("resultsUi.byRuleTitle")}
                valueHeader={t("resultsUi.ruleHeader")}
                countMode="checks"
                rows={ruleFacet.rows}
                mutedLabels={ruleFacet.mutedLabels}
                loading={breakdownRefetching}
                selected={filters.rule}
                onSelect={(label) => onRowToggle("rule", label)}
                collapsed={!ruleColOpen}
                onToggleCollapse={() => setRuleColOpen((o) => !o)}
              />
            </div>
            <div data-testid="breakdown-by-table">
              <DimensionBreakdown
                title={t("resultsUi.byTableTitle")}
                valueHeader={t("resultsUi.tableHeader")}
                rows={toRows(results?.by_table).map((r) => ({
                  ...r,
                  label: r.label == null ? null : friendlyTableName(r.label),
                }))}
                loading={breakdownRefetching}
                selected={selectedTable ? [selectedTable] : []}
                onSelect={onTableSelect}
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
                {selectedFqn && isLatest && !failedRows?.suppressed && (
                  <DownloadFailedRecordsMenu
                    fetchRows={fetchAllFailedRows}
                    tableName={selectedTable}
                  />
                )}
              </div>
              <CollapseRegion open={invalidSamplesOpen}>
                {!selectedFqn ? (
                  // dqlake's analogue is a member without a binding; ours is a
                  // By-table label that no longer resolves to a member FQN.
                  <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                    {t("resultsUi.invalidSamplesUnavailable")}
                  </div>
                ) : !isLatest ? (
                  // Our failed-rows endpoint always reads the latest run window
                  // (no run_id param), so showing it under an older picked run
                  // would be misleading — same gating as the monitored-table tab.
                  <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                    {t("resultsUi.failedRecordsLatestOnly")}
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
