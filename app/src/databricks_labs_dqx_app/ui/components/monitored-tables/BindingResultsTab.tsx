import { useState } from "react";
import type * as React from "react";
import { QueryErrorResetBoundary, keepPreviousData } from "@tanstack/react-query";
import { ErrorBoundary } from "react-error-boundary";
import { Trans, useTranslation } from "react-i18next";
import { ChevronDown, Loader2, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import {
  useGetTableResults,
  useGetDqResultsRunsSuspense,
  useGetDqResultsFailedRows,
  useListResultDimensionsSuspense,
  useListResultSeveritiesSuspense,
  getDqResultsFailedRows,
  type EntityResultsOut,
  type RunsOut,
  type TrendPointOut,
  type DimensionOut,
  type SeverityOut,
} from "@/lib/api";
import selector from "@/lib/selector";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ScoreBox } from "@/components/results/ScoreBox";
import { RunPicker } from "@/components/results/RunPicker";
import { RunModeSelect, includeDraftsParam } from "@/components/results/RunModeSelect";
import { RunInProgressBanner } from "@/components/results/RunInProgressBanner";
import { CollapsibleSection } from "@/components/results/CollapsibleSection";
import { CollapseRegion } from "@/components/results/CollapseRegion";
import { ScoreTrendChart } from "@/components/results/ScoreTrendChart";
import { DimensionBreakdown } from "@/components/results/DimensionBreakdown";
import { FilterChips } from "@/components/results/FilterChips";
import { FailingRecordsTable } from "@/components/results/FailingRecordsTable";
import { DownloadFailedRecordsMenu } from "@/components/results/DownloadFailedRecordsMenu";
import {
  EXPORT_ROW_LIMIT,
  toFailingRecords,
} from "@/components/results/failedRecordsExport";
import { toCountSeries } from "@/components/results/countSeries";

// Ported from dqlake's `components/bindings/BindingIssuesTab.tsx` (the spec —
// keep structure/section order/interactions aligned with it). Deviations are
// the sanctioned ones only: hooks re-pointed to `/api/v1/dq-results/*`, all
// strings via t(), Genie stripped, and NO idle polling — queries use
// RESULTS_QUERY_OPTIONS and refresh comes from the run-completion
// invalidation in `lib/results-invalidation.ts` (dqlake's resultsPolling /
// useBindingRunsActivity 5s/15s refetch loops are deliberately not ported).
// The RunInProgressBanner IS shown, but its signal is active-run-scoped:
// the detail page polls ONLY a run set it just triggered (see the
// `runInProgress` prop) — never an idle poll.

export type Facet = "dimension" | "severity" | "rule" | "column";

/** Help text shown behind the "?" icon on the two count charts. The rule /
 *  check / test terms are bold so the distinction reads at a glance. */
function CountInfo() {
  return (
    <span>
      <Trans
        i18nKey="resultsUi.countInfo"
        components={{ b: <strong /> }}
      />
    </span>
  );
}

export const COUNT_INFO = <CountInfo />;

export type MultiFilters = {
  dimension: string[];
  severity: string[];
  rule: string[];
  column: string[];
  runId?: string | null;
};

export const EMPTY_FILTERS: MultiFilters = {
  dimension: [],
  severity: [],
  rule: [],
  column: [],
};

/** Toggle a value in a facet's multi-select set: add if absent, remove if
 *  present. Other facets are untouched. */
export function toggleFacet(
  filters: MultiFilters,
  facet: Facet,
  value: string,
): MultiFilters {
  const cur = filters[facet];
  const next = cur.includes(value)
    ? cur.filter((v) => v !== value)
    : [...cur, value];
  return { ...filters, [facet]: next };
}

/** undefined when the array is empty, else the array — so an empty facet is
 *  omitted from the query params (and stays backward-compatible). */
function orUndef(values: string[]): string[] | undefined {
  return values.length ? values : undefined;
}

/** The facet chips → query params mapping shared by every FILTERED query on
 *  this tab (the filtered breakdown + the failing-records list). The BASE
 *  breakdown query deliberately does NOT use it — it passes no facets, so in
 *  "All" mode the excluded rows are still available to render greyed
 *  (dqlake's filtered-vs-base wiring). Exported for the facet→params tests. */
export function facetQueryParams(filters: MultiFilters): {
  dimension?: string[];
  severity?: string[];
  rule?: string[];
  column?: string[];
} {
  return {
    dimension: orUndef(filters.dimension),
    severity: orUndef(filters.severity),
    rule: orUndef(filters.rule),
    column: orUndef(filters.column),
  };
}

/** Coerce a pass-rate that may arrive as a number or a numeric string into a
 *  number; anything non-finite (incl. null/undefined) becomes null. */
export function toNum(value: unknown): number | null {
  const n = typeof value === "string" ? Number(value) : value;
  return typeof n === "number" && Number.isFinite(n) ? n : null;
}

/**
 * Results tab for a single monitored table. Reads the MV-backed DQ results
 * endpoints: a table-scoped scores overview with multi-select facet
 * drill-down, a run picker overlaid on the score, over-time charts, and the
 * failing source records for the selected run. The page shell renders
 * immediately; each widget loads independently (no page-wide skeleton).
 */
export function BindingResultsTab({
  bindingId,
  tableName,
  tableFqn,
  neverApproved,
  runInProgress,
}: {
  bindingId: string;
  /** Friendly table name (for the failed-records download filename). */
  tableName?: string;
  /** Fully-qualified `catalog.schema.table` — the dq-results endpoints key on it. */
  tableFqn: string;
  /** True when the binding has never been approved (version 0): its runs are
   *  all drafts, hidden by the published-only default — an informational
   *  notice points at the run-mode dropdown. */
  neverApproved?: boolean;
  /** True while a run triggered FROM this detail page is still executing
   *  (the page polls that run set's status — see the detail route). Shows
   *  dqlake's in-progress banner; results refresh via the completion
   *  invalidation once it settles. */
  runInProgress?: boolean;
}) {
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
            <ResultsBody
              bindingId={bindingId}
              tableName={tableName}
              tableFqn={tableFqn}
              neverApproved={neverApproved}
              runInProgress={runInProgress}
            />
          </ErrorBoundary>
        )}
      </QueryErrorResetBoundary>
    </div>
  );
}

/** Applicable / All segmented toggle for the whole Drilldown section. Both
 *  modes recompute the breakdown numbers for the active facet filters; they
 *  differ only in how rows excluded by that filter are shown. "Applicable"
 *  (default) hides the excluded base rows; "All" keeps them, rendered
 *  greyed/thin alongside the matching rows. */
export function ApplicableToggle({
  applicableOnly,
  onChange,
  disabled,
}: {
  applicableOnly: boolean;
  onChange: (applicableOnly: boolean) => void;
  /** When set, the toggle is inert (e.g. the product tab has no facet filters,
   *  so the two modes are identical). */
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div
      className={cn(
        "inline-flex rounded-md border p-0.5 text-xs",
        disabled && "opacity-50",
      )}
      role="group"
      aria-label={t("resultsUi.drilldownScopeAria")}
    >
      <button
        type="button"
        disabled={disabled}
        aria-pressed={!applicableOnly}
        onClick={() => onChange(false)}
        className={cn(
          "rounded px-2 py-0.5",
          !applicableOnly ? "bg-muted font-medium" : "text-muted-foreground",
        )}
      >
        {t("resultsUi.scopeAll")}
      </button>
      <button
        type="button"
        disabled={disabled}
        aria-pressed={applicableOnly}
        onClick={() => onChange(true)}
        className={cn(
          "rounded px-2 py-0.5",
          applicableOnly ? "bg-muted font-medium" : "text-muted-foreground",
        )}
      >
        {t("resultsUi.scopeApplicable")}
      </button>
    </div>
  );
}

/** Type-to-filter search box for the By rule / By column boxes (C4). */
function FacetSearch({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="relative">
      <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
      <Input
        aria-label={t("resultsUi.searchFacetAria", { label })}
        placeholder={t("resultsUi.searchFacetPlaceholder", { label })}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="h-7 w-40 pl-7 text-xs"
      />
    </div>
  );
}

/** Facet → chip-label i18n key (static keys so the extractor sees them). */
const CHIP_LABEL_KEYS: Record<Facet, string> = {
  dimension: "resultsUi.chipDimension",
  severity: "resultsUi.chipSeverity",
  rule: "resultsUi.chipRule",
  column: "resultsUi.chipColumn",
};

function ResultsBody({
  bindingId,
  tableName,
  tableFqn,
  neverApproved,
  runInProgress,
}: {
  bindingId: string;
  tableName?: string;
  tableFqn: string;
  neverApproved?: boolean;
  runInProgress?: boolean;
}) {
  const { t } = useTranslation();
  const [filters, setFilters] = useState<MultiFilters>(EMPTY_FILTERS);
  // Run mode: "Published only" (default) or "Published + Draft". Per-surface
  // state — every dq-results query on THIS tab gets `include_drafts` from it
  // (true only when drafts are included; omitted otherwise so the backend's
  // published-only default applies).
  const [includeDrafts, setIncludeDrafts] = useState(false);
  const draftsParam = includeDraftsParam(includeDrafts);
  // Drilldown scope: "applicable" (default) cross-filters the breakdowns by the
  // active chips; "all" shows each breakdown's full set ignoring those chips.
  const [applicableOnly, setApplicableOnly] = useState(true);
  // By rule/col: substring search.
  const [ruleSearch, setRuleSearch] = useState("");
  const [colSearch, setColSearch] = useState("");
  // Shared collapse state — each boolean drives BOTH members of a pair so the
  // chevron on either one toggles them together.
  const [scoreBreakdownOpen, setScoreBreakdownOpen] = useState(true);
  const [countChartsOpen, setCountChartsOpen] = useState(false);
  const [ruleColOpen, setRuleColOpen] = useState(true);
  const [failedRecordsOpen, setFailedRecordsOpen] = useState(true);

  // No run-activity hook and no poll cadence here (dqlake's
  // useBindingRunsActivity + resultsPolling): every query below is
  // staleTime-Infinity and is refreshed by the run-completion invalidation
  // (Runs History's RUNNING-run poll / the product run-set poll), which
  // covers the whole `/api/v1/dq-results/` prefix.

  // Runs list (per-run pass rate / failed / total). Suspense — the page needs a
  // run to scope the score, and switching runs is a deliberate action. Keyed
  // on the binding id (the endpoint accepts a binding id or a table FQN),
  // matching dqlake's binding_id param.
  const { data: runsData } = useGetDqResultsRunsSuspense<RunsOut>(
    bindingId,
    { include_drafts: draftsParam },
    {
      query: { ...selector<RunsOut>().query, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const runs = (runsData.rows ?? []).filter(
    (r): r is typeof r & { run_id: string } => typeof r.run_id === "string",
  );
  const latestRunId = runs[0]?.run_id;
  const isLatest = !filters.runId || filters.runId === latestRunId;
  const effectiveRunId = filters.runId ?? latestRunId;

  // Filter-INDEPENDENT, ALL-RUNS trends. NON-suspense so the chart frames can
  // render immediately and each shows its own spinner while loading (F1).
  const trendQuery = useGetTableResults(
    tableFqn,
    { axes: "trend", include_drafts: draftsParam },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const trend = trendQuery.data?.data;
  const trendsLoading = trendQuery.isPending;

  // The FILTERED breakdown: run-scoped AND cross-filtered by the active facet
  // chips. This is the live result in both modes — the numbers always reflect
  // the active filter.
  const filteredTableQuery = useGetTableResults(
    tableFqn,
    {
      ...facetQueryParams(filters),
      run_id: effectiveRunId,
      axes: "breakdown",
      include_drafts: draftsParam,
    },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const filteredTable = filteredTableQuery.data?.data;
  // The BASE (applicable) breakdown: run-scoped only, NO facet filter. Drives
  // the row set in "All" mode — rows absent from the filtered result show
  // greyed. In "Applicable" mode it's unused beyond being the same set.
  const baseTableQuery = useGetTableResults(
    tableFqn,
    { run_id: effectiveRunId, axes: "breakdown", include_drafts: draftsParam },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const baseTable = baseTableQuery.data?.data;
  // Background refetch (filter change) vs first load: only the former gets the
  // per-box spinner — the first load has no data to keep showing.
  const breakdownRefetching =
    filteredTableQuery.isFetching && filteredTableQuery.data != null;
  const filtered = {
    by_dimension: filteredTable?.by_dimension ?? [],
    by_severity: filteredTable?.by_severity ?? [],
    by_rule: filteredTable?.by_rule ?? [],
    by_column: filteredTable?.by_column ?? [],
  };
  const base = {
    by_dimension: baseTable?.by_dimension ?? [],
    by_severity: baseTable?.by_severity ?? [],
    by_rule: baseTable?.by_rule ?? [],
    by_column: baseTable?.by_column ?? [],
  };
  const hasActiveFilter =
    filters.dimension.length > 0 ||
    filters.severity.length > 0 ||
    filters.rule.length > 0 ||
    filters.column.length > 0;

  const { data: dimensions } =
    useListResultDimensionsSuspense<DimensionOut[]>(selector<DimensionOut[]>());
  const { data: severities } =
    useListResultSeveritiesSuspense<SeverityOut[]>(selector<SeverityOut[]>());

  const dimColors = Object.fromEntries(dimensions.map((d) => [d.name, d.color]));
  const sevColors = Object.fromEntries(severities.map((s) => [s.name, s.color]));
  const sevRanks = Object.fromEntries(severities.map((s) => [s.name, s.rank]));

  // Failing records: NON-suspense so filter changes refetch only this section.
  const failedRowsQuery = useGetDqResultsFailedRows(
    tableFqn,
    {
      ...facetQueryParams(filters),
      limit: 200,
      include_drafts: draftsParam,
    },
    {
      query: { placeholderData: keepPreviousData, ...RESULTS_QUERY_OPTIONS },
    },
  );
  const failedRows = failedRowsQuery.data?.data;

  // The score reflects a SINGLE run (the picked one, else the latest).
  const selectedIdx = filters.runId
    ? runs.findIndex((r) => r.run_id === filters.runId)
    : 0;
  const selectedRun = selectedIdx >= 0 ? runs[selectedIdx] : runs[0];
  const passRate = selectedRun?.pass_rate ?? null;
  const failedTests = selectedRun?.failed_tests ?? 0;
  const totalTests = selectedRun?.total_tests ?? 0;
  // Score direction vs the next-older run (runs are newest-first). null when
  // equal or there's no previous run.
  const prevPassRate =
    selectedIdx >= 0 ? (runs[selectedIdx + 1]?.pass_rate ?? null) : null;
  const scoreTrend: "up" | "down" | null =
    passRate != null && prevPassRate != null && passRate !== prevPassRate
      ? passRate > prevPassRate
        ? "up"
        : "down"
      : null;

  const toRows = (groups: EntityResultsOut["by_dimension"]) =>
    (groups ?? []).map((g) => ({
      label: g.label ?? null,
      pass_rate: g.pass_rate ?? null,
      failed_tests: g.failed_tests ?? null,
      rule_count: g.rule_count ?? null,
      check_count: g.check_count ?? null,
      total_tests: g.total_tests ?? null,
    }));

  // Build the rows + muted-label set for one facet box.
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

  // Registry order for the By dimension / By severity default sort: dimensions
  // in their list order; severities by rank descending (highest-severity
  // first). Rows not in the registry sort last. This sets the DEFAULT (unsorted)
  // order of those two tables to match the admin/registry order rather than
  // failed-count — header clicks still re-sort on top of it.
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

  const dimFacet = buildFacet(filtered.by_dimension, base.by_dimension);
  const sevFacet = buildFacet(filtered.by_severity, base.by_severity);
  const ruleFacet = buildFacet(filtered.by_rule, base.by_rule);
  const colFacet = buildFacet(filtered.by_column, base.by_column);

  const filterBySearch = <T extends { label: string | null }>(
    rows: T[],
    search: string,
  ) => {
    const q = search.trim().toLowerCase();
    return q
      ? rows.filter((r) => (r.label ?? "").toLowerCase().includes(q))
      : rows;
  };

  // Multi-series pass-rate trends. pass_rate can arrive as a numeric string
  // (the overall trend serialises it as a string), so coerce before plotting.
  const toTrendSeries = (points: TrendPointOut[] | undefined) =>
    (points ?? []).map((p) => ({
      run_date: String(p.run_date ?? ""),
      series: p.series ?? undefined,
      pass_rate: toNum(p.pass_rate),
    }));
  const toTrend = (rows: EntityResultsOut["trend"]) =>
    (rows ?? []).map((tp) => ({
      run_date: String(tp.run_date ?? ""),
      pass_rate: toNum(tp.pass_rate),
    }));

  // B5/B6 count series. Series keys are the ENGLISH canonical names — the
  // chart's ordering/legend logic pins on ["Rules","Checks","Tests","Rows"]
  // (see ScoreTrendChart's count-mode ordering); do not translate them here.
  const rulesChecksTestsSeries = toCountSeries(trend?.trend_counts, [
    { key: "rule_count", label: "Rules" },
    { key: "check_count", label: "Checks" },
    { key: "test_count", label: "Tests" },
  ]);
  const failuresSeries = toCountSeries(trend?.trend_failures, [
    { key: "failed_rule_count", label: "Rules" },
    { key: "failed_check_count", label: "Checks" },
    { key: "failed_test_count", label: "Tests" },
    { key: "failed_records", label: "Rows" },
  ]);

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

  const failingRows = toFailingRecords(failedRows?.rows);

  // Download fetches the WHOLE undrilled set for the table (no facet filters,
  // high limit) — independent of the 200-capped, filtered on-screen table.
  const fetchAllFailedRows = async () => {
    const res = await getDqResultsFailedRows(tableFqn, {
      limit: EXPORT_ROW_LIMIT,
      include_drafts: draftsParam,
    });
    return toFailingRecords(res.data.rows);
  };

  /** A small per-widget spinner overlay for the over-time charts (F1). */
  const ChartFrame = ({
    loading,
    children,
  }: {
    loading: boolean;
    children: React.ReactNode;
  }) =>
    loading ? (
      <div className="flex h-[224px] items-center justify-center rounded-md border">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    ) : (
      <>{children}</>
    );

  return (
    <div className="space-y-6">
      <RunInProgressBanner show={Boolean(runInProgress)}>
        {t("resultsUi.runInProgressBanner")}
      </RunInProgressBanner>

      {/* Draft-binding context (P3.6): a never-approved binding only has
          draft runs, which the published-only default hides — one plain
          informational line pointing at the run-mode dropdown. */}
      {neverApproved && (
        <p className="text-xs text-muted-foreground" role="note">
          {t("resultsUi.draftBindingNotice")}
        </p>
      )}

      {/* A2: the run picker (plus the run-mode dropdown) overlaps the score
          box's top-right corner; it drops below the score on very small
          screens. */}
      <div className="relative">
        <div className="z-10 flex items-center gap-2 sm:absolute sm:right-2 sm:top-2 max-sm:mb-2 max-sm:justify-end">
          <RunModeSelect includeDrafts={includeDrafts} onChange={setIncludeDrafts} />
          <RunPicker
            runs={runs}
            value={filters.runId ?? null}
            onChange={(id) => setFilters((f) => ({ ...f, runId: id }))}
          />
        </div>
        <div className="sm:pr-2">
          <ScoreBox
            passRate={passRate}
            failedTests={failedTests}
            totalTests={totalTests}
            trend={scoreTrend}
            info={COUNT_INFO}
          />
        </div>
      </div>

      <CollapsibleSection title={t("resultsUi.overTimeSection")} defaultOpen>
        <div className="space-y-6">
          <ChartFrame loading={trendsLoading}>
            <ScoreTrendChart
              data={toTrend(trend?.trend)}
              title={t("resultsUi.overallDqScoreTitle")}
            />
          </ChartFrame>
          <div className="grid gap-6 md:grid-cols-2">
            <ChartFrame loading={trendsLoading}>
              <ScoreTrendChart
                data={toTrendSeries(trend?.trend_by_dimension)}
                colorMap={dimColors}
                title={t("resultsUi.scoreByDimensionTitle")}
                collapsed={!scoreBreakdownOpen}
                onToggleCollapse={() => setScoreBreakdownOpen((o) => !o)}
              />
            </ChartFrame>
            <ChartFrame loading={trendsLoading}>
              <ScoreTrendChart
                data={toTrendSeries(trend?.trend_by_severity)}
                colorMap={sevColors}
                title={t("resultsUi.scoreBySeverityTitle")}
                collapsed={!scoreBreakdownOpen}
                onToggleCollapse={() => setScoreBreakdownOpen((o) => !o)}
              />
            </ChartFrame>
          </div>
          <div className="grid gap-6 md:grid-cols-2">
            <ChartFrame loading={trendsLoading}>
              <ScoreTrendChart
                mode="count"
                countData={rulesChecksTestsSeries}
                title={t("resultsUi.appliedCountsTitle")}
                collapsed={!countChartsOpen}
                onToggleCollapse={() => setCountChartsOpen((o) => !o)}
              />
            </ChartFrame>
            <ChartFrame loading={trendsLoading}>
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
            <DimensionBreakdown
              title={t("resultsUi.byRuleTitle")}
              valueHeader={t("resultsUi.ruleHeader")}
              countMode="checks"
              rows={filterBySearch(ruleFacet.rows, ruleSearch)}
              mutedLabels={ruleFacet.mutedLabels}
              loading={breakdownRefetching}
              selected={filters.rule}
              onSelect={(label) => onRowToggle("rule", label)}
              collapsed={!ruleColOpen}
              onToggleCollapse={() => setRuleColOpen((o) => !o)}
              pageSize={8}
              headerRight={
                <FacetSearch
                  label={t("resultsUi.facetRule")}
                  value={ruleSearch}
                  onChange={setRuleSearch}
                />
              }
            />
            <DimensionBreakdown
              title={t("resultsUi.byColumnTitle")}
              valueHeader={t("resultsUi.columnHeader")}
              rows={filterBySearch(colFacet.rows, colSearch)}
              defaultSort={{ key: "pass_rate", dir: "asc" }}
              mutedLabels={colFacet.mutedLabels}
              loading={breakdownRefetching}
              selected={filters.column}
              onSelect={(label) => onRowToggle("column", label)}
              collapsed={!ruleColOpen}
              onToggleCollapse={() => setRuleColOpen((o) => !o)}
              headerRight={
                <FacetSearch
                  label={t("resultsUi.facetColumn")}
                  value={colSearch}
                  onChange={setColSearch}
                />
              }
            />
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <button
                type="button"
                onClick={() => setFailedRecordsOpen((o) => !o)}
                aria-expanded={failedRecordsOpen}
                className="group flex items-center gap-1.5 text-left"
              >
                <span className="text-xs uppercase tracking-wide text-muted-foreground">
                  {t("resultsUi.failedRecordsTitle")}
                </span>
                <ChevronDown
                  className={`h-4 w-4 shrink-0 text-muted-foreground transition-transform ${
                    failedRecordsOpen ? "" : "-rotate-90"
                  }`}
                />
              </button>
              {isLatest && !failedRows?.suppressed && (
                <DownloadFailedRecordsMenu
                  fetchRows={fetchAllFailedRows}
                  tableName={tableName}
                />
              )}
            </div>
            <CollapseRegion open={failedRecordsOpen}>
              {!isLatest ? (
                <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                  {t("resultsUi.failedRecordsLatestOnly")}
                </div>
              ) : failedRows?.suppressed ? (
                // Our failed-rows envelope carries an extra `suppressed`
                // flag (fine-grained access controls on the source table —
                // Task 7 semantics); surface the standing suppression
                // message instead of an empty table.
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
        </div>
      </CollapsibleSection>
    </div>
  );
}
