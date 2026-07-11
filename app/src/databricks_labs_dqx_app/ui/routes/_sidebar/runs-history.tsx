import { createFileRoute, useNavigate, useSearch } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import {
  useGetRunSet,
  useListRules,
  type User as UserType,
} from "@/lib/api";
import { isAxiosError } from "axios";
import {
  useListValidationRuns,
  type ValidationRunSummaryOut,
} from "@/lib/api-custom";
import { useGetDryRunResults, getDryRunStatus, type DryRunResultsOut } from "@/lib/api";
import { DryRunResults } from "@/components/DryRunResults";
import { CommentThread } from "@/components/CommentThread";
import { RunReviewStatusPanel } from "@/components/RunReviewStatusPanel";
import { useRunReviewStatuses } from "@/lib/api-custom";
import { reviewStatusBadgeClasses } from "@/routes/_sidebar/config";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  AlertCircle,
  RotateCcw,
  Loader2,
  History,
  CheckCircle2,
  XCircle,
  Clock,
  User,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  X,
} from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { ShieldCheck } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useState, useCallback, useEffect, useRef, Suspense, useMemo, type ReactNode } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { invalidateResultsAfterRunCompletion } from "@/lib/results-invalidation";
import { ErrorBoundary } from "react-error-boundary";
import { FadeIn } from "@/components/anim/FadeIn";
import { ArrowDown, ArrowUp, ArrowUpDown, CircleStop, ShieldAlert } from "lucide-react";
import { parseFqn, formatDateTime as formatDate, getUserMetadata, labelToken } from "@/lib/format-utils";
import { LabelFilter, labelsMatchFilter } from "@/components/Labels";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";

export const Route = createFileRoute("/_sidebar/runs-history")({
  // Additive only (design spec Task 10): an optional `?runSetId=` deep link
  // from the Data Products Runs tab / monitored-table Run action toast
  // pre-filters the table to that run set's member runs. Every other part
  // of this page's contract is untouched.
  validateSearch: (search: Record<string, unknown>): { runSetId?: string } => ({
    runSetId: typeof search.runSetId === "string" ? search.runSetId : undefined,
  }),
  component: RunsHistoryPage,
});

const _SQL_CHECK_PREFIX = "__sql_check__/";

function cleanFqn(fqn: string) {
  return fqn.startsWith(_SQL_CHECK_PREFIX) ? fqn.slice(_SQL_CHECK_PREFIX.length) : fqn;
}

function StatusBadge({ status }: { status: string | null }) {
  const { t } = useTranslation();
  switch (status) {
    case "SUCCESS":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          {t("runsHistory.successBadge")}
        </Badge>
      );
    case "FAILED":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          {t("runsHistory.failedBadge")}
        </Badge>
      );
    case "RUNNING":
      return (
        <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600">
          <Loader2 className="h-3 w-3 animate-spin" />
          {t("runsHistory.runningBadge")}
        </Badge>
      );
    case "CANCELED":
      return (
        <Badge variant="outline" className="gap-1 border-gray-400 text-gray-500">
          <CircleStop className="h-3 w-3" />
          {t("runsHistory.canceledBadge")}
        </Badge>
      );
    default:
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          {status ?? t("runsHistory.pendingBadge")}
        </Badge>
      );
  }
}

// Renders the review-status badge for a row. Falls back to a neutral
// placeholder when the listing endpoint hasn't yet populated the field
// (transient pre-bulk-fetch state) so the column never collapses.
function reviewStatusCell(
  run: ValidationRunSummaryOut,
  catalogueColor: Map<string, string>,
  t: TFunction,
) {
  const value = run.review_status;
  if (!value) {
    return <span className="text-xs text-muted-foreground">—</span>;
  }
  const color = catalogueColor.get(value) ?? "gray";
  return (
    <div className="flex items-center gap-1.5">
      <Badge
        variant="outline"
        className={cn(
          "text-[10px] font-normal max-w-[160px] truncate",
          reviewStatusBadgeClasses(color),
        )}
        title={value}
      >
        {value}
      </Badge>
      {run.review_status_is_default && (
        <span
          className="text-[9px] uppercase tracking-wide text-muted-foreground"
          title={t("runsHistory.reviewAutoTitle")}
        >
          {t("runsHistory.reviewAuto")}
        </span>
      )}
    </div>
  );
}

// Multi-select chip for the toolbar — mirrors the existing
// ``LabelFilter`` look so the two filters feel consistent. We don't try
// to reuse LabelFilter directly because that one is keyed on
// ``key/value`` label pairs, not flat status strings.
function ReviewStatusFilter({
  available,
  selected,
  onChange,
}: {
  available: { value: string; color: string; description: string }[];
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
}) {
  const { t } = useTranslation();
  const toggle = (value: string) => {
    const next = new Set(selected);
    if (next.has(value)) next.delete(value);
    else next.add(value);
    onChange(next);
  };

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant={selected.size > 0 ? "default" : "outline"}
          size="sm"
          className="h-9 gap-1.5 text-xs"
          title={t("runsHistory.reviewFilterTitle")}
        >
          <ShieldCheck className="h-3.5 w-3.5" />
          {t("runsHistory.reviewFilterButton")}
          {selected.size > 0 && (
            <Badge variant="secondary" className="ml-1 text-[10px] h-4 px-1">
              {selected.size}
            </Badge>
          )}
          <ChevronDown className="h-3 w-3 opacity-60" />
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-1">
        {available.length === 0 && (
          <div className="px-2 py-1.5 text-[11px] text-muted-foreground">
            {t("runsHistory.reviewNoStatuses")}
          </div>
        )}
        <div className="space-y-0.5 max-h-64 overflow-y-auto">
          {available.map((opt) => {
            const checked = selected.has(opt.value);
            return (
              <button
                key={opt.value}
                type="button"
                onClick={() => toggle(opt.value)}
                className={cn(
                  "flex w-full items-start gap-2 rounded px-2 py-1.5 text-xs hover:bg-muted",
                  checked && "bg-muted",
                )}
              >
                <span
                  className={cn(
                    "mt-0.5 inline-block h-3.5 w-3.5 shrink-0 rounded border flex items-center justify-center",
                    checked
                      ? "bg-primary border-primary"
                      : "border-muted-foreground/40",
                  )}
                >
                  {checked && (
                    <svg
                      viewBox="0 0 16 16"
                      className="h-2.5 w-2.5 text-primary-foreground"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="3"
                    >
                      <path d="M3 8l3 3 7-7" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  )}
                </span>
                <div className="flex-1 min-w-0 space-y-0.5">
                  <Badge
                    variant="outline"
                    className={cn("text-[10px] font-normal", reviewStatusBadgeClasses(opt.color))}
                  >
                    {opt.value}
                  </Badge>
                  {opt.description && (
                    <p className="text-[11px] text-muted-foreground leading-tight">
                      {opt.description}
                    </p>
                  )}
                </div>
              </button>
            );
          })}
        </div>
        {selected.size > 0 && (
          <div className="border-t mt-1 pt-1">
            <button
              type="button"
              onClick={() => onChange(new Set())}
              className="w-full text-left px-2 py-1.5 text-[11px] text-muted-foreground hover:text-foreground"
            >
              {t("runsHistory.reviewClearSelection")}
            </button>
          </div>
        )}
      </PopoverContent>
    </Popover>
  );
}

type SortDir = "asc" | "desc";

function SortableHeader<K extends string>({
  label,
  sortKey,
  active,
  direction,
  onSort,
  children,
  align = "left",
}: {
  label: string;
  sortKey: K;
  active: boolean;
  direction: SortDir;
  onSort: (key: K) => void;
  children?: ReactNode;
  align?: "left" | "right";
}) {
  const Icon = active ? (direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      className={`flex items-center gap-1 hover:text-foreground transition-colors ${align === "right" ? "ml-auto" : ""}`}
      onClick={() => onSort(sortKey)}
    >
      {children}
      {label}
      <Icon className={`h-3 w-3 ${active ? "text-foreground" : "text-muted-foreground/50"}`} />
    </button>
  );
}

function useSort<K extends string>(defaultKey: K, defaultDir: SortDir = "desc") {
  const [sort, setSort] = useState<{ key: K; dir: SortDir }>({ key: defaultKey, dir: defaultDir });
  const handleSort = useCallback((key: K) => {
    setSort((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
      return { key, dir: key === ("run_date" as string) ? "desc" : "asc" };
    });
  }, []);
  return { sortKey: sort.key, sortDir: sort.dir, handleSort };
}

function RunsHistoryPage() {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col h-full">
      <PageBreadcrumb items={[]} page={t("runsHistory.breadcrumb")} />
      <div className="flex-1 overflow-hidden mt-4">
        <QueryErrorResetBoundary>
          {({ reset }) => (
            <ErrorBoundary onReset={reset} FallbackComponent={RunHistoryError}>
              <Suspense fallback={<RunHistorySkeleton />}>
                <RunHistoryContent />
              </Suspense>
            </ErrorBoundary>
          )}
        </QueryErrorResetBoundary>
      </div>
    </div>
  );
}

type RunsSortKey = "table" | "type" | "status" | "requested_by" | "total" | "valid" | "errors" | "warnings" | "run_date";

function RunHistoryContent() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { runSetId } = useSearch({ from: "/_sidebar/runs-history" });
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  // Run-set filter (Task 10, additive) — resolves the run set's member
  // run_ids via `getRunSet` (Task 3) and narrows the table to those rows.
  // Not a Suspense query: an unresolved/failed lookup just means the filter
  // doesn't narrow anything yet rather than blowing up the whole page.
  const runSetQuery = useGetRunSet(runSetId ?? "", {
    query: { enabled: !!runSetId, select: (d) => d.data },
  });
  const runSetFilterIds = useMemo(() => {
    if (!runSetId) return null;
    const members = runSetQuery.data?.members ?? [];
    return new Set(members.map((m) => m.run_id));
  }, [runSetId, runSetQuery.data]);
  const clearRunSetFilter = () =>
    void navigate({ to: "/runs-history", search: (prev) => ({ ...prev, runSetId: undefined }) });

  const { sortKey: rSortKey, sortDir: rSortDir, handleSort: handleRunsSort } = useSort<RunsSortKey>("run_date");

  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [tableSearch, setTableSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [runTypeFilter, setRunTypeFilter] = useState("all");
  // ``failedOnly`` keeps a row if it has either errors or warnings — the
  // user-facing "Has failures" toggle that replaced the old "Has invalid"
  // filter. We still tolerate ``error_rows`` being ``null`` on pre-v5 rows
  // by falling back to ``invalid_rows``.
  const [failedOnly, setFailedOnly] = useState(false);
  const [myRunsOnly, setMyRunsOnly] = useState(false);
  const [labelFilter, setLabelFilter] = useState<Set<string>>(new Set());
  // Review-status filter — empty set means "no filter" (show all values
  // including the auto-default for unreviewed runs). We default to empty
  // so a fresh page load doesn't hide anything; users opt into the
  // filter via the toolbar.
  const [reviewStatusFilter, setReviewStatusFilter] = useState<Set<string>>(new Set());
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  const { data: runsResp, isLoading, error, refetch } = useListValidationRuns();
  const { data: rulesResp } = useListRules({ status: "approved" }, { query: {} });
  // Catalogue drives the filter chips. We use the same hook the
  // Configuration page reads from, so admin edits propagate here on
  // staleTime expiry (default 5min) or query invalidation.
  const { data: reviewStatusCatalogue } = useRunReviewStatuses();

  const rawRuns: ValidationRunSummaryOut[] = Array.isArray(runsResp?.data) ? runsResp.data : [];
  const runningRuns = rawRuns.filter((r) => r.status === "RUNNING");

  // Run-completion detection: the score / failing-records queries never
  // refetch on their own (staleTime: Infinity — see lib/results-invalidation),
  // so when a run this page saw as RUNNING settles (or drops off the list),
  // invalidate those queries for its table. This is the page that already
  // polls RUNNING runs, which makes it the run-completion observer.
  const queryClient = useQueryClient();
  const runningFqnByIdRef = useRef<Map<string, string>>(new Map());
  useEffect(() => {
    const runs: ValidationRunSummaryOut[] = Array.isArray(runsResp?.data) ? runsResp.data : [];
    const current = new Map(
      runs.filter((r) => r.status === "RUNNING").map((r) => [r.run_id, r.source_table_fqn]),
    );
    const completedFqns: string[] = [];
    for (const [runId, fqn] of runningFqnByIdRef.current) {
      if (!current.has(runId)) completedFqns.push(fqn);
    }
    runningFqnByIdRef.current = current;
    if (completedFqns.length > 0) invalidateResultsAfterRunCompletion(queryClient, completedFqns);
  }, [runsResp, queryClient]);

  useEffect(() => {
    if (runningRuns.length === 0) return;
    const id = setInterval(async () => {
      for (const run of runningRuns) {
        try {
          await getDryRunStatus(run.run_id);
        } catch { /* status check is best-effort */ }
      }
      refetch();
    }, 8000);
    return () => clearInterval(id);
  }, [runningRuns.length, refetch]); // eslint-disable-line react-hooks/exhaustive-deps

  const rulesByTable = useMemo(() => {
    const map = new Map<string, Record<string, unknown>[]>();
    const rules = Array.isArray(rulesResp?.data) ? rulesResp.data : [];
    for (const rule of rules) {
      const existing = map.get(rule.table_fqn);
      if (existing) {
        existing.push(...rule.checks);
      } else {
        map.set(rule.table_fqn, [...rule.checks]);
      }
    }
    return map;
  }, [rulesResp]);

  const allRuns: ValidationRunSummaryOut[] = useMemo(() => {
    const raw = Array.isArray(runsResp?.data) ? runsResp.data : [];
    return raw.map((run) => {
      if (run.checks && run.checks.length > 0) return run;
      const fallback = rulesByTable.get(run.source_table_fqn);
      if (fallback && fallback.length > 0) {
        return { ...run, checks: fallback };
      }
      return run;
    });
  }, [runsResp, rulesByTable]);

  const [tableFilter, setTableFilter] = useState("all");

  const { catalogs, schemasByCatalog, tablesByCatalogSchema } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    const tableMap = new Map<string, Set<string>>();
    for (const run of allRuns) {
      if (run.source_table_fqn.startsWith(_SQL_CHECK_PREFIX)) {
        catalogSet.add("Cross-table rules");
        continue;
      }
      const { catalog, schema, table } = parseFqn(run.source_table_fqn);
      if (catalog) {
        catalogSet.add(catalog);
        if (!schemaMap.has(catalog)) schemaMap.set(catalog, new Set());
        if (schema) schemaMap.get(catalog)!.add(schema);
      }
      if (catalog && schema && table) {
        const key = `${catalog}.${schema}`;
        if (!tableMap.has(key)) tableMap.set(key, new Set());
        tableMap.get(key)!.add(table);
      }
    }
    return {
      catalogs: Array.from(catalogSet).sort(),
      schemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([cat, schemas]) => [cat, Array.from(schemas).sort()]),
      ),
      tablesByCatalogSchema: Object.fromEntries(
        Array.from(tableMap.entries()).map(([key, tables]) => [key, Array.from(tables).sort()]),
      ),
    };
  }, [allRuns]);

  const runs = useMemo(() => {
    const filtered = allRuns.filter((run) => {
      if (runSetFilterIds && !runSetFilterIds.has(run.run_id)) return false;
      const isSqlCheck = run.source_table_fqn.startsWith(_SQL_CHECK_PREFIX);
      if (catalogFilter !== "all") {
        if (catalogFilter === "Cross-table rules") {
          if (!isSqlCheck) return false;
        } else {
          if (isSqlCheck) return false;
          const { catalog } = parseFqn(run.source_table_fqn);
          if (catalog !== catalogFilter) return false;
        }
      }
      if (!isSqlCheck) {
        const { schema, table } = parseFqn(run.source_table_fqn);
        if (schemaFilter !== "all" && schema !== schemaFilter) return false;
        if (tableFilter !== "all" && table !== tableFilter) return false;
        if (tableSearch && !table.toLowerCase().includes(tableSearch.toLowerCase()) && !run.source_table_fqn.toLowerCase().includes(tableSearch.toLowerCase())) return false;
      } else if (tableSearch) {
        const name = run.source_table_fqn.slice(_SQL_CHECK_PREFIX.length);
        if (!name.toLowerCase().includes(tableSearch.toLowerCase())) return false;
      }
      if (statusFilter !== "all" && run.status !== statusFilter) return false;
      if (runTypeFilter !== "all" && (run.run_type ?? "dryrun") !== runTypeFilter) return false;
      if (failedOnly) {
        const errors = run.error_rows ?? run.invalid_rows;
        const warnings = run.warning_rows;
        const hasFailures = (errors != null && errors > 0) || (warnings != null && warnings > 0);
        if (!hasFailures) return false;
      }
      if (myRunsOnly && currentUserEmail && run.requesting_user !== currentUserEmail) return false;
      if (labelFilter.size > 0) {
        // Match the label filter against any check captured on the run; an
        // empty checks list means we can't match → exclude under filter.
        const checks = run.checks ?? [];
        const matched = checks.some((c) =>
          labelsMatchFilter(getUserMetadata(c as Record<string, unknown>), labelFilter),
        );
        if (!matched) return false;
      }
      if (reviewStatusFilter.size > 0) {
        // The listing endpoint always populates ``review_status`` with the
        // effective value (catalogue default for unreviewed runs); a null
        // value only happens for the brief window between a run starting
        // and the OLTP bulk-fetch succeeding, so we exclude those under
        // filter to stay consistent with the "match by effective value"
        // contract.
        if (!run.review_status || !reviewStatusFilter.has(run.review_status)) return false;
      }
      return true;
    });

    const dir = rSortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (rSortKey) {
        case "table":
          cmp = cleanFqn(a.source_table_fqn).localeCompare(cleanFqn(b.source_table_fqn));
          break;
        case "type":
          cmp = (a.run_type ?? "dryrun").localeCompare(b.run_type ?? "dryrun");
          break;
        case "status":
          cmp = (a.status ?? "").localeCompare(b.status ?? "");
          break;
        case "requested_by":
          cmp = (a.requesting_user ?? "").localeCompare(b.requesting_user ?? "");
          break;
        case "total":
          cmp = (a.total_rows ?? 0) - (b.total_rows ?? 0);
          break;
        case "valid":
          cmp = (a.valid_rows ?? 0) - (b.valid_rows ?? 0);
          break;
        case "errors":
          cmp = (a.error_rows ?? a.invalid_rows ?? 0) - (b.error_rows ?? b.invalid_rows ?? 0);
          break;
        case "warnings":
          cmp = (a.warning_rows ?? 0) - (b.warning_rows ?? 0);
          break;
        case "run_date":
          cmp = (a.created_at ?? "").localeCompare(b.created_at ?? "");
          break;
      }
      return cmp * dir;
    });
  }, [allRuns, runSetFilterIds, catalogFilter, schemaFilter, tableFilter, tableSearch, statusFilter, runTypeFilter, failedOnly, myRunsOnly, currentUserEmail, rSortKey, rSortDir, labelFilter, reviewStatusFilter]);

  // Distinct labels seen across all runs' checks. Drives the LabelFilter
  // dropdown content for this page.
  const availableLabels = useMemo(() => {
    const seen = new Set<string>();
    const out: { key: string; value: string }[] = [];
    for (const run of allRuns) {
      for (const check of run.checks ?? []) {
        const md = getUserMetadata(check as Record<string, unknown>);
        for (const [key, value] of Object.entries(md)) {
          const tok = labelToken(key, value);
          if (!seen.has(tok)) {
            seen.add(tok);
            out.push({ key, value });
          }
        }
      }
    }
    return out;
  }, [allRuns]);

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];
  const availableTables = (catalogFilter !== "all" && schemaFilter !== "all")
    ? tablesByCatalogSchema[`${catalogFilter}.${schemaFilter}`] || []
    : [];

  // Build once per catalogue refresh so the per-row cell renderer can do a
  // single Map lookup instead of scanning the catalogue array. Orphan
  // values (in dq_run_review_status but no longer in the catalogue) fall
  // through to the default colour token.
  const reviewStatusColorMap = useMemo(() => {
    const map = new Map<string, string>();
    for (const opt of reviewStatusCatalogue?.statuses ?? []) {
      map.set(opt.value, opt.color);
    }
    return map;
  }, [reviewStatusCatalogue]);

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
    setTableFilter("all");
  };

  const handleSchemaChange = (value: string) => {
    setSchemaFilter(value);
    setTableFilter("all");
  };

  const hasActiveFilters = !!runSetId || catalogFilter !== "all" || schemaFilter !== "all" || tableFilter !== "all" || tableSearch !== "" || statusFilter !== "all" || runTypeFilter !== "all" || failedOnly || myRunsOnly || labelFilter.size > 0 || reviewStatusFilter.size > 0;

  const PAGE_SIZE = 25;
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(runs.length / PAGE_SIZE));
  const pagedRuns = useMemo(
    () => runs.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE),
    [runs, currentPage],
  );

  useEffect(() => { setCurrentPage(1); }, [catalogFilter, schemaFilter, tableFilter, tableSearch, statusFilter, runTypeFilter, failedOnly, myRunsOnly, rSortKey, rSortDir, runSetId]);

  return (
    <div className="space-y-4 h-full overflow-y-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">{t("runsHistory.title")}</h1>
          <p className="text-muted-foreground text-sm">
            {t("runsHistory.subtitle")}
          </p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()} className="gap-1.5 text-xs">
          <RotateCcw className="h-3.5 w-3.5" />
          {t("common.refresh")}
        </Button>
      </div>

      {runSetId && (
        <div className="flex items-center gap-2">
          <Badge variant="secondary" className="gap-1.5 font-normal">
            {t("runsHistory.runSetFilterChip", { runSetId: runSetId.slice(0, 8) })}
            <button
              type="button"
              onClick={clearRunSetFilter}
              aria-label={t("runsHistory.runSetFilterClearAria")}
              className="inline-flex items-center justify-center rounded-full hover:bg-muted-foreground/20"
            >
              <X className="h-3 w-3" />
            </button>
          </Badge>
        </div>
      )}

      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2 text-base">
                <History className="h-4 w-4" />
                {t("runsHistory.validationRuns")}
              </CardTitle>
              <CardDescription>
                {isLoading
                  ? t("common.loading")
                  : `${t("runsHistory.runsCount", { count: runs.length })}${
                      runs.length !== allRuns.length ? t("runsHistory.filteredFrom", { total: allRuns.length }) : ""
                    }`}
              </CardDescription>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap pt-2">
            <Select value={catalogFilter} onValueChange={handleCatalogChange}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder={t("runsHistory.allCatalogs")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("runsHistory.allCatalogs")}</SelectItem>
                {catalogs.map((cat) => (
                  <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={schemaFilter} onValueChange={handleSchemaChange} disabled={catalogFilter === "all"}>
              <SelectTrigger className="w-[160px]">
                <SelectValue placeholder={t("runsHistory.allSchemas")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("runsHistory.allSchemas")}</SelectItem>
                {availableSchemas.map((sch) => (
                  <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={tableFilter} onValueChange={setTableFilter} disabled={schemaFilter === "all"}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder={t("runsHistory.allTables")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("runsHistory.allTables")}</SelectItem>
                {availableTables.map((tbl) => (
                  <SelectItem key={tbl} value={tbl}>{tbl}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={statusFilter} onValueChange={setStatusFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder={t("runsHistory.allStatuses")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("runsHistory.allStatuses")}</SelectItem>
                <SelectItem value="SUCCESS">{t("runsHistory.successOption")}</SelectItem>
                <SelectItem value="FAILED">{t("runsHistory.failedOption")}</SelectItem>
                <SelectItem value="RUNNING">{t("runsHistory.runningOption")}</SelectItem>
              </SelectContent>
            </Select>

            <Select value={runTypeFilter} onValueChange={setRunTypeFilter}>
              <SelectTrigger className="w-[140px]">
                <SelectValue placeholder={t("runsHistory.allTypes")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("runsHistory.allTypes")}</SelectItem>
                <SelectItem value="dryrun">{t("runsHistory.manualOption")}</SelectItem>
                <SelectItem value="scheduled">{t("runsHistory.scheduledOption")}</SelectItem>
              </SelectContent>
            </Select>

            <Button
              variant={failedOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setFailedOnly((prev) => !prev)}
              title="Show only runs that have at least one error or warning"
            >
              <AlertCircle className="h-3.5 w-3.5" />
              Has failures
            </Button>

            <Button
              variant={myRunsOnly ? "default" : "outline"}
              size="sm"
              className="h-9 gap-1.5 text-xs"
              onClick={() => setMyRunsOnly((prev) => !prev)}
            >
              <User className="h-3.5 w-3.5" />
              {t("runsHistory.myRuns")}
            </Button>

            <LabelFilter
              available={availableLabels}
              selected={labelFilter}
              onChange={setLabelFilter}
            />

            <ReviewStatusFilter
              available={reviewStatusCatalogue?.statuses ?? []}
              selected={reviewStatusFilter}
              onChange={setReviewStatusFilter}
            />

            {hasActiveFilters && (
              <Button
                variant="ghost"
                size="sm"
                className="h-9 text-xs"
                onClick={() => {
                  setCatalogFilter("all");
                  setSchemaFilter("all");
                  setTableFilter("all");
                  setTableSearch("");
                  setStatusFilter("all");
                  setRunTypeFilter("all");
                  setFailedOnly(false);
                  setMyRunsOnly(false);
                  setLabelFilter(new Set());
                  setReviewStatusFilter(new Set());
                  if (runSetId) clearRunSetFilter();
                }}
              >
                {t("common.clearFilters")}
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {isLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => <Skeleton key={i} className="h-14 w-full" />)}
            </div>
          )}

          {error && (
            isAxiosError(error) && error.response?.status === 403 ? (
              <div className="flex flex-col items-center justify-center py-16 text-center">
                <ShieldAlert className="h-12 w-12 text-destructive/30 mb-3" />
                <p className="text-destructive text-sm mb-1">{t("runsHistory.permissionsTitle")}</p>
                <p className="text-muted-foreground/70 text-xs">
                  {t("runsHistory.permissionsDescription")}
                </p>
              </div>
            ) : (
              <p className="text-destructive text-sm">{t("runsHistory.loadFailed", { error: (error as Error).message })}</p>
            )
          )}

          {!isLoading && !error && runs.length > 0 && (
            <FadeIn duration={0.3}>
              <div className="border rounded-lg overflow-x-auto">
                <table className="w-full text-sm min-w-[900px]">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="w-8 p-3"></th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerTable")} sortKey="table" active={rSortKey === "table"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerType")} sortKey="type" active={rSortKey === "type"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerStatus")} sortKey="status" active={rSortKey === "status"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-left p-3 font-medium" title={t("runsHistory.reviewHeaderTitle")}>
                        {t("runsHistory.reviewHeader")}
                      </th>
                      <th className="text-left p-3 font-medium">Rules</th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerRequestedBy")} sortKey="requested_by" active={rSortKey === "requested_by"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerTotal")} sortKey="total" active={rSortKey === "total"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerValid")} sortKey="valid" active={rSortKey === "valid"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label="Errors" sortKey="errors" active={rSortKey === "errors"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-right p-3 font-medium">
                        <SortableHeader label="Warnings" sortKey="warnings" active={rSortKey === "warnings"} direction={rSortDir} onSort={handleRunsSort} align="right" />
                      </th>
                      <th className="text-left p-3 font-medium">
                        <SortableHeader label={t("runsHistory.headerRunDate")} sortKey="run_date" active={rSortKey === "run_date"} direction={rSortDir} onSort={handleRunsSort} />
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {pagedRuns.map((run) => {
                      // ``error_rows`` is the DQX observer count (post-v5); fall back
                      // to ``invalid_rows`` for runs created before the rename.
                      const errors = run.error_rows ?? run.invalid_rows;
                      const errorPct =
                        run.total_rows && errors
                          ? ((errors / run.total_rows) * 100).toFixed(1)
                          : null;
                      const isExpanded = expandedRunId === run.run_id;
                      return (
                        <RunHistoryRow
                          key={run.run_id}
                          run={run}
                          errorPct={errorPct}
                          isExpanded={isExpanded}
                          onToggle={() => setExpandedRunId(isExpanded ? null : run.run_id)}
                          reviewStatusColorMap={reviewStatusColorMap}
                        />
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {totalPages > 1 && (
                <div className="flex items-center justify-between pt-3">
                  <p className="text-xs text-muted-foreground">
                    {t("common.showing")} {(currentPage - 1) * PAGE_SIZE + 1}–{Math.min(currentPage * PAGE_SIZE, runs.length)} {t("common.of")} {runs.length}
                  </p>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 w-8 p-0"
                      disabled={currentPage <= 1}
                      onClick={() => setCurrentPage((p) => p - 1)}
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </Button>
                    {Array.from({ length: totalPages }, (_, i) => i + 1)
                      .filter((p) => p === 1 || p === totalPages || Math.abs(p - currentPage) <= 2)
                      .reduce<(number | "...")[]>((acc, p, i, arr) => {
                        if (i > 0 && p - arr[i - 1] > 1) acc.push("...");
                        acc.push(p);
                        return acc;
                      }, [])
                      .map((p, i) =>
                        p === "..." ? (
                          <span key={`ellipsis-${i}`} className="px-1 text-xs text-muted-foreground">...</span>
                        ) : (
                          <Button
                            key={p}
                            variant={p === currentPage ? "default" : "outline"}
                            size="sm"
                            className="h-8 w-8 p-0 text-xs"
                            onClick={() => setCurrentPage(p)}
                          >
                            {p}
                          </Button>
                        ),
                      )}
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 w-8 p-0"
                      disabled={currentPage >= totalPages}
                      onClick={() => setCurrentPage((p) => p + 1)}
                    >
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
              )}
            </FadeIn>
          )}

          {!isLoading && !error && runs.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16 text-center">
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-6">
                <History className="h-8 w-8 text-muted-foreground" />
              </div>
              <h3 className="text-lg font-medium text-muted-foreground">
                {hasActiveFilters ? t("runsHistory.noMatching") : t("runsHistory.noRuns")}
              </h3>
              <p className="text-muted-foreground/70 text-sm mt-1 max-w-md">
                {hasActiveFilters
                  ? t("runsHistory.adjustFiltersHint")
                  : t("runsHistory.executeRulesHint")}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function RunHistoryRow({
  run,
  errorPct,
  isExpanded,
  onToggle,
  reviewStatusColorMap,
}: {
  run: ValidationRunSummaryOut;
  errorPct: string | null;
  isExpanded: boolean;
  onToggle: () => void;
  // Catalogue-derived value→color lookup. Passed from the page so the
  // row stays a pure renderer; we accept an empty Map and fall back to
  // a neutral colour token in ``reviewStatusCell``.
  reviewStatusColorMap: Map<string, string>;
}) {
  const { t } = useTranslation();
  const { data: resultsResp, isLoading: isLoadingResults } = useGetDryRunResults(
    run.run_id,
    { query: { enabled: isExpanded && run.status === "SUCCESS" } },
  );
  const results: DryRunResultsOut | null = isExpanded && resultsResp?.data ? resultsResp.data : null;

  return (
    <>
      <tr
        className={cn(
          "border-b hover:bg-muted/30 transition-colors cursor-pointer",
          isExpanded && "bg-muted/20",
        )}
        onClick={onToggle}
      >
        <td className="p-3 w-8">
          {isExpanded ? (
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )}
        </td>
        <td className="p-3 font-mono text-xs">{cleanFqn(run.source_table_fqn)}</td>
        <td className="p-3">
          <Badge variant={(run.run_type ?? "dryrun") === "scheduled" ? "default" : "outline"} className="text-[10px]">
            {(run.run_type ?? "dryrun") === "scheduled" ? t("runsHistory.scheduled") : t("runsHistory.manual")}
          </Badge>
        </td>
        <td className="p-3">
          <div className="flex flex-col gap-0.5">
            <StatusBadge status={run.status} />
            {run.status === "CANCELED" && run.canceled_by && (
              <span className="text-[10px] text-muted-foreground" title={t("runsHistory.canceledByTitle", { user: run.canceled_by })}>
                {t("runsHistory.canceledByPrefix")}{run.canceled_by.split("@")[0]}
              </span>
            )}
          </div>
        </td>
        <td className="p-3">{reviewStatusCell(run, reviewStatusColorMap, t)}</td>
        <td className="p-3">
          {run.checks && run.checks.length > 0 ? (() => {
            const labels = run.checks.map((c: Record<string, unknown>) => {
              const check = c.check as Record<string, unknown> | undefined;
              const fn = check?.function as string | undefined;
              const args = check?.arguments as Record<string, unknown> | undefined;
              if (!fn) return c.name as string ?? "check";
              const col = args?.column as string | undefined;
              return col ? `${fn}(${col})` : fn;
            });
            const MAX_SHOWN = 3;
            const shown = labels.slice(0, MAX_SHOWN);
            const remaining = labels.length - MAX_SHOWN;
            return (
              <div className="flex flex-col gap-0.5 max-w-[240px]" title={labels.join("\n")}>
                {shown.map((label, i) => (
                  <span key={i} className="font-mono text-[11px] text-muted-foreground truncate">{label}</span>
                ))}
                {remaining > 0 && (
                  <span className="text-[10px] text-muted-foreground/70">{t("runsHistory.moreSuffix", { count: remaining })}</span>
                )}
              </div>
            );
          })() : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground truncate max-w-[180px]" title={run.requesting_user ?? ""}>
          {run.requesting_user ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">{run.total_rows?.toLocaleString() ?? "—"}</td>
        <td className="p-3 text-right tabular-nums text-green-600">
          {run.valid_rows?.toLocaleString() ?? "—"}
        </td>
        <td className="p-3 text-right tabular-nums">
          {/* ``error_rows`` is the authoritative DQX observer count
              (``error_row_count``). Pre-v5 runs only have ``invalid_rows``
              — fall back to that so historical data still renders. */}
          {(() => {
            const errors = run.error_rows ?? run.invalid_rows;
            if (errors == null) return "—";
            return (
              <span className={errors > 0 ? "text-red-600 font-medium" : ""}>
                {errors.toLocaleString()}
                {errorPct && errors > 0 && (
                  <span className="text-muted-foreground font-normal ml-1">({errorPct}%)</span>
                )}
              </span>
            );
          })()}
        </td>
        <td className="p-3 text-right tabular-nums">
          {run.warning_rows != null ? (
            <span className={run.warning_rows > 0 ? "text-amber-600 font-medium" : ""}>
              {run.warning_rows.toLocaleString()}
            </span>
          ) : (
            // Em dash distinguishes pre-v3 history rows from "0 warnings".
            "—"
          )}
        </td>
        <td className="p-3 text-xs text-muted-foreground whitespace-nowrap" title={run.created_at ?? ""}>
          {formatDate(run.created_at)}
        </td>
      </tr>
      {isExpanded && (
        <tr>
          <td colSpan={12} className="p-0">
            <div className="border-t bg-muted/10 p-4 space-y-4">
              {run.status === "FAILED" && run.error_message && (
                <div className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/30 p-3">
                  <AlertCircle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                  <div className="text-sm">
                    <p className="font-medium text-red-700 dark:text-red-400">{t("runsHistory.runFailed")}</p>
                    <p className="text-red-600 dark:text-red-300 mt-0.5 whitespace-pre-wrap break-words font-mono text-xs">{run.error_message}</p>
                  </div>
                </div>
              )}
              {run.status !== "SUCCESS" && !run.error_message && run.status !== "CANCELED" && (
                <div className="text-sm text-muted-foreground">
                  {t("runsHistory.completedOnly")}
                </div>
              )}
              {run.status === "CANCELED" && !run.error_message && (
                <div className="text-sm text-muted-foreground">
                  {t("runsHistory.wasCanceled")}
                </div>
              )}
              {run.status === "SUCCESS" && isLoadingResults && (
                <div className="flex items-center gap-2 text-muted-foreground text-sm py-4">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  {t("runsHistory.loadingResults")}
                </div>
              )}
              {results && <DryRunResults result={results} />}
              {/* Review status sits above the comments — the dropdown is a
                  structured action that maps to filters on this page, so it
                  belongs above the free-text discussion that explains the
                  why. */}
              <div className="rounded-md border bg-background/80 p-3">
                <RunReviewStatusPanel runId={run.run_id} />
              </div>
              <CommentThread entityType="run" entityId={run.run_id} />
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function RunHistorySkeleton() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton className="h-7 w-32" />
          <Skeleton className="h-4 w-64" />
        </div>
        <Skeleton className="h-9 w-28" />
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

function RunHistoryError({ resetErrorBoundary }: { resetErrorBoundary: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <AlertCircle className="h-12 w-12 text-destructive/30 mb-3" />
      <p className="text-muted-foreground text-sm mb-1">{t("runsHistory.skeletonFailedLoad")}</p>
      <p className="text-muted-foreground/70 text-xs mb-3">
        {t("runsHistory.skeletonFailedHint")}
      </p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}
