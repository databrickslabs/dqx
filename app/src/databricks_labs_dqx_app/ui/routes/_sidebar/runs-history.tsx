import { createFileRoute, Link, useNavigate, useSearch } from "@tanstack/react-router";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import {
  useGetRunSet,
  useGetDataProduct,
  useListMonitoredTables,
  type User as UserType,
} from "@/lib/api";
import { isAxiosError } from "axios";
import {
  useListValidationRuns,
  useWorkspaceHost,
  type ValidationRunSummaryOut,
} from "@/lib/api-custom";
import { useListProfileRuns, type ProfileRunSummaryOut } from "@/lib/api";
import { CommentThread } from "@/components/CommentThread";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  AlertCircle,
  Loader2,
  History,
  CheckCircle2,
  XCircle,
  Clock,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ExternalLink,
  RefreshCw,
  X,
} from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SearchableSelect } from "@/components/data-table/SearchableSelect";
import { FILTER_TRIGGER_CLASS } from "@/components/data-table/filter-bar";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useState, useCallback, useEffect, useRef, Suspense, useMemo } from "react";
import { QueryErrorResetBoundary, useQueryClient } from "@tanstack/react-query";
import { invalidateResultsAfterRunCompletion } from "@/lib/results-invalidation";
import { ErrorBoundary } from "react-error-boundary";
import { FadeIn } from "@/components/anim/FadeIn";
import { ArrowDown, ArrowUp, ArrowUpDown, CircleStop, ShieldAlert, RotateCcw } from "lucide-react";
import { parseFqn, formatDateTime as formatDate, parseServerTimestampMs } from "@/lib/format-utils";
import { useTranslation } from "react-i18next";

export const Route = createFileRoute("/_sidebar/runs-history")({
  // Deep-link / surface filters. All optional and additive:
  //   ?runSetId=  — narrow to one run set's member runs (Data Products Runs tab)
  //   ?tableFqn=  — narrow to a single monitored table's runs (MT detail 3-dot)
  //   ?productId= — narrow to a table space's member tables (TS detail 3-dot)
  validateSearch: (
    search: Record<string, unknown>,
  ): { runSetId?: string; tableFqn?: string; productId?: string } => ({
    runSetId: typeof search.runSetId === "string" ? search.runSetId : undefined,
    tableFqn: typeof search.tableFqn === "string" ? search.tableFqn : undefined,
    productId: typeof search.productId === "string" ? search.productId : undefined,
  }),
  component: RunsHistoryPage,
});

const _SQL_CHECK_PREFIX = "__sql_check__/";

function cleanFqn(fqn: string) {
  return fqn.startsWith(_SQL_CHECK_PREFIX) ? fqn.slice(_SQL_CHECK_PREFIX.length) : fqn;
}

// ---------------------------------------------------------------------------
// Unified run model — validation (dry-run / scheduled) and profiling runs are
// merged into one list so Runs History shows both. ``kind`` is the run-type
// discriminator; the other fields are the common projection each source maps
// into.
// ---------------------------------------------------------------------------

type RunKind = "validation" | "profiling";

interface HistoryRun {
  kind: RunKind;
  run_id: string;
  source_table_fqn: string;
  status: string | null;
  requesting_user: string | null;
  created_at: string | null;
  updated_at: string | null;
  /** manual/scheduled signal. Validation: 'dryrun' | 'scheduled' | 'preview'.
   *  Profiling: 'manual' | 'scheduled' (derived server-side). Any non-'scheduled'
   *  value renders as "Manual". */
  run_type: string | null;
  /** validation only: failure detail shown in the expansion. */
  error_message: string | null;
  /** Reported wall-clock duration in seconds. Profiling carries it as a real
   *  column; validation derives it server-side from the placeholder→terminal
   *  span (both match the linked Databricks job). Null while RUNNING or when
   *  the true start can't be recovered (old runs). */
  duration_seconds: number | null;
  job_run_id: number | null;
}

function validationToHistory(run: ValidationRunSummaryOut): HistoryRun {
  return {
    kind: "validation",
    run_id: run.run_id,
    source_table_fqn: run.source_table_fqn,
    status: run.status,
    requesting_user: run.requesting_user,
    created_at: run.created_at,
    updated_at: run.updated_at,
    run_type: run.run_type ?? "dryrun",
    error_message: run.error_message,
    duration_seconds: run.duration_seconds ?? null,
    job_run_id: run.job_run_id ?? null,
  };
}

function profileToHistory(run: ProfileRunSummaryOut): HistoryRun {
  return {
    kind: "profiling",
    run_id: run.run_id,
    source_table_fqn: run.source_table_fqn,
    status: run.status ?? null,
    requesting_user: run.requesting_user ?? null,
    created_at: run.created_at ?? null,
    updated_at: run.updated_at ?? null,
    run_type: run.run_type ?? null,
    error_message: null,
    duration_seconds: run.duration_seconds ?? null,
    job_run_id: run.job_run_id ?? null,
  };
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

/** Elapsed wall-clock time for a run, formatted compactly. */
function formatDuration(ms: number): string {
  if (ms < 0) return "0s";
  const secs = ms / 1000;
  if (secs < 60) return `${secs.toFixed(0)}s`;
  const mins = Math.floor(secs / 60);
  const rem = Math.round(secs % 60);
  return `${mins}m ${rem}s`;
}

/** The value shown in the live "Time" column. RUNNING rows tick from
 *  ``created_at`` (driven by the 1s ``now`` clock); settled runs show the
 *  backend-reported wall-clock ``duration_seconds`` — both profiling and
 *  validation now carry it (validation derives it server-side from the
 *  RUNNING-placeholder → terminal-row span; see JobService.list_dryrun_rows),
 *  so the value matches the linked Databricks job (B2-126).
 *
 *  Timestamps are parsed via ``parseServerTimestampMs`` so the zone-less
 *  ``CAST(ts AS STRING)`` values from the analytical tables are read as UTC
 *  (matching the warehouse clock) rather than browser-local.
 *
 *  When ``duration_seconds`` is absent we do NOT fall back to
 *  ``created_at`` → ``updated_at`` for *validation* runs: the task runner
 *  back-dates the terminal row's ``created_at`` to exclude cluster startup, so
 *  that span understates the real runtime (the original bug). Instead we prefer
 *  ``frozenElapsedMs`` (the last live elapsed, captured only if we watched the
 *  run settle this session — see the ``lastElapsedRef`` note in
 *  ``RunHistoryContent``) and otherwise blank the cell. Profiling keeps the
 *  ``created_at`` → ``updated_at`` fallback for legacy rows without a duration
 *  column. */
function runElapsed(run: HistoryRun, now: number, frozenElapsedMs?: number): string {
  const startedMs = parseServerTimestampMs(run.created_at);
  if (run.status === "RUNNING") {
    return Number.isNaN(startedMs) ? "—" : formatDuration(now - startedMs);
  }
  if (run.duration_seconds != null) {
    return formatDuration(run.duration_seconds * 1000);
  }
  if (run.kind === "profiling") {
    const endedMs = parseServerTimestampMs(run.updated_at);
    if (!Number.isNaN(startedMs) && !Number.isNaN(endedMs) && endedMs >= startedMs) {
      return formatDuration(endedMs - startedMs);
    }
  }
  if (frozenElapsedMs != null && frozenElapsedMs >= 0) {
    return formatDuration(frozenElapsedMs);
  }
  return "—";
}

type SortDir = "asc" | "desc";
type RunsSortKey = "table" | "type" | "status" | "run_by" | "run_date";

function SortableHeader({
  label,
  sortKey,
  active,
  direction,
  onSort,
}: {
  label: string;
  sortKey: RunsSortKey;
  active: boolean;
  direction: SortDir;
  onSort: (key: RunsSortKey) => void;
}) {
  const Icon = active ? (direction === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      className="flex items-center gap-1 hover:text-foreground transition-colors"
      onClick={() => onSort(sortKey)}
    >
      {label}
      <Icon className={`h-3 w-3 ${active ? "text-foreground" : "text-muted-foreground/50"}`} />
    </button>
  );
}

function useSort(defaultKey: RunsSortKey, defaultDir: SortDir = "desc") {
  const [sort, setSort] = useState<{ key: RunsSortKey; dir: SortDir }>({ key: defaultKey, dir: defaultDir });
  const handleSort = useCallback((key: RunsSortKey) => {
    setSort((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === "asc" ? "desc" : "asc" };
      return { key, dir: key === "run_date" ? "desc" : "asc" };
    });
  }, []);
  return { sortKey: sort.key, sortDir: sort.dir, handleSort };
}

function RunsHistoryPage() {
  const { t } = useTranslation();
  return (
    <FadeIn>
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
    </FadeIn>
  );
}

function RunHistoryContent() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { runSetId, tableFqn, productId } = useSearch({ from: "/_sidebar/runs-history" });
  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";

  // Deep link into the workspace: {host}/jobs/{job_id}/runs/{job_run_id}.
  // Both host and job_id are workspace-global; the per-row job_run_id
  // completes the URL. Cached hard (host is fixed for the app's lifetime).
  const { data: workspace } = useWorkspaceHost();
  const runUrlBase = useMemo(() => {
    const host = workspace?.workspace_host ?? "";
    const jobId = workspace?.job_id ?? "";
    return host && jobId ? `${host}/jobs/${jobId}/runs` : null;
  }, [workspace]);

  // Map each run's source table FQN to its monitored-table binding id so the
  // Table cell can deep-link to the table's detail page. Not every run has a
  // monitored table (ad-hoc dry-run previews, cross-table SQL checks), so this
  // is a best-effort lookup — an absent entry renders as plain text. Not a
  // Suspense query: a permission/load failure just means no links.
  const { data: monitoredTablesResp } = useListMonitoredTables({});
  const bindingIdByFqn = useMemo(() => {
    const map = new Map<string, string>();
    for (const summary of monitoredTablesResp?.data ?? []) {
      if (summary.table.table_fqn) map.set(summary.table.table_fqn, summary.table.binding_id);
    }
    return map;
  }, [monitoredTablesResp]);

  // Run-set filter (Data Products Runs tab) — resolves the set's member
  // run_ids via getRunSet and narrows the table to those rows. Not a Suspense
  // query: an unresolved lookup just means the filter doesn't narrow.
  const runSetQuery = useGetRunSet(runSetId ?? "", {
    query: { enabled: !!runSetId, select: (d) => d.data },
  });
  const runSetFilterIds = useMemo(() => {
    if (!runSetId) return null;
    const members = runSetQuery.data?.members ?? [];
    return new Set(members.map((m) => m.run_id));
  }, [runSetId, runSetQuery.data]);

  // Table-space filter — resolves the product's member table FQNs so runs
  // against any of its tables are shown (the runs table has no product_id).
  const productQuery = useGetDataProduct(productId ?? "", {
    query: { enabled: !!productId, select: (d) => d.data },
  });
  const productTableFqns = useMemo(() => {
    if (!productId) return null;
    const members = productQuery.data?.members ?? [];
    return new Set(members.map((m) => m.table_fqn));
  }, [productId, productQuery.data]);

  const clearSurfaceFilter = () =>
    void navigate({
      to: "/runs-history",
      search: (prev) => ({
        ...prev,
        runSetId: undefined,
        tableFqn: undefined,
        productId: undefined,
      }),
    });

  const { sortKey: rSortKey, sortDir: rSortDir, handleSort: handleRunsSort } = useSort("run_date");

  const [catalogFilter, setCatalogFilter] = useState("all");
  const [schemaFilter, setSchemaFilter] = useState("all");
  const [tableFilter, setTableFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [kindFilter, setKindFilter] = useState("all");
  const [runByFilter, setRunByFilter] = useState("all");
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  const {
    data: validationResp,
    isLoading: validationLoading,
    isFetching: validationFetching,
    error: validationError,
    refetch: refetchValidation,
  } = useListValidationRuns({
    query: {
      refetchInterval: (query) => {
        const rows = (query.state.data as { data?: ValidationRunSummaryOut[] } | undefined)?.data;
        return Array.isArray(rows) && rows.some((r) => r.status === "RUNNING") ? 5000 : false;
      },
      refetchIntervalInBackground: false,
    },
  });
  const {
    data: profileResp,
    isLoading: profileLoading,
    isFetching: profileFetching,
    refetch: refetchProfile,
  } = useListProfileRuns(undefined, {
    query: {
      refetchInterval: (query) => {
        const rows = (query.state.data as { data?: ProfileRunSummaryOut[] } | undefined)?.data;
        return Array.isArray(rows) && rows.some((r) => r.status === "RUNNING") ? 5000 : false;
      },
      refetchIntervalInBackground: false,
    },
  });

  const isLoading = validationLoading || profileLoading;
  const error = validationError;

  const allRuns: HistoryRun[] = useMemo(() => {
    const validation = Array.isArray(validationResp?.data)
      ? validationResp.data.map(validationToHistory)
      : [];
    const profiling = Array.isArray(profileResp?.data)
      ? profileResp.data.map(profileToHistory)
      : [];
    return [...validation, ...profiling];
  }, [validationResp, profileResp]);

  const runningRuns = useMemo(() => allRuns.filter((r) => r.status === "RUNNING"), [allRuns]);
  const hasRunning = runningRuns.length > 0;

  // 1s clock — drives the live "Time" column, only while something is running.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!hasRunning) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [hasRunning]);

  // Last live elapsed observed for each RUNNING run, keyed by run. Terminal
  // validation runs carry no derivable duration (see runElapsed), so when a run
  // we watched ticking settles we freeze its final elapsed here rather than
  // blanking the "Time" cell. Only helps runs whose RUNNING→settled transition
  // we witnessed; rows already terminal on load stay "—" until the backend
  // persists a real duration.
  const lastElapsedRef = useRef<Map<string, number>>(new Map());
  useEffect(() => {
    for (const run of allRuns) {
      if (run.status !== "RUNNING") continue;
      const startedMs = parseServerTimestampMs(run.created_at);
      if (!Number.isNaN(startedMs)) {
        lastElapsedRef.current.set(`${run.kind}:${run.run_id}`, now - startedMs);
      }
    }
  }, [now, allRuns]);

  // Run-completion detection: the score / failing-records queries never
  // refetch on their own (staleTime: Infinity), so when a validation run this
  // page saw as RUNNING settles (or drops off the list), invalidate them for
  // its table. This is the page that already polls RUNNING runs.
  const queryClient = useQueryClient();
  const runningFqnByIdRef = useRef<Map<string, string>>(new Map());
  useEffect(() => {
    const current = new Map(
      allRuns
        .filter((r) => r.kind === "validation" && r.status === "RUNNING")
        .map((r) => [r.run_id, r.source_table_fqn]),
    );
    const completedFqns: string[] = [];
    for (const [runId, fqn] of runningFqnByIdRef.current) {
      if (!current.has(runId)) completedFqns.push(fqn);
    }
    runningFqnByIdRef.current = current;
    if (completedFqns.length > 0) invalidateResultsAfterRunCompletion(queryClient, completedFqns);
  }, [allRuns, queryClient]);

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

  // Distinct principals across the current run set, for the "Run by" dropdown.
  // "Me" is pinned first (mapping to the current user's email); every other
  // distinct requester follows, alphabetically.
  const runByOptions = useMemo(() => {
    const seen = new Set<string>();
    const others: string[] = [];
    for (const run of allRuns) {
      const u = run.requesting_user;
      if (!u || u === currentUserEmail) continue;
      if (!seen.has(u)) {
        seen.add(u);
        others.push(u);
      }
    }
    others.sort((a, b) => a.localeCompare(b));
    const opts: { value: string; label: string }[] = [];
    if (currentUserEmail) opts.push({ value: currentUserEmail, label: t("runsHistory.runByMe") });
    for (const u of others) opts.push({ value: u, label: u });
    return opts;
  }, [allRuns, currentUserEmail, t]);

  const runs = useMemo(() => {
    const filtered = allRuns.filter((run) => {
      if (runSetFilterIds && !runSetFilterIds.has(run.run_id)) return false;
      if (tableFqn && run.source_table_fqn !== tableFqn) return false;
      if (productTableFqns && !productTableFqns.has(run.source_table_fqn)) return false;
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
      }
      if (statusFilter !== "all" && run.status !== statusFilter) return false;
      if (kindFilter !== "all" && run.kind !== kindFilter) return false;
      if (runByFilter !== "all" && run.requesting_user !== runByFilter) return false;
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
          cmp = a.kind.localeCompare(b.kind);
          break;
        case "status":
          cmp = (a.status ?? "").localeCompare(b.status ?? "");
          break;
        case "run_by":
          cmp = (a.requesting_user ?? "").localeCompare(b.requesting_user ?? "");
          break;
        case "run_date":
          cmp = (a.created_at ?? "").localeCompare(b.created_at ?? "");
          break;
      }
      return cmp * dir;
    });
  }, [
    allRuns,
    runSetFilterIds,
    tableFqn,
    productTableFqns,
    catalogFilter,
    schemaFilter,
    tableFilter,
    statusFilter,
    kindFilter,
    runByFilter,
    rSortKey,
    rSortDir,
  ]);

  const availableSchemas = catalogFilter !== "all" ? schemasByCatalog[catalogFilter] || [] : [];
  const availableTables =
    catalogFilter !== "all" && schemaFilter !== "all"
      ? tablesByCatalogSchema[`${catalogFilter}.${schemaFilter}`] || []
      : [];

  const surfaceFilterActive = !!runSetId || !!tableFqn || !!productId;
  const hasActiveFilters =
    surfaceFilterActive ||
    catalogFilter !== "all" ||
    schemaFilter !== "all" ||
    tableFilter !== "all" ||
    statusFilter !== "all" ||
    kindFilter !== "all" ||
    runByFilter !== "all";

  const handleCatalogChange = (value: string) => {
    setCatalogFilter(value);
    setSchemaFilter("all");
    setTableFilter("all");
  };
  const handleSchemaChange = (value: string) => {
    setSchemaFilter(value);
    setTableFilter("all");
  };

  const PAGE_SIZE = 25;
  const [currentPage, setCurrentPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(runs.length / PAGE_SIZE));
  const pagedRuns = useMemo(
    () => runs.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE),
    [runs, currentPage],
  );
  useEffect(() => {
    setCurrentPage(1);
  }, [catalogFilter, schemaFilter, tableFilter, statusFilter, kindFilter, runByFilter, rSortKey, rSortDir, runSetId, tableFqn, productId]);

  const surfaceChip = runSetId
    ? t("runsHistory.runSetFilterChip", { runSetId: runSetId.slice(0, 8) })
    : tableFqn
      ? t("runsHistory.tableFilterChip", { table: cleanFqn(tableFqn) })
      : productId
        ? t("runsHistory.productFilterChip")
        : null;

  return (
    <div className="space-y-6 h-full overflow-y-auto">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{t("runsHistory.title")}</h1>
          <p className="text-sm text-muted-foreground mt-1">{t("runsHistory.subtitle")}</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => { void refetchValidation(); void refetchProfile(); }}
          disabled={validationFetching || profileFetching}
          aria-label={t("runsHistory.refresh")}
        >
          <RefreshCw className={cn("h-4 w-4 mr-2", (validationFetching || profileFetching) && "animate-spin")} />
          {t("runsHistory.refresh")}
        </Button>
      </div>

      {surfaceChip && (
        <div className="flex items-center gap-2">
          <Badge variant="secondary" className="gap-1.5 font-normal">
            {surfaceChip}
            <button
              type="button"
              onClick={clearSurfaceFilter}
              aria-label={t("runsHistory.surfaceFilterClearAria")}
              className="inline-flex items-center justify-center rounded-full hover:bg-muted-foreground/20"
            >
              <X className="h-3 w-3" />
            </button>
          </Badge>
        </div>
      )}

      <div className="flex flex-wrap items-center gap-2">
        <SearchableSelect
          value={catalogFilter}
          onChange={handleCatalogChange}
          options={catalogs.map((c) => ({ value: c, label: c }))}
          allLabel={t("runsHistory.allCatalogs")}
          searchPlaceholder={t("runsHistory.catalogSearchPlaceholder")}
          emptyText={t("runsHistory.catalogSearchEmpty")}
          ariaLabel={t("runsHistory.allCatalogs")}
        />

        <Select value={schemaFilter} onValueChange={handleSchemaChange} disabled={catalogFilter === "all"}>
          <SelectTrigger className={FILTER_TRIGGER_CLASS}>
            <SelectValue placeholder={t("runsHistory.allSchemas")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t("runsHistory.allSchemas")}</SelectItem>
            {availableSchemas.map((sch) => (
              <SelectItem key={sch} value={sch}>
                {sch}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <SearchableSelect
          value={tableFilter}
          onChange={setTableFilter}
          options={availableTables.map((tbl) => ({ value: tbl, label: tbl }))}
          allLabel={t("runsHistory.allTables")}
          searchPlaceholder={t("runsHistory.tableSearchPlaceholder")}
          emptyText={t("runsHistory.tableSearchEmpty")}
          ariaLabel={t("runsHistory.allTables")}
          disabled={schemaFilter === "all"}
        />

        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className={FILTER_TRIGGER_CLASS}>
            <SelectValue placeholder={t("runsHistory.allStatuses")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t("runsHistory.allStatuses")}</SelectItem>
            <SelectItem value="FAILED">{t("runsHistory.failedOption")}</SelectItem>
            <SelectItem value="RUNNING">{t("runsHistory.runningOption")}</SelectItem>
            <SelectItem value="SUCCESS">{t("runsHistory.successOption")}</SelectItem>
          </SelectContent>
        </Select>

        <Select value={kindFilter} onValueChange={setKindFilter}>
          <SelectTrigger className={FILTER_TRIGGER_CLASS}>
            <SelectValue placeholder={t("runsHistory.allTypes")} />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">{t("runsHistory.allTypes")}</SelectItem>
            <SelectItem value="validation">{t("runsHistory.typeValidation")}</SelectItem>
            <SelectItem value="profiling">{t("runsHistory.typeProfiling")}</SelectItem>
          </SelectContent>
        </Select>

        <SearchableSelect
          value={runByFilter}
          onChange={setRunByFilter}
          options={runByOptions}
          allLabel={t("runsHistory.runByAll")}
          searchPlaceholder={t("runsHistory.runBySearchPlaceholder")}
          emptyText={t("runsHistory.runByEmpty")}
          ariaLabel={t("runsHistory.runByAria")}
        />

        {hasActiveFilters && (
          <Button
            variant="ghost"
            size="sm"
            className="h-8 text-xs"
            onClick={() => {
              setCatalogFilter("all");
              setSchemaFilter("all");
              setTableFilter("all");
              setStatusFilter("all");
              setKindFilter("all");
              setRunByFilter("all");
              if (surfaceFilterActive) clearSurfaceFilter();
            }}
          >
            {t("common.clearFilters")}
          </Button>
        )}
      </div>

      <p className="text-xs text-muted-foreground">
        {isLoading
          ? t("common.loading")
          : `${t("runsHistory.runsCount", { count: runs.length })}${
              runs.length !== allRuns.length ? t("runsHistory.filteredFrom", { total: allRuns.length }) : ""
            }`}
      </p>

      {isLoading && (
        <div className="space-y-2">
          {[1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </div>
      )}

      {error &&
        (isAxiosError(error) && error.response?.status === 403 ? (
          <div className="flex flex-col items-center justify-center py-16 text-center">
            <ShieldAlert className="h-12 w-12 text-destructive/30 mb-3" />
            <p className="text-destructive text-sm mb-1">{t("runsHistory.permissionsTitle")}</p>
            <p className="text-muted-foreground/70 text-xs">{t("runsHistory.permissionsDescription")}</p>
          </div>
        ) : (
          <p className="text-destructive text-sm">
            {t("runsHistory.loadFailed", { error: (error as Error).message })}
          </p>
        ))}

      {!isLoading && !error && runs.length > 0 && (
        <div className="space-y-4">
          <div className="overflow-x-auto rounded-md border">
            <Table className="min-w-[720px]">
              <TableHeader>
                <TableRow className="bg-muted/50 hover:bg-muted/50">
                  <TableHead className="w-8" />
                  <TableHead className="text-xs font-medium">
                    <SortableHeader label={t("runsHistory.headerTable")} sortKey="table" active={rSortKey === "table"} direction={rSortDir} onSort={handleRunsSort} />
                  </TableHead>
                  <TableHead className="text-xs font-medium">
                    <SortableHeader label={t("runsHistory.headerType")} sortKey="type" active={rSortKey === "type"} direction={rSortDir} onSort={handleRunsSort} />
                  </TableHead>
                  <TableHead className="text-xs font-medium">
                    <SortableHeader label={t("runsHistory.headerStatus")} sortKey="status" active={rSortKey === "status"} direction={rSortDir} onSort={handleRunsSort} />
                  </TableHead>
                  <TableHead className="text-xs font-medium">
                    <SortableHeader label={t("runsHistory.headerRunBy")} sortKey="run_by" active={rSortKey === "run_by"} direction={rSortDir} onSort={handleRunsSort} />
                  </TableHead>
                  <TableHead className="text-xs font-medium text-right">{t("runsHistory.headerTime")}</TableHead>
                  <TableHead className="text-xs font-medium">
                    <SortableHeader label={t("runsHistory.headerRunDate")} sortKey="run_date" active={rSortKey === "run_date"} direction={rSortDir} onSort={handleRunsSort} />
                  </TableHead>
                  <TableHead className="text-xs font-medium text-right w-16">{t("runsHistory.headerRun")}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pagedRuns.map((run) => (
                  <RunHistoryRow
                    key={`${run.kind}:${run.run_id}`}
                    run={run}
                    now={now}
                    runUrlBase={runUrlBase}
                    monitoredTableId={bindingIdByFqn.get(run.source_table_fqn) ?? null}
                    frozenElapsedMs={lastElapsedRef.current.get(`${run.kind}:${run.run_id}`)}
                    isExpanded={expandedRunId === `${run.kind}:${run.run_id}`}
                    onToggle={() =>
                      setExpandedRunId((prev) =>
                        prev === `${run.kind}:${run.run_id}` ? null : `${run.kind}:${run.run_id}`,
                      )
                    }
                  />
                ))}
              </TableBody>
            </Table>
          </div>

          {totalPages > 1 && (
            <div className="flex items-center justify-between">
              <p className="text-xs text-muted-foreground">
                {t("common.showing")} {(currentPage - 1) * PAGE_SIZE + 1}–
                {Math.min(currentPage * PAGE_SIZE, runs.length)} {t("common.of")} {runs.length}
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
                      <span key={`ellipsis-${i}`} className="px-1 text-xs text-muted-foreground">
                        ...
                      </span>
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
        </div>
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
            {hasActiveFilters ? t("runsHistory.adjustFiltersHint") : t("runsHistory.executeRulesHint")}
          </p>
        </div>
      )}
    </div>
  );
}

function RunHistoryRow({
  run,
  now,
  runUrlBase,
  monitoredTableId,
  frozenElapsedMs,
  isExpanded,
  onToggle,
}: {
  run: HistoryRun;
  now: number;
  runUrlBase: string | null;
  monitoredTableId: string | null;
  frozenElapsedMs?: number;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  // Only FAILED runs are expandable — success/running rows carry no extra
  // detail on this page (results live in the Results UI; review status moved
  // there in Stream D).
  const expandable = run.status === "FAILED";
  const runUrl = runUrlBase && run.job_run_id != null ? `${runUrlBase}/${run.job_run_id}` : null;

  return (
    <>
      <TableRow
        className={cn(
          expandable && "cursor-pointer",
          isExpanded && "bg-muted/20",
        )}
        onClick={expandable ? onToggle : undefined}
      >
        <TableCell className="w-8">
          {expandable ? (
            isExpanded ? (
              <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
            )
          ) : null}
        </TableCell>
        <TableCell className="font-mono text-xs">
          {monitoredTableId ? (
            <Link
              to="/monitored-tables/$bindingId"
              params={{ bindingId: monitoredTableId }}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="text-foreground hover:underline underline-offset-2"
              title={t("runsHistory.openMonitoredTable")}
              aria-label={t("runsHistory.openMonitoredTable")}
            >
              {cleanFqn(run.source_table_fqn)}
            </Link>
          ) : (
            cleanFqn(run.source_table_fqn)
          )}
        </TableCell>
        <TableCell>
          <div className="flex items-center gap-1.5">
            <Badge variant={run.kind === "profiling" ? "secondary" : "outline"} className="text-[10px]">
              {run.kind === "profiling" ? t("runsHistory.typeProfiling") : t("runsHistory.typeValidation")}
            </Badge>
            <span className="text-[10px] text-muted-foreground">
              {run.run_type === "scheduled" ? t("runsHistory.scheduled") : t("runsHistory.manual")}
            </span>
          </div>
        </TableCell>
        <TableCell>
          <StatusBadge status={run.status} />
        </TableCell>
        <TableCell className="text-xs text-muted-foreground truncate max-w-[180px]" title={run.requesting_user ?? ""}>
          {run.requesting_user ?? "—"}
        </TableCell>
        <TableCell className="text-right font-mono text-xs tabular-nums text-muted-foreground">
          {runElapsed(run, now, frozenElapsedMs)}
        </TableCell>
        <TableCell className="text-xs text-muted-foreground whitespace-nowrap" title={run.created_at ?? ""}>
          {formatDate(run.created_at)}
        </TableCell>
        <TableCell className="text-right">
          {runUrl ? (
            <a
              href={runUrl}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="inline-flex items-center justify-center text-muted-foreground hover:text-foreground"
              title={t("runsHistory.openInDatabricks")}
              aria-label={t("runsHistory.openInDatabricks")}
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          ) : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </TableCell>
      </TableRow>
      {expandable && isExpanded && (
        <TableRow className="hover:bg-transparent">
          <TableCell colSpan={8} className="p-0">
            <div className="border-t bg-muted/10 p-4 space-y-4">
              {run.error_message ? (
                <div className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/30 p-3">
                  <AlertCircle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                  <div className="text-sm">
                    <p className="font-medium text-red-700 dark:text-red-400">{t("runsHistory.runFailed")}</p>
                    <p className="text-red-600 dark:text-red-300 mt-0.5 whitespace-pre-wrap break-words font-mono text-xs">
                      {run.error_message}
                    </p>
                  </div>
                </div>
              ) : (
                <div className="flex items-start gap-2 rounded-md border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/30 p-3">
                  <AlertCircle className="h-4 w-4 text-red-600 shrink-0 mt-0.5" />
                  <p className="text-sm font-medium text-red-700 dark:text-red-400">{t("runsHistory.runFailed")}</p>
                </div>
              )}
              <CommentThread entityType="run" entityId={run.run_id} />
            </div>
          </TableCell>
        </TableRow>
      )}
    </>
  );
}

function RunHistorySkeleton() {
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Skeleton className="h-7 w-40" />
        <Skeleton className="h-4 w-72" />
      </div>
      <Skeleton className="h-9 w-full max-w-2xl" />
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
      <p className="text-muted-foreground/70 text-xs mb-3">{t("runsHistory.skeletonFailedHint")}</p>
      <Button variant="outline" size="sm" onClick={resetErrorBoundary} className="gap-2">
        <RotateCcw className="h-3 w-3" />
        {t("common.retry")}
      </Button>
    </div>
  );
}
