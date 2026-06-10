import { createFileRoute, Link, Navigate } from "@tanstack/react-router";
import { formatDateTime as formatDate } from "@/lib/format-utils";
import { useState, useCallback, useEffect, useRef, useMemo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  BarChart3,
  Play,
  Loader2,
  CheckCircle2,
  AlertTriangle,
  ChevronLeft,
  ChevronRight as ChevronRightIcon,
  Clock,
  XCircle,
  History,
  ChevronDown,
  Plus,
  Eye,
  Settings2,
  User,
} from "lucide-react";
import { toast } from "sonner";
import { isAxiosError } from "axios";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useJobPolling } from "@/hooks/use-job-polling";
import {
  useSubmitProfileRun,
  useSubmitBatchProfileRun,
  useListProfileRuns,
  useGetProfileRunResults,
  useSaveRules,
  useGetTableColumns,
  useGetRules,
  getProfileRunStatus,
  type ProfileResultsOut,
  type ProfileRunSummaryOut,
  type RunStatusOut,
  type User as UserType,
} from "@/lib/api";
import { cancelProfileRun } from "@/lib/api-custom";
import { useCurrentUserSuspense } from "@/hooks/use-suspense-queries";
import selector from "@/lib/selector";

export const Route = createFileRoute("/_sidebar/profiler")({
  component: ProfilerPage,
});

// ──────────────────────────────────────────────────────────────────────────────
// localStorage helpers — survive page navigation
// ──────────────────────────────────────────────────────────────────────────────

const ACTIVE_RUNS_KEY = "dqx_active_profiler_runs";
const RUN_TTL_MS = 24 * 60 * 60 * 1000; // 24 h

interface StoredActiveRun {
  runId: string;
  jobRunId: number;
  viewFqn: string;
  tableFqn: string;
  mode: "single" | "batch";
  submittedAt: number;
}

function loadStoredRuns(): StoredActiveRun[] {
  try {
    const raw = localStorage.getItem(ACTIVE_RUNS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as StoredActiveRun[];
    const cutoff = Date.now() - RUN_TTL_MS;
    return parsed.filter((r) => r.submittedAt > cutoff);
  } catch {
    return [];
  }
}

function persistRun(run: StoredActiveRun): void {
  try {
    const existing = loadStoredRuns().filter((r) => r.runId !== run.runId);
    localStorage.setItem(ACTIVE_RUNS_KEY, JSON.stringify([...existing, run]));
  } catch { /* non-fatal */ }
}

function removeStoredRun(runId: string): void {
  try {
    localStorage.setItem(
      ACTIVE_RUNS_KEY,
      JSON.stringify(loadStoredRuns().filter((r) => r.runId !== runId)),
    );
  } catch { /* non-fatal */ }
}

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

interface ActiveBatchRun {
  runId: string;
  jobRunId: number;
  viewFqn: string;
  tableFqn: string;
  state: "running" | "success" | "failed";
  result?: ProfileResultsOut;
  message?: string;
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

function ProfilerStatusBadge({ status }: { status: string | null | undefined }) {
  const { t } = useTranslation();
  switch (status) {
    case "SUCCESS":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          {t("profiler.successBadge")}
        </Badge>
      );
    case "FAILED":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          {t("profiler.failedBadge")}
        </Badge>
      );
    case "RUNNING":
      return (
        <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600">
          <Loader2 className="h-3 w-3 animate-spin" />
          {t("profiler.runningBadge")}
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status ?? t("profiler.unknownBadge")}</Badge>;
  }
}

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function estimateEtaSeconds(
  tableFqn: string,
  sampleLimit: number,
  runs: ProfileRunSummaryOut[],
): number | null {
  const priorRuns = runs.filter(
    (r) =>
      r.source_table_fqn === tableFqn &&
      r.status === "SUCCESS" &&
      r.duration_seconds != null &&
      r.rows_profiled != null &&
      r.rows_profiled > 0,
  );
  if (priorRuns.length === 0) return null;
  const avgSecsPerRow =
    priorRuns.reduce((sum, r) => sum + r.duration_seconds! / r.rows_profiled!, 0) /
    priorRuns.length;
  return Math.round(avgSecsPerRow * sampleLimit);
}

function parseTableFqn(fqn: string): { catalog: string; schema: string; table: string } | null {
  const parts = fqn.split(".");
  if (parts.length !== 3) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
}

/**
 * Extract a human-readable error message from a profiler API failure.
 *
 * The backend now classifies the most common failure modes (missing
 * Unity Catalog grants, table not found, etc.) and returns them as
 * proper HTTP status codes with a friendly ``detail`` string in the
 * response body. We pull that string out so it can replace the generic
 * "Failed to submit profiling job" toast — the user needs to know
 * *exactly* which permission they're missing on which schema, not just
 * that something failed.
 *
 * Falls back through a few defaults when the response isn't shaped the
 * way we expect (network error, non-axios error, missing detail field,
 * ...).
 */
function extractProfilerError(err: unknown): string {
  const axErr = err as {
    response?: {
      data?: { detail?: unknown };
      status?: number;
      statusText?: string;
    };
    message?: string;
  };
  const detail = axErr?.response?.data?.detail;
  if (typeof detail === "string" && detail.trim()) return detail;
  if (Array.isArray(detail) && detail.length > 0) {
    // FastAPI validation errors come back as a list of objects with a
    // ``msg`` field. Surface the first one rather than ``[object Object]``.
    const first = detail[0] as { msg?: unknown };
    if (typeof first?.msg === "string") return first.msg;
  }
  if (axErr?.response?.status) {
    return `Server error (HTTP ${axErr.response.status}${axErr.response.statusText ? " " + axErr.response.statusText : ""})`;
  }
  if (typeof axErr?.message === "string" && axErr.message) return axErr.message;
  return "Failed to submit profiling job";
}

// ──────────────────────────────────────────────────────────────────────────────
// Sortable column header
// ──────────────────────────────────────────────────────────────────────────────

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
      return { key, dir: key === ("started" as string) || key === ("run_date" as string) ? "desc" : "asc" };
    });
  }, []);
  return { sortKey: sort.key, sortDir: sort.dir, handleSort };
}

// ──────────────────────────────────────────────────────────────────────────────
// Main Page Component
// ──────────────────────────────────────────────────────────────────────────────

function ProfilerPage() {
  const { t } = useTranslation();
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  // ── Single-table run state (used when one table + column subset via single-table API) ──
  const [jobRunId, setJobRunId] = useState<number | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [viewFqn, setViewFqn] = useState<string | null>(null);
  const [results, setResults] = useState<ProfileResultsOut | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [startedAt, setStartedAt] = useState<number | null>(null);

  // ── Table selection (one or many; batch API, or single-table API when 1 table + column pick) ──
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [batchRuns, setBatchRuns] = useState<ActiveBatchRun[]>([]);
  const batchRunsRef = useRef<ActiveBatchRun[]>([]);
  batchRunsRef.current = batchRuns;

  // ── Advanced options ────────────────────────────────────────────────────────
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [filterSql, setFilterSql] = useState("");
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [llmPkDetection, setLlmPkDetection] = useState(false);
  const [removeOutliers, setRemoveOutliers] = useState(true);
  const [numSigmas, setNumSigmas] = useState(3);

  // ── Historical results dialog ───────────────────────────────────────────────
  const [historyRunId, setHistoryRunId] = useState<string | null>(null);

  const singleTableFqn =
    selectedTables.length === 1 ? selectedTables[0] ?? "" : "";
  const hasSingleTable = parseTableFqn(singleTableFqn) !== null;
  const tableParts = hasSingleTable ? parseTableFqn(singleTableFqn) : null;

  const { data: currentUser } = useCurrentUserSuspense(selector<UserType>());
  const currentUserEmail = currentUser?.user_name ?? "";
  const [myRunsOnly, setMyRunsOnly] = useState(false);
  const [hCatalogFilter, setHCatalogFilter] = useState("all");
  const [hSchemaFilter, setHSchemaFilter] = useState("all");
  const [hTableFilter, setHTableFilter] = useState("all");

  const submitMutation = useSubmitProfileRun();
  const batchSubmitMutation = useSubmitBatchProfileRun();
  const { data: runsResp, isLoading: runsLoading, refetch: refetchRuns } = useListProfileRuns();
  const allRuns: ProfileRunSummaryOut[] = runsResp?.data ?? [];

  const { hCatalogs, hSchemasByCatalog, hTablesByCatalogSchema } = useMemo(() => {
    const catalogSet = new Set<string>();
    const schemaMap = new Map<string, Set<string>>();
    const tableMap = new Map<string, Set<string>>();
    for (const run of allRuns) {
      const parts = parseTableFqn(run.source_table_fqn);
      if (!parts) continue;
      catalogSet.add(parts.catalog);
      if (!schemaMap.has(parts.catalog)) schemaMap.set(parts.catalog, new Set());
      schemaMap.get(parts.catalog)!.add(parts.schema);
      const key = `${parts.catalog}.${parts.schema}`;
      if (!tableMap.has(key)) tableMap.set(key, new Set());
      tableMap.get(key)!.add(parts.table);
    }
    return {
      hCatalogs: Array.from(catalogSet).sort(),
      hSchemasByCatalog: Object.fromEntries(
        Array.from(schemaMap.entries()).map(([c, s]) => [c, Array.from(s).sort()]),
      ),
      hTablesByCatalogSchema: Object.fromEntries(
        Array.from(tableMap.entries()).map(([k, t]) => [k, Array.from(t).sort()]),
      ),
    };
  }, [allRuns]);

  const hAvailableSchemas = hCatalogFilter !== "all" ? hSchemasByCatalog[hCatalogFilter] || [] : [];
  const hAvailableTables = (hCatalogFilter !== "all" && hSchemaFilter !== "all")
    ? hTablesByCatalogSchema[`${hCatalogFilter}.${hSchemaFilter}`] || []
    : [];

  const handleHCatalogChange = (value: string) => {
    setHCatalogFilter(value);
    setHSchemaFilter("all");
    setHTableFilter("all");
  };
  const handleHSchemaChange = (value: string) => {
    setHSchemaFilter(value);
    setHTableFilter("all");
  };

  const hasHistoryFilters = hCatalogFilter !== "all" || hSchemaFilter !== "all" || hTableFilter !== "all" || myRunsOnly;

  type ProfileSortKey = "table" | "status" | "rows" | "duration" | "started" | "by";
  const { sortKey: pSortKey, sortDir: pSortDir, handleSort: handleProfileSort } = useSort<ProfileSortKey>("started");

  const runs = useMemo(() => {
    const filtered = allRuns.filter((r) => {
      if (myRunsOnly && currentUserEmail && r.requesting_user !== currentUserEmail) return false;
      const parts = parseTableFqn(r.source_table_fqn);
      if (hCatalogFilter !== "all") {
        if (!parts || parts.catalog !== hCatalogFilter) return false;
      }
      if (hSchemaFilter !== "all") {
        if (!parts || parts.schema !== hSchemaFilter) return false;
      }
      if (hTableFilter !== "all") {
        if (!parts || parts.table !== hTableFilter) return false;
      }
      return true;
    });
    const dir = pSortDir === "asc" ? 1 : -1;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (pSortKey) {
        case "table":
          cmp = (a.source_table_fqn ?? "").localeCompare(b.source_table_fqn ?? "");
          break;
        case "status":
          cmp = (a.status ?? "").localeCompare(b.status ?? "");
          break;
        case "rows":
          cmp = (a.rows_profiled ?? 0) - (b.rows_profiled ?? 0);
          break;
        case "duration":
          cmp = (a.duration_seconds ?? 0) - (b.duration_seconds ?? 0);
          break;
        case "started":
          cmp = (a.created_at ?? "").localeCompare(b.created_at ?? "");
          break;
        case "by":
          cmp = (a.requesting_user ?? "").localeCompare(b.requesting_user ?? "");
          break;
      }
      return cmp * dir;
    });
  }, [allRuns, myRunsOnly, currentUserEmail, hCatalogFilter, hSchemaFilter, hTableFilter, pSortKey, pSortDir]);

  const HISTORY_PAGE_SIZE = 25;
  const [historyPage, setHistoryPage] = useState(1);
  const historyTotalPages = Math.max(1, Math.ceil(runs.length / HISTORY_PAGE_SIZE));
  const pagedRuns = useMemo(
    () => runs.slice((historyPage - 1) * HISTORY_PAGE_SIZE, historyPage * HISTORY_PAGE_SIZE),
    [runs, historyPage],
  );

  useEffect(() => { setHistoryPage(1); }, [myRunsOnly, hCatalogFilter, hSchemaFilter, hTableFilter, pSortKey, pSortDir]);

  const { data: columnsResp } = useGetTableColumns(
    tableParts?.catalog ?? "",
    tableParts?.schema ?? "",
    tableParts?.table ?? "",
    { query: { enabled: hasSingleTable } },
  );
  const availableColumns = columnsResp?.data?.map((c) => c.name) ?? [];

  const resultsQuery = useGetProfileRunResults(runId ?? "", {
    query: { enabled: false },
  });

  const historyResultsQuery = useGetProfileRunResults(historyRunId ?? "", {
    query: { enabled: historyRunId !== null },
  });

  // ── Restore active runs from localStorage on mount ──────────────────────────
  useEffect(() => {
    const stored = loadStoredRuns();
    if (stored.length === 0) return;

    const singleRun = stored.find((r) => r.mode === "single");
    if (singleRun) {
      setSelectedTables([singleRun.tableFqn]);
      setRunId(singleRun.runId);
      setJobRunId(singleRun.jobRunId);
      setViewFqn(singleRun.viewFqn);
      setStartedAt(singleRun.submittedAt);
    }

    const batchStored = stored.filter((r) => r.mode === "batch");
    if (batchStored.length > 0) {
      setBatchRuns(
        batchStored.map((r) => ({
          runId: r.runId,
          jobRunId: r.jobRunId,
          viewFqn: r.viewFqn,
          tableFqn: r.tableFqn,
          state: "running" as const,
        })),
      );
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (selectedTables.length !== 1) setSelectedColumns([]);
  }, [selectedTables.length]);

  // ── Single-table polling ────────────────────────────────────────────────────
  const fetchStatus = useCallback(async () => {
    if (!runId || jobRunId === null) throw new Error("No active run");
    const resp = await getProfileRunStatus(runId);
    if (startedAt) setElapsedSeconds(Math.round((Date.now() - startedAt) / 1000));
    return resp.data;
  }, [runId, jobRunId, startedAt]);

  const polling = useJobPolling({
    fetchStatus,
    enabled: jobRunId !== null && runId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (runId) removeStoredRun(runId);
      if (status.result_state === "SUCCESS") {
        try {
          const resp = await resultsQuery.refetch();
          if (resp.data?.data) setResults(resp.data.data);
          toast.success(t("profiler.profilingComplete"));
        } catch {
          toast.error(t("profiler.failedFetchResults"));
        }
      } else {
        toast.error(t("profiler.profilingFailed", { message: status.message || t("common.unknownError") }));
      }
      setJobRunId(null);
      setViewFqn(null);
      setStartedAt(null);
      refetchRuns();
    },
    onError: () => {
      toast.error(t("profiler.failedCheckStatus"));
    },
  });

  // ── Multi-table polling (per-run interval) ──────────────────────────────────
  useEffect(() => {
    const stillRunning = batchRuns.filter((r) => r.state === "running");
    if (stillRunning.length === 0) return;

    const interval = setInterval(async () => {
      const current = batchRunsRef.current;
      const activeRuns = current.filter((r) => r.state === "running");
      if (activeRuns.length === 0) {
        clearInterval(interval);
        return;
      }

      const updates: Partial<ActiveBatchRun>[] = await Promise.all(
        activeRuns.map(async (run) => {
          try {
            const resp = await getProfileRunStatus(run.runId);
            const status: RunStatusOut = resp.data;
            const isTerminal = status.state === "TERMINATED" || status.state === "INTERNAL_ERROR" || status.state === "SKIPPED";
            if (!isTerminal) return { runId: run.runId };

            if (status.state === "TERMINATED" && status.result_state === "SUCCESS") {
              try {
                const { default: axios } = await import("axios");
                const resultsResp = await axios.get(`/api/v1/profiler/runs/${run.runId}/results`);
                return {
                  runId: run.runId,
                  state: "success" as const,
                  result: resultsResp.data as ProfileResultsOut,
                };
              } catch {
                return { runId: run.runId, state: "success" as const };
              }
            } else {
              return {
                runId: run.runId,
                state: "failed" as const,
                message: status.message ?? "Unknown error",
              };
            }
          } catch {
            return { runId: run.runId };
          }
        }),
      );

      setBatchRuns((prev) => {
        const updated = prev.map((run) => {
          const upd = updates.find((u) => u.runId === run.runId);
          if (!upd || !upd.state) return run;
          // Remove from localStorage when run reaches terminal state
          removeStoredRun(run.runId);
          return { ...run, ...upd };
        });
        return updated;
      });
    }, 4000);

    return () => clearInterval(interval);
  }, [batchRuns.filter((r) => r.state === "running").length]); // eslint-disable-line react-hooks/exhaustive-deps

  // Refetch run history when all batch runs finish
  useEffect(() => {
    if (batchRuns.length === 0) return;
    const allDone = batchRuns.every((r) => r.state !== "running");
    if (allDone) refetchRuns();
  }, [batchRuns, refetchRuns]);

  // Auto-refresh run history every 8 s while RUNNING rows are visible
  const hasRunningInHistory = runs.some((r) => r.status === "RUNNING");
  useEffect(() => {
    if (!hasRunningInHistory) return;
    const id = setInterval(() => refetchRuns(), 8000);
    return () => clearInterval(id);
  }, [hasRunningInHistory, refetchRuns]);

  // ── Handlers ────────────────────────────────────────────────────────────────
  const buildProfileOptions = () => {
    const opts: Record<string, unknown> = {
      remove_outliers: removeOutliers,
      num_sigmas: numSigmas,
      llm_primary_key_detection: llmPkDetection,
    };
    if (filterSql.trim()) opts.filter = filterSql.trim();
    return opts;
  };

  const handleSingleRun = async () => {
    const tableFqn = selectedTables[0];
    if (!tableFqn || !parseTableFqn(tableFqn)) {
      toast.error(t("profiler.selectTableFirst"));
      return;
    }
    try {
      setBatchRuns([]);
      setResults(null);
      setElapsedSeconds(0);
      const resp = await submitMutation.mutateAsync({
        data: {
          table_fqn: tableFqn,
          sample_limit: sampleLimit,
          columns: selectedColumns.length > 0 ? selectedColumns : undefined,
          profile_options: buildProfileOptions(),
        },
      });
      setRunId(resp.data.run_id);
      setJobRunId(resp.data.job_run_id);
      setViewFqn(resp.data.view_fqn);
      setStartedAt(Date.now());
      persistRun({
        runId: resp.data.run_id,
        jobRunId: resp.data.job_run_id,
        viewFqn: resp.data.view_fqn,
        tableFqn,
        mode: "single",
        submittedAt: Date.now(),
      });
      toast.info(t("profiler.submittedWaiting"));
    } catch (err) {
      // Surface the backend's detail message to the user instead of a
      // generic "Failed to submit". The most common cause is a missing
      // ``USE SCHEMA`` / ``SELECT`` grant on the source table, which
      // the backend now classifies as 403 with a crisp human-readable
      // explanation in ``response.data.detail``.
      const detail = extractProfilerError(err);
      const isPermission =
        isAxiosError(err) && err.response?.status === 403;
      toast.error(detail, {
        description: isPermission
          ? t("profiler.permissionHint")
          : undefined,
        duration: isPermission ? 12_000 : 8_000,
      });
    }
  };

  const handleBatchRun = async () => {
    if (selectedTables.length === 0) { toast.error(t("profiler.selectAtLeastOneTable")); return; }
    try {
      setResults(null);
      setRunId(null);
      setJobRunId(null);
      setViewFqn(null);
      setStartedAt(null);
      setBatchRuns([]);
      const resp = await batchSubmitMutation.mutateAsync({
        data: {
          table_fqns: selectedTables,
          sample_limit: sampleLimit,
          profile_options: buildProfileOptions(),
        },
      });
      // Map successful submissions back to the *originally selected*
      // tables. The backend preserves order for the ``runs`` array but
      // skips failed tables, so we walk both lists together to align
      // each run with its source table FQN.
      const succeededTables = selectedTables.filter(
        (fqn) =>
          !(resp.data.errors ?? []).some((e) => e.table_fqn === fqn),
      );
      const newRuns: ActiveBatchRun[] = resp.data.runs.map((run, i) => ({
        runId: run.run_id,
        jobRunId: run.job_run_id,
        viewFqn: run.view_fqn,
        tableFqn: succeededTables[i] ?? selectedTables[i] ?? "",
        state: "running",
      }));
      setBatchRuns(newRuns);
      newRuns.forEach((run) =>
        persistRun({
          runId: run.runId,
          jobRunId: run.jobRunId,
          viewFqn: run.viewFqn,
          tableFqn: run.tableFqn,
          mode: "batch",
          submittedAt: Date.now(),
        }),
      );
      const failures = resp.data.errors ?? [];
      if (newRuns.length > 0) {
        toast.info(t("profiler.jobsSubmitted", { count: newRuns.length }));
      }
      if (failures.length > 0) {
        const previewCount = Math.min(failures.length, 3);
        for (const f of failures.slice(0, previewCount)) {
          const tableShort = f.table_fqn.split(".").pop() || f.table_fqn;
          toast.error(`${tableShort}: ${f.error}`, {
            description:
              f.error_code === "INSUFFICIENT_PERMISSIONS"
                ? t("profiler.permissionHintTable")
                : undefined,
            duration: 12_000,
          });
        }
        if (failures.length > previewCount) {
          toast.error(
            t("profiler.moreFailed", { count: failures.length - previewCount }),
            { duration: 8_000 },
          );
        }
      }
    } catch (err) {
      const detail = extractProfilerError(err);
      const isPermission =
        isAxiosError(err) && err.response?.status === 403;
      toast.error(detail, {
        description: isPermission
          ? t("profiler.permissionHintBatch")
          : undefined,
        duration: isPermission ? 12_000 : 8_000,
      });
    }
  };

  const [sampleLimit, setSampleLimit] = useState(50_000);

  const [isCancellingSingle, setIsCancellingSingle] = useState(false);
  const [cancellingBatchRunIds, setCancellingBatchRunIds] = useState<Set<string>>(new Set());

  const isSingleRunning = submitMutation.isPending || polling.isPolling;
  const isBatchSubmitting = batchSubmitMutation.isPending;
  const isBatchPolling = batchRuns.some((r) => r.state === "running");
  const isBusy = isSingleRunning || isBatchSubmitting || isBatchPolling;

  const handleCancelSingle = async () => {
    if (!runId || jobRunId === null) return;
    setIsCancellingSingle(true);
    try {
      await cancelProfileRun(runId, {
        job_run_id: jobRunId,
        view_fqn: viewFqn ?? undefined,
      });
      toast.info(t("profiler.runCanceled"));
      polling.stopPolling();
      removeStoredRun(runId);
      setJobRunId(null);
      setViewFqn(null);
      setStartedAt(null);
      refetchRuns();
    } catch {
      toast.error(t("profiler.failedCancelRun"));
    } finally {
      setIsCancellingSingle(false);
    }
  };

  const handleCancelBatch = async (run: ActiveBatchRun) => {
    setCancellingBatchRunIds((prev) => new Set(prev).add(run.runId));
    try {
      await cancelProfileRun(run.runId, {
        job_run_id: run.jobRunId,
        view_fqn: run.viewFqn,
      });
      removeStoredRun(run.runId);
      setBatchRuns((prev) =>
        prev.map((r) =>
          r.runId === run.runId
            ? { ...r, state: "failed" as const, message: t("profiler.canceledByUser") }
            : r,
        ),
      );
      toast.info(t("profiler.canceledFor", { table: run.tableFqn.split(".").pop() ?? "" }));
    } catch {
      toast.error(t("profiler.failedCancelRun"));
    } finally {
      setCancellingBatchRunIds((prev) => {
        const next = new Set(prev);
        next.delete(run.runId);
        return next;
      });
    }
  };

  const handleCancelAllBatch = async () => {
    const runningRuns = batchRuns.filter((r) => r.state === "running");
    await Promise.allSettled(runningRuns.map((r) => handleCancelBatch(r)));
  };

  const etaSeconds =
    isSingleRunning && selectedTables.length === 1 && selectedTables[0]
      ? estimateEtaSeconds(selectedTables[0], sampleLimit, runs)
      : null;

  const batchCompleted = batchRuns.filter((r) => r.state !== "running").length;
  const batchTotal = batchRuns.length;
  const batchSucceeded = batchRuns.filter((r) => r.state === "success").length;
  const batchFailed = batchRuns.filter((r) => r.state === "failed").length;
  const batchProgress = batchTotal > 0 ? Math.round((batchCompleted / batchTotal) * 100) : 0;

  const toggleColumn = (col: string) => {
    setSelectedColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col],
    );
  };

  /** Batch API for multi-table or single-table without column subset; single-table API when columns are chosen. */
  const handleProfileRun = async () => {
    if (selectedTables.length === 0) {
      toast.error(t("profiler.selectAtLeastOneTable"));
      return;
    }
    if (selectedTables.length === 1 && selectedColumns.length > 0) {
      await handleSingleRun();
    } else {
      await handleBatchRun();
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[{ label: t("rulesCreate.breadcrumb"), to: "/rules/create" }]} page={t("profiler.breadcrumb")} />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">{t("profiler.title")}</h1>
          <p className="text-muted-foreground">
            {t("profiler.subtitle")}
          </p>
        </div>
      </div>

      {/* New run form */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            {t("profiler.newRunTitle")}
          </CardTitle>
          <CardDescription>
            {t("profiler.newRunDescription")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <CatalogBrowser
            value=""
            onChange={() => {}}
            disabled={isBusy}
            multiSelect
            selectedTables={selectedTables}
            onMultiChange={setSelectedTables}
          />
          {selectedTables.length > 0 && (
            <div className="space-y-1.5">
              <p className="text-sm font-medium">
                {t("profiler.tablesSelected", { count: selectedTables.length })}
              </p>
              <div className="flex flex-wrap gap-1">
                {selectedTables.map((t) => (
                  <Badge key={t} variant="secondary" className="font-mono text-xs gap-1">
                    {t.split(".").pop()}
                    <button
                      type="button"
                      className="ml-0.5 hover:text-destructive"
                      onClick={() => setSelectedTables((prev) => prev.filter((x) => x !== t))}
                      disabled={isBusy}
                    >
                      ×
                    </button>
                  </Badge>
                ))}
              </div>
            </div>
          )}

          <div className="flex items-end gap-4">
            <div className="grid gap-2 max-w-xs">
              <Label htmlFor="sample-limit">{t("profiler.sampleLimit")}</Label>
              <Input
                id="sample-limit"
                type="number"
                value={sampleLimit}
                onChange={(e) =>
                  setSampleLimit(Math.min(100_000, Math.max(1, Number(e.target.value))))
                }
                disabled={isBusy}
                min={1}
                max={100_000}
              />
              <p className="text-xs text-muted-foreground">{t("profiler.maxSampleLimit")}</p>
            </div>

            {isBusy ? (
              <Button
                variant="destructive"
                onClick={isBatchPolling ? handleCancelAllBatch : handleCancelSingle}
                disabled={isCancellingSingle || submitMutation.isPending}
                className="gap-2 mb-6"
              >
                {isCancellingSingle ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <XCircle className="h-4 w-4" />
                )}
                {isBatchPolling
                  ? t("profiler.stopAll", { done: batchCompleted, total: batchTotal })
                  : t("profiler.stopRun")}
              </Button>
            ) : (
              <Button
                onClick={handleProfileRun}
                disabled={selectedTables.length === 0}
                className="gap-2 mb-6"
              >
                <Play className="h-4 w-4" />
                {selectedTables.length > 1
                  ? t("profiler.runTables", { count: selectedTables.length })
                  : t("profiler.runProfile")}
              </Button>
            )}
          </div>

          {/* Advanced options */}
          <div>
            <Button
              variant="ghost"
              size="sm"
              className="gap-2 text-muted-foreground"
              onClick={() => setAdvancedOpen((v) => !v)}
              type="button"
            >
              <Settings2 className="h-4 w-4" />
              {t("profiler.advancedOptions")}
              <ChevronDown
                className={`h-3 w-3 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
              />
            </Button>
            {advancedOpen && (
              <div className="mt-3 space-y-4 rounded-lg border p-4">
                <div className="grid gap-2">
                  <Label htmlFor="filter-sql">{t("profiler.rowFilter")}</Label>
                  <Input
                    id="filter-sql"
                    placeholder={t("profiler.rowFilterPlaceholder")}
                    value={filterSql}
                    onChange={(e) => setFilterSql(e.target.value)}
                    disabled={isBusy}
                  />
                  <p className="text-xs text-muted-foreground">
                    {t("profiler.rowFilterHint")}
                    {selectedTables.length > 1 && t("profiler.rowFilterAllTables")}
                  </p>
                </div>

                {selectedTables.length === 1 && availableColumns.length > 0 && (
                  <div className="grid gap-2">
                    <Label>
                      {t("profiler.columnsToProfile")}
                      {selectedColumns.length > 0 && (
                        <span className="ml-2 text-xs text-muted-foreground">
                          {t("profiler.columnsSelected", { count: selectedColumns.length })}
                        </span>
                      )}
                    </Label>
                    <div className="flex flex-wrap gap-1.5 max-h-32 overflow-y-auto p-1">
                      {availableColumns.map((col) => (
                        <button
                          key={col}
                          type="button"
                          onClick={() => toggleColumn(col)}
                          disabled={isBusy}
                          className={`px-2 py-0.5 rounded text-xs font-mono border transition-colors ${
                            selectedColumns.includes(col)
                              ? "bg-primary text-primary-foreground border-primary"
                              : "bg-muted border-border hover:border-primary/50"
                          }`}
                        >
                          {col}
                        </button>
                      ))}
                    </div>
                    {selectedColumns.length > 0 && (
                      <button
                        type="button"
                        className="text-xs text-muted-foreground underline self-start"
                        onClick={() => setSelectedColumns([])}
                      >
                        {t("profiler.clearSelectionProfileAll")}
                      </button>
                    )}
                    <p className="text-xs text-muted-foreground">
                      {t("profiler.toggleColumnsHint")}
                    </p>
                  </div>
                )}

                <div className="flex items-center justify-between">
                  <div className="grid gap-0.5">
                    <Label htmlFor="remove-outliers">{t("profiler.removeOutliers")}</Label>
                    <p className="text-xs text-muted-foreground">
                      {t("profiler.removeOutliersHint")}
                    </p>
                  </div>
                  <Switch
                    id="remove-outliers"
                    checked={removeOutliers}
                    onCheckedChange={setRemoveOutliers}
                    disabled={isBusy}
                  />
                </div>

                {removeOutliers && (
                  <div className="grid gap-2 max-w-xs">
                    <Label htmlFor="num-sigmas">{t("profiler.outlierThreshold")}</Label>
                    <Input
                      id="num-sigmas"
                      type="number"
                      value={numSigmas}
                      onChange={(e) => setNumSigmas(Math.max(1, Number(e.target.value)))}
                      disabled={isBusy}
                      min={1}
                      max={10}
                      step={0.5}
                    />
                    <p className="text-xs text-muted-foreground">
                      {t("profiler.outlierThresholdHint")}
                    </p>
                  </div>
                )}

                <div className="flex items-center justify-between">
                  <div className="grid gap-0.5">
                    <Label htmlFor="llm-pk">{t("profiler.llmPkDetection")}</Label>
                    <p className="text-xs text-muted-foreground">
                      {t("profiler.llmPkHint")}
                    </p>
                  </div>
                  <Switch
                    id="llm-pk"
                    checked={llmPkDetection}
                    onCheckedChange={setLlmPkDetection}
                    disabled={isBusy}
                  />
                </div>
              </div>
            )}
          </div>

          {/* Single-table run progress */}
          {isSingleRunning && (
            <div className="rounded-lg border bg-muted/30 p-4 space-y-2">
              <div className="flex items-center gap-2 text-sm">
                <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
                <span className="font-medium">
                  {polling.status?.state ?? t("profiler.submitting")}
                </span>
                <span className="text-muted-foreground tabular-nums ml-auto">
                  {t("profiler.elapsed", { seconds: elapsedSeconds })}
                  {etaSeconds != null && t("profiler.estimated", { duration: formatDuration(etaSeconds) })}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  className="gap-1.5 text-red-600 border-red-300 hover:bg-red-50 hover:text-red-700 ml-2"
                  onClick={handleCancelSingle}
                  disabled={isCancellingSingle || submitMutation.isPending}
                >
                  {isCancellingSingle ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <XCircle className="h-3.5 w-3.5" />
                  )}
                  {t("profiler.stop")}
                </Button>
              </div>
            </div>
          )}

          {/* Batch run progress */}
          {(isBatchPolling || (batchRuns.length > 0 && !isBatchSubmitting)) && (
            <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2 font-medium">
                  {isBatchPolling ? (
                    <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
                  ) : (
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                  )}
                  {isBatchPolling
                    ? t("profiler.profilingInProgress", { done: batchCompleted, total: batchTotal })
                    : batchFailed > 0
                      ? t("profiler.batchCompleteWithFailed", { succeeded: batchSucceeded, failed: batchFailed })
                      : t("profiler.batchComplete", { succeeded: batchSucceeded })}
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground text-xs tabular-nums">
                    {batchProgress}%
                  </span>
                  {isBatchPolling && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-1.5 text-red-600 border-red-300 hover:bg-red-50 hover:text-red-700 h-7 text-xs"
                      onClick={handleCancelAllBatch}
                    >
                      <XCircle className="h-3 w-3" />
                      {t("profiler.stopAllShort")}
                    </Button>
                  )}
                </div>
              </div>
              <div className="w-full h-1.5 rounded-full bg-primary/20 overflow-hidden">
                <div
                  className="h-full bg-primary transition-all duration-500"
                  style={{ width: `${batchProgress}%` }}
                />
              </div>

              {/* Per-table status list */}
              <div className="space-y-1 max-h-48 overflow-y-auto">
                {batchRuns.map((run) => (
                  <div key={run.runId} className="flex items-center gap-2 text-xs py-1">
                    {run.state === "running" ? (
                      <Loader2 className="h-3 w-3 animate-spin text-blue-500 shrink-0" />
                    ) : run.state === "success" ? (
                      <CheckCircle2 className="h-3 w-3 text-green-500 shrink-0" />
                    ) : (
                      <XCircle className="h-3 w-3 text-red-500 shrink-0" />
                    )}
                    <code className="font-mono truncate flex-1">{run.tableFqn}</code>
                    <span className="text-muted-foreground shrink-0">
                      {run.state === "running"
                        ? t("common.running")
                        : run.state === "success"
                          ? run.result
                            ? t("profiler.rulesGenCountAndCount", { rows: run.result.rows_profiled?.toLocaleString() ?? "?", count: run.result.generated_rules?.length ?? 0 })
                            : t("common.done")
                          : run.message ?? t("common.failed")}
                    </span>
                    {run.state === "running" && (
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-5 px-1.5 text-red-600 hover:text-red-700 hover:bg-red-50 shrink-0"
                        onClick={() => handleCancelBatch(run)}
                        disabled={cancellingBatchRunIds.has(run.runId)}
                      >
                        {cancellingBatchRunIds.has(run.runId) ? (
                          <Loader2 className="h-3 w-3 animate-spin" />
                        ) : (
                          <XCircle className="h-3 w-3" />
                        )}
                      </Button>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Single-table API results (subset of columns) */}
          {results && (
            <>
              <Separator />
              <ProfileResults results={results} tableFqn={results.source_table_fqn} />
            </>
          )}

          {/* Batch results — expandable per-table sections */}
          {batchRuns.length > 0 && !isBatchPolling && (
            <>
              <Separator />
              <div className="space-y-4">
                <h3 className="text-sm font-semibold">{t("profiler.resultsByTable")}</h3>
                {batchRuns.map((run) =>
                  run.state === "success" && run.result ? (
                    <BatchTableResult key={run.runId} run={run} />
                  ) : run.state === "failed" ? (
                    <div
                      key={run.runId}
                      className="flex items-center gap-2 text-sm text-destructive border border-destructive/30 rounded-lg p-3"
                    >
                      <AlertTriangle className="h-4 w-4 shrink-0" />
                      <code className="font-mono text-xs">{run.tableFqn}</code>
                      <span className="text-muted-foreground ml-auto text-xs">{run.message}</span>
                    </div>
                  ) : null,
                )}
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Past runs */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <History className="h-5 w-5" />
                {t("profiler.runHistory")}
              </CardTitle>
              <CardDescription>
                {runsLoading
                  ? t("common.loading")
                  : `${t("profiler.runs", { count: runs.length })}${
                      runs.length !== allRuns.length ? t("profiler.filteredFrom", { total: allRuns.length }) : ""
                    }`}
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant={myRunsOnly ? "default" : "outline"}
                size="sm"
                className="h-8 gap-1.5 text-xs"
                onClick={() => setMyRunsOnly((prev) => !prev)}
              >
                <User className="h-3.5 w-3.5" />
                {t("profiler.myRuns")}
              </Button>
              <Button variant="ghost" size="sm" onClick={() => refetchRuns()} className="h-8 gap-1.5 text-xs">
                <Clock className="h-3.5 w-3.5" />
                {t("common.refresh")}
              </Button>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap pt-2">
            <Select value={hCatalogFilter} onValueChange={handleHCatalogChange}>
              <SelectTrigger className="w-[160px] h-8 text-xs">
                <SelectValue placeholder={t("profiler.allCatalogs")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("profiler.allCatalogs")}</SelectItem>
                {hCatalogs.map((cat) => (
                  <SelectItem key={cat} value={cat}>{cat}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={hSchemaFilter} onValueChange={handleHSchemaChange} disabled={hCatalogFilter === "all"}>
              <SelectTrigger className="w-[160px] h-8 text-xs">
                <SelectValue placeholder={t("profiler.allSchemas")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("profiler.allSchemas")}</SelectItem>
                {hAvailableSchemas.map((sch) => (
                  <SelectItem key={sch} value={sch}>{sch}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={hTableFilter} onValueChange={setHTableFilter} disabled={hSchemaFilter === "all"}>
              <SelectTrigger className="w-[180px] h-8 text-xs">
                <SelectValue placeholder={t("profiler.allTables")} />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("profiler.allTables")}</SelectItem>
                {hAvailableTables.map((tbl) => (
                  <SelectItem key={tbl} value={tbl}>{tbl}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            {hasHistoryFilters && (
              <Button
                variant="ghost"
                size="sm"
                className="h-8 text-xs"
                onClick={() => {
                  setHCatalogFilter("all");
                  setHSchemaFilter("all");
                  setHTableFilter("all");
                  setMyRunsOnly(false);
                }}
              >
                {t("common.clearFilters")}
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {runsLoading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          )}

          {!runsLoading && runs.length === 0 && (
            <p className="text-sm text-muted-foreground text-center py-6">
              {t("profiler.noRuns")}
            </p>
          )}

          {!runsLoading && runs.length > 0 && (
            <>
            <div className="border rounded-lg overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("profiler.tableHeader")} sortKey="table" active={pSortKey === "table"} direction={pSortDir} onSort={handleProfileSort} />
                    </th>
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("common.status")} sortKey="status" active={pSortKey === "status"} direction={pSortDir} onSort={handleProfileSort} />
                    </th>
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("profiler.rowsHeader")} sortKey="rows" active={pSortKey === "rows"} direction={pSortDir} onSort={handleProfileSort} />
                    </th>
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("profiler.durationHeader")} sortKey="duration" active={pSortKey === "duration"} direction={pSortDir} onSort={handleProfileSort} />
                    </th>
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("profiler.startedHeader")} sortKey="started" active={pSortKey === "started"} direction={pSortDir} onSort={handleProfileSort}>
                        <Clock className="h-3.5 w-3.5" />
                      </SortableHeader>
                    </th>
                    <th className="text-left p-3 font-medium">
                      <SortableHeader label={t("profiler.byHeader")} sortKey="by" active={pSortKey === "by"} direction={pSortDir} onSort={handleProfileSort} />
                    </th>
                    <th className="p-3"></th>
                  </tr>
                </thead>
                <tbody>
                  {pagedRuns.map((run) => (
                    <tr
                      key={run.run_id}
                      className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                    >
                      <td className="p-3 font-mono text-xs max-w-xs truncate">
                        {run.source_table_fqn}
                      </td>
                      <td className="p-3">
                        <div className="flex flex-col gap-0.5">
                          <ProfilerStatusBadge status={run.status} />
                          {run.status === "CANCELED" && run.canceled_by && (
                            <span className="text-[10px] text-muted-foreground" title={t("profiler.canceledFor", { table: run.canceled_by })}>
                              {t("profiler.canceledByPrefix")}{run.canceled_by.split("@")[0]}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="p-3 tabular-nums">
                        {run.rows_profiled?.toLocaleString() ?? "—"}
                      </td>
                      <td className="p-3 tabular-nums">
                        {formatDuration(run.duration_seconds)}
                      </td>
                      <td className="p-3 text-muted-foreground text-xs">
                        {formatDate(run.created_at)}
                      </td>
                      <td className="p-3 text-muted-foreground text-xs">
                        {run.requesting_user ?? "—"}
                      </td>
                      <td className="p-3">
                        {run.status === "SUCCESS" && (
                          <Button
                            variant="ghost"
                            size="sm"
                            className="gap-1 h-7 px-2 text-xs"
                            onClick={() => setHistoryRunId(run.run_id)}
                          >
                            <Eye className="h-3 w-3" />
                            {t("profiler.viewBtn")}
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {historyTotalPages > 1 && (
              <div className="flex items-center justify-between pt-3">
                <p className="text-xs text-muted-foreground">
                  Showing {(historyPage - 1) * HISTORY_PAGE_SIZE + 1}–{Math.min(historyPage * HISTORY_PAGE_SIZE, runs.length)} of {runs.length}
                </p>
                <div className="flex items-center gap-1">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 w-8 p-0"
                    disabled={historyPage <= 1}
                    onClick={() => setHistoryPage((p) => p - 1)}
                  >
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                  {Array.from({ length: historyTotalPages }, (_, i) => i + 1)
                    .filter((p) => p === 1 || p === historyTotalPages || Math.abs(p - historyPage) <= 2)
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
                          variant={p === historyPage ? "default" : "outline"}
                          size="sm"
                          className="h-8 w-8 p-0 text-xs"
                          onClick={() => setHistoryPage(p)}
                        >
                          {p}
                        </Button>
                      ),
                    )}
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 w-8 p-0"
                    disabled={historyPage >= historyTotalPages}
                    onClick={() => setHistoryPage((p) => p + 1)}
                  >
                    <ChevronRightIcon className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            )}
            </>
          )}
        </CardContent>
      </Card>

      {/* Historical results dialog */}
      <Dialog open={historyRunId !== null} onOpenChange={(open) => !open && setHistoryRunId(null)}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{t("profiler.profileRunResults")}</DialogTitle>
            <DialogDescription>
              {runs.find((r) => r.run_id === historyRunId)?.source_table_fqn ?? ""}
              {" · "}
              {formatDate(runs.find((r) => r.run_id === historyRunId)?.created_at)}
            </DialogDescription>
          </DialogHeader>
          {historyResultsQuery.isLoading && (
            <div className="space-y-2 py-4">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          )}
          {historyResultsQuery.isError && (
            <div className="flex items-center gap-2 text-sm text-destructive py-4">
              <AlertTriangle className="h-4 w-4" />
              {t("profiler.failedToLoadResults")}
            </div>
          )}
          {historyResultsQuery.data?.data && (
            <ProfileResults
              results={historyResultsQuery.data.data}
              tableFqn={historyResultsQuery.data.data.source_table_fqn}
            />
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Batch table result — collapsible card per table
// ──────────────────────────────────────────────────────────────────────────────

function BatchTableResult({ run }: { run: ActiveBatchRun }) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const tableName = run.tableFqn.split(".").pop() ?? run.tableFqn;

  return (
    <div className="border rounded-lg overflow-hidden">
      <button
        type="button"
        className="w-full flex items-center gap-3 p-3 text-sm hover:bg-muted/30 transition-colors text-left"
        onClick={() => setExpanded((v) => !v)}
      >
        <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />
        <code className="font-mono font-medium flex-1">{tableName}</code>
        <span className="text-muted-foreground text-xs">
          {t("profiler.rulesGenCountAndCount", { rows: run.result?.rows_profiled?.toLocaleString() ?? "?", count: run.result?.generated_rules?.length ?? 0 })}
        </span>
        <ChevronDown
          className={`h-4 w-4 text-muted-foreground transition-transform ${expanded ? "rotate-180" : ""}`}
        />
      </button>
      {expanded && run.result && (
        <div className="border-t p-4">
          <ProfileResults results={run.result} tableFqn={run.tableFqn} />
        </div>
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Profile Results Component
// ──────────────────────────────────────────────────────────────────────────────

function ProfileResults({
  results,
  tableFqn,
}: {
  results: ProfileResultsOut;
  tableFqn: string;
}) {
  const { t } = useTranslation();
  const saveRules = useSaveRules();
  const [added, setAdded] = useState(false);
  const [selectedRules, setSelectedRules] = useState<Set<number>>(new Set());
  const [criticalityFilter, setCriticalityFilter] = useState<"all" | "error" | "warn">("all");

  const { data: existingRulesResp } = useGetRules(tableFqn, {
    query: { enabled: !!tableFqn },
  });
  const existingChecks = (() => {
    const d = existingRulesResp?.data;
    if (!d) return [];
    const arr = Array.isArray(d) ? d : [d];
    return arr.flatMap((e) => e.checks ?? []);
  })();

  const ruleSig = (raw: Record<string, unknown>): string => {
    const inner = (raw.check ?? raw) as Record<string, unknown>;
    const fn = String(inner.function ?? "").toLowerCase();
    const args = (inner.arguments ?? {}) as Record<string, unknown>;
    const col = String(args.column ?? args.col_name ?? inner.for_each_column ?? "").toLowerCase();
    return `${fn}::${col}`;
  };

  const existingRuleSignatures = new Set(
    existingChecks.map((check) => ruleSig(check as Record<string, unknown>)),
  );

  const allRules = results.generated_rules ?? [];

  const isRuleExisting = (rule: Record<string, unknown>): boolean =>
    existingRuleSignatures.has(ruleSig(rule));

  const filteredIndices = allRules
    .map((rule, idx) => ({ rule, idx }))
    .filter(({ rule }) => {
      if (criticalityFilter === "all") return true;
      return String(rule.criticality ?? "warn") === criticalityFilter;
    })
    .map(({ idx }) => idx);

  const handleSelectAll = () => {
    const newRuleIndices = filteredIndices.filter(
      (idx) => !isRuleExisting(allRules[idx] as Record<string, unknown>)
    );
    setSelectedRules(new Set(newRuleIndices));
  };

  const handleSelectNone = () => setSelectedRules(new Set());

  const toggleRule = (idx: number) => {
    if (isRuleExisting(allRules[idx] as Record<string, unknown>)) return;
    setSelectedRules((prev) => {
      const next = new Set(prev);
      next.has(idx) ? next.delete(idx) : next.add(idx);
      return next;
    });
  };

  const handleAddToRules = async () => {
    if (!tableFqn || selectedRules.size === 0) return;
    const rawRules = allRules.filter((_, idx) => selectedRules.has(idx));
    const normalizedRules = rawRules.map((rule) => {
      const inner = (rule.check ?? rule) as Record<string, unknown>;
      const fn = String(inner.function ?? "");
      const rawArgs = (inner.arguments ?? {}) as Record<string, unknown>;
      const args: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(rawArgs)) {
        if (k === "trim_strings") continue;
        args[k] = v;
      }
      // Fold the profiler-suggested weight into user_metadata so the rule
      // round-trips through the same labels-only model used by the rest of
      // the app. ``weight=3`` is omitted (it's the implicit default).
      const userMetadata: Record<string, string> = {};
      const existingMd = rule.user_metadata;
      if (existingMd && typeof existingMd === "object") {
        for (const [k, v] of Object.entries(existingMd as Record<string, unknown>)) {
          if (typeof v === "string") userMetadata[k] = v;
        }
      }
      if (typeof rule.weight === "number" && rule.weight !== 3 && !("weight" in userMetadata)) {
        userMetadata.weight = String(rule.weight);
      }
      const out: Record<string, unknown> = {
        criticality: String(rule.criticality ?? "warn"),
        check: { function: fn, arguments: args },
      };
      if (Object.keys(userMetadata).length > 0) {
        out.user_metadata = userMetadata;
      }
      return out;
    });
    try {
      await saveRules.mutateAsync({ data: { table_fqn: tableFqn, checks: normalizedRules } });
      setAdded(true);
      toast.success(t("profiler.savedAsDrafts", { count: normalizedRules.length, table: tableFqn }));
    } catch {
      toast.error(t("profiler.failedAddRules"));
    }
  };

  const selectedCount = selectedRules.size;
  const existingCount = allRules.filter((rule) => isRuleExisting(rule as Record<string, unknown>)).length;
  const newRulesCount = allRules.length - existingCount;
  const allFilteredSelected =
    filteredIndices.length > 0 &&
    filteredIndices
      .filter((idx) => !isRuleExisting(allRules[idx] as Record<string, unknown>))
      .every((idx) => selectedRules.has(idx));

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {results.rows_profiled?.toLocaleString() ?? "—"}
          </div>
          <div className="text-xs text-muted-foreground">{t("profiler.rowsProfiled")}</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">{results.columns_profiled ?? "—"}</div>
          <div className="text-xs text-muted-foreground">{t("profiler.columnsLabel")}</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {results.duration_seconds != null ? formatDuration(results.duration_seconds) : "—"}
          </div>
          <div className="text-xs text-muted-foreground">{t("profiler.durationLabel")}</div>
        </div>
      </div>

      {allRules.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium flex items-center gap-1.5">
              <CheckCircle2 className="h-4 w-4 text-green-500" />
              {t("profiler.generatedRules", { count: allRules.length })}
            </h4>
            <div className="flex items-center gap-2">
              {added && (
                <Button size="sm" variant="default" className="gap-1.5" asChild>
                  <Link to="/rules/drafts">{t("profiler.viewInDrafts")}</Link>
                </Button>
              )}
              <Button
                size="sm"
                variant={added ? "outline" : "default"}
                className="gap-1.5"
                onClick={handleAddToRules}
                disabled={saveRules.isPending || added || selectedCount === 0}
              >
                {saveRules.isPending ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : added ? (
                  <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
                ) : (
                  <Plus className="h-3.5 w-3.5" />
                )}
                {added ? t("profiler.savedToDrafts") : t("profiler.addSelected", { count: selectedCount })}
              </Button>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <div className="flex items-center gap-1 border rounded-md p-0.5">
              {(["all", "error", "warn"] as const).map((f) => (
                <button
                  key={f}
                  type="button"
                  onClick={() => setCriticalityFilter(f)}
                  className={`px-2 py-1 text-xs rounded transition-colors ${
                    criticalityFilter === f
                      ? f === "all"
                        ? "bg-primary text-primary-foreground"
                        : f === "error"
                          ? "bg-destructive text-destructive-foreground"
                          : "bg-yellow-500 text-white"
                      : "hover:bg-muted"
                  }`}
                >
                  {f === "all" ? t("common.all") : f === "error" ? t("common.error") : t("common.warning")}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-1">
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={handleSelectAll}
                disabled={added || allFilteredSelected || newRulesCount === 0}
              >
                {t("common.selectAll")}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={handleSelectNone}
                disabled={added || selectedCount === 0}
              >
                {t("common.selectNone")}
              </Button>
            </div>
            <span className="text-xs text-muted-foreground ml-auto">
              {t("profiler.selectedOf", { selected: selectedCount, newCount: newRulesCount })}
              {existingCount > 0 && (
                <span className="ml-1 text-green-600">{t("profiler.alreadyInCatalog", { count: existingCount })}</span>
              )}
            </span>
          </div>

          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="w-8 p-2"></th>
                  <th className="text-left p-2 font-medium">{t("profiler.headerFunction")}</th>
                  <th className="text-left p-2 font-medium">{t("profiler.headerColumn")}</th>
                  <th className="text-left p-2 font-medium">{t("profiler.headerCriticality")}</th>
                </tr>
              </thead>
              <tbody>
                {allRules.map((rule, idx) => {
                  const check = (rule.check as Record<string, unknown>) ?? {};
                  const args = (check.arguments as Record<string, unknown>) ?? {};
                  const criticality = String(rule.criticality ?? "warn");
                  const isVisible = criticalityFilter === "all" || criticality === criticalityFilter;
                  const ruleExists = isRuleExisting(rule as Record<string, unknown>);

                  if (!isVisible) return null;

                  return (
                    <tr
                      key={idx}
                      className={`border-b last:border-b-0 transition-colors ${
                        ruleExists
                          ? "bg-green-500/10 opacity-60"
                          : selectedRules.has(idx)
                            ? "bg-primary/10 cursor-pointer"
                            : "hover:bg-muted/50 cursor-pointer"
                      }`}
                      onClick={() => !added && !ruleExists && toggleRule(idx)}
                    >
                      <td className="p-2 text-center">
                        {ruleExists ? (
                          <CheckCircle2 className="h-3.5 w-3.5 text-green-600 mx-auto" />
                        ) : (
                          <input
                            type="checkbox"
                            checked={selectedRules.has(idx)}
                            onChange={() => toggleRule(idx)}
                            disabled={added}
                            className="h-3.5 w-3.5 rounded border-gray-300 cursor-pointer"
                            onClick={(e) => e.stopPropagation()}
                          />
                        )}
                      </td>
                      <td className="p-2 font-mono">
                        {String(check.function ?? "—")}
                        {ruleExists && (
                          <Badge variant="outline" className="ml-2 text-[10px] py-0 px-1 text-green-600 border-green-600">
                            {t("profiler.addedBadge")}
                          </Badge>
                        )}
                      </td>
                      <td className="p-2">{String(args.column ?? check.for_each_column ?? "—")}</td>
                      <td className="p-2">
                        <Badge variant={criticality === "error" ? "destructive" : "secondary"}>
                          {criticality}
                        </Badge>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {allRules.length === 0 && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <AlertTriangle className="h-4 w-4" />
          {t("profiler.noRulesGenerated")}
        </div>
      )}
    </div>
  );
}
