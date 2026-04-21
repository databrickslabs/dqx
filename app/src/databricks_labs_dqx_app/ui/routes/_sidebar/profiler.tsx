import { createFileRoute, Link, Navigate } from "@tanstack/react-router";
import { formatDateTime as formatDate } from "@/lib/format-utils";
import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
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
  BarChart3,
  Play,
  Loader2,
  CheckCircle2,
  AlertTriangle,
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

function statusBadge(status: string | null | undefined) {
  switch (status) {
    case "SUCCESS":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          Success
        </Badge>
      );
    case "FAILED":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          Failed
        </Badge>
      );
    case "RUNNING":
      return (
        <Badge variant="outline" className="gap-1 border-blue-500 text-blue-600">
          <Loader2 className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status ?? "Unknown"}</Badge>;
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

// ──────────────────────────────────────────────────────────────────────────────
// Main Page Component
// ──────────────────────────────────────────────────────────────────────────────

function ProfilerPage() {
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

  const submitMutation = useSubmitProfileRun();
  const batchSubmitMutation = useSubmitBatchProfileRun();
  const { data: runsResp, isLoading: runsLoading, refetch: refetchRuns } = useListProfileRuns();
  const allRuns: ProfileRunSummaryOut[] = runsResp?.data ?? [];
  const runs = useMemo(() => {
    if (!myRunsOnly || !currentUserEmail) return allRuns;
    return allRuns.filter((r) => r.requesting_user === currentUserEmail);
  }, [allRuns, myRunsOnly, currentUserEmail]);

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
    const resp = await getProfileRunStatus(runId, {
      job_run_id: jobRunId,
      view_fqn: viewFqn ?? undefined,
    });
    if (startedAt) setElapsedSeconds(Math.round((Date.now() - startedAt) / 1000));
    return resp.data;
  }, [runId, jobRunId, startedAt, viewFqn]);

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
          toast.success("Profiling complete");
        } catch {
          toast.error("Failed to fetch profiler results");
        }
      } else {
        toast.error(`Profiling failed: ${status.message || "Unknown error"}`);
      }
      setJobRunId(null);
      setViewFqn(null);
      setStartedAt(null);
      refetchRuns();
    },
    onError: () => {
      toast.error("Failed to check profiler status");
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
            const resp = await getProfileRunStatus(run.runId, {
              job_run_id: run.jobRunId,
              view_fqn: run.viewFqn,
            });
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
      toast.error("Select a table first");
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
      toast.info("Profiling job submitted — waiting for results...");
    } catch {
      toast.error("Failed to submit profiling job");
    }
  };

  const handleBatchRun = async () => {
    if (selectedTables.length === 0) { toast.error("Select at least one table"); return; }
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
      const newRuns: ActiveBatchRun[] = resp.data.runs.map((run, i) => ({
        runId: run.run_id,
        jobRunId: run.job_run_id,
        viewFqn: run.view_fqn,
        tableFqn: selectedTables[i],
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
      toast.info(`${newRuns.length} profiling jobs submitted`);
    } catch {
      toast.error("Failed to submit batch profiling jobs");
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
      toast.info("Profiling run canceled");
      polling.stopPolling();
      removeStoredRun(runId);
      setJobRunId(null);
      setViewFqn(null);
      setStartedAt(null);
      refetchRuns();
    } catch {
      toast.error("Failed to cancel run");
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
            ? { ...r, state: "failed" as const, message: "Canceled by user" }
            : r,
        ),
      );
      toast.info(`Canceled profiling for ${run.tableFqn.split(".").pop()}`);
    } catch {
      toast.error("Failed to cancel run");
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
      toast.error("Select at least one table");
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
        <PageBreadcrumb items={[{ label: "Create Rules", to: "/rules/create" }]} page="Profile & Generate" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Profile & Generate</h1>
          <p className="text-muted-foreground">
            Profile tables to generate data quality rule suggestions based on data distribution.
          </p>
        </div>
      </div>

      {/* New run form */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            New Profile Run
          </CardTitle>
          <CardDescription>
            Select one or more tables (or an entire schema), configure sampling, and run the profiler.
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
                {selectedTables.length} table{selectedTables.length !== 1 ? "s" : ""} selected
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
              <Label htmlFor="sample-limit">Sample Limit (rows)</Label>
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
              <p className="text-xs text-muted-foreground">Max 100,000 rows per table</p>
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
                  ? `Stop All (${batchCompleted}/${batchTotal})`
                  : "Stop Run"}
              </Button>
            ) : (
              <Button
                onClick={handleProfileRun}
                disabled={selectedTables.length === 0}
                className="gap-2 mb-6"
              >
                <Play className="h-4 w-4" />
                {selectedTables.length > 1
                  ? `Run ${selectedTables.length} Tables`
                  : "Run Profile"}
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
              Advanced Options
              <ChevronDown
                className={`h-3 w-3 transition-transform ${advancedOpen ? "rotate-180" : ""}`}
              />
            </Button>
            {advancedOpen && (
              <div className="mt-3 space-y-4 rounded-lg border p-4">
                <div className="grid gap-2">
                  <Label htmlFor="filter-sql">Row Filter (SQL WHERE clause)</Label>
                  <Input
                    id="filter-sql"
                    placeholder="e.g. status = 'active' AND year >= 2024"
                    value={filterSql}
                    onChange={(e) => setFilterSql(e.target.value)}
                    disabled={isBusy}
                  />
                  <p className="text-xs text-muted-foreground">
                    Optional SQL condition applied before profiling.
                    {selectedTables.length > 1 && " Applied to all selected tables."}
                  </p>
                </div>

                {selectedTables.length === 1 && availableColumns.length > 0 && (
                  <div className="grid gap-2">
                    <Label>
                      Columns to Profile
                      {selectedColumns.length > 0 && (
                        <span className="ml-2 text-xs text-muted-foreground">
                          ({selectedColumns.length} selected)
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
                        Clear selection (profile all)
                      </button>
                    )}
                    <p className="text-xs text-muted-foreground">
                      Click to toggle. No selection = profile all columns.
                    </p>
                  </div>
                )}

                <div className="flex items-center justify-between">
                  <div className="grid gap-0.5">
                    <Label htmlFor="remove-outliers">Remove Outliers</Label>
                    <p className="text-xs text-muted-foreground">
                      Exclude statistical outliers when computing min/max range checks.
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
                    <Label htmlFor="num-sigmas">Outlier Threshold (σ)</Label>
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
                      Standard deviations from mean to consider an outlier (default: 3).
                    </p>
                  </div>
                )}

                <div className="flex items-center justify-between">
                  <div className="grid gap-0.5">
                    <Label htmlFor="llm-pk">LLM Primary Key Detection</Label>
                    <p className="text-xs text-muted-foreground">
                      Use AI to detect primary key columns and generate uniqueness checks.
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
                  {polling.status?.state ?? "Submitting..."}
                </span>
                <span className="text-muted-foreground tabular-nums ml-auto">
                  {elapsedSeconds}s elapsed
                  {etaSeconds != null && ` · ~${formatDuration(etaSeconds)} estimated`}
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
                  Stop
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
                    ? `Profiling in progress — ${batchCompleted} of ${batchTotal} complete`
                    : `Batch complete — ${batchSucceeded} succeeded${batchFailed > 0 ? `, ${batchFailed} failed` : ""}`}
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
                      Stop All
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
                        ? "running"
                        : run.state === "success"
                          ? run.result
                            ? `${run.result.rows_profiled?.toLocaleString() ?? "?"} rows · ${run.result.generated_rules?.length ?? 0} rules`
                            : "done"
                          : run.message ?? "failed"}
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
                <h3 className="text-sm font-semibold">Results by Table</h3>
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
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <History className="h-5 w-5" />
                Run History
              </CardTitle>
              <CardDescription>
                {runsLoading
                  ? "Loading..."
                  : `${runs.length} run${runs.length !== 1 ? "s" : ""}${
                      myRunsOnly && runs.length !== allRuns.length ? ` (filtered from ${allRuns.length})` : ""
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
                My runs
              </Button>
              <Button variant="ghost" size="sm" onClick={() => refetchRuns()} className="h-8 gap-1.5 text-xs">
                <Clock className="h-3.5 w-3.5" />
                Refresh
              </Button>
            </div>
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
              No profiling runs yet. Run the profiler above to get started.
            </p>
          )}

          {!runsLoading && runs.length > 0 && (
            <div className="border rounded-lg overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 font-medium">Table</th>
                    <th className="text-left p-3 font-medium">Status</th>
                    <th className="text-left p-3 font-medium">Rows</th>
                    <th className="text-left p-3 font-medium">Duration</th>
                    <th className="text-left p-3 font-medium">
                      <Clock className="h-3.5 w-3.5 inline mr-1" />
                      Started
                    </th>
                    <th className="text-left p-3 font-medium">By</th>
                    <th className="p-3"></th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr
                      key={run.run_id}
                      className="border-b last:border-b-0 hover:bg-muted/30 transition-colors"
                    >
                      <td className="p-3 font-mono text-xs max-w-xs truncate">
                        {run.source_table_fqn}
                      </td>
                      <td className="p-3">
                        <div className="flex flex-col gap-0.5">
                          {statusBadge(run.status)}
                          {run.status === "CANCELED" && run.canceled_by && (
                            <span className="text-[10px] text-muted-foreground" title={`Canceled by ${run.canceled_by}`}>
                              by {run.canceled_by.split("@")[0]}
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
                            View
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Historical results dialog */}
      <Dialog open={historyRunId !== null} onOpenChange={(open) => !open && setHistoryRunId(null)}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Profile Run Results</DialogTitle>
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
              Failed to load results.
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
          {run.result?.rows_profiled?.toLocaleString() ?? "?"} rows ·{" "}
          {run.result?.generated_rules?.length ?? 0} rules generated
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
      return {
        criticality: String(rule.criticality ?? "warn"),
        weight: typeof rule.weight === "number" ? rule.weight : 3,
        check: { function: fn, arguments: args },
      };
    });
    try {
      await saveRules.mutateAsync({ data: { table_fqn: tableFqn, checks: normalizedRules } });
      setAdded(true);
      toast.success(`${normalizedRules.length} rules saved as drafts for ${tableFqn}`);
    } catch {
      toast.error("Failed to add rules");
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
          <div className="text-xs text-muted-foreground">Rows Profiled</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">{results.columns_profiled ?? "—"}</div>
          <div className="text-xs text-muted-foreground">Columns</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {results.duration_seconds != null ? formatDuration(results.duration_seconds) : "—"}
          </div>
          <div className="text-xs text-muted-foreground">Duration</div>
        </div>
      </div>

      {allRules.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium flex items-center gap-1.5">
              <CheckCircle2 className="h-4 w-4 text-green-500" />
              Generated Rules ({allRules.length})
            </h4>
            <div className="flex items-center gap-2">
              {added && (
                <Button size="sm" variant="default" className="gap-1.5" asChild>
                  <Link to="/rules/drafts">View in Drafts & Review</Link>
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
                {added ? "Saved to Drafts" : `Add ${selectedCount} Selected`}
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
                  {f === "all" ? "All" : f === "error" ? "Error" : "Warning"}
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
                Select All
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={handleSelectNone}
                disabled={added || selectedCount === 0}
              >
                Select None
              </Button>
            </div>
            <span className="text-xs text-muted-foreground ml-auto">
              {selectedCount} of {newRulesCount} new selected
              {existingCount > 0 && (
                <span className="ml-1 text-green-600">({existingCount} already in catalog)</span>
              )}
            </span>
          </div>

          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="w-8 p-2"></th>
                  <th className="text-left p-2 font-medium">Function</th>
                  <th className="text-left p-2 font-medium">Column</th>
                  <th className="text-left p-2 font-medium">Criticality</th>
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
                            Added
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
          No rules were generated from the profiling data.
        </div>
      )}
    </div>
  );
}
