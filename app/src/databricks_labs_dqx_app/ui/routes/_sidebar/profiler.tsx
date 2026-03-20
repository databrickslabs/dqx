import { createFileRoute } from "@tanstack/react-router";
import { useState, useCallback } from "react";
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
} from "lucide-react";
import { toast } from "sonner";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useJobPolling } from "@/hooks/use-job-polling";
import {
  useSubmitProfileRun,
  useListProfileRuns,
  useGetProfileRunResults,
  useSaveRules,
  useGetTableColumns,
  getProfileRunStatus,
  type ProfileResultsOut,
  type ProfileRunSummaryOut,
} from "@/lib/api";

export const Route = createFileRoute("/_sidebar/profiler")({
  component: ProfilerPage,
});

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

function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

/** Estimate ETA in seconds from prior runs on the same table. */
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

/** Parse "catalog.schema.table" into parts. Returns null if not 3 parts. */
function parseTableFqn(fqn: string): { catalog: string; schema: string; table: string } | null {
  const parts = fqn.split(".");
  if (parts.length !== 3) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
}

function ProfilerPage() {
  const [tableFqn, setTableFqn] = useState("");
  const [sampleLimit, setSampleLimit] = useState(50_000);
  const [jobRunId, setJobRunId] = useState<number | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [results, setResults] = useState<ProfileResultsOut | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [startedAt, setStartedAt] = useState<number | null>(null);

  // Advanced options
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [filterSql, setFilterSql] = useState("");
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [llmPkDetection, setLlmPkDetection] = useState(false);
  const [removeOutliers, setRemoveOutliers] = useState(true);
  const [numSigmas, setNumSigmas] = useState(3);

  // Historical results dialog
  const [historyRunId, setHistoryRunId] = useState<string | null>(null);

  const hasTable = tableFqn.split(".").length === 3;
  const tableParts = hasTable ? parseTableFqn(tableFqn) : null;

  const submitMutation = useSubmitProfileRun();
  const { data: runsResp, isLoading: runsLoading, refetch: refetchRuns } = useListProfileRuns();
  const runs: ProfileRunSummaryOut[] = runsResp?.data ?? [];

  // Columns for the selected table (used in advanced options)
  const { data: columnsResp } = useGetTableColumns(
    tableParts?.catalog ?? "",
    tableParts?.schema ?? "",
    tableParts?.table ?? "",
    { query: { enabled: hasTable } },
  );
  const availableColumns = columnsResp?.data?.map((c) => c.name) ?? [];

  const resultsQuery = useGetProfileRunResults(runId ?? "", {
    query: { enabled: false },
  });

  const historyResultsQuery = useGetProfileRunResults(historyRunId ?? "", {
    query: { enabled: historyRunId !== null },
  });

  const fetchStatus = useCallback(async () => {
    if (!runId || jobRunId === null) throw new Error("No active run");
    const resp = await getProfileRunStatus(runId, { job_run_id: jobRunId });
    if (startedAt) setElapsedSeconds(Math.round((Date.now() - startedAt) / 1000));
    return resp.data;
  }, [runId, jobRunId, startedAt]);

  const polling = useJobPolling({
    fetchStatus,
    enabled: jobRunId !== null && runId !== null,
    interval: 3000,
    onComplete: async (status) => {
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
      setStartedAt(null);
      refetchRuns();
    },
    onError: () => {
      toast.error("Failed to check profiler status");
    },
  });

  const handleRun = async () => {
    if (!hasTable) {
      toast.error("Select a table first");
      return;
    }
    try {
      setResults(null);
      setElapsedSeconds(0);

      const profileOptions: Record<string, unknown> = {
        remove_outliers: removeOutliers,
        num_sigmas: numSigmas,
        llm_primary_key_detection: llmPkDetection,
      };
      if (filterSql.trim()) profileOptions.filter = filterSql.trim();

      const resp = await submitMutation.mutateAsync({
        data: {
          table_fqn: tableFqn,
          sample_limit: sampleLimit,
          columns: selectedColumns.length > 0 ? selectedColumns : undefined,
          profile_options: profileOptions,
        },
      });
      setRunId(resp.data.run_id);
      setJobRunId(resp.data.job_run_id);
      setStartedAt(Date.now());
      toast.info("Profiling job submitted — waiting for results...");
    } catch {
      toast.error("Failed to submit profiling job");
    }
  };

  const isRunning = submitMutation.isPending || polling.isPolling;
  const etaSeconds = isRunning ? estimateEtaSeconds(tableFqn, sampleLimit, runs) : null;

  const toggleColumn = (col: string) => {
    setSelectedColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col],
    );
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb items={[]} page="Profiler" />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Data Profiler</h1>
          <p className="text-muted-foreground">
            Profile a table to generate data quality rule suggestions based on data distribution.
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
            Select a table, configure sampling, and run the profiler.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <CatalogBrowser value={tableFqn} onChange={setTableFqn} disabled={isRunning} />

          {tableFqn && hasTable && (
            <p className="text-sm text-muted-foreground">
              Selected:{" "}
              <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">
                {tableFqn}
              </code>
            </p>
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
                disabled={isRunning}
                min={1}
                max={100_000}
              />
              <p className="text-xs text-muted-foreground">Max 100,000 rows</p>
            </div>

            <Button
              onClick={handleRun}
              disabled={!hasTable || isRunning}
              className="gap-2 mb-6"
            >
              {isRunning ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              {isRunning ? "Profiling..." : "Run Profiler"}
            </Button>
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
            {advancedOpen && <div className="mt-3 space-y-4 rounded-lg border p-4">
              {/* SQL filter */}
              <div className="grid gap-2">
                <Label htmlFor="filter-sql">Row Filter (SQL WHERE clause)</Label>
                <Input
                  id="filter-sql"
                  placeholder="e.g. status = 'active' AND year >= 2024"
                  value={filterSql}
                  onChange={(e) => setFilterSql(e.target.value)}
                  disabled={isRunning}
                />
                <p className="text-xs text-muted-foreground">
                  Optional SQL condition applied before profiling.
                </p>
              </div>

              {/* Column selection */}
              {availableColumns.length > 0 && (
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
                        disabled={isRunning}
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

              {/* Outlier removal */}
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
                  disabled={isRunning}
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
                    disabled={isRunning}
                    min={1}
                    max={10}
                    step={0.5}
                  />
                  <p className="text-xs text-muted-foreground">
                    Standard deviations from mean to consider an outlier (default: 3).
                  </p>
                </div>
              )}

              {/* LLM primary key detection */}
              <div className="flex items-center justify-between">
                <div className="grid gap-0.5">
                  <Label htmlFor="llm-pk">LLM Primary Key Detection</Label>
                  <p className="text-xs text-muted-foreground">
                    Use AI to detect primary key columns and generate uniqueness checks.
                    Requires the LLM optional dependency.
                  </p>
                </div>
                <Switch
                  id="llm-pk"
                  checked={llmPkDetection}
                  onCheckedChange={setLlmPkDetection}
                  disabled={isRunning}
                />
              </div>
            </div>}
          </div>

          {/* Active run progress */}
          {isRunning && (
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
              </div>
            </div>
          )}

          {/* Results for the current run */}
          {results && (
            <>
              <Separator />
              <ProfileResults results={results} tableFqn={results.source_table_fqn} />
            </>
          )}
        </CardContent>
      </Card>

      {/* Past runs */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <History className="h-5 w-5" />
            Run History
          </CardTitle>
          <CardDescription>Previous profiling runs, newest first. Click a successful run to view its results.</CardDescription>
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
                      <td className="p-3">{statusBadge(run.status)}</td>
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

function ProfileResults({
  results,
  tableFqn,
}: {
  results: ProfileResultsOut;
  tableFqn: string;
}) {
  const saveRules = useSaveRules();
  const [added, setAdded] = useState(false);

  const handleAddToRules = async () => {
    const rules = results.generated_rules ?? [];
    if (!tableFqn || !rules.length) return;
    try {
      await saveRules.mutateAsync({
        data: { table_fqn: tableFqn, checks: rules },
      });
      setAdded(true);
      toast.success(`${rules.length} rules added for ${tableFqn}`);
    } catch {
      toast.error("Failed to add rules");
    }
  };

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
          <div className="text-2xl font-bold tabular-nums">
            {results.columns_profiled ?? "—"}
          </div>
          <div className="text-xs text-muted-foreground">Columns</div>
        </div>
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {results.duration_seconds != null ? formatDuration(results.duration_seconds) : "—"}
          </div>
          <div className="text-xs text-muted-foreground">Duration</div>
        </div>
      </div>

      {(results.generated_rules?.length ?? 0) > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-medium flex items-center gap-1.5">
              <CheckCircle2 className="h-4 w-4 text-green-500" />
              Generated Rules ({(results.generated_rules ?? []).length})
            </h4>
            <Button
              size="sm"
              variant={added ? "outline" : "default"}
              className="gap-1.5"
              onClick={handleAddToRules}
              disabled={saveRules.isPending || added}
            >
              {saveRules.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : added ? (
                <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
              ) : (
                <Plus className="h-3.5 w-3.5" />
              )}
              {added ? "Added to Rules" : "Add to Rules"}
            </Button>
          </div>
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left p-2 font-medium">Function</th>
                  <th className="text-left p-2 font-medium">Column</th>
                  <th className="text-left p-2 font-medium">Criticality</th>
                </tr>
              </thead>
              <tbody>
                {(results.generated_rules ?? []).map((rule, idx) => {
                  const check = (rule.check as Record<string, unknown>) ?? {};
                  const args = (check.arguments as Record<string, unknown>) ?? {};
                  return (
                    <tr key={idx} className="border-b last:border-b-0">
                      <td className="p-2 font-mono">{String(check.function ?? "—")}</td>
                      <td className="p-2">
                        {String(args.column ?? check.for_each_column ?? "—")}
                      </td>
                      <td className="p-2">
                        <Badge
                          variant={
                            String(rule.criticality) === "error" ? "destructive" : "secondary"
                          }
                        >
                          {String(rule.criticality ?? "warn")}
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

      {(results.generated_rules?.length ?? 0) === 0 && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <AlertTriangle className="h-4 w-4" />
          No rules were generated from the profiling data.
        </div>
      )}
    </div>
  );
}
