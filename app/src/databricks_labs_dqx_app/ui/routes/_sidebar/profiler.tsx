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
import {
  BarChart3,
  Play,
  Loader2,
  CheckCircle2,
  AlertTriangle,
  Clock,
  XCircle,
  History,
} from "lucide-react";
import { toast } from "sonner";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useJobPolling } from "@/hooks/use-job-polling";
import {
  useSubmitProfileRun,
  useListProfileRuns,
  getProfileRunStatus,
  useGetProfileRunResults,
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

  // Average seconds per row from prior successful runs
  const avgSecsPerRow =
    priorRuns.reduce((sum, r) => sum + r.duration_seconds! / r.rows_profiled!, 0) /
    priorRuns.length;

  return Math.round(avgSecsPerRow * sampleLimit);
}

function ProfilerPage() {
  const [tableFqn, setTableFqn] = useState("");
  const [sampleLimit, setSampleLimit] = useState(50_000);
  const [jobRunId, setJobRunId] = useState<number | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [results, setResults] = useState<ProfileResultsOut | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [startedAt, setStartedAt] = useState<number | null>(null);
  const hasTable = tableFqn.split(".").length === 3;

  const submitMutation = useSubmitProfileRun();
  const { data: runsResp, isLoading: runsLoading, refetch: refetchRuns } = useListProfileRuns();
  const runs: ProfileRunSummaryOut[] = runsResp?.data ?? [];

  const resultsQuery = useGetProfileRunResults(runId ?? "", {
    query: { enabled: false },
  });

  const fetchStatus = useCallback(async () => {
    if (!runId || jobRunId === null) throw new Error("No active run");
    const resp = await getProfileRunStatus(runId, { job_run_id: jobRunId });
    // Update elapsed time
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
      const resp = await submitMutation.mutateAsync({
        data: { table_fqn: tableFqn, sample_limit: sampleLimit },
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
              <ProfileResults results={results} />
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
          <CardDescription>Previous profiling runs, newest first.</CardDescription>
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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function ProfileResults({ results }: { results: ProfileResultsOut }) {
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
          <h4 className="text-sm font-medium flex items-center gap-1.5">
            <CheckCircle2 className="h-4 w-4 text-green-500" />
            Generated Rules ({results.generated_rules.length})
          </h4>
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
                {results.generated_rules.map((rule, idx) => {
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
