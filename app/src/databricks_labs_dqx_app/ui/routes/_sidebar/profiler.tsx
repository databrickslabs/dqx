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
import {
  BarChart3,
  Play,
  Loader2,
  CheckCircle2,
  AlertTriangle,
} from "lucide-react";
import { toast } from "sonner";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { useJobPolling } from "@/hooks/use-job-polling";
import {
  useSubmitProfileRun,
  getProfileRunStatus,
  useGetProfileRunResults,
  type ProfileResultsOut,
} from "@/lib/api";

export const Route = createFileRoute("/_sidebar/profiler")({
  component: ProfilerPage,
});

function ProfilerPage() {
  const [tableFqn, setTableFqn] = useState("");
  const [sampleLimit, setSampleLimit] = useState(50_000);
  const [jobRunId, setJobRunId] = useState<number | null>(null);
  const [runId, setRunId] = useState<string | null>(null);
  const [results, setResults] = useState<ProfileResultsOut | null>(null);
  const hasTable = tableFqn.split(".").length === 3;

  const submitMutation = useSubmitProfileRun();
  const resultsQuery = useGetProfileRunResults(runId ?? "", {
    query: { enabled: false },
  });

  const fetchStatus = useCallback(async () => {
    if (!runId || jobRunId === null) throw new Error("No active run");
    const resp = await getProfileRunStatus(runId, { job_run_id: jobRunId });
    return resp.data;
  }, [runId, jobRunId]);

  const polling = useJobPolling({
    fetchStatus,
    enabled: jobRunId !== null && runId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (status.result_state === "SUCCESS") {
        try {
          const resp = await resultsQuery.refetch();
          if (resp.data?.data) {
            setResults(resp.data.data);
          }
          toast.success("Profiling complete");
        } catch {
          toast.error("Failed to fetch profiler results");
        }
      } else {
        toast.error(`Profiling failed: ${status.message || "Unknown error"}`);
      }
      setJobRunId(null);
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
      const resp = await submitMutation.mutateAsync({
        data: {
          table_fqn: tableFqn,
          sample_limit: sampleLimit,
        },
      });
      setRunId(resp.data.run_id);
      setJobRunId(resp.data.job_run_id);
      toast.info("Profiling job submitted — waiting for results...");
    } catch {
      toast.error("Failed to submit profiling job");
    }
  };

  const isRunning = submitMutation.isPending || polling.isPolling;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb
          items={[]}
          page="Profiler"
        />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Data Profiler</h1>
          <p className="text-muted-foreground">
            Profile a table to generate data quality rule suggestions based on the data distribution.
          </p>
        </div>
      </div>

      {/* Table selection */}
      <Card>
        <CardHeader>
          <CardTitle>1. Select Table</CardTitle>
          <CardDescription>
            Choose the table you want to profile.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <CatalogBrowser
            value={tableFqn}
            onChange={setTableFqn}
            disabled={isRunning}
          />
          {tableFqn && hasTable && (
            <p className="text-sm text-muted-foreground">
              Selected: <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">{tableFqn}</code>
            </p>
          )}
        </CardContent>
      </Card>

      {/* Configuration */}
      <Card>
        <CardHeader>
          <CardTitle>2. Configure Sampling</CardTitle>
          <CardDescription>
            Adjust how many rows to sample for profiling.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-2 max-w-xs">
            <Label htmlFor="sample-limit">Sample Limit (rows)</Label>
            <Input
              id="sample-limit"
              type="number"
              value={sampleLimit}
              onChange={(e) => setSampleLimit(Math.min(100_000, Math.max(1, Number(e.target.value))))}
              disabled={isRunning}
              min={1}
              max={100_000}
            />
            <p className="text-xs text-muted-foreground">Max 100,000 rows</p>
          </div>
        </CardContent>
      </Card>

      {/* Run profiler */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            3. Run Profiler
          </CardTitle>
          <CardDescription>
            Profile the table to generate rule suggestions.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <Button
            onClick={handleRun}
            disabled={!hasTable || isRunning}
            className="gap-2"
          >
            {isRunning ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            {isRunning ? "Profiling..." : "Run Profiler"}
          </Button>

          {/* Polling progress */}
          {isRunning && polling.status && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              <span>
                Job status: <span className="font-medium">{polling.status.state}</span>
              </span>
            </div>
          )}

          {/* Results */}
          {results && (
            <>
              <Separator />
              <ProfileResults results={results} />
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function ProfileResults({ results }: { results: ProfileResultsOut }) {
  return (
    <div className="space-y-4">
      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg border p-3 text-center">
          <div className="text-2xl font-bold tabular-nums">
            {results.rows_profiled ?? "—"}
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
            {results.duration_seconds != null ? `${results.duration_seconds}s` : "—"}
          </div>
          <div className="text-xs text-muted-foreground">Duration</div>
        </div>
      </div>

      {/* Generated rules */}
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
                {(results.generated_rules ?? []).map((rule, idx) => {
                  const check = (rule.check as Record<string, unknown>) ?? {};
                  const args = (check.arguments as Record<string, unknown>) ?? {};
                  return (
                    <tr key={idx} className="border-b last:border-b-0">
                      <td className="p-2 font-mono">
                        {String(check.function ?? "—")}
                      </td>
                      <td className="p-2">
                        {String(args.column ?? check.for_each_column ?? "—")}
                      </td>
                      <td className="p-2">
                        <Badge variant={String(rule.criticality) === "error" ? "destructive" : "secondary"}>
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
