import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useState, useEffect, useCallback } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Sparkles,
  Play,
  Save,
  Loader2,
  ArrowLeft,
  AlertCircle,
  FileEdit,
  Clock,
  CheckCircle2,
  XCircle,
  Info,
} from "lucide-react";
import { toast } from "sonner";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { RulesReview } from "@/components/RulesReview";
import { DryRunResults } from "@/components/DryRunResults";
import {
  useAiAssistedChecksGeneration,
  useSubmitDryRun,
  useGetDryRunResults,
  useSaveRules,
  useSubmitRulesForApproval,
  useGetRules,
  getDryRunStatus,
  type DryRunResultsOut,
  type RuleCatalogEntryOut,
} from "@/lib/api";
import { useJobPolling } from "@/hooks/use-job-polling";

type SearchParams = {
  table?: string;
};

export const Route = createFileRoute("/_sidebar/rules/generate")({
  component: GenerateRulesPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    table: (search.table as string) || undefined,
  }),
});

function statusBadge(status: string) {
  switch (status) {
    case "draft":
      return (
        <Badge variant="secondary" className="gap-1">
          <FileEdit className="h-3 w-3" />
          Draft
        </Badge>
      );
    case "pending_approval":
      return (
        <Badge variant="outline" className="gap-1 border-amber-500 text-amber-600">
          <Clock className="h-3 w-3" />
          Pending
        </Badge>
      );
    case "approved":
      return (
        <Badge variant="outline" className="gap-1 border-green-500 text-green-600">
          <CheckCircle2 className="h-3 w-3" />
          Approved
        </Badge>
      );
    case "rejected":
      return (
        <Badge variant="outline" className="gap-1 border-red-500 text-red-600">
          <XCircle className="h-3 w-3" />
          Rejected
        </Badge>
      );
    default:
      return <Badge variant="secondary">{status}</Badge>;
  }
}

function GenerateRulesPage() {
  const navigate = useNavigate();
  const { table: initialTable } = Route.useSearch();

  const [tableFqn, setTableFqn] = useState(initialTable ?? "");
  const [userInput, setUserInput] = useState("");
  const [checks, setChecks] = useState<Record<string, unknown>[]>([]);
  const [dryRunResult, setDryRunResult] = useState<DryRunResultsOut | null>(null);
  const [existingEntry, setExistingEntry] = useState<RuleCatalogEntryOut | null>(null);
  const [dryRunJobRunId, setDryRunJobRunId] = useState<number | null>(null);
  const [dryRunRunId, setDryRunRunId] = useState<string | null>(null);

  const hasTable = tableFqn.split(".").length === 3;

  const {
    data: rulesResp,
    isLoading: isLoadingRules,
  } = useGetRules(tableFqn, {
    query: { enabled: hasTable },
  });

  useEffect(() => {
    if (rulesResp?.data) {
      const entry = rulesResp.data;
      setExistingEntry(entry);
      if (checks.length === 0) {
        setChecks(entry.checks);
      }
    }
  }, [rulesResp]); // eslint-disable-line react-hooks/exhaustive-deps

  const isEditMode = existingEntry !== null;

  const generateMutation = useAiAssistedChecksGeneration();
  const submitDryRunMutation = useSubmitDryRun();
  const saveMutation = useSaveRules();
  const submitMutation = useSubmitRulesForApproval();

  // Fetch dry-run results when job completes
  const dryRunResultsQuery = useGetDryRunResults(dryRunRunId ?? "", {
    query: { enabled: false },
  });

  const fetchDryRunStatus = useCallback(async () => {
    if (!dryRunRunId || dryRunJobRunId === null) throw new Error("No active run");
    const resp = await getDryRunStatus(dryRunRunId, { job_run_id: dryRunJobRunId });
    return resp.data;
  }, [dryRunRunId, dryRunJobRunId]);

  const dryRunPolling = useJobPolling({
    fetchStatus: fetchDryRunStatus,
    enabled: dryRunJobRunId !== null && dryRunRunId !== null,
    interval: 3000,
    onComplete: async (status) => {
      if (status.result_state === "SUCCESS") {
        try {
          const resp = await dryRunResultsQuery.refetch();
          if (resp.data?.data) {
            setDryRunResult(resp.data.data);
            toast.success("Dry run complete");
          }
        } catch {
          toast.error("Failed to fetch dry run results");
        }
      } else {
        toast.error(`Dry run failed: ${status.message || "Unknown error"}`);
      }
      setDryRunJobRunId(null);
    },
    onError: () => {
      toast.error("Failed to check dry run status");
    },
  });

  const hasChecks = checks.length > 0;

  const handleGenerate = async () => {
    if (!userInput.trim()) {
      toast.error("Please describe your data quality requirements");
      return;
    }
    try {
      const resp = await generateMutation.mutateAsync({
        data: {
          user_input: userInput,
          table_fqn: hasTable ? tableFqn : undefined,
        },
      });
      const generated = resp.data?.checks ?? [];
      if (generated.length === 0) {
        toast.warning("No rules were generated. Try a more specific description.");
        return;
      }
      setChecks(generated);
      setDryRunResult(null);
      toast.success(`Generated ${generated.length} rule(s)`);
    } catch {
      toast.error("Failed to generate rules");
    }
  };

  const handleDryRun = async () => {
    if (!hasTable) {
      toast.error("Select a table to run a dry run");
      return;
    }
    if (!hasChecks) {
      toast.error("Generate or add rules first");
      return;
    }
    try {
      setDryRunResult(null);
      const resp = await submitDryRunMutation.mutateAsync({
        data: { table_fqn: tableFqn, checks },
      });
      setDryRunRunId(resp.data.run_id);
      setDryRunJobRunId(resp.data.job_run_id);
      toast.info("Dry run submitted — waiting for results...");
    } catch {
      toast.error("Failed to submit dry run");
    }
  };

  const handleSave = async () => {
    if (!hasTable) {
      toast.error("Select a table before saving");
      return;
    }
    if (!hasChecks) {
      toast.error("No rules to save");
      return;
    }
    try {
      await saveMutation.mutateAsync({
        data: { table_fqn: tableFqn, checks },
      });
      try {
        await submitMutation.mutateAsync({ tableFqn });
        toast.success("Rules saved and submitted for approval");
      } catch {
        toast.warning(
          "Rules saved but approval submission failed — you can submit manually from the catalog",
        );
      }
      navigate({ to: "/rules" });
    } catch {
      toast.error("Failed to save rules");
    }
  };

  const isGenerating = generateMutation.isPending;
  const isDryRunning = submitDryRunMutation.isPending || dryRunPolling.isPolling;
  const isSaving = saveMutation.isPending || submitMutation.isPending;
  const isBusy = isGenerating || isDryRunning || isSaving;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: "Rules", to: "/rules" }]}
          page={isEditMode ? "Edit Rules" : "Generate Rules"}
        />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              {isEditMode ? "Edit Rules" : "Generate Rules"}
            </h1>
            <p className="text-muted-foreground">
              {isEditMode
                ? "Edit the data quality rules for this table. Use AI to regenerate, or edit manually."
                : "Select a table and describe your data quality requirements to generate rules with AI."}
            </p>
          </div>
          <Button variant="outline" asChild className="gap-2">
            <Link to="/rules">
              <ArrowLeft className="h-4 w-4" />
              Back to Rules
            </Link>
          </Button>
        </div>
      </div>

      {/* Step 1: Table selection */}
      <Card>
        <CardHeader>
          <CardTitle>1. Select Table</CardTitle>
          <CardDescription>
            {isEditMode
              ? "Table is locked while editing an existing rule set."
              : "Choose the table you want to generate data quality rules for."}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <CatalogBrowser
            value={tableFqn}
            onChange={setTableFqn}
            disabled={isEditMode || isBusy}
          />
          {tableFqn && hasTable && !isEditMode && (
            <p className="text-sm text-muted-foreground">
              Selected: <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">{tableFqn}</code>
            </p>
          )}
          {isEditMode && existingEntry && (
            <div className="flex items-center gap-3 rounded-md border border-border bg-muted/50 px-4 py-3">
              <Info className="h-4 w-4 text-muted-foreground shrink-0" />
              <div className="flex items-center gap-3 text-sm">
                <code className="font-mono text-xs bg-background px-1.5 py-0.5 rounded">{tableFqn}</code>
                <span className="text-muted-foreground">·</span>
                <span className="tabular-nums font-medium">v{existingEntry.version}</span>
                <span className="text-muted-foreground">·</span>
                {statusBadge(existingEntry.status)}
                {existingEntry.updated_at && (
                  <>
                    <span className="text-muted-foreground">·</span>
                    <span className="text-muted-foreground text-xs">
                      Updated {new Date(existingEntry.updated_at).toLocaleDateString()}
                    </span>
                  </>
                )}
              </div>
            </div>
          )}
          {isLoadingRules && hasTable && (
            <p className="text-sm text-muted-foreground">Loading existing rules...</p>
          )}
        </CardContent>
      </Card>

      {/* AI generation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5" />
            {isEditMode ? "AI Assist (Optional)" : "2. Describe Requirements"}
          </CardTitle>
          <CardDescription>
            {isEditMode
              ? "Optionally regenerate rules with AI. This will replace the current rules."
              : "Describe what data quality checks you need in plain English."}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-2">
            <Label htmlFor="user-input">Requirements</Label>
            <Textarea
              id="user-input"
              value={userInput}
              onChange={(e) => setUserInput(e.target.value)}
              disabled={isBusy}
              placeholder="e.g. Ensure no null values in the id and email columns, validate email format, check that amount is positive..."
              rows={4}
            />
          </div>
          <Button
            onClick={handleGenerate}
            disabled={!userInput.trim() || isBusy}
            className="gap-2"
          >
            {isGenerating ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Sparkles className="h-4 w-4" />
            )}
            {isGenerating ? "Generating..." : "Generate Rules"}
          </Button>
        </CardContent>
      </Card>

      {/* Review rules */}
      {hasChecks && (
        <Card>
          <CardHeader>
            <CardTitle>{isEditMode ? "2. Review Rules" : "3. Review Rules"}</CardTitle>
            <CardDescription>
              {isEditMode
                ? "Edit the existing rules, change criticality, or remove rules."
                : "Review, edit, or remove the generated rules before saving."}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <RulesReview checks={checks} onChange={setChecks} />
          </CardContent>
        </Card>
      )}

      {/* Dry run + save */}
      {hasChecks && (
        <Card>
          <CardHeader>
            <CardTitle>{isEditMode ? "3. Validate & Save" : "4. Validate & Save"}</CardTitle>
            <CardDescription>
              Run a dry run to validate rules against live data, then save.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                onClick={handleDryRun}
                disabled={!hasTable || isBusy}
                className="gap-2"
              >
                {isDryRunning ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Play className="h-4 w-4" />
                )}
                {isDryRunning ? "Running..." : "Dry Run"}
              </Button>
              <Button
                onClick={handleSave}
                disabled={!hasTable || isBusy}
                className="gap-2"
              >
                {isSaving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Save className="h-4 w-4" />
                )}
                {isSaving ? "Saving..." : "Save Rules"}
              </Button>
              {!hasTable && (
                <p className="text-xs text-muted-foreground flex items-center gap-1">
                  <AlertCircle className="h-3 w-3" />
                  Select a table first
                </p>
              )}
            </div>

            {/* Polling progress */}
            {isDryRunning && dryRunPolling.status && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>
                  Job status: <span className="font-medium">{dryRunPolling.status.state}</span>
                </span>
              </div>
            )}

            {dryRunResult && (
              <>
                <Separator />
                <DryRunResults result={dryRunResult} />
              </>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
