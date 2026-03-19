import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { PageBreadcrumb } from "@/components/apx/PageBreadcrumb";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
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
} from "lucide-react";
import { toast } from "sonner";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { RulesReview } from "@/components/RulesReview";
import { DryRunResults } from "@/components/DryRunResults";
import {
  useAiAssistedChecksGeneration,
  useDryRun,
  useSaveRules,
  type DryRunOut,
} from "@/lib/api";

type SearchParams = {
  table?: string;
};

export const Route = createFileRoute("/_sidebar/rules/generate")({
  component: GenerateRulesPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    table: (search.table as string) || undefined,
  }),
});

function GenerateRulesPage() {
  const navigate = useNavigate();
  const { table: initialTable } = Route.useSearch();

  const [tableFqn, setTableFqn] = useState(initialTable ?? "");
  const [userInput, setUserInput] = useState("");
  const [checks, setChecks] = useState<Record<string, unknown>[]>([]);
  const [dryRunResult, setDryRunResult] = useState<DryRunOut | null>(null);

  const generateMutation = useAiAssistedChecksGeneration();
  const dryRunMutation = useDryRun();
  const saveMutation = useSaveRules();

  const hasChecks = checks.length > 0;
  const hasTable = tableFqn.split(".").length === 3;

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
      const resp = await dryRunMutation.mutateAsync({
        data: { table_fqn: tableFqn, checks },
      });
      setDryRunResult(resp.data);
      toast.success("Dry run complete");
    } catch {
      toast.error("Dry run failed");
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
      toast.success("Rules saved successfully");
      navigate({ to: "/rules" });
    } catch {
      toast.error("Failed to save rules");
    }
  };

  const isGenerating = generateMutation.isPending;
  const isDryRunning = dryRunMutation.isPending;
  const isSaving = saveMutation.isPending;
  const isBusy = isGenerating || isDryRunning || isSaving;

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: "Rules", to: "/rules" }]}
          page="Generate Rules"
        />
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">
              Generate Rules
            </h1>
            <p className="text-muted-foreground">
              Select a table and describe your data quality requirements to
              generate rules with AI.
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
            Choose the table you want to generate data quality rules for.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <CatalogBrowser
            value={tableFqn}
            onChange={setTableFqn}
            disabled={isBusy}
          />
          {tableFqn && hasTable && (
            <p className="text-sm text-muted-foreground mt-2">
              Selected: <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">{tableFqn}</code>
            </p>
          )}
        </CardContent>
      </Card>

      {/* Step 2: AI generation */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5" />
            2. Describe Requirements
          </CardTitle>
          <CardDescription>
            Describe what data quality checks you need in plain English.
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

      {/* Step 3: Review rules */}
      {hasChecks && (
        <Card>
          <CardHeader>
            <CardTitle>3. Review Rules</CardTitle>
            <CardDescription>
              Review, edit, or remove the generated rules before saving.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <RulesReview checks={checks} onChange={setChecks} />
          </CardContent>
        </Card>
      )}

      {/* Step 4: Dry run + save */}
      {hasChecks && (
        <Card>
          <CardHeader>
            <CardTitle>4. Validate & Save</CardTitle>
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
