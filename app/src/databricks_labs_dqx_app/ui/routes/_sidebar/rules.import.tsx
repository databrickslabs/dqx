import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useState, useRef, useCallback, useMemo } from "react";
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
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Upload,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Save,
  Send,
  Play,
  Square,
  Info,
} from "lucide-react";
import { toast } from "sonner";
import yaml from "js-yaml";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  type SaveRulesIn,
  type DryRunResultsOut,
  saveRules,
  submitRuleForApproval,
  useSubmitDryRun,
  useGetDryRunResults,
} from "@/lib/api";
import { useValidateChecks, getDryRunStatusCustom, cancelDryRun } from "@/lib/api-custom";
import { useJobPolling } from "@/hooks/use-job-polling";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { LabelsBadges } from "@/components/Labels";
import { DryRunResults } from "@/components/DryRunResults";
import { getUserMetadata } from "@/lib/format-utils";

/**
 * Naming convention for cross-table (a.k.a. dataset-level) SQL rules.
 * The exporter on the active-rules page writes them out as
 * ``function: __sql_check__/<rule_name>`` so the rule's name survives a
 * round-trip through YAML; on import we strip the prefix to recover the
 * canonical ``sql_query`` function name and surface the rule name in
 * the preview.
 */
const SQL_CHECK_PREFIX = "__sql_check__/";

interface ImportSearchParams {
  from?: string;
}

export const Route = createFileRoute("/_sidebar/rules/import")({
  component: ImportRulesPage,
  validateSearch: (search: Record<string, unknown>): ImportSearchParams => ({
    from: typeof search.from === "string" ? search.from : undefined,
  }),
});

function ImportRulesPage() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  const navigate = useNavigate();
  return (
    <div className="space-y-6">
      <PageBreadcrumb
        items={[{ label: "Create Rules", to: "/rules/create" }]}
        page="Import rules"
      />
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Import rules</h1>
          <p className="text-muted-foreground">
            Import data quality rules from a YAML file.
          </p>
        </div>
      </div>

      <YamlImportCard onDone={() => navigate({ to: "/rules/drafts" })} />
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// YAML Import
// ──────────────────────────────────────────────────────────────────────────────

function YamlImportCard({ onDone }: { onDone: () => void }) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [yamlText, setYamlText] = useState("");
  const [parsedChecks, setParsedChecks] = useState<Record<string, unknown>[] | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [targetTable, setTargetTable] = useState("");
  const [validationResult, setValidationResult] = useState<{ valid: boolean; errors: string[] } | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  const validateMutation = useValidateChecks();

  // ── Dry run state ──────────────────────────────────────────────────
  // The YAML may contain cross-table SQL checks (function name prefixed
  // with ``__sql_check__/``). Those have no per-table view, so the dry
  // run filters them out and only exercises table-bound checks against
  // the selected target table.
  const [dryRunSampleSize, setDryRunSampleSize] = useState(1000);
  const [dryRunResult, setDryRunResult] = useState<DryRunResultsOut | null>(null);
  const [dryRunRunId, setDryRunRunId] = useState<string | null>(null);
  const [dryRunJobRunId, setDryRunJobRunId] = useState<number | null>(null);
  const [dryRunViewFqn, setDryRunViewFqn] = useState<string | null>(null);

  const submitDryRunMutation = useSubmitDryRun();
  const dryRunResultsQuery = useGetDryRunResults(dryRunRunId ?? "", {
    query: { enabled: false },
  });

  const fetchDryRunStatus = useCallback(async () => {
    if (!dryRunRunId || dryRunJobRunId === null) throw new Error("No active run");
    const resp = await getDryRunStatusCustom(dryRunRunId, {
      job_run_id: dryRunJobRunId,
      view_fqn: dryRunViewFqn ?? undefined,
    });
    return resp.data;
  }, [dryRunRunId, dryRunJobRunId, dryRunViewFqn]);

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

  // Only table-bound checks run in a per-table dry run. Cross-table SQL
  // checks (``__sql_check__/<name>``) are skipped here; they are dry-run
  // from the SQL editor instead.
  const dryRunnableChecks = useMemo(() => {
    if (!parsedChecks) return [];
    return parsedChecks.filter((c) => {
      const inner = (c.check ?? c) as Record<string, unknown>;
      const fn = String(inner.function ?? "");
      return fn !== "" && !fn.startsWith(SQL_CHECK_PREFIX);
    });
  }, [parsedChecks]);

  const isDryRunning = submitDryRunMutation.isPending || dryRunPolling.isPolling;

  const handleDryRun = async () => {
    if (!targetTable) {
      toast.error("Select a target table to dry-run against");
      return;
    }
    if (dryRunnableChecks.length === 0) {
      toast.error("No table-bound checks to dry-run (cross-table SQL checks are skipped)");
      return;
    }
    try {
      setDryRunResult(null);
      const resp = await submitDryRunMutation.mutateAsync({
        data: {
          table_fqn: targetTable,
          checks: dryRunnableChecks,
          sample_size: dryRunSampleSize,
          skip_history: true,
        },
      });
      setDryRunRunId(resp.data.run_id);
      setDryRunJobRunId(resp.data.job_run_id);
      setDryRunViewFqn(resp.data.view_fqn ?? null);
      toast.info("Dry run submitted — waiting for results...");
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
      toast.error(detail ? `Dry run failed: ${detail}` : "Failed to submit dry run");
    }
  };

  const handleCancelDryRun = async () => {
    if (!dryRunRunId || dryRunJobRunId === null) return;
    try {
      await cancelDryRun(dryRunRunId, { job_run_id: dryRunJobRunId });
      toast.info("Dry run canceled");
    } catch {
      toast.error("Failed to cancel dry run");
    } finally {
      dryRunPolling.stopPolling();
      setDryRunJobRunId(null);
    }
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target;
    const file = input.files?.[0];
    // Reset the input's value so picking the *same* file again fires
    // ``onChange``. Without this the browser treats "same path twice" as
    // a no-op and the user's editor edits silently survive — i.e. the
    // re-import does nothing. We do this before the early return so a
    // cancelled file dialog still resets cleanly.
    input.value = "";
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target?.result as string;
      setYamlText(text);
      parseYaml(text);
    };
    reader.readAsText(file);
  };

  const parseYaml = (text: string) => {
    setParsedChecks(null);
    setParseError(null);
    setValidationResult(null);

    const trimmed = text.trim();
    if (!trimmed || trimmed === "-") return;

    try {
      const parsed = yaml.load(text);
      if (parsed == null) return;
      if (!Array.isArray(parsed)) {
        setParseError(
          "YAML must be a list of check definitions, for example:\n" +
          "- criticality: error\n" +
          "  check:\n" +
          "    function: is_not_null\n" +
          "    arguments:\n" +
          "      column: id",
        );
        return;
      }
      if (parsed.some((item) => item == null || typeof item !== "object")) {
        // Incomplete entry (e.g. a bare "- " while typing) — leave parsedChecks null
        return;
      }
      // Normalize legacy YAML: any top-level numeric ``weight`` is moved into
      // ``user_metadata.weight`` so the imported rules match the new
      // labels-only model used by the rest of the app.
      const normalized = (parsed as Record<string, unknown>[]).map((raw) => {
        const item = { ...raw };
        if (typeof item.weight === "number") {
          const md: Record<string, string> = {};
          const existing = item.user_metadata;
          if (existing && typeof existing === "object") {
            for (const [k, v] of Object.entries(existing as Record<string, unknown>)) {
              if (typeof v === "string") md[k] = v;
            }
          }
          if (!("weight" in md)) md.weight = String(item.weight);
          item.user_metadata = md;
          delete item.weight;
        }
        return item;
      });
      setParsedChecks(normalized);
    } catch {
      // Incomplete YAML while the user is still typing — suppress until they stop
    }
  };

  const handleValidate = async () => {
    if (!parsedChecks) return;
    try {
      const resp = await validateMutation.mutateAsync({ data: { checks: parsedChecks } });
      setValidationResult(resp.data);
      if (resp.data.valid) {
        toast.success("All checks are valid");
      } else {
        toast.error(`Validation found ${resp.data.errors.length} error(s)`);
      }
    } catch {
      toast.error("Failed to validate checks");
    }
  };

  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleSaveAsDrafts = async () => {
    if (!parsedChecks || !targetTable) return;
    setIsSaving(true);
    try {
      await saveRules({ table_fqn: targetTable, checks: parsedChecks, source: "imported" } as SaveRulesIn);
      toast.success(`Saved ${parsedChecks.length} check(s) as drafts`);
      onDone();
    } catch {
      toast.error("Failed to save rules");
    } finally {
      setIsSaving(false);
    }
  };

  const handleSubmitForReview = async () => {
    if (!parsedChecks || !targetTable) return;
    setIsSubmitting(true);
    try {
      const resp = await saveRules({ table_fqn: targetTable, checks: parsedChecks, source: "imported" } as SaveRulesIn);
      const savedRules = resp.data;

      let submitted = 0;
      let failed = 0;
      for (const rule of savedRules) {
        if (!rule.rule_id) continue;
        try {
          await submitRuleForApproval(rule.rule_id, null);
          submitted++;
        } catch {
          failed++;
        }
      }

      if (submitted > 0) toast.success(`Submitted ${submitted} rule(s) for review`);
      if (failed > 0) toast.error(`${failed} rule(s) failed to submit — saved as drafts instead`);
      onDone();
    } catch {
      toast.error("Failed to save rules");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <Upload className="h-4 w-4" />
          Import from YAML
        </CardTitle>
        <CardDescription>
          Upload a YAML file or paste YAML content. Each entry should follow the DQX check format.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* File upload */}
        <div className="space-y-2">
          <Label>Upload YAML file</Label>
          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              size="sm"
              onClick={() => fileInputRef.current?.click()}
              className="gap-2"
            >
              <Upload className="h-4 w-4" />
              Choose file
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml"
              onChange={handleFileUpload}
              className="hidden"
            />
            <span className="text-xs text-muted-foreground">or paste below</span>
          </div>
        </div>

        {/* YAML editor */}
        <div className="space-y-2">
          <Label>YAML content</Label>
          <Textarea
            value={yamlText}
            onChange={(e) => {
              setYamlText(e.target.value);
              if (e.target.value.trim()) parseYaml(e.target.value);
              else {
                setParsedChecks(null);
                setParseError(null);
                setValidationResult(null);
              }
            }}
            placeholder={`- criticality: error\n  check:\n    function: is_not_null\n    arguments:\n      column: id`}
            className="font-mono text-xs min-h-[200px]"
          />
        </div>

        {/* Parse status */}
        {parseError && (
          <div className="flex items-start gap-2 p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
            <XCircle className="h-4 w-4 mt-0.5 shrink-0" />
            <pre className="whitespace-pre-wrap font-mono text-xs">{parseError}</pre>
          </div>
        )}

        {parsedChecks && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-green-50 dark:bg-green-950/30 text-green-700 dark:text-green-400 text-sm">
            <CheckCircle2 className="h-4 w-4 shrink-0" />
            Loaded {parsedChecks.length} check{parsedChecks.length !== 1 ? "s" : ""} from YAML
          </div>
        )}

        {/* Preview */}
        {parsedChecks && parsedChecks.length > 0 && (
          <div className="space-y-2">
            <Label>Preview</Label>
            <div className="border rounded-lg overflow-auto max-h-[200px]">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-2 font-medium">#</th>
                    <th className="text-left p-2 font-medium">Name</th>
                    <th className="text-left p-2 font-medium">Function</th>
                    <th className="text-left p-2 font-medium">Arguments</th>
                    <th className="text-left p-2 font-medium">Criticality</th>
                    <th className="text-left p-2 font-medium">Labels</th>
                  </tr>
                </thead>
                <tbody>
                  {parsedChecks.map((c, i) => {
                    const entry = c ?? {};
                    const check = (entry.check as Record<string, unknown>) ?? entry;
                    const fnRaw = String(check.function ?? "—");
                    // Cross-table rules are stored as
                    // ``function: __sql_check__/<rule_name>`` so we
                    // surface the canonical "sql_query" function and
                    // recover the name from the suffix.
                    const isSqlCheck = fnRaw.startsWith(SQL_CHECK_PREFIX);
                    const fn = isSqlCheck ? "sql_query" : fnRaw;
                    const argsObj = (check.arguments as Record<string, unknown>) ?? {};
                    const args = check.arguments ? JSON.stringify(check.arguments) : "—";
                    const crit = String(entry.criticality ?? check.criticality ?? "warn");
                    const labels = getUserMetadata(entry as Record<string, unknown>);

                    // Rule name lookup falls back through the same
                    // priority order the active-rules export uses, so
                    // round-tripped YAML always preserves the name.
                    const name =
                      (typeof entry.name === "string" && entry.name) ||
                      (typeof argsObj.name === "string" && (argsObj.name as string)) ||
                      (isSqlCheck ? fnRaw.slice(SQL_CHECK_PREFIX.length) : null);

                    return (
                      <tr key={i} className="border-b last:border-b-0">
                        <td className="p-2 text-muted-foreground">{i + 1}</td>
                        <td className="p-2 font-mono max-w-[200px] truncate">
                          {name ? (
                            name
                          ) : (
                            <span className="italic text-muted-foreground/60">—</span>
                          )}
                        </td>
                        <td className="p-2 font-mono">{fn}</td>
                        <td className="p-2 font-mono text-muted-foreground max-w-[280px] truncate">{args}</td>
                        <td className="p-2">
                          <Badge variant={crit === "error" ? "destructive" : "secondary"} className="text-[10px]">
                            {crit}
                          </Badge>
                        </td>
                        <td className="p-2">
                          {Object.keys(labels).length === 0 ? (
                            <span className="text-[10px] italic text-muted-foreground/60">—</span>
                          ) : (
                            <LabelsBadges labels={labels} max={3} size="sm" />
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Validation result */}
        {validationResult && !validationResult.valid && (
          <div className="space-y-1 p-3 rounded-lg bg-amber-50 dark:bg-amber-950/30">
            <div className="flex items-center gap-2 text-amber-700 dark:text-amber-400 text-sm font-medium">
              <AlertTriangle className="h-4 w-4" />
              Validation errors
            </div>
            <ul className="text-xs text-amber-600 dark:text-amber-400/80 list-disc pl-5 space-y-0.5">
              {validationResult.errors.map((err, i) => (
                <li key={i}>{err}</li>
              ))}
            </ul>
          </div>
        )}

        <Separator />

        {/* Target table + actions */}
        <div className="space-y-3">
          <Label>Target table</Label>
          <CatalogBrowser
            value={targetTable}
            onChange={setTargetTable}
          />
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <Button
            variant="outline"
            onClick={handleValidate}
            disabled={!parsedChecks || validateMutation.isPending}
            className="gap-2"
            size="sm"
          >
            {validateMutation.isPending ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <CheckCircle2 className="h-4 w-4" />
            )}
            Validate
          </Button>

          <div className="flex items-center gap-1.5">
            <Select
              value={String(dryRunSampleSize)}
              onValueChange={(v) => setDryRunSampleSize(Number(v))}
            >
              <SelectTrigger className="w-[140px] h-8 text-xs" disabled={isDryRunning}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="100">100 rows</SelectItem>
                <SelectItem value="500">500 rows</SelectItem>
                <SelectItem value="1000">1,000 rows</SelectItem>
                <SelectItem value="5000">5,000 rows</SelectItem>
                <SelectItem value="10000">10,000 rows</SelectItem>
              </SelectContent>
            </Select>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3.5 w-3.5 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent className="max-w-xs">
                <p className="text-xs leading-relaxed">
                  Dry-run validates only the first <strong>1–10,000 rows</strong>{" "}
                  of the target table — it&apos;s a fast preview, not a full
                  validation. To run imported rules against every row after
                  approval, go to <strong>Run Rules</strong> and pick{" "}
                  <strong>&quot;All rows&quot;</strong>.
                </p>
              </TooltipContent>
            </Tooltip>
          </div>

          {!isDryRunning ? (
            <Button
              variant="outline"
              onClick={handleDryRun}
              disabled={
                !parsedChecks ||
                !targetTable ||
                dryRunnableChecks.length === 0 ||
                isSaving ||
                isSubmitting
              }
              className="gap-2"
              size="sm"
            >
              <Play className="h-4 w-4" />
              Dry run
            </Button>
          ) : (
            <Button
              variant="outline"
              onClick={handleCancelDryRun}
              className="gap-2"
              size="sm"
            >
              <Square className="h-4 w-4" />
              Cancel dry run
            </Button>
          )}

          <div className="ml-auto flex items-center gap-2">
            <Button
              variant="outline"
              onClick={handleSaveAsDrafts}
              disabled={!parsedChecks || !targetTable || isSaving || isSubmitting}
              className="gap-2"
              size="sm"
            >
              {isSaving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Save className="h-4 w-4" />
              )}
              Save as drafts
            </Button>
            <Button
              onClick={handleSubmitForReview}
              disabled={!parsedChecks || !targetTable || isSaving || isSubmitting}
              className="gap-2"
              size="sm"
            >
              {isSubmitting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Send className="h-4 w-4" />
              )}
              Submit for review
            </Button>
          </div>
        </div>

        {isDryRunning && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-muted/40 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Dry run in progress on{" "}
            <span className="font-mono text-foreground">{targetTable}</span>
            {" — "}
            sampling {dryRunSampleSize.toLocaleString()} row
            {dryRunSampleSize !== 1 ? "s" : ""}
            {parsedChecks && dryRunnableChecks.length < parsedChecks.length && (
              <span>
                {" "}
                · {parsedChecks.length - dryRunnableChecks.length} cross-table check
                {parsedChecks.length - dryRunnableChecks.length !== 1 ? "s" : ""} skipped
              </span>
            )}
          </div>
        )}

        {dryRunResult && !isDryRunning && (
          <DryRunResults result={dryRunResult} />
        )}
      </CardContent>
    </Card>
  );
}
