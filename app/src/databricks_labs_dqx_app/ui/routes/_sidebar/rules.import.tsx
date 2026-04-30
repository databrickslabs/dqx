import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useState, useRef } from "react";
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
  Upload,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Save,
  Send,
} from "lucide-react";
import { toast } from "sonner";
import yaml from "js-yaml";
import { type SaveRulesIn, saveRules, submitRuleForApproval } from "@/lib/api";
import { useValidateChecks } from "@/lib/api-custom";
import { CatalogBrowser } from "@/components/CatalogBrowser";

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

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
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
      setParsedChecks(parsed as Record<string, unknown>[]);
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
                    <th className="text-left p-2 font-medium">Function</th>
                    <th className="text-left p-2 font-medium">Arguments</th>
                    <th className="text-left p-2 font-medium">Criticality</th>
                  </tr>
                </thead>
                <tbody>
                  {parsedChecks.map((c, i) => {
                    const entry = c ?? {};
                    const check = (entry.check as Record<string, unknown>) ?? entry;
                    const fn = String(check.function ?? "—");
                    const args = check.arguments
                      ? JSON.stringify(check.arguments)
                      : "—";
                    const crit = String(entry.criticality ?? check.criticality ?? "warn");
                    return (
                      <tr key={i} className="border-b last:border-b-0">
                        <td className="p-2 text-muted-foreground">{i + 1}</td>
                        <td className="p-2 font-mono">{fn}</td>
                        <td className="p-2 font-mono text-muted-foreground max-w-[300px] truncate">{args}</td>
                        <td className="p-2">
                          <Badge variant={crit === "error" ? "destructive" : "secondary"} className="text-[10px]">
                            {crit}
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

        <div className="flex items-center gap-3">
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
      </CardContent>
    </Card>
  );
}
