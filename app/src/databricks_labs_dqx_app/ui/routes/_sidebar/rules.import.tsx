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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  Upload,
  FileText,
  Database,
  Loader2,
  CheckCircle2,
  XCircle,
  AlertTriangle,
} from "lucide-react";
import { toast } from "sonner";
import yaml from "js-yaml";
import { type SaveRulesIn, saveRules } from "@/lib/api";
import {
  useValidateChecks,
  useImportRulesFromTable,
} from "@/lib/api-custom";
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
            Import data quality rules from a YAML file or a Delta table.
          </p>
        </div>
      </div>

      <Tabs defaultValue="yaml" className="space-y-4">
        <TabsList>
          <TabsTrigger value="yaml" className="gap-2">
            <FileText className="h-4 w-4" />
            YAML File
          </TabsTrigger>
          <TabsTrigger value="delta" className="gap-2">
            <Database className="h-4 w-4" />
            Delta Table
          </TabsTrigger>
        </TabsList>

        <TabsContent value="yaml">
          <YamlImportTab onDone={() => navigate({ to: "/rules/drafts" })} />
        </TabsContent>

        <TabsContent value="delta">
          <DeltaImportTab onDone={() => navigate({ to: "/rules/drafts" })} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// YAML Import Tab
// ──────────────────────────────────────────────────────────────────────────────

function YamlImportTab({ onDone }: { onDone: () => void }) {
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
    try {
      const parsed = yaml.load(text);
      if (!Array.isArray(parsed)) {
        setParseError("YAML must be a list of check definitions (e.g. [{check: {function: ..., arguments: ...}, criticality: ...}])");
        return;
      }
      setParsedChecks(parsed as Record<string, unknown>[]);
    } catch (err) {
      setParseError(`YAML parse error: ${(err as Error).message}`);
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

  const handleImport = async () => {
    if (!parsedChecks || !targetTable) return;
    setIsSaving(true);
    try {
      await saveRules({ table_fqn: targetTable, checks: parsedChecks, source: "imported" } as SaveRulesIn);
      toast.success(`Imported ${parsedChecks.length} check(s) for ${targetTable.split(".").pop()}`);
      onDone();
    } catch {
      toast.error("Failed to import rules");
    } finally {
      setIsSaving(false);
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
            {parseError}
          </div>
        )}

        {parsedChecks && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-green-50 dark:bg-green-950/30 text-green-700 dark:text-green-400 text-sm">
            <CheckCircle2 className="h-4 w-4 shrink-0" />
            Parsed {parsedChecks.length} check{parsedChecks.length !== 1 ? "s" : ""} from YAML
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
                    const check = (c.check as Record<string, unknown>) ?? c;
                    const fn = String(check.function ?? "—");
                    const args = check.arguments
                      ? JSON.stringify(check.arguments)
                      : "—";
                    const crit = String(c.criticality ?? check.criticality ?? "warn");
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
          <Button
            onClick={handleImport}
            disabled={!parsedChecks || !targetTable || isSaving}
            className="gap-2"
            size="sm"
          >
            {isSaving ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Upload className="h-4 w-4" />
            )}
            Import {parsedChecks ? `${parsedChecks.length} check${parsedChecks.length !== 1 ? "s" : ""}` : ""}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Delta Table Import Tab
// ──────────────────────────────────────────────────────────────────────────────

function DeltaImportTab({ onDone }: { onDone: () => void }) {
  const [sourceTable, setSourceTable] = useState("");
  const importMutation = useImportRulesFromTable();

  const handleImport = async () => {
    if (!sourceTable) {
      toast.error("Enter the source Delta table FQN");
      return;
    }
    if (sourceTable.split(".").length !== 3) {
      toast.error("Table FQN must be 3-part: catalog.schema.table");
      return;
    }
    try {
      const resp = await importMutation.mutateAsync({ data: { source_table_fqn: sourceTable } });
      const { imported, skipped, errors } = resp.data;
      if (imported > 0) {
        toast.success(`Imported ${imported} rule set${imported !== 1 ? "s" : ""}${skipped > 0 ? `, ${skipped} skipped` : ""}`);
      }
      if (skipped > 0 && imported === 0) {
        toast.error(`All ${skipped} rule sets were skipped due to errors`);
      }
      if (errors.length > 0) {
        for (const err of errors.slice(0, 3)) {
          toast.error(`${err.table_fqn}: ${err.error}`);
        }
      }
      if (imported > 0) onDone();
    } catch (err) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      toast.error(axErr?.response?.data?.detail ?? "Failed to import from Delta table");
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <Database className="h-4 w-4" />
          Import from Delta Table
        </CardTitle>
        <CardDescription>
          Import rules from a Delta table that follows the DQX rules schema
          (<code className="text-xs bg-muted px-1 rounded">table_fqn STRING, checks STRING</code>).
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-2">
          <Label>Source table FQN</Label>
          <Input
            value={sourceTable}
            onChange={(e) => setSourceTable(e.target.value)}
            placeholder="catalog.schema.rules_table"
            className="font-mono text-sm max-w-md"
          />
          <p className="text-xs text-muted-foreground">
            The table must have at least <code className="bg-muted px-1 rounded">table_fqn</code> and{" "}
            <code className="bg-muted px-1 rounded">checks</code> columns.
            Each row's checks will be validated before import.
          </p>
        </div>

        {/* Import result */}
        {importMutation.isSuccess && (
          <div className="p-4 rounded-lg border space-y-2">
            <div className="flex items-center gap-2 text-sm font-medium">
              <CheckCircle2 className="h-4 w-4 text-green-600" />
              Import complete
            </div>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Imported:</span>{" "}
                <span className="font-medium text-green-600">{importMutation.data.data.imported}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Skipped:</span>{" "}
                <span className="font-medium text-amber-600">{importMutation.data.data.skipped}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Errors:</span>{" "}
                <span className="font-medium text-red-600">{importMutation.data.data.errors.length}</span>
              </div>
            </div>
            {importMutation.data.data.errors.length > 0 && (
              <div className="mt-2 space-y-1 max-h-[150px] overflow-auto">
                {importMutation.data.data.errors.map((err, i) => (
                  <div key={i} className="text-xs text-destructive flex gap-1">
                    <XCircle className="h-3 w-3 mt-0.5 shrink-0" />
                    <span><span className="font-mono">{err.table_fqn}</span>: {err.error}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        <Button
          onClick={handleImport}
          disabled={!sourceTable || importMutation.isPending}
          className="gap-2"
        >
          {importMutation.isPending ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Upload className="h-4 w-4" />
          )}
          {importMutation.isPending ? "Importing..." : "Import rules"}
        </Button>
      </CardContent>
    </Card>
  );
}
