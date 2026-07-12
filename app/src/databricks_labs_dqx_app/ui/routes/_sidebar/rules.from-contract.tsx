import { createFileRoute, Navigate } from "@tanstack/react-router";
import { useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  FileText,
  Info,
  Loader2,
  Save,
  Send,
  Sparkles,
  Upload,
  XCircle,
} from "lucide-react";

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
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  type ContractSchemaRulesOut,
  type GenerateRulesFromContractOut,
  generateRulesFromContract,
  useListCheckFunctions,
} from "@/lib/api";
import { importChecksAsRegistryDrafts } from "@/lib/import-registry-rules";

export const DOCS_URL =
  "https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation/";

// ``/rules/from-contract`` was the standalone entry point for contract-
// based rule generation. The flow has been folded into the unified
// ``/registry-rules/import`` page as a second tab so YAML and ODCS imports share
// a single landing. We keep this route as a permanent redirect so any
// existing bookmark or in-flight link lands on the registry import page with
// the Contract tab already selected.
export const Route = createFileRoute("/_sidebar/rules/from-contract")({
  component: () => (
    <Navigate to="/registry-rules/import" search={{ tab: "contract" }} replace />
  ),
});

// ──────────────────────────────────────────────────────────────────────────────
// Main workspace
// ──────────────────────────────────────────────────────────────────────────────

export function ContractWorkspace({ onDone }: { onDone: () => void }) {
  const { t } = useTranslation();
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Contract input
  const [contractText, setContractText] = useState("");

  // Generation options
  const [generatePredefined, setGeneratePredefined] = useState(true);
  const [generateSchemaValidation, setGenerateSchemaValidation] = useState(true);
  const [strictSchema, setStrictSchema] = useState(true);
  const [processTextRules, setProcessTextRules] = useState(false);
  const [defaultCriticality, setDefaultCriticality] = useState<"error" | "warn">(
    "error",
  );

  // Generation state
  const [isGenerating, setIsGenerating] = useState(false);
  const [generated, setGenerated] = useState<GenerateRulesFromContractOut | null>(
    null,
  );
  const [generationError, setGenerationError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target;
    const file = input.files?.[0];
    input.value = "";
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      setContractText(String(ev.target?.result ?? ""));
    };
    reader.readAsText(file);
  };

  const handleGenerate = async () => {
    if (!contractText.trim()) {
      toast.error(t("rulesFromContract.errors.emptyContract"));
      return;
    }
    setIsGenerating(true);
    setGenerationError(null);
    setGenerated(null);
    try {
      const resp = await generateRulesFromContract({
        contract_text: contractText,
        generate_predefined_rules: generatePredefined,
        process_text_rules: processTextRules,
        generate_schema_validation: generateSchemaValidation,
        strict_schema_validation: strictSchema,
        default_criticality: defaultCriticality,
      });
      const data = resp.data;
      setGenerated(data);
      toast.success(
        t("rulesFromContract.generated", { count: data.total_rules }),
      );
    } catch (err) {
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ?? t("rulesFromContract.errors.generateFailed");
      setGenerationError(detail);
      toast.error(detail);
    } finally {
      setIsGenerating(false);
    }
  };

  const importableRules = useMemo(() => {
    if (!generated) return [] as Record<string, unknown>[];
    return generated.schemas.flatMap((schema) =>
      schema.rules.map((rule) => rule as Record<string, unknown>),
    );
  }, [generated]);

  const totalImportableRules = importableRules.length;

  const handleSave = async (alsoSubmit: boolean) => {
    if (totalImportableRules === 0) {
      toast.error(t("rulesFromContract.errors.noRules"));
      return;
    }
    if (checkFunctions.length === 0) {
      toast.error(t("rulesFromContract.errors.saveFailed"));
      return;
    }
    if (alsoSubmit) setIsSubmitting(true);
    else setIsSaving(true);
    try {
      const toastId = toast.loading(
        alsoSubmit
          ? t("rulesImport.importingAndSubmitting", { count: totalImportableRules })
          : t("rulesImport.importing", { count: totalImportableRules }),
      );
      const result = await importChecksAsRegistryDrafts({
        checks: importableRules,
        checkFunctions,
        t,
        alsoSubmit,
      });
      toast.dismiss(toastId);
      if (result.saved > 0) {
        toast.success(t("rulesFromContract.saved", { count: result.saved }));
      }
      if (alsoSubmit && result.submitted > 0) {
        toast.success(t("rulesFromContract.submitted", { count: result.submitted }));
      }
      if (result.submitFailed > 0) {
        toast.error(t("rulesFromContract.submitFailed", { count: result.submitFailed }));
      }
      if (result.failed > 0) {
        toast.error(t("rulesImport.importPartialFailed", { count: result.failed }));
      }
      if (result.saved === 0 && result.failed === 0) {
        toast.error(t("rulesFromContract.errors.saveFailed"));
      }
      if (result.saved > 0) onDone();
    } catch (err) {
      toast.dismiss();
      const detail =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ?? t("rulesFromContract.errors.saveFailed");
      toast.error(detail);
    } finally {
      setIsSaving(false);
      setIsSubmitting(false);
    }
  };

  const busy = isGenerating || isSaving || isSubmitting;

  return (
    <div className="space-y-6">
      {/* Contract source */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <FileText className="h-4 w-4" />
            {t("rulesFromContract.input.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesFromContract.input.description")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label>{t("rulesFromContract.input.uploadLabel")}</Label>
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                size="sm"
                onClick={() => fileInputRef.current?.click()}
                className="gap-2"
                disabled={busy}
              >
                <Upload className="h-4 w-4" />
                {t("rulesFromContract.input.chooseFile")}
              </Button>
              <input
                ref={fileInputRef}
                type="file"
                accept=".yaml,.yml,.json"
                onChange={handleFileUpload}
                className="hidden"
              />
              <span className="text-xs text-muted-foreground">
                {t("rulesFromContract.input.orPaste")}
              </span>
            </div>
          </div>

          <div className="space-y-2">
            <Label>{t("rulesFromContract.input.contractContent")}</Label>
            <Textarea
              value={contractText}
              onChange={(e) => setContractText(e.target.value)}
              placeholder={`kind: DataContract\napiVersion: v3.0.2\nid: urn:datacontract:demo:orders\nname: Demo Orders\nversion: 1.0.0\nstatus: active\nschema:\n  - name: orders\n    physicalType: table\n    properties:\n      - name: order_id\n        logicalType: string\n        required: true\n        unique: true`}
              className="font-mono text-xs min-h-[220px]"
              disabled={busy}
            />
          </div>

          {/* Options */}
          <div className="grid gap-3 sm:grid-cols-2">
            <OptionRow
              checked={generatePredefined}
              onChange={setGeneratePredefined}
              disabled={busy}
              label={t("rulesFromContract.options.predefined")}
              hint={t("rulesFromContract.options.predefinedHint")}
            />
            <OptionRow
              checked={generateSchemaValidation}
              onChange={setGenerateSchemaValidation}
              disabled={busy}
              label={t("rulesFromContract.options.schemaValidation")}
              hint={t("rulesFromContract.options.schemaValidationHint")}
            />
            <OptionRow
              checked={strictSchema}
              onChange={setStrictSchema}
              disabled={busy || !generateSchemaValidation}
              label={t("rulesFromContract.options.strict")}
              hint={t("rulesFromContract.options.strictHint")}
            />
            <OptionRow
              checked={processTextRules}
              onChange={setProcessTextRules}
              disabled={busy}
              label={t("rulesFromContract.options.textRules")}
              hint={t("rulesFromContract.options.textRulesHint")}
            />
            <div className="flex items-start gap-3 p-3 rounded-lg border">
              <div className="flex-1 space-y-1">
                <div className="text-sm font-medium">
                  {t("rulesFromContract.options.criticality")}
                </div>
                <div className="text-xs text-muted-foreground">
                  {t("rulesFromContract.options.criticalityHint")}
                </div>
              </div>
              <Select
                value={defaultCriticality}
                onValueChange={(v) =>
                  setDefaultCriticality(v as "error" | "warn")
                }
                disabled={busy}
              >
                <SelectTrigger className="w-[110px] h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="error">error</SelectItem>
                  <SelectItem value="warn">warn</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <Button
              onClick={handleGenerate}
              disabled={!contractText.trim() || busy}
              className="gap-2"
              size="sm"
            >
              {isGenerating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {t("rulesFromContract.generate")}
            </Button>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3.5 w-3.5 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent className="max-w-sm">
                <p className="text-xs leading-relaxed">
                  {t("rulesFromContract.generateHint")}
                </p>
              </TooltipContent>
            </Tooltip>
          </div>

          {generationError && (
            <div className="flex items-start gap-2 p-3 rounded-lg bg-destructive/10 text-destructive text-sm">
              <XCircle className="h-4 w-4 mt-0.5 shrink-0" />
              <pre className="whitespace-pre-wrap font-mono text-xs">
                {generationError}
              </pre>
            </div>
          )}
        </CardContent>
      </Card>

      {generated && (
        <GeneratedResults
          generated={generated}
          onSave={() => handleSave(false)}
          onSubmit={() => handleSave(true)}
          totalImportableRules={totalImportableRules}
          isSaving={isSaving}
          isSubmitting={isSubmitting}
        />
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Sub-components
// ──────────────────────────────────────────────────────────────────────────────

function OptionRow({
  checked,
  onChange,
  disabled,
  label,
  hint,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
  label: string;
  hint: string;
}) {
  return (
    <label
      className={
        "flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors " +
        (disabled ? "opacity-60 cursor-not-allowed" : "hover:bg-muted/30")
      }
    >
      <Checkbox
        checked={checked}
        onCheckedChange={(v) => onChange(Boolean(v))}
        disabled={disabled}
        className="mt-0.5"
      />
      <div className="flex-1 space-y-1">
        <div className="text-sm font-medium">{label}</div>
        <div className="text-xs text-muted-foreground">{hint}</div>
      </div>
    </label>
  );
}

function GeneratedResults({
  generated,
  onSave,
  onSubmit,
  totalImportableRules,
  isSaving,
  isSubmitting,
}: {
  generated: GenerateRulesFromContractOut;
  onSave: () => void;
  onSubmit: () => void;
  totalImportableRules: number;
  isSaving: boolean;
  isSubmitting: boolean;
}) {
  const { t } = useTranslation();
  const { metadata, schemas, total_rules } = generated;
  const warnings = generated.warnings ?? [];
  const unassignedRules = generated.unassigned_rules ?? [];
  const busy = isSaving || isSubmitting;
  const canSave = totalImportableRules > 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <CheckCircle2 className="h-4 w-4 text-green-600" />
          {t("rulesFromContract.results.title", { count: total_rules })}
        </CardTitle>
        <CardDescription>
          {t("rulesFromContract.results.description")}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        {/* Contract metadata */}
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 text-xs">
          <Meta label={t("rulesFromContract.meta.name")} value={metadata.name} />
          <Meta
            label={t("rulesFromContract.meta.version")}
            value={metadata.version}
          />
          <Meta
            label={t("rulesFromContract.meta.odcs")}
            value={metadata.odcs_api_version}
          />
          <Meta
            label={t("rulesFromContract.meta.id")}
            value={metadata.contract_id}
            mono
          />
          <Meta
            label={t("rulesFromContract.meta.domain")}
            value={metadata.domain}
          />
          <Meta
            label={t("rulesFromContract.meta.owner")}
            value={metadata.owner}
          />
        </div>

        {warnings.length > 0 && (
          <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-950/30 text-amber-700 dark:text-amber-400 text-sm">
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
            <ul className="text-xs space-y-0.5">
              {warnings.map((w, i) => (
                <li key={i}>{w}</li>
              ))}
            </ul>
          </div>
        )}

        <Separator />

        {/* Per-schema sections */}
        <div className="space-y-3">
          {schemas.length === 0 && (
            <div className="text-sm text-muted-foreground italic">
              {t("rulesFromContract.results.noSchemas")}
            </div>
          )}
          {schemas.map((schema) => (
            <SchemaSection key={schema.schema_name} schema={schema} />
          ))}
          {unassignedRules.length > 0 && (
            <div className="border rounded-lg p-3 bg-muted/20">
              <div className="text-sm font-medium mb-1">
                {t("rulesFromContract.results.unassigned", {
                  count: unassignedRules.length,
                })}
              </div>
              <p className="text-xs text-muted-foreground">
                {t("rulesFromContract.results.unassignedHint")}
              </p>
            </div>
          )}
        </div>

        <Separator />

        <p className="text-xs text-muted-foreground">{t("rulesImport.registryHint")}</p>

        {/* Save actions */}
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="text-xs text-muted-foreground">
            {totalImportableRules === 0
              ? t("rulesFromContract.results.noRulesToSave")
              : t("rulesFromContract.results.saveSummaryRegistry", {
                  count: totalImportableRules,
                })}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              onClick={onSave}
              disabled={!canSave || busy}
              size="sm"
              className="gap-2"
            >
              {isSaving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Save className="h-4 w-4" />
              )}
              {t("rulesFromContract.saveAsDrafts")}
            </Button>
            <Button
              onClick={onSubmit}
              disabled={!canSave || busy}
              size="sm"
              className="gap-2"
            >
              {isSubmitting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Send className="h-4 w-4" />
              )}
              {t("rulesFromContract.submitForReview")}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function Meta({
  label,
  value,
  mono,
}: {
  label: string;
  value: string | null | undefined;
  mono?: boolean;
}) {
  return (
    <div className="space-y-0.5">
      <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      <div className={mono ? "font-mono truncate" : "truncate"}>
        {value && value.trim() !== "" ? (
          value
        ) : (
          <span className="italic text-muted-foreground/60">—</span>
        )}
      </div>
    </div>
  );
}

function SchemaSection({ schema }: { schema: ContractSchemaRulesOut }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(true);
  const propertyCount = schema.property_count ?? 0;

  return (
    <div className="border rounded-lg">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 p-3 text-left hover:bg-muted/30 transition-colors"
      >
        {open ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
        <span className="font-mono text-sm">{schema.schema_name}</span>
        <Badge variant="secondary" className="text-[10px]">
          {t("rulesFromContract.schema.ruleCount", { count: schema.rules.length })}
        </Badge>
        {propertyCount > 0 && (
          <Badge variant="outline" className="text-[10px]">
            {t("rulesFromContract.schema.propertyCount", {
              count: propertyCount,
            })}
          </Badge>
        )}
        {schema.physical_name && (
          <span className="ml-auto text-[11px] text-muted-foreground">
            {t("rulesFromContract.schema.physicalNameSuggested")}
            <span className="ml-1 font-mono text-foreground">
              {schema.physical_name}
            </span>
          </span>
        )}
      </button>

      {open && (
        <div className="px-3 pb-3 space-y-3 border-t pt-3">
          {schema.rules.length === 0 ? (
            <div className="text-xs text-muted-foreground italic p-2">
              {t("rulesFromContract.schema.noRules")}
            </div>
          ) : (
            <RulesPreviewTable rules={schema.rules} />
          )}
        </div>
      )}
    </div>
  );
}

function RulesPreviewTable({
  rules,
}: {
  rules: ContractSchemaRulesOut["rules"];
}) {
  const { t } = useTranslation();
  return (
    <div className="border rounded-lg overflow-auto max-h-[260px]">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b bg-muted/50 sticky top-0">
            <th className="text-left p-2 font-medium">
              {t("rulesFromContract.preview.name")}
            </th>
            <th className="text-left p-2 font-medium">
              {t("rulesFromContract.preview.function")}
            </th>
            <th className="text-left p-2 font-medium">
              {t("rulesFromContract.preview.field")}
            </th>
            <th className="text-left p-2 font-medium">
              {t("rulesFromContract.preview.type")}
            </th>
            <th className="text-left p-2 font-medium">
              {t("rulesFromContract.preview.criticality")}
            </th>
          </tr>
        </thead>
        <tbody>
          {rules.map((rule, i) => {
            const r = rule as Record<string, unknown>;
            const check = (r.check as Record<string, unknown>) ?? {};
            const fn = String(check.function ?? "—");
            const userMeta = (r.user_metadata as Record<string, unknown>) ?? {};
            const field = String(userMeta.field ?? "");
            const ruleType = String(userMeta.rule_type ?? "");
            const name = String(r.name ?? "");
            const crit = String(r.criticality ?? "error");
            return (
              <tr key={i} className="border-b last:border-b-0">
                <td className="p-2 font-mono max-w-[220px] truncate">
                  {name || (
                    <span className="italic text-muted-foreground/60">—</span>
                  )}
                </td>
                <td className="p-2 font-mono">{fn}</td>
                <td className="p-2 font-mono text-muted-foreground">
                  {field || (
                    <span className="italic text-muted-foreground/60">—</span>
                  )}
                </td>
                <td className="p-2">
                  {ruleType ? (
                    <Badge variant="outline" className="text-[10px]">
                      {ruleType}
                    </Badge>
                  ) : (
                    <span className="italic text-muted-foreground/60">—</span>
                  )}
                </td>
                <td className="p-2">
                  <Badge
                    variant={crit === "error" ? "destructive" : "secondary"}
                    className="text-[10px]"
                  >
                    {crit}
                  </Badge>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
