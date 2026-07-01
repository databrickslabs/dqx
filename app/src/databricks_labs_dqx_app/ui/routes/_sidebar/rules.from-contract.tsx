import { createFileRoute, Navigate } from "@tanstack/react-router";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { CatalogBrowser } from "@/components/CatalogBrowser";
import {
  type ContractSchemaRulesOut,
  type GenerateRulesFromContractOut,
  type SaveRulesIn,
  generateRulesFromContract,
  saveRules,
  submitRuleForApproval,
  useGetTableColumns,
} from "@/lib/api";

export const DOCS_URL =
  "https://databrickslabs.github.io/dqx/docs/guide/data_contract_quality_rules_generation/";

// ──────────────────────────────────────────────────────────────────────────────
// Validation types & helpers
// ──────────────────────────────────────────────────────────────────────────────

type SchemaValidationStatus =
  | "idle" // no target picked yet
  | "loading" // fetching column list from UC
  | "ok" // all referenced columns exist on the target
  | "mismatch" // target is missing one or more referenced columns
  | "error"; // table couldn't be inspected (404, permissions, ...)

interface SchemaValidation {
  status: SchemaValidationStatus;
  missing: string[];
}

/** Parse a 3-part UC FQN. Returns null if not a valid catalog.schema.table. */
function parseFqn(
  fqn: string,
): { catalog: string; schema: string; table: string } | null {
  if (!fqn) return null;
  const parts = fqn.trim().split(".");
  if (parts.length !== 3) return null;
  const [catalog, schema, table] = parts.map((p) => p.trim());
  if (!catalog || !schema || !table) return null;
  return { catalog, schema, table };
}

function isFullyQualified(fqn: string): boolean {
  return parseFqn(fqn) !== null;
}

/**
 * Collect column names referenced by a contract-generated rule.
 *
 * DQX's DataContractRulesGenerator stores the source ODCS property name
 * on ``user_metadata.field`` for property-level rules, and the actual
 * Spark check arguments live under ``check.arguments``. We harvest from
 * both so:
 *   - ``is_not_null({column: 'foo'})`` is matched on ``foo``
 *   - ``is_unique({columns: ['a','b']})`` is matched on ``a`` and ``b``
 *   - dataset-level rules (e.g. ``has_valid_schema``) contribute nothing
 *     because they don't pin a specific column.
 */
function extractColumnRefs(
  rules: ContractSchemaRulesOut["rules"],
): Set<string> {
  const cols = new Set<string>();
  for (const rule of rules) {
    const r = rule as Record<string, unknown>;
    const userMeta = (r.user_metadata as Record<string, unknown>) ?? {};
    const field = typeof userMeta.field === "string" ? userMeta.field : "";
    if (field) cols.add(field);

    const check = (r.check as Record<string, unknown>) ?? {};
    const args = (check.arguments as Record<string, unknown>) ?? {};
    if (typeof args.column === "string" && args.column) {
      cols.add(args.column);
    }
    if (Array.isArray(args.columns)) {
      for (const c of args.columns) {
        if (typeof c === "string" && c) cols.add(c);
      }
    }
  }
  return cols;
}

// ``/rules/from-contract`` was the standalone entry point for contract-
// based rule generation. The flow has been folded into the unified
// ``/rules/import`` page as a second tab so YAML and ODCS imports share
// a single landing. We keep this route as a permanent redirect so any
// existing bookmark or in-flight link lands on the new tabbed page with
// the Contract tab already selected.
export const Route = createFileRoute("/_sidebar/rules/from-contract")({
  component: () => (
    <Navigate to="/rules/import" search={{ tab: "contract" }} replace />
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

  // Per-schema target table + save state. ``targets`` is keyed by ODCS
  // schema name and holds the fully-qualified UC table the user picked
  // (or empty string if not picked yet). We deliberately do NOT pre-fill
  // it from the contract's ``physical_name`` hint — that's just a label
  // and is rarely a real UC FQN (often single-part like
  // ``customers_table``), which would otherwise trick the gating logic
  // into thinking a table was picked.
  const [targets, setTargets] = useState<Record<string, string>>({});
  // Per-schema column-validation outcome reported up from SchemaSection.
  // ``status`` mirrors the React Query lifecycle but with the extra
  // ``mismatch`` state we care about for save-gating: any schema with
  // missing columns blocks the save action.
  const [validations, setValidations] = useState<Record<string, SchemaValidation>>({});
  const [isSaving, setIsSaving] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const onValidationChange = useCallback(
    (schemaName: string, value: SchemaValidation) => {
      setValidations((prev) => {
        const existing = prev[schemaName];
        // ``validations`` is plumbed through every render; only update
        // state when something actually changed so we don't create
        // pointless re-renders that would re-fire the child's
        // useEffect and trigger an update loop.
        if (
          existing &&
          existing.status === value.status &&
          existing.missing.length === value.missing.length &&
          existing.missing.every((c, i) => c === value.missing[i])
        ) {
          return prev;
        }
        return { ...prev, [schemaName]: value };
      });
    },
    [],
  );

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
      // Start every schema with an empty target — see comment on the
      // ``targets`` state for why we don't pre-fill from ``physical_name``.
      const blankTargets: Record<string, string> = {};
      for (const schema of data.schemas) {
        blankTargets[schema.schema_name] = "";
      }
      setTargets(blankTargets);
      setValidations({});
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

  // A schema counts towards save/submit only when ALL of:
  //   1. It actually produced at least one rule.
  //   2. The user picked a real 3-part UC FQN (not just a contract hint).
  //   3. Column validation either passed cleanly or is still in flight
  //      (we don't gate on ``loading`` so the user isn't blocked by a
  //      slow column-list endpoint — we DO gate on ``mismatch``).
  // ``schemasWithTarget`` is the looser set (#1 + #2) used to tell the
  // user "you've picked tables but they're not valid yet"; the stricter
  // ``saveableSchemas`` is what the buttons actually act on.
  const schemasWithTarget = useMemo(() => {
    if (!generated) return [] as ContractSchemaRulesOut[];
    return generated.schemas.filter(
      (s) =>
        s.rules.length > 0 &&
        isFullyQualified(targets[s.schema_name] ?? ""),
    );
  }, [generated, targets]);

  const saveableSchemas = useMemo(() => {
    return schemasWithTarget.filter((s) => {
      const v = validations[s.schema_name];
      return !v || v.status === "ok" || v.status === "loading";
    });
  }, [schemasWithTarget, validations]);

  // Schemas whose target table is missing one or more rule columns —
  // shown as a hard-blocker warning above the save buttons.
  const schemasWithMissingColumns = useMemo(() => {
    return schemasWithTarget.filter(
      (s) => validations[s.schema_name]?.status === "mismatch",
    );
  }, [schemasWithTarget, validations]);

  const hasMissingColumns = schemasWithMissingColumns.length > 0;

  const totalSaveableRules = useMemo(
    () =>
      schemasWithMissingColumns.length > 0
        ? 0
        : saveableSchemas.reduce((acc, s) => acc + s.rules.length, 0),
    [saveableSchemas, schemasWithMissingColumns.length],
  );

  // Save = drafts; submit-for-review = drafts + transition each rule.
  // We reuse source="import" so contract-generated rules go through the
  // same approval pipeline as YAML imports; full ODCS lineage lives in
  // each rule's ``user_metadata.contract_id`` / ``schema`` / ``field``.
  const handleSave = async (alsoSubmit: boolean) => {
    if (hasMissingColumns) {
      toast.error(t("rulesFromContract.errors.missingColumns"));
      return;
    }
    if (saveableSchemas.length === 0) {
      toast.error(t("rulesFromContract.errors.noTargets"));
      return;
    }
    if (alsoSubmit) setIsSubmitting(true);
    else setIsSaving(true);
    try {
      let savedCount = 0;
      let submittedCount = 0;
      let submitFailed = 0;
      for (const schema of saveableSchemas) {
        const tableFqn = (targets[schema.schema_name] ?? "").trim();
        const payload: SaveRulesIn = {
          table_fqn: tableFqn,
          checks: schema.rules,
          source: "import",
        };
        const resp = await saveRules(payload);
        savedCount += resp.data.length;

        if (alsoSubmit) {
          for (const rule of resp.data) {
            if (!rule.rule_id) continue;
            try {
              await submitRuleForApproval(rule.rule_id, null);
              submittedCount++;
            } catch {
              submitFailed++;
            }
          }
        }
      }
      if (alsoSubmit) {
        if (submittedCount > 0) {
          toast.success(
            t("rulesFromContract.submitted", { count: submittedCount }),
          );
        }
        if (submitFailed > 0) {
          toast.error(
            t("rulesFromContract.submitFailed", { count: submitFailed }),
          );
        }
      } else {
        toast.success(t("rulesFromContract.saved", { count: savedCount }));
      }
      onDone();
    } catch (err) {
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
          targets={targets}
          onTargetChange={(name, value) =>
            setTargets((prev) => ({ ...prev, [name]: value }))
          }
          onValidationChange={onValidationChange}
          onSave={() => handleSave(false)}
          onSubmit={() => handleSave(true)}
          saveableSchemas={saveableSchemas}
          schemasWithTargetCount={schemasWithTarget.length}
          schemasWithMissingColumns={schemasWithMissingColumns}
          totalSaveableRules={totalSaveableRules}
          hasMissingColumns={hasMissingColumns}
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
  targets,
  onTargetChange,
  onValidationChange,
  onSave,
  onSubmit,
  saveableSchemas,
  schemasWithTargetCount,
  schemasWithMissingColumns,
  totalSaveableRules,
  hasMissingColumns,
  isSaving,
  isSubmitting,
}: {
  generated: GenerateRulesFromContractOut;
  targets: Record<string, string>;
  onTargetChange: (schemaName: string, value: string) => void;
  onValidationChange: (schemaName: string, value: SchemaValidation) => void;
  onSave: () => void;
  onSubmit: () => void;
  saveableSchemas: ContractSchemaRulesOut[];
  schemasWithTargetCount: number;
  schemasWithMissingColumns: ContractSchemaRulesOut[];
  totalSaveableRules: number;
  hasMissingColumns: boolean;
  isSaving: boolean;
  isSubmitting: boolean;
}) {
  const { t } = useTranslation();
  const { metadata, schemas, total_rules } = generated;
  const warnings = generated.warnings ?? [];
  const unassignedRules = generated.unassigned_rules ?? [];
  const busy = isSaving || isSubmitting;
  const canSave = saveableSchemas.length > 0 && !hasMissingColumns;
  const tablesPicked = schemasWithTargetCount;

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
            <SchemaSection
              key={schema.schema_name}
              schema={schema}
              target={targets[schema.schema_name] ?? ""}
              onTargetChange={(v) => onTargetChange(schema.schema_name, v)}
              onValidationChange={(v) =>
                onValidationChange(schema.schema_name, v)
              }
              disabled={busy}
            />
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

        {/* Global missing-columns blocker. Each affected SchemaSection
            already shows its own per-section warning with the column
            list — here we just surface a compact summary so the user
            knows the disabled save buttons aren't a UI bug. */}
        {hasMissingColumns && (
          <div className="flex items-start gap-2 p-3 rounded-lg border border-destructive/30 bg-destructive/5 text-destructive text-sm">
            <XCircle className="h-4 w-4 mt-0.5 shrink-0" />
            <div className="space-y-1 min-w-0">
              <div className="font-medium text-xs">
                {t("rulesFromContract.results.missingColumnsTitle", {
                  count: schemasWithMissingColumns.length,
                })}
              </div>
              <div className="text-[11px] opacity-80">
                {t("rulesFromContract.results.missingColumnsBody")}
              </div>
              <ul className="text-[11px] mt-1 list-disc pl-4 space-y-0.5">
                {schemasWithMissingColumns.map((s) => (
                  <li key={s.schema_name}>
                    <span className="font-mono">{s.schema_name}</span> →{" "}
                    <span className="font-mono opacity-90">
                      {targets[s.schema_name]}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        )}

        {/* Save actions */}
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="text-xs text-muted-foreground">
            {hasMissingColumns
              ? t("rulesFromContract.results.fixColumnsHint")
              : tablesPicked === 0
                ? t("rulesFromContract.results.pickTargets")
                : t("rulesFromContract.results.saveSummary", {
                    rules: totalSaveableRules,
                    tables: saveableSchemas.length,
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

function SchemaSection({
  schema,
  target,
  onTargetChange,
  onValidationChange,
}: {
  schema: ContractSchemaRulesOut;
  target: string;
  onTargetChange: (v: string) => void;
  onValidationChange: (v: SchemaValidation) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(true);
  const propertyCount = schema.property_count ?? 0;

  // Columns referenced by THIS schema's rules. Stable for the schema —
  // contract output never mutates after generation — but we memoize so
  // the validation effect below doesn't fire on every render.
  const referencedColumns = useMemo(
    () => Array.from(extractColumnRefs(schema.rules)).sort(),
    [schema.rules],
  );

  const parsed = parseFqn(target);

  // useGetTableColumns is auto-disabled when any of catalog/schema/table
  // is falsy, but we also gate on ``isFullyQualified`` to avoid firing
  // a request for half-typed input.
  const {
    data: colsResp,
    isFetching: colsFetching,
    isError: colsError,
  } = useGetTableColumns(
    parsed?.catalog ?? "",
    parsed?.schema ?? "",
    parsed?.table ?? "",
    {
      query: {
        enabled: parsed !== null && referencedColumns.length > 0,
        // Column lists are cheap and stable; don't thrash UC on every
        // re-render. 30s is enough to feel responsive after a schema
        // edit without hammering the discovery endpoint.
        staleTime: 30_000,
        retry: false,
      },
    },
  );

  // Compute the validation outcome and bubble it up to the parent so
  // it can gate the global save buttons.
  const validation = useMemo<SchemaValidation>(() => {
    if (parsed === null) return { status: "idle", missing: [] };
    if (referencedColumns.length === 0) return { status: "ok", missing: [] };
    if (colsFetching && !colsResp) return { status: "loading", missing: [] };
    if (colsError) return { status: "error", missing: [] };
    const tableCols = new Set(
      (colsResp?.data ?? []).map((c) => (c.name ?? "").toLowerCase()),
    );
    const missing = referencedColumns.filter(
      (c) => !tableCols.has(c.toLowerCase()),
    );
    return missing.length === 0
      ? { status: "ok", missing: [] }
      : { status: "mismatch", missing };
  }, [parsed, referencedColumns, colsFetching, colsError, colsResp]);

  useEffect(() => {
    onValidationChange(validation);
  }, [validation, onValidationChange]);

  const showSuggested = !!schema.physical_name && !parsed;

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
        {validation.status === "ok" && parsed && (
          <Badge
            variant="outline"
            className="text-[10px] border-green-500/40 text-green-700 dark:text-green-400"
          >
            <CheckCircle2 className="h-3 w-3 mr-1" />
            {t("rulesFromContract.schema.validationOk")}
          </Badge>
        )}
        {validation.status === "mismatch" && (
          <Badge variant="destructive" className="text-[10px]">
            <AlertTriangle className="h-3 w-3 mr-1" />
            {t("rulesFromContract.schema.validationMismatch", {
              count: validation.missing.length,
            })}
          </Badge>
        )}
        {validation.status === "loading" && (
          <Badge variant="outline" className="text-[10px]">
            <Loader2 className="h-3 w-3 mr-1 animate-spin" />
            {t("rulesFromContract.schema.validationLoading")}
          </Badge>
        )}
        {showSuggested && (
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
          <div className="space-y-2">
            <Label className="text-xs">
              {t("rulesFromContract.schema.targetTable")}
            </Label>
            <CatalogBrowser value={target} onChange={onTargetChange} />
            <p className="text-[11px] text-muted-foreground">
              {t("rulesFromContract.schema.targetTableHint")}
            </p>
          </div>

          {/* Per-schema validation feedback. We intentionally render
              this even when ``open === true`` since it answers the
              question the user is most likely asking: "why is my save
              button greyed out?" */}
          {validation.status === "mismatch" && (
            <div className="flex items-start gap-2 p-3 rounded-lg border border-destructive/30 bg-destructive/5 text-destructive text-sm">
              <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
              <div className="space-y-1 min-w-0">
                <div className="text-xs font-medium">
                  {t("rulesFromContract.schema.missingTitle", {
                    count: validation.missing.length,
                  })}
                </div>
                <div className="text-[11px] opacity-90">
                  {t("rulesFromContract.schema.missingBody", {
                    table: target,
                  })}
                </div>
                <div className="flex flex-wrap gap-1.5 mt-1">
                  {validation.missing.map((col) => (
                    <Badge
                      key={col}
                      variant="outline"
                      className="font-mono text-[10px] border-destructive/40 text-destructive"
                    >
                      {col}
                    </Badge>
                  ))}
                </div>
                <p className="text-[11px] opacity-70 mt-1">
                  {t("rulesFromContract.schema.missingHint")}
                </p>
              </div>
            </div>
          )}
          {validation.status === "error" && (
            <div className="flex items-start gap-2 p-3 rounded-lg border border-amber-500/30 bg-amber-50 dark:bg-amber-950/30 text-amber-700 dark:text-amber-400 text-sm">
              <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
              <div className="space-y-1 min-w-0">
                <div className="text-xs font-medium">
                  {t("rulesFromContract.schema.inspectErrorTitle")}
                </div>
                <div className="text-[11px] opacity-90">
                  {t("rulesFromContract.schema.inspectErrorBody")}
                </div>
              </div>
            </div>
          )}

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
