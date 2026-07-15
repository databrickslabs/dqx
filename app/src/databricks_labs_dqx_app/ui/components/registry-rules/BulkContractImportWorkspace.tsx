import { useCallback, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  FileText,
  Loader2,
  Play,
  Sparkles,
  Upload,
  X,
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
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  applyRuleToTable,
  bulkRegisterMonitoredTables,
  generateRulesFromContract,
  getGetTableColumnsQueryOptions,
  getListMonitoredTablesQueryOptions,
  getListSchemasQueryOptions,
  useListCatalogs,
  useListCheckFunctions,
  type ColumnOut,
  type CreateRegistryRuleIn,
  type CreateRegistryRuleOut,
} from "@/lib/api";
import {
  batchImportRegistryRulesWithDedup,
  batchRecordPendingApplications,
  useLabelDefinitions,
  type RecordPendingApplicationIn,
} from "@/lib/api-custom";
import { normalizeImportedCheck, parseChecksForImport } from "@/lib/import-registry-rules";
import { resolveCriticality, severityValueCriticality } from "@/lib/registry-rule-conversion";
import { orderSeverityValuesForDisplay, RESERVED_SEVERITY_KEY } from "@/components/RegistryRuleBadges";
import { AI_BUTTON_BG } from "@/lib/ai-style";
import { cn } from "@/lib/utils";
import { buildSlotMapping } from "@/lib/slot-mapping";
import {
  aggregateByContract,
  chunk,
  classifyRuleMapping,
  resolveTableFqn,
  type ContractResult,
  type SchemaOutcome,
} from "@/lib/bulk-contract-import";
import { invalidateAfterMonitoredTableChange } from "@/lib/monitored-table-invalidation";
import { invalidateResultsAfterRuleApplicationChange } from "@/lib/results-invalidation";
import { OptionRow } from "@/routes/_sidebar/rules.from-contract";

// Above this many tables we confirm before kicking off the (synchronous,
// per-rule) execute loop, mirroring the ApplyRuleModal "many tables" guard.
const CONFIRM_THRESHOLD = 10;

interface ContractFileInput {
  id: string;
  filename: string;
  text: string;
}

interface SchemaPlanEntry {
  contractId: string;
  filename: string;
  schemaName: string;
  physicalName: string | null;
  tableFqn: string | null;
  ruleInputs: CreateRegistryRuleIn[];
  parseErrors: string[];
  wholeTableCount: number;
  autoCount: number;
  manualCount: number;
  unknownCount: number;
  columnsAvailable: boolean;
  alreadyMonitored: boolean;
}

type Phase = "config" | "preview" | "results";

/** Splits a 3-part "catalog.schema.table" FQN into its parts. */
function splitFqn(fqn: string): [string, string, string] {
  const parts = fqn.split(".");
  return [parts[0] ?? "", parts[1] ?? "", parts[2] ?? ""];
}

function readFileText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (ev) => resolve(String(ev.target?.result ?? ""));
    reader.onerror = () => reject(reader.error ?? new Error("read failed"));
    reader.readAsText(file);
  });
}

/**
 * Bulk contract import (Phase 1 — client-side orchestration).
 *
 * Upload N ODCS contracts, preview a per-table plan (FQN resolution +
 * auto-map coverage), then in one action register the monitored tables,
 * create the registry rules (submitting for review), and auto-apply the ones
 * that come back approved and whose slots name-match the target columns.
 * Everything is sequenced over existing endpoints — same pattern as
 * ApplyRuleModal — so there's no net-new async/job infrastructure.
 */
export function BulkContractImportWorkspace({ onDone }: { onDone: () => void }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const fileInputRef = useRef<HTMLInputElement>(null);

  const [files, setFiles] = useState<ContractFileInput[]>([]);
  const [targetCatalog, setTargetCatalog] = useState<string>("");
  const [targetSchema, setTargetSchema] = useState<string>("");

  const [generatePredefined, setGeneratePredefined] = useState(true);
  const [generateSchemaValidation, setGenerateSchemaValidation] = useState(true);
  const [strictSchema, setStrictSchema] = useState(true);
  const [processTextRules, setProcessTextRules] = useState(false);
  // App-severity axis (Low/Medium/High/Critical), not raw DQX error/warn.
  const [defaultSeverity, setDefaultSeverity] = useState<string>("High");

  const [phase, setPhase] = useState<Phase>("config");
  const [isGenerating, setIsGenerating] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);
  const [plan, setPlan] = useState<SchemaPlanEntry[] | null>(null);
  const [results, setResults] = useState<ContractResult[] | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const { data: catalogsResp, isLoading: catalogsLoading } = useListCatalogs();
  const catalogs = useMemo(() => catalogsResp?.data ?? [], [catalogsResp]);
  const { data: schemasResp, isLoading: schemasLoading } = useQuery(
    getListSchemasQueryOptions(targetCatalog, { query: { enabled: targetCatalog !== "" } }),
  );
  const schemas = useMemo(() => schemasResp?.data ?? [], [schemasResp]);

  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = useMemo(() => labelDefsData?.definitions ?? [], [labelDefsData]);
  const severityValues = useMemo(
    () =>
      orderSeverityValuesForDisplay(
        labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? [],
      ),
    [labelDefinitions],
  );
  const severityVc = useMemo(() => severityValueCriticality(labelDefinitions), [labelDefinitions]);

  const busy = isGenerating || isExecuting;

  // ── File handling ────────────────────────────────────────────────────────
  const addFiles = useCallback(async (fileList: FileList | File[]) => {
    const incoming = Array.from(fileList);
    if (incoming.length === 0) return;
    const read = await Promise.all(
      incoming.map(async (f) => ({
        id: `${f.name}-${f.size}-${f.lastModified}-${crypto.randomUUID()}`,
        filename: f.name,
        text: await readFileText(f),
      })),
    );
    setFiles((prev) => [...prev, ...read]);
  }, []);

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target;
    if (input.files) void addFiles(input.files);
    input.value = "";
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    if (busy) return;
    if (e.dataTransfer.files) void addFiles(e.dataTransfer.files);
  };

  const removeFile = (id: string) => setFiles((prev) => prev.filter((f) => f.id !== id));

  const handleCatalogChange = (value: string) => {
    setTargetCatalog(value);
    setTargetSchema("");
  };

  // ── Preview ──────────────────────────────────────────────────────────────
  const buildPreview = async () => {
    if (files.length === 0) {
      toast.error(t("rulesBulkImport.errors.noFiles"));
      return;
    }
    if (!targetCatalog || !targetSchema) {
      toast.error(t("rulesBulkImport.errors.noTarget"));
      return;
    }
    if (checkFunctions.length === 0) {
      toast.error(t("rulesBulkImport.errors.previewFailed"));
      return;
    }
    setIsGenerating(true);
    setResults(null);
    try {
      const monitored = await queryClient.fetchQuery(getListMonitoredTablesQueryOptions({}));
      const monitoredFqns = new Set((monitored.data ?? []).map((s) => s.table.table_fqn));
      const columnsCache = new Map<string, ColumnOut[] | null>();
      const entries: SchemaPlanEntry[] = [];

      for (const file of files) {
        let generated;
        try {
          const resp = await generateRulesFromContract({
            contract_text: file.text,
            generate_predefined_rules: generatePredefined,
            process_text_rules: processTextRules,
            generate_schema_validation: generateSchemaValidation,
            strict_schema_validation: strictSchema,
            default_criticality: resolveCriticality(defaultSeverity, severityVc),
          });
          generated = resp.data;
        } catch (err) {
          const detail =
            (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
            t("rulesBulkImport.errors.generateFailedFor", { filename: file.filename });
          toast.error(detail);
          continue;
        }

        for (const schema of generated.schemas) {
          const normalizedRules = schema.rules.map((r) =>
            normalizeImportedCheck(r as Record<string, unknown>),
          );
          const { rules: ruleInputs, errors: parseErrors } = parseChecksForImport(
            normalizedRules,
            checkFunctions,
            t,
            "human",
          );
          const tableFqn = resolveTableFqn({
            schemaName: schema.schema_name,
            physicalName: schema.physical_name,
            targetCatalog,
            targetSchema,
          });

          let columns: ColumnOut[] | null = null;
          if (tableFqn) {
            if (columnsCache.has(tableFqn)) {
              columns = columnsCache.get(tableFqn) ?? null;
            } else {
              try {
                const [c, s, tbl] = splitFqn(tableFqn);
                const colResp = await queryClient.fetchQuery(getGetTableColumnsQueryOptions(c, s, tbl));
                columns = colResp.data ?? [];
              } catch {
                columns = null;
              }
              columnsCache.set(tableFqn, columns);
            }
          }

          let wholeTableCount = 0;
          let autoCount = 0;
          let manualCount = 0;
          let unknownCount = 0;
          for (const ri of ruleInputs) {
            const status = classifyRuleMapping(ri.definition.slots ?? [], columns);
            if (status === "whole-table") wholeTableCount += 1;
            else if (status === "auto") autoCount += 1;
            else if (status === "manual") manualCount += 1;
            else unknownCount += 1;
          }

          entries.push({
            contractId: file.id,
            filename: file.filename,
            schemaName: schema.schema_name,
            physicalName: schema.physical_name ?? null,
            tableFqn,
            ruleInputs,
            parseErrors,
            wholeTableCount,
            autoCount,
            manualCount,
            unknownCount,
            columnsAvailable: columns !== null,
            alreadyMonitored: tableFqn ? monitoredFqns.has(tableFqn) : false,
          });
        }
      }

      if (entries.length === 0) {
        toast.error(t("rulesBulkImport.errors.previewFailed"));
        return;
      }
      setPlan(entries);
      setPhase("preview");
    } finally {
      setIsGenerating(false);
    }
  };

  // ── Execute ──────────────────────────────────────────────────────────────
  const runExecute = async () => {
    if (!plan) return;
    setIsExecuting(true);
    const toastId = toast.loading(t("rulesBulkImport.toast.executing"));
    try {
      const resolvedFqns = [...new Set(plan.map((e) => e.tableFqn).filter((f): f is string => !!f))];
      let registeredSet = new Set<string>();
      let skippedSet = new Set<string>();
      if (resolvedFqns.length > 0) {
        try {
          const bulkResp = await bulkRegisterMonitoredTables({ table_fqns: resolvedFqns });
          registeredSet = new Set(bulkResp.data.registered ?? []);
          // Tables already under monitoring — reported separately so the
          // results don't read "0 registered" for a fully pre-monitored bundle.
          skippedSet = new Set(bulkResp.data.skipped_existing ?? []);
        } catch {
          // Registration failed wholesale — per-schema loop still creates the
          // registry rules; they just can't be applied without a binding.
        }
      }

      // The monitored-tables list was cached during preview; force a refetch so
      // the just-registered bindings are visible to the FQN→binding lookup.
      invalidateAfterMonitoredTableChange(queryClient);
      const monitored = await queryClient.fetchQuery(getListMonitoredTablesQueryOptions({}));
      const bindingByFqn = new Map(
        (monitored.data ?? []).map((s) => [s.table.table_fqn, s.table.binding_id]),
      );

      const columnsCache = new Map<string, ColumnOut[] | null>();
      const outcomes: SchemaOutcome[] = [];
      let anyApplied = false;
      // Applications for rules that came back pending_approval — pre-staged
      // (Phase 2) so the backend auto-applies them the moment the rule is
      // approved, instead of silently dropping the intended binding+mapping.
      const pendingRecords: RecordPendingApplicationIn[] = [];

      for (const entry of plan) {
        const bindingId = entry.tableFqn ? bindingByFqn.get(entry.tableFqn) ?? null : null;
        const outcome: SchemaOutcome = {
          contractId: entry.contractId,
          filename: entry.filename,
          tableFqn: entry.tableFqn,
          bindingId,
          registered: entry.tableFqn && registeredSet.has(entry.tableFqn) ? 1 : 0,
          alreadyMonitored: entry.tableFqn && skippedSet.has(entry.tableFqn) ? 1 : 0,
          created: 0,
          reused: 0,
          applied: 0,
          pending: 0,
          needsMapping: 0,
          failed: entry.parseErrors.length,
          errors: [...entry.parseErrors],
        };

        for (const ruleChunk of chunk(entry.ruleInputs)) {
          // Each entry is tagged so the created-vs-reused split is tracked while
          // the apply/pre-stage handling stays identical for both.
          let processed: { rule: CreateRegistryRuleOut["rule"]; reused: boolean }[];
          try {
            // skip_duplicates keeps re-imports idempotent: a structurally
            // identical active rule is reused instead of creating a copy.
            const resp = await batchImportRegistryRulesWithDedup({
              rules: ruleChunk,
              also_submit: true,
              skip_duplicates: true,
            });
            const created = resp.data.created ?? [];
            const reused = resp.data.reused ?? [];
            processed = [
              ...created.map((cr) => ({ rule: cr.rule, reused: false })),
              ...reused.map((cr) => ({ rule: cr.rule, reused: true })),
            ];
            const failed = resp.data.failed ?? [];
            // Surface the real (server-sanitized) per-rule reason instead of a
            // bare count — the backend already scrubs unexpected errors down to
            // a generic message, so these strings are safe to show.
            for (const f of failed) {
              outcome.failed += 1;
              const rawName = ruleChunk[f.index]?.user_metadata?.["name"];
              const name = typeof rawName === "string" && rawName ? rawName : null;
              outcome.errors.push(
                name
                  ? t("rulesBulkImport.errors.ruleCreateFailedNamed", { name, error: f.error })
                  : t("rulesBulkImport.errors.ruleCreateFailedDetail", { error: f.error }),
              );
            }
          } catch {
            outcome.failed += ruleChunk.length;
            outcome.errors.push(t("rulesBulkImport.errors.batchFailed"));
            continue;
          }

          for (const entryRule of processed) {
            const rule = entryRule.rule;
            if (entryRule.reused) outcome.reused += 1;
            else outcome.created += 1;

            // Resolve the column mapping once — shared by the auto-apply path
            // (approved rules) and the pre-stage path (pending rules). A null
            // mapping (no binding or unmatched slots) always means "needs
            // manual mapping"; there's nothing safe to apply or pre-stage.
            const slots = rule.definition.slots ?? [];
            let mapping: Record<string, string> | null;
            if (!bindingId) {
              mapping = null;
            } else if (slots.length === 0) {
              mapping = {};
            } else {
              let cols = columnsCache.get(entry.tableFqn as string);
              if (cols === undefined) {
                try {
                  const [c, s, tbl] = splitFqn(entry.tableFqn as string);
                  const colResp = await queryClient.fetchQuery(getGetTableColumnsQueryOptions(c, s, tbl));
                  cols = colResp.data ?? [];
                } catch {
                  cols = null;
                }
                columnsCache.set(entry.tableFqn as string, cols);
              }
              mapping = cols === null ? null : buildSlotMapping(slots, cols);
            }

            if (mapping === null) {
              outcome.needsMapping += 1;
              continue;
            }

            if (rule.status !== "approved") {
              // Rule is awaiting approval — pre-stage the application so it
              // activates automatically when the rule is approved.
              pendingRecords.push({
                binding_id: bindingId as string,
                rule_id: rule.rule_id,
                column_mapping: [mapping],
              });
              outcome.pending += 1;
              continue;
            }

            try {
              await applyRuleToTable(bindingId as string, {
                rule_id: rule.rule_id,
                column_mapping: [mapping],
              });
              outcome.applied += 1;
              anyApplied = true;
            } catch {
              outcome.failed += 1;
              outcome.errors.push(t("rulesBulkImport.errors.applyFailed", { fqn: entry.tableFqn }));
            }
          }
        }
        outcomes.push(outcome);
      }

      // Persist the pre-staged (pending-approval) applications in chunks. A
      // failure here is best-effort: the rules were still created + submitted,
      // so it downgrades the pending rows to a manual follow-up rather than
      // failing the whole import.
      if (pendingRecords.length > 0) {
        for (const recordChunk of chunk(pendingRecords)) {
          try {
            await batchRecordPendingApplications({ applications: recordChunk });
          } catch {
            // Non-fatal — surfaced only in logs; the pending counts still
            // point the user at the tables that need a follow-up approval.
          }
        }
      }

      toast.dismiss(toastId);
      if (anyApplied) invalidateResultsAfterRuleApplicationChange(queryClient);
      setResults(aggregateByContract(outcomes));
      setPhase("results");
      toast.success(t("rulesBulkImport.toast.done"));
    } catch {
      toast.dismiss(toastId);
      toast.error(t("rulesBulkImport.errors.executeFailed"));
    } finally {
      setIsExecuting(false);
    }
  };

  const handleExecuteClick = () => {
    if (!plan) return;
    const tableCount = new Set(plan.map((e) => e.tableFqn).filter(Boolean)).size;
    if (tableCount > CONFIRM_THRESHOLD) {
      setConfirmOpen(true);
      return;
    }
    void runExecute();
  };

  const startOver = () => {
    setPhase("config");
    setPlan(null);
    setResults(null);
    setFiles([]);
  };

  // ── Derived preview totals ─────────────────────────────────────────────────
  const planTotals = useMemo(() => {
    if (!plan) return { tables: 0, rules: 0, unresolved: 0 };
    const tables = new Set(plan.map((e) => e.tableFqn).filter(Boolean)).size;
    const rules = plan.reduce((n, e) => n + e.ruleInputs.length, 0);
    const unresolved = plan.filter((e) => !e.tableFqn).length;
    return { tables, rules, unresolved };
  }, [plan]);

  // ── Render ─────────────────────────────────────────────────────────────────
  if (phase === "results" && results) {
    return <BulkResults results={results} onStartOver={startOver} onDone={onDone} />;
  }

  if (phase === "preview" && plan) {
    return (
      <>
        <BulkPreview
          plan={plan}
          totals={planTotals}
          onBack={() => setPhase("config")}
          onExecute={handleExecuteClick}
          isExecuting={isExecuting}
        />
        <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>
                {t("rulesBulkImport.confirmTitle", { count: planTotals.tables })}
              </AlertDialogTitle>
              <AlertDialogDescription>
                {t("rulesBulkImport.confirmDescription", { count: planTotals.tables })}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>{t("common.cancel")}</AlertDialogCancel>
              <AlertDialogAction
                onClick={() => {
                  setConfirmOpen(false);
                  void runExecute();
                }}
              >
                {t("rulesBulkImport.confirmAction")}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </>
    );
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <FileText className="h-4 w-4" />
            {t("rulesBulkImport.upload.title")}
          </CardTitle>
          <CardDescription>{t("rulesBulkImport.upload.description")}</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div
            onDrop={handleDrop}
            onDragOver={(e) => e.preventDefault()}
            className="flex flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed p-6 text-center transition-colors hover:bg-muted/30"
          >
            <Upload className="h-6 w-6 text-muted-foreground" />
            <div className="text-sm text-muted-foreground">
              {t("rulesBulkImport.upload.dropzone")}
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => fileInputRef.current?.click()}
              className="gap-2"
              disabled={busy}
            >
              <Upload className="h-4 w-4" />
              {t("rulesBulkImport.upload.chooseFiles")}
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".yaml,.yml,.json"
              multiple
              onChange={handleFileInput}
              className="hidden"
            />
          </div>

          {files.length === 0 ? (
            <p className="text-xs text-muted-foreground italic">
              {t("rulesBulkImport.upload.empty")}
            </p>
          ) : (
            <div className="space-y-2">
              <Label>{t("rulesBulkImport.upload.fileCount", { count: files.length })}</Label>
              <ul className="space-y-1.5">
                {files.map((f) => (
                  <li
                    key={f.id}
                    className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm"
                  >
                    <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    <span className="flex-1 truncate font-mono text-xs">{f.filename}</span>
                    <button
                      type="button"
                      onClick={() => removeFile(f.id)}
                      disabled={busy}
                      className="text-muted-foreground hover:text-destructive disabled:opacity-50"
                      aria-label={t("rulesBulkImport.upload.removeFile")}
                    >
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t("rulesBulkImport.target.title")}</CardTitle>
          <CardDescription>{t("rulesBulkImport.target.description")}</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label>{t("rulesBulkImport.target.catalog")}</Label>
            <Select value={targetCatalog} onValueChange={handleCatalogChange} disabled={busy}>
              <SelectTrigger>
                <SelectValue
                  placeholder={
                    catalogsLoading
                      ? t("common.loading")
                      : t("rulesBulkImport.target.selectCatalog")
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {catalogs.map((c) => (
                  <SelectItem key={c.name} value={c.name}>
                    {c.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>{t("rulesBulkImport.target.schema")}</Label>
            <Select
              value={targetSchema}
              onValueChange={setTargetSchema}
              disabled={busy || !targetCatalog}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={
                    !targetCatalog
                      ? t("rulesBulkImport.target.selectCatalogFirst")
                      : schemasLoading
                        ? t("common.loading")
                        : t("rulesBulkImport.target.selectSchema")
                  }
                />
              </SelectTrigger>
              <SelectContent>
                {schemas.map((s) => (
                  <SelectItem key={s.name} value={s.name}>
                    {s.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">{t("rulesBulkImport.options.title")}</CardTitle>
          <CardDescription>{t("rulesBulkImport.options.description")}</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
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
              ai
              label={t("rulesFromContract.options.textRules")}
              hint={t("rulesFromContract.options.textRulesHint")}
            />
            <div className="flex items-start gap-3 rounded-lg border p-3">
              <div className="flex-1 space-y-1">
                <div className="text-sm font-medium">
                  {t("rulesFromContract.options.severity")}
                </div>
                <div className="text-xs text-muted-foreground">
                  {t("rulesFromContract.options.severityHint")}
                </div>
              </div>
              <Select
                value={defaultSeverity}
                onValueChange={setDefaultSeverity}
                disabled={busy}
              >
                <SelectTrigger className="h-8 w-[130px] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {severityValues.map((sev) => (
                    <SelectItem key={sev} value={sev}>
                      {sev}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <Separator />

          <div className="flex items-center justify-end">
            <Button onClick={buildPreview} disabled={busy} className={cn("gap-2", AI_BUTTON_BG)}>
              {isGenerating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Sparkles className="h-4 w-4" />
              )}
              {t("rulesBulkImport.generate")}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Preview
// ──────────────────────────────────────────────────────────────────────────────

function BulkPreview({
  plan,
  totals,
  onBack,
  onExecute,
  isExecuting,
}: {
  plan: SchemaPlanEntry[];
  totals: { tables: number; rules: number; unresolved: number };
  onBack: () => void;
  onExecute: () => void;
  isExecuting: boolean;
}) {
  const { t } = useTranslation();

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <CheckCircle2 className="h-4 w-4 text-green-600" />
            {t("rulesBulkImport.preview.title")}
          </CardTitle>
          <CardDescription>
            {t("rulesBulkImport.preview.description")}
            {" · "}
            {t("rulesBulkImport.preview.totalSummary", {
              tables: totals.tables,
              rules: totals.rules,
            })}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {totals.unresolved > 0 && (
            <div className="flex items-start gap-2 rounded-lg bg-amber-50 p-3 text-sm text-amber-700 dark:bg-amber-950/30 dark:text-amber-400">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
              <p className="text-xs">{t("rulesBulkImport.preview.unresolvedWarning")}</p>
            </div>
          )}

          <div className="overflow-auto rounded-lg border">
            <table className="w-full text-xs">
              <thead>
                <tr className="sticky top-0 border-b bg-muted/50">
                  <th className="p-2 text-left font-medium">
                    {t("rulesBulkImport.preview.headerContract")}
                  </th>
                  <th className="p-2 text-left font-medium">
                    {t("rulesBulkImport.preview.headerTable")}
                  </th>
                  <th className="p-2 text-left font-medium">
                    {t("rulesBulkImport.preview.headerRules")}
                  </th>
                  <th className="p-2 text-left font-medium">
                    {t("rulesBulkImport.preview.headerCoverage")}
                  </th>
                  <th className="p-2 text-left font-medium">
                    {t("rulesBulkImport.preview.headerStatus")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {plan.map((e, i) => (
                  <tr key={`${e.contractId}-${e.schemaName}-${i}`} className="border-b last:border-b-0">
                    <td className="max-w-[180px] truncate p-2 font-mono">{e.filename}</td>
                    <td className="max-w-[240px] p-2 font-mono">
                      {e.tableFqn ?? (
                        <span className="italic text-muted-foreground/60">
                          {e.schemaName} — {t("rulesBulkImport.preview.unresolved")}
                        </span>
                      )}
                    </td>
                    <td className="p-2">{e.ruleInputs.length}</td>
                    <td className="p-2">
                      {!e.tableFqn || !e.columnsAvailable ? (
                        <span className="text-muted-foreground">
                          {t("rulesBulkImport.preview.coverageUnknown")}
                        </span>
                      ) : (
                        <span>
                          {t("rulesBulkImport.preview.coverageAuto", {
                            auto: e.autoCount + e.wholeTableCount,
                            total: e.ruleInputs.length,
                          })}
                          {e.manualCount > 0 && (
                            <span className="ml-1 text-amber-600 dark:text-amber-400">
                              {" · "}
                              {t("rulesBulkImport.preview.coverageManual", { count: e.manualCount })}
                            </span>
                          )}
                        </span>
                      )}
                    </td>
                    <td className="p-2">
                      {!e.tableFqn ? (
                        <Badge variant="outline" className="text-[10px] text-amber-600">
                          {t("rulesBulkImport.preview.unresolved")}
                        </Badge>
                      ) : e.alreadyMonitored ? (
                        <Badge variant="secondary" className="text-[10px]">
                          {t("rulesBulkImport.preview.tableExisting")}
                        </Badge>
                      ) : (
                        <Badge variant="outline" className="text-[10px]">
                          {t("rulesBulkImport.preview.tableNew")}
                        </Badge>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <p className="text-xs text-muted-foreground">{t("rulesBulkImport.preview.applyNote")}</p>

          <div className="flex items-center justify-between gap-3">
            <Button variant="outline" onClick={onBack} disabled={isExecuting} className="gap-2" size="sm">
              <ArrowLeft className="h-4 w-4" />
              {t("rulesBulkImport.back")}
            </Button>
            <Button onClick={onExecute} disabled={isExecuting} className="gap-2">
              {isExecuting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              {t("rulesBulkImport.execute")}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Results
// ──────────────────────────────────────────────────────────────────────────────

function BulkResults({
  results,
  onStartOver,
  onDone,
}: {
  results: ContractResult[];
  onStartOver: () => void;
  onDone: () => void;
}) {
  const { t } = useTranslation();

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-base">
            <CheckCircle2 className="h-4 w-4 text-green-600" />
            {t("rulesBulkImport.results.title")}
          </CardTitle>
          <CardDescription>{t("rulesBulkImport.results.description")}</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {results.map((r) => (
            <ContractResultRow key={r.contractId} result={r} />
          ))}

          <Separator />

          <div className="flex items-center justify-end gap-2">
            <Button variant="outline" onClick={onStartOver} size="sm">
              {t("rulesBulkImport.results.startOver")}
            </Button>
            <Button onClick={onDone} size="sm">
              {t("rulesBulkImport.results.done")}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function ContractResultRow({ result }: { result: ContractResult }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const hasDetail = result.errors.length > 0 || result.attentionTables.length > 0;

  return (
    <div className="rounded-lg border">
      <button
        type="button"
        onClick={() => hasDetail && setOpen((v) => !v)}
        className={
          "flex w-full items-center gap-2 p-3 text-left transition-colors " +
          (hasDetail ? "hover:bg-muted/30" : "cursor-default")
        }
      >
        {hasDetail ? (
          open ? (
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )
        ) : (
          <span className="w-4" />
        )}
        <span className="flex-1 truncate font-mono text-sm">{result.filename}</span>
        <div className="flex flex-wrap items-center gap-1.5">
          <Stat label={t("rulesBulkImport.results.registered")} value={result.registered} />
          {result.alreadyMonitored > 0 && (
            <Stat
              label={t("rulesBulkImport.results.alreadyMonitored")}
              value={result.alreadyMonitored}
            />
          )}
          <Stat label={t("rulesBulkImport.results.created")} value={result.created} />
          {result.reused > 0 && (
            <Stat label={t("rulesBulkImport.results.reused")} value={result.reused} />
          )}
          <Stat label={t("rulesBulkImport.results.applied")} value={result.applied} tone="good" />
          {result.pending > 0 && (
            <Stat label={t("rulesBulkImport.results.pending")} value={result.pending} tone="warn" />
          )}
          {result.needsMapping > 0 && (
            <Stat
              label={t("rulesBulkImport.results.needsMapping")}
              value={result.needsMapping}
              tone="warn"
            />
          )}
          {result.failed > 0 && (
            <Stat label={t("rulesBulkImport.results.failed")} value={result.failed} tone="bad" />
          )}
        </div>
      </button>

      {open && hasDetail && (
        <div className="space-y-3 border-t p-3">
          {result.attentionTables.length > 0 && (
            <div className="space-y-1.5">
              <div className="text-xs font-medium">{t("rulesBulkImport.results.attention")}</div>
              <p className="text-[11px] text-muted-foreground">
                {t("rulesBulkImport.results.attentionHint")}
              </p>
              <ul className="space-y-1">
                {result.attentionTables.map((tbl) => (
                  <li key={tbl.fqn} className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs">
                    <span className="font-mono text-muted-foreground">{tbl.fqn}</span>
                    {tbl.pending > 0 && (
                      <span className="text-amber-700 dark:text-amber-400">
                        {t("rulesBulkImport.results.attentionPending", { count: tbl.pending })}
                      </span>
                    )}
                    {tbl.needsMapping > 0 && (
                      <span className="text-amber-700 dark:text-amber-400">
                        {t("rulesBulkImport.results.attentionNeedsMapping", { count: tbl.needsMapping })}
                      </span>
                    )}
                    {tbl.bindingId ? (
                      <Link
                        to="/monitored-tables/$bindingId"
                        params={{ bindingId: tbl.bindingId }}
                        search={{ tab: "apply-rules" }}
                        className="inline-flex items-center gap-1 text-primary hover:underline"
                      >
                        {t("rulesBulkImport.results.openApplyTab")}
                        <ExternalLink className="h-3 w-3" />
                      </Link>
                    ) : null}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {result.errors.length > 0 && (
            <div className="space-y-1">
              <div className="flex items-center gap-1.5 text-xs font-medium text-destructive">
                <XCircle className="h-3.5 w-3.5" />
                {t("rulesBulkImport.results.errors")}
              </div>
              <ul className="list-disc space-y-0.5 pl-5 text-xs text-muted-foreground">
                {result.errors.map((err, i) => (
                  <li key={i}>{err}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone?: "good" | "warn" | "bad";
}) {
  const toneClass =
    tone === "good"
      ? "bg-green-100 text-green-700 dark:bg-green-950/40 dark:text-green-400"
      : tone === "warn"
        ? "bg-amber-100 text-amber-700 dark:bg-amber-950/40 dark:text-amber-400"
        : tone === "bad"
          ? "bg-destructive/10 text-destructive"
          : "bg-muted text-muted-foreground";
  return (
    <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${toneClass}`}>
      {value} {label}
    </span>
  );
}
