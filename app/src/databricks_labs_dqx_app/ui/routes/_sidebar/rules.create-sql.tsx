import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { usePermissions } from "@/hooks/use-permissions";
import { PageBreadcrumb } from "@/components/layout/PageBreadcrumb";
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
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Database,
  Save,
  Loader2,
  AlertCircle,
  Plus,
  Trash2,
  Sparkles,
  Play,
  CircleStop,
  Copy,
  X,
} from "lucide-react";
import { toast } from "sonner";
import {
  saveRules,
  type SaveRulesIn,
  useGetRules,
  aiAssistedChecksGeneration,
  useSubmitDryRun,
  useGetDryRunResults,
  type DryRunResultsOut,
} from "@/lib/api";
import { checkDuplicates, type CheckDuplicatesIn, cancelDryRun, getDryRunStatusCustom, useLabelDefinitions } from "@/lib/api-custom";
import { LabelsEditor } from "@/components/Labels";
import { getUserMetadata } from "@/lib/format-utils";
import { useJobPolling } from "@/hooks/use-job-polling";
import { DryRunResults } from "@/components/DryRunResults";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { Switch } from "@/components/ui/switch";
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

interface SearchParams {
  edit?: string;
  from?: string;
}

export const Route = createFileRoute("/_sidebar/rules/create-sql")({
  component: CreateSqlCheckPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    edit: typeof search.edit === "string" ? search.edit : undefined,
    from: typeof search.from === "string" ? search.from : undefined,
  }),
});

const SQL_CHECK_PREFIX = "__sql_check__/";

function resolveTableFqn(check: SqlCheckDraft): string {
  if (check.targetTable.trim()) return check.targetTable.trim();
  return `${SQL_CHECK_PREFIX}${check.name.trim().replace(/\s+/g, "_").toLowerCase()}`;
}

const SQL_NAME_REGEX = /^[a-zA-Z][a-zA-Z0-9_]{0,127}$/;
const SQL_DDL_DML_PATTERN = /\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|MERGE)\b/i;

type TFunc = (key: string, options?: Record<string, unknown>) => string;

function validateCheckName(name: string, t: TFunc): string | null {
  if (!name) return null;
  if (name.length > 128) return t("rulesCreateSql.nameLengthError");
  if (!SQL_NAME_REGEX.test(name))
    return t("rulesCreateSql.nameInvalidError");
  return null;
}

function validateSqlQuery(query: string, t: TFunc): string | null {
  if (!query) return null;
  if (query.includes(";"))
    return t("rulesCreateSql.querySemicolonError");
  if (SQL_DDL_DML_PATTERN.test(query))
    return t("rulesCreateSql.queryProhibitedError");
  return null;
}

function extractApiError(err: unknown, t: TFunc): string {
  const axErr = err as { response?: { data?: { detail?: string }; status?: number }; message?: string };
  if (axErr?.response?.data?.detail) return axErr.response.data.detail;
  if (axErr?.response?.status) return `Server error (HTTP ${axErr.response.status})`;
  if (axErr?.message) return axErr.message;
  return t("common.unknownError");
}

interface SqlCheckDraft {
  id: string;
  name: string;
  query: string;
  criticality: "warn" | "error";
  /**
   * Free-form labels, including the reserved ``weight`` key. All authoring,
   * editing, and round-trip happens through ``user_metadata`` — there is no
   * separate native ``weight`` field on the rule.
   */
  userMetadata: Record<string, string>;
  targetTable: string;
  // When set, a save should ``update_rule(rule_id, ...)`` instead of creating
  // a new entry. Populated when the page loads in edit mode.
  ruleId?: string;
}

function newSqlCheck(): SqlCheckDraft {
  return {
    id: crypto.randomUUID(),
    name: "",
    query: "",
    criticality: "warn",
    userMetadata: {},
    targetTable: "",
  };
}

/**
 * Build the payload sent to the backend for a single SQL check. Used by the
 * dry-run, duplicate-check, and save call sites so the encoding stays
 * consistent (and so we don't reintroduce a top-level ``weight`` field).
 */
function buildSqlCheckPayload(c: SqlCheckDraft): Record<string, unknown> {
  const out: Record<string, unknown> = {
    name: c.name.trim(),
    criticality: c.criticality,
    check: { function: "sql_query", arguments: { query: c.query.trim() } },
  };
  if (Object.keys(c.userMetadata).length > 0) {
    out.user_metadata = { ...c.userMetadata };
  }
  return out;
}

function CreateSqlCheckPage() {
  const { t } = useTranslation();
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  const navigate = useNavigate();
  const { edit: editFqn, from: fromPage } = Route.useSearch();
  const isEditMode = !!editFqn;

  const cancelTarget = fromPage === "active" ? "/rules/active"
    : fromPage === "drafts" ? "/rules/drafts"
    : "/rules/create";

  const [checks, setChecks] = useState<SqlCheckDraft[]>([newSqlCheck()]);
  const [initialized, setInitialized] = useState(false);
  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = labelDefsData?.definitions ?? [];

  const { data: existingRule } = useGetRules(editFqn ?? "", {
    query: { enabled: !!editFqn },
  });

  const existingRuleIds = useMemo(() => {
    if (!existingRule?.data) return [];
    const entries = Array.isArray(existingRule.data) ? existingRule.data : [existingRule.data];
    return entries.map((e) => e.rule_id).filter((id): id is string => typeof id === "string" && id.length > 0);
  }, [existingRule]);

  useEffect(() => {
    if (!existingRule?.data || initialized) return;
    const entries = Array.isArray(existingRule.data) ? existingRule.data : [existingRule.data];
    const tableFqn = entries[0]?.table_fqn ?? "";
    const isAssignedToTable = tableFqn && !tableFqn.startsWith(SQL_CHECK_PREFIX);
    const allChecks = entries.flatMap((e) => e.checks ?? []);
    const loaded: SqlCheckDraft[] = allChecks.map((c: Record<string, unknown>) => {
      const check = (c.check ?? {}) as Record<string, unknown>;
      const args = (check.arguments ?? {}) as Record<string, unknown>;

      // Pull labels from the saved rule. Fold any legacy top-level ``weight``
      // into the labels map so existing rules round-trip cleanly into the new
      // labels-only model.
      const userMetadata = getUserMetadata(c as Record<string, unknown>);
      if (c.weight != null && typeof c.weight === "number" && !("weight" in userMetadata)) {
        userMetadata.weight = String(c.weight);
      }

      // Match each draft up with its saved rule_id so a save becomes an update
      // rather than a new-row insert.
      const matchedEntry = entries.find((e) =>
        Array.isArray(e.checks) && e.checks.some((ec) => (ec as Record<string, unknown>).name === c.name),
      );
      const ruleId = matchedEntry?.rule_id;

      return {
        id: crypto.randomUUID(),
        name: (c.name as string) ?? "",
        query: (args.query as string) ?? "",
        criticality: ((c.criticality as string) === "error" ? "error" : "warn") as "warn" | "error",
        userMetadata,
        targetTable: isAssignedToTable ? tableFqn : "",
        ruleId: typeof ruleId === "string" && ruleId.length > 0 ? ruleId : undefined,
      };
    });
    if (loaded.length > 0) {
      setChecks(loaded);
    }
    setInitialized(true);
  }, [existingRule, initialized]);

  // ── Real-time duplicate detection ──────────────────────────────────
  const [dupCheckIds, setDupCheckIds] = useState<Set<string>>(new Set());
  const [dupChecking, setDupChecking] = useState(false);
  const dupTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const checksFingerprint = useMemo(() => {
    return checks.filter((c) => c.name.trim() && c.query.trim()).map((c) => `${c.id}:${c.name}:${c.query}:${c.targetTable}`).join("|");
  }, [checks]);

  useEffect(() => {
    if (dupTimerRef.current) clearTimeout(dupTimerRef.current);
    const eligible = checks.filter((c) => c.name.trim() && c.query.trim());
    if (eligible.length === 0) { setDupCheckIds(new Set()); return; }

    dupTimerRef.current = setTimeout(async () => {
      setDupChecking(true);
      const dups = new Set<string>();
      for (const check of eligible) {
        const tableFqn = resolveTableFqn(check);
        const payload = [buildSqlCheckPayload(check)];
        try {
          const body: CheckDuplicatesIn = {
            table_fqn: tableFqn,
            checks: payload,
            ...(existingRuleIds.length > 0 && { exclude_rule_ids: existingRuleIds }),
          };
          const resp = await checkDuplicates(body);
          if (resp.data.duplicates.length > 0) dups.add(check.id);
        } catch {
          // ignore
        }
      }
      setDupCheckIds(dups);
      setDupChecking(false);
    }, 600);
    return () => { if (dupTimerRef.current) clearTimeout(dupTimerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checksFingerprint]);

  const hasDuplicates = dupCheckIds.size > 0;

  const addCheck = () => setChecks((prev) => [...prev, newSqlCheck()]);

  const removeCheck = (id: string) => {
    setChecks((prev) => (prev.length <= 1 ? prev : prev.filter((c) => c.id !== id)));
  };

  const updateCheck = (id: string, patch: Partial<SqlCheckDraft>) => {
    setChecks((prev) => prev.map((c) => (c.id === id ? { ...c, ...patch } : c)));
  };

  // AI generation
  const [aiPrompt, setAiPrompt] = useState("");
  const [aiGenerating, setAiGenerating] = useState(false);

  const handleAiGenerate = async () => {
    if (!aiPrompt.trim()) return;
    setAiGenerating(true);
    try {
      const fullPrompt = `Please generate SQL rules: ${aiPrompt.trim()}`;
      const resp = await aiAssistedChecksGeneration({ user_input: fullPrompt });
      const generated = resp.data.checks ?? [];
      const drafts: SqlCheckDraft[] = [];
      for (const raw of generated) {
        const checkObj = (raw.check as Record<string, unknown>) ?? raw;
        const fn = String(checkObj.function ?? "");
        if (fn !== "sql_query") continue;
        const args = (checkObj.arguments as Record<string, unknown>) ?? {};
        const query = String(args.query ?? "");
        if (!query) continue;
        const aiMetadata = getUserMetadata(raw as Record<string, unknown>);
        if (raw.weight != null && typeof raw.weight === "number" && !("weight" in aiMetadata)) {
          aiMetadata.weight = String(raw.weight);
        }
        drafts.push({
          id: crypto.randomUUID(),
          name: String(raw.name ?? args.name ?? ""),
          query,
          criticality: (raw.criticality as string) === "error" ? "error" : "warn",
          userMetadata: aiMetadata,
          targetTable: "",
        });
      }
      if (drafts.length === 0) {
        toast.error(t("rulesCreateSql.noSqlGenerated"));
        return;
      }
      const hasOnlyEmptyDefault = checks.length === 1 && !checks[0].name && !checks[0].query;
      setChecks(hasOnlyEmptyDefault ? drafts : [...checks, ...drafts]);
      toast.success(t("rulesCreateSql.sqlCheckGenerated", { count: drafts.length }));
      setAiPrompt("");
    } catch (err) {
      toast.error(t("rulesCreateSql.aiGenerationFailed", { detail: extractApiError(err, t) }));
    } finally {
      setAiGenerating(false);
    }
  };

  // ── Dry run ──────────────────────────────────────────────────────────
  const [dryRunCheckId, setDryRunCheckId] = useState<string | null>(null);
  const [dryRunResult, setDryRunResult] = useState<DryRunResultsOut | null>(null);
  const [dryRunError, setDryRunError] = useState<string | null>(null);
  const [dryRunJobRunId, setDryRunJobRunId] = useState<number | null>(null);
  const [dryRunRunId, setDryRunRunId] = useState<string | null>(null);
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
            toast.success(t("rulesCreateSql.dryRunComplete"));
          }
        } catch {
          setDryRunError(t("rulesCreateSql.fetchResultsFailed"));
        }
      } else {
        setDryRunError(status.message ?? status.result_state ?? t("rulesCreateSql.unknownDryRunError"));
      }
    },
  });

  const isDryRunning = submitDryRunMutation.isPending || dryRunPolling.isPolling;

  const justSavedRef = useRef(false);

  const hasUnsavedChanges = useMemo(
    () => checks.some((c) => c.name.trim() !== "" || c.query.trim() !== ""),
    [checks],
  );

  const { blocker } = useUnsavedGuard({ hasUnsavedChanges, isRunning: isDryRunning, bypassRef: justSavedRef });

  const handleConfirmLeave = async () => {
    if (isDryRunning && dryRunRunId && dryRunJobRunId !== null) {
      try {
        await cancelDryRun(dryRunRunId, { job_run_id: dryRunJobRunId });
      } catch {
        // best-effort cancel
      }
    }
    blocker.proceed?.();
  };

  const handleDryRun = async (check: SqlCheckDraft) => {
    if (!check.query.trim()) {
      toast.error(t("rulesCreateSql.enterSqlBeforeDryRun"));
      return;
    }
    const queryErr = validateSqlQuery(check.query, t);
    if (queryErr) {
      toast.error(queryErr);
      return;
    }
    if (check.name.trim()) {
      const nameErr = validateCheckName(check.name.trim(), t);
      if (nameErr) {
        toast.error(nameErr);
        return;
      }
    }

    setDryRunResult(null);
    setDryRunError(null);
    setDryRunCheckId(check.id);

    const checkName = check.name.trim() || `dryrun_${Date.now()}`;
    const tableFqn = resolveTableFqn({ ...check, name: checkName });
    const checksPayload = [buildSqlCheckPayload({ ...check, name: checkName })];
    try {
      const resp = await submitDryRunMutation.mutateAsync({
        data: { table_fqn: tableFqn, checks: checksPayload, sample_size: 1000, skip_history: true },
      });
      setDryRunRunId(resp.data.run_id);
      setDryRunJobRunId(resp.data.job_run_id);
      setDryRunViewFqn(resp.data.view_fqn ?? null);
      toast.info(t("rulesCreateSql.dryRunSubmitted"));
    } catch (err) {
      setDryRunError(extractApiError(err, t));
    }
  };

  const handleCancelDryRun = async () => {
    if (!dryRunRunId || dryRunJobRunId === null) return;
    try {
      await cancelDryRun(dryRunRunId, { job_run_id: dryRunJobRunId });
      dryRunPolling.stopPolling();
      setDryRunJobRunId(null);
      setDryRunRunId(null);
      setDryRunCheckId(null);
      toast.info(t("rulesCreateSql.dryRunCanceled"));
    } catch {
      toast.error(t("rulesCreateSql.failedCancelDryRun"));
    }
  };

  const nameErrors = useMemo(() => {
    const map = new Map<string, string>();
    for (const c of checks) {
      const err = validateCheckName(c.name.trim(), t);
      if (err) map.set(c.id, err);
    }
    return map;
  }, [checks, t]);

  const queryErrors = useMemo(() => {
    const map = new Map<string, string>();
    for (const c of checks) {
      const err = validateSqlQuery(c.query, t);
      if (err) map.set(c.id, err);
    }
    return map;
  }, [checks, t]);

  const isValid = checks.every((c) => c.name.trim() !== "" && c.query.trim() !== "")
    && nameErrors.size === 0
    && queryErrors.size === 0;

  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    if (nameErrors.size > 0 || queryErrors.size > 0) {
      toast.error(t("rulesCreateSql.fixValidationErrors"));
      return;
    }
    setSaving(true);
    let successCount = 0;
    let failCount = 0;

    for (const check of checks) {
      const tableFqn = resolveTableFqn(check);
      const payloadEntry = buildSqlCheckPayload(check);
      if (check.ruleId) {
        payloadEntry.rule_id = check.ruleId;
      }
      const checkPayload = [payloadEntry];
      try {
        await saveRules({ table_fqn: tableFqn, checks: checkPayload } as SaveRulesIn);
        successCount++;
      } catch (err) {
        toast.error(t("rulesCreateSql.failedSave", { name: check.name, detail: extractApiError(err, t) }));
        failCount++;
      }
    }

    setSaving(false);

    if (successCount > 0) {
      toast.success(t("rulesCreateSql.savedSqlChecks", { count: successCount }));
    }
    if (successCount > 0 && failCount === 0) {
      justSavedRef.current = true;
      navigate({ to: "/rules/drafts" });
    }
  };

  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: t("rulesCreate.breadcrumb"), to: "/rules/create" }]}
          page={isEditMode ? t("rulesCreateSql.editBreadcrumb") : t("rulesCreateSql.breadcrumb")}
        />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {isEditMode ? t("rulesCreateSql.editTitle") : t("rulesCreateSql.title")}
          </h1>
          <p className="text-muted-foreground">
            {t("rulesCreateSql.subtitle")}
          </p>
        </div>
      </div>

      {/* AI generation */}
      <Card>
        <CardContent className="pt-6">
          <div className="border border-violet-200 dark:border-violet-800 rounded-lg p-4 bg-violet-50/50 dark:bg-violet-950/30 space-y-3">
            <div className="flex items-center gap-2 mb-1">
              <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
              <span className="text-sm font-medium text-violet-900 dark:text-violet-200">{t("rulesCreateSql.generateWithAi")}</span>
            </div>
            <p className="text-xs text-muted-foreground">
              {t("rulesCreateSql.generateWithAiHint")}
            </p>
            <div className="flex gap-2">
              <Textarea
                value={aiPrompt}
                onChange={(e) => setAiPrompt(e.target.value)}
                placeholder={t("rulesCreateSql.generateWithAiPlaceholder")}
                className="min-h-[52px] resize-none text-sm"
                disabled={aiGenerating}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    handleAiGenerate();
                  }
                }}
              />
              <Button
                onClick={handleAiGenerate}
                disabled={aiGenerating || !aiPrompt.trim()}
                className="shrink-0 gap-1.5"
                size="sm"
              >
                {aiGenerating ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Sparkles className="h-3.5 w-3.5" />
                )}
                {aiGenerating ? t("rulesCreateSql.generating") : t("rulesCreateSql.generate")}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="space-y-4">
        {checks.map((check, idx) => {
          const isDup = dupCheckIds.has(check.id);
          return (
          <Card key={check.id} className={`border-l-[3px] ${isDup ? "border-red-300 bg-red-50/30 border-l-red-400" : "border-l-primary/40"}`}>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Database className="h-4 w-4" />
                  {t("rulesCreateSql.sqlCheckTitle")} {checks.length > 1 ? `#${idx + 1}` : ""}
                  {isDup && (
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Badge variant="destructive" className="text-[10px] gap-1">
                            <AlertCircle className="h-2.5 w-2.5" />
                            {t("rulesCreateSql.duplicate")}
                          </Badge>
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>{t("rulesCreateSql.duplicateTooltip")}</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  )}
                </CardTitle>
                <div className="flex items-center gap-1">
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-7 gap-1.5 text-xs"
                    disabled={!check.query.trim() || isDryRunning || saving}
                    onClick={() => handleDryRun(check)}
                  >
                    {isDryRunning && dryRunCheckId === check.id ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <Play className="h-3 w-3" />
                    )}
                    {isDryRunning && dryRunCheckId === check.id ? t("rulesCreateSql.running") : t("rulesCreateSql.dryRun")}
                  </Button>
                  {checks.length > 1 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 w-7 p-0 text-destructive"
                      onClick={() => removeCheck(check.id)}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  )}
                </div>
              </div>
              <CardDescription>
                {t("rulesCreateSql.queryDescription")}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <Label htmlFor={`name-${check.id}`}>{t("common.name")}</Label>
                  <Input
                    id={`name-${check.id}`}
                    placeholder={t("rulesCreateSql.namePlaceholder")}
                    value={check.name}
                    onChange={(e) => updateCheck(check.id, { name: e.target.value })}
                    className={nameErrors.has(check.id) ? "border-red-400 focus-visible:ring-red-400" : ""}
                  />
                  {nameErrors.has(check.id) && (
                    <p className="text-xs text-red-500 flex items-center gap-1">
                      <AlertCircle className="h-3 w-3 shrink-0" />
                      {nameErrors.get(check.id)}
                    </p>
                  )}
                  {!nameErrors.has(check.id) && !check.name.trim() && (
                    <p className="text-xs text-muted-foreground">
                      {t("rulesCreateSql.nameHint")}
                    </p>
                  )}
                </div>
                <div className="space-y-1.5">
                  <Label>{t("common.criticality")}</Label>
                  <Select
                    value={check.criticality}
                    onValueChange={(v) =>
                      updateCheck(check.id, { criticality: v as "warn" | "error" })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="warn">{t("rulesCreateSql.criticalityWarn")}</SelectItem>
                      <SelectItem value="error">{t("rulesCreateSql.criticalityError")}</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <TargetTableSection
                check={check}
                onUpdate={(val) => updateCheck(check.id, { targetTable: val })}
              />

              <div className="space-y-1.5">
                <Label htmlFor={`query-${check.id}`}>{t("rulesCreateSql.sqlQueryLabel")}</Label>
                <Textarea
                  id={`query-${check.id}`}
                  className={`font-mono text-sm min-h-[140px] ${queryErrors.has(check.id) ? "border-red-400 focus-visible:ring-red-400" : ""}`}
                  placeholder={t("rulesCreateSql.sqlQueryPlaceholder")}
                  value={check.query}
                  onChange={(e) => updateCheck(check.id, { query: e.target.value })}
                />
                {queryErrors.has(check.id) ? (
                  <p className="text-xs text-red-500 flex items-center gap-1">
                    <AlertCircle className="h-3 w-3 shrink-0" />
                    {queryErrors.get(check.id)}
                  </p>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    {t("rulesCreateSql.sqlQueryHint")}
                  </p>
                )}
              </div>

              <LabelsEditor
                value={check.userMetadata}
                onChange={(next) => updateCheck(check.id, { userMetadata: next })}
                defaultOpen={Object.keys(check.userMetadata).length > 0}
                definitions={labelDefinitions}
              />

              {dryRunCheckId === check.id && (isDryRunning || dryRunResult || dryRunError) && (
                <div className="space-y-3">
                  <Separator />
                  {isDryRunning && (
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      <span>
                        {t("common.status")}: <span className="font-medium">{dryRunPolling.status?.state ?? "SUBMITTING"}</span>
                      </span>
                      <Button variant="ghost" size="sm" className="h-6 gap-1 text-xs ml-auto" onClick={handleCancelDryRun}>
                        <CircleStop className="h-3 w-3" />
                        {t("common.cancel")}
                      </Button>
                    </div>
                  )}
                  {dryRunError && !isDryRunning && (
                    <div className="rounded-md border border-red-200 bg-red-50 dark:border-red-900 dark:bg-red-950/40 p-3 space-y-2">
                      <div className="flex items-start justify-between gap-2">
                        <div className="flex items-start gap-2 text-sm font-medium text-red-700 dark:text-red-400">
                          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                          {t("rulesCreateSql.dryRunFailed")}
                        </div>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-6 w-6 p-0 text-red-400 hover:text-red-600"
                          onClick={() => { setDryRunError(null); setDryRunCheckId(null); }}
                        >
                          <X className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                      <pre className="text-xs text-red-600 dark:text-red-300 whitespace-pre-wrap break-words max-h-48 overflow-y-auto bg-red-100/60 dark:bg-red-950/60 rounded p-2 font-mono">
                        {dryRunError}
                      </pre>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-6 gap-1 text-xs"
                        onClick={() => {
                          navigator.clipboard.writeText(dryRunError);
                          toast.success(t("rulesCreateSql.errorCopiedToClipboard"));
                        }}
                      >
                        <Copy className="h-3 w-3" />
                        {t("rulesCreateSql.copyError")}
                      </Button>
                    </div>
                  )}
                  {dryRunResult && <DryRunResults result={dryRunResult} />}
                </div>
              )}
            </CardContent>
          </Card>
          );
        })}

        <Button variant="outline" size="sm" onClick={addCheck} className="gap-1">
          <Plus className="h-3 w-3" />
          {t("rulesCreateSql.addAnotherSqlCheck")}
        </Button>
      </div>

      {/* Save bar */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 text-sm text-muted-foreground">
              {!isValid && (
                <>
                  <AlertCircle className="h-4 w-4 text-amber-500" />
                  {t("rulesCreateSql.everyCheckNeedsNameQuery")}
                </>
              )}
              {isValid && hasDuplicates && (
                <>
                  <AlertCircle className="h-4 w-4 text-red-500" />
                  <span className="text-red-600">
                    {t("rulesCreateSql.checksAlreadyExist", { count: dupCheckIds.size })}
                  </span>
                </>
              )}
              {isValid && !hasDuplicates && (
                <span>
                  {t("rulesCreateSql.checksReady", { count: checks.length })}
                  {dupChecking && <Loader2 className="inline h-3 w-3 animate-spin ml-2" />}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                onClick={() => navigate({ to: cancelTarget })}
                disabled={saving}
              >
                {t("common.cancel")}
              </Button>
              <Button
                onClick={handleSave}
                disabled={!isValid || saving || hasDuplicates}
                className="gap-2"
              >
                {saving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Save className="h-4 w-4" />
                )}
                {saving ? t("common.saving") : isEditMode ? t("rulesCreateSql.saveChanges") : t("rulesCreateSql.saveAsDrafts")}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {isDryRunning ? t("rulesCreateSql.dryRunInProgress") : t("common.unsavedChanges")}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {isDryRunning
                ? t("rulesCreateSql.dryRunInProgressDescription")
                : t("rulesCreateSql.unsavedSqlChecks")}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("common.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmLeave}
              className="bg-destructive text-white hover:bg-destructive/90"
            >
              {isDryRunning ? t("rulesCreateSql.leaveCancelDryRun") : t("rulesCreateSql.discardLeave")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function TargetTableSection({
  check,
  onUpdate,
}: {
  check: SqlCheckDraft;
  onUpdate: (value: string) => void;
}) {
  const { t } = useTranslation();
  const [assignToTable, setAssignToTable] = useState(!!check.targetTable);

  useEffect(() => {
    setAssignToTable(!!check.targetTable);
  }, [check.targetTable]);

  const handleToggle = (on: boolean) => {
    setAssignToTable(on);
    if (!on) onUpdate("");
  };

  return (
    <div className="space-y-2 rounded-lg border border-dashed p-3 bg-muted/20">
      <div className="flex items-center gap-3">
        <Switch
          id={`assign-table-${check.id}`}
          checked={assignToTable}
          onCheckedChange={handleToggle}
        />
        <div className="flex-1">
          <Label htmlFor={`assign-table-${check.id}`} className="text-sm font-medium cursor-pointer">
            {t("rulesCreateSql.assignToTable")}
          </Label>
          <p className="text-xs text-muted-foreground mt-0.5">
            {assignToTable
              ? t("rulesCreateSql.assignToTableOn")
              : t("rulesCreateSql.assignToTableOff")}
          </p>
        </div>
      </div>
      {assignToTable && (
        <div className="pl-10">
          <CatalogBrowser
            value={check.targetTable}
            onChange={onUpdate}
          />
        </div>
      )}
    </div>
  );
}
