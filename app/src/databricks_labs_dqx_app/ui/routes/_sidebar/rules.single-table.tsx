import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import { useState, useCallback, useEffect, useMemo, useRef } from "react";
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
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { DryRunResults } from "@/components/DryRunResults";
import {
  Sparkles,
  Plus,
  Trash2,
  Save,
  Loader2,
  AlertCircle,
  X,
  ChevronDown,
  ChevronUp,
  Table2,
  Play,
  SendHorizonal,
} from "lucide-react";
import { toast } from "sonner";
import {
  aiAssistedChecksGeneration,
  type SaveRulesIn,
  saveRules,
  useSubmitDryRun,
  useGetDryRunResults,
  useGetRules,
  getGetRulesQueryKey,
  type DryRunResultsOut,
} from "@/lib/api";
import { useQueryClient } from "@tanstack/react-query";
import { filterTablesByColumns, checkDuplicates, type CheckDuplicatesIn, submitRuleForApproval, cancelDryRun, getDryRunStatusCustom } from "@/lib/api-custom";
import { useJobPolling } from "@/hooks/use-job-polling";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
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

// ──────────────────────────────────────────────────────────────────────────────
// Types & Constants
// ──────────────────────────────────────────────────────────────────────────────

type SearchParams = { table?: string; rule_id?: string; from?: string };

const CHECK_FUNCTIONS = [
  { value: "is_not_null", label: "is_not_null", args: ["col_name"] },
  { value: "is_not_empty", label: "is_not_empty", args: ["col_name"] },
  { value: "is_not_null_and_not_empty", label: "is_not_null_and_not_empty", args: ["col_name"] },
  { value: "is_in_list", label: "is_in_list", args: ["col_name", "allowed"] },
  { value: "is_not_in_list", label: "is_not_in_list", args: ["col_name", "forbidden"] },
  { value: "is_min", label: "is_min", args: ["col_name", "limit"] },
  { value: "is_max", label: "is_max", args: ["col_name", "limit"] },
  { value: "is_in_range", label: "is_in_range", args: ["col_name", "min_limit", "max_limit"] },
  { value: "is_valid_date", label: "is_valid_date", args: ["col_name", "date_format"] },
  { value: "is_valid_timestamp", label: "is_valid_timestamp", args: ["col_name", "timestamp_format"] },
  { value: "is_valid_regex", label: "is_valid_regex", args: ["col_name", "regex"] },
  { value: "is_unique", label: "is_unique", args: ["col_name"] },
  { value: "is_not_negative", label: "is_not_negative", args: ["col_name"] },
  { value: "sql_expression", label: "sql_expression", args: ["expression", "msg"] },
];

interface CheckDraft {
  id: string;
  fn: string;
  args: Record<string, string>;
  criticality: "warn" | "error";
  weight: number;
  targetTables: string[];
  ruleId?: string;
}

function newCheck(): CheckDraft {
  return {
    id: crypto.randomUUID(),
    fn: "",
    args: {},
    criticality: "warn",
    weight: 3,
    targetTables: [],
  };
}

function checkToDict(c: CheckDraft): Record<string, unknown> {
  const args: Record<string, unknown> = {};
  const fnDef = CHECK_FUNCTIONS.find((f) => f.value === c.fn);
  const isKnownFn = !!fnDef;
  const declaredArgs = new Set(fnDef?.args ?? []);
  for (const [k, v] of Object.entries(c.args)) {
    if (!v) continue;
    if (k === "col_name" && isKnownFn && !declaredArgs.has("col_name")) continue;
    if (c.fn === "is_unique" && k === "col_name") {
      args["columns"] = v.split(",").map((s) => s.trim()).filter(Boolean);
      continue;
    }
    const key = UI_TO_ENGINE_ARG_MAP[k] ?? k;
    if (key === "allowed" || key === "forbidden") {
      args[key] = v.split(",").map((s) => s.trim()).filter(Boolean);
    } else if (key === "limit" || key === "min_limit" || key === "max_limit") {
      args[key] = Number(v) || v;
    } else {
      args[key] = v;
    }
  }
  return { criticality: c.criticality, weight: c.weight, check: { function: c.fn, arguments: args } };
}

function getCheckColumns(c: CheckDraft): string[] {
  const colName = c.args["col_name"];
  if (!colName) return [];
  if (c.fn === "is_unique") {
    return colName.split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [colName];
}

const AI_ARG_KEY_MAP: Record<string, string> = { column: "col_name", columns: "col_name" };
const UI_TO_ENGINE_ARG_MAP: Record<string, string> = { col_name: "column" };

function aiCheckToDraft(raw: Record<string, unknown>): CheckDraft | null {
  // The AI may return {check: {function, arguments}, criticality} or {function, arguments, criticality}
  const checkObj = (raw.check as Record<string, unknown>) ?? raw;
  const fn = String(checkObj.function ?? "");
  if (!fn) return null;
  const rawArgs = (checkObj.arguments as Record<string, unknown>) ?? {};
  const args: Record<string, string> = {};
  for (const [k, v] of Object.entries(rawArgs)) {
    const key = AI_ARG_KEY_MAP[k] ?? k;
    if (Array.isArray(v)) {
      args[key] = v.join(", ");
    } else if (v != null) {
      args[key] = String(v);
    }
  }
  const criticality = (raw.criticality as string) ?? (checkObj.criticality as string) ?? "warn";
  return {
    id: crypto.randomUUID(),
    fn,
    args,
    criticality: criticality === "error" ? "error" : "warn",
    weight: typeof raw.weight === "number" ? raw.weight : 3,
    targetTables: [],
  };
}

/** Convert a saved rule dict back into a CheckDraft for editing. */
function savedCheckToDraft(
  raw: Record<string, unknown>,
  tableFqn: string,
): CheckDraft | null {
  const draft = aiCheckToDraft(raw);
  if (!draft) return null;
  draft.targetTables = [tableFqn];
  return draft;
}

// ──────────────────────────────────────────────────────────────────────────────
// Route
// ──────────────────────────────────────────────────────────────────────────────

export const Route = createFileRoute("/_sidebar/rules/single-table")({
  component: UnifiedRulesPage,
  validateSearch: (search: Record<string, unknown>): SearchParams => ({
    table: (search.table as string) || undefined,
    rule_id: (search.rule_id as string) || undefined,
    from: (search.from as string) || undefined,
  }),
});

// ──────────────────────────────────────────────────────────────────────────────
// Main Page
// ──────────────────────────────────────────────────────────────────────────────

function UnifiedRulesPage() {
  const { canCreateRules } = usePermissions();
  if (!canCreateRules) return <Navigate to="/rules/active" replace />;

  const navigate = useNavigate();
  const { table: initialTable, rule_id: editRuleId, from: fromPage } = Route.useSearch();
  const isTableFqn = (initialTable ?? "").split(".").length === 3;
  const isSingleRuleEdit = Boolean(editRuleId);

  const cancelTarget = fromPage === "active" ? "/rules/active"
    : fromPage === "drafts" ? "/rules/drafts"
    : "/rules/create";

  const [checks, setChecks] = useState<CheckDraft[]>(() =>
    initialTable ? [] : [newCheck()],
  );
  const [isSaving, setIsSaving] = useState(false);
  const [loadedFromTable, setLoadedFromTable] = useState(false);

  const queryClient = useQueryClient();
  useEffect(() => {
    if (isTableFqn && initialTable) {
      queryClient.invalidateQueries({ queryKey: getGetRulesQueryKey(initialTable) });
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Load existing rules when navigated with ?table=
  const { data: existingRulesResp, isLoading: isLoadingRules } = useGetRules(
    initialTable ?? "",
    { query: { enabled: isTableFqn && !loadedFromTable } },
  );

  useEffect(() => {
    if (!existingRulesResp?.data || loadedFromTable) return;
    const entries = Array.isArray(existingRulesResp.data) ? existingRulesResp.data : [existingRulesResp.data];

    if (isSingleRuleEdit) {
      const target = entries.find((e) => e.rule_id === editRuleId);
      if (target && target.checks?.length) {
        const draft = savedCheckToDraft(target.checks[0] as Record<string, unknown>, initialTable!);
        if (draft) {
          draft.ruleId = editRuleId;
          setChecks([draft]);
          toast.info(`Editing rule for ${initialTable!.split(".").pop()}`);
        }
      }
    } else {
      if (entries.length > 0) {
        const drafts: CheckDraft[] = [];
        for (const entry of entries) {
          for (const c of entry.checks ?? []) {
            const draft = savedCheckToDraft(c as Record<string, unknown>, initialTable!);
            if (draft) {
              if (entry.rule_id) draft.ruleId = entry.rule_id;
              drafts.push(draft);
            }
          }
        }
        if (drafts.length > 0) {
          setChecks(drafts);
          toast.info(`Loaded ${drafts.length} existing rule${drafts.length > 1 ? "s" : ""} for ${initialTable!.split(".").pop()}`);
        }
      }
    }
    setLoadedFromTable(true);
  }, [existingRulesResp]); // eslint-disable-line react-hooks/exhaustive-deps

  // AI generation state
  const [aiPrompt, setAiPrompt] = useState("");
  const [aiGenerating, setAiGenerating] = useState(false);
  const [aiReferenceTable, setAiReferenceTable] = useState(initialTable ?? "");

  // Dry run state
  const [dryRunTable, setDryRunTable] = useState(initialTable ?? "");
  const [dryRunSampleSize, setDryRunSampleSize] = useState(1000);
  const [dryRunResult, setDryRunResult] = useState<DryRunResultsOut | null>(null);
  const [dryRunJobRunId, setDryRunJobRunId] = useState<number | null>(null);
  const [dryRunRunId, setDryRunRunId] = useState<string | null>(null);
  const [dryRunViewFqn, setDryRunViewFqn] = useState<string | null>(null);

  const submitDryRunMutation = useSubmitDryRun();
  // submitMutation replaced by direct submitRuleForApproval calls

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

  const addCheck = () => setChecks((prev) => [...prev, newCheck()]);

  const removeCheck = (id: string) => {
    setChecks((prev) => (prev.length <= 1 ? prev : prev.filter((c) => c.id !== id)));
  };

  const updateCheck = useCallback((id: string, patch: Partial<CheckDraft>) => {
    setChecks((prev) =>
      prev.map((c) => (c.id === id ? { ...c, ...patch } : c)),
    );
  }, []);

  const hasRefTable = aiReferenceTable.split(".").length === 3;
  const effectiveTable = hasRefTable ? aiReferenceTable : (isTableFqn ? initialTable : undefined);

  const handleAiGenerate = async () => {
    if (!aiPrompt.trim()) return;
    setAiGenerating(true);
    try {
      const resp = await aiAssistedChecksGeneration({
        user_input: aiPrompt.trim(),
        table_fqn: effectiveTable,
      });
      const generated = resp.data.checks ?? [];
      const drafts = generated
        .map((c) => aiCheckToDraft(c as Record<string, unknown>))
        .filter((d): d is CheckDraft => d !== null)
        .filter((d) => d.fn !== "sql_query");
      if (effectiveTable) {
        for (const d of drafts) {
          d.targetTables = [effectiveTable];
        }
      }
      if (drafts.length === 0) {
        toast.error("AI did not generate any valid checks. Try a different description.");
        return;
      }
      const hasOnlyEmptyDefault = checks.length === 1 && checks[0].fn === "";
      setChecks(hasOnlyEmptyDefault ? drafts : [...checks, ...drafts]);
      toast.success(`${drafts.length} check${drafts.length > 1 ? "s" : ""} generated by AI`);
      setAiPrompt("");
    } catch {
      toast.error("AI generation failed. Please try again.");
    } finally {
      setAiGenerating(false);
    }
  };

  // Collect all unique target tables across checks
  const allTargetTables = useMemo(() => {
    const set = new Set<string>();
    for (const c of checks) {
      for (const t of c.targetTables) set.add(t);
    }
    return Array.from(set);
  }, [checks]);

  const totalTargetPairs = useMemo(() => {
    return checks.reduce((sum, c) => sum + c.targetTables.length, 0);
  }, [checks]);

  const isValid = useMemo(() => {
    return (
      checks.length > 0 &&
      checks.every((c) => {
        if (!c.fn) return false;
        const def = CHECK_FUNCTIONS.find((f) => f.value === c.fn);
        if (def) {
          return def.args.every((arg) => (c.args[arg] ?? "").trim() !== "");
        }
        // Custom/unknown function: valid as long as all populated args are non-empty
        return Object.values(c.args).every((v) => (v ?? "").trim() !== "");
      }) &&
      totalTargetPairs > 0
    );
  }, [checks, totalTargetPairs]);

  const validationMessage = useMemo(() => {
    if (checks.some((c) => c.fn === "")) return "Select a function for every check";
    if (totalTargetPairs === 0) return "Assign at least one target table to a check";
    return null;
  }, [checks, totalTargetPairs]);

  // ── Real-time duplicate detection ──────────────────────────────────
  const [dupCheckIds, setDupCheckIds] = useState<Set<string>>(new Set());
  const [dupChecking, setDupChecking] = useState(false);
  const dupTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const checksFingerprint = useMemo(() => {
    return checks
      .filter((c) => c.fn && c.targetTables.length > 0)
      .map((c) => `${c.id}:${c.fn}:${JSON.stringify(c.args)}:${c.targetTables.join(",")}:${c.ruleId ?? ""}`)
      .join("|");
  }, [checks]);

  useEffect(() => {
    if (dupTimerRef.current) clearTimeout(dupTimerRef.current);
    const eligible = checks.filter((c) => c.fn && c.targetTables.length > 0);
    if (eligible.length === 0) {
      setDupCheckIds(new Set());
      return;
    }
    dupTimerRef.current = setTimeout(async () => {
      setDupChecking(true);
      const dups = new Set<string>();
      const tableCheckMap = new Map<string, { checkId: string; dict: Record<string, unknown>; ruleId?: string }[]>();
      for (const c of eligible) {
        const dict = checkToDict(c);
        for (const fqn of c.targetTables) {
          if (!tableCheckMap.has(fqn)) tableCheckMap.set(fqn, []);
          tableCheckMap.get(fqn)!.push({ checkId: c.id, dict, ruleId: c.ruleId });
        }
      }
      for (const [fqn, items] of tableCheckMap) {
        try {
          const ruleIds = [...new Set(items.map((i) => i.ruleId).filter(Boolean))] as string[];
          const body: CheckDuplicatesIn = {
            table_fqn: fqn,
            checks: items.map((i) => i.dict),
            exclude_rule_id: ruleIds.length === 1 ? ruleIds[0] : undefined,
            exclude_rule_ids: ruleIds.length > 1 ? ruleIds : undefined,
          };
          const resp = await checkDuplicates(body);
          if (resp.data.duplicates.length > 0) {
            const dupSigs = new Set(
              resp.data.duplicates.map((d: Record<string, unknown>) =>
                JSON.stringify((d as Record<string, unknown>).check ?? d, null, 0),
              ),
            );
            for (const item of items) {
              const sig = JSON.stringify(item.dict.check ?? item.dict, null, 0);
              if (dupSigs.has(sig)) dups.add(item.checkId);
            }
          }
        } catch {
          // ignore errors
        }
      }
      setDupCheckIds(dups);
      setDupChecking(false);
    }, 600);
    return () => { if (dupTimerRef.current) clearTimeout(dupTimerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [checksFingerprint]);

  const hasDuplicates = dupCheckIds.size > 0;

  // Dry run handler
  const isDryRunning = submitDryRunMutation.isPending || dryRunPolling.isPolling;

  const justSavedRef = useRef(false);

  const hasUnsavedChanges = useMemo(
    () => checks.some((c) => c.fn !== "" || c.targetTables.length > 0),
    [checks],
  );

  const { blocker } = useUnsavedGuard({ hasUnsavedChanges, isRunning: isDryRunning, bypassRef: justSavedRef });

  const handleConfirmLeave = async () => {
    if (isDryRunning && dryRunRunId && dryRunJobRunId) {
      try {
        await cancelDryRun(dryRunRunId, { job_run_id: dryRunJobRunId });
      } catch {
        // best-effort cancel
      }
    }
    blocker.proceed?.();
  };

  const handleDryRun = async () => {
    if (!dryRunTable) {
      toast.error("Select a table to run a dry run against");
      return;
    }
    const checksForTable = buildChecksForTable(checks, dryRunTable);
    if (checksForTable.length === 0) {
      toast.error("No checks target the selected table");
      return;
    }
    try {
      setDryRunResult(null);
      const resp = await submitDryRunMutation.mutateAsync({
        data: { table_fqn: dryRunTable, checks: checksForTable, sample_size: dryRunSampleSize, skip_history: true },
      });
      setDryRunRunId(resp.data.run_id);
      setDryRunJobRunId(resp.data.job_run_id);
      setDryRunViewFqn(resp.data.view_fqn ?? null);
      toast.info("Dry run submitted — waiting for results...");
    } catch (err) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      const detail = axErr?.response?.data?.detail;
      toast.error(detail ? `Dry run failed: ${detail}` : "Failed to submit dry run");
      console.error("Dry run error:", err);
    }
  };

  const hasArgErrors = useMemo(() => {
    return checks.some((c) => {
      const fnDef = CHECK_FUNCTIONS.find((f) => f.value === c.fn);
      const argKeys = fnDef?.args ?? Object.keys(c.args);
      return argKeys.some((arg) => validateArg(arg, c.args[arg] ?? "", c.fn) !== null);
    });
  }, [checks]);

  // Save handlers
  const handleSave = async (andSubmit: boolean) => {
    if (hasArgErrors) {
      toast.error("Fix validation errors before saving.");
      return;
    }
    setIsSaving(true);
    try {
      // Separate existing rules (update) from new rules (create)
      const existingChecks = checks.filter((c) => c.ruleId && c.fn);
      const newChecks = checks.filter((c) => !c.ruleId && c.fn && c.targetTables.length > 0);

      let updatedCount = 0;
      let createdCount = 0;
      const failedMessages: string[] = [];

      // 1. Update existing rules in-place
      for (const c of existingChecks) {
        const dict = checkToDict(c);
        try {
          const resp = await saveRules({
            table_fqn: c.targetTables[0],
            checks: [dict],
            rule_id: c.ruleId,
          } as SaveRulesIn);
          updatedCount++;
          if (andSubmit) {
            const savedRules = Array.isArray(resp.data) ? resp.data : [resp.data];
            for (const r of savedRules) {
              if (r.rule_id) {
                try { await submitRuleForApproval(r.rule_id); } catch (submitErr) {
                  const detail = (submitErr as { body?: { detail?: string } })?.body?.detail ?? String(submitErr);
                  toast.warning(`Rule updated but submission failed: ${detail}`, { duration: 6000 });
                }
              }
            }
          }
        } catch (e) {
          const detail = (e as { body?: { detail?: string } })?.body?.detail ?? String(e);
          failedMessages.push(`Update failed: ${detail}`);
        }
      }

      // 2. Create new rules (if any)
      if (newChecks.length > 0) {
        const tableCheckMap: Record<string, Record<string, unknown>[]> = {};
        for (const c of newChecks) {
          const dict = checkToDict(c);
          for (const fqn of c.targetTables) {
            if (!tableCheckMap[fqn]) tableCheckMap[fqn] = [];
            tableCheckMap[fqn].push(dict);
          }
        }

        // Check for duplicates before saving new rules
        const dupMessages: string[] = [];
        for (const [fqn, checksForTable] of Object.entries(tableCheckMap)) {
          try {
            const resp = await checkDuplicates({ table_fqn: fqn, checks: checksForTable });
            const dupes = resp.data.duplicates;
            if (dupes.length > 0) {
              const tableName = fqn.split(".").pop() ?? fqn;
              const dupDescs = dupes.map((d: Record<string, unknown>) => {
                const chk = (d.check as Record<string, unknown>) ?? d;
                return String(chk.function ?? "unknown");
              });
              dupMessages.push(`${tableName}: ${dupDescs.join(", ")}`);
            }
          } catch {
            // ignore duplicate check failure
          }
        }
        if (dupMessages.length > 0) {
          toast.error(
            `Duplicate rules already exist and cannot be saved:\n${dupMessages.join("\n")}`,
            { duration: 8000 },
          );
          if (updatedCount === 0) { setIsSaving(false); return; }
        } else {
          for (const [fqn, checksForTable] of Object.entries(tableCheckMap)) {
            try {
              const resp = await saveRules({ table_fqn: fqn, checks: checksForTable } as SaveRulesIn);
              createdCount++;
              if (andSubmit) {
                const savedRules = Array.isArray(resp.data) ? resp.data : [resp.data];
                for (const r of savedRules) {
                  if (r.rule_id) {
                    try { await submitRuleForApproval(r.rule_id); } catch (submitErr) {
                      const detail = (submitErr as { body?: { detail?: string } })?.body?.detail ?? String(submitErr);
                      toast.warning(`Rule saved but submission failed: ${detail}`, { duration: 6000 });
                    }
                  }
                }
              }
            } catch (e) {
              const detail = (e as { body?: { detail?: string } })?.body?.detail ?? String(e);
              const tableName = fqn.split(".").pop() ?? fqn;
              failedMessages.push(`${tableName}: ${detail}`);
            }
          }
        }
      }

      const totalSuccess = updatedCount + createdCount;
      if (totalSuccess > 0) {
        const parts: string[] = [];
        if (updatedCount > 0) parts.push(`${updatedCount} updated`);
        if (createdCount > 0) parts.push(`${createdCount} created`);
        toast.success(
          andSubmit
            ? `Rules ${parts.join(", ")} and submitted`
            : `Rules ${parts.join(", ")} as drafts`,
        );
      }
      if (failedMessages.length > 0) {
        toast.error(failedMessages.join("\n"), { duration: 8000 });
      }
      if (totalSuccess > 0 && failedMessages.length === 0) {
        justSavedRef.current = true;
        navigate({ to: "/rules/drafts" });
      }
    } catch {
      toast.error("Failed to save rules");
    } finally {
      setIsSaving(false);
    }
  };

  const isBusy = aiGenerating || isSaving;
  const isEditMode = (isTableFqn && loadedFromTable && checks.length > 0) || isSingleRuleEdit;

  if (isLoadingRules && isTableFqn) {
    return (
      <div className="flex items-center justify-center py-20 gap-3 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin" />
        <span>Loading rules for {initialTable?.split(".").pop()}...</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: "Create Rules", to: "/rules/create" }]}
          page={isEditMode ? "Edit rules" : "Single table rules"}
        />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {isEditMode ? "Edit rules" : "Single table rules"}
          </h1>
          <p className="text-muted-foreground">
            {isEditMode
              ? `Editing rules for ${initialTable}. Run a dry run or save changes.`
              : "Define rules with AI or manually, assign tables, then validate and save."}
          </p>
        </div>
      </div>

      {/* Step 1: Define Rules */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <span className="inline-flex items-center justify-center h-6 w-6 rounded-full bg-primary text-primary-foreground text-xs font-bold">1</span>
            Define Rules
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* AI generation */}
          <div className="border border-violet-200 dark:border-violet-800 rounded-lg p-4 bg-violet-50/50 dark:bg-violet-950/30 space-y-3">
            <div className="flex items-center gap-2 mb-1">
              <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
              <span className="text-sm font-medium text-violet-900 dark:text-violet-200">Generate with AI</span>
            </div>
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">
                Reference table (optional — provides column context to AI)
              </Label>
              <CatalogBrowser
                value={aiReferenceTable}
                onChange={setAiReferenceTable}
                disabled={aiGenerating}
              />
            </div>
            <div className="flex gap-2">
              <Textarea
                value={aiPrompt}
                onChange={(e) => setAiPrompt(e.target.value)}
                placeholder="e.g. All ID columns must not be null, email must be valid format, amounts must be positive"
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
                {aiGenerating ? "Generating..." : "Generate"}
              </Button>
            </div>
          </div>

          {/* Check cards */}
          {checks.map((check, idx) => (
            <CheckCard
              key={check.id}
              check={check}
              index={idx}
              onUpdate={updateCheck}
              onRemove={removeCheck}
              canRemove={checks.length > 1}
              disabled={isBusy}
              isDuplicate={dupCheckIds.has(check.id)}
            />
          ))}
          <Button variant="outline" size="sm" onClick={addCheck} className="gap-1" disabled={isBusy}>
            <Plus className="h-3 w-3" />
            Add check
          </Button>
        </CardContent>
      </Card>

      {/* Step 2: Validate & Save */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <span className="inline-flex items-center justify-center h-6 w-6 rounded-full bg-primary text-primary-foreground text-xs font-bold">2</span>
            Validate & Save
          </CardTitle>
          <CardDescription>
            Run a dry run against a specific table to validate, then save rules.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Dry run section */}
          {allTargetTables.length > 0 && (
            <div className="space-y-3 border rounded-lg p-4 bg-muted/20">
              <div className="flex items-center gap-3 flex-wrap">
                <Select value={dryRunTable} onValueChange={setDryRunTable}>
                  <SelectTrigger className="h-9 text-xs max-w-xs">
                    <SelectValue placeholder="Select table for dry run..." />
                  </SelectTrigger>
                  <SelectContent>
                    {allTargetTables.map((t) => (
                      <SelectItem key={t} value={t} className="text-xs font-mono">
                        {t}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <div className="flex items-center gap-1.5">
                  <Label className="text-xs text-muted-foreground whitespace-nowrap">Sample rows</Label>
                  <Input
                    type="number"
                    min={1}
                    max={10000}
                    value={dryRunSampleSize}
                    onChange={(e) => setDryRunSampleSize(Math.min(10000, Math.max(1, Number(e.target.value) || 1000)))}
                    disabled={isBusy}
                    className="w-24 h-9 text-xs"
                  />
                </div>
                <Button
                  variant="outline"
                  onClick={handleDryRun}
                  disabled={!dryRunTable || isBusy || isDryRunning}
                  className="gap-2"
                  size="sm"
                >
                  {isDryRunning ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Play className="h-4 w-4" />
                  )}
                  {isDryRunning ? "Running..." : "Dry Run"}
                </Button>
              </div>

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
            </div>
          )}

          {/* Summary + Save buttons */}
          <Separator />
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3 text-sm text-muted-foreground">
              {validationMessage ? (
                <>
                  <AlertCircle className="h-4 w-4 text-amber-500" />
                  {validationMessage}
                </>
              ) : hasDuplicates ? (
                <>
                  <AlertCircle className="h-4 w-4 text-red-500" />
                  <span className="text-red-600">
                    {dupCheckIds.size} check{dupCheckIds.size !== 1 ? "s" : ""} already exist for the selected table{allTargetTables.length !== 1 ? "s" : ""}
                  </span>
                </>
              ) : (
                <span>
                  {checks.filter((c) => c.fn !== "").length} check{checks.filter((c) => c.fn !== "").length !== 1 ? "s" : ""} &rarr;{" "}
                  <strong>{totalTargetPairs}</strong> rule{totalTargetPairs !== 1 ? "s" : ""} across{" "}
                  <strong>{allTargetTables.length}</strong> table{allTargetTables.length !== 1 ? "s" : ""}
                  {dupChecking && <Loader2 className="inline h-3 w-3 animate-spin ml-2" />}
                </span>
              )}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                onClick={() => navigate({ to: cancelTarget })}
                disabled={isBusy}
              >
                Cancel
              </Button>
              <Button
                variant="outline"
                onClick={() => handleSave(false)}
                disabled={!isValid || isBusy || hasDuplicates || hasArgErrors}
                className="gap-2"
              >
                {isSaving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Save className="h-4 w-4" />
                )}
                {isSingleRuleEdit ? "Update rule" : "Save as drafts"}
              </Button>
              <Button
                onClick={() => handleSave(true)}
                disabled={!isValid || isBusy || hasDuplicates || hasArgErrors}
                className="gap-2"
              >
                {isSaving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <SendHorizonal className="h-4 w-4" />
                )}
                {isSingleRuleEdit ? "Update & submit" : "Submit for Review"}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {isDryRunning ? "Dry run in progress" : "Unsaved changes"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {isDryRunning
                ? "A dry run is currently running. Leaving will cancel it and discard any results."
                : "You have unsaved rule changes. Leaving will discard your edits."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>Stay on page</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmLeave} className="bg-destructive text-white hover:bg-destructive/90">
              {isDryRunning ? "Leave & cancel dry run" : "Discard & leave"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

function buildChecksForTable(
  checks: CheckDraft[],
  tableFqn: string,
): Record<string, unknown>[] {
  return checks
    .filter((c) => c.fn !== "" && c.targetTables.includes(tableFqn))
    .map(checkToDict);
}

const COLUMN_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;
const SQL_KEYWORD_PATTERN = /\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|MERGE)\b/i;

function validateArg(arg: string, value: string, fn?: string): string | null {
  if (!value.trim()) return null;
  switch (arg) {
    case "col_name": {
      const names = fn === "is_unique"
        ? value.split(",").map((s) => s.trim()).filter(Boolean)
        : [value.trim()];
      for (const name of names) {
        if (!COLUMN_NAME_REGEX.test(name)) {
          return `"${name}" is not a valid column name. Use letters, numbers, underscores, and dots only.`;
        }
      }
      return null;
    }
    case "allowed":
    case "forbidden": {
      if (value.includes(";")) {
        return "Semicolons are not allowed in list values.";
      }
      const items = value.split(",").map((s) => s.trim());
      for (const item of items) {
        if (SQL_KEYWORD_PATTERN.test(item)) {
          return `"${item}" contains a prohibited SQL keyword.`;
        }
      }
      return null;
    }
    case "limit":
    case "min_limit":
    case "max_limit": {
      if (value.trim() !== "" && isNaN(Number(value.trim()))) {
        return "Must be a numeric value.";
      }
      return null;
    }
    case "expression": {
      if (value.includes(";")) {
        return "Semicolons are not allowed in expressions.";
      }
      if (SQL_KEYWORD_PATTERN.test(value)) {
        return "Expression contains a prohibited SQL keyword (DROP, DELETE, INSERT, etc.).";
      }
      return null;
    }
    default:
      return null;
  }
}

function argLabel(arg: string): string {
  switch (arg) {
    case "col_name": return "Column Name";
    case "allowed": return "Allowed Values";
    case "forbidden": return "Not Allowed Values";
    case "limit": return "Limit";
    case "min_limit": return "Min Limit";
    case "max_limit": return "Max Limit";
    case "regex": return "Regex Pattern";
    case "date_format": return "Date Format";
    case "timestamp_format": return "Timestamp Format";
    case "expression": return "Expression";
    case "msg": return "Error Message";
    default: return arg;
  }
}

function argHint(arg: string, fn?: string): string {
  if (arg === "col_name" && fn === "is_unique") {
    return "comma-separated, e.g. id or col1, col2";
  }
  switch (arg) {
    case "col_name": return "e.g. id";
    case "allowed": return "comma-separated, e.g. A,B,C";
    case "forbidden": return "comma-separated";
    case "limit": return "numeric value";
    case "min_limit": return "min value";
    case "max_limit": return "max value";
    case "regex": return "e.g. ^[A-Z]+$";
    case "date_format": return "e.g. yyyy-MM-dd";
    case "timestamp_format": return "e.g. yyyy-MM-dd HH:mm:ss";
    case "expression": return "SQL expression, e.g. col > 0";
    case "msg": return "Error message";
    default: return arg;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// CheckCard
// ──────────────────────────────────────────────────────────────────────────────

interface CheckCardProps {
  check: CheckDraft;
  index: number;
  onUpdate: (id: string, patch: Partial<CheckDraft>) => void;
  onRemove: (id: string) => void;
  canRemove: boolean;
  disabled?: boolean;
  isDuplicate?: boolean;
}

function CheckCard({ check, index, onUpdate, onRemove, canRemove, disabled, isDuplicate }: CheckCardProps) {
  const fnDef = CHECK_FUNCTIONS.find((f) => f.value === check.fn);
  const argFields = fnDef?.args ?? [];
  const isUnknownFn = check.fn !== "" && !fnDef;
  const columns = getCheckColumns(check);

  const [tablesOpen, setTablesOpen] = useState(check.fn !== "" && check.targetTables.length === 0);
  const [browsedTables, setBrowsedTables] = useState<string[]>([]);
  const [eligibleTables, setEligibleTables] = useState<Set<string>>(new Set());
  const [isFiltering, setIsFiltering] = useState(false);
  const prevColumnsRef = useRef("");

  const handleBrowsedTablesChange = useCallback((tables: string[]) => {
    setBrowsedTables(tables);
  }, []);

  useEffect(() => {
    if (columns.length === 0 || browsedTables.length === 0) {
      setEligibleTables(new Set(browsedTables));
      return;
    }
    const colKey = columns.join(",").toLowerCase();
    if (colKey === prevColumnsRef.current && eligibleTables.size > 0) return;
    prevColumnsRef.current = colKey;
    setIsFiltering(true);
    filterTablesByColumns({ required_columns: columns, table_fqns: browsedTables })
      .then((resp) => {
        setEligibleTables(new Set(resp.data.matching));
        onUpdate(check.id, { targetTables: check.targetTables.filter((t) => resp.data.matching.includes(t)) });
      })
      .catch(() => setEligibleTables(new Set(browsedTables)))
      .finally(() => setIsFiltering(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [columns.join(","), browsedTables]);

  const handleTableSelect = useCallback((tables: string[]) => {
    onUpdate(check.id, { targetTables: tables.filter((t) => eligibleTables.has(t)) });
  }, [check.id, eligibleTables, onUpdate]);

  const removeTable = (fqn: string) => {
    onUpdate(check.id, { targetTables: check.targetTables.filter((t) => t !== fqn) });
  };

  return (
    <div className={`border rounded-lg overflow-hidden border-l-[3px] ${isDuplicate ? "border-red-300 bg-red-50/30 border-l-red-400" : "border-l-primary/40"}`}>
      {/* Header */}
      <div className={`flex items-center justify-between px-4 py-2.5 border-b ${isDuplicate ? "bg-red-50/60" : "bg-muted/30"}`}>
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold text-muted-foreground">Check {index + 1}</span>
          {columns.length > 0 && (
            <Badge variant="outline" className="text-[10px] font-mono gap-1">
              col: {columns.join(", ")}
            </Badge>
          )}
          {check.targetTables.length > 0 && (
            <Badge variant="secondary" className="text-[10px] gap-1">
              <Table2 className="h-2.5 w-2.5" />
              {check.targetTables.length} table{check.targetTables.length !== 1 ? "s" : ""}
            </Badge>
          )}
          {isDuplicate && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Badge variant="destructive" className="text-[10px] gap-1">
                    <AlertCircle className="h-2.5 w-2.5" />
                    Duplicate
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  <p>This check already exists for the selected table(s). Remove or modify it.</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )}
        </div>
        {canRemove && (
          <Button variant="ghost" size="sm" className="h-6 w-6 p-0 text-destructive" onClick={() => onRemove(check.id)} disabled={disabled}>
            <Trash2 className="h-3 w-3" />
          </Button>
        )}
      </div>

      {/* Check definition */}
      <div className="p-4 space-y-3">
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label className="text-xs">Function</Label>
            <Select value={check.fn} onValueChange={(fn) => onUpdate(check.id, { fn, args: { col_name: check.args["col_name"] ?? "" } })} disabled={disabled}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Select function" />
              </SelectTrigger>
              <SelectContent>
                {CHECK_FUNCTIONS.map((f) => (
                  <SelectItem key={f.value} value={f.value} className="text-xs">
                    {f.label}
                  </SelectItem>
                ))}
                {isUnknownFn && (
                  <SelectItem value={check.fn} className="text-xs">
                    {check.fn} (custom)
                  </SelectItem>
                )}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs">Criticality</Label>
            <Select
              value={check.criticality}
              onValueChange={(v) => onUpdate(check.id, { criticality: v as "warn" | "error" })}
              disabled={disabled}
            >
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="warn" className="text-xs">warn</SelectItem>
                <SelectItem value="error" className="text-xs">error</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {argFields.length > 0 && (
          <div className="grid gap-2 sm:grid-cols-2">
            {argFields.map((arg) => {
              const val = check.args[arg] ?? "";
              const isEmpty = val.trim() === "";
              const validationErr = validateArg(arg, val, check.fn);
              const hasError = !!validationErr;
              return (
                <div key={arg} className="space-y-1">
                  <Label className={`text-[11px] ${hasError ? "text-red-500" : isEmpty ? "text-amber-500" : "text-muted-foreground"}`}>
                    {argLabel(arg)}{isEmpty && " (required)"}
                  </Label>
                  <Input
                    className={`h-7 text-xs ${hasError ? "border-red-400 focus-visible:ring-red-400" : isEmpty ? "border-amber-400 focus-visible:ring-amber-400" : ""}`}
                    placeholder={argHint(arg, check.fn)}
                    value={val}
                    onChange={(e) =>
                      onUpdate(check.id, { args: { ...check.args, [arg]: e.target.value } })
                    }
                    disabled={disabled}
                  />
                  {hasError && (
                    <p className="text-[10px] text-red-500 flex items-center gap-1">
                      <AlertCircle className="h-2.5 w-2.5 shrink-0" />
                      {validationErr}
                    </p>
                  )}
                </div>
              );
            })}
          </div>
        )}
        {isUnknownFn && Object.keys(check.args).length > 0 && (
          <div className="grid gap-2 sm:grid-cols-2">
            {Object.entries(check.args).map(([key, val]) => {
              const isEmpty = val.trim() === "";
              const validationErr = validateArg(key, val, check.fn);
              const hasError = !!validationErr;
              return (
                <div key={key} className="space-y-1">
                  <Label className={`text-[11px] ${hasError ? "text-red-500" : isEmpty ? "text-amber-500" : "text-muted-foreground"}`}>
                    {argLabel(key)}{isEmpty && " (required)"}
                  </Label>
                  <Input
                    className={`h-7 text-xs ${hasError ? "border-red-400 focus-visible:ring-red-400" : isEmpty ? "border-amber-400 focus-visible:ring-amber-400" : ""}`}
                    value={val}
                    onChange={(e) =>
                      onUpdate(check.id, { args: { ...check.args, [key]: e.target.value } })
                    }
                    disabled={disabled}
                  />
                  {hasError && (
                    <p className="text-[10px] text-red-500 flex items-center gap-1">
                      <AlertCircle className="h-2.5 w-2.5 shrink-0" />
                      {validationErr}
                    </p>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Target tables section */}
      <div className="border-t">
        <button
          type="button"
          className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-muted/20 transition-colors"
          onClick={() => setTablesOpen(!tablesOpen)}
          disabled={disabled}
        >
          <div className="flex items-center gap-2">
            <Table2 className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs font-medium">
              Target tables
              {check.targetTables.length > 0 && (
                <span className="text-muted-foreground ml-1">
                  ({check.targetTables.length} selected)
                </span>
              )}
              {check.fn !== "" && check.targetTables.length === 0 && (
                <span className="text-amber-500 ml-1">(none selected)</span>
              )}
            </span>
            {check.fn !== "" && columns.length > 0 && (
              <span className="text-[10px] text-muted-foreground">
                — filtered by column: <code className="bg-muted px-1 py-0.5 rounded">{columns.join(", ")}</code>
              </span>
            )}
          </div>
          {tablesOpen ? <ChevronUp className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />}
        </button>

        {tablesOpen && (
          <div className="px-4 pb-4 space-y-3">
            {isFiltering && (
              <div className="flex items-center gap-2 text-xs text-muted-foreground py-1">
                <Loader2 className="h-3 w-3 animate-spin" />
                Checking column compatibility...
              </div>
            )}
            <CatalogBrowser
              onChange={() => {}}
              multiSelect
              selectedTables={check.targetTables}
              onMultiChange={handleTableSelect}
              onAllTablesLoaded={handleBrowsedTablesChange}
              disabledTables={columns.length > 0
                ? browsedTables.filter((t) => !eligibleTables.has(t))
                : undefined}
            />
            {check.targetTables.length > 0 && (
              <div className="flex flex-wrap gap-1">
                {check.targetTables.map((t) => (
                  <Badge key={t} variant="secondary" className="text-[10px] font-mono gap-1 pr-1">
                    {t}
                    <button
                      type="button"
                      onClick={() => removeTable(t)}
                      className="ml-0.5 rounded-full p-0.5 hover:bg-destructive/20 hover:text-destructive transition-colors"
                    >
                      <X className="h-2 w-2" />
                    </button>
                  </Badge>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
