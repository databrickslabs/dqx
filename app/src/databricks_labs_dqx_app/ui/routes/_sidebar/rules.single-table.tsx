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
import { ColumnDiscoveryPanel } from "@/components/ColumnDiscoveryPanel";
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
  Info,
  Search,
  Check,
} from "lucide-react";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { toast } from "sonner";
import {
  aiAssistedChecksGeneration,
  type SaveRulesIn,
  saveRules,
  useSubmitDryRun,
  useGetDryRunResults,
  useGetRules,
  getGetRulesQueryKey,
  useListCheckFunctions,
  type CheckFunctionDef as ApiCheckFunctionDef,
  type CheckFunctionParam as ApiCheckFunctionParam,
  type DryRunResultsOut,
} from "@/lib/api";
import { useQueryClient } from "@tanstack/react-query";
import { filterTablesByColumns, checkDuplicates, type CheckDuplicatesIn, submitRuleForApproval, cancelDryRun, getDryRunStatusCustom, useLabelDefinitions, type LabelDefinition } from "@/lib/api-custom";
import { LabelsEditor } from "@/components/Labels";
import { getUserMetadata } from "@/lib/format-utils";
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

/**
 * Argument shape that drives both UI rendering and payload serialization.
 *
 * - ``column`` — single column name (writes to ``column`` key in payload).
 * - ``list_csv`` — comma-separated values; serialized as a JSON list.
 * - ``number`` — numeric input; serialized as a number.
 * - ``string`` — free-form string; serialized verbatim.
 * - ``boolean`` — true/false select; serialized as a JSON boolean.
 *
 * ``required`` controls whether the rule is allowed to save with this arg
 * empty. ``defaultValue`` (only meaningful for booleans today) is auto-
 * filled when the user picks the function so the input doesn't render as
 * blank.
 */
type ArgType = "column" | "list_csv" | "number" | "string" | "boolean";

interface CheckFunctionArg {
  /** Local key under ``CheckDraft.args``. ``col_name`` is mapped to the
   *  engine's ``column`` key at serialization time via
   *  :data:`UI_TO_ENGINE_ARG_MAP`. */
  name: string;
  label: string;
  type: ArgType;
  required: boolean;
  /** Default to inject when the function is selected (used for booleans
   *  so the dropdown shows DQX's documented default). */
  defaultValue?: string;
  /** Placeholder text for the input. */
  hint?: string;
  /** Helper text rendered under the input (small, muted). */
  help?: string;
}

interface CheckFunctionDef {
  value: string;
  label: string;
  args: CheckFunctionArg[];
  /** Short docstring summary surfaced in the function picker. */
  doc?: string;
  /** UX category bucket from the backend (e.g. "Null & Empty"). */
  category?: string;
  /** ``"row"`` or ``"dataset"`` — informational only on the UI side. */
  ruleType?: string;
  /** Cross-arg validation — runs after per-arg checks. Returns ``null``
   *  when the combined arg state is valid, otherwise an error message. */
  crossArgValidate?: (args: Record<string, string>) => string | null;
}

/**
 * Per-function UX overrides that the backend can't infer from
 * ``inspect.signature``. Anything purely cosmetic (friendly arg label,
 * placeholder, helper text on a boolean) goes here, plus cross-argument
 * validators that span multiple inputs.
 *
 * Keys not listed here fall back to the generic ``argLabel`` /
 * ``argHint`` helpers so new DQX checks render with sensible defaults
 * out of the box; we only add an entry when the default is wrong or
 * confusing.
 */
type ArgOverride = Partial<Pick<CheckFunctionArg, "label" | "hint" | "help">>;
interface CheckFunctionOverride {
  /** Cross-argument validator (e.g. ``is_in_range`` requires at least one bound). */
  crossArgValidate?: (args: Record<string, string>) => string | null;
  /** Per-argument label/hint/help overrides keyed by *engine* parameter name. */
  args?: Record<string, ArgOverride>;
}

const CHECK_FUNCTION_OVERRIDES: Record<string, CheckFunctionOverride> = {
  // ``is_in_range`` — each bound is individually optional in DQX, but a
  // check with neither bound is meaningless. Demand at least one.
  is_in_range: {
    crossArgValidate: (args) => {
      const hasMin = (args.min_limit ?? "").trim() !== "";
      const hasMax = (args.max_limit ?? "").trim() !== "";
      if (!hasMin && !hasMax) return "Provide at least one of Min Limit or Max Limit.";
      return null;
    },
  },
  is_not_in_range: {
    crossArgValidate: (args) => {
      const hasMin = (args.min_limit ?? "").trim() !== "";
      const hasMax = (args.max_limit ?? "").trim() !== "";
      if (!hasMin && !hasMax) return "Provide at least one of Min Limit or Max Limit.";
      return null;
    },
  },
  // ``is_unique`` accepts a list of columns. The UI captures them as a
  // CSV under ``col_name``; surface that intent in the placeholder.
  is_unique: {
    args: {
      columns: { label: "Column Name(s)", hint: "comma-separated, e.g. id or col1, col2" },
      nulls_distinct: {
        help: "When true (default), NULL values are treated as distinct from each other.",
      },
    },
  },
  is_not_empty: {
    args: { trim_strings: { help: "When true, treat whitespace-only values as empty." } },
  },
  is_not_null_and_not_empty: {
    args: { trim_strings: { help: "When true, treat whitespace-only values as empty." } },
  },
  is_empty: {
    args: { trim_strings: { help: "When true, treat whitespace-only values as empty." } },
  },
  is_null_or_empty: {
    args: { trim_strings: { help: "When true, treat whitespace-only values as empty." } },
  },
  is_in_list: {
    args: {
      case_sensitive: { help: "When false, the comparison is case-insensitive." },
    },
  },
  is_not_in_list: {
    args: {
      case_sensitive: { help: "When false, the comparison is case-insensitive." },
    },
  },
  is_valid_date: {
    args: {
      date_format: { help: "Optional. Leave blank to use DQX's default ISO date parser." },
    },
  },
  is_valid_timestamp: {
    args: {
      timestamp_format: { help: "Optional. Leave blank to use DQX's default ISO timestamp parser." },
    },
  },
  sql_expression: {
    args: {
      negate: { help: "When true, the rule fails when the expression evaluates to true." },
      msg: { hint: "Optional error message override" },
      name: { hint: "Optional custom name" },
    },
  },
  regex_match: {
    args: {
      negate: { help: "When true, the rule fails when the value matches the pattern." },
    },
  },
};

/**
 * Module-level mutable registry, populated from
 * ``GET /api/v1/check-functions`` on first render via
 * :func:`useCheckFunctionsRegistry`. Helpers like ``checkToDict`` read
 * this directly because they're called from event handlers / memo
 * callbacks, by which time the API response has resolved.
 *
 * The fallback empty array means a stale page (cache miss + slow
 * network) renders an empty function dropdown, which is fine — the
 * page already shows a loading shimmer until the rule list arrives.
 */
let CHECK_FUNCTIONS: CheckFunctionDef[] = [];

/**
 * Map an API ``CheckFunctionParam`` to the UI's local ``CheckFunctionArg``.
 *
 * The big rename is ``column`` / ``columns`` → ``col_name``: the rest of
 * the editor (validators, AI loaders, save serialization) uses
 * ``col_name`` as the canonical local key, so we collapse both column
 * inputs to that name here. ``checkToDict`` knows to expand ``is_unique``
 * back to a ``columns`` list at serialization time.
 */
function apiParamToArg(
  param: ApiCheckFunctionParam,
  overrides: Record<string, ArgOverride> | undefined,
  fnName: string,
): CheckFunctionArg {
  const isColumnParam = param.kind === "column" || param.kind === "columns";
  const localName = isColumnParam ? "col_name" : param.name;
  const overrideKey = param.name; // overrides are keyed by engine name
  const ov = overrides?.[overrideKey] ?? {};
  // ``columns`` list-of-columns is rendered through the same column
  // input but with a multi-column hint; honour any explicit override
  // first, then fall back to the generic per-arg hint.
  const defaultHint = argHint(localName, fnName) || (param.kind === "columns" ? "comma-separated, e.g. id or col1, col2" : undefined);
  const type: ArgType =
    param.kind === "boolean"
      ? "boolean"
      : param.kind === "number"
      ? "number"
      : param.kind === "list"
      ? "list_csv"
      : isColumnParam
      ? "column"
      : "string";
  const arg: CheckFunctionArg = {
    name: localName,
    label: ov.label ?? argLabel(localName),
    type,
    required: param.required,
  };
  const hint = ov.hint ?? defaultHint;
  if (hint) arg.hint = hint;
  const help = ov.help;
  if (help) arg.help = help;
  if (param.default != null) arg.defaultValue = param.default;
  return arg;
}

function apiToCheckFunctions(apiList: ApiCheckFunctionDef[]): CheckFunctionDef[] {
  return apiList.map((api) => {
    const overrides = CHECK_FUNCTION_OVERRIDES[api.name];
    const args = (api.params ?? []).map((p) => apiParamToArg(p, overrides?.args, api.name));
    return {
      value: api.name,
      label: api.name,
      doc: api.doc ?? "",
      category: api.category,
      ruleType: api.rule_type,
      args,
      ...(overrides?.crossArgValidate
        ? { crossArgValidate: overrides.crossArgValidate }
        : {}),
    };
  });
}

/**
 * React hook that fetches the registry, mirrors it into the module-level
 * variable, and returns the live list. Components depend on the return
 * value so they re-render when the registry arrives; module-level
 * helpers read the mirrored variable directly.
 */
function useCheckFunctionsRegistry(): CheckFunctionDef[] {
  const { data } = useListCheckFunctions();
  const fns = useMemo(
    () => apiToCheckFunctions(data?.data?.functions ?? []),
    [data],
  );
  useEffect(() => {
    CHECK_FUNCTIONS = fns;
  }, [fns]);
  return fns;
}

/**
 * Pre-DQX-rename function names that still appear in old saved rules and
 * AI-generated outputs. Mapping these on load lets users edit legacy
 * rules through the new schema without losing them; on save the
 * canonical name is used. (DQX itself rejects these names at validate
 * time, which is one of the bugs we're fixing.)
 */
const LEGACY_FN_NAME_MAP: Record<string, string> = {
  is_min: "is_not_less_than",
  is_max: "is_not_greater_than",
};

interface CheckDraft {
  id: string;
  fn: string;
  args: Record<string, string>;
  criticality: "warn" | "error";
  /**
   * Free-form labels, including the reserved ``weight`` key. All authoring,
   * editing, and round-trip happens through ``user_metadata`` — there is no
   * separate native ``weight`` field on the rule.
   */
  userMetadata: Record<string, string>;
  targetTables: string[];
  ruleId?: string;
  /**
   * Original table this ``ruleId`` is bound to in the catalog. Each rule
   * row in ``dq_rules_catalog`` lives under exactly one ``table_fqn``, so
   * if the user adds extra tables to an existing draft we update the
   * original row in place and create fresh rules for the additions.
   */
  originalTable?: string;
}

function newCheck(): CheckDraft {
  return {
    id: crypto.randomUUID(),
    fn: "",
    args: {},
    criticality: "warn",
    userMetadata: {},
    targetTables: [],
  };
}

function checkToDict(c: CheckDraft): Record<string, unknown> {
  const args: Record<string, unknown> = {};
  const fnDef = CHECK_FUNCTIONS.find((f) => f.value === c.fn);
  const isKnownFn = !!fnDef;
  const argDefByName = new Map((fnDef?.args ?? []).map((a) => [a.name, a] as const));

  for (const [k, raw] of Object.entries(c.args)) {
    const v = (raw ?? "").trim();
    if (v === "") continue;

    // Drop UI-only ``col_name`` for known functions that don't take it.
    if (k === "col_name" && isKnownFn && !argDefByName.has("col_name")) continue;

    // ``is_unique`` is special: the UI captures one or more columns under
    // ``col_name`` (CSV) but DQX expects a ``columns`` list.
    if (c.fn === "is_unique" && k === "col_name") {
      args["columns"] = v.split(",").map((s) => s.trim()).filter(Boolean);
      continue;
    }

    const argDef = argDefByName.get(k);
    const engineKey = UI_TO_ENGINE_ARG_MAP[k] ?? k;

    if (argDef) {
      switch (argDef.type) {
        case "list_csv":
          args[engineKey] = v.split(",").map((s) => s.trim()).filter(Boolean);
          break;
        case "number": {
          const num = Number(v);
          args[engineKey] = Number.isFinite(num) ? num : v;
          break;
        }
        case "boolean":
          args[engineKey] = v.toLowerCase() === "true";
          break;
        default:
          args[engineKey] = v;
      }
    } else {
      // Unknown / custom function — fall back to the legacy heuristics.
      if (engineKey === "allowed" || engineKey === "forbidden") {
        args[engineKey] = v.split(",").map((s) => s.trim()).filter(Boolean);
      } else if (engineKey === "limit" || engineKey === "min_limit" || engineKey === "max_limit") {
        args[engineKey] = Number(v) || v;
      } else {
        args[engineKey] = v;
      }
    }
  }

  // Drop boolean args that are equal to their declared default. Keeps
  // YAML round-trips clean (we don't emit defaults that the user never
  // touched).
  if (fnDef) {
    for (const a of fnDef.args) {
      if (a.type !== "boolean" || a.defaultValue == null) continue;
      const engineKey = UI_TO_ENGINE_ARG_MAP[a.name] ?? a.name;
      if (engineKey in args) {
        const defaultBool = a.defaultValue.toLowerCase() === "true";
        if (args[engineKey] === defaultBool) {
          delete args[engineKey];
        }
      }
    }
  }

  const out: Record<string, unknown> = {
    criticality: c.criticality,
    check: { function: c.fn, arguments: args },
  };
  if (Object.keys(c.userMetadata).length > 0) {
    out.user_metadata = { ...c.userMetadata };
  }
  return out;
}

function getCheckColumns(c: CheckDraft): string[] {
  const colName = c.args["col_name"];
  if (!colName || colName.trim() === "*") return [];
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
  const rawFn = String(checkObj.function ?? "");
  if (!rawFn) return null;
  // Map any pre-rename DQX names (``is_min``/``is_max``) to their
  // current canonical equivalents so old saved rules can be edited.
  const fn = LEGACY_FN_NAME_MAP[rawFn] ?? rawFn;
  const rawArgs = (checkObj.arguments as Record<string, unknown>) ?? {};
  const args: Record<string, string> = {};
  for (const [k, v] of Object.entries(rawArgs)) {
    const key = AI_ARG_KEY_MAP[k] ?? k;
    if (Array.isArray(v)) {
      args[key] = v.join(", ");
    } else if (typeof v === "boolean") {
      args[key] = v ? "true" : "false";
    } else if (v != null) {
      args[key] = String(v);
    }
  }
  const criticality = (raw.criticality as string) ?? (checkObj.criticality as string) ?? "warn";

  // Legacy/AI rules may carry a top-level numeric ``weight``. Fold it into the
  // labels map so all weight handling stays in one place.
  const userMetadata = getUserMetadata(raw);
  if (raw.weight != null && typeof raw.weight === "number" && !("weight" in userMetadata)) {
    userMetadata.weight = String(raw.weight);
  }

  return {
    id: crypto.randomUUID(),
    fn,
    args,
    criticality: criticality === "error" ? "error" : "warn",
    userMetadata,
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

  // Pull DQX's full check-function registry from the backend. The list
  // re-renders the function picker; ``checkToDict`` and friends read the
  // module-level mirror that ``useCheckFunctionsRegistry`` maintains.
  const checkFunctions = useCheckFunctionsRegistry();

  const { data: labelDefsData } = useLabelDefinitions();
  const labelDefinitions = labelDefsData?.definitions ?? [];

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
    const allEntries = Array.isArray(existingRulesResp.data) ? existingRulesResp.data : [existingRulesResp.data];
    // Scope the editor to the same set the calling page surfaced. The
    // backend ``getRules`` endpoint returns every rule (draft, pending,
    // approved, rejected) for the table; without this filter the editor
    // would show e.g. 2 checks while the Active page header only counts 1.
    const entries =
      fromPage === "active"
        ? allEntries.filter((e) => e.status === "approved")
        : fromPage === "drafts"
          ? allEntries.filter((e) => e.status !== "approved")
          : allEntries;

    if (isSingleRuleEdit) {
      const target = entries.find((e) => e.rule_id === editRuleId);
      if (target && target.checks?.length) {
        const draft = savedCheckToDraft(target.checks[0] as Record<string, unknown>, target.table_fqn);
        if (draft) {
          draft.ruleId = editRuleId;
          draft.originalTable = target.table_fqn;
          setChecks([draft]);
          toast.info(`Editing rule for ${target.table_fqn.split(".").pop()}`);
        }
      }
    } else {
      if (entries.length > 0) {
        const drafts: CheckDraft[] = [];
        for (const entry of entries) {
          for (const c of entry.checks ?? []) {
            const draft = savedCheckToDraft(c as Record<string, unknown>, entry.table_fqn);
            if (draft) {
              if (entry.rule_id) {
                draft.ruleId = entry.rule_id;
                draft.originalTable = entry.table_fqn;
              }
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

  /**
   * Sync the table picked in the Column Discovery panel into "Target
   * tables" for any check that doesn't already have an explicit target.
   *
   * Only checks with empty ``targetTables`` are touched — once a user
   * has deliberately picked targets via the per-check Catalog Browser
   * we treat that as locked. This makes the discovery panel a fast
   * shortcut for the common case (one table per draft) without
   * silently overwriting existing selections when the user is just
   * browsing schemas to compare columns.
   */
  const handleDiscoveryTableSelect = useCallback((fqn: string) => {
    setChecks((prev) => {
      const empties = prev.filter((c) => c.targetTables.length === 0);
      if (empties.length === 0) {
        // Every check already has a target — surface a hint so the user
        // knows the discovery selection is intentional but not applied.
        toast.info("All checks already have a target table.", {
          description: `Use a check's "Target tables" panel to add ${fqn} explicitly.`,
        });
        return prev;
      }
      toast.success(`Targeted ${empties.length} check${empties.length === 1 ? "" : "s"} at ${fqn}`, {
        description: "Checks with explicit targets were left unchanged.",
        duration: 2500,
      });
      return prev.map((c) =>
        c.targetTables.length === 0 ? { ...c, targetTables: [fqn] } : c,
      );
    });
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
        const def = checkFunctions.find((f) => f.value === c.fn);
        if (def) {
          // Required args must be populated.
          if (!def.args.filter((a) => a.required).every((a) => (c.args[a.name] ?? "").trim() !== "")) {
            return false;
          }
          // Cross-arg constraint (e.g. is_in_range needs at least one bound).
          if (def.crossArgValidate && def.crossArgValidate(c.args) !== null) {
            return false;
          }
          // Per-arg syntax must pass.
          return def.args.every((a) => validateArg(a.name, c.args[a.name] ?? "", c.fn) === null);
        }
        // Custom/unknown function: valid as long as all populated args are non-empty.
        return Object.values(c.args).every((v) => (v ?? "").trim() !== "");
      }) &&
      totalTargetPairs > 0
    );
  }, [checks, totalTargetPairs, checkFunctions]);

  const validationMessage = useMemo(() => {
    if (checks.some((c) => c.fn === "")) return "Select a function for every check";
    if (totalTargetPairs === 0) return "Assign at least one target table to a check";
    // Surface the first incomplete required arg so the user knows what's missing.
    for (const c of checks) {
      const def = checkFunctions.find((f) => f.value === c.fn);
      if (!def) continue;
      const missing = def.args.find((a) => a.required && (c.args[a.name] ?? "").trim() === "");
      if (missing) return `"${missing.label}" is required for ${def.label}`;
      const crossErr = def.crossArgValidate?.(c.args);
      if (crossErr) return crossErr;
    }
    return null;
  }, [checks, totalTargetPairs, checkFunctions]);

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
      const fnDef = checkFunctions.find((f) => f.value === c.fn);
      if (fnDef) {
        // Per-arg syntax errors.
        if (fnDef.args.some((a) => validateArg(a.name, c.args[a.name] ?? "", c.fn) !== null)) {
          return true;
        }
        // Cross-arg constraints.
        if (fnDef.crossArgValidate && fnDef.crossArgValidate(c.args) !== null) {
          return true;
        }
        return false;
      }
      // Custom/unknown function — best-effort syntax check on every populated arg.
      return Object.keys(c.args).some((arg) => validateArg(arg, c.args[arg] ?? "", c.fn) !== null);
    });
  }, [checks, checkFunctions]);

  // Save handlers
  const handleSave = async (andSubmit: boolean) => {
    if (hasArgErrors) {
      toast.error("Fix validation errors before saving.");
      return;
    }
    setIsSaving(true);
    try {
      // A check is "existing" if it carries a ``ruleId`` from the catalog.
      // Each catalog row is bound to one ``table_fqn``; if the user added
      // tables to an existing draft, the original row is updated in place
      // and each *additional* table becomes a fresh rule. Tables that are
      // newly added but match no existing row (i.e. on a brand-new check)
      // fall through to the create-new path below.
      const existingChecks = checks.filter((c) => c.ruleId && c.fn);
      const newChecks = checks.filter((c) => !c.ruleId && c.fn && c.targetTables.length > 0);
      const additionalRuleSpecs: { check: CheckDraft; table: string }[] = [];

      let updatedCount = 0;
      let createdCount = 0;
      const failedMessages: string[] = [];

      // 1. Update existing rules in-place — only their original table.
      for (const c of existingChecks) {
        const dict = checkToDict(c);
        const orig = c.originalTable ?? c.targetTables[0];
        const stillTargetsOriginal = c.targetTables.includes(orig);
        try {
          const resp = await saveRules({
            table_fqn: orig,
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
        // Queue any *added* tables as fresh rules.
        for (const t of c.targetTables) {
          if (t === orig) continue;
          additionalRuleSpecs.push({ check: c, table: t });
        }
        if (!stillTargetsOriginal) {
          toast.warning(
            `${orig.split(".").pop()}: original target was removed in the editor but the rule was kept. Delete it from Drafts if you no longer want it.`,
            { duration: 7000 },
          );
        }
      }

      // 2. Create new rules — both for brand-new checks and for additional
      // tables tacked on to existing checks.
      if (newChecks.length > 0 || additionalRuleSpecs.length > 0) {
        const tableCheckMap: Record<string, Record<string, unknown>[]> = {};
        for (const c of newChecks) {
          const dict = checkToDict(c);
          for (const fqn of c.targetTables) {
            if (!tableCheckMap[fqn]) tableCheckMap[fqn] = [];
            tableCheckMap[fqn].push(dict);
          }
        }
        for (const spec of additionalRuleSpecs) {
          const dict = checkToDict(spec.check);
          if (!tableCheckMap[spec.table]) tableCheckMap[spec.table] = [];
          tableCheckMap[spec.table].push(dict);
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

          {/* Two-column row: check editor on the left, column discovery
              side panel on the right. On smaller viewports the panel
              stacks above the checks so it stays visible. */}
          <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_320px] gap-4 items-start">
            <div className="space-y-5 min-w-0 order-2 xl:order-1">
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
                  labelDefinitions={labelDefinitions}
                  checkFunctions={checkFunctions}
                />
              ))}
              <Button variant="outline" size="sm" onClick={addCheck} className="gap-1" disabled={isBusy}>
                <Plus className="h-3 w-3" />
                Add check
              </Button>
            </div>
            <div className="order-1 xl:order-2 min-w-0">
              <ColumnDiscoveryPanel
                variant="inline"
                defaultCollapsed={false}
                className="xl:sticky xl:top-4"
                onTableSelect={handleDiscoveryTableSelect}
              />
            </div>
          </div>
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
                  <Label className="text-xs text-muted-foreground whitespace-nowrap flex items-center gap-1">
                    Sample rows
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Info className="h-3 w-3 cursor-help" />
                      </TooltipTrigger>
                      <TooltipContent className="max-w-xs">
                        <p className="text-xs leading-relaxed">
                          Dry-run validates only the first <strong>1–10,000 rows</strong>{" "}
                          of the selected table — it&apos;s a fast preview, not a
                          full validation. To run rules against every row, go to{" "}
                          <strong>Run Rules</strong> and pick{" "}
                          <strong>&quot;All rows&quot;</strong>.
                        </p>
                      </TooltipContent>
                    </Tooltip>
                  </Label>
                  <Input
                    type="number"
                    min={1}
                    max={10000}
                    value={dryRunSampleSize}
                    onChange={(e) => setDryRunSampleSize(Math.min(10000, Math.max(1, Number(e.target.value) || 1000)))}
                    disabled={isBusy}
                    className="w-24 h-9 text-xs"
                  />
                  <span className="text-[10px] text-muted-foreground whitespace-nowrap">
                    max 10,000
                  </span>
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

/**
 * Friendly label for an argument input. Falls back to a humanized
 * version of the snake_case arg name (``cidr_block`` → ``Cidr Block``)
 * when we don't have an explicit label, so newly surfaced DQX arguments
 * still render reasonably without requiring a UI change.
 */
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
    case "name": return "Check Name";
    case "cidr_block": return "CIDR Block";
    case "value": return "Value";
    case "days": return "Days";
    case "offset": return "Offset (seconds)";
    case "max_age_minutes": return "Max Age (minutes)";
    case "keys": return "Required Keys";
    case "schema": return "Expected Schema";
    case "dimension": return "Dimension";
    case "trim_strings": return "Trim Strings";
    case "case_sensitive": return "Case Sensitive";
    case "nulls_distinct": return "Nulls Distinct";
    case "negate": return "Negate";
    case "require_all": return "Require All Keys";
    case "abs_tolerance": return "Absolute Tolerance";
    case "rel_tolerance": return "Relative Tolerance";
    case "min_value": return "Min Value";
    case "max_value": return "Max Value";
    default:
      return arg
        .split("_")
        .filter(Boolean)
        .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
        .join(" ");
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
    case "cidr_block": return "e.g. 192.168.1.0/24";
    case "value": return "comparison value";
    case "days": return "number of days";
    case "offset": return "seconds";
    case "max_age_minutes": return "maximum age in minutes";
    case "keys": return "comma-separated keys";
    case "dimension": return "0, 1, or 2";
    default: return "";
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
  labelDefinitions?: LabelDefinition[];
  checkFunctions: CheckFunctionDef[];
}

function CheckCard({ check, index, onUpdate, onRemove, canRemove, disabled, isDuplicate, labelDefinitions, checkFunctions }: CheckCardProps) {
  const fnDef = checkFunctions.find((f) => f.value === check.fn);
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
            <FunctionCombobox
              value={check.fn}
              functions={checkFunctions}
              disabled={disabled}
              onChange={(fn) => {
                // When swapping functions, keep ``col_name`` only if the
                // new function still takes a column, and pre-fill any
                // boolean defaults so the dropdown doesn't render blank.
                const def = checkFunctions.find((f) => f.value === fn);
                const next: Record<string, string> = {};
                if (def?.args.some((a) => a.name === "col_name") && check.args["col_name"]) {
                  next.col_name = check.args["col_name"];
                }
                for (const a of def?.args ?? []) {
                  if (a.defaultValue != null && next[a.name] == null) {
                    next[a.name] = a.defaultValue;
                  }
                }
                onUpdate(check.id, { fn, args: next });
              }}
            />
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
          <>
            <div className="grid gap-2 sm:grid-cols-2">
              {argFields.map((argDef) => {
                const val = check.args[argDef.name] ?? "";
                const isEmpty = val.trim() === "";
                const validationErr = validateArg(argDef.name, val, check.fn);
                const hasError = !!validationErr;
                const showRequiredHint = argDef.required && isEmpty;

                if (argDef.type === "boolean") {
                  // Boolean inputs render as a yes/no select. We always
                  // show a value (defaulting to the declared default)
                  // so the user can see the active state at a glance.
                  const current = isEmpty ? (argDef.defaultValue ?? "false") : val;
                  return (
                    <div key={argDef.name} className="space-y-1">
                      <Label className="text-[11px] text-muted-foreground">
                        {argDef.label}
                        {!argDef.required && <span className="ml-1 text-muted-foreground/60">(optional)</span>}
                      </Label>
                      <Select
                        value={current}
                        onValueChange={(v) =>
                          onUpdate(check.id, { args: { ...check.args, [argDef.name]: v } })
                        }
                        disabled={disabled}
                      >
                        <SelectTrigger className="h-7 text-xs w-full">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="true" className="text-xs">true</SelectItem>
                          <SelectItem value="false" className="text-xs">false</SelectItem>
                        </SelectContent>
                      </Select>
                      {argDef.help && (
                        <p className="text-[10px] text-muted-foreground/80">{argDef.help}</p>
                      )}
                    </div>
                  );
                }

                return (
                  <div key={argDef.name} className="space-y-1">
                    <Label
                      className={`text-[11px] ${
                        hasError
                          ? "text-red-500"
                          : showRequiredHint
                            ? "text-amber-500"
                            : "text-muted-foreground"
                      }`}
                    >
                      {argDef.label}
                      {showRequiredHint && " (required)"}
                      {!argDef.required && isEmpty && (
                        <span className="ml-1 text-muted-foreground/60">(optional)</span>
                      )}
                    </Label>
                    <Input
                      className={`h-7 text-xs ${
                        hasError
                          ? "border-red-400 focus-visible:ring-red-400"
                          : showRequiredHint
                            ? "border-amber-400 focus-visible:ring-amber-400"
                            : ""
                      }`}
                      placeholder={argDef.hint || argHint(argDef.name, check.fn)}
                      value={val}
                      onChange={(e) =>
                        onUpdate(check.id, { args: { ...check.args, [argDef.name]: e.target.value } })
                      }
                      disabled={disabled}
                    />
                    {hasError && (
                      <p className="text-[10px] text-red-500 flex items-center gap-1">
                        <AlertCircle className="h-2.5 w-2.5 shrink-0" />
                        {validationErr}
                      </p>
                    )}
                    {!hasError && argDef.help && (
                      <p className="text-[10px] text-muted-foreground/80">{argDef.help}</p>
                    )}
                  </div>
                );
              })}
            </div>
            {(() => {
              const crossErr = fnDef?.crossArgValidate?.(check.args);
              if (!crossErr) return null;
              return (
                <p className="text-[11px] text-red-500 flex items-center gap-1">
                  <AlertCircle className="h-3 w-3 shrink-0" />
                  {crossErr}
                </p>
              );
            })()}
          </>
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

        <div className="pt-1">
          <LabelsEditor
            value={check.userMetadata}
            onChange={(next) => onUpdate(check.id, { userMetadata: next })}
            disabled={disabled}
            defaultOpen={Object.keys(check.userMetadata).length > 0}
            definitions={labelDefinitions}
          />
        </div>
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


// ──────────────────────────────────────────────────────────────────────────────
// FunctionCombobox — searchable, grouped function picker
// ──────────────────────────────────────────────────────────────────────────────

/**
 * Replacement for the previous ``<Select>`` with one ``<SelectItem>`` per
 * function. Now that the registry can carry 30+ entries we need three
 * affordances the native select doesn't give us:
 *
 *   1. Free-text search so users who already know the function name
 *      (e.g. ``regex_match``) can land on it instantly.
 *   2. Category grouping so the long list is visually scannable
 *      (Null & Empty, Numeric & Comparable, Aggregates, …).
 *   3. Inline doc snippets so users can pick the right check without
 *      jumping out to DQX docs.
 *
 * Implementation is built on the same Popover + Input + Button primitives
 * already used by ``RoleManagement.GroupCombobox``; we deliberately keep
 * the dependency surface tiny so this works in offline-installed
 * environments.
 */
interface FunctionComboboxProps {
  value: string;
  functions: CheckFunctionDef[];
  onChange: (fn: string) => void;
  disabled?: boolean;
}

function FunctionCombobox({ value, functions, onChange, disabled }: FunctionComboboxProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  // Re-group on every change. The list is at most a few dozen entries
  // so the cost is negligible and avoiding a memo keeps the component
  // simpler to follow.
  const grouped = useMemo(() => {
    const q = query.trim().toLowerCase();
    const matched = functions.filter((f) =>
      q === "" ? true : f.value.toLowerCase().includes(q) || (f.doc ?? "").toLowerCase().includes(q),
    );
    const byCategory = new Map<string, CheckFunctionDef[]>();
    for (const fn of matched) {
      const cat = fn.category ?? "Other";
      if (!byCategory.has(cat)) byCategory.set(cat, []);
      byCategory.get(cat)!.push(fn);
    }
    // Stable category order: alphabetic, with "Other" pinned last so
    // unrecognised categories don't sneak above the curated buckets.
    return Array.from(byCategory.entries()).sort(([a], [b]) => {
      if (a === "Other") return 1;
      if (b === "Other") return -1;
      return a.localeCompare(b);
    });
  }, [functions, query]);

  const isUnknownFn = value !== "" && !functions.some((f) => f.value === value);
  const displayValue = value === "" ? "Select function" : isUnknownFn ? `${value} (custom)` : value;

  // Reset the search box every time the popover closes so a re-open
  // shows the full list rather than a stale filtered slice.
  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          disabled={disabled}
          className="h-8 w-full justify-between text-xs font-normal"
        >
          <span className={value === "" ? "text-muted-foreground" : ""}>{displayValue}</span>
          <ChevronDown className="h-3 w-3 opacity-50 shrink-0" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="p-0 w-[--radix-popover-trigger-width] min-w-[280px]"
        align="start"
      >
        <div className="border-b p-2">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
            <Input
              autoFocus
              placeholder="Search functions..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="h-8 text-xs pl-7"
            />
          </div>
        </div>
        <div className="max-h-72 overflow-y-auto py-1">
          {functions.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">Loading functions...</div>
          ) : grouped.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">No matches</div>
          ) : (
            grouped.map(([category, fns]) => (
              <div key={category}>
                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground bg-muted/40">
                  {category}
                </div>
                {fns.map((fn) => {
                  const selected = fn.value === value;
                  return (
                    <button
                      key={fn.value}
                      type="button"
                      onClick={() => {
                        onChange(fn.value);
                        setOpen(false);
                      }}
                      className={`w-full text-left px-2 py-1.5 text-xs hover:bg-accent flex items-start gap-2 ${selected ? "bg-accent" : ""}`}
                    >
                      <Check
                        className={`h-3 w-3 shrink-0 mt-0.5 ${selected ? "opacity-100" : "opacity-0"}`}
                      />
                      <span className="min-w-0 flex-1">
                        <span className="font-mono">{fn.value}</span>
                        {fn.doc && (
                          <span className="block text-[10px] text-muted-foreground truncate">
                            {fn.doc}
                          </span>
                        )}
                      </span>
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

