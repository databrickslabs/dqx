import { createFileRoute, useNavigate, Navigate } from "@tanstack/react-router";
import { useUnsavedGuard } from "@/hooks/use-unsaved-guard";
import { useState, useCallback, useEffect, useMemo, useRef } from "react";
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
  Search,
  Check,
  LayoutGrid,
  Settings2,
  History,
  Columns3,
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
  useGetRules,
  getGetRulesQueryKey,
  useListCheckFunctions,
  getTableColumns,
  type CheckFunctionDef as ApiCheckFunctionDef,
  type CheckFunctionParam as ApiCheckFunctionParam,
  type RuleCatalogEntryOut,
} from "@/lib/api";
import { useQueryClient } from "@tanstack/react-query";
import { filterTablesByColumns, checkDuplicates, type CheckDuplicatesIn, submitRuleForApproval, useLabelDefinitions, type LabelDefinition, getTablePreview, runDryRunOnPreview, runDryRunOnTable, type PreviewDryRunIn, type TableDryRunIn, type PreviewDryRunOut } from "@/lib/api-custom";
import { PreviewDryRunResultPanel } from "@/components/PreviewDryRunResultPanel";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { LabelsEditor } from "@/components/Labels";
import { getUserMetadata } from "@/lib/format-utils";
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
  /** True when the engine expects a ``columns`` list rather than a scalar
   *  ``column``. Drives serialization in ``checkToDict`` for functions like
   *  ``sql_expression`` that take ``columns: list[str]``. */
  isMultiColumn?: boolean;
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
type TFunc = (key: string, options?: Record<string, unknown>) => string;

type ArgOverride = Partial<Pick<CheckFunctionArg, "label" | "hint" | "help">>;
interface CheckFunctionOverride {
  /** Cross-argument validator (e.g. ``is_in_range`` requires at least one bound). */
  crossArgValidate?: (args: Record<string, string>, t: TFunc) => string | null;
  /** Per-argument label/hint/help overrides keyed by *engine* parameter name. */
  args?: Record<string, ArgOverride>;
}

function buildCheckFunctionOverrides(t: TFunc): Record<string, CheckFunctionOverride> {
  return {
    is_in_range: {
      crossArgValidate: (args) => {
        const hasMin = (args.min_limit ?? "").trim() !== "";
        const hasMax = (args.max_limit ?? "").trim() !== "";
        if (!hasMin && !hasMax) return t("rulesSingleTable.crossArgRangeNeedsBound");
        return null;
      },
    },
    is_not_in_range: {
      crossArgValidate: (args) => {
        const hasMin = (args.min_limit ?? "").trim() !== "";
        const hasMax = (args.max_limit ?? "").trim() !== "";
        if (!hasMin && !hasMax) return t("rulesSingleTable.crossArgRangeNeedsBound");
        return null;
      },
    },
    is_unique: {
      args: {
        columns: {
          label: t("rulesSingleTable.overrideColumnsLabel"),
          hint: t("rulesSingleTable.argHintIsUniqueColumns"),
        },
        nulls_distinct: { help: t("rulesSingleTable.overrideHelpNullsDistinct") },
      },
    },
    is_not_empty: {
      args: { trim_strings: { help: t("rulesSingleTable.overrideHelpTrimStrings") } },
    },
    is_not_null_and_not_empty: {
      args: { trim_strings: { help: t("rulesSingleTable.overrideHelpTrimStrings") } },
    },
    is_empty: {
      args: { trim_strings: { help: t("rulesSingleTable.overrideHelpTrimStrings") } },
    },
    is_null_or_empty: {
      args: { trim_strings: { help: t("rulesSingleTable.overrideHelpTrimStrings") } },
    },
    is_in_list: {
      args: { case_sensitive: { help: t("rulesSingleTable.overrideHelpCaseSensitive") } },
    },
    is_not_in_list: {
      args: { case_sensitive: { help: t("rulesSingleTable.overrideHelpCaseSensitive") } },
    },
    is_valid_date: {
      args: { date_format: { help: t("rulesSingleTable.overrideHelpDateFormat") } },
    },
    is_valid_timestamp: {
      args: { timestamp_format: { help: t("rulesSingleTable.overrideHelpTimestampFormat") } },
    },
    sql_expression: {
      args: {
        negate: { help: t("rulesSingleTable.overrideHelpNegateExpression") },
        msg: { hint: t("rulesSingleTable.overrideHintMsg") },
        name: { hint: t("rulesSingleTable.overrideHintName") },
      },
    },
    regex_match: {
      args: { negate: { help: t("rulesSingleTable.overrideHelpNegateRegex") } },
    },
  };
}

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
  t: TFunc,
): CheckFunctionArg {
  const isColumnParam = param.kind === "column" || param.kind === "columns";
  const localName = isColumnParam ? "col_name" : param.name;
  const overrideKey = param.name; // overrides are keyed by engine name
  const ov = overrides?.[overrideKey] ?? {};
  // ``columns`` list-of-columns is rendered through the same column
  // input but with a multi-column hint; honour any explicit override
  // first, then fall back to the generic per-arg hint.
  const defaultHint = argHint(localName, fnName, t) || (param.kind === "columns" ? t("rulesSingleTable.argHintIsUniqueColumns") : undefined);
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
    label: ov.label ?? argLabel(localName, t),
    type,
    required: param.required,
  };
  if (param.kind === "columns") arg.isMultiColumn = true;
  const hint = ov.hint ?? defaultHint;
  if (hint) arg.hint = hint;
  const help = ov.help;
  if (help) arg.help = help;
  if (param.default != null) arg.defaultValue = param.default;
  return arg;
}

function apiToCheckFunctions(apiList: ApiCheckFunctionDef[], t: TFunc): CheckFunctionDef[] {
  const overridesByFn = buildCheckFunctionOverrides(t);
  return apiList.map((api) => {
    const overrides = overridesByFn[api.name];
    const args = (api.params ?? []).map((p) => apiParamToArg(p, overrides?.args, api.name, t));
    const crossValidator = overrides?.crossArgValidate;
    return {
      value: api.name,
      label: api.name,
      doc: api.doc ?? "",
      category: api.category,
      ruleType: api.rule_type,
      args,
      ...(crossValidator
        ? { crossArgValidate: (args: Record<string, string>) => crossValidator(args, t) }
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
  const { t } = useTranslation();
  const { data } = useListCheckFunctions();
  const fns = useMemo(
    () => apiToCheckFunctions(data?.data?.functions ?? [], t),
    [data, t],
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
  /** Original user prompt / instruction that produced this check via AI. */
  description?: string;
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

    const argDef = argDefByName.get(k);

    // Some functions (is_unique, sql_expression, …) take a ``columns`` list
    // rather than a scalar ``column``. The API marks these with kind="columns"
    // which sets isMultiColumn on the arg def. Serialize as a list so DQX
    // validation doesn't reject with "Unexpected argument 'column'".
    if (k === "col_name" && (argDef?.isMultiColumn || c.fn === "is_unique")) {
      const cols = v.split(",").map((s) => s.trim()).filter(Boolean);
      if (cols.length > 0) args["columns"] = cols;
      continue;
    }
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
  if (c.description) {
    out._description = c.description;
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
    description: typeof raw._description === "string" ? raw._description : undefined,
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
  const { t } = useTranslation();
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
  const [activeDefineTab, setActiveDefineTab] = useState<"generator" | "activity" | "columns">("generator");

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
          toast.info(t("rulesSingleTable.editingRuleFor", { table: target.table_fqn.split(".").pop() }));
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
          toast.info(t("rulesSingleTable.loadedExistingRules", { count: drafts.length, table: initialTable!.split(".").pop() }));
        }
      }
    }
    setLoadedFromTable(true);
  }, [existingRulesResp]); // eslint-disable-line react-hooks/exhaustive-deps

  // Preview Data state — table selection and loaded rows persist to localStorage
  const PREVIEW_TABLE_LS_KEY = "dqx:single_table:preview_table";
  const PREVIEW_DATA_LS_KEY = "dqx:single_table:preview_data";

  type PreviewData = { columns: string[]; rows: Record<string, string | null>[]; col_types: Record<string, string> };

  const [previewTable, setPreviewTable] = useState<string>(() => {
    if (isTableFqn && initialTable) return initialTable;
    try {
      const stored = localStorage.getItem(PREVIEW_TABLE_LS_KEY);
      if (stored && stored.split(".").length === 3) return stored;
    } catch { /* ignore */ }
    return "";
  });
  const [previewData, setPreviewData] = useState<PreviewData | null>(() => {
    try {
      const raw = localStorage.getItem(PREVIEW_DATA_LS_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { fqn: string; data: PreviewData };
      const storedFqn = localStorage.getItem(PREVIEW_TABLE_LS_KEY);
      return parsed.fqn === storedFqn ? parsed.data : null;
    } catch { return null; }
  });
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewFilterQuery, setPreviewFilterQuery] = useState("");
  const [dryRunSampleSize, setDryRunSampleSize] = useState(1000);
  const isPreviewTableFqn = previewTable.split(".").length === 3;

  const handlePreviewTableChange = useCallback((fqn: string) => {
    setPreviewTable(fqn);
    setPreviewData(null);
    setPreviewFilterQuery("");
    try {
      localStorage.setItem(PREVIEW_TABLE_LS_KEY, fqn);
      localStorage.removeItem(PREVIEW_DATA_LS_KEY);
    } catch { /* ignore */ }
  }, []);

  const handleLoadPreview = useCallback(async (filterQ?: string) => {
    if (!isPreviewTableFqn) return;
    const [cat, sch, tbl] = previewTable.split(".");
    setPreviewLoading(true);
    try {
      const [previewResp, colsResp] = await Promise.all([
        getTablePreview(cat, sch, tbl, dryRunSampleSize, filterQ),
        getTableColumns(cat, sch, tbl),
      ]);
      const col_types: Record<string, string> = {};
      for (const col of colsResp.data) {
        col_types[col.name] = col.type_name;
      }
      const data: PreviewData = { ...previewResp.data, col_types };
      setPreviewData(data);
      if (!filterQ) {
        try { localStorage.setItem(PREVIEW_DATA_LS_KEY, JSON.stringify({ fqn: previewTable, data })); } catch { /* ignore */ }
      }
    } catch {
      toast.error(t("rulesSingleTable.previewLoadError"));
    } finally {
      setPreviewLoading(false);
    }
  }, [previewTable, isPreviewTableFqn, dryRunSampleSize, t]);

  // AI generation state
  const [aiPrompt, setAiPrompt] = useState("");
  const [aiGenerating, setAiGenerating] = useState(false);
  const [previewDryRunResult, setPreviewDryRunResult] = useState<PreviewDryRunOut | null>(null);

  const [dryRunSubmitting, setDryRunSubmitting] = useState(false);

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
        toast.info(t("rulesSingleTable.toastAllChecksHaveTarget"), {
          description: t("rulesSingleTable.toastAllChecksHaveTargetDescription", { table: fqn }),
        });
        return prev;
      }
      toast.success(t("rulesSingleTable.toastTargetedChecks", { count: empties.length, table: fqn }), {
        description: t("rulesSingleTable.toastTargetedChecksDescription"),
        duration: 2500,
      });
      return prev.map((c) =>
        c.targetTables.length === 0 ? { ...c, targetTables: [fqn] } : c,
      );
    });
  }, [t]);

  const effectiveTable = isPreviewTableFqn ? previewTable : (isTableFqn ? initialTable : undefined);

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
        .filter((d): d is CheckDraft => d !== null);
      for (const d of drafts) {
        d.description = aiPrompt.trim();
        if (effectiveTable) d.targetTables = [effectiveTable];
      }
      if (drafts.length === 0) {
        toast.error(t("rulesSingleTable.toastAiNoChecks"));
        return;
      }
      const hasOnlyEmptyDefault = checks.length === 1 && checks[0].fn === "";
      setChecks(hasOnlyEmptyDefault ? drafts : [...checks, ...drafts]);
      toast.success(t("rulesSingleTable.toastAiGenerated", { count: drafts.length }));
    } catch {
      toast.error(t("rulesSingleTable.toastAiFailed"));
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
          return def.args.every((a) => validateArg(a.name, c.args[a.name] ?? "", c.fn, t) === null);
        }
        // Custom/unknown function: valid as long as all populated args are non-empty.
        return Object.values(c.args).every((v) => (v ?? "").trim() !== "");
      }) &&
      totalTargetPairs > 0
    );
  }, [checks, totalTargetPairs, checkFunctions, t]);

  const validationMessage = useMemo(() => {
    if (checks.some((c) => c.fn === "")) return t("rulesSingleTable.selectFunctionForEvery");
    if (totalTargetPairs === 0) return t("rulesSingleTable.assignAtLeastOneTable");
    // Surface the first incomplete required arg so the user knows what's missing.
    for (const c of checks) {
      const def = checkFunctions.find((f) => f.value === c.fn);
      if (!def) continue;
      const missing = def.args.find((a) => a.required && (c.args[a.name] ?? "").trim() === "");
      if (missing) return t("rulesSingleTable.argRequiredFor", { label: missing.label, fn: def.label });
      const crossErr = def.crossArgValidate?.(c.args);
      if (crossErr) return crossErr;
    }
    return null;
  }, [checks, totalTargetPairs, checkFunctions, t]);

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
  const isDryRunning = dryRunSubmitting;

  const justSavedRef = useRef(false);

  const hasUnsavedChanges = useMemo(
    () => checks.some((c) => c.fn !== "" || c.targetTables.length > 0),
    [checks],
  );

  const { blocker } = useUnsavedGuard({ hasUnsavedChanges, isRunning: isDryRunning, bypassRef: justSavedRef });

  const handleConfirmLeave = () => {
    blocker.proceed?.();
  };

  const hasPreviewForDryRun = isPreviewTableFqn && previewData !== null;

  const checksForPreviewTable = useMemo(
    () => (isPreviewTableFqn ? buildChecksForTable(checks, previewTable) : []),
    [checks, previewTable, isPreviewTableFqn],
  );

  const handleDryRun = async (overrideChecks?: CheckDraft[]) => {
    const checksToRun = overrideChecks ?? checks;
    if (!previewTable) {
      toast.error(t("rulesSingleTable.toastSelectTable"));
      return;
    }
    const checksForTable = buildChecksForTable(checksToRun, previewTable);
    if (checksForTable.length === 0) {
      toast.error(t("rulesSingleTable.toastNoChecksTargetTable"));
      return;
    }
    try {
      setPreviewDryRunResult(null);
      setDryRunSubmitting(true);

      if (previewData) {
        // Inline path using pre-loaded preview rows (fastest — no table read)
        const body: PreviewDryRunIn = {
          table_fqn: previewTable,
          checks: checksForTable as Array<Record<string, unknown>>,
          rows: previewData.rows,
        };
        const r = await runDryRunOnPreview(body);
        setPreviewDryRunResult(r.data);
        toast.success(t("rulesSingleTable.toastDryRunComplete"));
      } else {
        // Inline path reading directly from the live table via Spark (no job submission)
        const body: TableDryRunIn = {
          table_fqn: previewTable,
          checks: checksForTable as Array<Record<string, unknown>>,
          sample_size: dryRunSampleSize,
        };
        const r = await runDryRunOnTable(body);
        setPreviewDryRunResult(r.data);
        toast.success(t("rulesSingleTable.toastDryRunComplete"));
      }
    } catch (err) {
      const axErr = err as { response?: { data?: { detail?: string } } };
      const detail = axErr?.response?.data?.detail;
      toast.error(detail ? t("rulesSingleTable.toastDryRunSubmitFailedDetail", { detail }) : t("rulesSingleTable.toastDryRunSubmitFailed"));
      console.error("Dry run error:", err);
    } finally {
      setDryRunSubmitting(false);
    }
  };

  const handleLoadChecks = (newChecks: CheckDraft[]) => {
    const hasOnlyEmptyDefault = checks.length === 1 && checks[0].fn === "";
    setChecks(hasOnlyEmptyDefault ? newChecks : [...checks, ...newChecks]);
    setActiveDefineTab("generator");
    toast.success(t("rulesSingleTable.toastLoadedFromActivity", { count: newChecks.length }));
  };

  const handleLoadAndRunDryRun = async (newChecks: CheckDraft[]) => {
    const hasOnlyEmptyDefault = checks.length === 1 && checks[0].fn === "";
    const merged = hasOnlyEmptyDefault ? newChecks : [...checks, ...newChecks];
    setChecks(merged);
    setActiveDefineTab("generator");
    await handleDryRun(merged);
  };

  const hasArgErrors = useMemo(() => {
    return checks.some((c) => {
      const fnDef = checkFunctions.find((f) => f.value === c.fn);
      if (fnDef) {
        // Per-arg syntax errors.
        if (fnDef.args.some((a) => validateArg(a.name, c.args[a.name] ?? "", c.fn, t) !== null)) {
          return true;
        }
        // Cross-arg constraints.
        if (fnDef.crossArgValidate && fnDef.crossArgValidate(c.args) !== null) {
          return true;
        }
        return false;
      }
      // Custom/unknown function — best-effort syntax check on every populated arg.
      return Object.keys(c.args).some((arg) => validateArg(arg, c.args[arg] ?? "", c.fn, t) !== null);
    });
  }, [checks, checkFunctions, t]);

  // Save handlers
  const handleSave = async (andSubmit: boolean) => {
    if (hasArgErrors) {
      toast.error(t("rulesSingleTable.toastFixValidationErrors"));
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
                  toast.warning(t("rulesSingleTable.toastUpdatedSubmissionFailed", { detail }), { duration: 6000 });
                }
              }
            }
          }
        } catch (e) {
          const detail = (e as { body?: { detail?: string } })?.body?.detail ?? String(e);
          failedMessages.push(t("rulesSingleTable.toastUpdateFailed", { detail }));
        }
        // Queue any *added* tables as fresh rules.
        for (const tbl of c.targetTables) {
          if (tbl === orig) continue;
          additionalRuleSpecs.push({ check: c, table: tbl });
        }
        if (!stillTargetsOriginal) {
          toast.warning(
            t("rulesSingleTable.toastOriginalTargetRemoved", { table: orig.split(".").pop() }),
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
            t("rulesSingleTable.toastDuplicatesExist", { messages: dupMessages.join("\n") }),
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
                      toast.warning(t("rulesSingleTable.toastSavedSubmissionFailed", { detail }), { duration: 6000 });
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
        if (updatedCount > 0) parts.push(t("rulesSingleTable.toastUpdatedCount", { count: updatedCount }));
        if (createdCount > 0) parts.push(t("rulesSingleTable.toastCreatedCount", { count: createdCount }));
        toast.success(
          andSubmit
            ? t("rulesSingleTable.toastRulesSavedAndSubmitted", { parts: parts.join(", ") })
            : t("rulesSingleTable.toastRulesSavedAsDrafts", { parts: parts.join(", ") }),
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
      toast.error(t("rulesSingleTable.toastFailedSaveRules"));
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
        <span>{t("rulesSingleTable.loadingRulesFor", { table: initialTable?.split(".").pop() })}</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="space-y-2">
        <PageBreadcrumb
          items={[{ label: t("rulesSingleTable.createRulesBreadcrumb"), to: "/rules/create" }]}
          page={isEditMode ? t("rulesSingleTable.editTitle") : t("rulesSingleTable.title")}
        />
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {isEditMode ? t("rulesSingleTable.editTitle") : t("rulesSingleTable.title")}
          </h1>
          <p className="text-muted-foreground">
            {isEditMode
              ? t("rulesSingleTable.editSubtitle", { table: initialTable })
              : t("rulesSingleTable.createSubtitle")}
          </p>
        </div>
      </div>

      {/* Block 1: Preview Data */}
      <Card>
        <CardContent className="p-4 space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Table2 className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
                {t("rulesSingleTable.source")}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1.5">
                <Label className="text-xs text-muted-foreground whitespace-nowrap">
                  {t("rulesSingleTable.sampleRows")}
                </Label>
                <Input
                  type="number"
                  min={1}
                  max={10000}
                  value={dryRunSampleSize}
                  onChange={(e) => setDryRunSampleSize(Math.min(10000, Math.max(1, Number(e.target.value) || 1000)))}
                  className="w-20 h-8 text-xs"
                />
              </div>
              <Button
                size="sm"
                variant="outline"
                onClick={() => { setPreviewFilterQuery(""); void handleLoadPreview(); }}
                disabled={!isPreviewTableFqn || previewLoading}
                className="gap-2"
              >
                {previewLoading ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <LayoutGrid className="h-3.5 w-3.5" />
                )}
                {previewLoading ? t("rulesSingleTable.loadingPreview") : t("rulesSingleTable.loadDataPreview")}
              </Button>
            </div>
          </div>
          <CatalogBrowser
            value={previewTable}
            onChange={handlePreviewTableChange}
            disabled={previewLoading}
          />
          {previewData && previewData.columns.length > 0 && (
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 shrink-0">
                  <LayoutGrid className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-sm font-mono font-bold">
                    {t("rulesSingleTable.datasetLabel", { name: previewTable })}
                  </span>
                </div>
                <div className="flex items-center gap-1.5 flex-1 max-w-sm">
                  <div className="relative flex-1">
                    <Sparkles className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-primary" />
                    <Input
                      value={previewFilterQuery}
                      onChange={(e) => setPreviewFilterQuery(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") void handleLoadPreview(previewFilterQuery || undefined);
                      }}
                      placeholder={t("rulesSingleTable.previewFilterPlaceholder")}
                      className="h-8 pl-8 text-xs"
                      disabled={previewLoading}
                    />
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-8 w-8 p-0 shrink-0"
                    disabled={previewLoading}
                    onClick={() => void handleLoadPreview(previewFilterQuery || undefined)}
                  >
                    {previewLoading ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Search className="h-3.5 w-3.5" />
                    )}
                  </Button>
                </div>
              </div>
              <div className="overflow-auto border rounded-md max-h-[400px]">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 z-10 border-b bg-muted/90 backdrop-blur-sm">
                    <tr>
                      {previewData.columns.map((col) => (
                        <th
                          key={col}
                          className="px-3 py-2 text-left font-semibold uppercase tracking-wide text-[10px] text-muted-foreground whitespace-nowrap font-mono"
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewData.rows.map((row, i) => (
                      <tr key={i} className="border-b last:border-0 hover:bg-muted/20 transition-colors">
                        {previewData.columns.map((col) => (
                          <td key={col} className="px-3 py-2 font-mono text-[11px] whitespace-nowrap text-muted-foreground">
                            {row[col] ?? <span className="italic text-muted-foreground/40">null</span>}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
          {!previewData && !previewLoading && isPreviewTableFqn && (
            <p className="text-xs text-muted-foreground text-center py-2">
              {t("rulesSingleTable.noPreviewSelected")}
            </p>
          )}
        </CardContent>
      </Card>

      {/* Block 2: Define Rules */}
      <Card>
        <Tabs
          value={activeDefineTab}
          onValueChange={(v) => setActiveDefineTab(v as "generator" | "activity" | "columns")}
        >
          <div className="px-4 pt-4 pb-3 border-b">
            <TabsList className="h-9 rounded-full bg-muted/60 p-1">
              <TabsTrigger
                value="generator"
                className="rounded-full text-xs gap-1.5 px-3 data-[state=active]:bg-background data-[state=active]:shadow-sm"
              >
                <Settings2 className="h-3.5 w-3.5" />
                {t("rulesSingleTable.ruleGeneratorTab")}
              </TabsTrigger>
              <TabsTrigger
                value="columns"
                className="rounded-full text-xs gap-1.5 px-3 data-[state=active]:bg-background data-[state=active]:shadow-sm"
              >
                <Columns3 className="h-3.5 w-3.5" />
                {t("rulesSingleTable.columnsTab")}
              </TabsTrigger>
              <TabsTrigger
                value="activity"
                className="rounded-full text-xs gap-1.5 px-3 data-[state=active]:bg-background data-[state=active]:shadow-sm"
              >
                <History className="h-3.5 w-3.5" />
                {t("rulesSingleTable.recentActivityTab")}
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="generator" className="mt-0 focus-visible:ring-0 focus-visible:outline-none">
            <CardContent className="space-y-5 pt-5">
              {/* AI generation — table is selected in Preview Data block above */}
              <div className="border border-violet-200 dark:border-violet-800 rounded-lg p-4 bg-violet-50/50 dark:bg-violet-950/30 space-y-3">
                <div className="flex items-center gap-2 mb-1">
                  <Sparkles className="h-4 w-4 text-violet-600 dark:text-violet-400" />
                  <span className="text-sm font-medium text-violet-900 dark:text-violet-200">{t("rulesSingleTable.generateWithAi")}</span>
                  {isPreviewTableFqn && (
                    <Badge variant="outline" className="ml-auto text-[10px] font-mono">
                      {previewTable.split(".").pop()}
                    </Badge>
                  )}
                </div>
                <div className="flex gap-2">
                  <Textarea
                    value={aiPrompt}
                    onChange={(e) => setAiPrompt(e.target.value)}
                    placeholder={t("rulesSingleTable.aiPromptPlaceholder")}
                    className="min-h-[52px] text-sm"
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
                    {aiGenerating ? t("rulesSingleTable.generating") : t("rulesSingleTable.generate")}
                  </Button>
                </div>
              </div>

              {/* Check editor — full width now that column discovery is its own tab */}
              <div className="space-y-5">
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
                  {t("rulesSingleTable.addCheck")}
                </Button>
              </div>
            </CardContent>
          </TabsContent>

          <TabsContent value="columns" className="mt-0 focus-visible:ring-0 focus-visible:outline-none">
            <CardContent className="pt-5">
              <ColumnDiscoveryPanel
                variant="inline"
                defaultCollapsed={false}
                onTableSelect={handleDiscoveryTableSelect}
                initialTableFqn={isPreviewTableFqn ? previewTable : undefined}
              />
            </CardContent>
          </TabsContent>

          <TabsContent value="activity" className="mt-0 focus-visible:ring-0 focus-visible:outline-none">
            <CardContent className="pt-5">
              <RecentActivityPanel
                tableFqn={previewTable}
                isPreviewTableFqn={isPreviewTableFqn}
                onLoadChecks={handleLoadChecks}
                onValidate={handleLoadAndRunDryRun}
              />
            </CardContent>
          </TabsContent>
        </Tabs>
      </Card>

      {/* Step 2: Validate & Save */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <span className="inline-flex items-center justify-center h-6 w-6 rounded-full bg-primary text-primary-foreground text-xs font-bold">2</span>
            {t("rulesSingleTable.validateSaveStep")}
          </CardTitle>
          <CardDescription>
            {t("rulesSingleTable.validateSaveDescription")}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Dry run section */}
          {isPreviewTableFqn && (
            <div className="space-y-3 border rounded-lg p-4 bg-muted/20">
              <div className="flex items-center gap-3 flex-wrap">
                <div className="flex items-center gap-2 min-w-0">
                  <Table2 className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                  <span className="text-xs font-mono text-muted-foreground truncate">{previewTable}</span>
                </div>
                <div className="flex items-center gap-2 ml-auto">
                  {hasPreviewForDryRun && (
                    <Badge variant="secondary" className="text-[10px] gap-1 font-normal">
                      <LayoutGrid className="h-3 w-3" />
                      {t("rulesSingleTable.usingPreviewData")}
                    </Badge>
                  )}
                  <Button
                    variant="outline"
                    onClick={() => handleDryRun()}
                    disabled={checksForPreviewTable.length === 0 || isBusy || isDryRunning}
                    className="gap-2"
                    size="sm"
                  >
                    {isDryRunning ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                    {isDryRunning ? t("rulesSingleTable.running") : t("rulesSingleTable.dryRun")}
                  </Button>
                </div>
              </div>

              {previewDryRunResult && (
                <>
                  <Separator />
                  <PreviewDryRunResultPanel result={previewDryRunResult} />
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
                    {t("rulesSingleTable.duplicateChecksExist", {
                      count: dupCheckIds.size,
                      tables: t("rulesSingleTable.selectedTable", { count: allTargetTables.length }),
                    })}
                  </span>
                </>
              ) : (
                <span>
                  {t("rulesSingleTable.summary", {
                    checks: t("rulesSingleTable.summaryChecks", { count: checks.filter((c) => c.fn !== "").length }),
                    rules: t("rulesSingleTable.summaryRules", { count: totalTargetPairs }),
                    tables: t("rulesSingleTable.summaryTables", { count: allTargetTables.length }),
                  })}
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
                {t("common.cancel")}
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
                {isSingleRuleEdit ? t("rulesSingleTable.updateRule") : t("rulesSingleTable.saveAsDrafts")}
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
                {isSingleRuleEdit ? t("rulesSingleTable.updateAndSubmit") : t("rulesSingleTable.submitForReview")}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <AlertDialog open={blocker.status === "blocked"}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {isDryRunning ? t("rulesSingleTable.dryRunInProgressTitle") : t("rulesSingleTable.unsavedChangesTitle")}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {isDryRunning
                ? t("rulesSingleTable.dryRunInProgressDescription")
                : t("rulesSingleTable.unsavedChangesDescription")}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => blocker.reset?.()}>{t("rulesSingleTable.stayOnPage")}</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmLeave} className="bg-destructive text-white hover:bg-destructive/90">
              {isDryRunning ? t("rulesSingleTable.leaveCancelDryRun") : t("rulesSingleTable.discardLeave")}
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

function validateArg(arg: string, value: string, fn: string | undefined, t: TFunc): string | null {
  if (!value.trim()) return null;
  switch (arg) {
    case "col_name": {
      const names = fn === "is_unique"
        ? value.split(",").map((s) => s.trim()).filter(Boolean)
        : [value.trim()];
      for (const name of names) {
        if (!COLUMN_NAME_REGEX.test(name)) {
          return t("rulesSingleTable.argInvalidColumnName", { name });
        }
      }
      return null;
    }
    case "allowed":
    case "forbidden": {
      if (value.includes(";")) {
        return t("rulesSingleTable.argSemicolonsListNotAllowed");
      }
      const items = value.split(",").map((s) => s.trim());
      for (const item of items) {
        if (SQL_KEYWORD_PATTERN.test(item)) {
          return t("rulesSingleTable.argProhibitedSqlKeyword", { item });
        }
      }
      return null;
    }
    case "limit":
    case "min_limit":
    case "max_limit": {
      if (value.trim() !== "" && isNaN(Number(value.trim()))) {
        return t("rulesSingleTable.argMustBeNumeric");
      }
      return null;
    }
    case "expression": {
      if (value.includes(";")) {
        return t("rulesSingleTable.argSemicolonsExpressionNotAllowed");
      }
      if (SQL_KEYWORD_PATTERN.test(value)) {
        return t("rulesSingleTable.argExpressionProhibitedKeyword");
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
function argLabel(arg: string, t: TFunc): string {
  switch (arg) {
    case "col_name": return t("rulesSingleTable.argLabelColumnName");
    case "allowed": return t("rulesSingleTable.argLabelAllowed");
    case "forbidden": return t("rulesSingleTable.argLabelForbidden");
    case "limit": return t("rulesSingleTable.argLabelLimit");
    case "min_limit": return t("rulesSingleTable.argLabelMinLimit");
    case "max_limit": return t("rulesSingleTable.argLabelMaxLimit");
    case "regex": return t("rulesSingleTable.argLabelRegex");
    case "date_format": return t("rulesSingleTable.argLabelDateFormat");
    case "timestamp_format": return t("rulesSingleTable.argLabelTimestampFormat");
    case "expression": return t("rulesSingleTable.argLabelExpression");
    case "msg": return t("rulesSingleTable.argLabelMsg");
    case "name": return t("rulesSingleTable.argLabelName");
    case "cidr_block": return t("rulesSingleTable.argLabelCidrBlock");
    case "value": return t("rulesSingleTable.argLabelValue");
    case "days": return t("rulesSingleTable.argLabelDays");
    case "offset": return t("rulesSingleTable.argLabelOffset");
    case "max_age_minutes": return t("rulesSingleTable.argLabelMaxAgeMinutes");
    case "keys": return t("rulesSingleTable.argLabelKeys");
    case "schema": return t("rulesSingleTable.argLabelSchema");
    case "dimension": return t("rulesSingleTable.argLabelDimension");
    case "trim_strings": return t("rulesSingleTable.argLabelTrimStrings");
    case "case_sensitive": return t("rulesSingleTable.argLabelCaseSensitive");
    case "nulls_distinct": return t("rulesSingleTable.argLabelNullsDistinct");
    case "negate": return t("rulesSingleTable.argLabelNegate");
    case "require_all": return t("rulesSingleTable.argLabelRequireAll");
    case "abs_tolerance": return t("rulesSingleTable.argLabelAbsTolerance");
    case "rel_tolerance": return t("rulesSingleTable.argLabelRelTolerance");
    case "min_value": return t("rulesSingleTable.argLabelMinValue");
    case "max_value": return t("rulesSingleTable.argLabelMaxValue");
    default:
      return arg
        .split("_")
        .filter(Boolean)
        .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
        .join(" ");
  }
}

function argHint(arg: string, fn: string | undefined, t: TFunc): string {
  if (arg === "col_name" && fn === "is_unique") {
    return t("rulesSingleTable.argHintIsUniqueColumns");
  }
  switch (arg) {
    case "col_name": return t("rulesSingleTable.argHintColumnName");
    case "allowed": return t("rulesSingleTable.argHintAllowed");
    case "forbidden": return t("rulesSingleTable.argHintForbidden");
    case "limit": return t("rulesSingleTable.argHintLimit");
    case "min_limit": return t("rulesSingleTable.argHintMinLimit");
    case "max_limit": return t("rulesSingleTable.argHintMaxLimit");
    case "regex": return t("rulesSingleTable.argHintRegex");
    case "date_format": return t("rulesSingleTable.argHintDateFormat");
    case "timestamp_format": return t("rulesSingleTable.argHintTimestampFormat");
    case "expression": return t("rulesSingleTable.argHintExpression");
    case "msg": return t("rulesSingleTable.argHintMsg");
    case "cidr_block": return t("rulesSingleTable.argHintCidrBlock");
    case "value": return t("rulesSingleTable.argHintValue");
    case "days": return t("rulesSingleTable.argHintDays");
    case "offset": return t("rulesSingleTable.argHintOffset");
    case "max_age_minutes": return t("rulesSingleTable.argHintMaxAgeMinutes");
    case "keys": return t("rulesSingleTable.argHintKeys");
    case "dimension": return t("rulesSingleTable.argHintDimension");
    default: return "";
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// RecentActivityPanel
// ──────────────────────────────────────────────────────────────────────────────

function RecentActivityPanel({
  tableFqn,
  isPreviewTableFqn,
  onLoadChecks,
  onValidate,
}: {
  tableFqn: string;
  isPreviewTableFqn: boolean;
  onLoadChecks?: (checks: CheckDraft[]) => void;
  onValidate?: (checks: CheckDraft[]) => void;
}) {
  const { t } = useTranslation();
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const { data: rulesResp, isLoading } = useGetRules(tableFqn, {
    query: { enabled: isPreviewTableFqn },
  });

  if (!isPreviewTableFqn) {
    return (
      <p className="text-sm text-muted-foreground text-center py-6">
        {t("rulesSingleTable.selectTableForActivity")}
      </p>
    );
  }

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 py-6 justify-center">
        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        <span className="text-sm text-muted-foreground">{t("common.loading", "Loading…")}</span>
      </div>
    );
  }

  const entries: RuleCatalogEntryOut[] = Array.isArray(rulesResp?.data) ? rulesResp.data : [];

  if (entries.length === 0) {
    return (
      <p className="text-sm text-muted-foreground text-center py-6">
        {t("rulesSingleTable.noRulesForTable")}
      </p>
    );
  }

  const statusVariant: Record<string, "default" | "secondary" | "destructive" | "outline"> = {
    approved: "default",
    draft: "secondary",
    pending: "outline",
    rejected: "destructive",
  };

  const toggleId = (id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const buildDraftsFromSelected = (): CheckDraft[] => {
    const drafts: CheckDraft[] = [];
    for (const entry of entries) {
      const id = entry.rule_id ?? "";
      if (!selectedIds.has(id)) continue;
      for (const c of entry.checks ?? []) {
        const draft = savedCheckToDraft(c as Record<string, unknown>, entry.table_fqn);
        if (draft) {
          if (entry.rule_id) { draft.ruleId = entry.rule_id; draft.originalTable = entry.table_fqn; }
          drafts.push(draft);
        }
      }
    }
    return drafts;
  };

  return (
    <div className="space-y-3">
      <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
        {t("rulesSingleTable.recentRulesFor", { table: tableFqn.split(".").pop() })}
      </p>
      <div className="divide-y rounded-md border">
        {entries.map((entry, i) => {
          const id = entry.rule_id ?? String(i);
          const selected = selectedIds.has(id);
          const rawCheck = entry.checks[0] as Record<string, unknown> | undefined;
          const description = typeof rawCheck?._description === "string" ? rawCheck._description : undefined;
          const createdAtRaw = entry.created_at;
          const createdAt = createdAtRaw
            ? (() => { try { return new Date(createdAtRaw).toLocaleString(); } catch { return createdAtRaw; } })()
            : null;
          return (
            <div
              key={id}
              className={`flex items-start gap-3 px-3 py-3 cursor-pointer hover:bg-muted/20 transition-colors ${selected ? "bg-primary/5 dark:bg-primary/10" : ""}`}
              onClick={() => toggleId(id)}
            >
              <div className={`mt-0.5 h-4 w-4 rounded border-2 flex items-center justify-center shrink-0 transition-colors ${selected ? "bg-primary border-primary" : "border-muted-foreground/40"}`}>
                {selected && <Check className="h-2.5 w-2.5 text-primary-foreground" />}
              </div>
              <div className="flex-1 min-w-0 space-y-1">
                <div className="flex items-center gap-2">
                  <p className="text-sm font-semibold truncate leading-tight">{entry.table_fqn.split(".").pop()}</p>
                  <Badge variant={statusVariant[entry.status] ?? "outline"} className="capitalize shrink-0 text-[10px]">
                    {entry.status}
                  </Badge>
                </div>
                {createdAt && (
                  <p className="text-[11px] text-muted-foreground flex items-center gap-1">
                    <History className="h-3 w-3 shrink-0" />
                    {createdAt}
                  </p>
                )}
                {description && (
                  <p className="text-[11px] text-muted-foreground/80 italic line-clamp-2">
                    &ldquo;{description}&rdquo;
                  </p>
                )}
                <p className="text-[10px] text-muted-foreground/60 font-mono">
                  {entry.checks.length} {entry.checks.length === 1 ? "check" : "checks"} · v{entry.version}
                </p>
              </div>
            </div>
          );
        })}
      </div>
      {selectedIds.size > 0 && (
        <div className="flex gap-2">
          {onLoadChecks && (
            <Button
              size="sm"
              variant="outline"
              className="flex-1 gap-1.5"
              onClick={() => { const drafts = buildDraftsFromSelected(); if (drafts.length) onLoadChecks(drafts); }}
            >
              <Settings2 className="h-3.5 w-3.5" />
              {t("rulesSingleTable.loadIntoEditor")}
            </Button>
          )}
          {onValidate && (
            <Button
              size="sm"
              className="flex-1 gap-1.5"
              onClick={() => { const drafts = buildDraftsFromSelected(); if (drafts.length) onValidate(drafts); }}
            >
              <Play className="h-3.5 w-3.5" />
              {t("rulesSingleTable.validateSelected")}
            </Button>
          )}
        </div>
      )}
    </div>
  );
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
  const { t } = useTranslation();
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
          <span className="text-xs font-semibold text-muted-foreground">{t("rulesSingleTable.checkLabel", { index: index + 1 })}</span>
          {columns.length > 0 && (
            <Badge variant="outline" className="text-[10px] font-mono gap-1">
              {t("rulesSingleTable.colBadge", { columns: columns.join(", ") })}
            </Badge>
          )}
          {check.targetTables.length > 0 && (
            <Badge variant="secondary" className="text-[10px] gap-1">
              <Table2 className="h-2.5 w-2.5" />
              {t("rulesSingleTable.tablesBadge", { count: check.targetTables.length })}
            </Badge>
          )}
          {isDuplicate && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Badge variant="destructive" className="text-[10px] gap-1">
                    <AlertCircle className="h-2.5 w-2.5" />
                    {t("rulesSingleTable.duplicateBadge")}
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  <p>{t("rulesSingleTable.duplicateTooltip")}</p>
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
            <Label className="text-xs">{t("rulesSingleTable.function")}</Label>
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
            <Label className="text-xs">{t("rulesSingleTable.criticality")}</Label>
            <Select
              value={check.criticality}
              onValueChange={(v) => onUpdate(check.id, { criticality: v as "warn" | "error" })}
              disabled={disabled}
            >
              <SelectTrigger className="h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="warn" className="text-xs">{t("rulesSingleTable.criticalityWarn")}</SelectItem>
                <SelectItem value="error" className="text-xs">{t("rulesSingleTable.criticalityError")}</SelectItem>
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
                const validationErr = validateArg(argDef.name, val, check.fn, t);
                const hasError = !!validationErr;
                const showRequiredHint = argDef.required && isEmpty;
                const isQueryArg = argDef.name === "query";

                if (argDef.type === "boolean") {
                  // Boolean inputs render as a yes/no select. We always
                  // show a value (defaulting to the declared default)
                  // so the user can see the active state at a glance.
                  const current = isEmpty ? (argDef.defaultValue ?? "false") : val;
                  return (
                    <div key={argDef.name} className="space-y-1">
                      <Label className="text-[11px] text-muted-foreground">
                        {argDef.label}
                        {!argDef.required && <span className="ml-1 text-muted-foreground/60">{t("rulesSingleTable.optional")}</span>}
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
                  <div key={argDef.name} className={`space-y-1 ${isQueryArg ? "col-span-full" : ""}`}>
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
                      {showRequiredHint && ` ${t("rulesSingleTable.required")}`}
                      {!argDef.required && isEmpty && (
                        <span className="ml-1 text-muted-foreground/60">{t("rulesSingleTable.optional")}</span>
                      )}
                    </Label>
                    {isQueryArg ? (
                      <Textarea
                        className={`min-h-[100px] text-xs font-mono resize-y ${
                          hasError
                            ? "border-red-400 focus-visible:ring-red-400"
                            : showRequiredHint
                              ? "border-amber-400 focus-visible:ring-amber-400"
                              : ""
                        }`}
                        placeholder={argDef.hint || t("rulesSingleTable.argHintQuery")}
                        value={val}
                        onChange={(e) =>
                          onUpdate(check.id, { args: { ...check.args, [argDef.name]: e.target.value } })
                        }
                        disabled={disabled}
                      />
                    ) : (
                      <Input
                        className={`h-7 text-xs ${
                          hasError
                            ? "border-red-400 focus-visible:ring-red-400"
                            : showRequiredHint
                              ? "border-amber-400 focus-visible:ring-amber-400"
                              : ""
                        }`}
                        placeholder={argDef.hint || argHint(argDef.name, check.fn, t)}
                        value={val}
                        onChange={(e) =>
                          onUpdate(check.id, { args: { ...check.args, [argDef.name]: e.target.value } })
                        }
                        disabled={disabled}
                      />
                    )}
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
              const validationErr = validateArg(key, val, check.fn, t);
              const hasError = !!validationErr;
              return (
                <div key={key} className="space-y-1">
                  <Label className={`text-[11px] ${hasError ? "text-red-500" : isEmpty ? "text-amber-500" : "text-muted-foreground"}`}>
                    {argLabel(key, t)}{isEmpty && ` ${t("rulesSingleTable.required")}`}
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
              {t("rulesSingleTable.targetTables")}
              {check.targetTables.length > 0 && (
                <span className="text-muted-foreground ml-1">
                  {t("rulesSingleTable.selectedSuffix", { count: check.targetTables.length })}
                </span>
              )}
              {check.fn !== "" && check.targetTables.length === 0 && (
                <span className="text-amber-500 ml-1">{t("rulesSingleTable.noneSelected")}</span>
              )}
            </span>
            {check.fn !== "" && columns.length > 0 && (
              <span className="text-[10px] text-muted-foreground">
                {t("rulesSingleTable.filteredByColumn")} <code className="bg-muted px-1 py-0.5 rounded">{columns.join(", ")}</code>
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
                {t("rulesSingleTable.checkingColumnCompatibility")}
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
  const { t } = useTranslation();
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
  const displayValue = value === "" ? t("rulesSingleTable.selectFunction") : isUnknownFn ? t("rulesSingleTable.customSuffix", { name: value }) : value;

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
              placeholder={t("rulesSingleTable.searchFunctions")}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="h-8 text-xs pl-7"
            />
          </div>
        </div>
        <div className="max-h-72 overflow-y-auto py-1">
          {functions.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesSingleTable.loadingFunctions")}</div>
          ) : grouped.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesSingleTable.noMatches")}</div>
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

