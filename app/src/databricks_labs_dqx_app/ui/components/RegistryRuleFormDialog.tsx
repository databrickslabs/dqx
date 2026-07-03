import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { AlertCircle, Check, ChevronDown, Loader2, Search, Sparkles, Wand2 } from "lucide-react";
import { LabelsEditor } from "@/components/Labels";
import { HelpTooltip } from "@/components/HelpTooltip";
import type { LabelDefinition } from "@/lib/api-custom";
import {
  useCreateRegistryRule,
  useUpdateRegistryRule,
  useSubmitRegistryRule,
  useListCheckFunctions,
  useAiGenerateRule,
  useAiSuggestField,
  type RegistryRuleOut,
  type RuleDefinition,
  type RuleSlot,
  type RuleParameter,
  type RuleParameterType,
  type CheckFunctionDef as ApiCheckFunctionDef,
  type CreateRegistryRuleIn,
  type UpdateRegistryRuleIn,
  type CreateRegistryRuleInAuthorKind,
  type AiGenerateRuleOut,
} from "@/lib/api";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";

const RESERVED_NAME_KEY = "name";
const RESERVED_DESCRIPTION_KEY = "description";
const RESERVED_DIMENSION_KEY = "dimension";
const RESERVED_SEVERITY_KEY = "severity";

type RegistryMode = "dqx_native" | "lowcode" | "sql";
type Polarity = "pass" | "fail";
// The tab strip shows one extra pseudo-mode — "ai" — which isn't a
// persisted RegistryMode. It's the guided Build-with-AI experience; once a
// proposal is applied, the active tab switches to the real underlying mode.
export type AuthoringTab = RegistryMode | "ai";

const COLUMN_KINDS = new Set(["column", "columns"]);
const PARAM_KIND_TO_TYPE: Record<string, RuleParameterType> = {
  boolean: "boolean",
  number: "number",
  list: "list",
  string: "string",
  ref_table: "ref_table",
  ref_columns: "ref_column",
};

function deriveSlotsAndParameters(fn: ApiCheckFunctionDef | undefined): {
  slots: RuleSlot[];
  parameters: RuleParameter[];
} {
  if (!fn) return { slots: [], parameters: [] };
  const slots: RuleSlot[] = [];
  const parameters: RuleParameter[] = [];
  let position = 0;
  for (const p of fn.params ?? []) {
    if (COLUMN_KINDS.has(p.kind)) {
      slots.push({
        name: p.name,
        family: "any",
        position: position++,
        cardinality: p.kind === "columns" ? "many" : "one",
      });
    } else {
      parameters.push({
        name: p.name,
        type: PARAM_KIND_TO_TYPE[p.kind] ?? "string",
        value: null,
      });
    }
  }
  return { slots, parameters };
}

function nativeArguments(slots: RuleSlot[]): Record<string, unknown> {
  const args: Record<string, unknown> = {};
  for (const s of slots) args[s.name] = `{{${s.name}}}`;
  return args;
}

function parseParamValue(type: RuleParameterType, raw: string): RuleParameter["value"] {
  const trimmed = raw.trim();
  if (trimmed === "") return null;
  switch (type) {
    case "boolean":
      return trimmed === "true";
    case "number": {
      const n = Number(trimmed);
      return Number.isNaN(n) ? null : n;
    }
    case "list":
      return trimmed
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
    default:
      return trimmed;
  }
}

function paramValueToRaw(value: RuleParameter["value"]): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(", ");
  return String(value);
}

const SQL_DDL_DML_PATTERN = /\b(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|MERGE)\b/i;

function validateSqlPredicate(predicate: string, t: (key: string) => string): string | null {
  if (!predicate.trim()) return null;
  if (predicate.includes(";")) return t("rulesCreateSql.querySemicolonError");
  if (SQL_DDL_DML_PATTERN.test(predicate)) return t("rulesCreateSql.queryProhibitedError");
  return null;
}

function apiFunctionsGrouped(functions: ApiCheckFunctionDef[], query: string) {
  const q = query.trim().toLowerCase();
  const matched = functions.filter((f) =>
    q === "" ? true : f.name.toLowerCase().includes(q) || (f.doc ?? "").toLowerCase().includes(q),
  );
  const byCategory = new Map<string, ApiCheckFunctionDef[]>();
  for (const fn of matched) {
    const cat = fn.category ?? "Other";
    if (!byCategory.has(cat)) byCategory.set(cat, []);
    byCategory.get(cat)!.push(fn);
  }
  return Array.from(byCategory.entries()).sort(([a], [b]) => {
    if (a === "Other") return 1;
    if (b === "Other") return -1;
    return a.localeCompare(b);
  });
}

function FunctionCombobox({
  value,
  functions,
  onChange,
  disabled,
}: {
  value: string;
  functions: ApiCheckFunctionDef[];
  onChange: (fn: string) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const grouped = useMemo(() => apiFunctionsGrouped(functions, query), [functions, query]);

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
          <span className={value === "" ? "text-muted-foreground" : "font-mono"}>
            {value === "" ? t("rulesRegistry.selectFunction") : value}
          </span>
          <ChevronDown className="h-3 w-3 opacity-50 shrink-0" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="p-0 w-[--radix-popover-trigger-width] min-w-[280px]" align="start">
        <div className="border-b p-2">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground" />
            <Input
              autoFocus
              placeholder={t("rulesRegistry.searchFunctions")}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="h-8 text-xs pl-7"
            />
          </div>
        </div>
        <div className="max-h-72 overflow-y-auto py-1">
          {functions.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesRegistry.loadingFunctions")}</div>
          ) : grouped.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesRegistry.noMatches")}</div>
          ) : (
            grouped.map(([category, fns]) => (
              <div key={category}>
                <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground bg-muted/40">
                  {category}
                </div>
                {fns.map((fn) => {
                  const selected = fn.name === value;
                  return (
                    <button
                      key={fn.name}
                      type="button"
                      onClick={() => {
                        onChange(fn.name);
                        setOpen(false);
                      }}
                      className={`w-full text-left px-2 py-1.5 text-xs hover:bg-accent flex items-start gap-2 ${selected ? "bg-accent" : ""}`}
                    >
                      <Check className={`h-3 w-3 shrink-0 mt-0.5 ${selected ? "opacity-100" : "opacity-0"}`} />
                      <span className="min-w-0 flex-1">
                        <span className="font-mono">{fn.name}</span>
                        {fn.doc && (
                          <span className="block text-[10px] text-muted-foreground truncate">{fn.doc}</span>
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

function SuggestButton({
  field,
  busy,
  onClick,
  label,
}: {
  field: string;
  busy: boolean;
  onClick: () => void;
  label: string;
}) {
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      className="h-5 gap-1 px-1.5 text-[10px] text-muted-foreground hover:text-foreground"
      onClick={onClick}
      disabled={busy}
      aria-label={label}
      title={label}
      data-field={field}
    >
      {busy ? <Loader2 className="h-2.5 w-2.5 animate-spin" /> : <Sparkles className="h-2.5 w-2.5" />}
      {label}
    </Button>
  );
}

interface RegistryRuleFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Set when editing an existing draft rule (definition + tags fully editable). */
  editingRule: RegistryRuleOut | null;
  /** Set when opening a rule read-only (any status, including the one being edited). */
  viewingRule: RegistryRuleOut | null;
  labelDefinitions: LabelDefinition[];
  onSaved: () => void;
  /**
   * "dialog" (default) renders the classic modal used for creating a new
   * rule. "page" renders the same fields inline (no Dialog/overlay chrome)
   * for embedding on a routed detail page — see registry-rules.$ruleId.tsx.
   */
  variant?: "dialog" | "page";
  /** Controlled authoring tab (e.g. synced to a `?tab=` URL param). Falls back to internal state when omitted. */
  activeTab?: AuthoringTab;
  onActiveTabChange?: (tab: AuthoringTab) => void;
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export function RegistryRuleFormDialog({
  open,
  onOpenChange,
  editingRule,
  viewingRule,
  labelDefinitions,
  onSaved,
  variant = "dialog",
  activeTab: controlledActiveTab,
  onActiveTabChange,
}: RegistryRuleFormDialogProps) {
  const { t } = useTranslation();
  const sourceRule = editingRule ?? viewingRule;
  // Read-only only applies when explicitly viewing a rule (viewingRule set).
  // Creating a new rule also has editingRule === null, so gating on that
  // alone would incorrectly lock the create form.
  const readOnly = viewingRule !== null;
  const isEditing = editingRule !== null;

  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);

  const dimensionValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_DIMENSION_KEY)?.values ?? [],
    [labelDefinitions],
  );
  const severityValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? [],
    [labelDefinitions],
  );
  const tagDefinitions = useMemo(
    () => labelDefinitions.filter((d) => d.key !== RESERVED_DIMENSION_KEY && d.key !== RESERVED_SEVERITY_KEY),
    [labelDefinitions],
  );

  const [mode, setMode] = useState<RegistryMode>("dqx_native");
  const [internalActiveTab, setInternalActiveTab] = useState<AuthoringTab>("dqx_native");
  const activeTab = controlledActiveTab ?? internalActiveTab;
  const setActiveTab = useCallback(
    (tab: AuthoringTab) => {
      setInternalActiveTab(tab);
      onActiveTabChange?.(tab);
    },
    [onActiveTabChange],
  );
  const [functionName, setFunctionName] = useState("");
  const [paramRawValues, setParamRawValues] = useState<Record<string, string>>({});
  const [sqlPredicate, setSqlPredicate] = useState("");
  const [polarity, setPolarity] = useState<Polarity>("pass");
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [dimension, setDimension] = useState<string>("");
  const [severity, setSeverity] = useState<string>("");
  const [tags, setTags] = useState<Record<string, string>>({});
  const [steward, setSteward] = useState("");
  const [nameError, setNameError] = useState<string | null>(null);
  const [authorKind, setAuthorKind] = useState<CreateRegistryRuleInAuthorKind | undefined>(undefined);
  const [pendingNativeArgs, setPendingNativeArgs] = useState<Record<string, unknown> | null>(null);

  // AI — Build-with-AI (full-form generate) + per-field suggest.
  const aiAvailability = useAiAvailability();
  const [aiDescription, setAiDescription] = useState("");
  const [aiBusy, setAiBusy] = useState(false);
  const [aiProposal, setAiProposal] = useState<AiGenerateRuleOut | null>(null);
  const generateRuleMutation = useAiGenerateRule();
  const suggestFieldMutation = useAiSuggestField();
  const [suggestingField, setSuggestingField] = useState<string | null>(null);

  // (Re)hydrate the local draft whenever the dialog opens for a
  // different rule (or opens fresh for creation). Keyed on open + rule_id via
  // a ref so we don't re-run on every parent re-render that hands us a new
  // `sourceRule` object identity for the same underlying rule.
  const rehydrateKeyRef = useRef<string | null>(null);
  useEffect(() => {
    if (!open) return;
    const key = `${open}:${sourceRule?.rule_id ?? "new"}`;
    if (rehydrateKeyRef.current === key) return;
    rehydrateKeyRef.current = key;

    const md = (sourceRule?.user_metadata ?? {}) as Record<string, unknown>;
    const asString = (k: string) => (typeof md[k] === "string" ? (md[k] as string) : "");
    setName(asString(RESERVED_NAME_KEY));
    setDescription(asString(RESERVED_DESCRIPTION_KEY));
    setDimension(asString(RESERVED_DIMENSION_KEY));
    setSeverity(asString(RESERVED_SEVERITY_KEY));
    setSteward(sourceRule?.steward ?? "");
    setNameError(null);
    setAuthorKind(sourceRule?.author_kind ?? undefined);
    setAiDescription("");
    setAiProposal(null);
    setPendingNativeArgs(null);
    const freeTags: Record<string, string> = {};
    for (const [k, v] of Object.entries(md)) {
      if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
      if (typeof v === "string") freeTags[k] = v;
    }
    setTags(freeTags);

    if (sourceRule) {
      setMode(sourceRule.mode);
      setActiveTab(sourceRule.mode);
      setPolarity(sourceRule.polarity ?? "pass");
      if (sourceRule.mode === "dqx_native") {
        const fn = String((sourceRule.definition?.body ?? {}).function ?? "");
        setFunctionName(fn);
        const raw: Record<string, string> = {};
        for (const p of sourceRule.definition?.parameters ?? []) {
          raw[p.name] = paramValueToRaw(p.value);
        }
        setParamRawValues(raw);
        setSqlPredicate("");
      } else {
        const predicate = (sourceRule.definition?.body ?? {}).predicate;
        setSqlPredicate(typeof predicate === "string" ? predicate : "");
        setFunctionName("");
        setParamRawValues({});
      }
    } else {
      // Creating a brand-new rule: lead with the guided Build-with-AI tab
      // when AI is available, since Low-Code is disabled and DQX Native's
      // function picker is the least approachable starting point for a
      // non-technical steward. Falls back to DQX Native, as before, when
      // AI isn't available.
      setMode("dqx_native");
      setActiveTab(aiAvailability.available ? "ai" : "dqx_native");
      setFunctionName("");
      setParamRawValues({});
      setSqlPredicate("");
      setPolarity("pass");
    }
  }, [open, sourceRule, aiAvailability.available]);

  const selectedFn = useMemo(
    () => checkFunctions.find((f) => f.name === functionName),
    [checkFunctions, functionName],
  );
  const { slots, parameters: derivedParams } = useMemo(
    () => deriveSlotsAndParameters(selectedFn),
    [selectedFn],
  );

  // After applying an AI proposal for a dqx_native function, we need the
  // function's real parameter list (derived above, once `checkFunctions`
  // resolves) before we know which of the AI's raw `arguments` are
  // non-slot parameters vs. column slots. Stash them here and flush once
  // the function catalog has resolved.
  useEffect(() => {
    if (pendingNativeArgs === null) return;
    if (checkFunctions.length === 0) return;
    if (!selectedFn) {
      setPendingNativeArgs(null);
      return;
    }
    const raw: Record<string, string> = {};
    for (const p of derivedParams) {
      const v = pendingNativeArgs[p.name];
      if (v !== undefined && v !== null) {
        raw[p.name] = Array.isArray(v) ? v.join(", ") : String(v);
      }
    }
    setParamRawValues(raw);
    setPendingNativeArgs(null);
  }, [pendingNativeArgs, checkFunctions, selectedFn, derivedParams]);

  const createMutation = useCreateRegistryRule();
  const updateMutation = useUpdateRegistryRule();
  const submitMutation = useSubmitRegistryRule();
  const [saving, setSaving] = useState(false);

  const sqlError = mode === "sql" ? validateSqlPredicate(sqlPredicate, t) : null;

  const buildDefinition = (): RuleDefinition => {
    if (mode === "sql") {
      return { body: { predicate: sqlPredicate.trim() }, slots: [], parameters: [] };
    }
    const parameters: RuleParameter[] = derivedParams.map((p) => ({
      ...p,
      value: parseParamValue(p.type, paramRawValues[p.name] ?? ""),
    }));
    return {
      body: { function: functionName, arguments: nativeArguments(slots) },
      slots,
      parameters,
    };
  };

  const buildUserMetadata = (): Record<string, unknown> => {
    const md: Record<string, unknown> = { ...tags };
    if (name.trim()) md[RESERVED_NAME_KEY] = name.trim();
    if (description.trim()) md[RESERVED_DESCRIPTION_KEY] = description.trim();
    if (dimension) md[RESERVED_DIMENSION_KEY] = dimension;
    if (severity) md[RESERVED_SEVERITY_KEY] = severity;
    return md;
  };

  const matchAllowedValue = (candidate: string, allowed: string[]): string | null =>
    allowed.find((v) => v.toLowerCase() === candidate.trim().toLowerCase()) ?? null;

  const buildSuggestContext = (): string => {
    const parts: string[] = [];
    if (name.trim()) parts.push(`Name: ${name.trim()}`);
    if (description.trim()) parts.push(`Description: ${description.trim()}`);
    if (mode === "dqx_native" && functionName) parts.push(`Check function: ${functionName}`);
    if (mode === "sql" && sqlPredicate.trim()) parts.push(`SQL predicate: ${sqlPredicate.trim()}`);
    if (dimension) parts.push(`Dimension: ${dimension}`);
    if (severity) parts.push(`Severity: ${severity}`);
    return parts.join("\n");
  };

  const applyAiProposal = (proposal: AiGenerateRuleOut) => {
    const appliedMode: RegistryMode = proposal.mode === "sql" ? "sql" : "dqx_native";
    setMode(appliedMode);
    // Switch off the "ai" tab onto the real authoring tab so the steward
    // immediately sees (and can tweak) what the proposal filled in.
    setActiveTab(appliedMode);
    setName(proposal.name?.trim() ?? "");
    setDescription(proposal.description?.trim() ?? "");
    if (proposal.dimension) {
      const match = matchAllowedValue(proposal.dimension, dimensionValues);
      if (match) setDimension(match);
    }
    if (proposal.severity) {
      const match = matchAllowedValue(proposal.severity, severityValues);
      if (match) setSeverity(match);
    }
    const validAuthorKinds: CreateRegistryRuleInAuthorKind[] = ["human", "ai_generated", "ai_assisted"];
    setAuthorKind(
      validAuthorKinds.includes(proposal.author_kind as CreateRegistryRuleInAuthorKind)
        ? (proposal.author_kind as CreateRegistryRuleInAuthorKind)
        : "ai_generated",
    );

    const body = (proposal.definition ?? {}) as Record<string, unknown>;
    if (proposal.mode === "sql") {
      setSqlPredicate(typeof body.sql_query === "string" ? body.sql_query : "");
      setPolarity(proposal.polarity === "fail" ? "fail" : "pass");
      setFunctionName("");
      setParamRawValues({});
      setPendingNativeArgs(null);
    } else {
      const fn = typeof body.function === "string" ? body.function : "";
      setFunctionName(fn);
      setSqlPredicate("");
      const args =
        body.arguments && typeof body.arguments === "object"
          ? (body.arguments as Record<string, unknown>)
          : {};
      setPendingNativeArgs(args);
    }
    setAiProposal(null);
    setAiDescription("");
    toast.success(t("rulesRegistry.aiProposalApplied"));
  };

  const handleAiGenerate = async () => {
    if (!aiDescription.trim()) return;
    setAiBusy(true);
    try {
      const resp = await generateRuleMutation.mutateAsync({ data: { description: aiDescription.trim() } });
      setAiProposal(resp.data);
    } catch (err) {
      const reason = aiUnavailableReason(err);
      if (reason) {
        aiAvailability.reportUnavailable(reason);
      } else {
        const axErr = err as { response?: { status?: number } };
        toast.error(
          axErr?.response?.status === 429
            ? t("rulesRegistry.aiRateLimited")
            : extractApiError(err, t("rulesRegistry.aiGenerateFailed")),
          { duration: 6000 },
        );
      }
    } finally {
      setAiBusy(false);
    }
  };

  const handleAiSuggestField = async (field: "name" | "description" | "dimension" | "severity") => {
    setSuggestingField(field);
    try {
      const resp = await suggestFieldMutation.mutateAsync({
        data: { field, context: buildSuggestContext() },
      });
      const value = resp.data.value?.trim() ?? "";
      if (!value) {
        toast.error(t("rulesRegistry.aiSuggestFailed"));
        return;
      }
      if (field === "name") {
        setName(value);
      } else if (field === "description") {
        setDescription(value);
      } else if (field === "dimension") {
        const match = matchAllowedValue(value, dimensionValues);
        if (!match) {
          toast.error(t("rulesRegistry.aiSuggestInvalidValue"));
          return;
        }
        setDimension(match);
      } else if (field === "severity") {
        const match = matchAllowedValue(value, severityValues);
        if (!match) {
          toast.error(t("rulesRegistry.aiSuggestInvalidValue"));
          return;
        }
        setSeverity(match);
      }
      setAuthorKind((prev) => prev ?? "ai_assisted");
      toast.success(t("rulesRegistry.aiSuggestApplied"));
    } catch (err) {
      const reason = aiUnavailableReason(err);
      if (reason) {
        aiAvailability.reportUnavailable(reason);
      } else {
        toast.error(extractApiError(err, t("rulesRegistry.aiSuggestFailed")), { duration: 6000 });
      }
    } finally {
      setSuggestingField(null);
    }
  };

  const validate = (): boolean => {
    if (!name.trim()) {
      setNameError(t("rulesRegistry.nameRequired"));
      return false;
    }
    setNameError(null);
    if (mode === "dqx_native" && !functionName) {
      toast.error(t("rulesRegistry.functionRequired"));
      return false;
    }
    if (mode === "sql") {
      if (!sqlPredicate.trim()) {
        toast.error(t("rulesRegistry.predicateRequired"));
        return false;
      }
      if (sqlError) {
        toast.error(sqlError);
        return false;
      }
    }
    return true;
  };

  const closeAndReset = () => {
    onOpenChange(false);
  };

  const handleSave = async (thenSubmit: boolean) => {
    if (readOnly) return;
    if (!validate()) return;
    setSaving(true);
    try {
      const definition = buildDefinition();
      const userMetadata = buildUserMetadata();
      let ruleId: string;
      if (isEditing && editingRule) {
        const payload: UpdateRegistryRuleIn = {
          mode,
          definition,
          polarity: mode === "sql" ? polarity : null,
          user_metadata: userMetadata,
          steward: steward.trim() || null,
          // Persist AI provenance stamped during this edit-in-place session
          // (e.g. accepting an AI-suggested field on an otherwise
          // human-authored draft) rather than silently dropping it.
          author_kind: authorKind,
        };
        const resp = await updateMutation.mutateAsync({ ruleId: editingRule.rule_id, data: payload });
        ruleId = resp.data.rule_id;
        toast.success(t("rulesRegistry.toastUpdated"));
      } else {
        const payload: CreateRegistryRuleIn = {
          mode,
          definition,
          polarity: mode === "sql" ? polarity : null,
          user_metadata: userMetadata,
          steward: steward.trim() || null,
          author_kind: authorKind,
        };
        const resp = await createMutation.mutateAsync({ data: payload });
        ruleId = resp.data.rule.rule_id;
        toast.success(t("rulesRegistry.toastCreated"));
        if (resp.data.dedup_warning) {
          toast.warning(resp.data.dedup_warning, { duration: 8000 });
        }
      }
      if (thenSubmit) {
        await submitMutation.mutateAsync({ ruleId });
        toast.success(t("rulesRegistry.toastSubmitted"));
      }
      onSaved();
      closeAndReset();
    } catch (err) {
      toast.error(extractApiError(err, t("rulesRegistry.saveFailed")), { duration: 6000 });
    } finally {
      setSaving(false);
    }
  };

  const dialogTitle = readOnly
    ? t("rulesRegistry.viewTitle")
    : isEditing
      ? t("rulesRegistry.editTitle")
      : t("rulesRegistry.createTitle");

  const showAiTab = !readOnly && aiAvailability.available;

  const authorKindBadges = (
    <>
      {authorKind === "ai_generated" && (
        <Badge variant="secondary" className="gap-1 text-[10px] font-normal">
          <Sparkles className="h-2.5 w-2.5" />
          {t("rulesRegistry.authorKindAiGenerated")}
        </Badge>
      )}
      {authorKind === "ai_assisted" && (
        <Badge variant="secondary" className="gap-1 text-[10px] font-normal">
          <Sparkles className="h-2.5 w-2.5" />
          {t("rulesRegistry.authorKindAiAssisted")}
        </Badge>
      )}
    </>
  );

  const formBody = (
    <div className="space-y-4">
          <Tabs
            value={activeTab}
            onValueChange={(v) => {
              if (readOnly) return;
              setActiveTab(v as AuthoringTab);
              if (v !== "ai") setMode(v as RegistryMode);
            }}
          >
                <TabsList className={`grid w-full ${showAiTab ? "grid-cols-4" : "grid-cols-3"}`}>
                  {showAiTab && (
                    <TabsTrigger value="ai" className="gap-1">
                      <Sparkles className="h-3 w-3" />
                      {t("rulesRegistry.aiBuildTitle")}
                    </TabsTrigger>
                  )}
                  <TabsTrigger value="dqx_native" disabled={readOnly && mode !== "dqx_native"}>
                    {t("rulesRegistry.modeDqxNative")}
                  </TabsTrigger>
                  <TabsTrigger value="lowcode" disabled className="gap-1">
                    <Sparkles className="h-3 w-3" />
                    {t("rulesRegistry.modeLowcode")}
                  </TabsTrigger>
                  <TabsTrigger value="sql" disabled={readOnly && mode !== "sql"}>
                    {t("rulesRegistry.modeSql")}
                  </TabsTrigger>
                </TabsList>

                {showAiTab && (
                  <TabsContent value="ai" className="pt-2">
                    <div className="rounded-lg border bg-gradient-to-br from-primary/5 via-background to-secondary/5 p-3 space-y-2.5">
                      <div className="flex items-center gap-2">
                        <div className="p-1.5 bg-primary/10 rounded-md">
                          <Sparkles className="h-3.5 w-3.5 text-primary" />
                        </div>
                        <div className="min-w-0">
                          <p className="text-sm font-semibold">{t("rulesRegistry.aiBuildTitle")}</p>
                          <p className="text-[11px] text-muted-foreground">{t("rulesRegistry.aiBuildDescription")}</p>
                        </div>
                      </div>

                      {aiProposal ? (
                        <div className="rounded-md border bg-card/70 p-3 space-y-2">
                          <p className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                            {t("rulesRegistry.aiProposalTitle")}
                          </p>
                          <div className="space-y-1 text-xs">
                            <p><span className="text-muted-foreground">{t("rulesRegistry.nameLabel")}:</span> {aiProposal.name}</p>
                            {aiProposal.description && (
                              <p><span className="text-muted-foreground">{t("rulesRegistry.descriptionLabel")}:</span> {aiProposal.description}</p>
                            )}
                            <div className="flex flex-wrap gap-1 pt-1">
                              <Badge variant="outline" className="text-[10px]">
                                {t("rulesRegistry.aiProposalModeLabel")}: {aiProposal.mode}
                              </Badge>
                              {aiProposal.dimension && <Badge variant="outline" className="text-[10px]">{aiProposal.dimension}</Badge>}
                              {aiProposal.severity && <Badge variant="outline" className="text-[10px]">{aiProposal.severity}</Badge>}
                            </div>
                            <pre className="mt-1 max-h-32 overflow-auto rounded bg-muted/50 p-2 text-[10px] font-mono whitespace-pre-wrap break-words">
                              {JSON.stringify(aiProposal.definition, null, 2)}
                            </pre>
                          </div>
                          <div className="flex items-center gap-2 pt-1">
                            <Button size="sm" className="h-7 text-xs gap-1.5" onClick={() => applyAiProposal(aiProposal)}>
                              <Wand2 className="h-3 w-3" />
                              {t("rulesRegistry.aiProposalUseButton")}
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-7 text-xs"
                              onClick={() => setAiProposal(null)}
                            >
                              {t("rulesRegistry.aiProposalDiscardButton")}
                            </Button>
                          </div>
                        </div>
                      ) : (
                        <div className="space-y-2">
                          <Textarea
                            value={aiDescription}
                            onChange={(e) => setAiDescription(e.target.value)}
                            placeholder={t("rulesRegistry.aiBuildPlaceholder")}
                            className="min-h-[64px] text-xs bg-card/60"
                            disabled={aiBusy}
                            maxLength={4000}
                          />
                          <Button
                            size="sm"
                            className="h-7 text-xs gap-1.5"
                            onClick={handleAiGenerate}
                            disabled={aiBusy || !aiDescription.trim()}
                          >
                            {aiBusy ? <Loader2 className="h-3 w-3 animate-spin" /> : <Sparkles className="h-3 w-3" />}
                            {aiBusy ? t("rulesRegistry.aiGenerating") : t("rulesRegistry.aiGenerateButton")}
                          </Button>
                        </div>
                      )}
                    </div>
                  </TabsContent>
                )}

                <TabsContent value="lowcode" className="pt-2">
                  <p className="text-xs text-muted-foreground italic">{t("rulesRegistry.lowcodeComingSoon")}</p>
                </TabsContent>

                <TabsContent value="dqx_native" className="pt-2 space-y-3">
              <div className="space-y-1.5">
                <Label className="text-xs">{t("rulesRegistry.functionLabel")}</Label>
                <FunctionCombobox
                  value={functionName}
                  functions={checkFunctions}
                  onChange={(fn) => {
                    setFunctionName(fn);
                    setParamRawValues({});
                  }}
                  disabled={readOnly}
                />
              </div>
              {slots.length > 0 && (
                <div className="space-y-1.5">
                  <div className="flex items-center gap-1.5">
                    <Label className="text-xs">{t("rulesRegistry.slotsLabel")}</Label>
                    <HelpTooltip text={t("rulesRegistry.slotsTooltip")} />
                  </div>
                  <p className="text-[10px] text-muted-foreground">{t("rulesRegistry.slotsHint")}</p>
                  <div className="flex flex-wrap gap-1.5">
                    {slots.map((s) => (
                      <Badge key={s.name} variant="secondary" className="font-mono text-[10px]">
                        {`{{${s.name}}}`}
                        <span className="ml-1 opacity-60" title={t("rulesRegistry.typeFamilyTooltip")}>
                          ({s.family})
                        </span>
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
              {derivedParams.length > 0 && (
                <div className="space-y-2">
                  <Label className="text-xs">{t("rulesRegistry.parametersLabel")}</Label>
                  <div className="grid gap-2 sm:grid-cols-2">
                    {derivedParams.map((p) => (
                      <div key={p.name} className="space-y-1">
                        <Label className="text-[11px] text-muted-foreground font-mono">{p.name}</Label>
                        {p.type === "boolean" ? (
                          <Select
                            value={paramRawValues[p.name] || "false"}
                            onValueChange={(v) => setParamRawValues((prev) => ({ ...prev, [p.name]: v }))}
                            disabled={readOnly}
                          >
                            <SelectTrigger className="h-7 text-xs w-full">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="true" className="text-xs">true</SelectItem>
                              <SelectItem value="false" className="text-xs">false</SelectItem>
                            </SelectContent>
                          </Select>
                        ) : (
                          <Input
                            className="h-7 text-xs"
                            type={p.type === "number" ? "number" : "text"}
                            placeholder={p.type === "list" ? t("rulesRegistry.listPlaceholder") : undefined}
                            value={paramRawValues[p.name] ?? ""}
                            onChange={(e) => setParamRawValues((prev) => ({ ...prev, [p.name]: e.target.value }))}
                            disabled={readOnly}
                          />
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {functionName === "" && (
                <p className="text-[11px] text-muted-foreground italic">{t("rulesRegistry.nativeHint")}</p>
              )}
            </TabsContent>

            <TabsContent value="sql" className="pt-2 space-y-3">
              <div className="space-y-1.5">
                <Label className="text-xs">{t("rulesRegistry.sqlPredicateLabel")}</Label>
                <Textarea
                  className={`font-mono text-xs min-h-[100px] ${sqlError ? "border-red-400 focus-visible:ring-red-400" : ""}`}
                  placeholder={t("rulesRegistry.sqlPredicatePlaceholder")}
                  value={sqlPredicate}
                  onChange={(e) => setSqlPredicate(e.target.value)}
                  disabled={readOnly}
                />
                {sqlError && (
                  <p className="text-[10px] text-red-500 flex items-center gap-1">
                    <AlertCircle className="h-2.5 w-2.5 shrink-0" />
                    {sqlError}
                  </p>
                )}
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  checked={polarity === "fail"}
                  onCheckedChange={(checked) => setPolarity(checked ? "fail" : "pass")}
                  disabled={readOnly}
                  id="registry-rule-polarity"
                />
                <Label htmlFor="registry-rule-polarity" className="text-xs cursor-pointer">
                  {polarity === "fail" ? t("rulesRegistry.polarityFail") : t("rulesRegistry.polarityPass")}
                </Label>
                <span className="text-[10px] text-muted-foreground">{t("rulesRegistry.polarityHint")}</span>
                <HelpTooltip text={t("rulesRegistry.polarityTooltip")} />
              </div>
            </TabsContent>
          </Tabs>

          <div className="border-t pt-3 space-y-3">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1.5">
                <div className="flex items-center justify-between gap-2">
                  <Label className="text-xs">{t("rulesRegistry.nameLabel")}</Label>
                  {!readOnly && aiAvailability.available && (
                    <SuggestButton
                      field="name"
                      busy={suggestingField === "name"}
                      onClick={() => handleAiSuggestField("name")}
                      label={t("rulesRegistry.aiSuggestButton")}
                    />
                  )}
                </div>
                <Input
                  className={`h-8 text-xs ${nameError ? "border-red-400 focus-visible:ring-red-400" : ""}`}
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  disabled={readOnly}
                  placeholder={t("rulesRegistry.namePlaceholder")}
                />
                {nameError && <p className="text-[10px] text-red-500">{nameError}</p>}
              </div>
              <div className="space-y-1.5">
                <Label className="text-xs">{t("rulesRegistry.stewardLabel")}</Label>
                <Input
                  className="h-8 text-xs"
                  value={steward}
                  onChange={(e) => setSteward(e.target.value)}
                  disabled={readOnly}
                  placeholder={t("rulesRegistry.stewardPlaceholder")}
                />
              </div>
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label className="text-xs">{t("rulesRegistry.descriptionLabel")}</Label>
                {!readOnly && aiAvailability.available && (
                  <SuggestButton
                    field="description"
                    busy={suggestingField === "description"}
                    onClick={() => handleAiSuggestField("description")}
                    label={t("rulesRegistry.aiSuggestButton")}
                  />
                )}
              </div>
              <Textarea
                className="text-xs min-h-[60px]"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                disabled={readOnly}
                placeholder={t("rulesRegistry.descriptionPlaceholder")}
              />
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1.5">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-1.5">
                    <Label className="text-xs">{t("rulesRegistry.dimensionLabel")}</Label>
                    <HelpTooltip text={t("rulesRegistry.dimensionTooltip")} />
                  </div>
                  {!readOnly && aiAvailability.available && dimensionValues.length > 0 && (
                    <SuggestButton
                      field="dimension"
                      busy={suggestingField === "dimension"}
                      onClick={() => handleAiSuggestField("dimension")}
                      label={t("rulesRegistry.aiSuggestButton")}
                    />
                  )}
                </div>
                <Select value={dimension || undefined} onValueChange={setDimension} disabled={readOnly}>
                  <SelectTrigger className="h-8 text-xs w-full">
                    <SelectValue placeholder={t("rulesRegistry.selectDimension")} />
                  </SelectTrigger>
                  <SelectContent>
                    {dimensionValues.map((v) => (
                      <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1.5">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-1.5">
                    <Label className="text-xs">{t("rulesRegistry.severityLabel")}</Label>
                    <HelpTooltip text={t("rulesRegistry.severityTooltip")} />
                  </div>
                  {!readOnly && aiAvailability.available && severityValues.length > 0 && (
                    <SuggestButton
                      field="severity"
                      busy={suggestingField === "severity"}
                      onClick={() => handleAiSuggestField("severity")}
                      label={t("rulesRegistry.aiSuggestButton")}
                    />
                  )}
                </div>
                <Select value={severity || undefined} onValueChange={setSeverity} disabled={readOnly}>
                  <SelectTrigger className="h-8 text-xs w-full">
                    <SelectValue placeholder={t("rulesRegistry.selectSeverity")} />
                  </SelectTrigger>
                  <SelectContent>
                    {severityValues.map((v) => (
                      <SelectItem key={v} value={v} className="text-xs">{v}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>

            <LabelsEditor
              value={tags}
              onChange={setTags}
              disabled={readOnly}
              title={t("rulesRegistry.tagsLabel")}
              defaultOpen={Object.keys(tags).length > 0}
              definitions={tagDefinitions}
            />
          </div>
        </div>
  );

  const footerButtons = (
    <>
      <Button variant="outline" onClick={closeAndReset} disabled={saving}>
        {readOnly ? t("common.close") : t("common.cancel")}
      </Button>
      {!readOnly && (
        <>
          <Button variant="secondary" onClick={() => handleSave(false)} disabled={saving} className="gap-2">
            {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {t("rulesRegistry.saveDraft")}
          </Button>
          <Button onClick={() => handleSave(true)} disabled={saving} className="gap-2">
            {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {t("rulesRegistry.saveAndSubmit")}
          </Button>
        </>
      )}
    </>
  );

  if (variant === "page") {
    // The routed detail page already renders its own name/status/mode/
    // author-kind header above this component, so skip the dialog-style
    // title here (which would duplicate it) and keep just the helper text.
    return (
      <div className="space-y-6">
        <p className="text-sm text-muted-foreground">{t("rulesRegistry.dialogDescription")}</p>
        {formBody}
        <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end sm:gap-2 pt-4 border-t">
          {footerButtons}
        </div>
      </div>
    );
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !saving && onOpenChange(next)}>
      <DialogContent className="max-w-2xl w-[95vw] max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {dialogTitle}
            {authorKindBadges}
          </DialogTitle>
          <DialogDescription>{t("rulesRegistry.dialogDescription")}</DialogDescription>
        </DialogHeader>
        {formBody}
        <DialogFooter>{footerButtons}</DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
