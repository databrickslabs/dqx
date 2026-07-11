import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
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
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Checkbox } from "@/components/ui/checkbox";
import { Separator } from "@/components/ui/separator";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
  AlertCircle,
  Braces,
  Check,
  ChevronDown,
  LineChart,
  FlaskConical,
  History as HistoryIcon,
  Info,
  Loader2,
  Shield,
  Sparkles,
  Wrench,
  X,
} from "lucide-react";
import { useApprovalsMode } from "@/hooks/use-approvals-mode";
import { LabelsEditor } from "@/components/Labels";
import { HelpTooltip } from "@/components/HelpTooltip";
import { PermissionsTab } from "@/components/permissions/PermissionsTab";
import { RuleResultsTab } from "@/components/registry-rules/RuleResultsTab";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ruleResultsState } from "@/lib/results-display";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { PredicatePolaritySwitch } from "@/components/rules/PredicatePolaritySwitch";
import { PredicateEditorExplainer } from "@/components/rules/PredicateEditorExplainer";
import { PredicateEditor } from "@/components/rules/PredicateEditor";
import { AdvancedDisclosure } from "@/components/rules/AdvancedDisclosure";
import { LowcodeBuilder } from "@/components/rules/lowcode/LowcodeBuilder";
import { JoinsBuilder } from "@/components/rules/lowcode/JoinsBuilder";
import { GroupByField } from "@/components/rules/lowcode/GroupByField";
import { ModeSwitchDialog, type ModeSwitchDirection } from "@/components/rules/lowcode/ModeSwitchDialog";
import { RuleTestPanel } from "@/components/rules/test/RuleTestPanel";
import { useJoinedColumns } from "@/hooks/useJoinedColumns";
import {
  compileAstToSql,
  compileJoinsToSql,
  compileLowcodeBody,
  lowcodeHasAdvancedShape,
  slotFamilyToLowcode,
  type LowcodeColumnRef,
} from "@/lib/lowcodeCompile";
import { EMPTY_LOWCODE_AST, isV2Ast, type AnyRow, type LowcodeAstV2 } from "@/lib/lowcodeAst";
import { cn } from "@/lib/utils";
import { stripSqlLineComments } from "@/lib/sqlComments";
import type { LabelDefinition } from "@/lib/api-custom";
import {
  useCreateRegistryRule,
  useUpdateRegistryRule,
  useSubmitRegistryRule,
  useListRegistryRuleVersions,
  useGetRuleScore,
  useListCheckFunctions,
  useGetTableColumns,
  useAiGenerateRule,
  useAiSuggestField,
  type RegistryRuleOut,
  type RuleDefinition,
  type RuleSlot,
  type RuleSlotFamily as RuleSlotFamilyType,
  type RuleParameter,
  type CheckFunctionDef as ApiCheckFunctionDef,
  type CreateRegistryRuleIn,
  type UpdateRegistryRuleIn,
  type CreateRegistryRuleInAuthorKind,
  type AiGenerateRuleOut,
} from "@/lib/api";
import { useAiAvailability, aiUnavailableReason } from "@/hooks/use-ai-availability";
import { AI_BUTTON_BG, AI_BANNER_BG, AI_BANNER_BORDER, AI_GRADIENT_URL } from "@/lib/ai-style";
import {
  orderSeverityValuesForDisplay,
  colorFor,
  ColorDot,
  type LabelColorDefinition,
} from "@/components/RegistryRuleBadges";
import {
  buildDqxCheckJson,
  COLUMN_KINDS,
  deriveSlotsAndParameters,
  fnSupportsNegate,
  listColumnArgKey,
  nativeArguments,
  parseParamValue,
  paramValueToRaw,
  severityValueCriticality,
  type ParsedCheckDefinition,
} from "@/lib/registry-rule-conversion";
import { RegistryRuleFormJsonDialog } from "@/components/registry-rules/RegistryRuleFormJsonDialog";
import { SqlAiAssistMenu } from "@/components/rules/SqlAiAssistMenu";

const RESERVED_NAME_KEY = "name";
const RESERVED_DESCRIPTION_KEY = "description";
const RESERVED_DIMENSION_KEY = "dimension";
const RESERVED_SEVERITY_KEY = "severity";

type RegistryMode = "dqx_native" | "lowcode" | "sql";
type Polarity = "pass" | "fail";

// Top-level page tabs. Persisted to the URL (`?tab=`) by the routed detail
// page so browser back/forward moves between them, mirroring the old dqx
// editor's page-based structure.
export type PageTab = "about" | "permissions" | "implementation" | "test" | "history" | "results";

// Mirrors the backend's `forbidden_statements` list verbatim — see
// `is_sql_query_safe()` in `src/databricks/labs/dqx/utils.py`, the source of
// truth. This is a client-side UX guard only (fast, no round-trip); the
// backend re-validates on save and is the actual security boundary. Keep
// this list in lock-step with the backend one — a mismatch lets the user
// save a draft here that then 422s server-side.
const SQL_DDL_DML_PATTERN =
  /\b(DELETE|INSERT|UPDATE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|GRANT|REVOKE|MERGE|USE|REFRESH|ANALYZE|OPTIMIZE|ZORDER)\b/i;

function validateSqlPredicate(predicate: string, t: (key: string) => string): string | null {
  if (!predicate.trim()) return null;
  // Scan with comments removed (item 6): a leading `-- explanation` block (or
  // any hand-typed comment) is inert at runtime, so its prose must not trip the
  // keyword/semicolon guards. The stripper is quote-aware, so a `--`/`;` inside
  // a string literal still counts as live SQL. Mirrors the app-side gates.
  const scanned = stripSqlLineComments(predicate);
  if (scanned.includes(";")) return t("rulesCreateSql.querySemicolonError");
  if (SQL_DDL_DML_PATTERN.test(scanned)) return t("rulesCreateSql.queryProhibitedError");
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
        <Command shouldFilter={false}>
          <CommandInput
            placeholder={t("rulesRegistry.searchFunctions")}
            value={query}
            onValueChange={setQuery}
            className="h-8 text-xs"
          />
          <CommandList className="max-h-72">
            {functions.length === 0 ? (
              <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesRegistry.loadingFunctions")}</div>
            ) : (
              <>
                <CommandEmpty>
                  <span className="text-xs text-muted-foreground">{t("rulesRegistry.noMatches")}</span>
                </CommandEmpty>
                {grouped.map(([category, fns]) => (
                  <CommandGroup key={category} heading={category}>
                    {fns.map((fn) => {
                      const selected = fn.name === value;
                      return (
                        <CommandItem
                          key={fn.name}
                          value={fn.name}
                          onSelect={() => {
                            onChange(fn.name);
                            setOpen(false);
                          }}
                          className="items-start gap-2 text-xs"
                        >
                          <Check className={`h-3 w-3 shrink-0 mt-0.5 ${selected ? "opacity-100" : "opacity-0"}`} />
                          <span className="min-w-0 flex-1">
                            <span className="font-mono">{fn.name}</span>
                            {fn.doc && (
                              <span className="block text-[10px] text-muted-foreground truncate">{fn.doc}</span>
                            )}
                          </span>
                        </CommandItem>
                      );
                    })}
                  </CommandGroup>
                ))}
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

/**
 * In-field "Suggest with AI" affordance, ported from dqlake's
 * `RuleNameDescriptionFields`/`AboutTab` — a gradient Sparkles icon that
 * fades in on hover/focus of the surrounding `group`. Two placements:
 * `"input"` / `"textarea"` absolutely position the icon inside a
 * `relative group` wrapper around an `Input`/`Textarea` (the field needs a
 * matching `pr-9`); `"inline"` renders the icon as a plain flex sibling
 * next to a `Select` trigger (wrap both in a `flex items-center gap-2
 * group` container instead).
 */
function AiSuggestIcon({
  field,
  busy,
  onClick,
  label,
  position,
}: {
  field: string;
  busy: boolean;
  onClick: () => void;
  label: string;
  position: "input" | "textarea" | "inline";
}) {
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label={label}
            onClick={onClick}
            disabled={busy}
            data-field={field}
            className={cn(
              "opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 hover:scale-110 disabled:opacity-50 disabled:cursor-not-allowed transition-all shrink-0",
              position === "input" && "absolute right-2 top-1/2 -translate-y-1/2",
              position === "textarea" && "absolute right-2 top-2",
            )}
          >
            <Sparkles className="h-4 w-4" stroke={AI_GRADIENT_URL} />
          </button>
        </TooltipTrigger>
        <TooltipContent>{label}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/**
 * Common section-header style used above chrome-free form sections (e.g.
 * "Rule Type", "Columns Used") — same typography as the form's `CardTitle`
 * headers, but without the surrounding card, plus an optional action/help
 * slot on the right so both usages share one visual language.
 */
function SectionHeader({
  children,
  tooltip,
  action,
}: {
  children: ReactNode;
  tooltip?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <div className="flex items-center gap-1.5">
        <h2 className="text-sm font-semibold leading-none">{children}</h2>
        {tooltip && <HelpTooltip text={tooltip} />}
      </div>
      {action}
    </div>
  );
}

/**
 * All-caps grey connective word ("IF" / "THEN THE ROW") that frames the
 * Implementation section as a readable sentence — dqlake's
 * `PredicatePolaritySwitch` / `ImplementationTab` visual language (item 12),
 * applied uniformly across native / SQL / low-code modes.
 */
function FramingWord({ children }: { children: ReactNode }) {
  return (
    <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">{children}</span>
  );
}

/** Reference-table picker for a `ref_table` DQX-native argument (foreign_key, has_valid_schema, …). */
function ReferenceTableField({
  value,
  onChange,
  disabled,
}: {
  value: string;
  onChange: (fqn: string) => void;
  disabled?: boolean;
}) {
  return <CatalogBrowser value={value} onChange={onChange} disabled={disabled} />;
}

/** Multi-column picker for a `ref_columns` DQX-native argument, sourced from the sibling `ref_table` argument. */
function ReferenceColumnsField({
  refTableFqn,
  value,
  onChange,
  disabled,
}: {
  refTableFqn: string;
  value: string;
  onChange: (csv: string) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const parts = refTableFqn.split(".");
  const [catalog, schema, table] = parts.length === 3 ? parts : ["", "", ""];
  const { data } = useGetTableColumns(catalog, schema, table, {
    query: { enabled: Boolean(catalog && schema && table) },
  });
  const columns = useMemo(() => data?.data ?? [], [data]);
  const selected = useMemo(
    () => value.split(",").map((s) => s.trim()).filter((s) => s.length > 0),
    [value],
  );

  const toggle = (col: string) => {
    const next = selected.includes(col) ? selected.filter((c) => c !== col) : [...selected, col];
    onChange(next.join(", "));
  };

  if (!refTableFqn) {
    return (
      <p className="text-[10px] text-muted-foreground italic">{t("rulesRegistry.refColumnsNeedsTable")}</p>
    );
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          disabled={disabled}
          className="h-auto min-h-8 w-full justify-between text-xs font-normal flex-wrap"
        >
          <span className="flex flex-wrap gap-1">
            {selected.length === 0 ? (
              <span className="text-muted-foreground">{t("rulesRegistry.selectColumns")}</span>
            ) : (
              selected.map((c) => (
                <Badge key={c} variant="secondary" className="font-mono text-[10px]">
                  {c}
                </Badge>
              ))
            )}
          </span>
          <ChevronDown className="h-3 w-3 opacity-50 shrink-0" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="p-0 w-[--radix-popover-trigger-width] min-w-[220px]" align="start">
        <div className="max-h-56 overflow-y-auto py-1">
          {columns.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">{t("rulesRegistry.loadingColumns")}</div>
          ) : (
            columns.map((col) => (
              <label
                key={col.name}
                className="flex items-center gap-2 px-2 py-1.5 text-xs hover:bg-accent cursor-pointer"
              >
                <Checkbox checked={selected.includes(col.name)} onCheckedChange={() => toggle(col.name)} />
                <span className="font-mono">{col.name}</span>
                <span className="text-[10px] text-muted-foreground ml-auto">{col.type_name}</span>
              </label>
            ))
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

const SLOT_FAMILIES: RuleSlotFamilyType[] = ["any", "numeric", "text", "temporal", "boolean", "array"];
const SLOT_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function nextSlotName(existing: string[]): string {
  const taken = new Set(existing);
  let i = 1;
  while (taken.has(`column_${i}`)) i++;
  return `column_${i}`;
}

/**
 * "Columns used" slot-declaration panel, ported from the dqlake
 * `ColumnsUsedPanel`. Shared by all three authoring modes: SQL / Low-Code
 * authors freely add/remove/rename/retype `{{slot}}` placeholders their
 * predicate references (`allowAddRemove={true}`, the default — every row is
 * addable/removable).
 *
 * DQX Native's slot SET is seeded from the selected function's signature and
 * mostly has fixed arity (one slot per scalar column-kind parameter) — pass
 * `allowAddRemove={false}` for those. The exception is a LIST-typed column
 * parameter (e.g. `foreign_key`'s `columns`): pass its `arg_key` as
 * `expandableArgKey` so slots sharing that `arg_key` form an add/removable
 * group (down to a minimum of one, since the function still needs at least
 * one column), while every other native slot stays fixed. `expandableArgKey`
 * is ignored when `allowAddRemove` is already `true`.
 */
function SlotsPanel({
  value,
  onChange,
  disabled,
  allowAddRemove = true,
  expandableArgKey,
  lockFamily = false,
}: {
  value: RuleSlot[];
  onChange: (next: RuleSlot[]) => void;
  disabled?: boolean;
  /** Whether every slot row is freely addable/removable (SQL/Low-Code). `false` for dqx_native, where arity is normally fixed by the selected function's signature. */
  allowAddRemove?: boolean;
  /** dqx_native only: `arg_key` of the one column parameter that accepts a LIST of columns — see {@link listColumnArgKey}. Slots sharing it form an expandable group. */
  expandableArgKey?: string;
  /** dqx_native only (item 10): the slot family is fixed by the check's
   * semantics and shown read-only rather than as an editable Select. */
  lockFamily?: boolean;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState<number | null>(null);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (expanded === null) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (rootRef.current?.contains(target)) return;
      if (target.closest("[data-radix-popper-content-wrapper]")) return;
      setExpanded(null);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [expanded]);

  const familyLabel = (f: RuleSlotFamilyType): string =>
    ({
      numeric: t("rulesRegistry.slotFamilyNumeric"),
      text: t("rulesRegistry.slotFamilyText"),
      temporal: t("rulesRegistry.slotFamilyTemporal"),
      boolean: t("rulesRegistry.slotFamilyBoolean"),
      array: t("rulesRegistry.slotFamilyArray"),
      any: t("rulesRegistry.slotFamilyAny"),
    })[f];

  const expandableGroupSize = expandableArgKey
    ? value.filter((s) => (s.arg_key ?? s.name) === expandableArgKey).length
    : 0;
  const canAddSlot = allowAddRemove || expandableArgKey !== undefined;
  const canRemoveSlot = (slot: RuleSlot): boolean => {
    if (allowAddRemove) return true;
    return expandableArgKey !== undefined && (slot.arg_key ?? slot.name) === expandableArgKey && expandableGroupSize > 1;
  };

  const setAt = (i: number, patch: Partial<RuleSlot>) => {
    const next = value.slice();
    next[i] = { ...next[i], ...patch };
    onChange(next);
  };
  const removeAt = (i: number) => {
    const next = value.slice();
    next.splice(i, 1);
    onChange(next.map((s, idx) => ({ ...s, position: idx })));
    if (expanded === i) setExpanded(null);
  };
  const add = () => {
    const name = nextSlotName(value.map((s) => s.name));
    const arg_key = allowAddRemove ? undefined : expandableArgKey;
    onChange([...value, { name, family: "any", position: value.length, cardinality: "one", arg_key }]);
    setExpanded(value.length);
  };

  return (
    <div ref={rootRef} className="space-y-2">
      <SectionHeader
        tooltip={t("rulesRegistry.slotsPanelTooltip")}
        action={
          // Always reserve the add-button's footprint when the panel isn't
          // fully disabled (read-only), even when this mode's arity is fixed
          // (DQX Native with no expandable list argument) — otherwise the
          // "Columns used" header sits at a different height than in SQL/
          // Low-Code, which do show a real button here.
          !disabled ? (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={canAddSlot ? add : undefined}
              tabIndex={canAddSlot ? undefined : -1}
              aria-hidden={!canAddSlot}
              className={cn("h-7 px-2.5 text-xs gap-1.5", !canAddSlot && "invisible pointer-events-none")}
            >
              {t("rulesRegistry.slotsPanelAddButton")}
            </Button>
          ) : undefined
        }
      >
        {t("rulesRegistry.slotsPanelTitle")}
      </SectionHeader>
      <div className="space-y-2">
        {value.length === 0 && (
          <p className="text-xs text-muted-foreground py-1">{t("rulesRegistry.slotsPanelEmpty")}</p>
        )}
        {value.map((slot, i) => {
          const isOpen = expanded === i;
          const nameOk = SLOT_NAME_PATTERN.test(slot.name);
          const removable = canRemoveSlot(slot);
          return (
            <div
              key={i}
              className={cn(
                "border rounded-md bg-muted/30 transition-colors",
                !disabled && "cursor-pointer",
                isOpen && "bg-background border-primary/40",
              )}
              onClick={() => !disabled && setExpanded(isOpen ? null : i)}
            >
              <div className="grid grid-cols-[1fr_auto_auto] items-center gap-3 px-3 py-2">
                <code className={cn("text-xs", !nameOk && "text-destructive")}>{`{{${slot.name}}}`}</code>
                <Badge variant="outline" className="text-[10px] font-medium">
                  {familyLabel(slot.family)}
                </Badge>
                {!disabled && removable && (
                  <button
                    type="button"
                    aria-label={t("rulesRegistry.slotsPanelRemove")}
                    className="text-muted-foreground hover:text-destructive transition-colors"
                    onClick={(e) => {
                      e.stopPropagation();
                      removeAt(i);
                    }}
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                )}
              </div>
              {isOpen && !disabled && (
                <div
                  className="grid grid-cols-2 gap-3 px-3 pb-3 pt-2 border-t"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground">{t("rulesRegistry.slotsPanelNameLabel")}</Label>
                    <Input
                      value={slot.name}
                      onChange={(e) => setAt(i, { name: e.target.value })}
                      className="font-mono text-xs h-8"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-[11px] text-muted-foreground">{t("rulesRegistry.slotsPanelFamilyLabel")}</Label>
                    {lockFamily ? (
                      // Native mode (item 10): the check's semantics fix the
                      // family, so it's shown read-only with a hint rather than
                      // an editable Select.
                      <div className="flex h-8 items-center gap-1.5">
                        <Badge variant="outline" className="text-[10px] font-medium">
                          {familyLabel(slot.family)}
                        </Badge>
                        <HelpTooltip text={t("rulesRegistry.slotFamilyLockedTooltip")} />
                      </div>
                    ) : (
                      <Select value={slot.family} onValueChange={(v) => setAt(i, { family: v as RuleSlotFamilyType })}>
                        <SelectTrigger className="h-8 text-xs w-full">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {SLOT_FAMILIES.map((f) => (
                            <SelectItem key={f} value={f} className="text-xs">
                              {familyLabel(f)}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    )}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

const MODE_SEGMENTS: RegistryMode[] = ["dqx_native", "lowcode", "sql"];

/**
 * Segmented pill control selecting the authoring mode (DQX Native /
 * Low-Code / SQL), ported from dqlake's Low-code/SQL sliding switch in
 * `ImplementationTab`. dqlake only has two segments (it has no DQX-native
 * concept); this extends the same sliding-indicator pattern to three.
 */
function ModeSegmentedSwitch({
  value,
  onChange,
  disabled,
}: {
  value: RegistryMode;
  onChange: (next: RegistryMode) => void;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const labels: Record<RegistryMode, string> = {
    dqx_native: t("rulesRegistry.modeDqxNative"),
    lowcode: t("rulesRegistry.modeLowcode"),
    sql: t("rulesRegistry.modeSql"),
  };
  const activeIndex = MODE_SEGMENTS.indexOf(value);
  return (
    <div className="relative inline-grid grid-cols-3 rounded-md border p-0.5 bg-background h-9 items-stretch">
      <span
        aria-hidden
        className="absolute top-0.5 bottom-0.5 left-0.5 w-[calc(33.333%-2.67px)] rounded bg-primary shadow-sm transition-transform duration-200 ease-out"
        style={{ transform: `translateX(${activeIndex * 100}%)` }}
      />
      {MODE_SEGMENTS.map((m) => (
        <button
          key={m}
          type="button"
          onClick={() => {
            if (!disabled) onChange(m);
          }}
          disabled={disabled}
          className={cn(
            "relative z-10 rounded px-3 text-xs font-medium leading-none transition-colors duration-200 ease-out",
            value === m ? "text-primary-foreground" : "text-muted-foreground hover:text-foreground",
            disabled && "cursor-not-allowed opacity-60",
          )}
        >
          {labels[m]}
        </button>
      ))}
    </div>
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
  /** Called after a successful save/create. Receives the (possibly newly created) rule_id. */
  onSaved: (ruleId?: string) => void;
  /**
   * "dialog" (default) renders the classic modal used for creating a new
   * rule. "page" renders the same fields inline (no Dialog/overlay chrome)
   * for embedding on a routed detail page — see registry-rules.$ruleId.tsx
   * and registry-rules.new.tsx.
   */
  variant?: "dialog" | "page";
  /** Controlled top-level page tab (e.g. synced to a `?tab=` URL param). Falls back to internal state when omitted. */
  activeTab?: PageTab;
  onActiveTabChange?: (tab: PageTab) => void;
  /**
   * Notified whenever the dirty (unsaved-changes) state changes, so the
   * routed page can drive an unsaved-changes navigation guard. Always
   * `true` while creating a new rule (nothing to diff against) and `false`
   * once saved/submitted.
   */
  onDirtyChange?: (dirty: boolean) => void;
  /**
   * Controls the "As JSON" dialog from outside the component — e.g. the
   * routed detail page's "…" menu opens it (in apply-to-form mode, item 11)
   * for an existing rule, since the inline "As JSON" button is only shown
   * while creating. Falls back to internal state when omitted.
   */
  jsonDialogOpen?: boolean;
  onJsonDialogOpenChange?: (open: boolean) => void;
}

function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

/**
 * Deterministic JSON serialization (object keys sorted recursively) so two
 * snapshots built from the same logical values compare equal regardless of
 * insertion order — tags/parameters can be re-keyed as the steward edits
 * them without spuriously flipping `isDirty`.
 */
function stableStringify(value: unknown): string {
  const sort = (val: unknown): unknown => {
    if (Array.isArray(val)) return val.map(sort);
    if (val && typeof val === "object") {
      const obj = val as Record<string, unknown>;
      const sorted: Record<string, unknown> = {};
      for (const k of Object.keys(obj).sort()) sorted[k] = sort(obj[k]);
      return sorted;
    }
    return val;
  };
  return JSON.stringify(sort(value));
}

/** Snapshot of every editable field, used to detect unsaved changes against the last-persisted rule. */
interface RuleEditSnapshot {
  name: string;
  description: string;
  dimension: string;
  severity: string;
  steward: string;
  tags: Record<string, string>;
  mode: RegistryMode;
  polarity: Polarity;
  errorMessage: string;
  functionName: string;
  paramRawValues: Record<string, string>;
  sqlPredicate: string;
  sqlSlots: RuleSlot[];
  nativeSlots: RuleSlot[];
  lowcodeAst: LowcodeAstV2;
  groupBy: string;
  authorKind: CreateRegistryRuleInAuthorKind | undefined;
}

function snapshotFromRule(rule: RegistryRuleOut): RuleEditSnapshot {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const asString = (k: string) => (typeof md[k] === "string" ? (md[k] as string) : "");
  const tags: Record<string, string> = {};
  for (const [k, v] of Object.entries(md)) {
    if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
    if (typeof v === "string") tags[k] = v;
  }
  const isNative = rule.mode === "dqx_native";
  const paramRawValues: Record<string, string> = {};
  if (isNative) {
    for (const p of rule.definition?.parameters ?? []) paramRawValues[p.name] = paramValueToRaw(p.value);
  }
  const body = (rule.definition?.body ?? {}) as Record<string, unknown>;
  const storedAst = body.lowcode_ast;
  return {
    name: asString(RESERVED_NAME_KEY),
    description: asString(RESERVED_DESCRIPTION_KEY),
    dimension: asString(RESERVED_DIMENSION_KEY),
    severity: asString(RESERVED_SEVERITY_KEY),
    steward: rule.steward ?? "",
    tags,
    mode: rule.mode,
    polarity: rule.polarity ?? "pass",
    errorMessage: rule.definition?.error_message ?? "",
    functionName: isNative ? String((rule.definition?.body ?? {}).function ?? "") : "",
    paramRawValues,
    sqlPredicate: isNative ? "" : String(body.predicate ?? ""),
    sqlSlots: isNative ? [] : (rule.definition?.slots ?? []),
    nativeSlots: isNative ? (rule.definition?.slots ?? []) : [],
    lowcodeAst: rule.mode === "lowcode" && isV2Ast(storedAst) ? storedAst : EMPTY_LOWCODE_AST,
    groupBy: rule.mode === "lowcode" && typeof body.group_by === "string" ? body.group_by : "",
    authorKind: rule.author_kind ?? undefined,
  };
}

/** A new rule starts with one reusable column slot already declared (matching
 * dqlake's ColumnsUsedPanel, which seeds `column_1`/ANY) so authors don't have
 * to add the first slot by hand. Surfaces only in SQL / Low-Code mode; ignored
 * by native `buildDefinition`. */
const seededFirstSlot = (): RuleSlot => ({ name: "column_1", family: "any", position: 0, cardinality: "one" });

/** A new lowcode rule starts with one condition row already declared (same
 * seeding precedent as {@link seededFirstSlot}) so the author lands on a
 * ready-to-edit row instead of an empty builder with just "Add condition"
 * buttons. Defaults the column ref to the first declared slot, matching
 * `LowcodeBuilder`'s own `defaultColumnRef` fallback for added rows. */
const seededFirstLowcodeRow = (columnRef: string): AnyRow => ({
  kind: "row",
  combinator: null,
  column_ref: columnRef,
  operator: "is null",
  value: null,
});

/** Pristine defaults the "create new rule" hydration branch resets the form to — mirrors {@link snapshotFromRule}'s shape so an untouched new-rule form reads as clean (not dirty). */
const PRISTINE_NEW_SNAPSHOT: RuleEditSnapshot = {
  name: "",
  description: "",
  dimension: "",
  severity: "",
  steward: "",
  tags: {},
  mode: "dqx_native",
  polarity: "pass",
  errorMessage: "",
  functionName: "",
  paramRawValues: {},
  sqlPredicate: "",
  sqlSlots: [seededFirstSlot()],
  nativeSlots: [],
  lowcodeAst: EMPTY_LOWCODE_AST,
  groupBy: "",
  authorKind: "human",
};

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
  onDirtyChange,
  jsonDialogOpen: controlledJsonDialogOpen,
  onJsonDialogOpenChange,
}: RegistryRuleFormDialogProps) {
  const { t } = useTranslation();
  const sourceRule = editingRule ?? viewingRule;
  // Read-only only applies when explicitly viewing a rule (viewingRule set).
  // Creating a new rule also has editingRule === null, so gating on that
  // alone would incorrectly lock the create form.
  const readOnly = viewingRule !== null;
  const isEditing = editingRule !== null;
  // Editing an already-published (version > 0) rule is the edit-in-place
  // REVISION path: Save persists the revision while the rule stays
  // approved-at-vN ("Modified since vN"), and Submit sends it back through the
  // approval gate to be re-published as vN+1. The footer labels switch to this
  // "revision" wording (Save / Submit for review) instead of the draft-author
  // wording (Save draft / Save & submit).
  const isPublishedRevision = editingRule !== null && editingRule.version > 0;

  const { data: fnData } = useListCheckFunctions();
  const checkFunctions = useMemo(() => fnData?.data?.functions ?? [], [fnData]);

  const dimensionValues = useMemo(
    () => labelDefinitions.find((d) => d.key === RESERVED_DIMENSION_KEY)?.values ?? [],
    [labelDefinitions],
  );
  const severityValues = useMemo(
    () => orderSeverityValuesForDisplay(labelDefinitions.find((d) => d.key === RESERVED_SEVERITY_KEY)?.values ?? []),
    [labelDefinitions],
  );
  const tagDefinitions = useMemo(
    () => labelDefinitions.filter((d) => d.key !== RESERVED_DIMENSION_KEY && d.key !== RESERVED_SEVERITY_KEY),
    [labelDefinitions],
  );

  const [mode, setMode] = useState<RegistryMode>("dqx_native");
  const [internalPageTab, setInternalPageTab] = useState<PageTab>("about");
  const pageTab = controlledActiveTab ?? internalPageTab;
  const setPageTab = useCallback(
    (tab: PageTab) => {
      setInternalPageTab(tab);
      onActiveTabChange?.(tab);
    },
    [onActiveTabChange],
  );
  const [functionName, setFunctionName] = useState("");
  const [paramRawValues, setParamRawValues] = useState<Record<string, string>>({});
  const [sqlPredicate, setSqlPredicate] = useState("");
  const [sqlSlots, setSqlSlots] = useState<RuleSlot[]>([]);
  // Low-Code authoring state (shares `sqlSlots` for its "Columns used"
  // placeholders — the row/aggregate pickers bind to the same declared
  // slots). `lowcodeAst` holds the row stack + joins; `groupBy` is the raw
  // group-by SQL string (advanced). Both are persisted under the rule's body
  // (`lowcode_ast` / `group_by`) for re-editing, alongside the compiled SQL.
  const [lowcodeAst, setLowcodeAst] = useState<LowcodeAstV2>(EMPTY_LOWCODE_AST);
  const [groupBy, setGroupBy] = useState("");
  const [modeSwitch, setModeSwitch] = useState<{ direction: ModeSwitchDirection; next: RegistryMode } | null>(null);
  // item 5: cache the last Low-Code AST + group-by when leaving Low-Code for
  // SQL, so switching back RESTORES the built conditions instead of a blank
  // stack (covers flitting between the two). The cache is honored only when the
  // SQL still matches what the cached AST compiles to; if the SQL was
  // hand-edited away from that, the built conditions can't be reconstructed —
  // the cache is dropped and the builder is cleared with a warning.
  const lowcodeCacheRef = useRef<{ ast: LowcodeAstV2; groupBy: string } | null>(null);
  // Joined-table columns feed the Low-Code row/aggregate/group-by pickers
  // alongside the declared `{{slot}}` placeholders — qualified as
  // `<fqn>.<col>` so the compiler emits them as raw SQL, not placeholders.
  const joinedColumns = useJoinedColumns(lowcodeAst.joins);
  // DQX Native's slot SET (arity) is fixed by the selected function's
  // column-kind parameters, but each slot's `name`/`family` is author-
  // editable through the same SlotsPanel used by SQL/Low-Code — so, unlike
  // `slots`/`derivedParams` below (which are pure derivations of
  // `selectedFn`), this needs to be real state: seeded fresh whenever the
  // function selection changes, but otherwise left alone so edits persist.
  const [nativeSlots, setNativeSlots] = useState<RuleSlot[]>([]);
  const [errorMessage, setErrorMessage] = useState("");
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
  // "As JSON" surface (item 11, P24-C) — edits round-trip back into the form
  // state below via `applyParsedToForm`, for both the create form and
  // in-place editing of an existing rule. The open state can be driven
  // externally (the routed detail page's "…" menu, for the editing case)
  // since the inline trigger button is only rendered while creating.
  const [internalJsonDialogOpen, setInternalJsonDialogOpen] = useState(false);
  const jsonDialogOpen = controlledJsonDialogOpen ?? internalJsonDialogOpen;
  const setJsonDialogOpen = useCallback(
    (next: boolean) => {
      setInternalJsonDialogOpen(next);
      onJsonDialogOpenChange?.(next);
    },
    [onJsonDialogOpenChange],
  );

  // AI — Build-with-AI (full-form generate) + per-field suggest.
  const aiAvailability = useAiAvailability();
  const [aiDescription, setAiDescription] = useState("");
  const [aiBusy, setAiBusy] = useState(false);
  // Auto-grow the Build-with-AI textarea so long prompts don't scroll,
  // matching dqlake's BuildWithAiBanner.
  const aiTextareaRef = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    const el = aiTextareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${el.scrollHeight}px`;
  }, [aiDescription]);
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
    // Drop any Low-Code cache from a previous rule/session (item 5) so a
    // reopened dialog never restores conditions that belong to another rule.
    lowcodeCacheRef.current = null;

    const md = (sourceRule?.user_metadata ?? {}) as Record<string, unknown>;
    const asString = (k: string) => (typeof md[k] === "string" ? (md[k] as string) : "");
    setName(asString(RESERVED_NAME_KEY));
    setDescription(asString(RESERVED_DESCRIPTION_KEY));
    setDimension(asString(RESERVED_DIMENSION_KEY));
    setSeverity(asString(RESERVED_SEVERITY_KEY));
    setSteward(sourceRule?.steward ?? "");
    setNameError(null);
    setErrorMessage(sourceRule?.definition?.error_message ?? "");
    setAiDescription("");
    setPendingNativeArgs(null);
    const freeTags: Record<string, string> = {};
    for (const [k, v] of Object.entries(md)) {
      if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
      if (typeof v === "string") freeTags[k] = v;
    }
    setTags(freeTags);

    if (sourceRule) {
      setAuthorKind(sourceRule.author_kind ?? undefined);
      setMode(sourceRule.mode);
      setPageTab("about");
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
        setSqlSlots([]);
        setLowcodeAst(EMPTY_LOWCODE_AST);
        setGroupBy("");
        // Load the persisted slots as-authored (custom names/families
        // preserved) rather than re-deriving canonical ones from the
        // function signature.
        setNativeSlots(sourceRule.definition?.slots ?? []);
      } else {
        const body = (sourceRule.definition?.body ?? {}) as Record<string, unknown>;
        const predicate = body.predicate;
        setSqlPredicate(typeof predicate === "string" ? predicate : "");
        setSqlSlots(sourceRule.definition?.slots ?? []);
        setFunctionName("");
        setParamRawValues({});
        // Low-Code rules rehydrate the exact row stack / joins / group-by the
        // author built (stored alongside the compiled SQL) so the visual
        // builder reopens as-authored.
        const storedAst = body.lowcode_ast;
        setLowcodeAst(isV2Ast(storedAst) ? storedAst : EMPTY_LOWCODE_AST);
        setGroupBy(typeof body.group_by === "string" ? body.group_by : "");
      }
    } else {
      // Creating a brand-new rule: default to "human" authorship until an
      // AI affordance is used, and land on the About tab first. The
      // Build-with-AI banner (rendered above the tab strip, see formBody)
      // remains available regardless of which authoring mode is selected.
      setAuthorKind("human");
      setMode("dqx_native");
      setPageTab("about");
      setFunctionName("");
      setParamRawValues({});
      setSqlPredicate("");
      setSqlSlots([seededFirstSlot()]);
      setNativeSlots([]);
      setLowcodeAst(EMPTY_LOWCODE_AST);
      setGroupBy("");
      setPolarity("pass");
    }
  }, [open, sourceRule, setPageTab]);

  const selectedFn = useMemo(
    () => checkFunctions.find((f) => f.name === functionName),
    [checkFunctions, functionName],
  );
  // A native check that accepts `negate` surfaces the PASS/FAIL polarity
  // switcher as an ENABLED control (item 11); one that doesn't shows it frozen
  // (disabled) at its inherent polarity — every DQX check flags the NEGATION
  // of its named assertion (see check_funcs.make_condition), so a
  // non-negatable check's rows PASS when the named condition holds, i.e. the
  // disabled switcher rests at "pass".
  const nativeSupportsNegate = mode === "dqx_native" && fnSupportsNegate(selectedFn);
  // Whether the polarity value should be persisted for this rule: always for
  // SQL / Low-Code, and for a native check only when it supports `negate`.
  const polarityIsMeaningful = mode === "sql" || mode === "lowcode" || nativeSupportsNegate;
  // Pure derivation of the selected function's signature — the canonical
  // slot SET (arity + default names) and non-column parameters. Used to
  // (re-)seed `nativeSlots` whenever the function changes, and directly for
  // `derivedParams` (parameter arity is always fixed by the signature, no
  // renaming concept applies there).
  const { slots: fnDerivedSlots, parameters: derivedParams } = useMemo(
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
    // The AI proposal picked the function; re-seed the slot set to match
    // its arity (fresh canonical names — an AI proposal never carries
    // author-chosen slot names).
    setNativeSlots(fnDerivedSlots);
    setPendingNativeArgs(null);
  }, [pendingNativeArgs, checkFunctions, selectedFn, derivedParams, fnDerivedSlots]);

  const createMutation = useCreateRegistryRule();
  const updateMutation = useUpdateRegistryRule();
  const submitMutation = useSubmitRegistryRule();
  // When the approvals mode will auto-approve this user's submit (#94), the
  // submit buttons publish in one step — relabel them accordingly.
  const { willAutoApprove } = useApprovalsMode();
  const [saving, setSaving] = useState(false);

  // Published version lineage for the History tab. Only queried for a rule
  // that has been published at least once (version > 0) and while the dialog
  // is open — a fresh create form or an unpublished draft has no snapshots.
  const versionsRuleId = sourceRule?.rule_id ?? "";
  const versionsQuery = useListRegistryRuleVersions(versionsRuleId, {
    query: { enabled: open && Boolean(sourceRule && sourceRule.version > 0) },
  });
  const publishedVersions = useMemo(() => versionsQuery.data?.data ?? [], [versionsQuery.data]);

  // Rule-level DQ score, fetched here (non-suspending) only to decide the
  // Results tab trigger's enabled state: a rule with zero current
  // applications has no results, so the trigger is disabled with a tooltip
  // telling the steward to apply the rule to a monitored table first. The
  // tab CONTENT re-reads the same query via its own suspense boundary
  // (RuleResultsTab), so the two never disagree. Never refetches on its own
  // (RESULTS_QUERY_OPTIONS) — run-completion invalidation is the only
  // refresh, same as every other score query.
  const ruleScoreQuery = useGetRuleScore(versionsRuleId, undefined, {
    query: { enabled: open && Boolean(sourceRule), select: (d) => d.data, ...RESULTS_QUERY_OPTIONS },
  });
  const ruleScore = ruleScoreQuery.data;
  const resultsNotApplied = ruleScore !== undefined && ruleResultsState(ruleScore) === "not-applied";
  // A FAILED score fetch also leaves the trigger disabled, but with its own
  // explanatory tooltip + click-to-retry — previously it was silently
  // disabled, indistinguishable from the still-loading state (P3.6).
  const resultsScoreError = ruleScoreQuery.isError;
  // Disabled while there's no saved rule (create flow, like History) or the
  // score hasn't loaded yet — the tooltip only shows for the definitive
  // "not applied anywhere" / fetch-error states.
  const resultsDisabled = !sourceRule || ruleScore === undefined || resultsNotApplied;

  // -- Dirty (unsaved-changes) tracking -------------------------------------
  // Editing an existing rule diffs against the last-persisted rule (mirrors
  // dqlake's `useEditRuleState.isDirty`). Creating a new rule diffs against
  // the pristine "just opened the form" defaults instead of always being
  // `true`, so a blank, untouched create form doesn't spuriously trip the
  // unsaved-changes navigation guard.
  const currentSnapshot: RuleEditSnapshot = {
    name,
    description,
    dimension,
    severity,
    steward,
    tags,
    mode,
    polarity,
    errorMessage,
    functionName,
    paramRawValues,
    sqlPredicate,
    sqlSlots,
    nativeSlots,
    lowcodeAst,
    groupBy,
    authorKind,
  };
  const isDirty =
    stableStringify(currentSnapshot) !==
    stableStringify(editingRule ? snapshotFromRule(editingRule) : PRISTINE_NEW_SNAPSHOT);

  useEffect(() => {
    if (!open) return;
    onDirtyChange?.(readOnly ? false : isDirty);
  }, [open, readOnly, isDirty, onDirtyChange]);

  const sqlError = mode === "sql" ? validateSqlPredicate(sqlPredicate, t) : null;

  // -- Save gating -------------------------------------------------------
  // Names of the selected function's non-column parameters that have no
  // default (i.e. genuinely required) — drives both the red `*` markers on
  // parameter labels below and `canSubmit`'s native-mode check.
  const requiredParamNames = useMemo(
    () =>
      new Set(
        (selectedFn?.params ?? []).filter((p) => p.required && !COLUMN_KINDS.has(p.kind)).map((p) => p.name),
      ),
    [selectedFn],
  );
  const nativeRequiredParamsFilled = derivedParams
    .filter((p) => requiredParamNames.has(p.name))
    .every((p) => (paramRawValues[p.name] ?? "").trim().length > 0);
  const slotsHaveValidNames = (slots: RuleSlot[]) => slots.every((s) => SLOT_NAME_PATTERN.test(s.name));
  // Two-tier save gating (P17-A). A DRAFT must stay saveable while it is
  // still incomplete — that is what a draft is for, and the registry holds
  // rules persisted before dimension/severity/required-parameter gating
  // existed (imported YAML, AI proposals, pre-gate builds). Requiring FULL
  // completeness just to persist e.g. a description edit dead-ends those
  // rules: Save sits greyed out until the steward hunts down unrelated
  // fields across tabs — and a draft cloned from a pre-gate approved rule
  // via "Edit as new draft" hits the same wall, so every edit path on such
  // a registry reads as "rules cannot be updated".
  //
  // `canSaveDraft` therefore only requires structural validity — the fields
  // without which `buildDefinition` would persist a malformed body (name,
  // the mode's body source, well-formed slot names, no prohibited SQL).
  // `canSubmit` layers the completeness requirements (severity, dimension,
  // required parameters) on top and gates "Save & submit" / "Submit for
  // approval" — the quality bar moves to the review boundary instead of
  // blocking persistence. Low-Code gates on its compiled predicate being
  // non-empty (at least one fully-built condition row).
  // Compiled low-code predicate — non-empty only once at least one row
  // fully compiles (a column/aggregate + operator + any required value).
  // Drives Low-Code's structural-validity gate the way `sqlPredicate` does
  // for SQL mode.
  const lowcodePredicate = mode === "lowcode" ? compileAstToSql(lowcodeAst).trim() : "";
  // A Low-Code rule that folds joins and/or group-by into a dataset-level
  // sql_query can't be row-tested (only the row predicate reaches the runner),
  // so the Test tab hides its surface. Reuses compileLowcodeBody's own
  // classification so the gate never drifts from what actually materializes.
  const lowcodeAdvanced = mode === "lowcode" && lowcodeHasAdvancedShape(lowcodeAst, groupBy);
  const structurallyValid =
    mode === "dqx_native"
      ? functionName.trim().length > 0 && slotsHaveValidNames(nativeSlots)
      : mode === "sql"
        ? sqlPredicate.trim().length > 0 && sqlError === null && slotsHaveValidNames(sqlSlots)
        : lowcodePredicate.length > 0 && slotsHaveValidNames(sqlSlots);
  const canSaveDraft = name.trim().length > 0 && !nameError && structurallyValid;
  const canSubmit =
    canSaveDraft &&
    severity.trim().length > 0 &&
    dimension.trim().length > 0 &&
    (mode !== "dqx_native" || nativeRequiredParamsFilled);

  // Human-readable lists of exactly which field(s) each gate is still
  // waiting on, surfaced as tooltips on the disabled buttons (see
  // `footerButtons` below) so a blocked save/submit never looks
  // permanently broken. `missingDraftFieldLabels` mirrors `canSaveDraft`;
  // `missingSubmitFieldLabels` extends it with `canSubmit`'s
  // completeness-only fields.
  const missingDraftFieldLabels: string[] = [];
  if (!name.trim() || nameError) missingDraftFieldLabels.push(t("rulesRegistry.nameLabel"));
  if (mode === "dqx_native") {
    if (!functionName.trim()) missingDraftFieldLabels.push(t("rulesRegistry.conditionLabel"));
    if (!slotsHaveValidNames(nativeSlots)) missingDraftFieldLabels.push(t("rulesRegistry.columnSlotsLabel"));
  } else if (mode === "sql") {
    if (!sqlPredicate.trim() || sqlError) missingDraftFieldLabels.push(t("rulesRegistry.conditionLabel"));
    if (!slotsHaveValidNames(sqlSlots)) missingDraftFieldLabels.push(t("rulesRegistry.columnSlotsLabel"));
  } else if (mode === "lowcode") {
    if (!lowcodePredicate) missingDraftFieldLabels.push(t("rulesRegistry.lowcodeConditionsLabel"));
    if (!slotsHaveValidNames(sqlSlots)) missingDraftFieldLabels.push(t("rulesRegistry.columnSlotsLabel"));
  }
  const missingSubmitFieldLabels: string[] = [...missingDraftFieldLabels];
  if (!severity.trim()) missingSubmitFieldLabels.push(t("rulesRegistry.severityLabel"));
  if (!dimension.trim()) missingSubmitFieldLabels.push(t("rulesRegistry.dimensionLabel"));
  if (mode === "dqx_native" && functionName.trim() && !nativeRequiredParamsFilled)
    missingSubmitFieldLabels.push(t("rulesRegistry.requiredParametersLabel"));

  const buildDefinition = (): RuleDefinition => {
    const trimmedError = errorMessage.trim();
    if (mode === "sql") {
      return {
        body: { predicate: sqlPredicate.trim() },
        slots: sqlSlots,
        parameters: [],
        error_message: trimmedError || undefined,
      };
    }
    if (mode === "lowcode") {
      // Persist the re-editable AST + group-by alongside the compiled SQL.
      // The compiled `predicate` (simple) or `sql_query` + `merge_columns`
      // (joins/group-by) is what materializes and runs via the existing
      // sql-mode path — the AST is display/edit-only.
      const compiled = compileLowcodeBody(lowcodeAst, groupBy);
      const body: Record<string, unknown> = { lowcode_ast: lowcodeAst };
      if (groupBy.trim()) body.group_by = groupBy.trim();
      if (compiled.predicate !== undefined) body.predicate = compiled.predicate;
      if (compiled.sql_query !== undefined) body.sql_query = compiled.sql_query;
      if (compiled.merge_columns !== undefined) body.merge_columns = compiled.merge_columns;
      return {
        body,
        slots: sqlSlots,
        parameters: [],
        error_message: trimmedError || undefined,
      };
    }
    const parameters: RuleParameter[] = derivedParams.map((p) => ({
      ...p,
      value: parseParamValue(p.type, paramRawValues[p.name] ?? ""),
    }));
    return {
      body: { function: functionName, arguments: nativeArguments(nativeSlots, selectedFn) },
      slots: nativeSlots,
      parameters,
      error_message: trimmedError || undefined,
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

  // Write a parsed "As JSON" edit (item 11) back into the form state. Mirrors
  // the rehydrate branch, but sourced from `parseDqxCheckJson` rather than a
  // stored rule — so editing the JSON is exactly equivalent to editing the
  // visual fields. Native slot names are canonical (`{{column_N}}`) per the
  // parser's documented caveat.
  const applyParsedToForm = (parsed: ParsedCheckDefinition) => {
    const def = parsed.definition;
    const body = (def.body ?? {}) as Record<string, unknown>;
    setMode(parsed.mode);
    setPolarity(parsed.polarity ?? "pass");
    setErrorMessage(def.error_message ?? "");
    if (parsed.mode === "dqx_native") {
      setFunctionName(String(body.function ?? ""));
      const raw: Record<string, string> = {};
      for (const p of def.parameters ?? []) raw[p.name] = paramValueToRaw(p.value);
      setParamRawValues(raw);
      setNativeSlots(def.slots ?? []);
      setSqlPredicate("");
      setSqlSlots([]);
      setLowcodeAst(EMPTY_LOWCODE_AST);
      setGroupBy("");
    } else {
      setFunctionName("");
      setParamRawValues({});
      setSqlPredicate(typeof body.predicate === "string" ? body.predicate : "");
      setSqlSlots(def.slots ?? []);
      if (parsed.mode === "lowcode") {
        const storedAst = body.lowcode_ast;
        setLowcodeAst(isV2Ast(storedAst) ? storedAst : EMPTY_LOWCODE_AST);
        setGroupBy(typeof body.group_by === "string" ? body.group_by : "");
      } else {
        setLowcodeAst(EMPTY_LOWCODE_AST);
        setGroupBy("");
      }
    }
    const md = parsed.userMetadata;
    setName(md[RESERVED_NAME_KEY] ?? "");
    setDescription(md[RESERVED_DESCRIPTION_KEY] ?? "");
    setDimension(md[RESERVED_DIMENSION_KEY] ?? "");
    setSeverity(md[RESERVED_SEVERITY_KEY] ?? "");
    const freeTags: Record<string, string> = {};
    for (const [k, v] of Object.entries(md)) {
      if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
      freeTags[k] = v;
    }
    setTags(freeTags);
    // JSON is mostly the implementation body — land the steward there to review.
    setPageTab("implementation");
    toast.success(t("rulesRegistry.jsonAppliedToForm"));
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
    const body = (proposal.definition ?? {}) as Record<string, unknown>;
    const nativeFn = typeof body.function === "string" ? body.function : "";
    // The AI can legitimately propose a raw SQL predicate two ways: an
    // explicit `mode: "sql"` proposal, or a `dqx_native` proposal whose
    // selected function is the `sql_query`/`sql_expression` check (both are
    // real registered DQX functions). Either shape should land the form
    // directly on the SQL authoring mode with the predicate filled in —
    // never on DQX Native with that function selected, which would force
    // the steward to redirect out manually via the same special-case the
    // Function combobox applies below.
    const isSqlProposal = proposal.mode === "sql" || nativeFn === "sql_query" || nativeFn === "sql_expression";
    const appliedMode: RegistryMode = isSqlProposal ? "sql" : "dqx_native";
    // Switch the mode segmented control onto the real authoring mode so the
    // steward immediately sees (and can tweak) what the proposal filled in.
    setMode(appliedMode);
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

    if (isSqlProposal) {
      const nativeArgs =
        body.arguments && typeof body.arguments === "object"
          ? (body.arguments as Record<string, unknown>)
          : {};
      const predicate =
        typeof body.sql_query === "string"
          ? body.sql_query
          : typeof nativeArgs.query === "string"
            ? nativeArgs.query
            : typeof nativeArgs.expression === "string"
              ? nativeArgs.expression
              : "";
      setSqlPredicate(predicate);
      setPolarity(proposal.polarity === "fail" ? "fail" : "pass");
      setFunctionName("");
      setParamRawValues({});
      setPendingNativeArgs(null);
    } else {
      setFunctionName(nativeFn);
      setSqlPredicate("");
      const args =
        body.arguments && typeof body.arguments === "object"
          ? (body.arguments as Record<string, unknown>)
          : {};
      setPendingNativeArgs(args);
    }
    setAiDescription("");
    // Jump to Implementation so the steward immediately sees what the
    // proposal filled in, regardless of which page tab they were on when
    // they used the (now always-visible) Build-with-AI banner.
    setPageTab("implementation");
    toast.success(t("rulesRegistry.aiProposalApplied"));
  };

  const handleAiGenerate = async () => {
    if (!aiDescription.trim()) return;
    setAiBusy(true);
    try {
      const resp = await generateRuleMutation.mutateAsync({ data: { description: aiDescription.trim() } });
      // Apply directly to the form (dqlake writes the AI result straight into
      // form state via onResult — no intermediate JSON-preview card).
      applyAiProposal(resp.data);
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
      setAuthorKind((prev) => (prev && prev !== "human" ? prev : "ai_assisted"));
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
      setPageTab("about");
      return false;
    }
    setNameError(null);
    if (mode === "dqx_native" && !functionName) {
      toast.error(t("rulesRegistry.functionRequired"));
      setPageTab("implementation");
      return false;
    }
    if (mode === "sql") {
      if (!sqlPredicate.trim()) {
        toast.error(t("rulesRegistry.predicateRequired"));
        setPageTab("implementation");
        return false;
      }
      if (sqlError) {
        toast.error(sqlError);
        setPageTab("implementation");
        return false;
      }
    }
    if (mode === "lowcode" && !lowcodePredicate) {
      toast.error(t("rulesRegistry.lowcodeConditionsRequired"));
      setPageTab("implementation");
      return false;
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
          polarity: polarityIsMeaningful ? polarity : null,
          user_metadata: userMetadata,
          steward: steward.trim() || null,
          // Persist AI provenance stamped during this edit-in-place session
          // (e.g. accepting an AI-suggested field on an otherwise
          // human-authored draft) rather than silently dropping it.
          author_kind: authorKind,
        };
        const resp = await updateMutation.mutateAsync({ ruleId: editingRule.rule_id, data: payload });
        ruleId = resp.data.rule_id;
        // Skip the "updated" toast when this save is immediately followed by
        // a submit — the final toastSubmitted below is the one that reflects
        // where the rule actually lands, and firing both back-to-back reads
        // as a contradiction ("updated" then "submitted for approval").
        if (!thenSubmit) toast.success(t("rulesRegistry.toastUpdated"));
      } else {
        const payload: CreateRegistryRuleIn = {
          mode,
          definition,
          polarity: polarityIsMeaningful ? polarity : null,
          user_metadata: userMetadata,
          steward: steward.trim() || null,
          author_kind: authorKind ?? "human",
        };
        const resp = await createMutation.mutateAsync({ data: payload });
        ruleId = resp.data.rule.rule_id;
        // Same reasoning as above: "Save & submit" on a brand-new rule must
        // not show "Rule created as a draft." — the rule never rests in
        // draft state, it goes straight to pending approval.
        if (!thenSubmit) toast.success(t("rulesRegistry.toastCreated"));
        if (resp.data.dedup_warning) {
          toast.warning(resp.data.dedup_warning, { duration: 8000 });
        }
      }
      if (thenSubmit) {
        await submitMutation.mutateAsync({ ruleId });
        toast.success(t("rulesRegistry.toastSubmitted"));
      }
      onSaved(ruleId);
      closeAndReset();
    } catch (err) {
      toast.error(extractApiError(err, t("rulesRegistry.saveFailed")), { duration: 6000 });
    } finally {
      setSaving(false);
    }
  };

  // Submits an already-saved, unmodified draft for approval without a
  // redundant PATCH — mirrors dqlake's plain "Publish" button, shown in
  // place of "Save and Submit" once the draft has no pending edits.
  const handleSubmitOnly = async () => {
    if (readOnly || !editingRule) return;
    setSaving(true);
    try {
      await submitMutation.mutateAsync({ ruleId: editingRule.rule_id });
      toast.success(t("rulesRegistry.toastSubmitted"));
      onSaved(editingRule.rule_id);
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

  // Gates the always-visible Build-with-AI banner rendered above the page
  // tab strip (formBody), matching dqlake's `header` slot placement.
  const showAiBanner = !readOnly && aiAvailability.available;
  // Match dqlake: only offer per-field "Suggest with AI" once there's enough
  // context for the model to ground a suggestion — a predicate, a
  // meaningfully-declared slot (not the pristine `column_N`/any seed), a name,
  // or a description. On a blank form the model would only return filler.
  const isPristineSlot = (s: RuleSlot) => /^column_\d+$/.test(s.name) && s.family === "any";
  const declaredSlots = mode === "dqx_native" ? nativeSlots : sqlSlots;
  const enoughAiContext =
    sqlPredicate.trim().length > 0 ||
    functionName.trim().length > 0 ||
    declaredSlots.some((s) => !isPristineSlot(s)) ||
    name.trim().length > 0 ||
    description.trim().length > 0;
  const showFieldSuggest = showAiBanner && enoughAiContext;

  const authorKindBadges = (
    <>
      {authorKind === "ai_generated" && (
        <Badge variant="secondary" className={cn("gap-1 text-[10px] font-normal", AI_BUTTON_BG)}>
          <Sparkles className="h-2.5 w-2.5" />
          {t("rulesRegistry.authorKindAiGenerated")}
        </Badge>
      )}
      {authorKind === "ai_assisted" && (
        <Badge variant="secondary" className={cn("gap-1 text-[10px] font-normal", AI_BUTTON_BG)}>
          <Sparkles className="h-2.5 w-2.5" />
          {t("rulesRegistry.authorKindAiAssisted")}
        </Badge>
      )}
    </>
  );

  // Field order mirrors dqlake's AboutTab: Name, Description, a divider,
  // then Default Severity before Dimension — each picker shows the same
  // colored dot dqlake renders next to the selected value (sourced from the
  // label definition's configured value_colors, same data DQX already uses
  // for badges elsewhere). Steward now lives on its own Permissions tab (see
  // permissionsTabContent below), alongside the UC-style grants surface.
  const aboutTabContent = (
    <div className="space-y-4 pt-2">
      <div className="space-y-1.5">
        <Label className="text-xs">
          {t("rulesRegistry.nameLabel")} <span className="text-destructive">*</span>
        </Label>
        <div className="relative group">
          <Input
            className={cn(
              "h-8 text-xs",
              nameError && "border-red-400 focus-visible:ring-red-400",
              showFieldSuggest && "pr-9",
            )}
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={readOnly}
            placeholder={t("rulesRegistry.namePlaceholder")}
          />
          {showFieldSuggest && (
            <AiSuggestIcon
              field="name"
              busy={suggestingField === "name"}
              onClick={() => handleAiSuggestField("name")}
              label={t("rulesRegistry.aiSuggestButton")}
              position="input"
            />
          )}
        </div>
        {nameError && <p className="text-[10px] text-red-500">{nameError}</p>}
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">{t("rulesRegistry.descriptionLabel")}</Label>
        <div className="relative group">
          <Textarea
            className={cn("text-xs min-h-[60px]", showFieldSuggest && "pr-9")}
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            disabled={readOnly}
            placeholder={t("rulesRegistry.descriptionPlaceholder")}
          />
          {showFieldSuggest && (
            <AiSuggestIcon
              field="description"
              busy={suggestingField === "description"}
              onClick={() => handleAiSuggestField("description")}
              label={t("rulesRegistry.aiSuggestButton")}
              position="textarea"
            />
          )}
        </div>
      </div>

      <Separator />

      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5">
          <Label className="text-xs">
            {t("rulesRegistry.severityLabel")} <span className="text-destructive">*</span>
          </Label>
          <HelpTooltip text={t("rulesRegistry.severityTooltip")} />
        </div>
        <div className="flex items-center gap-2 group">
          {/* Always a Select (disabled when read-only) — matches Dimension's
              rendering below so severity doesn't visually downgrade from a
              dropdown to a plain badge for non-editable rules (regression:
              a rule WITH severity set, e.g. rejected, used to lose its
              dropdown chrome entirely). The read-only "None" placeholder
              still communicates a genuinely-unset severity — see
              `SeverityBadge`'s read-only rendering elsewhere for the
              plain-badge equivalent used outside this form. */}
          <Select value={severity || undefined} onValueChange={setSeverity} disabled={readOnly}>
            <SelectTrigger className="h-8 w-full max-w-xs text-xs">
              <SelectValue
                placeholder={readOnly ? t("monitoredTables.severityNoneLabel") : t("rulesRegistry.selectSeverity")}
              />
            </SelectTrigger>
            <SelectContent>
              {severityValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">
                  <span className="flex items-center gap-1.5">
                    <ColorDot color={colorFor(labelDefinitions as LabelColorDefinition[], RESERVED_SEVERITY_KEY, v)} />
                    {v}
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {showFieldSuggest && severityValues.length > 0 && (
            <AiSuggestIcon
              field="severity"
              busy={suggestingField === "severity"}
              onClick={() => handleAiSuggestField("severity")}
              label={t("rulesRegistry.aiSuggestButton")}
              position="inline"
            />
          )}
        </div>
      </div>

      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5">
          <Label className="text-xs">
            {t("rulesRegistry.dimensionLabel")} <span className="text-destructive">*</span>
          </Label>
          <HelpTooltip text={t("rulesRegistry.dimensionTooltip")} />
        </div>
        <div className="flex items-center gap-2 group">
          <Select value={dimension || undefined} onValueChange={setDimension} disabled={readOnly}>
            <SelectTrigger className="h-8 w-full max-w-xs text-xs">
              <SelectValue placeholder={t("rulesRegistry.selectDimension")} />
            </SelectTrigger>
            <SelectContent>
              {dimensionValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">
                  <span className="flex items-center gap-1.5">
                    <ColorDot color={colorFor(labelDefinitions as LabelColorDefinition[], RESERVED_DIMENSION_KEY, v)} />
                    {v}
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {showFieldSuggest && dimensionValues.length > 0 && (
            <AiSuggestIcon
              field="dimension"
              busy={suggestingField === "dimension"}
              onClick={() => handleAiSuggestField("dimension")}
              label={t("rulesRegistry.aiSuggestButton")}
              position="inline"
            />
          )}
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
  );

  // Permissions tab — a UC-style permissions surface (steward + per-principal
  // grants). The grants table needs a saved object id, so for a not-yet-created
  // rule (create mode) `PermissionsTab` still renders the steward picker but
  // replaces the grants table with an empty shell ("save first" message),
  // guarded on `objectId`.
  const permissionsTabContent = (
    <div className="pt-2">
      <PermissionsTab
        objectType="registry_rule"
        objectId={sourceRule?.rule_id ?? ""}
        showSteward
        canEditSteward={!readOnly}
        steward={steward}
        onStewardChange={setSteward}
      />
    </div>
  );

  // "Columns used" (SlotsPanel) renders for SQL / Low-Code unconditionally —
  // authors freely declare which `{{slot}}` placeholders their predicate
  // references. DQX Native shows the same editable panel seeded from the
  // selected function's signature, but only when that function actually
  // HAS column-kind parameters (`fnDerivedSlots`) — a function like
  // `has_no_row_anomalies` takes none, so there's nothing to show and the
  // panel would just be empty chrome.
  const showColumnsUsedPanel = mode === "sql" || mode === "lowcode" || (mode === "dqx_native" && fnDerivedSlots.length > 0);
  const currentSlots = mode === "dqx_native" ? nativeSlots : sqlSlots;
  const setCurrentSlots = mode === "dqx_native" ? setNativeSlots : setSqlSlots;
  // The one column parameter (if any) on the selected native function that
  // accepts a LIST of columns — its slots form an add/removable group (see
  // `SlotsPanel`'s `expandableArgKey`); every other native slot has fixed
  // arity (one slot per scalar parameter).
  const nativeExpandableArgKey = listColumnArgKey(selectedFn);

  // The columns the Low-Code builder offers in its pickers: every declared
  // `{{slot}}` (family-mapped to the builder's UPPERCASE vocabulary) plus any
  // joined-table columns.
  const lowcodeColumns: LowcodeColumnRef[] = [
    ...sqlSlots.map((s) => ({ name: s.name, family: slotFamilyToLowcode(s.family) })),
    ...joinedColumns,
  ];

  // Guarded authoring-mode switch (ported from dqlake's ModeSwitchDialog).
  // Only prompts when the source mode holds real content the target can't
  // preserve; otherwise switches immediately.
  const performModeSwitch = (next: RegistryMode) => {
    // Low-Code -> SQL: translate the built rows + joins into the SQL editor
    // so the author keeps their work instead of retyping it, and cache the
    // AST + group-by so switching back can restore it (item 5).
    if (mode === "lowcode" && next === "sql") {
      lowcodeCacheRef.current = { ast: lowcodeAst, groupBy };
      const predicate = compileAstToSql(lowcodeAst);
      const joinsSql = compileJoinsToSql(lowcodeAst.joins);
      if (predicate) setSqlPredicate(predicate);
      if (joinsSql || groupBy.trim()) {
        // Fold joins/group-by into the SQL editor's Advanced note is out of
        // scope for the single-predicate SQL editor; surface them inline so
        // nothing is silently dropped.
        setSqlPredicate((prev) => prev || predicate);
      }
    }
    // SQL -> Low-Code with a cached AST (item 5): restore the built conditions
    // when the SQL still matches what that AST compiles to. If the SQL was
    // hand-edited away from the compiled output, the structured rows can't be
    // recovered from free SQL — drop the cache, clear the builder and warn,
    // rather than silently restoring a stale, mismatched AST. With no cache
    // (SQL authored from scratch) this falls through to the blank seed below.
    if (mode === "sql" && next === "lowcode" && lowcodeCacheRef.current) {
      const cache = lowcodeCacheRef.current;
      const compiledFromCache = compileAstToSql(cache.ast).trim();
      const currentSql = stripSqlLineComments(sqlPredicate).trim();
      if (compiledFromCache === currentSql) {
        setLowcodeAst(cache.ast);
        setGroupBy(cache.groupBy);
      } else {
        lowcodeCacheRef.current = null;
        setGroupBy("");
        setLowcodeAst({
          ...EMPTY_LOWCODE_AST,
          rows: [seededFirstLowcodeRow(lowcodeColumns[0]?.name ?? "column_1")],
        });
        toast.warning(t("rulesRegistry.lowcodeSqlDivergedWarning"));
      }
      setMode(next);
      setModeSwitch(null);
      return;
    }
    // Landing in Low-Code with nothing translated over (e.g. from a blank SQL
    // predicate) — seed the first condition row (item 7) rather than leaving
    // an empty builder.
    if (next === "lowcode" && lowcodeAst.rows.length === 0) {
      setLowcodeAst((prev) => ({ ...prev, rows: [seededFirstLowcodeRow(lowcodeColumns[0]?.name ?? "column_1")] }));
    }
    setMode(next);
    setModeSwitch(null);
  };

  const requestModeChange = (next: RegistryMode) => {
    if (readOnly || next === mode) return;
    const hasLowcodeContent = lowcodeAst.rows.length > 0 || lowcodeAst.joins.length > 0 || groupBy.trim().length > 0;
    const hasSqlContent = sqlPredicate.trim().length > 0;
    const hasNativeContent = functionName.trim().length > 0;
    if (mode === "lowcode" && hasLowcodeContent) {
      setModeSwitch({ direction: next === "sql" ? "LOWCODE_TO_SQL" : "LOWCODE_TO_NATIVE", next });
      return;
    }
    if (mode === "sql" && hasSqlContent) {
      setModeSwitch({ direction: next === "lowcode" ? "SQL_TO_LOWCODE" : "SQL_TO_NATIVE", next });
      return;
    }
    if (mode === "dqx_native" && hasNativeContent) {
      setModeSwitch({ direction: next === "lowcode" ? "NATIVE_TO_LOWCODE" : "NATIVE_TO_SQL", next });
      return;
    }
    // New/blank rule switching straight into Low-Code (no content anywhere
    // to guard) — seed the first condition row (item 7).
    if (next === "lowcode" && lowcodeAst.rows.length === 0) {
      setLowcodeAst((prev) => ({ ...prev, rows: [seededFirstLowcodeRow(lowcodeColumns[0]?.name ?? "column_1")] }));
    }
    setMode(next);
  };

  const implementationTabContent = (
    // `w-full` pins this tab's content to the tab strip's stable width
    // regardless of which mode's fields it's currently rendering (DQX
    // Native / Low-Code / SQL each have different intrinsic content), so
    // switching Rule Type doesn't visibly shift the panel left/right.
    <div className="w-full space-y-4 pt-2">
      {/* Rule Type only applies to (and is only shown within) the
          Implementation tab — it drives the rest of this tab's content, so
          it renders at the top of it rather than persistently across all
          tabs. */}
      <div className="space-y-2 pb-2">
        <SectionHeader
          action={
            // "As JSON" is offered while creating a new rule (item 11) — a saved
            // rule's detail page exposes the equivalent via its "…" menu instead.
            !readOnly && !isEditing ? (
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 gap-1.5 text-xs"
                onClick={() => setJsonDialogOpen(true)}
              >
                <Braces className="h-3.5 w-3.5" />
                {t("rulesRegistry.actionAsJson")}
              </Button>
            ) : undefined
          }
        >
          {t("rulesRegistry.ruleTypeHeader")}
        </SectionHeader>
        <ModeSegmentedSwitch value={mode} onChange={requestModeChange} disabled={readOnly} />
      </div>

      {/* "Columns used" leads the rest of the Implementation area, matching
          dqlake's ImplementationTab order (ColumnsUsedPanel first, mode
          switch + predicate editor below it). */}
      {showColumnsUsedPanel && (
        <SlotsPanel
          value={currentSlots}
          onChange={setCurrentSlots}
          disabled={readOnly}
          allowAddRemove={mode !== "dqx_native"}
          expandableArgKey={mode === "dqx_native" ? nativeExpandableArgKey : undefined}
          lockFamily={mode === "dqx_native"}
        />
      )}

      {mode === "lowcode" && (
        <div className="space-y-3">
          <div className="space-y-1.5">
            <Label className="text-xs">{t("rulesRegistry.conditionLabel")}</Label>
            {/* Unlike Native/SQL, Low-Code renders "IF" inline with the first
                condition row (vertically centered against it, item 6) rather
                than as a standalone label above the builder — there's no
                duplicate framing word to add here. */}
            <LowcodeBuilder
              ast={lowcodeAst}
              onChange={setLowcodeAst}
              declaredColumns={lowcodeColumns}
              readOnly={readOnly}
            />
          </div>
          {/* Advanced — group-by + joins, folded into the compiled SQL that
              actually runs (see lowcodeCompile.compileLowcodeBody). Placed
              above "THEN THE ROW" (item 23f) since group-by/joins configure
              the IF condition's inputs, not the row-level outcome below it. */}
          <AdvancedDisclosure
            label={t("rulesRegistry.advancedSectionLabel")}
            defaultOpen={!!groupBy || lowcodeAst.joins.length > 0}
          >
            {/* Joins come first: they widen the set of columns available to
                the condition (and to group-by below) by pulling in
                joined-table columns, so configuring them before grouping
                matches the data-flow the compiled SQL follows. Gets the full
                `lowcodeColumns` (declared slots + joined-table columns). */}
            <JoinsBuilder
              ast={lowcodeAst}
              onChange={setLowcodeAst}
              declaredColumns={lowcodeColumns}
              readOnly={readOnly}
            />
            {/* Group-by is restricted to declared `{{slot}}` columns (not
                joined-table columns): a grouping key becomes a `merge_column`,
                and DQX's row-level merge-back requires those columns to exist
                on the monitored input table. */}
            <GroupByField
              value={groupBy}
              onChange={setGroupBy}
              declaredColumns={lowcodeColumns.filter((c) => !c.name.includes("."))}
              disabled={readOnly}
            />
          </AdvancedDisclosure>
          <div className="flex flex-wrap items-center gap-3">
            <FramingWord>{t("rulesRegistry.thenTheRow")}</FramingWord>
            <PredicatePolaritySwitch value={polarity} onChange={setPolarity} disabled={readOnly} />
          </div>
        </div>
      )}

      {mode === "dqx_native" && (
        <div className="space-y-3">
          <div className="space-y-1.5">
            <Label className="text-xs">
              {t("rulesRegistry.conditionLabel")} <span className="text-destructive">*</span>
            </Label>
            {/* IF on its own line with the same vertical breathing room before
                the control as SQL mode (item 8) — `space-y-3` matches the SQL
                editor's IF-to-editor gap. The function selector is content-
                narrow (`max-w-sm`) rather than full dialog width — a check-
                function name is short, so a full-width combobox over-emphasized
                it (item 8). */}
            <div className="space-y-3">
              <FramingWord>{t("rulesRegistry.ifCondition")}</FramingWord>
              <div className="max-w-sm">
                <FunctionCombobox
                  value={functionName}
                  functions={checkFunctions}
                  onChange={(fn) => {
                    // `sql_expression` / `sql_query` are technically selectable
                    // dqx_native functions, but authoring a raw SQL predicate is
                    // exactly what SQL mode is for — redirect there instead of
                    // wiring them up as a native function selection, mirroring
                    // how `applyAiProposal` resets the *other* mode's fields
                    // when switching modes.
                    if (fn === "sql_expression" || fn === "sql_query") {
                      setMode("sql");
                      setFunctionName("");
                      setParamRawValues({});
                      setNativeSlots([]);
                      return;
                    }
                    setFunctionName(fn);
                    setParamRawValues({});
                    // A freshly selected native function resets polarity to its
                    // default ("pass" = the check's named assertion passing); the
                    // switcher is only editable when the new function supports
                    // `negate` (item 11).
                    setPolarity("pass");
                    // Arity is fixed by the function signature — switching
                    // functions must fully replace the slot set (e.g.
                    // is_not_null's 1 slot -> is_unique's many-cardinality
                    // `columns` slot), not merge with whatever was there before.
                    const nextFn = checkFunctions.find((f) => f.name === fn);
                    setNativeSlots(deriveSlotsAndParameters(nextFn).slots);
                  }}
                  disabled={readOnly}
                />
              </div>
            </div>
          </div>
          {derivedParams.length > 0 && (
            <div className="space-y-2 border-l pl-5 ml-3">
              <Label className="text-xs">{t("rulesRegistry.parametersLabel")}</Label>
              {/* Item 17: `flex-wrap` + a fixed narrow basis (rather than a
                  2-column grid stretching each field to half the dialog's
                  width) — DQX Native parameters are typically short scalars
                  (thresholds, flags, small literals), so a half-dialog-wide
                  input just wastes space and reads as over-emphasized.
                  Reference-table/-column pickers still take the full row —
                  they genuinely need it. */}
              <div className="flex flex-wrap gap-3">
                {derivedParams.map((p) => {
                  if (p.type === "ref_table") {
                    return (
                      <div key={p.name} className="space-y-1 w-full">
                        <div className="flex items-center gap-1.5">
                          <Label className="text-[11px] text-muted-foreground font-mono">
                            {p.name} {requiredParamNames.has(p.name) && <span className="text-destructive">*</span>}
                          </Label>
                          <HelpTooltip text={t("rulesRegistry.refTableTooltip")} />
                        </div>
                        <ReferenceTableField
                          value={paramRawValues[p.name] ?? ""}
                          onChange={(fqn) => setParamRawValues((prev) => ({ ...prev, [p.name]: fqn }))}
                          disabled={readOnly}
                        />
                      </div>
                    );
                  }
                  if (p.type === "ref_column") {
                    return (
                      <div key={p.name} className="space-y-1 w-full">
                        <div className="flex items-center gap-1.5">
                          <Label className="text-[11px] text-muted-foreground font-mono">
                            {p.name} {requiredParamNames.has(p.name) && <span className="text-destructive">*</span>}
                          </Label>
                          <HelpTooltip text={t("rulesRegistry.refColumnsTooltip")} />
                        </div>
                        <ReferenceColumnsField
                          refTableFqn={paramRawValues["ref_table"] ?? ""}
                          value={paramRawValues[p.name] ?? ""}
                          onChange={(csv) => setParamRawValues((prev) => ({ ...prev, [p.name]: csv }))}
                          disabled={readOnly}
                        />
                      </div>
                    );
                  }
                  return (
                    <div key={p.name} className="space-y-1 w-40">
                      <Label className="text-[11px] text-muted-foreground font-mono">
                        {p.name} {requiredParamNames.has(p.name) && <span className="text-destructive">*</span>}
                      </Label>
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
                      ) : p.type === "number" ? (
                        <Input
                          className="h-7 w-40 text-xs"
                          type="number"
                          value={paramRawValues[p.name] ?? ""}
                          onChange={(e) => setParamRawValues((prev) => ({ ...prev, [p.name]: e.target.value }))}
                          disabled={readOnly}
                        />
                      ) : (
                        // Free-text params (string / list / regex) get a
                        // resizable field (item 17) — a plain `Input` can't be
                        // resized by the user, but a `Textarea` can (Tailwind's
                        // base styles make it vertically resizable, and we
                        // additionally allow horizontal resize since these
                        // values are often single long tokens like a regex or
                        // comma-separated list).
                        <Textarea
                          className="min-h-7 h-7 w-40 resize py-1 px-2 text-xs font-mono"
                          rows={1}
                          placeholder={p.type === "list" ? t("rulesRegistry.listPlaceholder") : undefined}
                          value={paramRawValues[p.name] ?? ""}
                          onChange={(e) => setParamRawValues((prev) => ({ ...prev, [p.name]: e.target.value }))}
                          disabled={readOnly}
                        />
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
          {functionName === "" && (
            <p className="text-[11px] text-muted-foreground italic">{t("rulesRegistry.nativeHint")}</p>
          )}
          {/* THEN THE ROW polarity switcher (item 11 + 12). Enabled only when
              the selected check accepts `negate`; otherwise frozen at "pass"
              (its inherent polarity) with an explanatory tooltip. Hidden until
              a function is chosen so the empty-state hint reads cleanly. */}
          {functionName !== "" && (
            <div className="flex flex-wrap items-center gap-3">
              <FramingWord>{t("rulesRegistry.thenTheRow")}</FramingWord>
              <PredicatePolaritySwitch
                value={nativeSupportsNegate ? polarity : "pass"}
                onChange={setPolarity}
                disabled={readOnly || !nativeSupportsNegate}
                disabledReason={!nativeSupportsNegate ? t("rulesRegistry.polaritySwitcherUnsupported") : undefined}
              />
            </div>
          )}
        </div>
      )}

      {mode === "sql" && (
        <div className="space-y-3">
          <div className="space-y-1.5">
            <Label className="text-xs">
              {t("rulesRegistry.conditionLabel")} <span className="text-destructive">*</span>
            </Label>
            {/* IF on its own line, more vertical breathing room before the
                condition logic below it than the tight label-to-control gap
                elsewhere (item 6). The SQL AI assistants (write / improve /
                explain — item 12) sit right-aligned on the IF row, directly
                above the editor, matching dqlake's ImplementationTab layout. */}
            <div className="space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <FramingWord>{t("rulesRegistry.ifCondition")}</FramingWord>
                <SqlAiAssistMenu
                  predicate={sqlPredicate}
                  slots={sqlSlots}
                  onPredicateReplace={setSqlPredicate}
                  onPolarityChange={setPolarity}
                  aiAvailability={aiAvailability}
                  disabled={readOnly}
                />
              </div>
              {/* The "reference columns as {{column_name}}" hint sits BELOW the
                  IF text (item 7), directly above the editor it explains. */}
              <PredicateEditorExplainer />
              <div className={cn(sqlError && "rounded-md ring-1 ring-red-400")}>
                <PredicateEditor
                  value={sqlPredicate}
                  onChange={setSqlPredicate}
                  declaredColumns={sqlSlots}
                  placeholder={t("rulesRegistry.sqlPredicatePlaceholder")}
                  disabled={readOnly}
                />
              </div>
            </div>
            {sqlError && (
              <p className="text-[10px] text-red-500 flex items-center gap-1">
                <AlertCircle className="h-2.5 w-2.5 shrink-0" />
                {sqlError}
              </p>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <FramingWord>{t("rulesRegistry.thenTheRow")}</FramingWord>
            <PredicatePolaritySwitch value={polarity} onChange={setPolarity} disabled={readOnly} />
          </div>
        </div>
      )}
    </div>
  );

  // Effective SQL predicate to test: the typed predicate in SQL mode, the
  // compiled AST in Low-Code mode (dqlake's draftForTest convention). DQX
  // Native rules are not testable — they show the P19-D notice instead.
  const testEffectivePredicate = mode === "sql" ? sqlPredicate.trim() : mode === "lowcode" ? lowcodePredicate : "";
  const testTabContent = (
    <div className="space-y-3 pt-2">
      {mode === "dqx_native" ? (
        <div className="rounded-lg border bg-muted/30 p-4 flex items-center gap-3">
          <FlaskConical className="h-4 w-4 text-muted-foreground shrink-0" />
          <p className="text-xs text-muted-foreground">{t("rulesRegistry.testNotAvailableDqxNative")}</p>
        </div>
      ) : lowcodeAdvanced ? (
        <div className="rounded-lg border bg-muted/30 p-4 flex items-center gap-3">
          <FlaskConical className="h-4 w-4 text-muted-foreground shrink-0" />
          <p className="text-xs text-muted-foreground">{t("rulesRegistry.testNotAvailableAdvancedLowcode")}</p>
        </div>
      ) : (
        <RuleTestPanel
          predicate={testEffectivePredicate}
          polarity={polarity}
          slots={sqlSlots}
          ruleMode={mode === "lowcode" ? "lowcode" : "sql"}
          lowcodeAdvanced={lowcodeAdvanced}
          canTest={testEffectivePredicate.length > 0 && (mode !== "sql" || sqlError === null)}
        />
      )}
    </div>
  );

  const historyTabContent = (
    <div className="space-y-4 pt-2">
      {sourceRule ? (
        <>
          {/* Published version lineage (frozen dq_rule_versions snapshots),
              newest first. Only fetched for a rule that has been published at
              least once; a never-published draft shows the empty state. */}
          <div className="space-y-2">
            <SectionHeader tooltip={t("rulesRegistry.historyVersionsTooltip")}>
              {t("rulesRegistry.historyVersionsTitle")}
            </SectionHeader>
            {sourceRule.version > 0 ? (
              versionsQuery.isLoading ? (
                <p className="text-xs text-muted-foreground flex items-center gap-2">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  {t("common.loading")}
                </p>
              ) : publishedVersions.length > 0 ? (
                <div className="rounded-lg border divide-y text-xs">
                  {publishedVersions.map((v) => (
                    <div key={v.version} className="flex items-center justify-between gap-3 p-3">
                      <span className="font-mono font-medium">
                        {t("rulesRegistry.historyVersionLabel", { version: v.version })}
                      </span>
                      <span className="text-muted-foreground">{v.created_by ?? "—"}</span>
                      <span className="text-muted-foreground">{v.created_at ?? "—"}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-muted-foreground italic">{t("rulesRegistry.historyNoData")}</p>
              )
            ) : (
              <p className="text-xs text-muted-foreground italic">{t("rulesRegistry.historyNoVersionsYet")}</p>
            )}
          </div>

          <div className="rounded-lg border divide-y text-xs">
            <div className="grid grid-cols-2 gap-2 p-3">
              <span className="text-muted-foreground">{t("rulesRegistry.historyVersion")}</span>
              <span className="font-mono">{sourceRule.version}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 p-3">
              <span className="text-muted-foreground">{t("rulesRegistry.historyCreatedBy")}</span>
              <span>{sourceRule.created_by ?? "—"}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 p-3">
              <span className="text-muted-foreground">{t("rulesRegistry.historyCreatedAt")}</span>
              <span>{sourceRule.created_at ?? "—"}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 p-3">
              <span className="text-muted-foreground">{t("rulesRegistry.historyUpdatedBy")}</span>
              <span>{sourceRule.updated_by ?? "—"}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 p-3">
              <span className="text-muted-foreground">{t("rulesRegistry.historyUpdatedAt")}</span>
              <span>{sourceRule.updated_at ?? "—"}</span>
            </div>
          </div>
        </>
      ) : (
        <p className="text-xs text-muted-foreground italic">{t("rulesRegistry.historyNoData")}</p>
      )}
    </div>
  );

  // Two-group tab strip, matching dqlake's RuleTabs: editing tabs (About /
  // Sharing / Implementation) on the left, review tabs (Test / History) on
  // the right, sharing one Tabs root so either side drives the same
  // TabsContent panels.
  // Build-with-AI banner, ported from dqlake's `BuildWithAiBanner` — always
  // visible above the tab strip (not a sub-tab), gated on AI availability.
  const buildWithAiBanner = showAiBanner && (
    <div
      className={cn(
        "ai-glow-mouse flex items-start gap-3 rounded-lg px-4 py-3 shadow-sm",
        AI_BANNER_BG,
        AI_BANNER_BORDER,
      )}
      onMouseMove={(e) => {
        const r = e.currentTarget.getBoundingClientRect();
        e.currentTarget.style.setProperty("--ai-mx", `${e.clientX - r.left}px`);
        e.currentTarget.style.setProperty("--ai-my", `${e.clientY - r.top}px`);
      }}
    >
      <Sparkles className="h-4 w-4 mt-2 shrink-0" stroke={AI_GRADIENT_URL} />
      <div className="relative flex-1">
        <Textarea
          ref={aiTextareaRef}
          rows={1}
          value={aiDescription}
          onChange={(e) => setAiDescription(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void handleAiGenerate();
            }
          }}
          disabled={aiBusy}
          maxLength={4000}
          aria-label={t("rulesRegistry.aiBuildPlaceholder")}
          className="min-h-[36px] resize-none border-0 bg-transparent dark:bg-transparent shadow-none focus-visible:ring-0 px-0 py-1.5 text-sm overflow-hidden"
        />
        {aiDescription === "" && (
          <span
            aria-hidden
            className="ai-text-shine pointer-events-none absolute left-0 top-1.5 text-sm whitespace-nowrap"
          >
            {t("rulesRegistry.aiBuildPlaceholder")}
          </span>
        )}
      </div>
      <Button
        type="button"
        size="sm"
        onClick={() => void handleAiGenerate()}
        disabled={aiBusy || !aiDescription.trim()}
        className={cn("gap-1.5", AI_BUTTON_BG, (aiBusy || !aiDescription.trim()) && "opacity-50 cursor-not-allowed")}
      >
        {aiBusy ? (
          <>
            <Loader2 className="h-3 w-3 animate-spin" />
            {t("rulesRegistry.aiGenerating")}
          </>
        ) : (
          t("rulesRegistry.aiGenerateButton")
        )}
      </Button>
    </div>
  );

  // Two-group tab strip, matching dqlake's RuleTabs: editing tabs (About /
  // Sharing / Implementation) on the left, review tabs (Test / History) on
  // the right, sharing one Tabs root so either side drives the same
  // TabsContent panels.
  const formBody = (
    <div className="space-y-4">
      <ModeSwitchDialog
        open={modeSwitch !== null}
        direction={modeSwitch?.direction ?? null}
        onCancel={() => setModeSwitch(null)}
        onConfirm={() => modeSwitch && performModeSwitch(modeSwitch.next)}
      />
      {buildWithAiBanner}
      <Tabs value={pageTab} onValueChange={(v) => setPageTab(v as PageTab)}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <TabsList>
            <TabsTrigger value="about" className="gap-1.5">
              <Info className="h-3.5 w-3.5" />
              {t("rulesRegistry.tabAbout")}
            </TabsTrigger>
            <TabsTrigger value="permissions" className="gap-1.5">
              <Shield className="h-3.5 w-3.5" />
              {t("rulesRegistry.tabPermissions")}
            </TabsTrigger>
            {/* Muted vertical rule separating the metadata group (About /
                Permissions) from Implementation (item 10) — same divider the
                MT/TS tab shells use; `muted-foreground/40` stays visible over
                TabsList's `bg-muted` in both themes. */}
            <div aria-hidden="true" className="mx-1 self-stretch w-px my-1.5 bg-muted-foreground/40" />
            <TabsTrigger value="implementation" className="gap-1.5">
              <Wrench className="h-3.5 w-3.5" />
              {t("rulesRegistry.tabImplementation")}
            </TabsTrigger>
          </TabsList>
          <TabsList>
            <TabsTrigger value="test" className="gap-1.5">
              <FlaskConical className="h-3.5 w-3.5" />
              {t("rulesRegistry.tabTest")}
            </TabsTrigger>
            <TabsTrigger value="history" className="gap-1.5" disabled={!sourceRule}>
              <HistoryIcon className="h-3.5 w-3.5" />
              {t("rulesRegistry.tabHistory")}
            </TabsTrigger>
            {/* Muted vertical rule separating Test / History from Results —
                the same divider MT/TS use, which RR was missing (item 25).
                Kept OUTSIDE the Results trigger's disabled/enabled conditional
                below so it renders in both states, and it doubles as item 77's
                "Results sits in its own group" placement. */}
            <div aria-hidden="true" className="mx-1 self-stretch w-px my-1.5 bg-muted-foreground/40" />
            {/* Results is only meaningful once the rule is applied to at
                least one monitored table (applied_to_count > 0). Until then
                the trigger is disabled; the tooltip explains why for the
                definitive not-applied state. The wrapping <span> is the
                tooltip trigger because the disabled button itself swallows
                pointer events (`disabled:pointer-events-none`). */}
            {resultsNotApplied || resultsScoreError ? (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span
                    tabIndex={0}
                    className={cn(
                      "inline-flex h-full",
                      resultsScoreError ? "cursor-pointer" : "cursor-not-allowed",
                    )}
                    aria-disabled="true"
                    // On a fetch error the wrapper doubles as the retry
                    // affordance (the disabled trigger swallows clicks).
                    onClick={resultsScoreError ? () => void ruleScoreQuery.refetch() : undefined}
                  >
                    <TabsTrigger value="results" className="gap-1.5" disabled aria-disabled="true">
                      <LineChart className="h-3.5 w-3.5" />
                      {t("rulesRegistry.tabResults")}
                    </TabsTrigger>
                  </span>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  {resultsScoreError
                    ? t("rulesRegistry.resultsScoreErrorTooltip")
                    : t("rulesRegistry.resultsNotAppliedTooltip")}
                </TooltipContent>
              </Tooltip>
            ) : (
              <TabsTrigger
                value="results"
                className="gap-1.5"
                disabled={resultsDisabled}
                aria-disabled={resultsDisabled}
              >
                <LineChart className="h-3.5 w-3.5" />
                {t("rulesRegistry.tabResults")}
              </TabsTrigger>
            )}
          </TabsList>
        </div>
        <TabsContent value="about" className="pt-4">{aboutTabContent}</TabsContent>
        <TabsContent value="permissions" className="pt-4">{permissionsTabContent}</TabsContent>
        <TabsContent value="implementation" className="pt-4">{implementationTabContent}</TabsContent>
        <TabsContent value="test" className="pt-4">{testTabContent}</TabsContent>
        <TabsContent value="history" className="pt-4">{historyTabContent}</TabsContent>
        <TabsContent value="results" className="pt-4">
          {sourceRule && <RuleResultsTab ruleId={sourceRule.rule_id} />}
        </TabsContent>
      </Tabs>
      {jsonDialogOpen && !readOnly && (
        <RegistryRuleFormJsonDialog
          open={jsonDialogOpen}
          onOpenChange={setJsonDialogOpen}
          description={t("rulesRegistry.jsonDialogDescriptionApply")}
          checkJson={buildDqxCheckJson(
            {
              ...(sourceRule ?? {}),
              mode,
              definition: buildDefinition(),
              polarity: polarityIsMeaningful ? polarity : null,
              user_metadata: buildUserMetadata(),
            } as RegistryRuleOut,
            severityValueCriticality(labelDefinitions),
          )}
          currentDefinition={buildDefinition()}
          currentUserMetadata={buildUserMetadata()}
          currentMode={mode}
          checkFunctions={checkFunctions}
          onApply={applyParsedToForm}
          aiAvailable={showAiBanner}
        />
      )}
    </div>
  );

  // When a gate (`canSaveDraft` / `canSubmit`) is the reason a Save/Submit
  // button is disabled (as opposed to `saving` or a plain "nothing changed
  // yet" `!isDirty`), wrap it so hovering/focusing explains exactly which
  // field(s) still need a value — a disabled `<button>` never fires
  // pointer/focus events, so the trigger is a focusable wrapping span
  // around it (standard Radix pattern for tooltips on disabled controls).
  const withMissingFieldsTooltip = (
    button: ReactNode,
    disabledByGate: boolean,
    labels: string[],
    tooltipKey: string,
  ) => {
    if (!disabledByGate || labels.length === 0) return button;
    return (
      <TooltipProvider delayDuration={200}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span tabIndex={0} className="inline-flex">
              {button}
            </span>
          </TooltipTrigger>
          <TooltipContent>{t(tooltipKey, { fields: labels.join(", ") })}</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    );
  };

  const footerButtons = (
    <>
      {/* Item 18: the routed detail page ("page" variant) already offers a
          breadcrumb back to the registry list, so a redundant "Close" button
          here is pure clutter when the rule is being viewed read-only. Only
          the dialog variant (which has no other way to dismiss) and the
          editable ("Cancel") case keep this button. */}
      {!(readOnly && variant === "page") && (
        <Button variant="outline" onClick={closeAndReset} disabled={saving}>
          {readOnly ? t("common.close") : t("common.cancel")}
        </Button>
      )}
      {!readOnly && (
        <>
          {/* Grey out once there's nothing to save — either a blank,
              untouched create form or an already-persisted draft with no
              pending edits (re-saving it would just churn the audit log
              with no real change), matching dqlake's steward editor.
              Draft saves only need structural validity (`canSaveDraft`);
              the completeness bar applies to the submit buttons below. */}
          {withMissingFieldsTooltip(
            <Button
              variant="secondary"
              onClick={() => handleSave(false)}
              disabled={saving || !isDirty || !canSaveDraft}
              className="gap-2"
            >
              {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {isPublishedRevision ? t("rulesRegistry.saveRevision") : t("rulesRegistry.saveDraft")}
            </Button>,
            !canSaveDraft,
            missingDraftFieldLabels,
            "rulesRegistry.canSaveMissingFieldsTooltip",
          )}
          {isEditing && !isDirty ? (
            // The draft is already persisted and unchanged — submit it
            // for approval directly rather than issuing a redundant save.
            withMissingFieldsTooltip(
              <Button onClick={handleSubmitOnly} disabled={saving || !canSubmit} className="gap-2">
                {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                {willAutoApprove
                  ? t("rulesRegistry.publishNow")
                  : isPublishedRevision
                    ? t("rulesRegistry.submitForReview")
                    : t("rulesRegistry.actionSubmit")}
              </Button>,
              !canSubmit,
              missingSubmitFieldLabels,
              "rulesRegistry.canSubmitMissingFieldsTooltip",
            )
          ) : (
            withMissingFieldsTooltip(
              <Button onClick={() => handleSave(true)} disabled={saving || !isDirty || !canSubmit} className="gap-2">
                {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                {willAutoApprove
                  ? t("rulesRegistry.saveAndPublish")
                  : isPublishedRevision
                    ? t("rulesRegistry.saveAndSubmitReview")
                    : t("rulesRegistry.saveAndSubmit")}
              </Button>,
              !canSubmit,
              missingSubmitFieldLabels,
              "rulesRegistry.canSubmitMissingFieldsTooltip",
            )
          )}
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
        {formBody}
        <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end sm:gap-2 pt-4 border-t">
          {footerButtons}
        </div>
      </div>
    );
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !saving && onOpenChange(next)}>
      <DialogContent className="max-w-2xl w-[95vw] max-h-[90vh] overflow-y-auto [scrollbar-gutter:stable]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {dialogTitle}
            {authorKindBadges}
          </DialogTitle>
          <DialogDescription className="sr-only">{t("rulesRegistry.dialogSrDescription")}</DialogDescription>
        </DialogHeader>
        {formBody}
        <DialogFooter>{footerButtons}</DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
