import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
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
  ArrowLeft,
  ArrowRight,
  ChevronDown,
  LineChart,
  FlaskConical,
  History as HistoryIcon,
  Info,
  Loader2,
  Send,
  Shield,
  Sparkles,
  Wrench,
  X,
} from "lucide-react";
import { useApprovalsMode } from "@/hooks/use-approvals-mode";
import { LabelsEditor } from "@/components/Labels";
import { HelpTooltip } from "@/components/HelpTooltip";
import { TagPicker } from "@/components/apply-rules/TagPicker";
import { PermissionsTab } from "@/components/permissions/PermissionsTab";
import { RuleResultsTab } from "@/components/registry-rules/RuleResultsTab";
import { RESULTS_QUERY_OPTIONS } from "@/lib/results-invalidation";
import { ruleResultsState } from "@/lib/results-display";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { PredicatePolaritySwitch } from "@/components/rules/PredicatePolaritySwitch";
import { PredicateEditorExplainer } from "@/components/rules/PredicateEditorExplainer";
import { PredicateEditor } from "@/components/rules/PredicateEditor";
import { AdvancedDisclosure } from "@/components/rules/AdvancedDisclosure";
import { CursorTooltip } from "@/components/rules/CursorTooltip";
import { LowcodeBuilder } from "@/components/rules/lowcode/LowcodeBuilder";
import { JoinsBuilder } from "@/components/rules/lowcode/JoinsBuilder";
import { GroupByField } from "@/components/rules/lowcode/GroupByField";
import {
  ModeSwitchDialog,
  modeSwitchDirection,
  type ModeSwitchDirection,
} from "@/components/rules/lowcode/ModeSwitchDialog";
import { RuleTestPanel } from "@/components/rules/test/RuleTestPanel";
import { useJoinedColumns } from "@/hooks/useJoinedColumns";
import {
  buildSqlBody,
  compileAstToSql,
  compileLowcodeBody,
  lowcodeHasAdvancedShape,
  slotFamilyToLowcode,
  type LowcodeColumnRef,
} from "@/lib/lowcodeCompile";
import { OPERATORS_BY_FAMILY, type Family as LowcodeFamily } from "@/lib/lowcodeOperators";
import { EMPTY_LOWCODE_AST, isV2Ast, type AnyRow, type JoinAst, type LowcodeAstV2 } from "@/lib/lowcodeAst";
import { cn } from "@/lib/utils";
import selector from "@/lib/selector";
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
  useListGovernedTags,
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
  slotTagsFromUserMetadata,
  userMetadataWithSlotTags,
  type ParsedCheckDefinition,
} from "@/lib/registry-rule-conversion";
import { RegistryRuleFormJsonDialog } from "@/components/registry-rules/RegistryRuleFormJsonDialog";
import { SqlAiAssistMenu } from "@/components/rules/SqlAiAssistMenu";

const RESERVED_NAME_KEY = "name";
const RESERVED_DESCRIPTION_KEY = "description";
const RESERVED_DIMENSION_KEY = "dimension";
const RESERVED_SEVERITY_KEY = "severity";

type RegistryMode = "dqx_native" | "lowcode" | "sql";

/** A condition-selector / change-type selection: the target authoring surface
 * plus, for a native check, the chosen function name; for a low-code choice, an
 * optional operator to seed the first condition row with, and (when the operator
 * was picked from a specific data-type group while the column was still "any")
 * the family to auto-assign to the anchor column. */
type DecisionPointChoice = {
  type: "lowcode" | "sql" | "native";
  fnName?: string;
  operator?: string;
  operatorFamily?: LowcodeFamily;
};
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

/**
 * Curated shortlist of check labels shown in the cycling placeholder.
 * These are the friendly labels as generated by the backend's `_friendly_label`
 * for the named functions, plus the two Core pseudo-options. Confirmed to exist
 * in the registry: is_not_null, is_unique, is_in_range, regex_match,
 * is_in_list, is_not_null_and_not_empty, is_valid_date.
 * Order: high-frequency nullability/uniqueness first, then range/pattern/list,
 * then compound, then date, then the two low-code choices last.
 *
 * NOTE: these are matched against fn.label at runtime so the displayed text
 * always comes from the loaded check-functions list (localized labels are served
 * by the backend). The static array here is the ordered SELECTOR — the actual
 * label strings rendered come from the matched fn.label or the i18n keys below.
 */
const DECISION_POINT_SHORTLIST_FN_NAMES: readonly string[] = [
  "is_not_null",
  "is_unique",
  "is_in_range",
  "regex_match",
  "is_in_list",
  "is_not_null_and_not_empty",
  "is_valid_date",
] as const;

/**
 * Resolve the cycling-placeholder shortlist to displayable labels. **Condition
 * Builder** and **SQL** lead the list — they carry the high draw weight in the
 * cycling animation so authors discover them — followed by example individual
 * basic-check labels (Is Not Null, Regex Match, …). We deliberately do NOT show
 * the literal words "Basic Checks"; the concrete check names hint at that path
 * instead. Falls back to title-casing the function name if the fn isn't in the
 * catalog yet. The two type labels are the FIRST two entries; the weighting
 * logic (see `shortlistWeights`) keys off that.
 */
function useDecisionPointShortlist(
  checkFunctions: ApiCheckFunctionDef[],
  t: (key: string) => string,
): string[] {
  return useMemo(() => {
    const fnLabels = DECISION_POINT_SHORTLIST_FN_NAMES.map((name) => {
      const fn = checkFunctions.find((f) => f.name === name);
      return fn?.label ?? name.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    });
    return [
      t("rulesRegistry.coreConditionBuilder"),
      t("rulesRegistry.coreSql"),
      ...fnLabels,
    ];
  }, [checkFunctions, t]);
}

/** Which drill-in view the merged condition selector is showing. */
type ConditionSelectorView = "root" | "basic" | "operators";

/** Override the cmdk group-heading styling to the ALL-CAPS framing-word style
 * used by the IF / THEN THE ROW headers (not the default sans-medium), so the
 * dropdown section headers read as section labels rather than list items. */
const COMMAND_GROUP_HEADING_CLASS =
  "[&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wider [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:text-[10px]";

/**
 * The single merged condition selector that sits in the operator-cell position
 * of the always-present `IF <column> <selector>` row. Its root offers the three
 * ways to author a rule — **Basic Checks** (native DQX check functions),
 * **Condition Builder** (low-code operators), and **SQL** — styled like the old
 * decision-point dropdown (search + sectioned + arrowed). Picking Basic Checks
 * or Condition Builder drills IN (the dropdown stays open) to that type's own
 * options — the arity-filtered native checks, or the column-family's monospace
 * operators respectively — with a back affordance returning to the root (which
 * is also how the author changes the rule type). SQL selects immediately.
 *
 * The trigger shows the CURRENT selection (monospace for a low-code operator,
 * plain label for a check / SQL) once chosen, or a weighted cycling placeholder
 * before anything is picked.
 */
function ConditionSelector({
  checkFunctions,
  currentSlots,
  onSelect,
  disabled,
  currentLabel,
  operatorFamily,
  initialView,
}: {
  checkFunctions: ApiCheckFunctionDef[];
  currentSlots: RuleSlot[];
  onSelect: (choice: DecisionPointChoice) => void;
  disabled?: boolean;
  /** The current selection's display label; undefined before anything is chosen
   * (then the cycling placeholder shows). Always rendered grey + monospace to
   * match the low-code column picker. */
  currentLabel?: string;
  /** The anchor column's low-code family — drives which monospace operators the
   * "Condition Builder" drill-in offers. */
  operatorFamily: LowcodeFamily;
  /** Which view to open in. Once a type is chosen the parent passes the matching
   * drill-in ("basic" for a native check, "operators" for Condition Builder) so
   * re-opening lands directly on that type's options instead of the root — the
   * back-arrow then returns to root to change type. Defaults to "root". */
  initialView?: ConditionSelectorView;
}) {
  const { t } = useTranslation();
  const isChanging = currentLabel !== undefined;
  const [open, setOpen] = useState(false);
  const [view, setView] = useState<ConditionSelectorView>(initialView ?? "root");
  const [query, setQuery] = useState("");
  const shouldReduceMotion = useReducedMotion();
  const shortlist = useDecisionPointShortlist(checkFunctions, t);
  const [exampleIndex, setExampleIndex] = useState(0);
  const [visible, setVisible] = useState(true);

  // On each (re)open, land in the caller's initial view — the matching drill-in
  // once a type is chosen, else the root.
  useEffect(() => {
    if (open) setView(initialView ?? "root");
  }, [open, initialView]);

  // Condition Builder operators for the anchor column, grouped by type with
  // section headings (mirrors the low-code OperatorDropdown). For a typed
  // column: the column family's own operators first, then the universal (ANY)
  // operators deduped under a second heading. For an "any"-typed column (data
  // type not yet chosen) we show EVERY data type's operators — each under its
  // own heading — so the author can pick any operator; selecting one then lets
  // them set the column's data type in "Columns used".
  // Each group carries the *family* to auto-assign to the anchor column when an
  // operator is picked from it while the column is still "any" (null family =
  // don't change the column's data type — the universal / already-typed groups).
  const operatorGroups = useMemo((): { heading: string; ops: string[]; family: LowcodeFamily | null }[] => {
    const familyHeadings: Record<LowcodeFamily, string> = {
      NUMERIC: t("rulesRegistry.slotFamilyNumeric"),
      TEXTUAL: t("rulesRegistry.slotFamilyText"),
      TEMPORAL: t("rulesRegistry.slotFamilyTemporal"),
      BOOLEAN: t("rulesRegistry.slotFamilyBoolean"),
      ANY: t("rulesRegistry.slotFamilyAny"),
    };
    if (operatorFamily === "ANY") {
      // Universal operators first (no data-type implied → family null), then
      // every typed family's operators (deduped against the universal set),
      // each under its data-type heading. Picking from a typed group
      // auto-assigns that data type to the column.
      const universal = OPERATORS_BY_FAMILY.ANY;
      const universalSet = new Set(universal);
      const groups: { heading: string; ops: string[]; family: LowcodeFamily | null }[] = [
        { heading: familyHeadings.ANY, ops: universal, family: null },
      ];
      for (const fam of ["NUMERIC", "TEXTUAL", "TEMPORAL", "BOOLEAN"] as const) {
        const ops = OPERATORS_BY_FAMILY[fam].filter((o) => !universalSet.has(o));
        if (ops.length > 0) groups.push({ heading: familyHeadings[fam], ops, family: fam });
      }
      return groups;
    }
    // Column already typed → its family's operators + a Universal group; no
    // family change on pick.
    const familyOps = OPERATORS_BY_FAMILY[operatorFamily] ?? OPERATORS_BY_FAMILY.ANY;
    const familySet = new Set(familyOps);
    const universalOps = OPERATORS_BY_FAMILY.ANY.filter((o) => !familySet.has(o));
    const groups: { heading: string; ops: string[]; family: LowcodeFamily | null }[] = [
      { heading: familyHeadings[operatorFamily], ops: familyOps, family: null },
    ];
    if (universalOps.length > 0) {
      groups.push({ heading: t("rulesRegistry.lowcodeUniversalOperators"), ops: universalOps, family: null });
    }
    return groups;
  }, [operatorFamily, t]);

  const filteredNativeFns = useMemo(() => {
    const nativeFns = checkFunctions.filter((fn) => fn.name !== "sql_expression" && fn.name !== "sql_query");
    if (currentSlots.length === 0) return nativeFns;
    const declaredCount = currentSlots.length;
    const declaredFamilies: Set<string> = new Set(currentSlots.map((s) => s.family ?? "any"));
    return nativeFns.filter((fn) => {
      const colParams = (fn.params ?? []).filter((p) => p.kind === "column" || p.kind === "columns");
      if (colParams.length === 0) return true;
      // Arity check: a list-typed column param (kind === "columns") can accept
      // any number of declared columns (≥1). A set of only single-typed params
      // requires exactly one primary declared column.
      const hasListParam = colParams.some((p) => p.kind === "columns");
      if (!hasListParam && declaredCount !== 1) return false;
      // Family check: each column param whose family is constrained must match
      // at least one of the declared slots' families (or the slots include "any").
      for (const colParam of colParams) {
        const paramFamily = colParam.family;
        if (!paramFamily || paramFamily === "any") continue;
        if (!declaredFamilies.has(paramFamily) && !declaredFamilies.has("any")) return false;
      }
      return true;
    });
  }, [checkFunctions, currentSlots]);

  const filteredByQuery = useMemo(() => {
    if (!query.trim()) return filteredNativeFns;
    const q = query.toLowerCase();
    return filteredNativeFns.filter(
      (fn) =>
        fn.name.toLowerCase().includes(q) ||
        fn.label.toLowerCase().includes(q) ||
        fn.doc?.toLowerCase().includes(q),
    );
  }, [filteredNativeFns, query]);

  const grouped = useMemo(() => apiFunctionsGrouped(filteredByQuery, ""), [filteredByQuery]);

  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);

  // Per-entry draw weights: the FIRST two shortlist entries (Condition Builder
  // / SQL) are surfaced much more prominently than the example basic checks so
  // authors discover them — they carry a high weight; every other entry
  // (individual basic-check labels) carries weight 1.
  const shortlistWeights = useMemo(
    () => shortlist.map((_, i) => (i < 2 ? 5 : 1)),
    [shortlist],
  );

  // Cycling animation: fade out → pick a WEIGHTED-RANDOM next label (never the
  // same one twice in a row) → fade in, every 2s. Stopped while the picker is
  // open or reduced-motion is requested.
  useEffect(() => {
    if (shouldReduceMotion || open) return;
    if (shortlist.length <= 1) return;
    const id = setInterval(() => {
      setVisible(false);
      setTimeout(() => {
        setExampleIndex((current) => {
          // Weighted pick over all indices except the current one (no repeat).
          const total = shortlistWeights.reduce((sum, w, i) => (i === current ? sum : sum + w), 0);
          let r = Math.random() * total;
          for (let i = 0; i < shortlistWeights.length; i++) {
            if (i === current) continue;
            r -= shortlistWeights[i];
            if (r < 0) return i;
          }
          return (current + 1) % shortlist.length; // fallback (shouldn't hit)
        });
        setVisible(true);
      }, 200);
    }, 2000);
    return () => clearInterval(id);
  }, [shouldReduceMotion, open, shortlist.length, shortlistWeights]);

  const exampleLabel = shortlist[exampleIndex] ?? "";

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        {/* Trigger styled to match the low-code OperatorDropdown's shadcn
            SelectTrigger exactly (same height / border / bg / radius / font),
            so the merged condition selector reads as the SAME control the
            low-code rows use — see components/ui/select.tsx SelectTrigger. */}
        <button
          type="button"
          disabled={disabled}
          data-slot="select-trigger"
          data-size="sm"
          className="border-input dark:bg-input/30 dark:hover:bg-input/50 focus-visible:border-ring focus-visible:ring-ring/50 flex h-8 w-full items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 font-mono text-xs whitespace-nowrap shadow-xs transition-[color,box-shadow] outline-none focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50"
        >
          {/* Monospace like the low-code operator/column controls. A COMMITTED
              selection renders in foreground (so it matches the plain operator
              dropdowns on rows 2+ and the column picker's selected value); the
              cycling PLACEHOLDER (nothing chosen yet) stays muted grey. */}
          <span
            className={cn(
              "relative flex items-center overflow-hidden",
              isChanging ? "text-foreground" : "text-muted-foreground",
            )}
          >
            {isChanging ? (
              <span className="truncate">{currentLabel}</span>
            ) : shouldReduceMotion ? (
              <span>{t("rulesRegistry.decisionPointPlaceholder")}</span>
            ) : (
              <AnimatePresence mode="wait" initial={false}>
                <motion.span
                  key={exampleIndex}
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: visible ? 1 : 0, y: visible ? 0 : -6 }}
                  exit={{ opacity: 0, y: -6 }}
                  transition={{ duration: 0.18, ease: "easeInOut" }}
                  className="whitespace-nowrap"
                >
                  {exampleLabel}
                </motion.span>
              </AnimatePresence>
            )}
          </span>
          <ChevronDown className="size-4 opacity-50 shrink-0" />
        </button>
      </PopoverTrigger>
      <PopoverContent className="p-0 w-[--radix-popover-trigger-width] min-w-72" align="start">
        <Command shouldFilter={false}>
          {/* Search only applies inside the Basic Checks drill-in (native
              functions); the root + operators views are short curated lists. */}
          {view === "basic" && (
            <CommandInput
              placeholder={t("rulesRegistry.searchFunctions")}
              value={query}
              onValueChange={setQuery}
              className="h-8 text-xs"
            />
          )}
          <CommandList className="max-h-80">
            {view === "root" && (
              // ── Root: the three ways to author a rule. Basic Checks and
              // Condition Builder drill IN (keep the dropdown open); SQL selects
              // immediately. This root is also where the author returns to CHANGE
              // the rule type, so every choice routes through the parent's
              // guarded onSelect.
              <CommandGroup>
                <CommandItem
                  value="__basic_checks__"
                  onSelect={() => {
                    setQuery("");
                    setView("basic");
                  }}
                  className="items-start gap-2 text-xs"
                >
                  <span className="min-w-0 flex-1 flex items-center justify-between gap-2">
                    <span>
                      <span className="font-semibold">{t("rulesRegistry.coreBasicChecks")}</span>
                      <span className="block text-[10px] text-muted-foreground">
                        {t("rulesRegistry.coreBasicChecksDesc")}
                      </span>
                    </span>
                    <ArrowRight className="h-3 w-3 shrink-0 text-muted-foreground" />
                  </span>
                </CommandItem>
                <CommandItem
                  value="__condition_builder__"
                  onSelect={() => setView("operators")}
                  className="items-start gap-2 text-xs"
                >
                  <span className="min-w-0 flex-1 flex items-center justify-between gap-2">
                    <span>
                      <span className="font-semibold">{t("rulesRegistry.coreConditionBuilder")}</span>
                      <span className="block text-[10px] text-muted-foreground">
                        {t("rulesRegistry.coreConditionBuilderDesc")}
                      </span>
                    </span>
                    <ArrowRight className="h-3 w-3 shrink-0 text-muted-foreground" />
                  </span>
                </CommandItem>
                <CommandItem
                  value="__sql__"
                  onSelect={() => {
                    onSelect({ type: "sql" });
                    setOpen(false);
                  }}
                  className="items-start gap-2 text-xs"
                >
                  <span className="min-w-0 flex-1 flex items-center justify-between gap-2">
                    <span>
                      <span className="font-semibold">{t("rulesRegistry.coreSql")}</span>
                      <span className="block text-[10px] text-muted-foreground">
                        {t("rulesRegistry.coreSqlDesc")}
                      </span>
                    </span>
                    <ArrowRight className="h-3 w-3 shrink-0 text-muted-foreground" />
                  </span>
                </CommandItem>
              </CommandGroup>
            )}

            {view === "basic" && (
              // ── Basic Checks drill-in: the arity-filtered native DQX checks,
              // grouped by category, searchable. Back returns to root.
              <>
                <button
                  type="button"
                  onClick={() => setView("root")}
                  className="flex w-full items-center gap-1.5 px-2 py-1.5 text-[11px] font-medium text-muted-foreground border-b hover:text-foreground"
                >
                  <ArrowLeft className="h-3 w-3 shrink-0" />
                  {t("rulesRegistry.coreBasicChecks")}
                </button>
                <CommandEmpty>
                  <span className="text-xs text-muted-foreground">{t("rulesRegistry.noMatches")}</span>
                </CommandEmpty>
                {grouped.map(([category, fns]) => (
                  <CommandGroup key={category} heading={category} className={COMMAND_GROUP_HEADING_CLASS}>
                    {fns.map((fn) => (
                      <CommandItem
                        key={fn.name}
                        value={fn.name}
                        onSelect={() => {
                          onSelect({ type: "native", fnName: fn.name });
                          setOpen(false);
                        }}
                        className="items-start gap-2 text-xs"
                      >
                        <span className="min-w-0 flex-1">
                          <span className="font-medium">{fn.label}</span>
                          {fn.doc && (
                            <span className="block text-[10px] text-muted-foreground truncate">{fn.doc}</span>
                          )}
                        </span>
                      </CommandItem>
                    ))}
                  </CommandGroup>
                ))}
              </>
            )}

            {view === "operators" && (
              // ── Condition Builder drill-in: operators for the anchor column,
              // GROUPED by type with basic-checks-style section headings —
              // the column-family's own operators first, then the universal
              // (ANY) operators under a second heading (deduped), mirroring the
              // low-code OperatorDropdown. Filtered by the column's data type
              // (via operatorFamily). Monospace like the low-code row. Picking
              // one enters low-code mode with that operator on the first row.
              <>
                <button
                  type="button"
                  onClick={() => setView("root")}
                  className="flex w-full items-center gap-1.5 px-2 py-1.5 text-[11px] font-medium text-muted-foreground border-b hover:text-foreground"
                >
                  <ArrowLeft className="h-3 w-3 shrink-0" />
                  {t("rulesRegistry.coreConditionBuilder")}
                </button>
                {operatorGroups.map(({ heading, ops, family }) => (
                  <CommandGroup key={heading} heading={heading} className={COMMAND_GROUP_HEADING_CLASS}>
                    {ops.map((op) => (
                      <CommandItem
                        key={op}
                        value={op}
                        onSelect={() => {
                          // Picking from a typed group while the column is "any"
                          // auto-assigns that data type to the anchor column.
                          onSelect({ type: "lowcode", operator: op, operatorFamily: family ?? undefined });
                          setOpen(false);
                        }}
                        className="text-xs font-mono"
                      >
                        {op}
                      </CommandItem>
                    ))}
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
 * apply-on-tag: the per-slot `class.*` tag region on a slot's header row —
 * removable chips for each declared tag plus a dashed "+ Apply to a tag"
 * button that opens a {@link TagPicker} popover. All controls
 * `stopPropagation` so interacting with tags never toggles the slot's
 * expand/collapse. When *disabled* (read-only view), chips render statically
 * with no remove affordance and no add button.
 */
function SlotTagRegion({
  tags,
  disabled,
  onAddTag,
  onRemoveTag,
}: {
  tags: string[];
  disabled?: boolean;
  onAddTag: (tag: string) => void;
  onRemoveTag: (tag: string) => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  // Governed-tag descriptions for chip hovertext. React Query dedupes this with
  // the TagPicker's own fetch, so the second call is cheap. Look up by the full
  // `key=value` string first, falling back to the bare `key` (values inherit the
  // tag's description).
  const { data: governedTagsData } = useListGovernedTags(selector());
  const descriptions = new Map<string, string | null>(
    (governedTagsData?.tags ?? []).map((g) => [g.tag, g.description ?? null]),
  );
  const describeTag = (tag: string): string | undefined => {
    const direct = descriptions.get(tag);
    if (direct) return direct;
    const eq = tag.indexOf("=");
    if (eq > 0) {
      const bare = descriptions.get(tag.slice(0, eq));
      if (bare) return bare;
    }
    return undefined;
  };
  return (
    <TooltipProvider delayDuration={200}>
    <div className="flex flex-wrap items-center gap-1" onClick={(e) => e.stopPropagation()}>
      {tags.map((tag) => {
        const description = describeTag(tag);
        const chip = (
          <span
            className="inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs font-mono bg-muted/60 border-border text-muted-foreground"
          >
            {tag}
            {!disabled && (
              <button
                type="button"
                aria-label={t("monitoredTables.slotTagRemove")}
                onClick={(e) => {
                  e.stopPropagation();
                  onRemoveTag(tag);
                }}
                className="ml-0.5 opacity-60 hover:opacity-100 focus:outline-none leading-none"
              >
                ×
              </button>
            )}
          </span>
        );
        // App-native tooltip with the governed-tag description — only when the
        // tag actually has one (many governed tags have a NULL description).
        return description ? (
          <Tooltip key={tag}>
            <TooltipTrigger asChild>{chip}</TooltipTrigger>
            <TooltipContent className="max-w-xs">{description}</TooltipContent>
          </Tooltip>
        ) : (
          <span key={tag} className="contents">
            {chip}
          </span>
        );
      })}
      {!disabled && (
        <Popover open={open} onOpenChange={setOpen}>
            <Tooltip>
              <TooltipTrigger asChild>
                <PopoverTrigger asChild>
                  <button
                    type="button"
                    onClick={(e) => e.stopPropagation()}
                    className="text-xs text-muted-foreground hover:text-foreground border border-dashed border-border rounded px-2 py-0.5"
                  >
                    {tags.length === 0
                      ? t("monitoredTables.applyToTagButton")
                      : t("monitoredTables.applyToAnotherTagButton")}
                  </button>
                </PopoverTrigger>
              </TooltipTrigger>
              <TooltipContent>{t("monitoredTables.applyToTagTooltip")}</TooltipContent>
            </Tooltip>
          <PopoverContent
            className="w-72 p-0"
            align="start"
            onClick={(e) => e.stopPropagation()}
          >
            <TagPicker
              selected={tags}
              onSelect={(tag) => {
                onAddTag(tag);
                setOpen(false);
              }}
            />
          </PopoverContent>
        </Popover>
      )}
    </div>
    </TooltipProvider>
  );
}

/**
 * Merge carried slots (from the previous authoring mode) into a function's
 * canonical signature slots when switching INTO dqx_native.
 *
 * Strategy: overlay the carried slot's name + family onto the signature's
 * positional slots — preserving whatever the author had named/typed, rather
 * than discarding it and re-seeding. Any extra carried slots beyond the
 * signature arity are appended as-is (they'll surface as filter-only extras,
 * consistent with the single-column arity UX described in B3). For each
 * matched position, `arg_key` and `position` always come from the signature
 * (they reflect the function's semantics, not the carried state), but `name`
 * and `family` come from the carried slot where available.
 *
 * When *carried* is empty (brand-new rule, never had any slots) the signature
 * slots are returned unchanged — this preserves the existing seed behaviour.
 */
function mergeCarriedSlotsIntoSignature(signature: RuleSlot[], carried: RuleSlot[]): RuleSlot[] {
  if (carried.length === 0) return signature;
  const merged: RuleSlot[] = signature.map((sigSlot, i) => {
    const c = carried[i];
    if (!c) return sigSlot;
    return {
      ...sigSlot,
      name: c.name || sigSlot.name,
      // Only carry the family when it's compatible (non-"any") and the
      // signature doesn't lock a specific family. If the signature already
      // specifies a non-"any" family, keep it — the check's semantics win.
      family: sigSlot.family !== "any" ? sigSlot.family : c.family !== "any" ? c.family : sigSlot.family,
    };
  });
  // Append any extra carried slots that exceed the signature arity. These
  // become filter-only extras from the user's perspective (consistent with B3).
  for (let i = signature.length; i < carried.length; i++) {
    merged.push({ ...carried[i], position: i, arg_key: undefined });
  }
  return merged;
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
  slotTags,
  onSlotTagsChange,
  isSingleColumnFn = false,
  addDisabledReason,
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
  /** apply-on-tag: per-slot `class.*` tag map (slot name -> tags). Chips render
   * on each slot's header row; editable exactly when `!disabled`. */
  slotTags: Record<string, string[]>;
  onSlotTagsChange: (next: Record<string, string[]>) => void;
  /** dqx_native only: when `true` the selected function has exactly one column
   * parameter, so any extra columns the author adds are filter-only — clicking
   * "+ Add column" opens an explanatory popover before proceeding. */
  isSingleColumnFn?: boolean;
  /** When set, "+ Add column" is disabled and shows this text as a tooltip —
   * used to gate adding columns until a rule condition/type is chosen. */
  addDisabledReason?: string;
}) {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState<number | null>(null);
  const [singleColPopoverOpen, setSingleColPopoverOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (expanded === null) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (rootRef.current?.contains(target)) return;
      // Ignore clicks inside any portaled Radix overlay: popper-positioned
      // content exposes `data-radix-popper-content-wrapper`, but an
      // item-aligned Select's content is portaled without that wrapper — match
      // its `data-slot="select-content"` / `role="listbox"` too so committing a
      // selection in such a Select doesn't collapse the slot editor mid-click.
      if (
        target.closest(
          '[data-radix-popper-content-wrapper], [data-slot="select-content"], [role="listbox"]',
        )
      )
        return;
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
  const canRemoveSlot = (slot: RuleSlot, index: number): boolean => {
    if (allowAddRemove) return true;
    // Single-column native fn: the first slot is the fixed input column (bound
    // to the function); any EXTRA slots the author added are filter-only and
    // freely removable — just never the fixed first one.
    if (isSingleColumnFn) return index > 0;
    return expandableArgKey !== undefined && (slot.arg_key ?? slot.name) === expandableArgKey && expandableGroupSize > 1;
  };

  const setAt = (i: number, patch: Partial<RuleSlot>) => {
    const prevName = value[i]?.name;
    const next = value.slice();
    next[i] = { ...next[i], ...patch };
    onChange(next);
    // Follow a slot rename: move its tag entry to the new name so the
    // slot -> tags map stays keyed on the live slot name (no orphan left).
    const nextName = next[i]?.name;
    if (patch.name !== undefined && prevName !== undefined && nextName !== undefined && prevName !== nextName) {
      const entry = slotTags[prevName];
      if (entry !== undefined) {
        const updated = { ...slotTags };
        delete updated[prevName];
        updated[nextName] = entry;
        onSlotTagsChange(updated);
      }
    }
  };
  const removeAt = (i: number) => {
    const removedName = value[i]?.name;
    const next = value.slice();
    next.splice(i, 1);
    onChange(next.map((s, idx) => ({ ...s, position: idx })));
    if (expanded === i) setExpanded(null);
    // Drop the removed slot's tag entry so it never dangles as an orphan.
    if (removedName !== undefined && slotTags[removedName] !== undefined) {
      const updated = { ...slotTags };
      delete updated[removedName];
      onSlotTagsChange(updated);
    }
  };
  const addTagToSlot = (slotName: string, tag: string) => {
    const existing = slotTags[slotName] ?? [];
    if (existing.includes(tag)) return;
    onSlotTagsChange({ ...slotTags, [slotName]: [...existing, tag] });
  };
  const removeTagFromSlot = (slotName: string, tag: string) => {
    const existing = slotTags[slotName] ?? [];
    const nextTags = existing.filter((tg) => tg !== tag);
    const updated = { ...slotTags };
    if (nextTags.length > 0) updated[slotName] = nextTags;
    else delete updated[slotName];
    onSlotTagsChange(updated);
  };
  const add = () => {
    const name = nextSlotName(value.map((s) => s.name));
    const arg_key = allowAddRemove ? undefined : expandableArgKey;
    onChange([...value, { name, family: "any", position: value.length, cardinality: "one", arg_key }]);
    setExpanded(value.length);
  };

  // When the function is single-column arity, clicking "+ Add column" first
  // shows an explanatory popover. Confirming (clicking the button again, or
  // clicking anywhere outside) still adds the column — extras are valid as
  // filter-only columns; the popover only explains the semantic.
  const handleAddClick = () => {
    if (isSingleColumnFn && !singleColPopoverOpen) {
      setSingleColPopoverOpen(true);
      return;
    }
    setSingleColPopoverOpen(false);
    add();
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
            addDisabledReason ? (
              // Gated: no rule condition chosen yet. Disabled + cursor-following
              // explanatory tooltip (tracks the mouse rather than pinning to the
              // button center).
              <CursorTooltip text={addDisabledReason}>
                <span className="inline-block">
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled
                    className="h-7 px-2.5 text-xs gap-1.5"
                  >
                    {t("rulesRegistry.slotsPanelAddButton")}
                  </Button>
                </span>
              </CursorTooltip>
            ) : isSingleColumnFn ? (
              <Popover open={singleColPopoverOpen} onOpenChange={setSingleColPopoverOpen}>
                <PopoverTrigger asChild>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={handleAddClick}
                    className="h-7 px-2.5 text-xs gap-1.5"
                  >
                    {t("rulesRegistry.slotsPanelAddButton")}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-72 p-3 text-xs text-muted-foreground space-y-2" side="bottom" align="end">
                  <p className="flex gap-1.5">
                    <Info className="h-3.5 w-3.5 mt-0.5 shrink-0 text-foreground/60" />
                    {t("rulesRegistry.singleColumnFnAddExplainer")}
                  </p>
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    className="h-6 text-xs w-full"
                    onClick={() => { setSingleColPopoverOpen(false); add(); }}
                  >
                    {t("rulesRegistry.slotsPanelAddButton")}
                  </Button>
                </PopoverContent>
              </Popover>
            ) : (
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
            )
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
          const removable = canRemoveSlot(slot, i);
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
                <div className="flex flex-wrap items-center gap-2 min-w-0">
                  <code className={cn("text-xs mr-1", !nameOk && "text-destructive")}>{`{{${slot.name}}}`}</code>
                  <SlotTagRegion
                    tags={slotTags[slot.name] ?? []}
                    disabled={disabled}
                    onAddTag={(tag) => addTagToSlot(slot.name, tag)}
                    onRemoveTag={(tag) => removeTagFromSlot(slot.name, tag)}
                  />
                </div>
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
                        <SelectContent position="popper">
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
  /**
   * "page" variant only: extra action controls (e.g. the routed detail
   * page's Approve/Reject buttons + the "…" actions menu) rendered inline,
   * immediately after the Save/Submit buttons, in the single top-right
   * header action row — mirroring the Monitored Table / Table Space headers
   * where the action buttons and the ⋮ menu sit together (B2-7). Ignored in
   * the dialog variant.
   */
  headerActions?: ReactNode;
  /**
   * "page" variant only: the page's title block (name + status/version
   * badges). Rendered on the LEFT of the single top-right header row so the
   * title and the Save/Submit + Approve/Reject + "…" actions share one line
   * (B2-78) — no dropped action row, no extra vertical gap above the tabs.
   * Ignored in the dialog variant (which renders its own DialogTitle).
   */
  headerTitle?: ReactNode;
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
  filter: string;
  functionName: string;
  paramRawValues: Record<string, string>;
  sqlPredicate: string;
  /** Joins declared in the SQL editor — derived body type (predicate vs sql_query)
   * is computed from join presence at save time rather than stored as a flag. */
  sqlJoins: JoinAst[];
  sqlSlots: RuleSlot[];
  nativeSlots: RuleSlot[];
  /** apply-on-tag: per-slot `class.*` tag map (slot name -> tags). Hydrated
   * from / persisted to `user_metadata.slot_tags`. */
  slotTags: Record<string, string[]>;
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
  // Restore the SQL predicate text from whichever key was used — joins are not
  // round-trippable from a raw sql_query string, so sqlJoins always starts empty
  // on load; the body type is re-derived from join presence on next save.
  const sqlPredicate = isNative
    ? ""
    : rule.mode === "sql" && typeof body.sql_query === "string"
      ? String(body.sql_query)
      : String(body.predicate ?? "");
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
    filter: rule.definition?.filter ?? "",
    functionName: isNative ? String((rule.definition?.body ?? {}).function ?? "") : "",
    paramRawValues,
    sqlPredicate,
    // Joins are not round-trippable from stored sql_query — always start empty.
    sqlJoins: [],
    sqlSlots: isNative ? [] : (rule.definition?.slots ?? []),
    nativeSlots: isNative ? (rule.definition?.slots ?? []) : [],
    slotTags: slotTagsFromUserMetadata(md),
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
  mode: "lowcode",
  polarity: "pass",
  errorMessage: "",
  filter: "",
  functionName: "",
  paramRawValues: {},
  sqlPredicate: "",
  sqlJoins: [],
  sqlSlots: [seededFirstSlot()],
  nativeSlots: [],
  slotTags: {},
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
  headerActions,
  headerTitle,
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
  /** Whether the SQL editor's optional joins card starts open — true when the user
   * picked "Cross-Table SQL" from the decision-point menu so the joins card is
   * immediately visible without an extra click. */
  const [sqlJoinsDefaultOpen, setSqlJoinsDefaultOpen] = useState(false);
  const [decisionPointChosen, setDecisionPointChosen] = useState(false);
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
  /** Joins declared in the SQL editor. An empty array = single-table SQL (predicate);
   * one or more joins = cross-table SQL (sql_query) — derived at save time via
   * compileJoinsToSql, no separate body-type flag needed. */
  const [sqlJoins, setSqlJoins] = useState<JoinAst[]>([]);
  const [sqlSlots, setSqlSlots] = useState<RuleSlot[]>([]);
  // Low-Code authoring state (shares `sqlSlots` for its "Columns used"
  // placeholders — the row/aggregate pickers bind to the same declared
  // slots). `lowcodeAst` holds the row stack + joins; `groupBy` is the raw
  // group-by SQL string (advanced). Both are persisted under the rule's body
  // (`lowcode_ast` / `group_by`) for re-editing, alongside the compiled SQL.
  const [lowcodeAst, setLowcodeAst] = useState<LowcodeAstV2>(EMPTY_LOWCODE_AST);
  const [groupBy, setGroupBy] = useState("");
  // A guarded rule-type change awaiting confirmation. `choice` is the
  // decision-point selection (target mode + optional native fn) applied by
  // performModeSwitch when the author confirms.
  const [modeSwitch, setModeSwitch] = useState<{ direction: ModeSwitchDirection; choice: DecisionPointChoice } | null>(
    null,
  );
  // item 5: cache the last Low-Code AST + group-by when leaving Low-Code for
  // SQL, so switching back RESTORES the built conditions instead of a blank
  // stack (covers flitting between the two). The cache is honored only when the
  // SQL still matches what the cached AST compiles to; if the SQL was
  // hand-edited away from that, the built conditions can't be reconstructed —
  // the cache is dropped and the builder is cleared with a warning.
  const lowcodeCacheRef = useRef<{ ast: LowcodeAstV2; groupBy: string } | null>(null);
  // CRIT-2: non-null while the editor holds a rule loaded as a cross-table
  // sql_query. Joins aren't round-trippable from a raw sql_query string, so such
  // a rule reopens with sqlJoins=[] and its whole SELECT sitting in the
  // predicate editor; without this marker, buildDefinition would re-emit the
  // SELECT as { predicate }, flipping a valid sql_query into a broken
  // sql_expression. While set, buildSqlBody persists the CURRENT predicate text
  // as sql_query (so edits are saved, not corrupted), preserving these
  // merge_columns. Reset to null on every (re)open of the dialog and on any
  // rule-type change (applyChoice).
  const loadedSqlQueryRef = useRef<{ merge_columns?: string[] } | null>(null);
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
  // apply-on-tag: per-slot `class.*` tag map (slot name -> tags), shared across
  // all authoring modes' "Columns used" panel. Round-trips through
  // `user_metadata.slot_tags` (see buildUserMetadata / snapshotFromRule).
  const [slotTags, setSlotTags] = useState<Record<string, string[]>>({});
  const [errorMessage, setErrorMessage] = useState("");
  const [filter, setFilter] = useState("");
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
  // Typed column slots carried by an AI proposal (item B2-32), stashed
  // alongside `pendingNativeArgs` and flushed once the function catalog
  // resolves. Preserves the model's chosen slot names/families instead of
  // re-seeding canonical `column_N` slots. `null` = no AI slots to apply.
  const [pendingNativeSlots, setPendingNativeSlots] = useState<RuleSlot[] | null>(null);
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
    // CRIT-2: cleared here for every (re)open; set below only when loading a
    // stored cross-table sql_query rule.
    loadedSqlQueryRef.current = null;

    const md = (sourceRule?.user_metadata ?? {}) as Record<string, unknown>;
    const asString = (k: string) => (typeof md[k] === "string" ? (md[k] as string) : "");
    setName(asString(RESERVED_NAME_KEY));
    setDescription(asString(RESERVED_DESCRIPTION_KEY));
    setDimension(asString(RESERVED_DIMENSION_KEY));
    setSeverity(asString(RESERVED_SEVERITY_KEY));
    setSteward(sourceRule?.steward ?? "");
    setNameError(null);
    setErrorMessage(sourceRule?.definition?.error_message ?? "");
    setFilter(sourceRule?.definition?.filter ?? "");
    setAiDescription("");
    setPendingNativeArgs(null);
    setPendingNativeSlots(null);
    const freeTags: Record<string, string> = {};
    for (const [k, v] of Object.entries(md)) {
      if (k === RESERVED_NAME_KEY || k === RESERVED_DESCRIPTION_KEY || k === RESERVED_DIMENSION_KEY || k === RESERVED_SEVERITY_KEY) continue;
      if (typeof v === "string") freeTags[k] = v;
    }
    setTags(freeTags);
    // apply-on-tag: hydrate the slot -> tags map (nested object, ignored by the
    // string-only `freeTags` loop above). Empty for a brand-new rule.
    setSlotTags(sourceRule ? slotTagsFromUserMetadata(md) : {});

    if (sourceRule) {
      setDecisionPointChosen(true);
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
        // Restore the SQL predicate text — if a cross-table rule was stored as
        // sql_query, restore that text; otherwise restore predicate. Joins are
        // not round-trippable from a raw sql_query string, so sqlJoins always
        // resets to empty on load (body type is re-derived from join presence at save).
        if (sourceRule.mode === "sql") {
          if (typeof body.sql_query === "string") {
            // CRIT-2: mark the editor as holding a loaded cross-table sql_query
            // so re-saving persists the (possibly edited) text as sql_query
            // rather than mis-emitting { predicate: <full SELECT> }. Preserve
            // merge_columns — dropping them would flip the runtime from a
            // row-level merge to a dataset-level single-row query.
            const mergeCols = Array.isArray(body.merge_columns)
              ? body.merge_columns.filter((c): c is string => typeof c === "string")
              : undefined;
            loadedSqlQueryRef.current = {
              merge_columns: mergeCols && mergeCols.length > 0 ? mergeCols : undefined,
            };
          }
          setSqlPredicate(
            typeof body.sql_query === "string"
              ? body.sql_query
              : typeof body.predicate === "string"
                ? body.predicate
                : "",
          );
        } else {
          setSqlPredicate(typeof body.predicate === "string" ? body.predicate : "");
        }
        setSqlJoins([]);
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
      setDecisionPointChosen(false);
      setAuthorKind("human");
      setMode("lowcode");
      setPageTab("about");
      setFunctionName("");
      setParamRawValues({});
      setSqlPredicate("");
      setSqlJoins([]);
      setSqlSlots([seededFirstSlot()]);
      setNativeSlots([]);
      setLowcodeAst(EMPTY_LOWCODE_AST);
      setGroupBy("");
      setPolarity("pass");
      setFilter("");
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
      // The AI named a function that isn't in the frontend catalog (e.g. an
      // optional module not installed on this deployment). Don't silently drop
      // everything: still surface the AI's raw scalar arguments (best-effort)
      // and any typed slots it carried, then warn the steward that the function
      // couldn't be matched so they can pick a real one. Column-slot arguments
      // are placeholders, so only the non-`{{…}}` scalar args are shown.
      const raw: Record<string, string> = {};
      for (const [key, v] of Object.entries(pendingNativeArgs)) {
        if (v === undefined || v === null) continue;
        const rendered = Array.isArray(v) ? v.join(", ") : String(v);
        if (/^\{\{.*\}\}$/.test(rendered.trim())) continue;
        raw[key] = rendered;
      }
      setParamRawValues(raw);
      if (pendingNativeSlots && pendingNativeSlots.length > 0) setNativeSlots(pendingNativeSlots);
      setPendingNativeArgs(null);
      setPendingNativeSlots(null);
      toast.warning(t("rulesRegistry.aiFunctionUnavailable"));
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
    // Prefer the AI proposal's typed column slots (author-meaningful names +
    // families locked to the check's semantics — item B2-32). Fall back to the
    // canonical `column_N` slots derived from the function signature only when
    // the proposal carried none.
    setNativeSlots(pendingNativeSlots && pendingNativeSlots.length > 0 ? pendingNativeSlots : fnDerivedSlots);
    setPendingNativeArgs(null);
    setPendingNativeSlots(null);
  }, [pendingNativeArgs, pendingNativeSlots, checkFunctions, selectedFn, derivedParams, fnDerivedSlots, t]);

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
    filter,
    functionName,
    paramRawValues,
    sqlPredicate,
    sqlJoins,
    sqlSlots,
    nativeSlots,
    slotTags,
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

  // -- Unused-slot detection (B5) -------------------------------------------
  // Derive which declared slots are NOT referenced anywhere in the effective
  // rule body + filter. A slot is "used" when:
  //   Native: its {{slotName}} placeholder appears in the serialized nativeArguments
  //           output (all slots bound to a function parameter) OR in the filter.
  //   SQL:    {{slotName}} appears in sqlPredicate, a join's column_ref (plain
  //           slot refs, not qualified joined-table refs), OR the filter.
  //   Lowcode: slotName appears as a plain column_ref in rows/joins, OR
  //            {{slotName}} appears in groupBy or the filter.
  //
  // Conservative: we only block when we're confident a slot is unreferenced.
  // The check runs on the effective current slots for the active mode.
  const unusedSlots = useMemo((): RuleSlot[] => {
    if (mode === "dqx_native") {
      if (!functionName.trim()) return []; // no function yet — can't classify
      const args = nativeArguments(nativeSlots, selectedFn);
      // Serialize the args values to a flat string to search for {{slotName}}
      const serialized = JSON.stringify(args);
      return nativeSlots.filter((s) => {
        if (serialized.includes(`{{${s.name}}}`)) return false;
        if (filter.includes(`{{${s.name}}}`)) return false;
        return true;
      });
    }
    if (mode === "sql") {
      // Join column_refs: plain (no dot) refs are slot names
      const joinRefs = new Set<string>();
      for (const j of sqlJoins) {
        for (const k of j.keys ?? []) {
          if (k.column_ref && !k.column_ref.includes(".")) joinRefs.add(k.column_ref);
        }
      }
      return sqlSlots.filter((s) => {
        if (sqlPredicate.includes(`{{${s.name}}}`)) return false;
        if (joinRefs.has(s.name)) return false;
        if (filter.includes(`{{${s.name}}}`)) return false;
        return true;
      });
    }
    if (mode === "lowcode") {
      // Collect all plain (non-qualified) column_ref values used in the AST
      const astRefs = new Set<string>();
      for (const row of lowcodeAst.rows) {
        if (row.column_ref && !row.column_ref.includes(".")) astRefs.add(row.column_ref);
        // aggregated rows may also reference a nested column_ref in value
        if (row.kind === "aggregated") {
          const v = row.value;
          if (v && typeof v === "object" && !Array.isArray(v)) {
            const nested = (v as Record<string, unknown>).column_ref;
            if (typeof nested === "string" && !nested.includes(".")) astRefs.add(nested);
          }
        }
      }
      for (const j of lowcodeAst.joins) {
        for (const k of j.keys ?? []) {
          if (k.column_ref && !k.column_ref.includes(".")) astRefs.add(k.column_ref);
        }
      }
      return sqlSlots.filter((s) => {
        if (astRefs.has(s.name)) return false;
        if (groupBy.includes(`{{${s.name}}}`)) return false;
        if (filter.includes(`{{${s.name}}}`)) return false;
        return true;
      });
    }
    return [];
  }, [mode, functionName, nativeSlots, selectedFn, sqlSlots, sqlPredicate, sqlJoins, lowcodeAst, groupBy, filter]);

  const structurallyValid =
    mode === "dqx_native"
      ? functionName.trim().length > 0 && slotsHaveValidNames(nativeSlots)
      : mode === "sql"
        ? sqlPredicate.trim().length > 0 && sqlError === null && slotsHaveValidNames(sqlSlots)
        : lowcodePredicate.length > 0 && slotsHaveValidNames(sqlSlots);
  const canSaveDraft = name.trim().length > 0 && !nameError && structurallyValid && unusedSlots.length === 0;
  const canSubmit =
    canSaveDraft &&
    severity.trim().length > 0 &&
    dimension.trim().length > 0 &&
    (mode !== "dqx_native" || nativeRequiredParamsFilled);
  // An already-approved rule with no unsaved edits has nothing to resubmit —
  // resubmitting would just re-run the (already-passed) approval gate. Disable
  // Submit in that state, matching the Monitored Table / Table Space headers
  // (ProductHeader's `submitDisabledNoChanges`). Editing it flips `isDirty`
  // true and re-enables Submit so an approved rule can still be revised.
  const submitDisabledNoChanges = sourceRule?.status === "approved" && !isDirty;

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
  if (unusedSlots.length > 0)
    missingDraftFieldLabels.push(
      t("rulesRegistry.unusedColumnsError", {
        columns: unusedSlots.map((s) => `{{${s.name}}}`).join(", "),
      }),
    );
  const missingSubmitFieldLabels: string[] = [...missingDraftFieldLabels];
  if (!severity.trim()) missingSubmitFieldLabels.push(t("rulesRegistry.severityLabel"));
  if (!dimension.trim()) missingSubmitFieldLabels.push(t("rulesRegistry.dimensionLabel"));
  if (mode === "dqx_native" && functionName.trim() && !nativeRequiredParamsFilled)
    missingSubmitFieldLabels.push(t("rulesRegistry.requiredParametersLabel"));

  const buildDefinition = (): RuleDefinition => {
    const trimmedError = errorMessage.trim();
    const trimmedFilter = filter.trim();
    if (mode === "sql") {
      // Body type is derived from join presence — mirrors compileLowcodeBody's
      // own classification: no joins → { predicate }; joins present → compile
      // the predicate + joins into { sql_query, merge_columns? }.
      // The runner dispatches on these keys; getting it wrong silently breaks execution.
      //
      // CRIT-2: a cross-table rule loaded from a stored sql_query reopens with
      // sqlJoins=[] (joins aren't round-trippable from raw SQL) and the whole
      // SELECT sitting in the predicate editor. While the rule is still loaded as
      // a cross-table query (loadedSqlQueryRef set), buildSqlBody persists the
      // CURRENT editor text as sql_query — so editing + re-saving keeps it a
      // valid sql_query (preserving merge_columns) instead of flipping it into a
      // broken sql_expression. The ref is cleared on a rule-TYPE change
      // (applyChoice), so an intentional conversion to another mode is honoured;
      // re-declaring joins recompiles regardless.
      const sqlBody = buildSqlBody({
        sqlPredicate,
        sqlJoins,
        sqlQueryPassthrough: loadedSqlQueryRef.current,
      });
      return {
        body: sqlBody as Record<string, unknown>,
        slots: sqlSlots,
        parameters: [],
        error_message: trimmedError || undefined,
        filter: trimmedFilter || undefined,
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
        filter: trimmedFilter || undefined,
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
      filter: trimmedFilter || undefined,
    };
  };

  const buildUserMetadata = (): Record<string, unknown> => {
    const md: Record<string, unknown> = { ...tags };
    if (name.trim()) md[RESERVED_NAME_KEY] = name.trim();
    if (description.trim()) md[RESERVED_DESCRIPTION_KEY] = description.trim();
    if (dimension) md[RESERVED_DIMENSION_KEY] = dimension;
    if (severity) md[RESERVED_SEVERITY_KEY] = severity;
    // apply-on-tag: persist the slot -> tags map, pruning any orphan entry that
    // no longer references a declared slot (e.g. a slot the author removed or
    // renamed) so the stored `slot_tags` never keys off a non-existent slot.
    const currentSlotNames = new Set((mode === "dqx_native" ? nativeSlots : sqlSlots).map((s) => s.name));
    const prunedSlotTags: Record<string, string[]> = {};
    for (const [slot, slotTagList] of Object.entries(slotTags)) {
      if (currentSlotNames.has(slot)) prunedSlotTags[slot] = slotTagList;
    }
    return userMetadataWithSlotTags(md, prunedSlotTags);
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
    setFilter(def.filter ?? "");
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
    // apply-on-tag: mirror the string-tag rehydration for the slot -> tags map.
    // `parsed.userMetadata` is string-only (the JSON parser drops non-string
    // values), so a JSON round-trip that omits `slot_tags` clears the map —
    // consistent with how the JSON surface treats any non-string metadata.
    setSlotTags(slotTagsFromUserMetadata(parsed.userMetadata));
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
    const isLowcodeProposal = proposal.mode === "lowcode";
    const isSqlProposal =
      !isLowcodeProposal && (proposal.mode === "sql" || nativeFn === "sql_query" || nativeFn === "sql_expression");
    const appliedMode: RegistryMode = isLowcodeProposal ? "lowcode" : isSqlProposal ? "sql" : "dqx_native";
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

    if (isLowcodeProposal) {
      // A low-code proposal carries the re-editable AST (+ optional group-by)
      // the visual builder rehydrates from, plus the declared {{slot}} columns
      // and a PASS/FAIL polarity — load them straight into the low-code editor.
      const storedAst = body.lowcode_ast;
      setLowcodeAst(isV2Ast(storedAst) ? storedAst : EMPTY_LOWCODE_AST);
      setGroupBy(typeof body.group_by === "string" ? body.group_by : "");
      setSqlSlots(proposal.slots ?? []);
      setPolarity(proposal.polarity === "fail" ? "fail" : "pass");
      setSqlPredicate("");
      setFunctionName("");
      setParamRawValues({});
      setPendingNativeArgs(null);
      setPendingNativeSlots(null);
    } else if (isSqlProposal) {
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
      setPendingNativeSlots(null);
    } else {
      setFunctionName(nativeFn);
      setSqlPredicate("");
      const args =
        body.arguments && typeof body.arguments === "object"
          ? (body.arguments as Record<string, unknown>)
          : {};
      setPendingNativeArgs(args);
      // Stash the proposal's typed column slots (if any) to flush alongside the
      // args once the function catalog resolves (item B2-32).
      setPendingNativeSlots(proposal.slots ?? null);
    }
    // NOTE: intentionally do NOT clear `aiDescription` here (item B2-32 Part B) —
    // the steward's typed prompt survives after a generation so they can tweak
    // and regenerate without retyping.
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
        <Label>
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
        <Label>{t("rulesRegistry.descriptionLabel")}</Label>
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
          <Label>
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
          <Label>
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
  // Whether the author has committed to a rule condition/type yet. Until then,
  // "+ Add column" and the "Advanced" section are gated (disabled + tooltip) —
  // there's no rule to add filter columns to or configure advanced options for.
  // Native additionally requires a function actually selected.
  const conditionChosen = decisionPointChosen && (mode !== "dqx_native" || functionName.trim().length > 0);
  // Human label for the CURRENTLY-selected rule type, shown on the persistent
  // "condition picker" chip after a type has been chosen (the entry point the
  // author returns to — via the chip's dropdown back affordance — to change
  // the rule type). Native shows the selected function's friendly label.
  const currentTypeLabel =
    mode === "sql"
      ? t("rulesRegistry.coreSql")
      : mode === "lowcode"
        ? t("rulesRegistry.coreConditionBuilder")
        : selectedFn?.label || functionName || t("rulesRegistry.modeDqxNative");
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

  // The low-code family of the ANCHOR column (the first declared slot) — drives
  // which monospace operators the merged selector's Condition Builder drill-in
  // offers. Falls back to ANY when nothing is declared yet.
  const anchorOperatorFamily: LowcodeFamily = currentSlots[0]
    ? slotFamilyToLowcode(currentSlots[0].family)
    : "ANY";

  // The target authoring mode a decision-point / change-type *choice* lands in.
  // Both "Cross-Table SQL" (type "sql") and "Single Table SQL"
  // (native "sql_expression") share the sql-mode editor.
  const choiceTargetMode = (choice: DecisionPointChoice): RegistryMode =>
    choice.type === "lowcode"
      ? "lowcode"
      : choice.type === "sql"
        ? "sql"
        : choice.fnName === "sql_expression"
          ? "sql"
          : "dqx_native";

  // Whether the CURRENT mode holds real authored content that a switch to a
  // different mode couldn't preserve losslessly — drives the guarded confirm
  // dialog (see requestModeChange). A blank editor switches silently.
  const currentModeHasContent = (): boolean => {
    if (mode === "dqx_native") return functionName.trim().length > 0;
    if (mode === "sql") return sqlPredicate.trim().length > 0;
    return lowcodeAst.rows.some((r) => (r.column_ref ?? "").trim().length > 0);
  };

  // Auto-assign a data type to the ANCHOR (first) low-code column when the
  // author picked an operator from a typed group while the column was still
  // "any" — so the column's family follows the operator they chose.
  const applyOperatorFamilyToAnchor = (family: LowcodeFamily) => {
    const slotFamily: RuleSlotFamilyType = (
      { NUMERIC: "numeric", TEXTUAL: "text", TEMPORAL: "temporal", BOOLEAN: "boolean", ANY: "any" } as const
    )[family];
    setSqlSlots((prev) => {
      if (prev.length === 0 || prev[0].family === slotFamily) return prev;
      const next = prev.slice();
      next[0] = { ...next[0], family: slotFamily };
      return next;
    });
  };

  // Apply a rule-type *choice*: land in the target mode, translating or
  // carrying over as much authored state as the transition allows (ported from
  // dqlake's ModeSwitchDialog, generalized to the three DQX modes + the native
  // function picker). Shared by the first-time decision point and the guarded
  // change-type re-pick so both stay byte-identical.
  const applyChoice = (choice: DecisionPointChoice) => {
    const next = choiceTargetMode(choice);
    // CRIT-2: a rule-TYPE change abandons the loaded cross-table sql_query body
    // (its SELECT is being replaced by a fresh native/lowcode/single-table
    // surface), so drop the passthrough — a later switch back to SQL must build
    // from the new state, not resurrect the old query.
    loadedSqlQueryRef.current = null;
    // Low-Code -> SQL: translate the built rows + joins into the SQL editor
    // so the author keeps their work instead of retyping it, and cache the
    // AST + group-by so switching back can restore it (item 5).
    if (mode === "lowcode" && next === "sql") {
      lowcodeCacheRef.current = { ast: lowcodeAst, groupBy };
      const predicate = compileAstToSql(lowcodeAst);
      if (predicate) setSqlPredicate(predicate);
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
      // Leaving native for SQL/Low-Code is a body-type change: clear the native
      // function selection so a stale function name can't leak into the saved rule.
      setFunctionName("");
      setParamRawValues({});
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
    // Column carry-over: leaving native → sql/lowcode: copy nativeSlots →
    // sqlSlots so the "Columns used" panel is unchanged. Clear the native
    // function selection too — a body-type change must not leave a stale
    // functionName behind (the sql branch below clears it as well; this covers
    // the native → lowcode fall-through path).
    if (mode === "dqx_native" && (next === "sql" || next === "lowcode")) {
      if (nativeSlots.length > 0) setSqlSlots(nativeSlots.map((s, i) => ({ ...s, arg_key: undefined, position: i })));
      setFunctionName("");
      setParamRawValues({});
    }
    if (next === "sql") {
      // A fresh SQL surface: pick between Single Table (sql_expression) and the
      // Cross-Table joins card. Clear the native function selection so it never
      // leaks into the saved sql-mode rule.
      setFunctionName("");
      setParamRawValues({});
      // Reset the SQL editor to empty when entering it from NATIVE, so a stale
      // predicate/joins can't survive a change-away-then-back sequence and
      // resave as a broken rule (CRIT-2 residual — a loaded cross-table SELECT
      // left in sqlPredicate would otherwise re-emit as { predicate }). The
      // Low-Code -> SQL branch above intentionally seeds sqlPredicate with the
      // compiled predicate, so it's excluded; sql -> sql is a no-op blocked by
      // requestModeChange upstream.
      if (mode === "dqx_native") {
        setSqlPredicate("");
        setSqlJoins([]);
      }
      setSqlJoinsDefaultOpen(false);
      setMode("sql");
      setModeSwitch(null);
      return;
    }
    if (next === "dqx_native" && choice.type === "native" && choice.fnName) {
      setFunctionName(choice.fnName);
      setParamRawValues({});
      setPolarity("pass");
      const nextFn = checkFunctions.find((f) => f.name === choice.fnName);
      const { slots: sigSlots } = deriveSlotsAndParameters(nextFn);
      // Merge sqlSlots into the function's signature so the "Columns used" panel
      // retains the author's column names/tags rather than blindly re-seeding.
      setNativeSlots(mergeCarriedSlotsIntoSignature(sigSlots, sqlSlots));
      setMode("dqx_native");
      setModeSwitch(null);
      return;
    }
    // Condition Builder with a chosen operator (from the merged selector's
    // operators drill-in): ensure a first row exists and stamp the operator +
    // the anchor column onto it, so picking "is null" lands the author on a
    // ready row rather than a blank builder.
    if (next === "lowcode" && choice.operator) {
      const anchorCol = lowcodeColumns[0]?.name ?? currentSlots[0]?.name ?? "column_1";
      setLowcodeAst((prev) => {
        const rows = prev.rows.length > 0 ? prev.rows.slice() : [seededFirstLowcodeRow(anchorCol)];
        rows[0] = { ...rows[0], kind: "row", column_ref: rows[0]?.column_ref || anchorCol, operator: choice.operator! };
        return { ...prev, rows };
      });
      if (choice.operatorFamily) applyOperatorFamilyToAnchor(choice.operatorFamily);
    }
    setMode(next);
    setModeSwitch(null);
  };

  // Apply the confirmed guarded switch.
  const performModeSwitch = (choice: DecisionPointChoice) => applyChoice(choice);

  // First-time decision point (create flow): no prior content, so no guard.
  const handleDecisionPointSelect = (choice: DecisionPointChoice) => {
    setDecisionPointChosen(true);
    applyChoice(choice);
  };

  // Re-pick after the decision point (change rule type). Guarded: if the
  // current mode holds content the target can't preserve, confirm first;
  // otherwise switch immediately. A no-op re-pick of the same mode is ignored.
  const requestModeChange = (choice: DecisionPointChoice) => {
    const next = choiceTargetMode(choice);
    // Already in Low-Code and the author just picked an operator from the merged
    // selector: that's a plain operator change on the first row, NOT a rule-type
    // switch — apply it directly (no guarded confirm).
    if (next === "lowcode" && mode === "lowcode" && choice.operator) {
      setLowcodeAst((prev) => {
        if (prev.rows.length === 0) return prev;
        const rows = prev.rows.slice();
        rows[0] = { ...rows[0], operator: choice.operator! } as AnyRow;
        return { ...prev, rows };
      });
      if (choice.operatorFamily) applyOperatorFamilyToAnchor(choice.operatorFamily);
      return;
    }
    // Re-selecting the same native function (or the same non-native surface with
    // no fn change) is a no-op — don't reset the editor.
    if (next === mode && !(next === "dqx_native" && choice.fnName && choice.fnName !== functionName)) {
      return;
    }
    const direction = modeSwitchDirection(mode, next, currentModeHasContent());
    if (direction === null) {
      applyChoice(choice);
      return;
    }
    setModeSwitch({ direction, choice });
  };

  const implementationTabContent = (
    // `w-full` pins this tab's content to the tab strip's stable width
    // regardless of which mode's fields it's currently rendering (DQX
    // Native / Low-Code / SQL each have different intrinsic content), so
    // switching Rule Type doesn't visibly shift the panel left/right.
    <div className="w-full space-y-4 pt-2">
      {/* "Columns used" ALWAYS leads the Implementation area — from the very
          start, before any rule type is chosen (item: always show the panel +
          let authors "+ Add column" up front). It renders for SQL / Low-Code
          unconditionally and for DQX Native when the selected function has
          column-kind params; the default create mode is Low-Code, so a
          brand-new rule shows the seeded `{{column_1}}` slot immediately. */}
      {showColumnsUsedPanel && (
        <SlotsPanel
          value={currentSlots}
          onChange={setCurrentSlots}
          disabled={readOnly}
          allowAddRemove={mode !== "dqx_native"}
          expandableArgKey={mode === "dqx_native" ? nativeExpandableArgKey : undefined}
          lockFamily={mode === "dqx_native"}
          slotTags={slotTags}
          onSlotTagsChange={setSlotTags}
          isSingleColumnFn={mode === "dqx_native" && fnDerivedSlots.length === 1 && !nativeExpandableArgKey}
          addDisabledReason={conditionChosen ? undefined : t("rulesRegistry.advancedGatedTooltip")}
        />
      )}

      {/* Condition — ALWAYS shown (item: the "Condition" header appears from
          the start). Before a rule type is chosen, the section is just the
          entry-point row `IF {{column_1}} [type/function picker]`, reusing the
          low-code IF framing. Once a type is chosen the entry row is replaced
          by a compact "current type" picker chip (whose dropdown carries the
          "← change rule type" back affordance) followed by the chosen mode's
          body below. */}
      <div className="space-y-2">
        <Label>{t("rulesRegistry.conditionLabel")}</Label>
        {/* Unified top row — the low-code condition-row chrome
            (`IF [column ▾] [selector] …`) is reused for EVERY rule type. The
            operator cell is the merged ConditionSelector: a cycling rule-type
            picker before anything is chosen, the native-checks list once a
            basic check is chosen, or (in Condition Builder) the operators list.
            SQL and Low-Code render their own bodies below; native shows its
            parameters below. This standalone anchor renders for the UNCHOSEN,
            NATIVE and SQL states — Low-Code uses LowcodeBuilder (same chrome),
            whose first row hosts the selector. */}
        {(!decisionPointChosen || mode === "dqx_native") && (
          <div className="grid max-w-2xl grid-cols-[80px_minmax(0,1fr)_minmax(0,1fr)] gap-2 items-center py-1">
            <div className="flex items-center h-8 pl-2 justify-self-start">
              {/* Same IF styling as LowcodeRow's inline IF so the framing word is
                  identical in size across native / low-code / SQL. */}
              <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                {t("rulesRegistry.ifCondition")}
              </span>
            </div>
            {/* Column cell — styled like the low-code column Select but DISABLED.
                Before a type is chosen it explains that a condition must be
                picked first; once a native basic check is chosen it explains
                that the input column is governed by "Columns used" above (a
                basic check takes a single input column). Cursor-following tooltip. */}
            <CursorTooltip
              text={
                conditionChosen
                  ? t("rulesRegistry.nativeSingleColumnTooltip")
                  : t("rulesRegistry.advancedGatedTooltip")
              }
            >
              <span className="inline-block w-full">
                <button
                  type="button"
                  disabled
                  data-slot="select-trigger"
                  className="border-input dark:bg-input/30 flex h-8 w-full items-center justify-between gap-2 rounded-md border bg-transparent px-3 py-1 font-mono text-xs whitespace-nowrap shadow-xs opacity-70 cursor-not-allowed"
                >
                  <span className="truncate">
                    {currentSlots[0] ? `{{${currentSlots[0].name}}}` : "{{column_1}}"}
                  </span>
                  <ChevronDown className="size-4 opacity-50 shrink-0" />
                </button>
              </span>
            </CursorTooltip>
            <ConditionSelector
              checkFunctions={checkFunctions}
              currentSlots={currentSlots}
              operatorFamily={anchorOperatorFamily}
              onSelect={decisionPointChosen ? requestModeChange : handleDecisionPointSelect}
              disabled={readOnly}
              currentLabel={decisionPointChosen ? currentTypeLabel : undefined}
              // In native mode (a chosen basic check) re-open straight into the
              // Basic Checks list; SQL mode has no submenu so root is fine.
              initialView={decisionPointChosen && mode === "dqx_native" ? "basic" : "root"}
            />
          </div>
        )}
        {/* Low-Code builder renders INSIDE the Condition section (directly under
            the header, same as the anchor row above) so the IF row sits at the
            identical vertical position across every rule type. Its first row's
            operator cell is the merged ConditionSelector (escalate / change
            type). Advanced + THEN THE ROW follow as their own block below. */}
        {decisionPointChosen && mode === "lowcode" && (
          <LowcodeBuilder
            ast={lowcodeAst}
            onChange={setLowcodeAst}
            declaredColumns={lowcodeColumns}
            readOnly={readOnly}
            firstRowOperatorSlot={
              <ConditionSelector
                checkFunctions={checkFunctions}
                currentSlots={currentSlots}
                operatorFamily={anchorOperatorFamily}
                onSelect={requestModeChange}
                disabled={readOnly}
                currentLabel={lowcodeAst.rows[0]?.operator || t("rulesRegistry.coreConditionBuilder")}
                // Already in Condition Builder: re-open straight into the
                // operators list (back-arrow returns to root to change type).
                initialView="operators"
              />
            }
          />
        )}
      </div>

      {/* Advanced — always present in the layout, but DISABLED with an
          explanatory tooltip until a rule condition is chosen (there's nothing
          to configure yet). Once chosen, each mode renders its own populated
          Advanced section (below) instead of this gated placeholder. */}
      {!conditionChosen && (
        <CursorTooltip text={t("rulesRegistry.advancedGatedTooltip")}>
          <AdvancedDisclosure label={t("rulesRegistry.advancedSectionLabel")} disabled>
            <span />
          </AdvancedDisclosure>
        </CursorTooltip>
      )}

      {decisionPointChosen && mode === "lowcode" && (
        <div className="space-y-3">
          {/* The IF row / builder rows render up in the Condition section (so
              the row aligns across rule types); this block holds only the
              Advanced section + THEN THE ROW. */}
          {/* Advanced — group-by + joins, folded into the compiled SQL that
              actually runs (see lowcodeCompile.compileLowcodeBody). Placed
              above "THEN THE ROW" (item 23f) since group-by/joins configure
              the IF condition's inputs, not the row-level outcome below it. */}
          <AdvancedDisclosure
            label={t("rulesRegistry.advancedSectionLabel")}
            defaultOpen={!!groupBy || !!filter || lowcodeAst.joins.length > 0}
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
            {/* Row filter — a SQL WHERE predicate applied before the rule
                condition, scoped to rows the check should consider. Supports
                {{slot}} placeholders substituted at materialize time. */}
            <div className="space-y-1.5">
              <Label className="text-xs">{t("rulesRegistry.filterLabel")}</Label>
              <Input
                className="h-7 text-xs font-mono"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder={t("rulesRegistry.filterPlaceholder")}
                disabled={readOnly}
              />
            </div>
          </AdvancedDisclosure>
          <div className="flex flex-wrap items-center gap-3">
            <FramingWord>{t("rulesRegistry.thenTheRow")}</FramingWord>
            <PredicatePolaritySwitch value={polarity} onChange={setPolarity} disabled={readOnly} />
          </div>
        </div>
      )}

      {decisionPointChosen && mode === "dqx_native" && (
        <div className="space-y-3">
          {/* The selected function is shown (and re-selected) via the
              persistent condition picker chip above — no separate function
              combobox here. Only the function's parameters render below. */}
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
            // A little extra breathing room between the check's parameters and
            // the THEN THE ROW polarity switch.
            <div className="flex flex-wrap items-center gap-3 pt-2">
              <FramingWord>{t("rulesRegistry.thenTheRow")}</FramingWord>
              <PredicatePolaritySwitch
                value={nativeSupportsNegate ? polarity : "pass"}
                onChange={setPolarity}
                disabled={readOnly || !nativeSupportsNegate}
                disabledReason={!nativeSupportsNegate ? t("rulesRegistry.polaritySwitcherUnsupported") : undefined}
              />
            </div>
          )}
          {/* Advanced — row filter pre-applied before the rule condition. */}
          <AdvancedDisclosure
            label={t("rulesRegistry.advancedSectionLabel")}
            defaultOpen={!!filter}
          >
            <div className="space-y-1.5">
              <Label className="text-xs">{t("rulesRegistry.filterLabel")}</Label>
              <Input
                className="h-7 text-xs font-mono"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder={t("rulesRegistry.filterPlaceholder")}
                disabled={readOnly}
              />
            </div>
          </AdvancedDisclosure>
        </div>
      )}

      {decisionPointChosen && mode === "sql" && (
        <div className="space-y-3">
          <div className="space-y-1.5">
            {/* The "Condition" header + type picker chip render once, above.
                IF sits on its own line here, with the SQL AI assistants (write
                / improve / explain — item 12) right-aligned on the IF row,
                directly above the editor (dqlake's ImplementationTab layout). */}
            <div className="space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  {/* Back to the rule-type picker — SQL has no in-row selector
                      cell (its whole body IS the editor), so the change-type
                      affordance is a small back arrow next to IF. */}
                  {!readOnly && (
                    <button
                      type="button"
                      onClick={() => setDecisionPointChosen(false)}
                      aria-label={t("rulesRegistry.changeRuleTypeHeader")}
                      title={t("rulesRegistry.changeRuleTypeHeader")}
                      className="text-muted-foreground hover:text-foreground -ml-1 p-0.5 rounded"
                    >
                      <ArrowLeft className="h-3.5 w-3.5" />
                    </button>
                  )}
                  <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    {t("rulesRegistry.ifCondition")}
                  </span>
                </div>
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
          {/* Optional joins card — reuses the low-code JoinsBuilder. A declared
              join makes this rule "cross-table" (sql_query path); no joins keeps
              it single-table (predicate path). Body type is derived at save time
              from join presence — no hidden flag needed. "Cross-Table SQL" from
              the decision-point menu opens this card by default. */}
          <AdvancedDisclosure
            label={t("rulesRegistry.advancedSectionLabel")}
            defaultOpen={sqlJoinsDefaultOpen || sqlJoins.length > 0 || !!filter}
          >
            <JoinsBuilder
              ast={{ rows: [], joins: sqlJoins }}
              onChange={(next) => setSqlJoins(next.joins)}
              declaredColumns={sqlSlots.map((s) => ({ name: s.name, family: slotFamilyToLowcode(s.family) }))}
              readOnly={readOnly}
            />
            {/* Row filter — a SQL WHERE predicate applied before the rule
                condition. Supports {{slot}} placeholders. */}
            <div className="space-y-1.5">
              <Label className="text-xs">{t("rulesRegistry.filterLabel")}</Label>
              <Input
                className="h-7 text-xs font-mono"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder={t("rulesRegistry.filterPlaceholder")}
                disabled={readOnly}
              />
            </div>
          </AdvancedDisclosure>
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
        onConfirm={() => modeSwitch && performModeSwitch(modeSwitch.choice)}
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

  // Save-draft + submit buttons, shared by the dialog footer and the page
  // header. `showSubmitIcon` adds the Send glyph on the submit button to match
  // the MT/TS header treatment; the dialog footer keeps its icon-less look by
  // passing `false`. All label/disabled logic (approvals-mode relabel via
  // `willAutoApprove`, published-revision wording, the `isEditing && !isDirty`
  // submit-only branch, and the missing-fields tooltips) is preserved verbatim.
  const renderSaveSubmitButtons = (showSubmitIcon: boolean): ReactNode => {
    if (readOnly) return null;
    const submitIcon = showSubmitIcon ? <Send className="h-3.5 w-3.5" /> : null;
    return (
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
            <Button onClick={handleSubmitOnly} disabled={saving || !canSubmit || submitDisabledNoChanges} className="gap-2">
              {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : submitIcon}
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
            <Button onClick={() => handleSave(true)} disabled={saving || !isDirty || !canSubmit || submitDisabledNoChanges} className="gap-2">
              {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : submitIcon}
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
      {renderSaveSubmitButtons(false)}
    </>
  );

  if (variant === "page") {
    // The routed detail page already renders its own name/status/mode/
    // author-kind title above this component, so skip the dialog-style
    // title here (which would duplicate it). Save/Submit actions live in a
    // single top-right header row (matching the MT/TS binding headers)
    // rather than a bottom footer; the page's breadcrumb covers the dropped
    // Cancel/Close. The route passes its Approve/Reject buttons + the "…"
    // actions menu as `headerActions` so they render inline, immediately
    // after Save/Submit, in this same row (B2-7) instead of a separate row
    // in the page's own title header.
    const saveSubmitButtons = renderSaveSubmitButtons(true);
    const hasHeaderActions = !!(saveSubmitButtons || headerActions);
    return (
      <div className="space-y-6">
        {(headerTitle || hasHeaderActions) && (
          // Title (left) + Save/Submit + Approve/Reject + "…" (right) share
          // ONE row so pending-approval actions sit in line with the title
          // rather than dropping to a separate row below it (B2-78). The
          // title flexes/truncates; the actions never wrap under the title
          // until the viewport genuinely can't fit both.
          <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
            {headerTitle ? <div className="flex min-w-0 flex-1 items-center">{headerTitle}</div> : <div />}
            {hasHeaderActions && (
              <div className="flex flex-wrap items-center justify-end gap-2">
                {saveSubmitButtons}
                {headerActions}
              </div>
            )}
          </div>
        )}
        {formBody}
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
