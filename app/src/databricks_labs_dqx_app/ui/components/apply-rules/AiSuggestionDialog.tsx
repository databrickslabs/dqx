// AiSuggestionDialog — AI-suggested registry rules for a monitored table,
// ported faithfully (structure + interactions) from dqlake's
// `bindings/AiSuggestionDialog.tsx`. Every genuinely-good (rule, column-mapping)
// match renders as a toggleable card; the same rule can appear on more than one
// card when it fits more than one column. Cards can be grouped BY RULE (the
// rule's name/dimension/severity/description shown once, then a compact row per
// column-mapping option) or BY COLUMN (one header per column, collecting every
// rule suggested for it). A tri-state group header toggles a whole group at
// once; the footer applies exactly the toggled-ON suggestions onto the tab's
// LOCAL editor state (no network write — see `AddRulesDialog.tsx`). Suggestions
// already staged this session are filtered out client-side (the backend only
// excludes PERSISTED mappings). Always renders (never blocks the tab): a
// missing/misconfigured AI backend degrades to an "unavailable" empty state via
// `available`/`reason` on the response.

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { ArrowRight, Check, Loader2, Minus, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useListRegistryRules, type RegistryRuleOut, type SuggestedRuleMappingOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";
import type { ColumnRef } from "./RulesByColumn";
import { AI_BUTTON_BG, AI_GRADIENT_URL, AI_ICON_COLOR, AI_TEXT_GRADIENT } from "@/lib/ai-style";
import { SeverityBadge } from "@/components/RegistryRuleBadges";
import { useDefaultAutoUpgrade } from "@/hooks/use-default-auto-upgrade";
import { cn } from "@/lib/utils";
import {
  RESERVED_DIMENSION_KEY,
  RESERVED_SEVERITY_KEY,
  TagBadge,
  colorFor,
  getTag,
  newStagedRow,
} from "./shared";
import {
  filterAlreadyApplied,
  groupSelectState,
  groupSuggestions,
  suggestionKey,
  type AppliedRuleMappingLike,
  type GroupMode,
  type GroupSelectState,
} from "./ai-suggestion-utils";

export interface SuggestRulesState {
  available: boolean;
  reason?: string;
  suggestions: SuggestedRuleMappingOut[];
}

interface AiSuggestionDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bindingId: string;
  labelDefinitions: LabelDefinition[];
  /** Prefetched suggestion result, owned by the Apply Rules tab and fired the
   *  moment the tab is entered (dqlake behaviour) — so opening this dialog is
   *  instant instead of waiting on the request. `null` while the first fetch
   *  is still in flight. */
  state: SuggestRulesState | null;
  /** True while a (pre)fetch is in flight and no result is available yet. */
  loading: boolean;
  /** Manual re-run of the suggestion request. Retained for caller
   *  compatibility; the dialog no longer surfaces a Refresh affordance
   *  (prefetch-on-tab-entry keeps the result fresh). */
  onRefresh: () => void;
  /** The tab's staged applied-rule rows — used to hide suggestions the steward
   *  has ALREADY applied this session (unsaved), which the backend can't know
   *  about (it only excludes persisted mappings). */
  appliedRules: AppliedRuleMappingLike[];
  /** Appends one locally-staged row per accepted suggestion. Pure
   *  local-state mutation — no network call. */
  onAdd: (rows: ReturnType<typeof newStagedRow>[], columnContext?: ColumnRef | null) => void;
  onApplied: () => void;
}

export function AiSuggestionDialog({
  open,
  onOpenChange,
  bindingId,
  labelDefinitions,
  state,
  loading,
  appliedRules,
  onAdd,
  onApplied,
}: AiSuggestionDialogProps) {
  const { t } = useTranslation();
  const defaultAutoUpgrade = useDefaultAutoUpgrade();
  // Suggestions only carry `rule_id` — resolving it back to a full
  // `RegistryRuleOut` is what lets `newStagedRow` denormalize the rule's
  // name/dimension/severity tags onto the staged row the same way every
  // other staging path does, and lets the card show the rule description.
  const { data: registryData } = useListRegistryRules({ status: "approved" });
  const ruleById = useMemo(() => {
    const map = new Map<string, RegistryRuleOut>();
    for (const r of registryData?.data ?? []) map.set(r.rule_id, r);
    return map;
  }, [registryData]);

  // Selection is keyed by (rule_id + sorted mapping), NOT list index — so the
  // same rule under two columns toggles independently and toggle state
  // survives a regroup (see suggestionKey).
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // Group the cards by rule (default) or by column. Toggling only changes
  // presentation — `selected` is keyed independently of the grouping.
  const [groupBy, setGroupBy] = useState<GroupMode>("rule");
  // Initial-focus target on open so the close X doesn't get auto-focused and
  // paint a square focus-visible outline (dqlake behaviour).
  const titleRef = useRef<HTMLHeadingElement>(null);

  // Exclude (rule, column) pairs already staged in the CURRENT editor state —
  // including unsaved applies from this session.
  const suggestions = useMemo(
    () => filterAlreadyApplied(state?.suggestions ?? [], appliedRules),
    [state, appliedRules],
  );

  // Default every (surviving) suggestion to ON whenever a fresh result arrives.
  useEffect(() => {
    setSelected(new Set((state?.suggestions ?? []).map(suggestionKey)));
  }, [state]);

  const selectedCount = suggestions.filter((s) => selected.has(suggestionKey(s))).length;
  const allSelected = suggestions.length > 0 && selectedCount === suggestions.length;

  const toggle = (key: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  // Toggle every suggestion in one group at once: if the group is fully
  // selected, clicking the header deselects all of its items; otherwise (none
  // or only some) it selects all of them.
  const toggleGroup = (items: SuggestedRuleMappingOut[]) => {
    const keys = items.map(suggestionKey);
    const allOn = keys.every((k) => selected.has(k));
    setSelected((prev) => {
      const next = new Set(prev);
      if (allOn) keys.forEach((k) => next.delete(k));
      else keys.forEach((k) => next.add(k));
      return next;
    });
  };

  const dimColor = (dim?: string | null) =>
    dim ? colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, dim) : undefined;

  const handleAdd = () => {
    const chosen = suggestions.filter((s) => selected.has(suggestionKey(s)));
    if (chosen.length === 0) {
      toast.error(t("monitoredTables.suggestRulesNoneSelected"));
      return;
    }
    // A suggestion whose rule_id isn't in the (approved) registry snapshot is
    // dropped rather than staged with missing display metadata — this can only
    // happen if the rule was unpublished between the suggestion call and Add.
    const rows = chosen
      .map((s) => {
        const rule = ruleById.get(s.rule_id);
        if (!rule) return null;
        return newStagedRow(bindingId, rule, [s.column_mapping], defaultAutoUpgrade);
      })
      .filter((row): row is NonNullable<typeof row> => row !== null);
    if (rows.length === 0) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
      return;
    }
    onAdd(rows);
    toast.success(t("monitoredTables.suggestRulesAddedToast", { count: rows.length }));
    onApplied();
    if (rows.length < chosen.length) {
      toast.error(t("monitoredTables.suggestRulesAddFailed"));
    }
    onOpenChange(false);
  };

  const showToggle = suggestions.length > 0 && !loading;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="sm:max-w-2xl max-h-[85vh] flex flex-col"
        // The dialog auto-focuses its first focusable child (the close X),
        // which paints an ugly square focus ring. Move initial focus to the
        // neutral title heading; the X stays clickable + keyboard-reachable.
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          titleRef.current?.focus();
        }}
      >
        <DialogHeader>
          <div className="flex items-center justify-between gap-2 pr-8">
            <DialogTitle
              ref={titleRef}
              tabIndex={-1}
              className="flex items-center gap-2 outline-none"
            >
              <Sparkles className="h-4 w-4" stroke={AI_GRADIENT_URL} />
              {/* leading-normal + pb-0.5: the DialogTitle's leading-none clips
                  the gradient text's descenders (the "g") under bg-clip-text.
                  Local fix only — the shared AI_TEXT_GRADIENT token is reused
                  elsewhere and must stay untouched. */}
              <span className={cn(AI_TEXT_GRADIENT, "leading-normal pb-0.5")}>
                {t("monitoredTables.suggestRulesDialogTitle")}
              </span>
            </DialogTitle>
            {showToggle && <GroupByToggle value={groupBy} onChange={setGroupBy} />}
          </div>
          <DialogDescription>{t("monitoredTables.suggestRulesDialogDescription")}</DialogDescription>
        </DialogHeader>

        {/* scrollbar-gutter:stable reserves the scrollbar's space up front so
            it can't paint/reflow mid-way through the dialog's zoom/slide-in
            (the 200ms open animation), which otherwise caused a visible jump. */}
        <div className="flex-1 overflow-y-auto min-h-0 -mr-2 pr-2 [scrollbar-gutter:stable]">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" aria-label={t("monitoredTables.suggestRulesLoading")} />
              <p className="text-sm text-muted-foreground">{t("monitoredTables.suggestRulesLoading")}</p>
            </div>
          ) : state && !state.available ? (
            <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
              <Sparkles className="h-8 w-8 text-muted-foreground/30" />
              <p className="text-sm font-medium text-muted-foreground">
                {t("monitoredTables.suggestRulesUnavailableTitle")}
              </p>
              {/* Honest state-reason plumbing (P19-B) stays: the specific
                  reason the backend reported, if any, still surfaces. */}
              {state.reason && <p className="text-xs text-muted-foreground/70 max-w-sm">{state.reason}</p>}
              <p className="text-xs text-muted-foreground/70 max-w-sm">
                {t("monitoredTables.suggestRulesUnavailableFallback")}
              </p>
            </div>
          ) : state && suggestions.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-10 gap-2 text-center">
              <Sparkles className="h-8 w-8 text-muted-foreground/30" />
              <p className="text-sm font-medium text-muted-foreground">
                {t("monitoredTables.suggestRulesEmptyTitle")}
              </p>
              <p className="text-xs text-muted-foreground/70 max-w-sm">
                {state.reason || t("monitoredTables.suggestRulesEmptyDescription")}
              </p>
            </div>
          ) : state && groupBy === "rule" ? (
            <div className="space-y-4">
              {groupSuggestions(suggestions, "rule", t("monitoredTables.suggestRulesUnmappedGroup")).map((group, gi) => {
                const first = group.items[0];
                const rule = ruleById.get(first.rule_id);
                return (
                  <RuleGroupCard
                    key={group.key}
                    ruleName={first.rule_name || first.rule_id}
                    dimension={first.dimension}
                    severity={first.severity}
                    ruleDescription={rule ? getTag(rule, "description") : ""}
                    dimensionColor={dimColor(first.dimension)}
                    severityColor={first.severity ? colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, first.severity) : undefined}
                    items={group.items}
                    groupIndex={gi}
                    groupState={groupSelectState(group.items, selected)}
                    toggleAllLabel={t("monitoredTables.suggestRulesToggleAll", { label: first.rule_name || first.rule_id })}
                    onToggleGroup={() => toggleGroup(group.items)}
                    isSelected={(s) => selected.has(suggestionKey(s))}
                    onToggle={(s) => toggle(suggestionKey(s))}
                  />
                );
              })}
            </div>
          ) : state ? (
            <div className="space-y-4">
              {groupSuggestions(suggestions, "column", t("monitoredTables.suggestRulesUnmappedGroup")).map((group, gi) => (
                <ColumnGroupCard
                  key={group.key}
                  column={group.label}
                  items={group.items}
                  groupIndex={gi}
                  groupState={groupSelectState(group.items, selected)}
                  toggleAllLabel={t("monitoredTables.suggestRulesToggleAll", { label: group.label })}
                  labelDefinitions={labelDefinitions}
                  onToggleGroup={() => toggleGroup(group.items)}
                  isSelected={(s) => selected.has(suggestionKey(s))}
                  onToggle={(s) => toggle(suggestionKey(s))}
                />
              ))}
            </div>
          ) : null}
        </div>

        <DialogFooter className="flex-col sm:flex-row items-center gap-2 border-t pt-4">
          {showToggle && (
            <span className="text-xs text-muted-foreground mr-auto">
              {t("monitoredTables.suggestRulesSelectedCount", { selected: selectedCount, total: suggestions.length })}
            </span>
          )}
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            {t("common.cancel")}
          </Button>
          {state && state.available && suggestions.length > 0 && (
            <Button
              onClick={handleAdd}
              disabled={selectedCount === 0}
              className={cn(
                "gap-2 transition-transform duration-200 ease-out hover:scale-[1.02] active:scale-[0.99] motion-reduce:transform-none",
                AI_BUTTON_BG,
              )}
            >
              <Sparkles className="h-4 w-4" />
              {allSelected
                ? t("monitoredTables.suggestRulesAddAllButton")
                : t("monitoredTables.suggestRulesAddButton", { count: selectedCount })}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

/** Segmented "Group by: rule / column" control. */
function GroupByToggle({ value, onChange }: { value: GroupMode; onChange: (v: GroupMode) => void }) {
  const { t } = useTranslation();
  const labels: Record<GroupMode, string> = {
    rule: t("monitoredTables.suggestRulesGroupByRule"),
    column: t("monitoredTables.suggestRulesGroupByColumn"),
  };
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs text-muted-foreground">{t("monitoredTables.suggestRulesGroupBy")}</span>
      <div role="group" aria-label={t("monitoredTables.suggestRulesGroupBy")} className="inline-flex items-center rounded-md border p-0.5">
        {(["rule", "column"] as const).map((opt) => (
          <button
            key={opt}
            type="button"
            aria-pressed={value === opt}
            onClick={() => onChange(opt)}
            className={cn(
              "rounded px-2 py-0.5 text-xs transition-colors",
              value === opt ? "bg-muted font-medium text-foreground" : "text-muted-foreground hover:text-foreground",
            )}
          >
            {labels[opt]}
          </button>
        ))}
      </div>
    </div>
  );
}

/**
 * Tri-state checkbox shown in a group header. Reflects whether ALL / SOME /
 * NONE of the group's items are selected, and toggles the whole group on click.
 * `aria-checked` "true" | "false" | "mixed" exposes the state to assistive tech.
 */
function GroupHeaderCheckbox({ state, onToggle, label }: { state: GroupSelectState; onToggle: () => void; label: string }) {
  const ariaChecked = state === "all" ? "true" : state === "some" ? "mixed" : "false";
  return (
    <span
      role="checkbox"
      aria-checked={ariaChecked}
      aria-label={label}
      tabIndex={0}
      onClick={(e) => {
        e.stopPropagation();
        onToggle();
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          e.stopPropagation();
          onToggle();
        }
      }}
      className={cn(
        "inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-[4px] border cursor-pointer transition-colors",
        state === "none"
          ? "border-muted-foreground/40 bg-transparent hover:border-muted-foreground/70"
          : "border-fuchsia-500/70 bg-fuchsia-500/15 text-fuchsia-600 dark:border-fuchsia-400/60 dark:text-fuchsia-300",
      )}
    >
      {state === "all" ? <Check className="h-3 w-3" /> : state === "some" ? <Minus className="h-3 w-3" /> : null}
    </span>
  );
}

/** The `column → {{slot}}` chips for one suggestion's mapping. */
function MappingChipsInline({ suggestion: s }: { suggestion: SuggestedRuleMappingOut }) {
  const entries = Object.entries(s.column_mapping ?? {});
  if (entries.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {entries.map(([slot, column]) => (
        <span
          key={`${slot}-${column}`}
          className="inline-flex items-center gap-1 rounded border bg-muted/40 px-2 py-0.5 text-[11px]"
        >
          <span className="font-mono font-medium">{column}</span>
          <ArrowRight className={cn("h-3 w-3", AI_ICON_COLOR)} />
          <span className="font-mono text-muted-foreground">{`{{${slot}}}`}</span>
        </span>
      ))}
    </div>
  );
}

/**
 * Bordered card shell for one suggestion group — shared by the by-rule and
 * by-column lenses so both read as the same "inverted" card: a clickable
 * tri-state header (checkbox + `header` node, optional `subhead`) over a
 * compact list of toggleable rows. Clicking the header toggles the whole group.
 */
function SuggestionGroupCard({
  groupIndex,
  groupState,
  toggleAllLabel,
  onToggleGroup,
  header,
  subhead,
  children,
}: {
  groupIndex: number;
  groupState: GroupSelectState;
  toggleAllLabel: string;
  onToggleGroup: () => void;
  header: ReactNode;
  subhead?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div
      className="rounded-md border bg-card p-3 space-y-3 [animation:dq-suggestion-in_280ms_ease-out_both]"
      style={{ animationDelay: `${groupIndex * 60}ms` }}
    >
      <div
        role="button"
        tabIndex={0}
        aria-label={toggleAllLabel}
        onClick={onToggleGroup}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggleGroup();
          }
        }}
        className="space-y-1 cursor-pointer select-none"
      >
        <div className="flex items-center gap-2 flex-wrap">
          <GroupHeaderCheckbox state={groupState} onToggle={onToggleGroup} label={toggleAllLabel} />
          {header}
        </div>
        {subhead}
      </div>

      <div className="space-y-1.5">{children}</div>
    </div>
  );
}

/** Toggleable compact row shell shared by the by-rule and by-column rows. */
function SuggestionRow({
  label,
  selected,
  onToggle,
  children,
}: {
  label: string;
  selected: boolean;
  onToggle: () => void;
  children: ReactNode;
}) {
  return (
    <div
      role="button"
      tabIndex={0}
      aria-pressed={selected}
      aria-label={label}
      onClick={onToggle}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onToggle();
        }
      }}
      className={cn(
        "rounded p-2 space-y-1 cursor-pointer select-none transition-colors",
        selected
          ? "border border-fuchsia-500/60 bg-fuchsia-500/5 dark:border-fuchsia-400/50"
          : "border border-border hover:border-muted-foreground/40",
      )}
    >
      {children}
    </div>
  );
}

/**
 * By-rule presentation: the rule's name, dimension, severity, and description
 * appear ONCE in a header; below it, one compact row per column-mapping option
 * — each row shows its mapping chip(s), the per-mapping explanation, and its own
 * toggle. The header (or its tri-state checkbox) toggles every option at once.
 */
function RuleGroupCard({
  ruleName,
  dimension,
  severity,
  ruleDescription,
  dimensionColor,
  severityColor,
  items,
  groupIndex,
  groupState,
  toggleAllLabel,
  onToggleGroup,
  isSelected,
  onToggle,
}: {
  ruleName: string;
  dimension?: string | null;
  severity?: string | null;
  ruleDescription?: string;
  dimensionColor?: string;
  severityColor?: string;
  items: SuggestedRuleMappingOut[];
  groupIndex: number;
  groupState: GroupSelectState;
  toggleAllLabel: string;
  onToggleGroup: () => void;
  isSelected: (s: SuggestedRuleMappingOut) => boolean;
  onToggle: (s: SuggestedRuleMappingOut) => void;
}) {
  return (
    <SuggestionGroupCard
      groupIndex={groupIndex}
      groupState={groupState}
      toggleAllLabel={toggleAllLabel}
      onToggleGroup={onToggleGroup}
      header={
        <>
          <span className="text-sm font-semibold">{ruleName}</span>
          {dimension && <TagBadge label={dimension} color={dimensionColor} />}
          {severity && <SeverityBadge severity={severity} color={severityColor} />}
        </>
      }
      subhead={ruleDescription ? <p className="text-xs text-muted-foreground">{ruleDescription}</p> : undefined}
    >
      {items.map((s) => (
        <MappingRow key={suggestionKey(s)} suggestion={s} selected={isSelected(s)} onToggle={() => onToggle(s)} />
      ))}
    </SuggestionGroupCard>
  );
}

/** One column-mapping option under a rule header: chip(s) + explanation + toggle. */
function MappingRow({
  suggestion: s,
  selected,
  onToggle,
}: {
  suggestion: SuggestedRuleMappingOut;
  selected: boolean;
  onToggle: () => void;
}) {
  const cols = Object.values(s.column_mapping ?? {}).join(", ");
  return (
    <SuggestionRow label={cols || s.rule_name || s.rule_id} selected={selected} onToggle={onToggle}>
      <MappingChipsInline suggestion={s} />
      {s.explanation && <p className="text-xs text-muted-foreground">{s.explanation}</p>}
    </SuggestionRow>
  );
}

/**
 * By-column presentation — the inverse of the by-rule card: the column name is
 * the card header, and each rule suggested for that column is a compact row
 * (rule name + badges + explanation + mapping chip(s)). Reuses the same card and
 * row shells as the by-rule lens so both read identically.
 */
function ColumnGroupCard({
  column,
  items,
  groupIndex,
  groupState,
  toggleAllLabel,
  labelDefinitions,
  onToggleGroup,
  isSelected,
  onToggle,
}: {
  column: string;
  items: SuggestedRuleMappingOut[];
  groupIndex: number;
  groupState: GroupSelectState;
  toggleAllLabel: string;
  labelDefinitions: LabelDefinition[];
  onToggleGroup: () => void;
  isSelected: (s: SuggestedRuleMappingOut) => boolean;
  onToggle: (s: SuggestedRuleMappingOut) => void;
}) {
  return (
    <SuggestionGroupCard
      groupIndex={groupIndex}
      groupState={groupState}
      toggleAllLabel={toggleAllLabel}
      onToggleGroup={onToggleGroup}
      header={<span className="font-mono text-sm font-semibold">{column}</span>}
    >
      {items.map((s) => (
        <ColumnRuleRow
          key={suggestionKey(s)}
          suggestion={s}
          selected={isSelected(s)}
          dimensionColor={s.dimension ? colorFor(labelDefinitions, RESERVED_DIMENSION_KEY, s.dimension) : undefined}
          severityColor={s.severity ? colorFor(labelDefinitions, RESERVED_SEVERITY_KEY, s.severity) : undefined}
          onToggle={() => onToggle(s)}
        />
      ))}
    </SuggestionGroupCard>
  );
}

/** One rule suggested for a column (by-column view): rule name + badges +
 *  explanation + mapping chip(s), as a compact toggleable row. */
function ColumnRuleRow({
  suggestion: s,
  selected,
  dimensionColor,
  severityColor,
  onToggle,
}: {
  suggestion: SuggestedRuleMappingOut;
  selected: boolean;
  dimensionColor?: string;
  severityColor?: string;
  onToggle: () => void;
}) {
  return (
    <SuggestionRow label={s.rule_name || s.rule_id} selected={selected} onToggle={onToggle}>
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-sm font-semibold">{s.rule_name || s.rule_id}</span>
        {s.dimension && <TagBadge label={s.dimension} color={dimensionColor} />}
        {s.severity && <SeverityBadge severity={s.severity} color={severityColor} />}
      </div>
      {s.explanation && <p className="text-xs text-muted-foreground">{s.explanation}</p>}
      <MappingChipsInline suggestion={s} />
    </SuggestionRow>
  );
}
