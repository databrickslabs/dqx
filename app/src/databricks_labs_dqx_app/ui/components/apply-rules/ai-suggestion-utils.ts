// Pure grouping / selection / filter helpers for AiSuggestionDialog, kept out
// of the .tsx so they are unit-testable under `bun test` (see
// ai-suggestion-utils.test.ts). Ported from dqlake's
// `bindings/AiSuggestionDialog.tsx` (suggestionKey / mappingSetKey /
// filterAlreadyApplied / groupSuggestions / groupSelectState), adapted to our
// suggestion shape: each `SuggestedRuleMappingOut` carries exactly ONE
// slot->column mapping GROUP (`column_mapping`), so the same rule can appear in
// several suggestions (one per column set) — identity is therefore
// (rule_id + its sorted mapping), never rule_id alone.

import type { AppliedRuleOutColumnMappingItem, SuggestedRuleMappingOut } from "@/lib/api";

/** Normalize one slot key or column value for IDENTITY comparison only.
 *
 * The two sides of `filterAlreadyApplied` derive their mapping strings from
 * different sources: an AI suggestion's column value comes from the live Unity
 * Catalog schema the suggester read (`RuleSuggester._resolve_columns` ->
 * `column.name`, original case), while a persisted applied rule's value is
 * whatever was stored when it was bound — which for a profiler-applied rule can
 * be a lower-cased column, and in general may differ in case or stray
 * whitespace. UC column names are case-insensitive-unique, so `Email` and
 * `email` are the SAME column; comparing the raw strings would leave the same
 * logical (rule, column) uncompared and let the already-applied combo survive
 * as a duplicate suggestion. Lower-casing + trimming both the slot key and the
 * column value collapses those equivalent forms. Only the dedup key is
 * normalized — display labels (`groupSuggestions` "by column") keep the raw
 * value. */
function normalizePart(value: string): string {
  return value.trim().toLowerCase();
}

/** Stable, order-independent, case/whitespace-insensitive key for one
 *  slot->column mapping group. Used as the single identity chokepoint by
 *  `suggestionKey` and both sides of `filterAlreadyApplied`, so equivalent
 *  mappings always compare equal regardless of which side produced them. */
export function mappingSetKey(group: Record<string, string>): string {
  return Object.entries(group)
    .map(([slot, column]) => `${normalizePart(slot)}=${normalizePart(column)}`)
    .sort()
    .join(",");
}

/**
 * Stable per-suggestion identity. The engine may return the SAME rule more than
 * once with different mappings (e.g. "Is Not Null" for both `budget_amount` and
 * `actual_spend`), so `rule_id` alone is NOT unique — we key the React list, the
 * toggle set, and the apply filter on rule_id + its sorted slot->column set.
 */
export function suggestionKey(s: SuggestedRuleMappingOut): string {
  return `${s.rule_id}::${mappingSetKey(s.column_mapping ?? {})}`;
}

/** Minimal shape of a staged/applied rule row this module reads. */
export interface AppliedRuleMappingLike {
  rule_id: string;
  column_mapping?: AppliedRuleOutColumnMappingItem[] | null;
}

/**
 * Drop any suggestion whose (rule, slot->column set) is ALREADY applied in the
 * current editor state — including UNSAVED applies from this session. The
 * backend only excludes PERSISTED mappings against the applied set at fetch
 * time, so without this a steward who applies a few suggestions and re-opens
 * the (prefetched, cached) Suggest dialog would see the same (rule, column)
 * again.
 *
 * Column-less rules: a dataset-level rule applied with NO column binding is
 * tracked by `rule_id`, so a re-suggested column-less rule dedups too (its
 * mapping is empty, so there is no slot->column set to key on). A rule applied
 * WITH a column does not populate the column-less set, so an empty-mapping
 * suggestion of that same rule stays a genuinely-distinct suggestion.
 */
export function filterAlreadyApplied<T extends SuggestedRuleMappingOut>(
  suggestions: T[],
  appliedRules: AppliedRuleMappingLike[],
): T[] {
  const appliedByRule = new Map<string, Set<string>>();
  const appliedColumnLessRuleIds = new Set<string>();
  for (const rule of appliedRules) {
    const keys = appliedByRule.get(rule.rule_id) ?? new Set<string>();
    let hadColumn = false;
    for (const group of rule.column_mapping ?? []) {
      const k = mappingSetKey(group);
      if (k) {
        keys.add(k);
        hadColumn = true;
      }
    }
    if (keys.size > 0) appliedByRule.set(rule.rule_id, keys);
    if (!hadColumn) appliedColumnLessRuleIds.add(rule.rule_id);
  }
  if (appliedByRule.size === 0 && appliedColumnLessRuleIds.size === 0) return suggestions;
  return suggestions.filter((s) => {
    const key = mappingSetKey(s.column_mapping ?? {});
    // Column-less suggestion: dedup by rule_id against column-less applies only.
    if (!key) return !appliedColumnLessRuleIds.has(s.rule_id);
    return !appliedByRule.get(s.rule_id)?.has(key);
  });
}

export type GroupMode = "rule" | "column";

export interface SuggestionGroup {
  /** Stable key for the group header + React key. */
  key: string;
  /** Header label shown above the group's cards. */
  label: string;
  /** Suggestions in this group, first-seen order preserved. */
  items: SuggestedRuleMappingOut[];
}

/**
 * Bucket suggestions into display groups.
 *
 * - by rule: one group per rule_id; a rule matched to N columns shows its N
 *   option rows together. Header = rule name (or rule_id when unnamed).
 * - by column: one group per mapped column; a multi-slot rule appears under
 *   EACH column it touches. A suggestion with no mapped column falls into an
 *   "Unmapped" group so nothing is dropped.
 *
 * First-seen order is preserved for both groups and the items within them.
 */
export function groupSuggestions(
  suggestions: SuggestedRuleMappingOut[],
  mode: GroupMode,
  unmappedLabel: string,
): SuggestionGroup[] {
  const groups = new Map<string, SuggestionGroup>();
  const push = (key: string, label: string, s: SuggestedRuleMappingOut) => {
    const g = groups.get(key);
    if (g) g.items.push(s);
    else groups.set(key, { key, label, items: [s] });
  };

  for (const s of suggestions) {
    if (mode === "rule") {
      push(`rule:${s.rule_id}`, s.rule_name || s.rule_id, s);
      continue;
    }
    const cols = Object.values(s.column_mapping ?? {});
    if (cols.length === 0) {
      push("col:__unmapped__", unmappedLabel, s);
      continue;
    }
    for (const col of cols) push(`col:${col}`, col, s);
  }
  return [...groups.values()];
}

/** Aggregate select state of a group, for the header toggle-all control. */
export type GroupSelectState = "all" | "some" | "none";

export function groupSelectState(
  items: SuggestedRuleMappingOut[],
  selected: Set<string>,
): GroupSelectState {
  let on = 0;
  for (const s of items) if (selected.has(suggestionKey(s))) on++;
  if (on === 0) return "none";
  if (on === items.length) return "all";
  return "some";
}
