// Shared helpers for the Apply Rules tab components (registry-rule tag
// lookups, label coloring, API error extraction). Kept in one place so
// AddRulesDialog / RuleConfigCard / RulesByColumn agree on the same
// conventions instead of re-deriving them.

import { Badge } from "@/components/ui/badge";
import type { AppliedRuleOut, AppliedRuleOutColumnMappingItem, DesiredAppliedRuleIn, RegistryRuleOut } from "@/lib/api";
import type { LabelDefinition } from "@/lib/api-custom";

export const RESERVED_NAME_KEY = "name";
export const RESERVED_DIMENSION_KEY = "dimension";
export const RESERVED_SEVERITY_KEY = "severity";

export function getTag(rule: RegistryRuleOut, key: string): string {
  const md = (rule.user_metadata ?? {}) as Record<string, unknown>;
  const v = md[key];
  return typeof v === "string" ? v : "";
}

export function extractApiError(err: unknown, fallback: string): string {
  const axErr = err as { response?: { data?: { detail?: string } } };
  return axErr?.response?.data?.detail ?? fallback;
}

export function colorFor(defs: LabelDefinition[], key: string, value: string): string | undefined {
  const def = defs.find((d) => d.key === key);
  return def?.value_colors?.[value] ?? undefined;
}

// ---------------------------------------------------------------------------
// Rule grouping — DQX materializes one `dq_applied_rules` ROW per mapping
// GROUP (each call to `useApplyRuleToTable` with a single-entry
// `column_mapping` array creates its own row/applied-check). dqlake's
// bindings model keeps every mapping group inside a single binding entity;
// to render the equivalent "N mapping groups under one rule card" UI here,
// the by-rule lens groups the flat `AppliedRuleOut[]` list by `rule_id` and
// flattens every row's `column_mapping` into one combined list.
// ---------------------------------------------------------------------------

export interface RuleRowGroup {
  ruleId: string;
  /** Every applied-rule ROW for this rule_id, in list order. Each row is
   *  expected to carry exactly one mapping group (the convention every
   *  apply path in this app follows), so `rows[i]` owns combined-mapping
   *  index `i` — used to resolve "remove this mapping group" back to the
   *  concrete row id to delete. */
  rows: AppliedRuleOut[];
}

/** Group a flat applied-rules list by `rule_id`, preserving first-seen order. */
export function groupAppliedRulesByRuleId(appliedRules: AppliedRuleOut[]): RuleRowGroup[] {
  const order: string[] = [];
  const map = new Map<string, AppliedRuleOut[]>();
  for (const rule of appliedRules) {
    if (!map.has(rule.rule_id)) {
      map.set(rule.rule_id, []);
      order.push(rule.rule_id);
    }
    map.get(rule.rule_id)!.push(rule);
  }
  return order.map((ruleId) => ({ ruleId, rows: map.get(ruleId)! }));
}

/** Merge a rule-id's rows into one display-only `AppliedRuleOut` whose
 *  `column_mapping` is the concatenation of every row's mapping groups
 *  (display metadata — name/dimension/severity/pin — comes from the first
 *  row). Used everywhere the by-rule lens needs one card per rule_id. */
export function mergeRuleRowGroup(group: RuleRowGroup): AppliedRuleOut {
  const [first] = group.rows;
  return {
    ...first,
    column_mapping: group.rows.flatMap((row) => row.column_mapping ?? []),
  };
}

/** Every column name a rule is already mapped to, across every mapping
 *  group and slot (multi-value slots store their columns as a
 *  comma-joined string — see `AddRulesDialog#handleApply`). Used by the
 *  "+ Apply to another column" flow to exclude columns the rule already
 *  covers from the column picker, mirroring dqlake's `usedSetForNew`
 *  exclusion in `bindings/MappingChips.tsx`. */
export function getUsedColumnsForRule(rule: AppliedRuleOut): string[] {
  const used = new Set<string>();
  for (const group of rule.column_mapping ?? []) {
    for (const value of Object.values(group)) {
      if (!value) continue;
      for (const col of value.split(",")) {
        if (col) used.add(col);
      }
    }
  }
  return [...used];
}

// ---------------------------------------------------------------------------
// Staged editor helpers (P16-F) — the Apply Rules tab stages every add /
// mapping-edit / severity-override / pin / removal in a local `stagedRows`
// array (same `AppliedRuleOut[]` shape the server returns, one row per
// mapping group, per the "each row owns exactly one mapping group"
// convention above) and only writes it in one batch via `saveAppliedRules`
// on Save-as-draft/Publish. These two helpers are the single choke point
// for creating a new local-only row and for turning `stagedRows` back into
// the `saveAppliedRules` request payload — every staging call site
// (AddRulesDialog, AiSuggestionDialog, RuleConfigCard) goes through them so
// the local-id convention and payload shape never drift apart.
// ---------------------------------------------------------------------------

let localRowCounter = 0;

/** A stable, never-persisted id for a row staged locally this session.
 *  `buildDesiredApplications` drops `id` entirely (it regroups by `rule_id`
 *  and lets the backend re-derive/ignore row identity), so nothing in this
 *  app currently needs to distinguish a local id from a real server one —
 *  it only has to be unique within `stagedRows` for the session. */
export function nextLocalRowId(): string {
  localRowCounter += 1;
  return `local-${Date.now()}-${localRowCounter}`;
}

/** Split every row into one row per mapping GROUP so the "each staged row
 *  owns exactly one mapping group" convention (relied on by
 *  `handleRemoveMappingGroup` / `handleChangeMapping` / `handleAddMapping` in
 *  `monitored-tables.$bindingId.tsx`, which resolve a `groupIdx` from the
 *  flattened `mergeRuleRowGroup` list back to `rowsForRule[groupIdx]`) always
 *  holds — even though the server does NOT follow it: `saveAppliedRules`
 *  persists one `dq_applied_rules` row per `rule_id` carrying the FULL
 *  `column_mapping` list (see `ApplyRulesService.reconcile`/`apply_rule`), so
 *  a rule with 2 mapping groups round-trips as a single row with a 2-entry
 *  `column_mapping`. Without this normalization, `groupIdx` (a position in
 *  the flattened list) would misalign with `rowsForRule` (server rows) and
 *  silently corrupt or drop mapping groups on edit/remove.
 *
 *  Call this on every path that seeds `stagedRows`/`baseline` from server
 *  data — initial load, binding switch, and the Save-as-draft/Publish
 *  response handlers. Rows staged locally (`newStagedRow`, `handleAddMapping`)
 *  already carry at most one group and pass through unchanged. Split-off
 *  rows get a fresh local id (`row.id` is display-only here — it's never
 *  read back by `buildDesiredApplications`, which regroups by `rule_id`). */
export function normalizeStagedRows(rows: AppliedRuleOut[]): AppliedRuleOut[] {
  return rows.flatMap((row) => {
    const groups = row.column_mapping ?? [];
    if (groups.length <= 1) return [row];
    return groups.map((group, idx) => ({
      ...row,
      id: idx === 0 ? row.id : nextLocalRowId(),
      column_mapping: [group],
    }));
  });
}

/** Build a new locally-staged applied-rule row for *rule*, not yet persisted
 *  anywhere. Display metadata (name/dimension/severity tags) is denormalized
 *  onto the row up front, exactly like the server's `AppliedRuleOut.from_summary`
 *  join, so every display component that reads `rule_name`/`rule_dimension`/
 *  `rule_severity` off a row works identically for staged and persisted rows. */
export function newStagedRow(
  bindingId: string,
  rule: RegistryRuleOut,
  columnMapping: AppliedRuleOutColumnMappingItem[],
): AppliedRuleOut {
  return {
    id: nextLocalRowId(),
    binding_id: bindingId,
    rule_id: rule.rule_id,
    pinned_version: null,
    severity_override: null,
    column_mapping: columnMapping,
    user_metadata: {},
    mapping_hash: null,
    created_by: null,
    created_at: null,
    rule_name: getTag(rule, RESERVED_NAME_KEY) || null,
    rule_dimension: getTag(rule, RESERVED_DIMENSION_KEY) || null,
    rule_severity: getTag(rule, RESERVED_SEVERITY_KEY) || null,
  };
}

/** Turn the flat staged row list into the FULL desired-set payload for
 *  `saveAppliedRules` — one entry per `rule_id`, whose `column_mapping` is
 *  the concatenation of every one of that rule_id's rows' mapping groups
 *  (mirrors `mergeRuleRowGroup`'s display-side merge). Display-only fields
 *  (`rule_name`/`rule_dimension`/`rule_severity`/`mapping_hash`/`created_*`)
 *  are dropped — the backend re-derives or ignores them. */
export function buildDesiredApplications(stagedRows: AppliedRuleOut[]): DesiredAppliedRuleIn[] {
  return groupAppliedRulesByRuleId(stagedRows).map(({ ruleId, rows }) => {
    const [first] = rows;
    return {
      rule_id: ruleId,
      column_mapping: rows.flatMap((row) => row.column_mapping ?? []),
      pinned_version: first?.pinned_version ?? null,
      severity_override: first?.severity_override ?? null,
      tags: (first?.user_metadata ?? {}) as Record<string, unknown>,
    };
  });
}

/** Stable, order-independent serialization of a `saveAppliedRules` payload —
 *  used to diff the staged editor's local rows against the last-persisted
 *  baseline for `isDirty` (mirrors `RegistryRuleFormDialog`'s
 *  `stableStringify(currentSnapshot) !== stableStringify(snapshotFromRule(...))`
 *  pattern). Sorts by `rule_id` and, within each application, by mapping
 *  group so row insertion order and mapping-group order never cause a false
 *  "dirty" positive. */
export function desiredApplicationsKey(stagedRows: AppliedRuleOut[]): string {
  const normalized = buildDesiredApplications(stagedRows)
    .map((application) => ({
      rule_id: application.rule_id,
      column_mapping: (application.column_mapping ?? [])
        .map((group) => JSON.stringify(Object.fromEntries(Object.entries(group).sort())))
        .sort(),
      pinned_version: application.pinned_version ?? null,
      severity_override: application.severity_override ?? null,
      tags: JSON.stringify(Object.fromEntries(Object.entries(application.tags ?? {}).sort())),
    }))
    .sort((a, b) => a.rule_id.localeCompare(b.rule_id));
  return JSON.stringify(normalized);
}

export function TagBadge({ label, color }: { label: string; color?: string }) {
  if (!label) return null;
  return (
    <Badge variant="outline" className="gap-1 text-[10px] font-normal">
      {color && (
        <span className="inline-block h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} aria-hidden />
      )}
      {label}
    </Badge>
  );
}
