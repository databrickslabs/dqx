// THROWAWAY prototype — pure helpers for the wizard-sheet variant (B).
import {
  MOCK_RULES,
  printExpr,
  type ColumnCondition,
  type ConditionExpr,
  type Family,
  type MockRule,
  type MockSlot,
  type PolicyDraft,
  type PolicyScope,
  type TableCondition,
} from "../mock";

export type StepKey = "rules" | "where" | "columns" | "review";

export const STEPS: { key: StepKey; label: string; hint: string }[] = [
  { key: "rules", label: "Rules", hint: "What to apply" },
  { key: "where", label: "Where", hint: "Which tables" },
  { key: "columns", label: "Columns", hint: "Which columns" },
  { key: "review", label: "Review", hint: "Check and create" },
];

export type Errors = Record<string, string>;

export function ruleById(id: string): MockRule | undefined {
  return MOCK_RULES.find((r) => r.rule_id === id);
}

export function selectedRules(draft: PolicyDraft): MockRule[] {
  return draft.rule_ids.map(ruleById).filter((r): r is MockRule => !!r);
}

export function hasSlots(draft: PolicyDraft): boolean {
  return selectedRules(draft).some((r) => r.slots.length > 0);
}

export const FAMILY_LABEL: Record<Family, string> = {
  numeric: "number",
  text: "text",
  temporal: "date/time",
  boolean: "boolean",
  any: "any type",
};

export function slotLabel(slot: MockSlot): string {
  return `${FAMILY_LABEL[slot.family]} · ${slot.cardinality === "one" ? "one column" : "one or more columns"}`;
}

export const DEFAULT_COLUMN_CONDITION: ColumnCondition = { kind: "tags", tags: [] };

export function columnCondition(draft: PolicyDraft, ruleId: string, slot: string): ColumnCondition {
  return draft.column_conditions[ruleId]?.[slot] ?? DEFAULT_COLUMN_CONDITION;
}

export function withColumnCondition(
  draft: PolicyDraft,
  ruleId: string,
  slot: string,
  cond: ColumnCondition,
): PolicyDraft {
  return {
    ...draft,
    column_conditions: {
      ...draft.column_conditions,
      [ruleId]: { ...(draft.column_conditions[ruleId] ?? {}), [slot]: cond },
    },
  };
}

/** Drop column conditions for rules no longer in the policy; fill defaults for new slots. */
export function normalizeDraft(draft: PolicyDraft): PolicyDraft {
  const cc: PolicyDraft["column_conditions"] = {};
  for (const rule of selectedRules(draft)) {
    if (rule.slots.length === 0) continue;
    cc[rule.rule_id] = {};
    for (const slot of rule.slots) cc[rule.rule_id][slot.name] = columnCondition(draft, rule.rule_id, slot.name);
  }
  const anyMulti = selectedRules(draft).some((r) => r.slots.length >= 2);
  return {
    ...draft,
    name: draft.name.trim(),
    description: draft.description.trim(),
    scope: draft.scope.filter((s) => s.catalog),
    column_conditions: cc,
    pair_by_tag: anyMulti ? draft.pair_by_tag : null,
  };
}

// ── Human-readable text ─────────────────────────────────────────────────

export function scopeText(scope: PolicyScope[]): string {
  const filled = scope.filter((s) => s.catalog);
  if (filled.length === 0) return "no catalog yet";
  const byCatalog = new Map<string, string[]>();
  for (const s of filled) byCatalog.set(s.catalog, [...(byCatalog.get(s.catalog) ?? []), s.schema]);
  return [...byCatalog.entries()]
    .map(([cat, schemas]) =>
      schemas.includes("*") ? `${cat} (all schemas)` : schemas.map((s) => `${cat}.${s}`).join(", "),
    )
    .join(", ");
}

function joinOr(items: string[]): string {
  if (items.length <= 1) return items.join("");
  return `${items.slice(0, -1).join(", ")} or ${items[items.length - 1]}`;
}

export function tableConditionText(tc: TableCondition): string | null {
  if (tc.kind === "none") return null;
  if (tc.kind === "tags") return tc.tags.length ? `tagged ${joinOr(tc.tags)}` : "matching selected tags";
  return "matching a custom condition";
}

export function conditionDetail(cond: TableCondition | ColumnCondition): string {
  if (cond.kind === "none") return "Any table in scope";
  if (cond.kind === "tags") return cond.tags.length ? `Tagged ${joinOr(cond.tags)}` : "No tags selected";
  return cond.expr.rows.length ? printExpr(cond.expr) : "No conditions";
}

/** e.g. "Applies 2 rules to columns tagged class=email in prod (all schemas)". */
export function summarize(draft: PolicyDraft): string {
  const rules = selectedRules(draft);
  const verb = draft.action === "auto_apply" ? "Applies" : "Suggests";
  const what = rules.length === 1 ? `"${rules[0].name}"` : `${rules.length} rules`;

  const slotted = rules.filter((r) => r.slots.length > 0);
  let target: string;
  if (slotted.length === 0) {
    target = "to tables";
  } else {
    const conds = slotted.flatMap((r) => r.slots.map((s) => columnCondition(draft, r.rule_id, s.name)));
    if (conds.some((c) => c.kind === "builder")) {
      target = "to columns matching custom conditions";
    } else {
      const tags = [...new Set(conds.flatMap((c) => (c.kind === "tags" ? c.tags : [])))];
      target = tags.length ? `to columns tagged ${joinOr(tags)}` : "to columns";
    }
  }
  const tc = tableConditionText(draft.table_condition);
  const tablePart = tc ? ` on tables ${tc}` : "";
  const reg = draft.register_tables ? ", and starts monitoring new matching tables" : "";
  return `${verb} ${what} ${target}${tablePart} in ${scopeText(draft.scope)}${reg}`;
}

// ── Validation ──────────────────────────────────────────────────────────

export function exprErrors(expr: ConditionExpr): string | null {
  if (expr.rows.length === 0) return "Add at least one condition";
  for (const r of expr.rows) {
    if (!r.value.trim()) return "Every condition needs a value";
    if ((r.operator === "has_tag_value" || r.operator === "not_has_tag_value") && !/^[^=]+=.+$/.test(r.value))
      return "Pick a tag value (key=value)";
  }
  return null;
}

function condError(cond: TableCondition | ColumnCondition): string | null {
  if (cond.kind === "none") return null;
  if (cond.kind === "tags") return cond.tags.length ? null : "Select at least one tag";
  return exprErrors(cond.expr);
}

export function validateStep(step: StepKey, draft: PolicyDraft, takenNames: string[]): Errors {
  const e: Errors = {};
  if (step === "rules") {
    const name = draft.name.trim();
    if (!name) e.name = "Name is required";
    else if (takenNames.some((n) => n.toLowerCase() === name.toLowerCase())) e.name = "A policy with this name already exists";
    if (draft.rule_ids.length === 0) e.rules = "Add at least one rule";
  }
  if (step === "where") {
    if (!draft.scope.some((s) => s.catalog)) e.scope = "Pick at least one catalog";
    const seen = new Set<string>();
    draft.scope.forEach((s, i) => {
      if (!s.catalog && draft.scope.length > 1) e[`scope.${i}`] = "Pick a catalog or remove this row";
      const key = `${s.catalog}.${s.schema}`;
      if (s.catalog && seen.has(key)) e[`scope.${i}`] = "Duplicate scope";
      seen.add(key);
      if (s.catalog && s.schema !== "*" && draft.scope.some((o) => o.catalog === s.catalog && o.schema === "*"))
        e[`scope.${i}`] = `Already covered by ${s.catalog} (all schemas)`;
    });
    const tc = condError(draft.table_condition);
    if (tc) e.table_condition = tc;
  }
  if (step === "columns") {
    for (const rule of selectedRules(draft)) {
      for (const slot of rule.slots) {
        const err = condError(columnCondition(draft, rule.rule_id, slot.name));
        if (err) e[`col.${rule.rule_id}.${slot.name}`] = err;
      }
    }
  }
  return e;
}
