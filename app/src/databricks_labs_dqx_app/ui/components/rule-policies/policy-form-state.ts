// Pure form state for the rule Policies tab: the editable draft, its
// defaults/prefills, validation, and conversion to and from the API payloads.
// No React, no I/O — everything here is unit tested.
import type { TFunction } from "i18next";
import type {
  ColumnCondition,
  ConditionRow,
  CreateRulePoliciesIn,
  PolicyScope,
  PreviewRulePolicyIn,
  RegistryRuleOut,
  RulePolicyIn,
  RulePolicyOut,
  RulePolicyPreviewTableOut,
  TableCondition,
} from "@/lib/api";
import { slotTagsFromUserMetadata } from "@/lib/registry-rule-conversion";

/** "Everything" at a scope level; also the API's wildcard value. */
export const ALL = "*";

export type TableObject = "catalog" | "schema" | "table";
export type DraftObject = TableObject | "column";
export type Predicate = "has_tag" | "not_has_tag" | "has_name" | "name_like" | "has_owner";
export type Combinator = "AND" | "OR";

export const TABLE_OBJECTS: TableObject[] = ["catalog", "schema", "table"];
export const TABLE_PREDICATES: Predicate[] = ["has_tag", "not_has_tag", "has_name", "name_like", "has_owner"];
export const COLUMN_PREDICATES: Predicate[] = ["has_tag", "not_has_tag", "has_name", "name_like"];

export interface DraftRow {
  combinator: Combinator | null;
  object: DraftObject;
  predicate: Predicate;
  value: string;
}

/** Each level is a list of names or [ALL]. Schemas are "cat.sch", tables "cat.sch.tbl". */
export interface ScopeDraft {
  catalogs: string[];
  schemas: string[];
  tables: string[];
}

export type TableConditionDraft =
  | { kind: "none" }
  | { kind: "tags"; tags: string[] }
  | { kind: "builder"; rows: DraftRow[] };

export type ColumnConditionDraft = { kind: "tags"; tags: string[] } | { kind: "builder"; rows: DraftRow[] };

export type ColumnConditionsDraft = Record<string, ColumnConditionDraft>;

export interface PolicyDraft {
  policyId: string | null;
  ruleId: string;
  name: string;
  description: string;
  scope: ScopeDraft;
  registerTables: boolean;
  tableCondition: TableConditionDraft;
  /** slot name -> condition */
  columnConditions: ColumnConditionsDraft;
}

/** Another rule added in the Rules section: shares the draft's fields, keeps its own column conditions. */
export interface ExtraRuleDraft {
  ruleId: string;
  columnConditions: ColumnConditionsDraft;
}

/** Which kind of value input a predicate takes. */
export function valueKind(predicate: Predicate): "tag" | "name" | "owner" {
  if (predicate === "has_tag" || predicate === "not_has_tag") return "tag";
  return predicate === "has_owner" ? "owner" : "name";
}

export function defaultRow(object: DraftObject, combinator: Combinator | null = null): DraftRow {
  return { combinator, object, predicate: "has_tag", value: "" };
}

function slotTags(rule: RegistryRuleOut, slot: string): string[] {
  return slotTagsFromUserMetadata(rule.user_metadata as Record<string, unknown> | undefined)[slot] ?? [];
}

/** Name placeholder derived from the slot: {{email}} -> "email" / "%email%"; plural "keys" -> "key". */
export function slotPlaceholder(slot: string, predicate: Predicate): string {
  const stem = slot.length > 3 && slot.endsWith("s") ? slot.slice(0, -1) : slot;
  return predicate === "name_like" ? `%${stem}%` : stem;
}

/** Custom-condition prefill for a slot: has tag <slot tag> OR name is like %slot%. */
export function customPrefill(rule: RegistryRuleOut, slot: string): ColumnConditionDraft {
  const tag = slotTags(rule, slot)[0];
  const likeRow: DraftRow = {
    combinator: null,
    object: "column",
    predicate: "name_like",
    value: slotPlaceholder(slot, "name_like"),
  };
  if (!tag) return { kind: "builder", rows: [likeRow] };
  return {
    kind: "builder",
    rows: [{ combinator: null, object: "column", predicate: "has_tag", value: tag }, { ...likeRow, combinator: "OR" }],
  };
}

/** Tags condition from the slot's own tags; a slot without tags starts from the custom prefill. */
export function defaultColumnCondition(rule: RegistryRuleOut, slot: string): ColumnConditionDraft {
  const tags = slotTags(rule, slot);
  return tags.length > 0 ? { kind: "tags", tags: [...tags] } : customPrefill(rule, slot);
}

export function defaultColumnConditions(rule: RegistryRuleOut): ColumnConditionsDraft {
  return Object.fromEntries(
    (rule.definition.slots ?? []).map((slot) => [slot.name, defaultColumnCondition(rule, slot.name)]),
  );
}

/** Column conditions covering exactly the rule's current slots (stored ones kept, new slots defaulted). */
export function alignColumnConditions(rule: RegistryRuleOut, conds: ColumnConditionsDraft): ColumnConditionsDraft {
  return Object.fromEntries(
    (rule.definition.slots ?? []).map((slot) => [slot.name, conds[slot.name] ?? defaultColumnCondition(rule, slot.name)]),
  );
}

export function emptyPolicyDraft(rule: RegistryRuleOut): PolicyDraft {
  return {
    policyId: null,
    ruleId: rule.rule_id,
    name: "",
    description: "",
    scope: { catalogs: [ALL], schemas: [ALL], tables: [ALL] },
    registerTables: false,
    tableCondition: { kind: "none" },
    columnConditions: defaultColumnConditions(rule),
  };
}

// ── Scope ────────────────────────────────────────────────────────────────────

/** "All …" is exclusive: picking it clears the rest, picking anything else clears it; empty falls back to All. */
export function normalizeAll(prev: string[], next: string[]): string[] {
  const hadAll = prev.includes(ALL);
  const hasAll = next.includes(ALL);
  if (hasAll && !hadAll) return [ALL];
  if (hasAll && next.length > 1) return next.filter((v) => v !== ALL);
  return next.length === 0 ? [ALL] : next;
}

const parentOf = (name: string): string => name.slice(0, name.lastIndexOf("."));
const catalogOf = (name: string): string => name.slice(0, name.indexOf("."));

/** Keep child levels consistent after a parent level changes. */
export function pruneScope(scope: ScopeDraft): ScopeDraft {
  if (scope.catalogs.includes(ALL)) return { catalogs: [ALL], schemas: [ALL], tables: [ALL] };
  const catalogs = new Set(scope.catalogs);
  const keptSchemas = scope.schemas.includes(ALL) ? [] : scope.schemas.filter((s) => catalogs.has(catalogOf(s)));
  const schemas = keptSchemas.length > 0 ? keptSchemas : [ALL];
  const schemaSet = new Set(keptSchemas);
  const keptTables = scope.tables.includes(ALL)
    ? []
    : scope.tables.filter((tbl) =>
        schemaSet.size > 0 ? schemaSet.has(parentOf(tbl)) : catalogs.has(catalogOf(tbl)),
      );
  return { catalogs: scope.catalogs, schemas, tables: keptTables.length > 0 ? keptTables : [ALL] };
}

function levelFromApi(value: PolicyScope["catalogs"]): string[] {
  return value === undefined || value === ALL ? [ALL] : [...value];
}

function levelToApi(values: string[]): string[] | "*" {
  return values.includes(ALL) ? ALL : [...values];
}

function toScopeDraft(scope: PolicyScope | ScopeDraft | undefined): ScopeDraft {
  return {
    catalogs: levelFromApi(scope?.catalogs),
    schemas: levelFromApi(scope?.schemas),
    tables: levelFromApi(scope?.tables),
  };
}

/** One-line scope description for policy cards. */
export function scopeSummary(scope: PolicyScope | ScopeDraft | undefined, t: TFunction): string {
  const s = toScopeDraft(scope);
  const level = (values: string[], allKey: string) => (values.includes(ALL) ? t(allKey) : values.join(", "));
  if (!s.tables.includes(ALL)) return s.tables.join(", ");
  if (!s.schemas.includes(ALL)) return `${s.schemas.join(", ")} · ${t("rulePolicies.scopeSummary.allTables")}`;
  return `${level(s.catalogs, "rulePolicies.scopeSummary.allCatalogs")} · ${t("rulePolicies.scopeSummary.allSchemas")}`;
}

// ── Conversion ───────────────────────────────────────────────────────────────

function rowsToApi(rows: DraftRow[]): ConditionRow[] {
  return rows.map((row, i) => ({
    combinator: i === 0 ? null : (row.combinator ?? "AND"),
    object: row.object,
    predicate: row.predicate,
    value: row.value.trim(),
  }));
}

function rowsFromApi(rows: ConditionRow[] | undefined): DraftRow[] {
  return (rows ?? []).map((row) => ({
    combinator: row.combinator ?? null,
    object: row.object,
    predicate: row.predicate,
    value: row.value,
  }));
}

function tableConditionToApi(cond: TableConditionDraft): TableCondition {
  if (cond.kind === "none") return { kind: "none" };
  if (cond.kind === "tags") return { kind: "tags", tags: [...cond.tags] };
  return { kind: "builder", expr: { rows: rowsToApi(cond.rows) } };
}

function columnConditionToApi(cond: ColumnConditionDraft): ColumnCondition {
  if (cond.kind === "tags") return { kind: "tags", tags: [...cond.tags] };
  return { kind: "builder", expr: { rows: rowsToApi(cond.rows) } };
}

function columnConditionsToApi(conds: ColumnConditionsDraft): Record<string, ColumnCondition> {
  return Object.fromEntries(Object.entries(conds).map(([slot, c]) => [slot, columnConditionToApi(c)]));
}

/** The shared policy body (name, scope, conditions) for the draft's own rule. */
export function toUpdatePayload(draft: PolicyDraft): RulePolicyIn {
  const description = draft.description.trim();
  return {
    name: draft.name.trim(),
    description: description || null,
    scope: {
      catalogs: levelToApi(draft.scope.catalogs),
      schemas: levelToApi(draft.scope.schemas),
      tables: levelToApi(draft.scope.tables),
    },
    register_tables: draft.registerTables,
    table_condition: tableConditionToApi(draft.tableCondition),
    column_conditions: columnConditionsToApi(draft.columnConditions),
  };
}

/**
 * One create request for the draft's rule plus every extra rule. With *includeOwnRule* false
 * (editing an existing policy) only the extra rules' copies are created.
 */
export function toCreatePayload(
  draft: PolicyDraft,
  extraRules: ExtraRuleDraft[],
  options: { includeOwnRule?: boolean } = {},
): CreateRulePoliciesIn {
  const includeOwnRule = options.includeOwnRule ?? true;
  const own: ExtraRuleDraft[] = includeOwnRule
    ? [{ ruleId: draft.ruleId, columnConditions: draft.columnConditions }]
    : [];
  const all = [...own, ...extraRules];
  return {
    rule_ids: all.map((r) => r.ruleId),
    policy: toUpdatePayload(draft),
    column_conditions_by_rule: Object.fromEntries(
      all.map((r) => [r.ruleId, columnConditionsToApi(r.columnConditions)]),
    ),
  };
}

export function toPreviewPayload(draft: PolicyDraft): PreviewRulePolicyIn {
  return { rule_id: draft.ruleId, policy: toUpdatePayload(draft) };
}

/** Editable draft from a stored policy. */
export function draftFromPolicy(policy: RulePolicyOut): PolicyDraft {
  const tc = policy.table_condition;
  const tableCondition: TableConditionDraft =
    tc?.kind === "tags"
      ? { kind: "tags", tags: [...(tc.tags ?? [])] }
      : tc?.kind === "builder"
        ? { kind: "builder", rows: rowsFromApi(tc.expr?.rows) }
        : { kind: "none" };
  const columnConditions: ColumnConditionsDraft = Object.fromEntries(
    Object.entries(policy.column_conditions ?? {}).map(([slot, c]) => [
      slot,
      c.kind === "builder"
        ? { kind: "builder" as const, rows: rowsFromApi(c.expr?.rows) }
        : { kind: "tags" as const, tags: [...(c.tags ?? [])] },
    ]),
  );
  return {
    policyId: policy.policy_id,
    ruleId: policy.rule_id,
    name: policy.name,
    description: policy.description ?? "",
    scope: toScopeDraft(policy.scope),
    registerTables: policy.register_tables ?? false,
    tableCondition,
    columnConditions,
  };
}

// ── Validation ───────────────────────────────────────────────────────────────

/** Error key for one rule slot's column condition. */
export function columnErrorKey(ruleId: string, slot: string): string {
  return `column:${ruleId}:${slot}`;
}

function conditionError(cond: TableConditionDraft | ColumnConditionDraft): string | null {
  if (cond.kind === "tags") return cond.tags.length === 0 ? "rulePolicies.errors.tagsRequired" : null;
  if (cond.kind === "builder") {
    if (cond.rows.length === 0 || cond.rows.some((r) => !r.value.trim())) return "rulePolicies.errors.valuesRequired";
  }
  return null;
}

/**
 * Field errors as i18n keys. Keys: *name*, *scope*, *tableCondition*, and
 * columnErrorKey(ruleId, slot) per column condition. *takenNames* are the
 * rule's other policy names (compared trimmed, case-insensitively).
 */
export function validateDraft(
  draft: PolicyDraft,
  takenNames: string[],
  extraRules: ExtraRuleDraft[] = [],
): Record<string, string> {
  const errors: Record<string, string> = {};
  const name = draft.name.trim();
  if (!name) errors.name = "rulePolicies.errors.nameRequired";
  else if (takenNames.some((n) => n.trim().toLowerCase() === name.toLowerCase())) {
    errors.name = "rulePolicies.errors.nameTaken";
  }

  const { catalogs, schemas, tables } = draft.scope;
  const narrowsBelowCatalog = !schemas.includes(ALL) || !tables.includes(ALL);
  if (catalogs.length === 0 || schemas.length === 0 || tables.length === 0) {
    errors.scope = "rulePolicies.errors.scopeNeedsCatalog";
  } else if (narrowsBelowCatalog && catalogs.includes(ALL)) {
    errors.scope = "rulePolicies.errors.scopeNeedsCatalog";
  }

  const tableError = conditionError(draft.tableCondition);
  if (tableError) errors.tableCondition = tableError;

  for (const group of [{ ruleId: draft.ruleId, columnConditions: draft.columnConditions }, ...extraRules]) {
    for (const [slot, cond] of Object.entries(group.columnConditions)) {
      const err = conditionError(cond);
      if (err) errors[columnErrorKey(group.ruleId, slot)] = err;
    }
  }
  return errors;
}

// ── Preview helpers ──────────────────────────────────────────────────────────

/** Distinct columns a previewed table would attach to (mapping values may be comma-joined). */
export function distinctColumnCount(table: RulePolicyPreviewTableOut): number {
  const cols = new Set<string>();
  for (const m of table.mappings) {
    for (const value of Object.values(m.column_mapping)) {
      for (const col of value.split(",")) {
        const trimmed = col.trim();
        if (trimmed) cols.add(trimmed);
      }
    }
  }
  return cols.size;
}

/** Localised "matched on" text for a backend match reason. */
export function reasonLabel(reason: string, t: TFunction): string {
  if (reason === "table-level") return t("rulePolicies.preview.wholeTable");
  if (reason === "matched condition") return t("rulePolicies.preview.matchedCondition");
  if (reason.startsWith("matched ")) return t("rulePolicies.preview.matchedTags", { tags: reason.slice(8) });
  return reason;
}
