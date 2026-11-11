// THROWAWAY prototype — variant A model: a policy is 1:1 with a rule. Adapts ../mock.ts.
import { CURRENT_USER, MOCK_CATALOGS, MOCK_RULES, type MockRule, type MockSlot, type PreviewAttachment, type PreviewTable, type PolicyPreview } from "../mock";

export { CURRENT_USER, MOCK_CATALOGS };
export type { MockSlot, PreviewAttachment, PolicyPreview, PreviewTable };

// ── Rules (with tags used to prefill column conditions) ──────────────────────
export interface LfRule extends MockRule {
  tags: string[];
}

const RULE_TAGS: Record<string, string[]> = {
  "r-email": ["class=email"],
  "r-notnull": [],
  "r-daterange": ["class=date"],
  "r-unique": ["key=primary"],
  "r-rowcount": [],
  "r-iso2": ["class=location"],
};

export const RULES: LfRule[] = MOCK_RULES.map((r) => ({ ...r, tags: RULE_TAGS[r.rule_id] ?? [] }));

export function ruleById(id: string): LfRule | undefined {
  return RULES.find((r) => r.rule_id === id);
}

// ── Scope ────────────────────────────────────────────────────────────────────
export const ALL = "*";

/** Each level is a list of values, or [ALL]. Schemas are "cat.sch", tables "cat.sch.tbl". */
export interface PolicyScope {
  catalogs: string[];
  schemas: string[];
  tables: string[];
}

export const MOCK_TABLE_NAMES = ["customers", "orders", "contracts"];

export function allScope(): PolicyScope {
  return { catalogs: [ALL], schemas: [ALL], tables: [ALL] };
}

export function schemasFor(catalogs: string[]): string[] {
  const cats = catalogs.includes(ALL) ? Object.keys(MOCK_CATALOGS) : catalogs;
  return cats.flatMap((c) => (MOCK_CATALOGS[c] ?? []).map((s) => `${c}.${s}`));
}

export function tablesFor(scope: Pick<PolicyScope, "catalogs" | "schemas">): string[] {
  const schemas = scope.schemas.includes(ALL) ? schemasFor(scope.catalogs) : scope.schemas;
  return schemas.flatMap((s) => MOCK_TABLE_NAMES.map((t) => `${s}.${t}`));
}

/** Tables the scope resolves to. */
export function scopedTables(scope: PolicyScope): string[] {
  return scope.tables.includes(ALL) ? tablesFor(scope) : scope.tables;
}

/** "All …" is exclusive: picking it clears the rest, picking anything else clears it; empty falls back to All. */
export function normalizeAll(prev: string[], next: string[]): string[] {
  const hadAll = prev.includes(ALL);
  const hasAll = next.includes(ALL);
  if (hasAll && !hadAll) return [ALL];
  if (hasAll && next.length > 1) return next.filter((v) => v !== ALL);
  return next.length === 0 ? [ALL] : next;
}

/** Keep the scope consistent after a parent level changes. */
export function pruneScope(scope: PolicyScope): PolicyScope {
  const validSchemas = new Set(schemasFor(scope.catalogs));
  const schemas = scope.schemas.includes(ALL) ? [ALL] : scope.schemas.filter((s) => validSchemas.has(s));
  const next = { ...scope, schemas: schemas.length ? schemas : [ALL] };
  const validTables = new Set(tablesFor(next));
  const tables = scope.tables.includes(ALL) ? [ALL] : scope.tables.filter((t) => validTables.has(t));
  return { ...next, tables: tables.length ? tables : [ALL] };
}

export function scopeSummary(scope: PolicyScope): string {
  const level = (values: string[], all: string) => (values.includes(ALL) ? all : values.join(", "));
  if (!scope.tables.includes(ALL)) return level(scope.tables, "all tables");
  if (!scope.schemas.includes(ALL)) return `${level(scope.schemas, "all schemas")} · all tables`;
  return `${level(scope.catalogs, "all catalogs")} · all schemas`;
}

// ── Conditions ───────────────────────────────────────────────────────────────
/** Table conditions pick the object; column conditions are always about the column. */
export type TableObject = "catalog" | "schema" | "table";
export type ConditionObject = TableObject | "column";
export type Predicate = "has_tag" | "not_has_tag" | "has_name" | "name_like" | "has_owner";

export interface ConditionRow {
  combinator: null | "AND" | "OR";
  object: ConditionObject;
  predicate: Predicate;
  value: string;
}

export interface ConditionExpr {
  rows: ConditionRow[];
}

export const TABLE_OBJECTS: TableObject[] = ["catalog", "schema", "table"];
export const TABLE_PREDICATES: Predicate[] = ["has_tag", "not_has_tag", "has_name", "name_like", "has_owner"];
export const COLUMN_PREDICATES: Predicate[] = ["has_tag", "not_has_tag", "has_name", "name_like"];

export const OBJECT_LABEL: Record<ConditionObject, string> = {
  catalog: "Catalog",
  schema: "Schema",
  table: "Table",
  column: "Column",
};

export const PREDICATE_LABEL: Record<Predicate, string> = {
  has_tag: "has tag",
  not_has_tag: "does not have tag",
  has_name: "has name",
  name_like: "name is like",
  has_owner: "has owner",
};

/** Which kind of value input a predicate takes. */
export function valueKind(p: Predicate): "tag" | "name" | "owner" {
  if (p === "has_tag" || p === "not_has_tag") return "tag";
  return p === "has_owner" ? "owner" : "name";
}

export function defaultRow(object: ConditionObject, combinator: ConditionRow["combinator"] = null): ConditionRow {
  return { combinator, object, predicate: "has_tag", value: "" };
}

export type TableCondition =
  | { kind: "none" }
  | { kind: "tags"; tags: string[] }
  | { kind: "builder"; expr: ConditionExpr };

export type ColumnCondition = { kind: "tags"; tags: string[] } | { kind: "builder"; expr: ConditionExpr };

/** Name placeholder / pattern derived from the slot name, e.g. {{email}} → "email" / "%email%". */
export function slotNameHint(slotName: string, like: boolean): string {
  const stem = slotName.length > 3 && slotName.endsWith("s") ? slotName.slice(0, -1) : slotName;
  return like ? `%${stem}%` : stem;
}

export function defaultColumnCondition(rule: LfRule): ColumnCondition {
  return { kind: "tags", tags: [...rule.tags] };
}

/** Custom-condition prefill: has tag <rule tag> OR name is like <slot pattern>. */
export function defaultColumnBuilder(rule: LfRule, slot: MockSlot): ColumnCondition {
  return {
    kind: "builder",
    expr: {
      rows: [
        { combinator: null, object: "column", predicate: "has_tag", value: rule.tags[0] ?? "" },
        { combinator: "OR", object: "column", predicate: "name_like", value: slotNameHint(slot.name, true) },
      ],
    },
  };
}

// ── Policy ───────────────────────────────────────────────────────────────────
export interface Policy {
  policy_id?: string;
  rule_id: string;
  name: string;
  description: string;
  scope: PolicyScope;
  register_tables: boolean;
  table_condition: TableCondition;
  /** slot name -> condition */
  column_conditions: Record<string, ColumnCondition>;
}

export function emptyPolicy(ruleId: string): Policy {
  const rule = ruleById(ruleId);
  return {
    rule_id: ruleId,
    name: "",
    description: "",
    scope: allScope(),
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: defaultColumnConditions(rule),
  };
}

/** Column-condition prefill for a rule, same as a new policy for it: selected-tags with the rule's own tags per slot. */
export function defaultColumnConditions(rule: LfRule | undefined): Record<string, ColumnCondition> {
  return Object.fromEntries((rule?.slots ?? []).map((s) => [s.name, defaultColumnCondition(rule!)]));
}

/**
 * Independent copy of a policy for another rule. Shared fields (name, description, scope, table condition,
 * register flag) carry over; column conditions are prefilled from the target rule's own tags and slots.
 */
export function copyForRule(source: Policy, ruleId: string): Policy {
  return {
    ...structuredClone(source),
    policy_id: undefined,
    rule_id: ruleId,
    column_conditions: defaultColumnConditions(ruleById(ruleId)),
  };
}

export const MOCK_POLICIES: Policy[] = [
  {
    policy_id: "p-1",
    rule_id: "r-email",
    name: "PII emails in prod",
    description: "Every column tagged class=email in prod gets a format check.",
    scope: { catalogs: ["prod"], schemas: [ALL], tables: [ALL] },
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: { email: { kind: "tags", tags: ["class=email"] } },
  },
  {
    policy_id: "p-2",
    rule_id: "r-email",
    name: "CRM contact emails",
    description: "",
    scope: { catalogs: ["prod"], schemas: ["prod.crm"], tables: [ALL] },
    register_tables: true,
    table_condition: { kind: "tags", tags: ["tier=gold"] },
    column_conditions: {
      email: {
        kind: "builder",
        expr: {
          rows: [
            { combinator: null, object: "column", predicate: "has_tag", value: "class=email" },
            { combinator: "OR", object: "column", predicate: "name_like", value: "%email%" },
          ],
        },
      },
    },
  },
  {
    policy_id: "p-3",
    rule_id: "r-notnull",
    name: "PII emails in prod",
    description: "",
    scope: { catalogs: ["prod"], schemas: [ALL], tables: [ALL] },
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: { column: { kind: "tags", tags: ["class=email"] } },
  },
  {
    policy_id: "p-4",
    rule_id: "r-daterange",
    name: "Gold-tier date ranges",
    description: "",
    scope: { catalogs: ["prod"], schemas: ["prod.sales", "prod.finance"], tables: [ALL] },
    register_tables: true,
    table_condition: {
      kind: "builder",
      expr: {
        rows: [
          { combinator: null, object: "table", predicate: "has_tag", value: "tier=gold" },
          { combinator: "AND", object: "schema", predicate: "has_owner", value: "data-eng" },
        ],
      },
    },
    column_conditions: {
      start: { kind: "builder", expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%start%" }] } },
      end: { kind: "builder", expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%end%" }] } },
    },
  },
];

// ── Preview (fake, deterministic) ───────────────────────────────────────────
export function mockPreview(policy: Policy): PolicyPreview {
  const rule = ruleById(policy.rule_id);
  const tables: PreviewTable[] = [];
  if (rule) {
    scopedTables(policy.scope).forEach((fqn, i) => {
      const t = fqn.split(".")[2];
      const monitored = i % 3 !== 2;
      if (!monitored && !policy.register_tables) return;
      const skipped = i % 7 === 5 ? "No APPLY or MANAGE" : undefined;
      let attachments: PreviewAttachment[];
      if (rule.slots.length === 0) {
        attachments = [{ rule_id: rule.rule_id, mapping: {}, status: "auto", reason: "table-level" }];
      } else if (rule.slots.length >= 2) {
        attachments =
          t === "orders"
            ? [
                {
                  rule_id: rule.rule_id,
                  mapping: { start: ["order_start_date"], end: ["order_end_date"] },
                  status: "auto",
                  reason: "paired by name order_*_date",
                },
                {
                  rule_id: rule.rule_id,
                  mapping: { start: ["ship_start_ts"], end: ["ship_end_ts", "promised_end_ts"] },
                  status: "ambiguous",
                  reason: "2 candidates for end",
                },
              ]
            : [{ rule_id: rule.rule_id, mapping: { start: ["valid_from"] }, status: "partial", reason: "no candidate for end" }];
      } else {
        const slot = rule.slots[0];
        const stem = slotNameHint(slot.name, false);
        const cols = t === "customers" ? [stem, `contact_${stem}`] : [`billing_${stem}`];
        const reason = rule.tags[0] ? `matched ${rule.tags[0]}` : `name like ${slotNameHint(slot.name, true)}`;
        attachments =
          slot.cardinality === "many"
            ? [{ rule_id: rule.rule_id, mapping: { [slot.name]: cols }, status: "auto", reason }]
            : cols.map((c) => ({ rule_id: rule.rule_id, mapping: { [slot.name]: [c] }, status: "auto" as const, reason }));
      }
      tables.push({ table_fqn: fqn, monitored, skipped_reason: skipped, attachments });
    });
  }
  const active = tables.filter((t) => !t.skipped_reason);
  return {
    matched_monitored: active.filter((t) => t.monitored).length,
    attachments: active.reduce((n, t) => n + t.attachments.filter((a) => a.status === "auto").length, 0),
    to_register: active.filter((t) => !t.monitored).length,
    skipped: tables.length - active.length,
    ambiguous: active.reduce((n, t) => n + t.attachments.filter((a) => a.status === "ambiguous").length, 0),
    truncated: false,
    tables,
  };
}

/** Mock of the rule's Permissions: policies are editable by anyone with MODIFY on their rule. */
export function canModifyRule(ruleId: string): boolean {
  return MOCK_RULES.find((r) => r.rule_id === ruleId)?.owner === CURRENT_USER;
}
