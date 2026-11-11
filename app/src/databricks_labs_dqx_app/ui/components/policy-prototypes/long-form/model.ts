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
export type TableSubject = "catalog_tag" | "schema_tag" | "table_tag" | "table_owner" | "table_name";
export type ColumnSubject = "column_tag" | "column_name";
export type Subject = TableSubject | ColumnSubject;
export type Operator = "has_tag" | "not_has_tag" | "is" | "is_not" | "like" | "not_like";

export interface ConditionRow {
  combinator: null | "AND" | "OR";
  subject: Subject;
  operator: Operator;
  value: string;
}

export interface ConditionExpr {
  rows: ConditionRow[];
}

export const TABLE_SUBJECTS: TableSubject[] = ["catalog_tag", "schema_tag", "table_tag", "table_owner", "table_name"];
export const COLUMN_SUBJECTS: ColumnSubject[] = ["column_tag", "column_name"];

export const SUBJECT_LABEL: Record<Subject, string> = {
  catalog_tag: "Catalog tag",
  schema_tag: "Schema tag",
  table_tag: "Table tag",
  table_owner: "Table owner",
  table_name: "Table name",
  column_tag: "Tag",
  column_name: "Name",
};

export function isTagSubject(s: Subject): boolean {
  return s.endsWith("_tag");
}

export function operatorsFor(subject: Subject): Operator[] {
  if (isTagSubject(subject)) return ["has_tag", "not_has_tag"];
  if (subject === "table_owner") return ["is", "is_not"];
  return ["is", "is_not", "like", "not_like"];
}

export function operatorLabel(subject: Subject, op: Operator): string {
  if (subject === "column_tag") return op === "has_tag" ? "is" : "is not";
  const labels: Record<Operator, string> = {
    has_tag: "has tag",
    not_has_tag: "does not have tag",
    is: "is",
    is_not: "is not",
    like: "is like",
    not_like: "is not like",
  };
  return labels[op];
}

export function defaultRow(subject: Subject, combinator: ConditionRow["combinator"] = null): ConditionRow {
  return { combinator, subject, operator: operatorsFor(subject)[0], value: "" };
}

/** Print a ConditionExpr in the expression-view syntax. A bare tag key matches any value. */
export function printExpr(expr: ConditionExpr): string {
  const fn = (r: ConditionRow): string => {
    const level = r.subject.split("_")[0];
    const neg = r.operator === "not_has_tag" || r.operator === "is_not" || r.operator === "not_like";
    let core: string;
    if (isTagSubject(r.subject)) {
      const [k, v] = r.value.split("=");
      core = v === undefined ? `hasTag(${level}, '${k}')` : `hasTagValue(${level}, '${k}', '${v}')`;
    } else if (r.operator === "like" || r.operator === "not_like") {
      core = `nameLike(${level}, '${r.value}')`;
    } else {
      core = r.subject === "table_owner" ? `ownerIs('${r.value}')` : `nameIs(${level}, '${r.value}')`;
    }
    return neg ? `NOT ${core}` : core;
  };
  return expr.rows.map((r, i) => (i === 0 ? fn(r) : `${r.combinator ?? "AND"} ${fn(r)}`)).join(" ");
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

/** Custom-condition prefill: Tag is <rule tag> OR Name is like <slot pattern>. */
export function defaultColumnBuilder(rule: LfRule, slot: MockSlot): ColumnCondition {
  return {
    kind: "builder",
    expr: {
      rows: [
        { combinator: null, subject: "column_tag", operator: "has_tag", value: rule.tags[0] ?? "" },
        { combinator: "OR", subject: "column_name", operator: "like", value: slotNameHint(slot.name, true) },
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
  action: "auto_apply" | "suggest";
  scope: PolicyScope;
  register_tables: boolean;
  table_condition: TableCondition;
  /** slot name -> condition */
  column_conditions: Record<string, ColumnCondition>;
  pair_by_tag: string | null;
}

export function emptyPolicy(ruleId: string): Policy {
  const rule = ruleById(ruleId);
  return {
    rule_id: ruleId,
    name: "",
    description: "",
    action: "auto_apply",
    scope: allScope(),
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: Object.fromEntries((rule?.slots ?? []).map((s) => [s.name, defaultColumnCondition(rule!)])),
    pair_by_tag: null,
  };
}

/** Independent copy of a policy for another rule. Slot conditions carry over by position; extra slots get defaults. */
export function copyForRule(source: Policy, ruleId: string): Policy {
  const target = ruleById(ruleId);
  const sourceSlots = ruleById(source.rule_id)?.slots ?? [];
  const column_conditions: Record<string, ColumnCondition> = {};
  (target?.slots ?? []).forEach((slot, i) => {
    const from = sourceSlots[i] ? source.column_conditions[sourceSlots[i].name] : undefined;
    column_conditions[slot.name] = from ? structuredClone(from) : defaultColumnCondition(target!);
  });
  return {
    ...structuredClone(source),
    policy_id: undefined,
    rule_id: ruleId,
    column_conditions,
    pair_by_tag: (target?.slots.length ?? 0) >= 2 ? source.pair_by_tag : null,
  };
}

export const MOCK_POLICIES: Policy[] = [
  {
    policy_id: "p-1",
    rule_id: "r-email",
    name: "PII emails in prod",
    description: "Every column tagged class=email in prod gets a format check.",
    action: "auto_apply",
    scope: { catalogs: ["prod"], schemas: [ALL], tables: [ALL] },
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: { email: { kind: "tags", tags: ["class=email"] } },
    pair_by_tag: null,
  },
  {
    policy_id: "p-2",
    rule_id: "r-email",
    name: "CRM contact emails",
    description: "",
    action: "suggest",
    scope: { catalogs: ["prod"], schemas: ["prod.crm"], tables: [ALL] },
    register_tables: true,
    table_condition: { kind: "tags", tags: ["tier=gold"] },
    column_conditions: {
      email: {
        kind: "builder",
        expr: {
          rows: [
            { combinator: null, subject: "column_tag", operator: "has_tag", value: "class=email" },
            { combinator: "OR", subject: "column_name", operator: "like", value: "%email%" },
          ],
        },
      },
    },
    pair_by_tag: null,
  },
  {
    policy_id: "p-3",
    rule_id: "r-notnull",
    name: "PII emails in prod",
    description: "",
    action: "auto_apply",
    scope: { catalogs: ["prod"], schemas: [ALL], tables: [ALL] },
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: { column: { kind: "tags", tags: ["class=email"] } },
    pair_by_tag: null,
  },
  {
    policy_id: "p-4",
    rule_id: "r-daterange",
    name: "Gold-tier date ranges",
    description: "",
    action: "suggest",
    scope: { catalogs: ["prod"], schemas: ["prod.sales", "prod.finance"], tables: [ALL] },
    register_tables: true,
    table_condition: { kind: "tags", tags: ["tier=gold"] },
    column_conditions: {
      start: { kind: "builder", expr: { rows: [{ combinator: null, subject: "column_name", operator: "like", value: "%start%" }] } },
      end: { kind: "builder", expr: { rows: [{ combinator: null, subject: "column_name", operator: "like", value: "%end%" }] } },
    },
    pair_by_tag: "pair",
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
