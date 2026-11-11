// THROWAWAY: shared mock data + types for the rule-policies UI prototypes.
// See docs/superpowers/specs/2026-11-11-rule-policies-design.md. Not production code.

export type Family = "numeric" | "text" | "temporal" | "boolean" | "any";

export interface MockSlot {
  name: string;
  family: Family;
  cardinality: "one" | "many";
}

export interface MockRule {
  rule_id: string;
  name: string;
  description: string;
  dimension: string;
  status: "approved" | "draft" | "pending_approval";
  slots: MockSlot[];
  owner: string;
}

export type ConditionSubject =
  | "catalog_tag"
  | "schema_tag"
  | "table_tag"
  | "table_owner"
  | "table_name"
  | "column_tag"
  | "column_name"
  | "column_type";

export type ConditionOperator =
  | "has_tag"
  | "not_has_tag"
  | "has_tag_value"
  | "not_has_tag_value"
  | "is"
  | "is_not"
  | "like"
  | "not_like";

export interface ConditionRow {
  combinator: null | "AND" | "OR";
  subject: ConditionSubject;
  operator: ConditionOperator;
  value: string;
}

export interface ConditionExpr {
  rows: ConditionRow[];
}

export type TableCondition =
  | { kind: "none" }
  | { kind: "tags"; tags: string[] }
  | { kind: "builder"; expr: ConditionExpr };

export type ColumnCondition = { kind: "tags"; tags: string[] } | { kind: "builder"; expr: ConditionExpr };

export interface PolicyScope {
  catalog: string;
  schema: string; // "*" = all schemas
}

export interface PolicyDraft {
  policy_id?: string;
  name: string;
  description: string;
  action: "auto_apply" | "suggest";
  rule_ids: string[];
  scope: PolicyScope[];
  register_tables: boolean;
  table_condition: TableCondition;
  /** rule_id -> slot name -> condition */
  column_conditions: Record<string, Record<string, ColumnCondition>>;
  pair_by_tag: string | null;
  owner?: string;
}

export interface PreviewAttachment {
  rule_id: string;
  mapping: Record<string, string[]>; // slot -> columns
  status: "auto" | "ambiguous" | "partial";
  reason: string; // e.g. "matched class=email", "paired by name order_*_date"
}

export interface PreviewTable {
  table_fqn: string;
  monitored: boolean;
  skipped_reason?: string; // e.g. "No APPLY or MANAGE"
  attachments: PreviewAttachment[];
}

export interface PolicyPreview {
  matched_monitored: number;
  attachments: number;
  to_register: number;
  skipped: number;
  ambiguous: number;
  truncated: boolean;
  tables: PreviewTable[];
}

export const CURRENT_USER = "steward@example.com";

export const MOCK_RULES: MockRule[] = [
  {
    rule_id: "r-email",
    name: "Email format is valid",
    description: "Value matches a basic email pattern.",
    dimension: "validity",
    status: "approved",
    slots: [{ name: "email", family: "text", cardinality: "one" }],
    owner: CURRENT_USER,
  },
  {
    rule_id: "r-notnull",
    name: "Value is not null",
    description: "Column has no nulls.",
    dimension: "completeness",
    status: "approved",
    slots: [{ name: "column", family: "any", cardinality: "one" }],
    owner: "data-eng@example.com",
  },
  {
    rule_id: "r-daterange",
    name: "Start before end",
    description: "Start timestamp is earlier than end timestamp.",
    dimension: "consistency",
    status: "approved",
    slots: [
      { name: "start", family: "temporal", cardinality: "one" },
      { name: "end", family: "temporal", cardinality: "one" },
    ],
    owner: CURRENT_USER,
  },
  {
    rule_id: "r-unique",
    name: "Key is unique",
    description: "Combination of key columns is unique.",
    dimension: "uniqueness",
    status: "approved",
    slots: [{ name: "keys", family: "any", cardinality: "many" }],
    owner: CURRENT_USER,
  },
  {
    rule_id: "r-rowcount",
    name: "Table is not empty",
    description: "Table has at least one row.",
    dimension: "completeness",
    status: "approved",
    slots: [],
    owner: CURRENT_USER,
  },
  {
    rule_id: "r-iso2",
    name: "ISO-2 country code",
    description: "Two-letter uppercase ISO country code.",
    dimension: "validity",
    status: "draft",
    slots: [{ name: "country", family: "text", cardinality: "one" }],
    owner: CURRENT_USER,
  },
];

export const MOCK_CATALOGS: Record<string, string[]> = {
  prod: ["sales", "crm", "finance", "hr"],
  dev: ["sandbox", "staging"],
  dqx: ["demo", "bronze", "silver"],
};

export const MOCK_GOVERNED_TAGS: { key: string; values: string[]; description?: string }[] = [
  { key: "class", values: ["email", "phone", "pii", "credit_card", "location", "date"], description: "Data classification" },
  { key: "pii", values: [], description: "Contains personal data" },
  { key: "domain", values: ["sales", "finance", "hr"] },
  { key: "tier", values: ["gold", "silver", "bronze"] },
  { key: "pair", values: ["order", "ship", "contract"], description: "Groups related columns" },
  { key: "key", values: ["primary", "natural"] },
];

export const MOCK_PRINCIPALS = ["data-eng", "finance-analysts", CURRENT_USER, "data-eng@example.com"];

export const SUBJECT_LABEL: Record<ConditionSubject, string> = {
  catalog_tag: "Catalog tag",
  schema_tag: "Schema tag",
  table_tag: "Table tag",
  table_owner: "Table owner",
  table_name: "Table name",
  column_tag: "Column tag",
  column_name: "Column name",
  column_type: "Column type",
};

export const OPERATOR_LABEL: Record<ConditionOperator, string> = {
  has_tag: "has tag",
  not_has_tag: "does not have tag",
  has_tag_value: "has tag value",
  not_has_tag_value: "does not have tag value",
  is: "is",
  is_not: "is not",
  like: "is like",
  not_like: "is not like",
};

export const TABLE_SUBJECTS: ConditionSubject[] = ["catalog_tag", "schema_tag", "table_tag", "table_owner", "table_name"];
export const COLUMN_SUBJECTS: ConditionSubject[] = [...TABLE_SUBJECTS, "column_tag", "column_name", "column_type"];

export function operatorsFor(subject: ConditionSubject): ConditionOperator[] {
  if (subject.endsWith("_tag")) return ["has_tag", "not_has_tag", "has_tag_value", "not_has_tag_value"];
  if (subject === "table_owner" || subject === "column_type") return ["is", "is_not"];
  return ["is", "is_not", "like", "not_like"];
}

/** Print a ConditionExpr in the expression-view syntax. */
export function printExpr(expr: ConditionExpr): string {
  const fn = (r: ConditionRow): string => {
    const level = r.subject.split("_")[0];
    const neg = r.operator.startsWith("not_") || r.operator === "is_not";
    let core: string;
    switch (r.operator) {
      case "has_tag":
      case "not_has_tag":
        core = `hasTag(${level}, '${r.value}')`;
        break;
      case "has_tag_value":
      case "not_has_tag_value": {
        const [k, v = ""] = r.value.split("=");
        core = `hasTagValue(${level}, '${k}', '${v}')`;
        break;
      }
      case "like":
      case "not_like":
        core = `nameLike(${level}, '${r.value}')`;
        break;
      default:
        core =
          r.subject === "table_owner"
            ? `ownerIs('${r.value}')`
            : r.subject === "column_type"
              ? `typeIs('${r.value}')`
              : `nameIs(${level}, '${r.value}')`;
    }
    return neg ? `NOT ${core}` : core;
  };
  return expr.rows.map((r, i) => (i === 0 ? fn(r) : `${r.combinator ?? "AND"} ${fn(r)}`)).join(" ");
}

export function emptyDraft(ruleIds: string[] = []): PolicyDraft {
  return {
    name: "",
    description: "",
    action: "auto_apply",
    rule_ids: ruleIds,
    scope: [{ catalog: "", schema: "*" }],
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: {},
    pair_by_tag: null,
  };
}

export const MOCK_POLICIES: PolicyDraft[] = [
  {
    policy_id: "p-1",
    name: "PII emails in prod",
    description: "Every column tagged class=email in prod gets format + not-null checks.",
    action: "auto_apply",
    rule_ids: ["r-email", "r-notnull"],
    scope: [{ catalog: "prod", schema: "*" }],
    register_tables: false,
    table_condition: { kind: "none" },
    column_conditions: {
      "r-email": { email: { kind: "tags", tags: ["class=email"] } },
      "r-notnull": { column: { kind: "tags", tags: ["class=email"] } },
    },
    pair_by_tag: null,
    owner: CURRENT_USER,
  },
  {
    policy_id: "p-2",
    name: "Gold-tier date ranges",
    description: "",
    action: "suggest",
    rule_ids: ["r-daterange"],
    scope: [
      { catalog: "prod", schema: "sales" },
      { catalog: "prod", schema: "finance" },
    ],
    register_tables: true,
    table_condition: { kind: "tags", tags: ["tier=gold"] },
    column_conditions: {
      "r-daterange": {
        start: {
          kind: "builder",
          expr: { rows: [{ combinator: null, subject: "column_name", operator: "like", value: "%start%" }] },
        },
        end: {
          kind: "builder",
          expr: { rows: [{ combinator: null, subject: "column_name", operator: "like", value: "%end%" }] },
        },
      },
    },
    pair_by_tag: "pair",
    owner: "data-eng@example.com",
  },
];

/** Fake, deterministic preview so prototypes can show a realistic result panel. */
export function mockPreview(draft: PolicyDraft): PolicyPreview {
  const scoped = draft.scope.filter((s) => s.catalog);
  if (scoped.length === 0 || draft.rule_ids.length === 0) {
    return { matched_monitored: 0, attachments: 0, to_register: 0, skipped: 0, ambiguous: 0, truncated: false, tables: [] };
  }
  const tables: PreviewTable[] = [];
  for (const s of scoped) {
    const schemas = s.schema === "*" ? (MOCK_CATALOGS[s.catalog] ?? []) : [s.schema];
    schemas.forEach((schema, si) => {
      ["customers", "orders", "contracts"].forEach((t, ti) => {
        const fqn = `${s.catalog}.${schema}.${t}`;
        const monitored = (si + ti) % 3 !== 2;
        if (!monitored && !draft.register_tables) return;
        const skipped = si === 1 && ti === 2 ? "No APPLY or MANAGE" : undefined;
        const attachments: PreviewAttachment[] = draft.rule_ids.flatMap((rid): PreviewAttachment[] => {
          const rule = MOCK_RULES.find((r) => r.rule_id === rid);
          if (!rule) return [];
          if (rule.slots.length === 0) return [{ rule_id: rid, mapping: {}, status: "auto" as const, reason: "table-level" }];
          if (rule.slots.length >= 2) {
            return t === "orders"
              ? [
                  {
                    rule_id: rid,
                    mapping: { start: ["order_start_date"], end: ["order_end_date"] },
                    status: "auto" as const,
                    reason: "paired by name order_*_date",
                  },
                  {
                    rule_id: rid,
                    mapping: { start: ["ship_start_ts"], end: ["ship_end_ts", "promised_end_ts"] },
                    status: "ambiguous" as const,
                    reason: "2 candidates for end",
                  },
                ]
              : [{ rule_id: rid, mapping: { start: ["valid_from"] }, status: "partial" as const, reason: "no candidate for end" }];
          }
          const col = t === "customers" ? ["email", "contact_email"] : ["billing_email"];
          return col.map((c) => ({
            rule_id: rid,
            mapping: { [rule.slots[0].name]: rule.slots[0].cardinality === "many" ? col : [c] },
            status: "auto" as const,
            reason: "matched class=email",
          }));
        });
        tables.push({ table_fqn: fqn, monitored, skipped_reason: skipped, attachments });
      });
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
