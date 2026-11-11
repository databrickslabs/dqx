import { describe, expect, test } from "bun:test";
import type { TFunction } from "i18next";
import type { RegistryRuleOut, RulePolicyOut, RuleSlot } from "@/lib/api";
import en from "@/lib/i18n/locales/en.json";
import {
  ALL,
  alignColumnConditions,
  customPrefill,
  defaultColumnConditions,
  draftFromPolicy,
  emptyPolicyDraft,
  normalizeAll,
  pruneScope,
  scopeSummary,
  slotPlaceholder,
  toCreatePayload,
  toPreviewPayload,
  toUpdatePayload,
  validateDraft,
  type PolicyDraft,
} from "./policy-form-state";

type Node = string | { [k: string]: Node };

function template(key: string): string | undefined {
  let node: Node | undefined = en as Node;
  for (const part of key.split(".")) {
    if (typeof node !== "object" || node === null) return undefined;
    node = node[part];
  }
  return typeof node === "string" ? node : undefined;
}

/** Minimal `t` backed by the real en.json bundle. */
const t = ((key: string, vars?: Record<string, unknown>): string => {
  const tmpl = template(key);
  if (tmpl === undefined) return key;
  return tmpl.replace(/\{\{(\w+)\}\}/g, (_m, name: string) => String(vars?.[name] ?? ""));
}) as unknown as TFunction;

function makeRule(ruleId: string, slots: RuleSlot[], slotTags: Record<string, string[]> = {}): RegistryRuleOut {
  return {
    rule_id: ruleId,
    mode: "sql",
    status: "approved",
    version: 1,
    display_status: "approved",
    definition: { slots } as RegistryRuleOut["definition"],
    user_metadata: { name: ruleId, slot_tags: slotTags },
  } as RegistryRuleOut;
}

const emailRule = makeRule("r-email", [{ name: "emails", family: "text" }], { emails: ["class=email"] });
const rangeRule = makeRule("r-range", [
  { name: "start", family: "temporal" },
  { name: "end", family: "temporal" },
]);
const tableRule = makeRule("r-rows", []);

function namedDraft(rule: RegistryRuleOut, name = "PII emails"): PolicyDraft {
  return { ...emptyPolicyDraft(rule), name };
}

describe("emptyPolicyDraft / defaultColumnConditions", () => {
  test("new draft scopes everything with no table condition", () => {
    const d = emptyPolicyDraft(emailRule);
    expect(d.ruleId).toBe("r-email");
    expect(d.policyId).toBeNull();
    expect(d.name).toBe("");
    expect(d.scope).toEqual({ catalogs: [ALL], schemas: [ALL], tables: [ALL] });
    expect(d.registerTables).toBe(false);
    expect(d.tableCondition).toEqual({ kind: "none" });
  });

  test("slot with tags prefills a tags condition from that slot's own tags", () => {
    expect(defaultColumnConditions(emailRule)).toEqual({ emails: { kind: "tags", tags: ["class=email"] } });
  });

  test("slot without tags falls back to the custom prefill", () => {
    const conds = defaultColumnConditions(rangeRule);
    expect(Object.keys(conds)).toEqual(["start", "end"]);
    expect(conds.start).toEqual({
      kind: "builder",
      rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%start%" }],
    });
  });

  test("whole-table rule has no column conditions", () => {
    expect(defaultColumnConditions(tableRule)).toEqual({});
  });
});

describe("alignColumnConditions", () => {
  test("keeps conditions for current slots, defaults new slots, drops removed ones", () => {
    const stored = {
      start: { kind: "tags" as const, tags: ["kind=start"] },
      gone: { kind: "tags" as const, tags: ["x"] },
    };
    expect(alignColumnConditions(rangeRule, stored)).toEqual({
      start: { kind: "tags", tags: ["kind=start"] },
      end: {
        kind: "builder",
        rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%end%" }],
      },
    });
  });
});

describe("customPrefill", () => {
  test("has tag <slot tag> OR name is like %slot%", () => {
    expect(customPrefill(emailRule, "emails")).toEqual({
      kind: "builder",
      rows: [
        { combinator: null, object: "column", predicate: "has_tag", value: "class=email" },
        { combinator: "OR", object: "column", predicate: "name_like", value: "%email%" },
      ],
    });
  });

  test("only the name row when the slot has no tags", () => {
    expect(customPrefill(rangeRule, "end")).toEqual({
      kind: "builder",
      rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%end%" }],
    });
  });
});

describe("slotPlaceholder", () => {
  test("plural slot names drop the trailing s", () => {
    expect(slotPlaceholder("keys", "name_like")).toBe("%key%");
    expect(slotPlaceholder("keys", "has_name")).toBe("key");
  });

  test("short names keep their s", () => {
    expect(slotPlaceholder("ids", "name_like")).toBe("%ids%");
  });

  test("singular slot names are used as-is", () => {
    expect(slotPlaceholder("email", "has_name")).toBe("email");
    expect(slotPlaceholder("email", "name_like")).toBe("%email%");
  });
});

describe("scope helpers", () => {
  test("picking All clears the rest; picking a value clears All; empty falls back to All", () => {
    expect(normalizeAll(["prod"], ["prod", ALL])).toEqual([ALL]);
    expect(normalizeAll([ALL], [ALL, "prod"])).toEqual(["prod"]);
    expect(normalizeAll(["prod"], [])).toEqual([ALL]);
  });

  test("narrowing catalogs drops schemas and tables outside them", () => {
    const pruned = pruneScope({
      catalogs: ["prod"],
      schemas: ["prod.crm", "dev.crm"],
      tables: ["prod.crm.a", "dev.crm.b"],
    });
    expect(pruned).toEqual({ catalogs: ["prod"], schemas: ["prod.crm"], tables: ["prod.crm.a"] });
  });

  test("all catalogs resets schemas and tables to all", () => {
    expect(pruneScope({ catalogs: [ALL], schemas: ["prod.crm"], tables: ["prod.crm.a"] })).toEqual({
      catalogs: [ALL],
      schemas: [ALL],
      tables: [ALL],
    });
  });

  test("tables under all schemas only need a selected catalog", () => {
    expect(pruneScope({ catalogs: ["prod"], schemas: [ALL], tables: ["prod.x.a", "dev.x.b"] })).toEqual({
      catalogs: ["prod"],
      schemas: [ALL],
      tables: ["prod.x.a"],
    });
  });
});

describe("toCreatePayload / toUpdatePayload", () => {
  test("create payload lists every rule and its own column conditions", () => {
    const draft: PolicyDraft = {
      ...namedDraft(emailRule, "  PII emails  "),
      description: "",
      scope: { catalogs: ["prod"], schemas: [ALL], tables: [ALL] },
      tableCondition: { kind: "tags", tags: ["tier=gold"] },
    };
    const extra = { ruleId: "r-range", columnConditions: defaultColumnConditions(rangeRule) };
    const payload = toCreatePayload(draft, [extra]);
    expect(payload.rule_ids).toEqual(["r-email", "r-range"]);
    expect(payload.policy.name).toBe("PII emails");
    expect(payload.policy.description).toBeNull();
    expect(payload.policy.scope).toEqual({ catalogs: ["prod"], schemas: "*", tables: "*" });
    expect(payload.policy.table_condition).toEqual({ kind: "tags", tags: ["tier=gold"] });
    expect(payload.policy.column_conditions).toEqual({ emails: { kind: "tags", tags: ["class=email"] } });
    expect(payload.column_conditions_by_rule).toEqual({
      "r-email": { emails: { kind: "tags", tags: ["class=email"] } },
      "r-range": {
        start: {
          kind: "builder",
          expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%start%" }] },
        },
        end: {
          kind: "builder",
          expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%end%" }] },
        },
      },
    });
  });

  test("copies-only create payload leaves out the draft's own rule", () => {
    const extra = { ruleId: "r-rows", columnConditions: {} };
    const payload = toCreatePayload(namedDraft(emailRule), [extra], { includeOwnRule: false });
    expect(payload.rule_ids).toEqual(["r-rows"]);
    expect(Object.keys(payload.column_conditions_by_rule ?? {})).toEqual(["r-rows"]);
  });

  test("update payload converts builder rows and trims values", () => {
    const draft: PolicyDraft = {
      ...namedDraft(emailRule),
      description: " Emails ",
      registerTables: true,
      tableCondition: {
        kind: "builder",
        rows: [
          { combinator: null, object: "table", predicate: "has_owner", value: " data-eng " },
          { combinator: "AND", object: "schema", predicate: "name_like", value: "%crm%" },
        ],
      },
    };
    expect(toUpdatePayload(draft)).toEqual({
      name: "PII emails",
      description: "Emails",
      scope: { catalogs: "*", schemas: "*", tables: "*" },
      register_tables: true,
      table_condition: {
        kind: "builder",
        expr: {
          rows: [
            { combinator: null, object: "table", predicate: "has_owner", value: "data-eng" },
            { combinator: "AND", object: "schema", predicate: "name_like", value: "%crm%" },
          ],
        },
      },
      column_conditions: { emails: { kind: "tags", tags: ["class=email"] } },
    });
  });

  test("preview payload targets the draft's rule", () => {
    const payload = toPreviewPayload(namedDraft(emailRule));
    expect(payload.rule_id).toBe("r-email");
    expect(payload.policy.name).toBe("PII emails");
  });

  test("a stored policy round-trips through the draft", () => {
    const stored: RulePolicyOut = {
      policy_id: "p-1",
      rule_id: "r-email",
      name: "CRM emails",
      description: null,
      scope: { catalogs: ["prod"], schemas: ["prod.crm"], tables: "*" },
      register_tables: true,
      table_condition: { kind: "none", tags: [], expr: null },
      column_conditions: {
        emails: {
          kind: "builder",
          tags: [],
          expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%email%" }] },
        },
      },
      tables_matched: 4,
    };
    const draft = draftFromPolicy(stored);
    expect(draft.policyId).toBe("p-1");
    expect(draft.description).toBe("");
    expect(draft.scope).toEqual({ catalogs: ["prod"], schemas: ["prod.crm"], tables: [ALL] });
    expect(draft.tableCondition).toEqual({ kind: "none" });
    const back = toUpdatePayload(draft);
    expect(back.scope).toEqual(stored.scope);
    expect(back.column_conditions).toEqual({
      emails: {
        kind: "builder",
        expr: { rows: [{ combinator: null, object: "column", predicate: "name_like", value: "%email%" }] },
      },
    });
  });
});

describe("validateDraft", () => {
  test("a complete draft has no errors", () => {
    expect(validateDraft(namedDraft(emailRule), [])).toEqual({});
  });

  test("name is required", () => {
    expect(validateDraft(namedDraft(emailRule, "   "), [])).toEqual({ name: "rulePolicies.errors.nameRequired" });
  });

  test("names are unique per rule, case-insensitively", () => {
    expect(validateDraft(namedDraft(emailRule, "PII Emails "), ["pii emails"])).toEqual({
      name: "rulePolicies.errors.nameTaken",
    });
  });

  test("narrowing by schema or table needs at least one catalog", () => {
    const draft: PolicyDraft = {
      ...namedDraft(emailRule),
      scope: { catalogs: [ALL], schemas: ["prod.crm"], tables: [ALL] },
    };
    expect(validateDraft(draft, [])).toEqual({ scope: "rulePolicies.errors.scopeNeedsCatalog" });
    expect(validateDraft({ ...draft, scope: { catalogs: [], schemas: [ALL], tables: [ALL] } }, [])).toEqual({
      scope: "rulePolicies.errors.scopeNeedsCatalog",
    });
  });

  test("tag conditions need a tag", () => {
    const draft: PolicyDraft = { ...namedDraft(emailRule), tableCondition: { kind: "tags", tags: [] } };
    expect(validateDraft(draft, [])).toEqual({ tableCondition: "rulePolicies.errors.tagsRequired" });
  });

  test("builder rows need values, for table and column conditions", () => {
    const draft: PolicyDraft = {
      ...namedDraft(emailRule),
      tableCondition: {
        kind: "builder",
        rows: [{ combinator: null, object: "table", predicate: "has_tag", value: "" }],
      },
      columnConditions: {
        emails: {
          kind: "builder",
          rows: [{ combinator: null, object: "column", predicate: "has_name", value: "  " }],
        },
      },
    };
    expect(validateDraft(draft, [])).toEqual({
      tableCondition: "rulePolicies.errors.valuesRequired",
      "column:r-email:emails": "rulePolicies.errors.valuesRequired",
    });
  });

  test("extra rules' column conditions are validated too", () => {
    const extra = { ruleId: "r-range", columnConditions: { start: { kind: "tags" as const, tags: [] } } };
    expect(validateDraft(namedDraft(emailRule), [], [extra])).toEqual({
      "column:r-range:start": "rulePolicies.errors.tagsRequired",
    });
  });
});

describe("scopeSummary", () => {
  test("everything", () => {
    expect(scopeSummary({ catalogs: [ALL], schemas: [ALL], tables: [ALL] }, t)).toBe("all catalogs · all schemas");
  });

  test("catalogs only", () => {
    expect(scopeSummary({ catalogs: ["prod", "dev"], schemas: [ALL], tables: [ALL] }, t)).toBe(
      "prod, dev · all schemas",
    );
  });

  test("schemas", () => {
    expect(scopeSummary({ catalogs: ["prod"], schemas: ["prod.crm"], tables: [ALL] }, t)).toBe(
      "prod.crm · all tables",
    );
  });

  test("tables", () => {
    expect(scopeSummary({ catalogs: ["prod"], schemas: [ALL], tables: ["prod.crm.a", "prod.crm.b"] }, t)).toBe(
      "prod.crm.a, prod.crm.b",
    );
  });

  test("accepts the stored API scope shape", () => {
    expect(scopeSummary({ catalogs: ["prod"], schemas: "*", tables: "*" }, t)).toBe("prod · all schemas");
    expect(scopeSummary(undefined, t)).toBe("all catalogs · all schemas");
  });
});
