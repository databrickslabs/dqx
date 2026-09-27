import { describe, expect, it } from "bun:test";
import type { ImplementedRuleOut } from "@/lib/api";
import { groupImplementedRules, mappedRuleColumns } from "./ImplementedRulesExplorer";

function row(overrides: Partial<ImplementedRuleOut>): ImplementedRuleOut {
  return {
    binding_id: "b1",
    rule_id: "r1",
    table_fqn: "main.sales.orders",
    rule_name: "Not null",
    column_mapping: [],
    ...overrides,
  } as ImplementedRuleOut;
}

describe("mappedRuleColumns", () => {
  it("dedupes columns across mapping groups", () => {
    expect(mappedRuleColumns([{ col: "id" }, { col: "id" }, { col: "amount" }])).toBe("id, amount");
  });

  it("is empty for a table-level rule", () => {
    expect(mappedRuleColumns(undefined)).toBe("");
  });
});

describe("groupImplementedRules", () => {
  const rows = [
    row({ id: "1", rule_id: "r2", rule_name: "Unique", binding_id: "b1", column_mapping: [{ col: "id" }] }),
    row({ id: "2", rule_id: "r1", rule_name: "Not null", binding_id: "b1" }),
    row({ id: "3", rule_id: "r1", rule_name: "Not null", binding_id: "b2", table_fqn: "main.hr.people" }),
  ];

  it("groups applications by rule, sorted by rule name", () => {
    const groups = groupImplementedRules(rows, "");
    expect(groups.map((g) => [g.name, g.rows.length])).toEqual([
      ["Not null", 2],
      ["Unique", 1],
    ]);
  });

  it("filters on table and column as well as rule name", () => {
    expect(groupImplementedRules(rows, "people").map((g) => g.ruleId)).toEqual(["r1"]);
    expect(groupImplementedRules(rows, "ID").map((g) => g.ruleId)).toEqual(["r2"]);
  });
});
