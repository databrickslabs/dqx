import { describe, expect, it } from "bun:test";
import type { ImplementedRuleOut } from "@/lib/api";
import { mappedRuleColumns, sortImplementedRules } from "./ImplementedRulesExplorer";

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

describe("sortImplementedRules", () => {
  const rows = [
    row({ id: "1", rule_id: "r2", rule_name: "Unique", table_fqn: "main.sales.orders" }),
    row({ id: "2", rule_id: "r1", rule_name: "Not null", table_fqn: "main.hr.people" }),
    row({ id: "3", rule_id: "r3", rule_name: undefined, table_fqn: "main.fin.ledger" }),
  ];

  it("orders a table's rules by rule name, falling back to the rule id", () => {
    expect(sortImplementedRules(rows, "rule").map((r) => r.id)).toEqual(["2", "3", "1"]);
  });

  it("orders a rule's tables by table FQN", () => {
    expect(sortImplementedRules(rows, "table").map((r) => r.table_fqn)).toEqual([
      "main.fin.ledger",
      "main.hr.people",
      "main.sales.orders",
    ]);
  });

  it("does not mutate its input", () => {
    const input = [...rows];
    sortImplementedRules(input, "table");
    expect(input).toEqual(rows);
  });
});
