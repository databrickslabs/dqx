import { describe, expect, test } from "bun:test";
import type { RegistryRuleOut, RuleSlotFamily } from "@/lib/api";
import {
  compareRuleColumnTypeGroups,
  datasetCheckFunctionNames,
  ruleColumnTypeGroup,
  ruleLevel,
} from "./rule-taxonomy";

function rule(ruleId: string, families: RuleSlotFamily[]): RegistryRuleOut {
  return {
    rule_id: ruleId,
    mode: "dqx_native",
    polarity: "pass",
    status: "approved",
    version: 1,
    definition: {
      body: { function: ruleId },
      slots: families.map((family, index) => ({
        name: `column_${index}`,
        family,
      })),
    },
  } as RegistryRuleOut;
}

describe("rule column-type taxonomy", () => {
  test("derives stable groups from typed slots", () => {
    expect(ruleColumnTypeGroup(rule("table", []))).toBe("table");
    expect(ruleColumnTypeGroup(rule("universal", ["any"]))).toBe("any");
    expect(ruleColumnTypeGroup(rule("money", ["numeric"]))).toBe("numeric");
    expect(ruleColumnTypeGroup(rule("join", ["text", "numeric"]))).toBe(
      "mixed",
    );
    expect(ruleColumnTypeGroup(rule("same-family", ["text", "text"]))).toBe(
      "text",
    );
  });

  test("orders marketplace categories consistently", () => {
    const rules = [
      rule("table", []),
      rule("date", ["temporal"]),
      rule("universal", ["any"]),
      rule("money", ["numeric"]),
    ];
    rules.sort(compareRuleColumnTypeGroups);
    expect(rules.map((item) => ruleColumnTypeGroup(item))).toEqual([
      "any",
      "numeric",
      "temporal",
      "table",
    ]);
  });
});

describe("ruleLevel — mirrors the backend scoring granularity", () => {
  const datasetFns = datasetCheckFunctionNames([
    { name: "is_unique", rule_type: "dataset" },
    { name: "is_not_null", rule_type: "row" },
  ]);
  const level = (mode: RegistryRuleOut["mode"], body: Record<string, unknown>) =>
    ruleLevel({ mode, definition: { body, slots: [], parameters: [] } } as unknown as RegistryRuleOut, datasetFns);

  test("collects only dataset-registered functions", () => {
    expect([...datasetFns]).toEqual(["is_unique"]);
  });

  test("a dataset-registered native function is table-level", () => {
    expect(level("dqx_native", { function: "is_unique", arguments: {} })).toBe("table");
  });

  test("a row-registered native function is row-level", () => {
    expect(level("dqx_native", { function: "is_not_null", arguments: {} })).toBe("row");
  });

  test("an unknown native function falls back to row-level", () => {
    expect(level("dqx_native", { function: "custom_check", arguments: {} })).toBe("row");
  });

  test("sql_query without merge_columns is table-level", () => {
    expect(level("sql", { sql_query: "SELECT COUNT(*) = 0 AS ok FROM {{t}}" })).toBe("table");
    expect(level("sql", { sql_query: "SELECT 1", merge_columns: [] })).toBe("table");
  });

  test("sql_query with merge_columns is row-level", () => {
    expect(level("sql", { sql_query: "SELECT 1", merge_columns: ["id"] })).toBe("row");
    expect(level("lowcode", { sql_query: "SELECT 1", merge_columns: ["region"], group_by: "region" })).toBe("row");
  });

  test("a SQL or low-code predicate is row-level", () => {
    expect(level("sql", { predicate: "amount > 0" })).toBe("row");
    expect(level("lowcode", { predicate: "amount > 0" })).toBe("row");
  });
});
