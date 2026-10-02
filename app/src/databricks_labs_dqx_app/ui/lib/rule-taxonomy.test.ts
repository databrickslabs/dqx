import { describe, expect, test } from "bun:test";
import type { RegistryRuleOut, RuleSlotFamily } from "@/lib/api";
import {
  compareRuleColumnTypeGroups,
  ruleColumnTypeGroup,
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
