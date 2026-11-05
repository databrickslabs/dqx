import { describe, expect, test } from "bun:test";

import { computeStatus, statusNeedsAttention } from "./RuleConfigCard";
import type { AppliedRuleOut, RuleParameter, RuleSlot } from "@/lib/api";

const slots: RuleSlot[] = [{ name: "col", family: "any", position: 0, cardinality: "one" }];
const listParam: RuleParameter[] = [{ name: "allowed", type: "list", value: null }];

function rule(partial: Partial<AppliedRuleOut>): AppliedRuleOut {
  return { binding_id: "b", rule_id: "r", indices: [], ...partial };
}

describe("computeStatus", () => {
  test("a rule with column slots and nothing mapped needs attention", () => {
    for (const column_mapping of [[], [{}]]) {
      const status = computeStatus(rule({ column_mapping }), slots, [], listParam, false, ["allowed"]);
      expect(status.kind).toBe("incomplete");
      expect(status.incompleteGroupCount).toBe(1);
      expect(status.totalGroupCount).toBe(1);
      expect(statusNeedsAttention(status)).toBe(true);
    }
  });

  test("filling a parameter alone doesn't complete an unmapped rule", () => {
    const status = computeStatus(
      rule({ column_mapping: [], parameter_values: { "{}": { allowed: ["qwe"] } } }),
      slots,
      [],
      listParam,
      false,
      ["allowed"],
    );
    expect(status.kind).toBe("incomplete");
  });

  test("mapped column with its parameter filled is complete", () => {
    const group = { col: "account_tier" };
    const status = computeStatus(
      rule({ column_mapping: [group], parameter_values: { [JSON.stringify(group)]: { allowed: ["Free"] } } }),
      slots,
      [],
      listParam,
      false,
      ["allowed"],
    );
    expect(status.kind).toBe("complete");
  });

  test("a rule with no column slots checks its parameters on the blank mapping", () => {
    const missing = computeStatus(rule({ column_mapping: [] }), [], [], listParam, false, ["allowed"]);
    expect(missing.kind).toBe("incomplete");
    const filled = computeStatus(
      rule({ column_mapping: [], parameter_values: { "{}": { allowed: ["Free"] } } }),
      [],
      [],
      listParam,
      false,
      ["allowed"],
    );
    expect(filled.kind).toBe("complete");
  });
});
