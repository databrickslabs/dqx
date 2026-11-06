import { describe, expect, test } from "bun:test";
import type { RegistryRuleOut } from "@/lib/api";
import type { BatchImportRuleMatch } from "@/lib/api-custom";
import { defaultReuseRuleIds, groupSimilarRules, parameterSummary } from "./similar-rules";

function rule(ruleId: string): RegistryRuleOut {
  return { rule_id: ruleId } as RegistryRuleOut;
}

describe("groupSimilarRules", () => {
  test("groups inputs under their best similar rule", () => {
    const matches: BatchImportRuleMatch[] = [
      { index: 0, similar: [rule("trimmed"), rule("other")] },
      { index: 2, similar: [rule("trimmed")] },
      { index: 3, similar: [rule("limit")] },
    ];
    expect(groupSimilarRules(matches, []).map((c) => [c.rule.rule_id, c.inputIndexes])).toEqual([
      ["trimmed", [0, 2]],
      ["limit", [3]],
    ]);
  });

  test("skips inputs that already have an identical rule", () => {
    const matches: BatchImportRuleMatch[] = [{ index: 0, exact: rule("same"), similar: [rule("trimmed")] }];
    expect(groupSimilarRules(matches, [])).toEqual([]);
  });

  test("prefers a similar rule whose settings don't conflict", () => {
    const upper = { ...rule("upper"), definition: { parameters: [{ name: "case", type: "string", value: "upper" }] } };
    const lower = { ...rule("lower"), definition: { parameters: [{ name: "case", type: "string", value: "lower" }] } };
    const matches = [{ index: 0, similar: [upper, lower] }] as BatchImportRuleMatch[];
    const inputs = [{ definition: { parameters: [{ name: "case", type: "string" as const, value: "lower" }] } }];
    expect(groupSimilarRules(matches, inputs).map((c) => c.rule.rule_id)).toEqual(["lower"]);
  });
});

describe("defaultReuseRuleIds", () => {
  const trim = (value: boolean | null) => ({
    parameters: [{ name: "trim_strings", type: "boolean" as const, value }],
  });
  const caseOf = (value: string) => ({ parameters: [{ name: "case", type: "string" as const, value }] });

  test("reuses a rule whose settings only add to the imported ones", () => {
    const choices = [{ rule: { ...rule("trimmed"), definition: trim(true) } as RegistryRuleOut, inputIndexes: [0] }];
    expect([...defaultReuseRuleIds(choices, [{ definition: trim(null) }])]).toEqual(["trimmed"]);
  });

  test("does not reuse a rule that sets a parameter to a different value", () => {
    const choices = [{ rule: { ...rule("upper"), definition: caseOf("upper") } as RegistryRuleOut, inputIndexes: [0] }];
    expect([...defaultReuseRuleIds(choices, [{ definition: caseOf("lower") }])]).toEqual([]);
  });
});

describe("parameterSummary", () => {
  test("lists parameters that are set", () => {
    expect(
      parameterSummary({
        parameters: [
          { name: "trim_strings", type: "boolean", value: true },
          { name: "limit", type: "number", value: 0 },
          { name: "unset", type: "string", value: null },
        ],
      }),
    ).toBe("trim_strings: true, limit: 0");
  });

  test("returns null when everything is left at its default", () => {
    expect(parameterSummary({ parameters: [{ name: "trim_strings", type: "boolean", value: null }] })).toBeNull();
    expect(parameterSummary(undefined)).toBeNull();
  });
});
