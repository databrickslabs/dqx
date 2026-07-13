import { describe, expect, test } from "bun:test";
import type { SuggestedRuleMappingOut } from "@/lib/api";
import {
  filterAlreadyApplied,
  groupSelectState,
  groupSuggestions,
  mappingSetKey,
  suggestionKey,
} from "./ai-suggestion-utils";

const sug = (over: Partial<SuggestedRuleMappingOut> = {}): SuggestedRuleMappingOut => ({
  rule_id: "r1",
  rule_name: "Is Not Null",
  dimension: "completeness",
  severity: "error",
  column_mapping: { column: "email" },
  explanation: "email should never be null",
  ...over,
});

describe("mappingSetKey", () => {
  test("is order-independent", () => {
    expect(mappingSetKey({ a: "x", b: "y" })).toBe(mappingSetKey({ b: "y", a: "x" }));
  });
  test("empty group yields empty key", () => {
    expect(mappingSetKey({})).toBe("");
  });
});

describe("suggestionKey", () => {
  test("same rule + different column are distinct", () => {
    const a = sug({ column_mapping: { column: "email" } });
    const b = sug({ column_mapping: { column: "phone" } });
    expect(suggestionKey(a)).not.toBe(suggestionKey(b));
  });
  test("same rule + same mapping collide", () => {
    expect(suggestionKey(sug())).toBe(suggestionKey(sug()));
  });
});

describe("filterAlreadyApplied", () => {
  test("drops a suggestion whose (rule, mapping) is already staged", () => {
    const suggestions = [sug({ column_mapping: { column: "email" } }), sug({ column_mapping: { column: "phone" } })];
    const applied = [{ rule_id: "r1", column_mapping: [{ column: "email" }] }];
    const out = filterAlreadyApplied(suggestions, applied);
    expect(out).toHaveLength(1);
    expect(out[0].column_mapping).toEqual({ column: "phone" });
  });
  test("keeps everything when nothing is applied", () => {
    const suggestions = [sug()];
    expect(filterAlreadyApplied(suggestions, [])).toHaveLength(1);
  });
  test("same column on a DIFFERENT rule is not excluded", () => {
    const suggestions = [sug({ rule_id: "r2", column_mapping: { column: "email" } })];
    const applied = [{ rule_id: "r1", column_mapping: [{ column: "email" }] }];
    expect(filterAlreadyApplied(suggestions, applied)).toHaveLength(1);
  });
});

describe("groupSuggestions", () => {
  test("by rule groups every mapping of a rule together", () => {
    const suggestions = [
      sug({ column_mapping: { column: "email" } }),
      sug({ column_mapping: { column: "phone" } }),
      sug({ rule_id: "r2", rule_name: "Range", column_mapping: { column: "age" } }),
    ];
    const groups = groupSuggestions(suggestions, "rule", "Unmapped");
    expect(groups).toHaveLength(2);
    expect(groups[0].items).toHaveLength(2);
    expect(groups[0].label).toBe("Is Not Null");
  });
  test("by column creates one group per mapped column", () => {
    const suggestions = [
      sug({ column_mapping: { column_1: "a", column_2: "b" } }),
      sug({ rule_id: "r2", column_mapping: { column: "a" } }),
    ];
    const groups = groupSuggestions(suggestions, "column", "Unmapped");
    const labels = groups.map((g) => g.label).sort();
    expect(labels).toEqual(["a", "b"]);
    // column "a" collects both the multi-slot rule and r2.
    expect(groups.find((g) => g.label === "a")?.items).toHaveLength(2);
  });
  test("falls back to rule_id label when unnamed", () => {
    const groups = groupSuggestions([sug({ rule_name: null })], "rule", "Unmapped");
    expect(groups[0].label).toBe("r1");
  });
});

describe("groupSelectState", () => {
  const items = [sug({ column_mapping: { column: "email" } }), sug({ column_mapping: { column: "phone" } })];
  test("all / some / none", () => {
    const all = new Set(items.map(suggestionKey));
    expect(groupSelectState(items, all)).toBe("all");
    expect(groupSelectState(items, new Set([suggestionKey(items[0])]))).toBe("some");
    expect(groupSelectState(items, new Set())).toBe("none");
  });
});
