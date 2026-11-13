import { describe, expect, test } from "bun:test";
import type { SuggestedRuleMappingOut } from "@/lib/api";
import {
  filterAlreadyApplied,
  groupSelectState,
  groupSuggestions,
  mappingSetKey,
  mergeSuggestions,
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
  test("is case- and whitespace-insensitive (UC column names are case-insensitive-unique)", () => {
    expect(mappingSetKey({ column: "Email" })).toBe(mappingSetKey({ column: " email " }));
    expect(mappingSetKey({ Column: "EMAIL" })).toBe(mappingSetKey({ column: "email" }));
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
  test("drops a suggestion whose column differs only by case from an applied rule", () => {
    // The AI suggester reads original-case UC columns; a persisted (e.g.
    // profiler-applied) mapping can carry a lower-cased value — same column.
    const suggestions = [sug({ column_mapping: { column: "Email" } })];
    const applied = [{ rule_id: "r1", column_mapping: [{ column: "email" }] }];
    expect(filterAlreadyApplied(suggestions, applied)).toHaveLength(0);
  });
  test("dedups a column-less suggestion by rule_id when applied column-less", () => {
    const suggestions = [sug({ column_mapping: {} })];
    const applied = [{ rule_id: "r1", column_mapping: [] }];
    expect(filterAlreadyApplied(suggestions, applied)).toHaveLength(0);
  });
  test("keeps a column-less suggestion when the rule is applied WITH a column (distinct)", () => {
    const suggestions = [sug({ column_mapping: {} })];
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

describe("mergeSuggestions", () => {
  const tag = sug({ rule_id: "dob", column_mapping: { column: "date_of_birth" }, explanation: "Matched tag class.date_of_birth" });
  const ai = sug({ rule_id: "dob", column_mapping: { column: "Date_Of_Birth" }, explanation: "Birth dates cannot be in the future." });

  test("tag-only suggestions keep their reason and no secondary text", () => {
    const [only] = mergeSuggestions([], [tag]);
    expect(only.explanation).toBe("Matched tag class.date_of_birth");
    expect(only.secondary_explanation).toBeUndefined();
  });

  test("AI-only suggestions keep the AI explanation", () => {
    const merged = mergeSuggestions([ai], []);
    expect(merged).toHaveLength(1);
    expect(merged[0].explanation).toBe("Birth dates cannot be in the future.");
    expect(merged[0].secondary_explanation).toBeUndefined();
  });

  test("a collision keeps the tag reason first and the AI explanation underneath", () => {
    const other = sug({ rule_id: "nn", column_mapping: { column: "id" }, explanation: "ids are required" });
    const merged = mergeSuggestions([ai, other], [tag]);
    expect(merged).toHaveLength(2);
    expect(merged[0].explanation).toBe("Matched tag class.date_of_birth");
    expect(merged[0].secondary_explanation).toBe("Birth dates cannot be in the future.");
    expect(merged[1].rule_id).toBe("nn");
  });

  test("a policy-ambiguity collision keeps the policy reason", () => {
    const policy = sug({
      rule_id: "range",
      column_mapping: { start: "start_ts", end: "end_a" },
      explanation: "Policy Date ranges: several possible columns",
    });
    const aiRange = { ...policy, explanation: "start should precede end" };
    const merged = mergeSuggestions([aiRange], [policy]);
    expect(merged).toEqual([
      { ...policy, secondary_explanation: "start should precede end" },
    ]);
  });
});
