import { describe, expect, test } from "bun:test";
import type { RuleScoreOut } from "@/lib/api";
import { ruleResultsState } from "./results-display";

describe("ruleResultsState", () => {
  const score = (over: Partial<RuleScoreOut> = {}): RuleScoreOut => ({
    rule_id: "r1",
    applied_to_count: 2,
    overall_score: 0.93,
    per_table: [
      { source_table_fqn: "c.s.t1", score: 0.95 },
      { source_table_fqn: "c.s.t2", score: 0.91 },
    ],
    ...over,
  });

  test("not-applied when the rule has zero current applications", () => {
    expect(ruleResultsState(score({ applied_to_count: 0, overall_score: null, per_table: [] }))).toBe("not-applied");
  });

  test("not-applied when applied_to_count is absent (optional in the generated types)", () => {
    expect(ruleResultsState(score({ applied_to_count: undefined, per_table: [] }))).toBe("not-applied");
  });

  test("no-access when applied somewhere but the viewer can see none of the tables", () => {
    expect(ruleResultsState(score({ applied_to_count: 3, overall_score: null, per_table: [] }))).toBe("no-access");
  });

  test("no-access when per_table is absent but applications exist", () => {
    expect(ruleResultsState(score({ applied_to_count: 1, per_table: undefined }))).toBe("no-access");
  });

  test("has-data when the viewer can see at least one applied table", () => {
    expect(ruleResultsState(score())).toBe("has-data");
  });

  test("not-applied wins over a stale non-empty per_table list", () => {
    expect(ruleResultsState(score({ applied_to_count: 0 }))).toBe("not-applied");
  });
});
