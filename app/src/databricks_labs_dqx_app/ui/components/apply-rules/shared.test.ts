import { describe, expect, test } from "bun:test";
import type { AppliedRuleOut, RegistryRuleOut, RuleSlot } from "@/lib/api";
import { buildDesiredApplications, computeRunGating, mergeColumnThresholds, newStagedRow, pickSlotForColumn } from "./shared";

// Minimal RegistryRuleOut for newStagedRow — only rule_id, version, and
// user_metadata (reserved name/dimension/severity tags) are read.
function fakeRule(version: number): RegistryRuleOut {
  return {
    rule_id: "r1",
    version,
    user_metadata: { name: "Not Null", dimension: "Completeness", severity: "High" },
  } as unknown as RegistryRuleOut;
}

// B2-116: a freshly staged applied-rule row must reflect the admin
// `default_auto_upgrade` setting — follow-latest when ON, pinned to the rule's
// current version when OFF — instead of always seeding "Following latest".
describe("newStagedRow pin seeding", () => {
  test("default_auto_upgrade ON → follows latest (pinned_version null)", () => {
    const row = newStagedRow("b1", fakeRule(3), [], true);
    expect(row.pinned_version).toBeNull();
  });

  test("default_auto_upgrade OFF → pinned to the rule's current version", () => {
    const row = newStagedRow("b1", fakeRule(3), [], false);
    expect(row.pinned_version).toBe(3);
  });

  test("still denormalizes rule display tags and binding id", () => {
    const row = newStagedRow("b1", fakeRule(2), [], false);
    expect(row.binding_id).toBe("b1");
    expect(row.rule_id).toBe("r1");
    expect(row.rule_name).toBe("Not Null");
    expect(row.rule_severity).toBe("High");
  });
});

// ---------------------------------------------------------------------------
// mergeColumnThresholds
// ---------------------------------------------------------------------------

// Minimal AppliedRuleOut factory — only the fields mergeColumnThresholds reads.
function fakeRow(
  ruleId: string,
  colThresholds?: Record<string, number>,
): AppliedRuleOut {
  return {
    id: ruleId,
    binding_id: "b1",
    rule_id: ruleId,
    column_mapping: [],
    column_pass_thresholds: colThresholds,
  } as unknown as AppliedRuleOut;
}

describe("mergeColumnThresholds", () => {
  test("returns undefined when no rows have overrides", () => {
    expect(mergeColumnThresholds([fakeRow("r1"), fakeRow("r1", {})])).toBeUndefined();
  });

  test("returns the single column's threshold", () => {
    const result = mergeColumnThresholds([fakeRow("r1", { email: 90 })]);
    expect(result).toEqual({ email: 90 });
  });

  test("0 is a valid threshold — stored, not treated as absent", () => {
    const result = mergeColumnThresholds([fakeRow("r1", { col: 0 })]);
    expect(result).toEqual({ col: 0 });
  });

  test("merges multiple rows — later rows win on key conflict", () => {
    const result = mergeColumnThresholds([
      fakeRow("r1", { email: 80, name: 70 }),
      fakeRow("r1", { email: 95 }),
    ]);
    expect(result).toEqual({ email: 95, name: 70 });
  });

  test("returns undefined when rows list is empty", () => {
    expect(mergeColumnThresholds([])).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// buildDesiredApplications — column_pass_thresholds round-trip
// ---------------------------------------------------------------------------

describe("buildDesiredApplications column_pass_thresholds", () => {
  test("carries column_pass_thresholds into the payload", () => {
    const rows: AppliedRuleOut[] = [
      {
        id: "row1",
        binding_id: "b1",
        rule_id: "r1",
        pinned_version: null,
        severity_override: null,
        pass_threshold: null,
        column_pass_thresholds: { email: 90 },
        column_mapping: [{ col: "email" }],
        user_metadata: {},
        mapping_hash: null,
        created_by: null,
        created_at: null,
      } as unknown as AppliedRuleOut,
    ];
    const payload = buildDesiredApplications(rows);
    expect(payload[0]?.column_pass_thresholds).toEqual({ email: 90 });
  });

  test("column_pass_thresholds is null (not {}) when no overrides exist", () => {
    const rows: AppliedRuleOut[] = [
      {
        id: "row1",
        binding_id: "b1",
        rule_id: "r1",
        pinned_version: null,
        severity_override: null,
        pass_threshold: null,
        column_pass_thresholds: {},
        column_mapping: [{ col: "email" }],
        user_metadata: {},
        mapping_hash: null,
        created_by: null,
        created_at: null,
      } as unknown as AppliedRuleOut,
    ];
    const payload = buildDesiredApplications(rows);
    // Empty map → mergeColumnThresholds returns undefined → null in payload
    expect(payload[0]?.column_pass_thresholds).toBeNull();
  });

  test("value 0 persists as 0 in the payload (not dropped)", () => {
    const rows: AppliedRuleOut[] = [
      {
        id: "row1",
        binding_id: "b1",
        rule_id: "r1",
        pinned_version: null,
        severity_override: null,
        pass_threshold: null,
        column_pass_thresholds: { col: 0 },
        column_mapping: [{ col: "col" }],
        user_metadata: {},
        mapping_hash: null,
        created_by: null,
        created_at: null,
      } as unknown as AppliedRuleOut,
    ];
    const payload = buildDesiredApplications(rows);
    expect(payload[0]?.column_pass_thresholds).toEqual({ col: 0 });
  });
});

// ---------------------------------------------------------------------------
// pickSlotForColumn
// ---------------------------------------------------------------------------

function fakeSlot(name: string, family: RuleSlot["family"]): RuleSlot {
  return { name, family, position: 0 } as RuleSlot;
}

describe("pickSlotForColumn", () => {
  test("returns null when slots is empty", () => {
    expect(pickSlotForColumn([], "numeric")).toBeNull();
  });

  test("returns the first slot when only one slot exists (any family)", () => {
    expect(pickSlotForColumn([fakeSlot("col", "any")], "numeric")).toBe("col");
  });

  test("prefers slot whose family matches the column family", () => {
    const slots = [fakeSlot("col1", "text"), fakeSlot("col2", "numeric")];
    expect(pickSlotForColumn(slots, "numeric")).toBe("col2");
  });

  test("treats 'any' family slot as compatible with any column family", () => {
    const slots = [fakeSlot("col1", "text"), fakeSlot("col2", "any")];
    expect(pickSlotForColumn(slots, "numeric")).toBe("col2");
  });

  test("falls back to first slot when no slot matches", () => {
    const slots = [fakeSlot("col1", "text"), fakeSlot("col2", "temporal")];
    expect(pickSlotForColumn(slots, "numeric")).toBe("col1");
  });

  test("match wins over first-slot fallback when match is not first", () => {
    const slots = [fakeSlot("col1", "temporal"), fakeSlot("col2", "boolean"), fakeSlot("col3", "numeric")];
    expect(pickSlotForColumn(slots, "numeric")).toBe("col3");
  });
});

// P23-F fix: "Run now" executes the last-persisted (approved) snapshot and
// must gate on the baseline count; "Run draft" executes the local staged
// edit buffer and must gate on the staged count. These must never be
// conflated — see `computeRunGating` in `shared.tsx`.
describe("computeRunGating", () => {
  test("persisted-empty, staged-nonempty: Run now disabled, Run draft enabled", () => {
    const gating = computeRunGating(0, 2);
    expect(gating.runNowHasRules).toBe(false);
    expect(gating.runDraftHasRules).toBe(true);
  });

  test("persisted-nonempty, staged-empty (all staged rules removed locally): Run now enabled, Run draft disabled", () => {
    const gating = computeRunGating(3, 0);
    expect(gating.runNowHasRules).toBe(true);
    expect(gating.runDraftHasRules).toBe(false);
  });

  test("both empty: Run now and Run draft disabled", () => {
    const gating = computeRunGating(0, 0);
    expect(gating.runNowHasRules).toBe(false);
    expect(gating.runDraftHasRules).toBe(false);
  });

  test("both non-empty: Run now and Run draft enabled", () => {
    const gating = computeRunGating(1, 1);
    expect(gating.runNowHasRules).toBe(true);
    expect(gating.runDraftHasRules).toBe(true);
  });

  // P21-F save-then-run interplay: when the editor is dirty (staged rules
  // differ from baseline) and staged is non-empty, Run draft's *rules* gate
  // reports enabled here — `handleRunDraft` in
  // monitored-tables.$bindingId.tsx layers the separate `canRunDraft`
  // (status === "draft" || isDirty) check on top and saves staged rows
  // before running, but this "has rules to run" gate is independent of
  // dirtiness by design: it only cares whether stagedRows is non-empty.
  test("dirty + staged rules present: Run draft rules-gate reports enabled regardless of baseline", () => {
    const gating = computeRunGating(1, 2); // baseline=1 rule, staged diverged to 2 rules
    expect(gating.runDraftHasRules).toBe(true);
  });
});
