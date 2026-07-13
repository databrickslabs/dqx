import { describe, expect, test } from "bun:test";
import type { RegistryRuleOut } from "@/lib/api";
import { computeRunGating, newStagedRow } from "./shared";

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
