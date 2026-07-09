import { describe, expect, test } from "bun:test";
import { computeRunGating } from "./shared";

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
