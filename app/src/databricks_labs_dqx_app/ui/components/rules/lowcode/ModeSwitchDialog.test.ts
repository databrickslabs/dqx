import { describe, expect, test } from "bun:test";
import { isCustomSurfaceHop, modeSwitchDirection } from "./ModeSwitchDialog";

// Unit tests for the guarded rule-type-change direction resolver. Wiring the
// post-decision-point "Change rule type" re-pick: a switch is only guarded
// (confirm dialog shown) when the current mode holds content the target can't
// preserve losslessly; otherwise it proceeds silently.

describe("modeSwitchDirection", () => {
  test("same mode is never guarded", () => {
    expect(modeSwitchDirection("sql", "sql", true)).toBeNull();
    expect(modeSwitchDirection("dqx_native", "dqx_native", true)).toBeNull();
    expect(modeSwitchDirection("lowcode", "lowcode", true)).toBeNull();
  });

  test("no content -> unguarded (switch silently) even across modes", () => {
    expect(modeSwitchDirection("sql", "dqx_native", false)).toBeNull();
    expect(modeSwitchDirection("lowcode", "sql", false)).toBeNull();
    expect(modeSwitchDirection("dqx_native", "lowcode", false)).toBeNull();
  });

  test("cross-mode switch with content resolves the correct guarded direction", () => {
    expect(modeSwitchDirection("lowcode", "dqx_native", true)).toBe("LOWCODE_TO_NATIVE");
    expect(modeSwitchDirection("sql", "dqx_native", true)).toBe("SQL_TO_NATIVE");
    expect(modeSwitchDirection("dqx_native", "lowcode", true)).toBe("NATIVE_TO_LOWCODE");
    expect(modeSwitchDirection("dqx_native", "sql", true)).toBe("NATIVE_TO_SQL");
  });

  test("the two custom-condition SURFACES are never guarded, content or not", () => {
    // Visual builder <-> SQL are tabs on one rule type: the builder compiles into
    // the editor, hand-edited SQL is left alone, and the AST is cached for the way
    // back — so there is nothing for a confirm dialog to protect.
    expect(modeSwitchDirection("lowcode", "sql", true)).toBeNull();
    expect(modeSwitchDirection("sql", "lowcode", true)).toBeNull();
  });
});

describe("isCustomSurfaceHop", () => {
  test("both directions between the two custom surfaces", () => {
    expect(isCustomSurfaceHop("sql", "lowcode")).toBe(true);
    expect(isCustomSurfaceHop("lowcode", "sql")).toBe(true);
  });

  test("anything involving native replaces the body instead", () => {
    expect(isCustomSurfaceHop("sql", "dqx_native")).toBe(false);
    expect(isCustomSurfaceHop("dqx_native", "sql")).toBe(false);
    expect(isCustomSurfaceHop("lowcode", "dqx_native")).toBe(false);
    expect(isCustomSurfaceHop("dqx_native", "lowcode")).toBe(false);
  });

  test("staying put is not a hop", () => {
    expect(isCustomSurfaceHop("sql", "sql")).toBe(false);
    expect(isCustomSurfaceHop("lowcode", "lowcode")).toBe(false);
  });

  test("covers exactly the pair modeSwitchDirection leaves unguarded", () => {
    // The two are the same claim seen from opposite sides — a switch worth
    // guarding replaces the body, and a switch that replaces the body must reset
    // the granularity choice. Pinning them together keeps one from drifting.
    const modes = ["dqx_native", "lowcode", "sql"] as const;
    for (const from of modes) {
      for (const to of modes) {
        if (from === to) continue;
        expect(isCustomSurfaceHop(from, to)).toBe(modeSwitchDirection(from, to, true) === null);
      }
    }
  });
});
