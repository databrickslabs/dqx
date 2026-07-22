import { describe, expect, test } from "bun:test";
import { modeSwitchDirection } from "./ModeSwitchDialog";

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
    expect(modeSwitchDirection("lowcode", "sql", true)).toBe("LOWCODE_TO_SQL");
    expect(modeSwitchDirection("lowcode", "dqx_native", true)).toBe("LOWCODE_TO_NATIVE");
    expect(modeSwitchDirection("sql", "lowcode", true)).toBe("SQL_TO_LOWCODE");
    expect(modeSwitchDirection("sql", "dqx_native", true)).toBe("SQL_TO_NATIVE");
    expect(modeSwitchDirection("dqx_native", "lowcode", true)).toBe("NATIVE_TO_LOWCODE");
    expect(modeSwitchDirection("dqx_native", "sql", true)).toBe("NATIVE_TO_SQL");
  });
});
