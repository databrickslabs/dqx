import { describe, expect, test } from "bun:test";
import { checkContrast } from "./contrast";
import { deriveAllTokens } from "./derive";
import { COLOR_GROUPS } from "./groups";
import { PRESET_IDS, PRESETS } from "./presets";

describe("presets", () => {
  test("ids and order", () => {
    expect(PRESETS.map((p) => p.id)).toEqual([...PRESET_IDS]);
    expect(PRESETS[0].id).toBe("dqx-default");
    expect(PRESETS[0].light).toEqual({});
    expect(PRESETS[0].dark).toEqual({});
  });
  test.each(PRESETS.slice(1).map((p) => [p.id, p] as const))("%s defines all five groups for both modes", (_id, p) => {
    expect(Object.keys(p.light).sort()).toEqual([...COLOR_GROUPS].sort());
    expect(Object.keys(p.dark).sort()).toEqual([...COLOR_GROUPS].sort());
  });
  test.each(PRESETS.map((p) => [p.id, p] as const))("%s has no contrast warnings", (_id, p) => {
    expect(checkContrast("light", deriveAllTokens("light", p.light))).toEqual([]);
    expect(checkContrast("dark", deriveAllTokens("dark", p.dark))).toEqual([]);
  });
});
