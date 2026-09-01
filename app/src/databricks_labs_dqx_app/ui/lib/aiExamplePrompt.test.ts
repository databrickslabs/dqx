import { describe, expect, test } from "bun:test";
import { pickAiExampleKey } from "./aiExamplePrompt";

describe("pickAiExampleKey", () => {
  test("returns a 1-based key within range", () => {
    expect(pickAiExampleKey(20, () => 0)).toBe("aiBuildExample1");
    expect(pickAiExampleKey(20, () => 0.999)).toBe("aiBuildExample20");
  });
  test("maps mid-range value correctly", () => {
    expect(pickAiExampleKey(20, () => 0.5)).toBe("aiBuildExample11");
  });
  test("never returns index 0 or > count", () => {
    for (let r = 0; r < 1; r += 0.017) {
      const key = pickAiExampleKey(20, () => r);
      const n = Number(key.replace("aiBuildExample", ""));
      expect(n).toBeGreaterThanOrEqual(1);
      expect(n).toBeLessThanOrEqual(20);
    }
  });
});
