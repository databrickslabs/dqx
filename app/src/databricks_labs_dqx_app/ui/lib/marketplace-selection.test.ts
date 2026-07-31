import { describe, expect, it } from "vitest";
import { polarityLineKey } from "./marketplace-selection";

describe("polarityLineKey", () => {
  it("maps pass -> then-passes key", () => {
    expect(polarityLineKey("pass")).toBe("monitoredTables.ruleLogicThenPasses");
  });
  it("maps fail -> then-fails key", () => {
    expect(polarityLineKey("fail")).toBe("monitoredTables.ruleLogicThenFails");
  });
  it("returns null when polarity is absent", () => {
    expect(polarityLineKey(null)).toBeNull();
    expect(polarityLineKey(undefined)).toBeNull();
  });
});
