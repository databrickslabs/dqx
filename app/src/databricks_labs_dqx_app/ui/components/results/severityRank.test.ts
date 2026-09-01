import { describe, expect, it } from "bun:test";
import { pickTopSeverity } from "./severityRank";

describe("pickTopSeverity", () => {
  const rank = { Critical: 4, High: 3, Medium: 2, Low: 1 };

  it("returns the highest-rank severity from a set", () => {
    expect(pickTopSeverity(["High", "Critical"], rank)).toBe("Critical");
  });

  it("ignores unknown severities (rank 0) when a ranked one is present", () => {
    expect(pickTopSeverity(["High", "Mystery"], rank)).toBe("High");
  });

  it("still returns an unknown severity when nothing ranked is present", () => {
    expect(pickTopSeverity(["Mystery"], rank)).toBe("Mystery");
  });

  it("returns undefined for an empty list", () => {
    expect(pickTopSeverity([], rank)).toBeUndefined();
  });

  it("skips undefined entries", () => {
    expect(pickTopSeverity([undefined, "Low"], rank)).toBe("Low");
  });

  it("returns undefined when every entry is undefined", () => {
    expect(pickTopSeverity([undefined, undefined], rank)).toBeUndefined();
  });
});
