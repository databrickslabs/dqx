import { describe, expect, it } from "bun:test";
import { orderSeverityValuesForDisplay } from "@/components/RegistryRuleBadges";
import { compareSeverityGroups } from "./severity-group-order";

// Stored lowest-first in settings; displayed (and grouped) most severe first.
const DISPLAY_ORDER = orderSeverityValuesForDisplay(["Low", "Medium", "High", "Critical"]);
const sortGroups = (values: string[]) => [...values].sort((a, b) => compareSeverityGroups(a, b, DISPLAY_ORDER));

describe("compareSeverityGroups", () => {
  it("follows the settings order rather than the alphabet", () => {
    expect(sortGroups(["Low", "Critical", "Medium", "High"])).toEqual(["Critical", "High", "Medium", "Low"]);
  });

  it("follows a custom settings order", () => {
    const custom = ["Blocker", "Major", "Minor"];
    expect([...["Minor", "Blocker", "Major"]].sort((a, b) => compareSeverityGroups(a, b, custom))).toEqual(custom);
  });

  it("puts undefined severities after the defined ones, alphabetically", () => {
    expect(sortGroups(["Zeta", "Low", "Alpha", "Critical"])).toEqual(["Critical", "Low", "Alpha", "Zeta"]);
  });

  it("puts rules with no severity last", () => {
    expect(sortGroups(["", "Unknown", "Low", "High"])).toEqual(["High", "Low", "Unknown", ""]);
  });

  it("matches severities case-insensitively", () => {
    expect(sortGroups(["low", "CRITICAL"])).toEqual(["CRITICAL", "low"]);
  });

  it("treats equal severities as equal so the column sort holds within a group", () => {
    expect(compareSeverityGroups("High", "high", DISPLAY_ORDER)).toBe(0);
  });
});
