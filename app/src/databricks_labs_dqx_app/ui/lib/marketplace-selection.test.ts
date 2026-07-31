import { describe, expect, it } from "vitest";
import {
  polarityLineKey,
  ruleMatchesFilters,
  collectIndustries,
  collectRegions,
  packSelectionState,
  toggleRule,
  togglePack,
  selectedCheckDicts,
  checkDictToPreviewRule,
} from "./marketplace-selection";
import type { CheckFunctionDef, MarketplacePackOut, MarketplaceRuleOut } from "@/lib/api";

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

function rule(over: Partial<MarketplaceRuleOut>): MarketplaceRuleOut {
  return {
    rule_key: "p:r",
    name: "Rule",
    description: "Desc.",
    industries: [],
    regions: [],
    dimension: "Validity",
    severity: "Low",
    check: { criticality: "error", check: { function: "is_not_null", arguments: {} }, user_metadata: {} },
    ...over,
  } as MarketplaceRuleOut;
}

describe("ruleMatchesFilters", () => {
  it("general (no industry) always shows under any industry chip", () => {
    expect(ruleMatchesFilters(rule({ industries: [] }), { industry: "banking", region: "all", search: "" })).toBe(true);
  });
  it("industry chip narrows to tagged rules", () => {
    expect(ruleMatchesFilters(rule({ industries: ["retail"] }), { industry: "banking", region: "all", search: "" })).toBe(false);
    expect(ruleMatchesFilters(rule({ industries: ["banking"] }), { industry: "banking", region: "all", search: "" })).toBe(true);
  });
  it("global (no region) always shows under any region chip", () => {
    expect(ruleMatchesFilters(rule({ regions: [] }), { industry: "all", region: "uk", search: "" })).toBe(true);
  });
  it("industry AND region combine", () => {
    const r = rule({ industries: ["banking"], regions: ["eu"] });
    expect(ruleMatchesFilters(r, { industry: "banking", region: "eu", search: "" })).toBe(true);
    expect(ruleMatchesFilters(r, { industry: "banking", region: "uk", search: "" })).toBe(false);
  });
  it("search matches name or description case-insensitively", () => {
    expect(ruleMatchesFilters(rule({ name: "Valid email" }), { industry: "all", region: "all", search: "EMAIL" })).toBe(true);
    expect(ruleMatchesFilters(rule({ name: "Valid email" }), { industry: "all", region: "all", search: "phone" })).toBe(false);
  });
  it("search matches description when name does not match", () => {
    const r = rule({ name: "Generic rule", description: "Checks that the phone number is valid" });
    expect(ruleMatchesFilters(r, { industry: "all", region: "all", search: "phone" })).toBe(true);
  });
});

describe("collectIndustries / collectRegions", () => {
  const packs = [
    { rules: [rule({ industries: ["retail"] }), rule({ industries: ["banking"] })] },
    { rules: [rule({ regions: ["uk"] }), rule({ regions: ["eu"] })] },
  ] as unknown as MarketplacePackOut[];
  it("prepends all and unions/sorts", () => {
    expect(collectIndustries(packs)).toEqual(["all", "banking", "retail"]);
    expect(collectRegions(packs)).toEqual(["all", "eu", "uk"]);
  });
});

describe("selection", () => {
  it("tri-state reflects selected subset", () => {
    const keys = ["a", "b", "c"];
    expect(packSelectionState(keys, new Set())).toBe("none");
    expect(packSelectionState(keys, new Set(["a"]))).toBe("some");
    expect(packSelectionState(keys, new Set(["a", "b", "c"]))).toBe("all");
  });
  it("empty packRuleKeys returns none regardless of selected set", () => {
    expect(packSelectionState([], new Set())).toBe("none");
    expect(packSelectionState([], new Set(["a", "b"]))).toBe("none");
  });
  it("toggleRule adds/removes", () => {
    expect(toggleRule(new Set(), "a").has("a")).toBe(true);
    expect(toggleRule(new Set(["a"]), "a").has("a")).toBe(false);
  });
  it("togglePack selects all then clears", () => {
    const keys = ["a", "b"];
    const s1 = togglePack(new Set(), keys);
    expect([...s1].sort()).toEqual(["a", "b"]);
    const s2 = togglePack(s1, keys);
    expect(s2.size).toBe(0);
  });
  it("selectedCheckDicts returns check dicts of selected rules", () => {
    const ruleA = rule({ rule_key: "p:a" });
    const packs = [{ rules: [ruleA, rule({ rule_key: "p:b" })] }] as unknown as MarketplacePackOut[];
    const dicts = selectedCheckDicts(packs, new Set(["p:a"]));
    expect(dicts).toHaveLength(1);
    expect(dicts[0]).toEqual(ruleA.check);
  });
});

const T = (k: string) => k;
// Minimal stub so parseDqxCheckJson recognises is_not_null as a known function.
const FNS: CheckFunctionDef[] = [
  { name: "is_not_null", params: [{ name: "column", kind: "column", family: "any" }] },
] as unknown as CheckFunctionDef[];

describe("checkDictToPreviewRule", () => {
  it("produces a RegistryRuleOut-shaped object for a native check", () => {
    const r = rule({
      rule_key: "standard-checks:must-not-be-null",
      check: {
        criticality: "error",
        check: { function: "is_not_null", arguments: { column: "{{column}}" } },
        user_metadata: { name: "Must not be null" },
      },
    });
    const preview = checkDictToPreviewRule(r, FNS, T);
    expect(preview).toBeDefined();
    expect(preview?.rule_id).toBe("standard-checks:must-not-be-null");
    expect(preview?.definition).toBeDefined();
  });

  it("returns undefined for an unrecognised function", () => {
    const r = rule({
      rule_key: "p:unknown",
      check: {
        criticality: "error",
        check: { function: "no_such_function", arguments: {} },
        user_metadata: {},
      },
    });
    expect(checkDictToPreviewRule(r, FNS, T)).toBeUndefined();
  });
});
