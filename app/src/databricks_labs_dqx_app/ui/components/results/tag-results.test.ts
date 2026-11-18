import { describe, expect, it } from "bun:test";
import type { GroupRowOut, TrendPointOut } from "@/lib/api";
import {
  effectiveTagKey,
  groupsForTagKey,
  parseTagFacetValue,
  tagFacetLabel,
  tagFacetValue,
  tagFilterOptions,
  tagKeysFromHistory,
  tagKeysOf,
  trendForTagKey,
} from "./tag-results";

const groups: GroupRowOut[] = [
  { tag_key: "pii", label: "true" },
  { tag_key: "domain", label: "finance" },
  { tag_key: "domain", label: "customer" },
  { tag_key: "domain", label: null },
];

describe("tag facet values", () => {
  it("round-trips key=value, splitting on the first '='", () => {
    expect(tagFacetValue("domain", "a=b")).toBe("domain=a=b");
    expect(parseTagFacetValue("domain=a=b")).toEqual({ key: "domain", value: "a=b" });
  });

  it("uses an empty value for checks missing a tag key", () => {
    expect(tagFacetValue("domain", null)).toBe("domain=");
    expect(parseTagFacetValue("domain=")).toEqual({ key: "domain", value: null });
    expect(tagFacetLabel("domain=", "No tag")).toBe("domain = No tag");
  });

  it("rejects values without a key", () => {
    expect(parseTagFacetValue("domain")).toBeNull();
    expect(parseTagFacetValue("=x")).toBeNull();
  });

  it("labels a value as 'key = value', leaving malformed ones as is", () => {
    expect(tagFacetLabel("domain=finance")).toBe("domain = finance");
    expect(tagFacetLabel("bogus")).toBe("bogus");
  });
});

describe("tag keys", () => {
  it("collects distinct keys across lists, sorted", () => {
    expect(tagKeysOf(groups, [{ tag_key: "zone", label: "eu" }], undefined)).toEqual(["domain", "pii", "zone"]);
    expect(tagKeysOf(undefined)).toEqual([]);
    expect(tagKeysFromHistory({ archived: ["old"] }, groups)).toEqual(["archived", "domain", "pii"]);
  });

  it("keeps the chosen key while it is in scope, else falls back to the first", () => {
    expect(effectiveTagKey(["domain", "pii"], "pii")).toBe("pii");
    expect(effectiveTagKey(["domain", "pii"], "gone")).toBe("domain");
    expect(effectiveTagKey(["domain"], null)).toBe("domain");
    expect(effectiveTagKey([], "pii")).toBeNull();
  });
});

describe("per-key slices", () => {
  it("keeps one key's groups, untagged row included", () => {
    expect(groupsForTagKey(groups, "domain").map((g) => g.label)).toEqual(["finance", "customer", null]);
    expect(groupsForTagKey(groups, null)).toEqual([]);
  });

  it("keeps one key's trend points", () => {
    const points: TrendPointOut[] = [
      { tag_key: "domain", series: "finance", run_date: "d1" },
      { tag_key: "pii", series: "true", run_date: "d1" },
    ];
    expect(trendForTagKey(points, "pii").map((p) => p.series)).toEqual(["true"]);
  });
});

describe("tagFilterOptions", () => {
  it("offers historical and missing values, sorted by label", () => {
    expect(tagFilterOptions({ domain: ["customer", "finance", null], pii: ["true"], retired: ["old"] }, "No tag")).toEqual([
      { value: "domain=customer", label: "domain = customer" },
      { value: "domain=finance", label: "domain = finance" },
      { value: "domain=", label: "domain = No tag" },
      { value: "pii=true", label: "pii = true" },
      { value: "retired=old", label: "retired = old" },
    ]);
  });
});
