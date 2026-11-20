import { describe, expect, it } from "bun:test";
import type { GroupRowOut, TrendPointOut } from "@/lib/api";
import { selectionFromTokens } from "@/components/tags/tag-filter";
import {
  shownTagGroups,
  shownTagTrend,
  tagOptionsFromHistory,
  tagSeriesSelected,
  tagSeriesToken,
  toggleTagSeries,
} from "./tag-results";

const groups: GroupRowOut[] = [
  { tag_key: "rule:domain", label: "finance" },
  { tag_key: "rule:domain", label: "customer" },
  { tag_key: "rule:domain", label: null },
  { tag_key: "table:tier", label: "gold" },
];

const history = { "rule:domain": ["customer", "finance", null], "table:tier": ["gold"] };

describe("series tokens", () => {
  it("are filter tokens: id=value, or !id for checks without the tag", () => {
    expect(tagSeriesToken("rule:domain", "a=b")).toBe("rule:domain=a=b");
    expect(tagSeriesToken("rule:domain", null)).toBe("!rule:domain");
  });
});

describe("shown series follow the tag filter", () => {
  it("show every tag value, but no Untagged rows, when no tag is picked", () => {
    expect(shownTagGroups(groups, new Map()).map((g) => g.label)).toEqual([
      "rule:domain=finance",
      "rule:domain=customer",
      "table:tier=gold",
    ]);
  });

  it("show the values of a ticked key, or just the picked values", () => {
    expect(shownTagGroups(groups, selectionFromTokens(["rule:domain"])).map((g) => g.label)).toEqual([
      "rule:domain=finance",
      "rule:domain=customer",
    ]);
    expect(shownTagGroups(groups, selectionFromTokens(["!rule:domain", "table:tier=gold"])).map((g) => g.label)).toEqual([
      "!rule:domain",
      "table:tier=gold",
    ]);
  });

  it("filters trend points the same way, keyed by token, and charts nothing until tags are picked", () => {
    const points: TrendPointOut[] = [
      { tag_key: "rule:domain", series: "finance", run_date: "d1" },
      { tag_key: "table:tier", series: "gold", run_date: "d1" },
    ];
    expect(shownTagTrend(points, selectionFromTokens(["table:tier"])).map((p) => p.series)).toEqual([
      "table:tier=gold",
    ]);
    expect(shownTagTrend(points, new Map())).toEqual([]);
  });

  it("highlights only explicitly picked series", () => {
    expect(tagSeriesSelected(new Map(), "rule:domain=finance")).toBe(false);
    expect(tagSeriesSelected(selectionFromTokens(["rule:domain"]), "rule:domain=finance")).toBe(true);
    expect(tagSeriesSelected(selectionFromTokens(["!rule:domain"]), "!rule:domain")).toBe(true);
  });
});

describe("clicking a series", () => {
  it("toggles that tag value in the filter", () => {
    expect(toggleTagSeries(new Map(), history, "rule:domain=finance")).toEqual(["rule:domain=finance"]);
    expect(toggleTagSeries(selectionFromTokens(["rule:domain"]), history, "rule:domain=finance")).toEqual([
      "rule:domain=customer",
    ]);
    expect(toggleTagSeries(new Map(), history, "!rule:domain")).toEqual(["!rule:domain"]);
  });

  it("leaves the filter alone for an unknown tag", () => {
    expect(toggleTagSeries(new Map(), history, "rule:gone=x")).toEqual([]);
  });
});

describe("tagOptionsFromHistory", () => {
  it("lists every value of every tag, Untagged included", () => {
    expect(tagOptionsFromHistory(history)).toEqual([
      { id: "rule:domain", value: "customer" },
      { id: "rule:domain", value: "finance" },
      { id: "rule:domain", value: null },
      { id: "table:tier", value: "gold" },
    ]);
    expect(tagOptionsFromHistory(undefined)).toEqual([]);
  });
});
