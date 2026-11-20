import { describe, expect, test } from "bun:test";
import {
  filterTagGroups,
  groupTagOptions,
  hasValuesBelow,
  itemTags,
  keyState,
  selectionFromTokens,
  selectionToTokens,
  splitTagId,
  tagTokenLabel,
  tagsMatchSelection,
  toggleKey,
  toggleValue,
  tokensOfOptions,
  valueSelected,
  type TagLabels,
  type TagOption,
  type TagSelection,
} from "./tag-filter";

const LABELS: TagLabels = {
  untagged: "Untagged",
  source: { rule: "Rule tag", column: "Column tag", table: "Table tag", schema: "Schema tag", catalog: "Catalog tag" },
};

const sel = (entries: Array<[string, Array<string | null>]>): TagSelection =>
  new Map(entries.map(([id, values]) => [id, new Set(values)]));

const OPTIONS: TagOption[] = [
  { id: "rule:domain", value: "finance" },
  { id: "rule:domain", value: "customer" },
  { id: "rule:domain", value: null },
  { id: "table:tier", value: "gold" },
  { id: "catalog:env", value: "" },
  { id: "column:class.location", value: "" },
];

describe("splitTagId", () => {
  test("splits known sources and rejects the rest", () => {
    expect(splitTagId("rule:a:b")).toEqual({ source: "rule", key: "a:b" });
    expect(splitTagId("column:pii")).toEqual({ source: "column", key: "pii" });
    expect(splitTagId("label:pii")).toBeNull();
    expect(splitTagId("rule:")).toBeNull();
    expect(splitTagId("domain")).toBeNull();
  });
});

describe("tagsMatchSelection", () => {
  const finance = itemTags([["rule:domain", "finance"], ["table:tier", "gold"]]);
  const untagged = itemTags([]);

  test("an empty selection matches everything", () => {
    expect(tagsMatchSelection(untagged, new Map())).toBe(true);
  });

  test("any value, one value, and untagged", () => {
    expect(tagsMatchSelection(finance, sel([["rule:domain", []]]))).toBe(true);
    expect(tagsMatchSelection(untagged, sel([["rule:domain", []]]))).toBe(false);
    expect(tagsMatchSelection(finance, sel([["rule:domain", ["customer"]]]))).toBe(false);
    expect(tagsMatchSelection(untagged, sel([["rule:domain", [null]]]))).toBe(true);
    expect(tagsMatchSelection(finance, sel([["rule:domain", [null, "finance"]]]))).toBe(true);
  });

  test("different tag ids are ANDed, also across sources", () => {
    expect(tagsMatchSelection(finance, sel([["rule:domain", ["finance"]], ["table:tier", ["gold"]]]))).toBe(true);
    expect(tagsMatchSelection(finance, sel([["rule:domain", ["finance"]], ["table:tier", ["bronze"]]]))).toBe(false);
  });

  test("a multi-valued item matches when any of its values is selected", () => {
    const table = itemTags([["rule:domain", "finance"], ["rule:domain", "customer"]]);
    expect(tagsMatchSelection(table, sel([["rule:domain", ["customer"]]]))).toBe(true);
  });
});

describe("tokens", () => {
  test("round-trip any, value, key-only and untagged selections", () => {
    const selection = sel([["rule:domain", ["finance", null]], ["table:tier", []], ["catalog:env", [""]]]);
    const tokens = selectionToTokens(selection);
    expect(tokens).toEqual(["!rule:domain", "catalog:env=", "rule:domain=finance", "table:tier"]);
    expect(selectionFromTokens(tokens)).toEqual(selection);
  });

  test("drops invalid tokens and lets a key token win over its values", () => {
    expect(selectionFromTokens(["domain=x", "!rule:a=b", "rule:a=b", "rule:a"])).toEqual(sel([["rule:a", []]]));
  });

  test("tokensOfOptions lists every storable token", () => {
    expect(new Set(tokensOfOptions(OPTIONS))).toEqual(
      new Set([
        "rule:domain",
        "rule:domain=finance",
        "rule:domain=customer",
        "!rule:domain",
        "table:tier",
        "table:tier=gold",
        "catalog:env",
        "catalog:env=",
        "column:class.location",
        "column:class.location=",
      ]),
    );
  });
});

describe("tagTokenLabel", () => {
  test("always names the source, and shows key-only tags as just the key", () => {
    expect(tagTokenLabel("rule:domain=finance", LABELS)).toBe("Rule tag · domain: finance");
    expect(tagTokenLabel("!rule:domain", LABELS)).toBe("Rule tag · domain: Untagged");
    expect(tagTokenLabel("table:layer=gold", LABELS)).toBe("Table tag · layer: gold");
    expect(tagTokenLabel("column:class.location=", LABELS)).toBe("Column tag · class.location");
    expect(tagTokenLabel("rule:domain", LABELS)).toBe("Rule tag · domain");
    expect(tagTokenLabel("nonsense", LABELS)).toBe("nonsense");
  });
});

describe("grouping", () => {
  test("groups rule tags first, then column, table, schema and catalog; values sorted with untagged last", () => {
    const groups = groupTagOptions(OPTIONS);
    expect(groups.map((g) => g.id)).toEqual(["rule:domain", "column:class.location", "table:tier", "catalog:env"]);
    expect(groups[0].values).toEqual(["customer", "finance", null]);
  });

  test("a key-only tag has no values below it", () => {
    const groups = groupTagOptions([...OPTIONS, { id: "column:class.location", value: null }]);
    expect(groups.map((g) => [g.id, hasValuesBelow(g)])).toEqual([
      ["rule:domain", true],
      ["column:class.location", false],
      ["table:tier", true],
      ["catalog:env", false],
    ]);
  });

  test("search matches keys and values", () => {
    const groups = groupTagOptions(OPTIONS);
    expect(filterTagGroups(groups, "gold").map((g) => g.id)).toEqual(["table:tier"]);
    expect(filterTagGroups(groups, "DOM").map((g) => g.id)).toEqual(["rule:domain"]);
  });
});

describe("selection edits", () => {
  const domain = groupTagOptions(OPTIONS).find((g) => g.id === "rule:domain")!;

  test("ticking a key selects any value; ticking it again clears it", () => {
    const any = toggleKey(new Map(), "rule:domain");
    expect(keyState(any, "rule:domain")).toBe("all");
    expect(valueSelected(any, "rule:domain", "finance")).toBe(true);
    expect(valueSelected(any, "rule:domain", null)).toBe(false);
    expect(toggleKey(any, "rule:domain").size).toBe(0);
    expect(keyState(toggleKey(sel([["rule:domain", ["finance"]]]), "rule:domain"), "rule:domain")).toBe("all");
  });

  test("unticking a value of a fully selected key keeps the others", () => {
    const next = toggleValue(toggleKey(new Map(), "rule:domain"), domain, "finance");
    expect(next).toEqual(sel([["rule:domain", ["customer"]]]));
  });

  test("ticking every real value turns the key into any value; no values left clears it", () => {
    const one = toggleValue(new Map(), domain, "finance");
    expect(keyState(one, "rule:domain")).toBe("some");
    expect(keyState(toggleValue(one, domain, "customer"), "rule:domain")).toBe("all");
    expect(toggleValue(one, domain, "finance").size).toBe(0);
  });
});
