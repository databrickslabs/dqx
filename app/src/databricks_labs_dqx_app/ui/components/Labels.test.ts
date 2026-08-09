import { describe, expect, test } from "bun:test";
import { labelsMatchFilter, tagPairsMatchFilter, type LabelSelection } from "./Labels";

// Unit tests for the key-first `labelsMatchFilter` semantics used by the
// shared LabelFilter (Rules Registry overview + Drafts & Review list). A
// selection is a `key -> set-of-values` map: an empty value set means "match
// any value of that key"; a non-empty set narrows to specific values.
// Matching is OR across the whole selection. Run via `bun test`.

const sel = (entries: [string, string[]][]): LabelSelection =>
  new Map(entries.map(([k, v]) => [k, new Set(v)]));

describe("labelsMatchFilter — key-first selection", () => {
  test("empty selection matches everything", () => {
    expect(labelsMatchFilter({ team: "finance" }, new Map())).toBe(true);
    expect(labelsMatchFilter({}, new Map())).toBe(true);
  });

  test("key selected with no values matches any value of that key", () => {
    const selection = sel([["team", []]]);
    expect(labelsMatchFilter({ team: "finance" }, selection)).toBe(true);
    expect(labelsMatchFilter({ team: "sales" }, selection)).toBe(true);
    expect(labelsMatchFilter({ region: "eu" }, selection)).toBe(false);
    expect(labelsMatchFilter({}, selection)).toBe(false);
  });

  test("key selected with specific values matches only those values", () => {
    const selection = sel([["team", ["finance", "sales"]]]);
    expect(labelsMatchFilter({ team: "finance" }, selection)).toBe(true);
    expect(labelsMatchFilter({ team: "sales" }, selection)).toBe(true);
    expect(labelsMatchFilter({ team: "ops" }, selection)).toBe(false);
  });

  test("OR semantics across multiple selected keys", () => {
    const selection = sel([
      ["team", ["finance"]],
      ["region", []],
    ]);
    // matches via team=finance
    expect(labelsMatchFilter({ team: "finance", region: "us" }, selection)).toBe(true);
    // matches via region present (any value)
    expect(labelsMatchFilter({ team: "ops", region: "eu" }, selection)).toBe(true);
    // satisfies neither
    expect(labelsMatchFilter({ team: "ops" }, selection)).toBe(false);
  });

  test("absent key is skipped, not treated as a match", () => {
    const selection = sel([["team", ["finance"]]]);
    expect(labelsMatchFilter({ region: "eu" }, selection)).toBe(false);
  });
});

describe("tagPairsMatchFilter — multi-valued bag", () => {
  test("empty selection matches everything", () => {
    expect(tagPairsMatchFilter([{ key: "team", value: "finance" }], new Map())).toBe(true);
    expect(tagPairsMatchFilter([], new Map())).toBe(true);
  });

  test("key selected with no values matches when key is present with any value", () => {
    const selection = sel([["team", []]]);
    expect(tagPairsMatchFilter([{ key: "team", value: "finance" }], selection)).toBe(true);
    expect(
      tagPairsMatchFilter(
        [
          { key: "team", value: "finance" },
          { key: "team", value: "sales" },
        ],
        selection,
      ),
    ).toBe(true);
    expect(tagPairsMatchFilter([{ key: "region", value: "eu" }], selection)).toBe(false);
  });

  test("key selected with values matches if any pair carries one of them", () => {
    const selection = sel([["team", ["finance"]]]);
    expect(
      tagPairsMatchFilter(
        [
          { key: "team", value: "sales" },
          { key: "team", value: "finance" },
        ],
        selection,
      ),
    ).toBe(true);
    expect(tagPairsMatchFilter([{ key: "team", value: "ops" }], selection)).toBe(false);
  });

  test("OR semantics across keys", () => {
    const selection = sel([
      ["team", ["finance"]],
      ["region", ["eu"]],
    ]);
    expect(tagPairsMatchFilter([{ key: "region", value: "eu" }], selection)).toBe(true);
    expect(tagPairsMatchFilter([{ key: "team", value: "ops" }], selection)).toBe(false);
  });
});
