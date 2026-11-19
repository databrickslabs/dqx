import { describe, expect, test } from "bun:test";
import type { LabelDefinition } from "./api-custom";
import { initialTagValue, missingRequiredTags, seedTagDefaults } from "./rule-tag-defaults";

const def = (over: Partial<LabelDefinition>): LabelDefinition => ({
  key: "team",
  values: ["data-eng", "finance"],
  allow_custom_values: false,
  ...over,
});

describe("initialTagValue", () => {
  test("uses the default value when it is one of the allowed values", () => {
    expect(initialTagValue(def({ default_value: "finance" }))).toBe("finance");
  });

  test("ignores a default that is no longer an allowed value", () => {
    expect(initialTagValue(def({ default_value: "gone" }))).toBeNull();
  });

  test("adds a mandatory tag with no default as blank", () => {
    expect(initialTagValue(def({ required: true }))).toBe("");
  });

  test("adds a mandatory boolean tag as true", () => {
    expect(initialTagValue(def({ values: [], required: true }))).toBe("true");
  });

  test("leaves an optional tag with no default off the rule", () => {
    expect(initialTagValue(def({}))).toBeNull();
  });
});

describe("seedTagDefaults", () => {
  const defs = [
    def({ key: "team", default_value: "finance" }),
    def({ key: "owner", required: true }),
    def({ key: "note" }),
  ];

  test("adds defaulted and mandatory tags", () => {
    expect(seedTagDefaults({}, defs)).toEqual({ team: "finance", owner: "" });
  });

  test("never overwrites a tag the rule already has", () => {
    const tags = { team: "data-eng", owner: "x" };
    expect(seedTagDefaults(tags, defs)).toBe(tags);
  });
});

describe("missingRequiredTags", () => {
  const defs = [def({ key: "owner", required: true }), def({ key: "team" })];

  test("reports mandatory tags that are absent or blank", () => {
    expect(missingRequiredTags({}, defs)).toEqual(["owner"]);
    expect(missingRequiredTags({ owner: "  " }, defs)).toEqual(["owner"]);
  });

  test("is empty once every mandatory tag has a value", () => {
    expect(missingRequiredTags({ owner: "data-eng" }, defs)).toEqual([]);
  });

  test("never reports dimension or severity", () => {
    expect(missingRequiredTags({}, [def({ key: "severity", required: true })])).toEqual([]);
  });
});
