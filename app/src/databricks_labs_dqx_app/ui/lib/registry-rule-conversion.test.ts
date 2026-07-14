import { describe, expect, test } from "bun:test";
import {
  deriveSlotsAndParameters,
  fnSupportsNegate,
  slotTagsFromUserMetadata,
  userMetadataWithSlotTags,
} from "./registry-rule-conversion";
import { familyForSparkType } from "./slot-mapping";
import type { CheckFunctionDef } from "./api";

// Unit tests for the registry-rule <-> DQX-check conversion helpers touched by
// P19-D items 10 (typed slot families + ARRAY) and 11 (negate -> polarity).
// Run via `bun test` (see `make app-test-ui`).

const fn = (over: Partial<CheckFunctionDef> = {}): CheckFunctionDef =>
  ({ name: "x", rule_type: "row", category: "Other", doc: "", params: [], ...over }) as CheckFunctionDef;

const param = (name: string, kind: string, extra: Record<string, unknown> = {}) =>
  ({ name, kind, required: false, ...extra }) as CheckFunctionDef["params"][number];

describe("fnSupportsNegate", () => {
  test("true when the function declares a negate argument", () => {
    expect(fnSupportsNegate(fn({ params: [param("column", "column"), param("negate", "boolean")] }))).toBe(true);
  });
  test("false when there is no negate argument", () => {
    expect(fnSupportsNegate(fn({ params: [param("column", "column")] }))).toBe(false);
  });
  test("false for undefined", () => {
    expect(fnSupportsNegate(undefined)).toBe(false);
  });
});

describe("deriveSlotsAndParameters — item 10 family seeding + item 11 negate skip", () => {
  test("seeds the column slot family from the param's implied family", () => {
    const { slots } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column", { family: "temporal" })] }),
    );
    expect(slots).toHaveLength(1);
    expect(slots[0]?.family).toBe("temporal");
  });

  test("falls back to any when the param carries no family", () => {
    const { slots } = deriveSlotsAndParameters(fn({ params: [param("column", "column")] }));
    expect(slots[0]?.family).toBe("any");
  });

  test("carries the array family through for array-column checks", () => {
    const { slots } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column", { family: "array" })] }),
    );
    expect(slots[0]?.family).toBe("array");
  });

  test("drops negate from the derived parameter list (surfaced as polarity)", () => {
    const { parameters } = deriveSlotsAndParameters(
      fn({ params: [param("column", "column"), param("regex", "string"), param("negate", "boolean")] }),
    );
    expect(parameters.map((p) => p.name)).toEqual(["regex"]);
  });
});

describe("familyForSparkType — ARRAY family (item 10)", () => {
  test("classifies array column types as array", () => {
    expect(familyForSparkType("array<string>")).toBe("array");
    expect(familyForSparkType("ARRAY<INT>")).toBe("array");
  });
  test("still classifies the primitive families", () => {
    expect(familyForSparkType("bigint")).toBe("numeric");
    expect(familyForSparkType("string")).toBe("text");
    expect(familyForSparkType("timestamp")).toBe("temporal");
    expect(familyForSparkType("boolean")).toBe("boolean");
    expect(familyForSparkType("struct<a:int>")).toBe("any");
  });
});

// ── Admin-editable severity -> criticality mapping (DQ Score Task 2) ────────
// `resolveCriticality` prefers the `value_criticality` map stored on the
// reserved `severity` label definition, mirroring the backend's
// `registry_models.resolve_criticality` resolution order: stored entry →
// built-in default → "warn".

import {
  buildDqxCheckJson,
  parseDqxCheckJson,
  resolveCriticality,
  severityValueCriticality,
} from "./registry-rule-conversion";
import type { RegistryRuleOut, RuleDefinition } from "./api";

describe("resolveCriticality — stored mapping precedence", () => {
  test("uses the stored mapping over the built-in default", () => {
    expect(resolveCriticality("Critical", { Critical: "warn" })).toBe("warn");
    expect(resolveCriticality("Low", { Low: "error" })).toBe("error");
  });

  test("falls back to the built-in default for unmapped severities", () => {
    expect(resolveCriticality("High", { Critical: "warn" })).toBe("error");
    expect(resolveCriticality("Low")).toBe("warn");
    expect(resolveCriticality("Critical")).toBe("error");
  });

  test("ignores stored values that are neither warn nor error", () => {
    expect(resolveCriticality("High", { High: "fatal" })).toBe("error");
  });

  test("defaults to warn for missing or unknown severities", () => {
    expect(resolveCriticality(undefined)).toBe("warn");
    expect(resolveCriticality("Custom")).toBe("warn");
    expect(resolveCriticality("Custom", { Custom: "error" })).toBe("error");
  });
});

describe("severityValueCriticality — map extraction from label definitions", () => {
  test("returns the severity definition's value_criticality map", () => {
    expect(
      severityValueCriticality([
        { key: "dimension", value_criticality: null },
        { key: "severity", value_criticality: { Critical: "warn" } },
      ]),
    ).toEqual({ Critical: "warn" });
  });

  test("returns undefined when absent", () => {
    expect(severityValueCriticality(undefined)).toBeUndefined();
    expect(severityValueCriticality([])).toBeUndefined();
    expect(severityValueCriticality([{ key: "severity" }])).toBeUndefined();
    expect(severityValueCriticality([{ key: "severity", value_criticality: null }])).toBeUndefined();
  });
});

describe("buildDqxCheckJson — criticality honours the admin mapping", () => {
  const rule = (severity: string): RegistryRuleOut =>
    ({
      mode: "dqx_native",
      polarity: null,
      definition: {
        body: { function: "is_not_null", arguments: { column: "{{column_1}}" } },
        slots: [],
        parameters: [],
      },
      user_metadata: { severity },
    }) as unknown as RegistryRuleOut;

  test("built-in default without a stored mapping", () => {
    expect(buildDqxCheckJson(rule("Critical")).criticality).toBe("error");
  });

  test("stored mapping wins when provided", () => {
    expect(buildDqxCheckJson(rule("Critical"), { Critical: "warn" }).criticality).toBe("warn");
  });
});

// ── parseDqxCheckJson: slot-name round-trip (item 32) + severity import (56) ──

const identity = (key: string) => key;
const emptyDefinition: RuleDefinition = { body: {}, slots: [], parameters: [] } as unknown as RuleDefinition;

const parse = (
  check: Record<string, unknown>,
  checkFunctions: CheckFunctionDef[],
  current: Record<string, unknown> | null = null,
) => parseDqxCheckJson(JSON.stringify(check), emptyDefinition, current, checkFunctions, identity);

describe("parseDqxCheckJson — reusable slot names survive the round-trip (item 32)", () => {
  const isNotNull = fn({ name: "is_not_null", params: [param("column", "column")] });
  const foreignKey = fn({ name: "foreign_key", params: [param("columns", "columns")] });

  test("adopts the author's {{name}} token as the slot name", () => {
    const result = parse(
      { check: { function: "is_not_null", arguments: { column: "{{customer_id}}" } } },
      [isNotNull],
    );
    expect(result.mode).toBe("dqx_native");
    expect(result.definition.slots?.map((s) => s.name)).toEqual(["customer_id"]);
    const args = (result.definition.body as { arguments: Record<string, unknown> }).arguments;
    expect(args.column).toBe("{{customer_id}}");
  });

  test("expands a list argument into one slot per {{token}}", () => {
    const result = parse(
      { check: { function: "foreign_key", arguments: { columns: ["{{order_id}}", "{{line_no}}"] } } },
      [foreignKey],
    );
    expect(result.definition.slots?.map((s) => s.name)).toEqual(["order_id", "line_no"]);
    const args = (result.definition.body as { arguments: Record<string, unknown> }).arguments;
    expect(args.columns).toEqual(["{{order_id}}", "{{line_no}}"]);
  });

  test("falls back to the canonical column_N name when no placeholder is present", () => {
    const result = parse(
      { check: { function: "is_not_null", arguments: { column: "not_a_placeholder" } } },
      [isNotNull],
    );
    expect(result.definition.slots?.map((s) => s.name)).toEqual(["column_1"]);
  });
});

describe("parseDqxCheckJson — severity is authoritative on import (item 56)", () => {
  const isNotNull = fn({ name: "is_not_null", params: [param("column", "column")] });
  const severityOf = (result: { userMetadata: Record<string, string> }) => result.userMetadata.severity;

  test("back-fills a representative severity from a criticality-only JSON", () => {
    const err = parse(
      { criticality: "error", check: { function: "is_not_null", arguments: { column: "{{c}}" } } },
      [isNotNull],
    );
    expect(severityOf(err)).toBe("High");
    const warn = parse(
      { criticality: "warn", check: { function: "is_not_null", arguments: { column: "{{c}}" } } },
      [isNotNull],
    );
    expect(severityOf(warn)).toBe("Medium");
  });

  test("user_metadata.severity wins over a conflicting criticality", () => {
    const result = parse(
      {
        criticality: "warn",
        check: { function: "is_not_null", arguments: { column: "{{c}}" } },
        user_metadata: { severity: "Critical" },
      },
      [isNotNull],
    );
    expect(severityOf(result)).toBe("Critical");
  });

  test("leaves severity unset when neither criticality nor severity is present", () => {
    const result = parse(
      { check: { function: "is_not_null", arguments: { column: "{{c}}" } } },
      [isNotNull],
    );
    expect(severityOf(result)).toBeUndefined();
  });
});

describe("slot_tags helpers (apply-on-tag) — mirror backend get_slot_tags/set_slot_tags", () => {
  test("round-trips a slot -> tags map through user_metadata", () => {
    const md = userMetadataWithSlotTags({ name: "x" }, { c1: ["class.pii"] });
    expect(md.slot_tags).toEqual({ c1: ["class.pii"] });
    expect(md.name).toBe("x");
    expect(slotTagsFromUserMetadata(md)).toEqual({ c1: ["class.pii"] });
  });

  test("returns {} when slot_tags is absent", () => {
    expect(slotTagsFromUserMetadata({})).toEqual({});
    expect(slotTagsFromUserMetadata(undefined)).toEqual({});
  });

  test("drops slots with empty tag lists on write", () => {
    const md = userMetadataWithSlotTags({}, { c1: ["class.pii"], c2: [] });
    expect(md.slot_tags).toEqual({ c1: ["class.pii"] });
  });

  test("removes the slot_tags key entirely when the map is empty", () => {
    const md = userMetadataWithSlotTags({ slot_tags: { c1: ["class.pii"] } }, { c1: [] });
    expect("slot_tags" in md).toBe(false);
  });

  test("ignores non-array slot values and non-string tags on read", () => {
    expect(
      slotTagsFromUserMetadata({
        slot_tags: { c1: "class.pii", c2: ["class.ok", 3, "", null], c3: ["class.a"] },
      } as Record<string, unknown>),
    ).toEqual({ c2: ["class.ok"], c3: ["class.a"] });
  });

  test("returns {} when slot_tags is a non-object (array / string)", () => {
    expect(slotTagsFromUserMetadata({ slot_tags: ["x"] } as Record<string, unknown>)).toEqual({});
    expect(slotTagsFromUserMetadata({ slot_tags: "nope" } as Record<string, unknown>)).toEqual({});
  });

  test("does not mutate the input user_metadata", () => {
    const md = { name: "x" };
    userMetadataWithSlotTags(md, { c1: ["class.pii"] });
    expect(md).toEqual({ name: "x" });
  });
});
