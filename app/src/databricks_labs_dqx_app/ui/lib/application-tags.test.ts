import { describe, expect, test } from "bun:test";
import {
  appliedRuleTags,
  applicationTagsFrom,
  genericRuleMetadata,
  tagSignature,
  withAppliedSystemKeys,
} from "./application-tags";

describe("application tags", () => {
  const imported = {
    name: "Not null",
    severity: "High",
    slot_tags: { column: ["class.pii"] },
    pass_threshold: "95",
    businessTerm: "GGR",
    weight: 2,
    nested: { a: 1 },
  };

  test("registry rules keep only reserved keys", () => {
    expect(genericRuleMetadata(imported)).toEqual({
      name: "Not null",
      severity: "High",
      slot_tags: { column: ["class.pii"] },
      pass_threshold: "95",
    });
  });

  test("custom scalar tags move to the applied rule", () => {
    expect(applicationTagsFrom(imported)).toEqual({ businessTerm: "GGR", weight: "2" });
  });

  test("applied-rule system keys are hidden and preserved on edit", () => {
    const md = {
      origin: "tag_auto_apply",
      column_pass_thresholds: { a: 90 },
      parameter_values: { '{"column":"a"}': { limit: 10 } },
      team: "payments",
    };
    expect(appliedRuleTags(md)).toEqual({ team: "payments" });
    expect(withAppliedSystemKeys(md, { team: "risk" })).toEqual({
      origin: "tag_auto_apply",
      column_pass_thresholds: { a: 90 },
      parameter_values: { '{"column":"a"}': { limit: 10 } },
      team: "risk",
    });
  });

  test("tag signatures are order independent", () => {
    expect(tagSignature({ b: "2", a: "1" })).toBe(tagSignature({ a: "1", b: "2" }));
  });
});
