import { describe, expect, it } from "bun:test";
import {
  buildContextPreamble,
  buildSuggestedQuestions,
  withContext,
} from "./genieSuggestedQuestions";

/** Flatten every question across categories for easy membership checks. */
function allQuestions(kind: "table" | "product" | "rule" | undefined) {
  return buildSuggestedQuestions(kind).flatMap((c) => c.questions);
}

describe("buildSuggestedQuestions", () => {
  it("keeps the four-category structure (label keys, in order)", () => {
    for (const kind of ["table", "product", "rule"] as const) {
      expect(buildSuggestedQuestions(kind).map((c) => c.labelKey)).toEqual([
        "genie.categoryBasicStats",
        "genie.categoryDrilldown",
        "genie.categoryTrends",
        "genie.categoryDiagnose",
      ]);
    }
  });

  it("questions are the canonical English strings the space is taught", () => {
    // The space's curated example SQLs key on these exact strings — they are
    // displayed AND sent verbatim, never translated (see the module comment).
    expect(allQuestions("table")).toContain(
      "What is the current data quality score?",
    );
    expect(allQuestions("product")).toContain(
      "Which tables have the lowest pass rate?",
    );
    for (const q of allQuestions("table")) {
      expect(q).not.toMatch(/this (table|data product)/);
    }
  });

  it("words rule-scoped questions around 'this rule'", () => {
    const qs = allQuestions("rule");
    expect(qs).toContain("What is this rule's overall pass rate?");
    // Rule questions are about the rule's spread, never a single table's health.
    for (const q of qs) {
      expect(q.toLowerCase()).not.toContain("rows that failed");
    }
    expect(qs.some((q) => /this rule/.test(q))).toBe(true);
  });

  it("words the rule change-since-last-run prompt by direction", () => {
    const prompt = (dir?: "up" | "down") =>
      buildSuggestedQuestions("rule", dir)
        .find((c) => c.labelKey === "genie.categoryDiagnose")!
        .questions[0];
    expect(prompt()).toBe("Why did this rule's pass rate change since the last run?");
    expect(prompt("up")).toBe("Why did this rule's pass rate increase since the last run?");
    expect(prompt("down")).toBe("Why did this rule's pass rate decrease since the last run?");
  });

  it("words the change-since-last-run prompt by direction", () => {
    const prompt = (dir?: "up" | "down") =>
      buildSuggestedQuestions("table", dir)
        .find((c) => c.labelKey === "genie.categoryDiagnose")!
        .questions[0];
    expect(prompt()).toBe("Why did my DQ score change since the last run?");
    expect(prompt("up")).toBe(
      "Why did my DQ score increase since the last run?",
    );
    expect(prompt("down")).toBe(
      "Why did my DQ score decrease since the last run?",
    );
  });

  it("drops dqlake's row-level table prompts (aggregates-only space)", () => {
    // The SP-owned space has no dq_quarantine_records (P3.8 controller
    // decision) — the "show me the rows" prompts must not resurface.
    for (const q of allQuestions("table")) {
      expect(q.toLowerCase()).not.toContain("rows that failed");
      expect(q.toLowerCase()).not.toContain("failing rows");
    }
  });
});

describe("buildContextPreamble", () => {
  it("wraps a table subject", () => {
    expect(buildContextPreamble("table", "cat.sch.tbl")).toBe(
      "(Table: cat.sch.tbl)",
    );
  });

  it("defaults to Table when the kind is unknown", () => {
    expect(buildContextPreamble(undefined, "cat.sch.tbl")).toBe(
      "(Table: cat.sch.tbl)",
    );
  });

  it("wraps a rule subject", () => {
    expect(buildContextPreamble("rule", "id_not_null")).toBe("(Rule: id_not_null)");
  });

  it("carries the product's member tables (P3.9 instructions route on it)", () => {
    expect(
      buildContextPreamble("product", "Customer 360", [
        "cat.sch.orders",
        "cat.sch.users",
      ]),
    ).toBe("(Data product: Customer 360 — tables: cat.sch.orders, cat.sch.users)");
  });

  it("falls back to the bare product form without member tables", () => {
    expect(buildContextPreamble("product", "Customer 360")).toBe(
      "(Data product: Customer 360)",
    );
    expect(buildContextPreamble("product", "Customer 360", ["", "  "])).toBe(
      "(Data product: Customer 360)",
    );
  });

  it("returns empty when there is no subject", () => {
    expect(buildContextPreamble("table", undefined)).toBe("");
    expect(buildContextPreamble("product", "   ", ["cat.sch.t"])).toBe("");
  });
});

describe("withContext", () => {
  it("appends the preamble on its own paragraph", () => {
    expect(withContext("Which rules are failing?", "table", "c.s.t")).toBe(
      "Which rules are failing?\n\n(Table: c.s.t)",
    );
  });

  it("passes member tables through for products", () => {
    expect(
      withContext("What is the overall data quality score?", "product", "P", [
        "c.s.a",
        "c.s.b",
      ]),
    ).toBe(
      "What is the overall data quality score?\n\n(Data product: P — tables: c.s.a, c.s.b)",
    );
  });

  it("leaves the question untouched without a subject", () => {
    expect(withContext("How many rules do I have?", "table", undefined)).toBe(
      "How many rules do I have?",
    );
  });
});
