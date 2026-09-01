import { describe, expect, test } from "bun:test";
import type { AppliedRuleOut, ColumnOut, RuleSlot } from "@/lib/api";
import {
  columnMappingDriftKind,
  computeRuleSchemaDrift,
  computeSchemaDriftSummary,
  splitMappedColumns,
} from "./schemaDrift";

function col(name: string, type_name: string): ColumnOut {
  return { name, type_name };
}

function slot(name: string, family: RuleSlot["family"] = "any"): RuleSlot {
  return { name, family, cardinality: "one" } as RuleSlot;
}

function rule(
  ruleId: string,
  column_mapping: AppliedRuleOut["column_mapping"],
): Pick<AppliedRuleOut, "rule_id" | "column_mapping"> {
  return { rule_id: ruleId, column_mapping };
}

describe("splitMappedColumns", () => {
  test("splits comma-joined multi-value slots and trims", () => {
    expect(splitMappedColumns("a, b,c")).toEqual(["a", "b", "c"]);
  });

  test("empty / null → []", () => {
    expect(splitMappedColumns("")).toEqual([]);
    expect(splitMappedColumns(null)).toEqual([]);
    expect(splitMappedColumns(undefined)).toEqual([]);
  });
});

describe("computeRuleSchemaDrift", () => {
  const slots = [slot("amount", "numeric"), slot("label", "text")];
  const columns = [col("amount", "DOUBLE"), col("label", "STRING")];

  test("returns empty when live columns are unavailable", () => {
    const r = rule("r1", [{ amount: "gone" }]);
    expect(computeRuleSchemaDrift(r, slots, []).issues).toEqual([]);
  });

  test("returns empty when rule has no slots", () => {
    const r = rule("r1", [{ amount: "gone" }]);
    expect(computeRuleSchemaDrift(r, [], columns).issues).toEqual([]);
  });

  test("detects missing mapped columns", () => {
    const r = rule("r1", [{ amount: "deleted_col", label: "label" }]);
    const drift = computeRuleSchemaDrift(r, slots, columns);
    expect(drift.missingCount).toBe(1);
    expect(drift.typeMismatchCount).toBe(0);
    expect(drift.issues[0]).toMatchObject({
      kind: "missing",
      columnName: "deleted_col",
      slotName: "amount",
    });
  });

  test("detects type-family mismatches", () => {
    const r = rule("r1", [{ amount: "label", label: "label" }]);
    const drift = computeRuleSchemaDrift(r, slots, columns);
    expect(drift.typeMismatchCount).toBe(1);
    expect(drift.issues[0]).toMatchObject({
      kind: "type_mismatch",
      columnName: "label",
      slotName: "amount",
      slotFamily: "numeric",
      liveFamily: "text",
    });
  });

  test("skips type check when slot family is any", () => {
    const anySlots = [slot("col", "any")];
    const r = rule("r1", [{ col: "amount" }]);
    expect(computeRuleSchemaDrift(r, anySlots, columns).issues).toEqual([]);
  });

  test("skips type check when live type maps to any", () => {
    const weird = [col("payload", "STRUCT")];
    const r = rule("r1", [{ amount: "payload" }]);
    expect(computeRuleSchemaDrift(r, [slot("amount", "numeric")], weird).issues).toEqual([]);
  });

  test("splits multi-value mapped columns", () => {
    const r = rule("r1", [{ amount: "amount,gone" }]);
    const drift = computeRuleSchemaDrift(r, [slot("amount", "numeric")], columns);
    expect(drift.missingCount).toBe(1);
    expect(drift.issues[0].columnName).toBe("gone");
  });

  test("healthy mapping → no issues", () => {
    const r = rule("r1", [{ amount: "amount", label: "label" }]);
    expect(computeRuleSchemaDrift(r, slots, columns).issues).toEqual([]);
  });
});

describe("computeSchemaDriftSummary", () => {
  test("aggregates across rules and lists affected ids", () => {
    const columns = [col("amount", "DOUBLE")];
    const slotsByRuleId = new Map<string, RuleSlot[]>([
      ["r1", [slot("amount", "numeric")]],
      ["r2", [slot("amount", "numeric")]],
    ]);
    const summary = computeSchemaDriftSummary(
      [rule("r1", [{ amount: "missing" }]), rule("r2", [{ amount: "amount" }])],
      columns,
      slotsByRuleId,
    );
    expect(summary.missingCount).toBe(1);
    expect(summary.affectedRuleIds).toEqual(["r1"]);
  });
});

describe("columnMappingDriftKind", () => {
  const columns = [col("amount", "DOUBLE"), col("label", "STRING")];

  test("missing wins over type mismatch in multi-value", () => {
    expect(columnMappingDriftKind("amount,gone", { family: "numeric" }, columns)).toBe("missing");
  });

  test("type mismatch when all columns exist but family differs", () => {
    expect(columnMappingDriftKind("label", { family: "numeric" }, columns)).toBe("type_mismatch");
  });

  test("null when healthy", () => {
    expect(columnMappingDriftKind("amount", { family: "numeric" }, columns)).toBeNull();
  });

  test("null when columns not loaded", () => {
    expect(columnMappingDriftKind("gone", { family: "numeric" }, [])).toBeNull();
  });
});
