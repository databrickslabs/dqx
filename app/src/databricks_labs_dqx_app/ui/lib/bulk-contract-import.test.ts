import { describe, expect, test } from "bun:test";
import type { ColumnOut, RuleSlot } from "@/lib/api";
import {
  aggregateByContract,
  chunk,
  classifyRuleMapping,
  isThreePartFqn,
  resolveTableFqn,
  type SchemaOutcome,
} from "./bulk-contract-import";

function slot(name: string, family: RuleSlot["family"] = "any", cardinality: RuleSlot["cardinality"] = "one"): RuleSlot {
  return { name, family, cardinality };
}

function col(name: string, type_name = "string"): ColumnOut {
  return { name, type_name };
}

function outcome(partial: Partial<SchemaOutcome> & Pick<SchemaOutcome, "contractId" | "filename">): SchemaOutcome {
  return {
    tableFqn: null,
    bindingId: null,
    registered: 0,
    alreadyMonitored: 0,
    created: 0,
    reused: 0,
    applied: 0,
    pending: 0,
    needsMapping: 0,
    failed: 0,
    errors: [],
    ...partial,
  };
}

describe("isThreePartFqn", () => {
  test("accepts exactly three non-empty parts", () => {
    expect(isThreePartFqn("cat.sch.tbl")).toBe(true);
  });

  test("rejects wrong part counts and empty segments", () => {
    expect(isThreePartFqn("sch.tbl")).toBe(false);
    expect(isThreePartFqn("a.b.c.d")).toBe(false);
    expect(isThreePartFqn("cat..tbl")).toBe(false);
    expect(isThreePartFqn("tbl")).toBe(false);
  });
});

describe("resolveTableFqn", () => {
  test("uses a physical_name that is already a 3-part FQN verbatim", () => {
    expect(
      resolveTableFqn({
        schemaName: "orders",
        physicalName: "prod.sales.orders",
        targetCatalog: "ignored",
        targetSchema: "ignored",
      }),
    ).toBe("prod.sales.orders");
  });

  test("places a bare physical_name under the target catalog.schema", () => {
    expect(
      resolveTableFqn({
        schemaName: "orders",
        physicalName: "orders_tbl",
        targetCatalog: "main",
        targetSchema: "dq",
      }),
    ).toBe("main.dq.orders_tbl");
  });

  test("falls back to schema_name when no physical_name", () => {
    expect(
      resolveTableFqn({
        schemaName: "orders",
        physicalName: null,
        targetCatalog: "main",
        targetSchema: "dq",
      }),
    ).toBe("main.dq.orders");
  });

  test("collapses a dotted (non-FQN) physical_name to its last segment", () => {
    expect(
      resolveTableFqn({
        schemaName: "orders",
        physicalName: "sales.orders",
        targetCatalog: "main",
        targetSchema: "dq",
      }),
    ).toBe("main.dq.orders");
  });

  test("returns null when the FQN can't be formed", () => {
    expect(
      resolveTableFqn({ schemaName: "orders", physicalName: "orders", targetCatalog: "", targetSchema: "dq" }),
    ).toBeNull();
    expect(
      resolveTableFqn({ schemaName: "", physicalName: null, targetCatalog: "main", targetSchema: "dq" }),
    ).toBeNull();
  });
});

describe("classifyRuleMapping", () => {
  test("whole-table when there are no slots", () => {
    expect(classifyRuleMapping([], null)).toBe("whole-table");
    expect(classifyRuleMapping([], [col("id")])).toBe("whole-table");
  });

  test("unknown when columns are uninspectable", () => {
    expect(classifyRuleMapping([slot("id")], null)).toBe("unknown");
  });

  test("auto when every slot name+family matches a column", () => {
    expect(classifyRuleMapping([slot("id", "numeric")], [col("id", "bigint")])).toBe("auto");
  });

  test("manual when a slot can't be auto-matched", () => {
    // No same-named column.
    expect(classifyRuleMapping([slot("customer_id")], [col("id")])).toBe("manual");
    // Family mismatch.
    expect(classifyRuleMapping([slot("id", "numeric")], [col("id", "string")])).toBe("manual");
    // "many" cardinality is never auto-mapped.
    expect(classifyRuleMapping([slot("cols", "any", "many")], [col("cols")])).toBe("manual");
  });
});

describe("chunk", () => {
  test("splits into consecutive chunks of at most size", () => {
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
  });

  test("returns a single chunk when smaller than size", () => {
    expect(chunk([1, 2], 10)).toEqual([[1, 2]]);
  });

  test("returns empty for empty input", () => {
    expect(chunk([], 3)).toEqual([]);
  });

  test("throws on non-positive size", () => {
    expect(() => chunk([1], 0)).toThrow();
  });
});

describe("aggregateByContract", () => {
  test("sums per-schema counters into one row per contract", () => {
    const results = aggregateByContract([
      outcome({
        contractId: "c1",
        filename: "orders.yaml",
        tableFqn: "main.dq.orders",
        bindingId: "b1",
        registered: 1,
        created: 3,
        reused: 1,
        applied: 2,
        pending: 1,
      }),
      outcome({
        contractId: "c1",
        filename: "orders.yaml",
        tableFqn: "main.dq.line_items",
        bindingId: "b2",
        registered: 0,
        alreadyMonitored: 1,
        created: 2,
        reused: 2,
        needsMapping: 1,
        failed: 1,
        errors: ["apply failed"],
      }),
      outcome({
        contractId: "c2",
        filename: "customers.yaml",
        tableFqn: "main.dq.customers",
        bindingId: "b3",
        registered: 1,
        created: 1,
        applied: 1,
      }),
    ]);

    expect(results).toHaveLength(2);
    const c1 = results.find((r) => r.contractId === "c1")!;
    expect(c1.registered).toBe(1);
    expect(c1.alreadyMonitored).toBe(1);
    expect(c1.created).toBe(5);
    expect(c1.reused).toBe(3);
    expect(c1.applied).toBe(2);
    expect(c1.pending).toBe(1);
    expect(c1.needsMapping).toBe(1);
    expect(c1.failed).toBe(1);
    expect(c1.errors).toEqual(["apply failed"]);
  });

  test("collects attention tables only for pending / needs-mapping with an FQN", () => {
    const [row] = aggregateByContract([
      outcome({
        contractId: "c1",
        filename: "f.yaml",
        tableFqn: "main.dq.pending_tbl",
        bindingId: "b1",
        pending: 2,
      }),
      outcome({
        contractId: "c1",
        filename: "f.yaml",
        tableFqn: "main.dq.manual_tbl",
        bindingId: "b2",
        needsMapping: 1,
      }),
      // Fully-applied schema → no attention entry.
      outcome({ contractId: "c1", filename: "f.yaml", tableFqn: "main.dq.done_tbl", bindingId: "b3", applied: 1 }),
      // Needs mapping but unresolved FQN → not deep-linkable, skipped.
      outcome({ contractId: "c1", filename: "f.yaml", tableFqn: null, bindingId: null, needsMapping: 1 }),
    ]);

    expect(row.attentionTables).toEqual([
      { fqn: "main.dq.pending_tbl", bindingId: "b1", pending: 2, needsMapping: 0 },
      { fqn: "main.dq.manual_tbl", bindingId: "b2", pending: 0, needsMapping: 1 },
    ]);
  });
});
