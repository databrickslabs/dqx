import { describe, expect, test } from "bun:test";
import { inferRefTableColumns, isReferenceTableSlot, refTableAlias } from "./refTableColumns";

const ORPHAN =
  "SELECT {{order_id}}, (c.id IS NULL) AS condition\n" +
  "FROM {{input_view}}\n" +
  "LEFT JOIN {{ref_table}} c ON c.id = {{customer_id}}";

describe("refTableAlias", () => {
  test("finds the alias following the placeholder", () => {
    expect(refTableAlias(ORPHAN, "ref_table")).toBe("c");
  });

  test("finds an explicit AS alias", () => {
    expect(refTableAlias("JOIN {{ref}} AS cust ON cust.id = {{cid}}", "ref")).toBe("cust");
  });

  test("tolerates whitespace inside the braces", () => {
    expect(refTableAlias("JOIN {{ ref }} c ON c.id = 1", "ref")).toBe("c");
  });

  test("returns null when the join declares no alias", () => {
    expect(refTableAlias("JOIN {{ref}} ON {{ref}}.id = {{cid}}", "ref")).toBeNull();
  });

  test("returns null for a slot that isn't joined", () => {
    expect(refTableAlias(ORPHAN, "other")).toBeNull();
  });

  test("ignores a placeholder that only appears in a comment", () => {
    expect(refTableAlias("-- JOIN {{ref}} c\nSELECT 1", "ref")).toBeNull();
  });
});

describe("isReferenceTableSlot", () => {
  test("a table-family slot names a reference table", () => {
    expect(isReferenceTableSlot({ name: "ref_table", family: "table" })).toBe(true);
  });

  test("a column slot does not", () => {
    expect(isReferenceTableSlot({ name: "customer_id", family: "text" })).toBe(false);
  });

  test("the reserved input_view does not — it IS the data being checked", () => {
    expect(isReferenceTableSlot({ name: "input_view", family: "table" })).toBe(false);
  });
});

describe("inferRefTableColumns", () => {
  test("collects the alias-qualified columns in first-appearance order", () => {
    expect(inferRefTableColumns(ORPHAN, "ref_table")).toEqual(["id"]);
  });

  test("dedupes repeated references", () => {
    const sql = "JOIN {{ref}} c ON c.id = {{cid}} WHERE c.tier > 0 AND c.id IS NOT NULL";
    expect(inferRefTableColumns(sql, "ref")).toEqual(["id", "tier"]);
  });

  test("does not pick up another alias's columns", () => {
    const sql = "JOIN {{ref}} c ON c.id = {{cid}} LEFT JOIN other.tbl d ON d.k = {{cid}} AND d.zzz > 1";
    expect(inferRefTableColumns(sql, "ref")).toEqual(["id"]);
  });

  test("returns nothing when there is no alias to attribute columns to", () => {
    expect(inferRefTableColumns("JOIN {{ref}} ON x = 1", "ref")).toEqual([]);
  });
});
