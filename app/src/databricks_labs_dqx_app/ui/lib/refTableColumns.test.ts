import { describe, expect, test } from "bun:test";
import {
  findReferenceTables,
  inferRefTableColumns,
  normalizeTableRef,
  refTableAlias,
} from "./refTableColumns";

const REF = "main.sales.customers";
const ORPHAN =
  "SELECT {{order_id}}, (c.id IS NULL) AS condition\n" +
  "FROM {{input_view}}\n" +
  `LEFT JOIN ${REF} c ON c.id = {{customer_id}}`;

describe("normalizeTableRef", () => {
  test("leaves a plain name alone", () => {
    expect(normalizeTableRef(REF)).toBe(REF);
  });

  test("backticks and the spacing around dots are typing, not the name", () => {
    expect(normalizeTableRef("`main` . `sales`.`customers`")).toBe(REF);
  });

  test("keeps a doubled backtick as one literal backtick", () => {
    expect(normalizeTableRef("main.sales.`od``d`")).toBe("main.sales.od`d");
  });
});

describe("findReferenceTables", () => {
  test("finds the joined table", () => {
    expect(findReferenceTables(ORPHAN)).toEqual([REF]);
  });

  test("the input view and bare relations are not tables the author supplies", () => {
    expect(findReferenceTables("SELECT (x) AS condition FROM {{input_view}} JOIN src s ON s.k = 1")).toEqual([]);
  });

  test("a reference written quoted is the same table", () => {
    expect(findReferenceTables("FROM {{input_view}} JOIN `main`.`sales`.`customers` c ON c.id = 1")).toEqual([REF]);
  });

  test("dedupes case-insensitively — Unity Catalog names are", () => {
    const sql = `FROM {{input_view}} JOIN ${REF} c ON c.id = 1 LEFT JOIN MAIN.SALES.CUSTOMERS d ON d.id = 2`;
    expect(findReferenceTables(sql)).toEqual([REF]);
  });

  test("does not ask for a grid for a commented-out join", () => {
    expect(findReferenceTables("FROM {{input_view}}\n-- LEFT JOIN main.ref.old o ON o.id = 1")).toEqual([]);
  });

  test("keeps first-appearance order across several tables", () => {
    const sql = "FROM {{input_view}} JOIN main.ref.b b ON b.id = 1 JOIN main.ref.a a ON a.id = 2";
    expect(findReferenceTables(sql)).toEqual(["main.ref.b", "main.ref.a"]);
  });
});

describe("refTableAlias", () => {
  test("finds the alias following the table", () => {
    expect(refTableAlias(ORPHAN, REF)).toBe("c");
  });

  test("finds an explicit AS alias", () => {
    expect(refTableAlias(`JOIN ${REF} AS cust ON cust.id = {{cid}}`, REF)).toBe("cust");
  });

  test("matches however the author spelled the table", () => {
    expect(refTableAlias("JOIN `main`.`sales`.`customers` c ON c.id = 1", REF)).toBe("c");
  });

  test("returns null when the join declares no alias", () => {
    expect(refTableAlias(`JOIN ${REF} ON ${REF}.id = {{cid}}`, REF)).toBeNull();
  });

  test("returns null for a table that isn't joined", () => {
    expect(refTableAlias(ORPHAN, "main.ref.other")).toBeNull();
  });

  test("ignores a table that only appears in a comment", () => {
    expect(refTableAlias(`-- JOIN ${REF} c\nSELECT 1`, REF)).toBeNull();
  });
});

describe("inferRefTableColumns", () => {
  test("collects the alias-qualified columns in first-appearance order", () => {
    expect(inferRefTableColumns(ORPHAN, REF)).toEqual(["id"]);
  });

  test("dedupes repeated references", () => {
    const sql = `JOIN ${REF} c ON c.id = {{cid}} WHERE c.tier > 0 AND c.id IS NOT NULL`;
    expect(inferRefTableColumns(sql, REF)).toEqual(["id", "tier"]);
  });

  test("does not pick up another alias's columns", () => {
    const sql = `JOIN ${REF} c ON c.id = {{cid}} LEFT JOIN main.ref.other d ON d.k = {{cid}} AND d.zzz > 1`;
    expect(inferRefTableColumns(sql, REF)).toEqual(["id"]);
  });

  test("returns nothing when there is no alias to attribute columns to", () => {
    expect(inferRefTableColumns(`JOIN ${REF} ON x = 1`, REF)).toEqual([]);
  });
});
