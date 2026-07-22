import { describe, expect, test } from "bun:test";
import { buildTableFqn, parseTableFqn } from "./joinTableFqn";

describe("parseTableFqn", () => {
  test("splits a complete three-part FQN", () => {
    expect(parseTableFqn("cat.sch.tbl")).toEqual({ catalog: "cat", schema: "sch", table: "tbl" });
  });

  test("returns empty parts for unset / null / undefined", () => {
    const empty = { catalog: "", schema: "", table: "" };
    expect(parseTableFqn("")).toEqual(empty);
    expect(parseTableFqn(null)).toEqual(empty);
    expect(parseTableFqn(undefined)).toEqual(empty);
  });

  test("returns empty parts for incomplete or malformed FQNs", () => {
    const empty = { catalog: "", schema: "", table: "" };
    expect(parseTableFqn("cat")).toEqual(empty);
    expect(parseTableFqn("cat.sch")).toEqual(empty);
    expect(parseTableFqn("cat.sch.tbl.extra")).toEqual(empty);
    expect(parseTableFqn("cat..tbl")).toEqual(empty);
  });
});

describe("buildTableFqn", () => {
  test("joins three non-empty parts", () => {
    expect(buildTableFqn("cat", "sch", "tbl")).toBe("cat.sch.tbl");
  });

  test("returns empty string when any part is missing", () => {
    expect(buildTableFqn("", "sch", "tbl")).toBe("");
    expect(buildTableFqn("cat", "", "tbl")).toBe("");
    expect(buildTableFqn("cat", "sch", "")).toBe("");
    expect(buildTableFqn("", "", "")).toBe("");
  });

  test("round-trips with parseTableFqn", () => {
    const parts = parseTableFqn("customer360.silver.orders");
    expect(buildTableFqn(parts.catalog, parts.schema, parts.table)).toBe("customer360.silver.orders");
  });
});
