import { describe, expect, test } from "bun:test";
import {
  BY_TABLE_PREVERIFY_LIMIT,
  VERIFY_REQUEST_MAX_FQNS,
  isPlainTableFqn,
  selectFqnsToVerify,
} from "./entitlement-preverify";

const NONE: ReadonlySet<string> = new Set();

describe("constants", () => {
  test("request cap mirrors the backend's VERIFY_ENTITLEMENTS_MAX_FQNS", () => {
    // GenieVerifyEntitlementsIn caps table_fqns at 50 (422 above it).
    expect(VERIFY_REQUEST_MAX_FQNS).toBe(50);
  });

  test("the global by-table surface sends the first 25", () => {
    expect(BY_TABLE_PREVERIFY_LIMIT).toBe(25);
  });
});

describe("isPlainTableFqn", () => {
  test("accepts plain three-part names, including hyphens and mixed case", () => {
    expect(isPlainTableFqn("main.sales.orders")).toBe(true);
    expect(isPlainTableFqn("my-catalog.my-schema.t1")).toBe(true);
    expect(isPlainTableFqn("Main.Sales.Orders_2024")).toBe(true);
  });

  test("rejects non-three-part names", () => {
    expect(isPlainTableFqn("orders")).toBe(false);
    expect(isPlainTableFqn("sales.orders")).toBe(false);
    expect(isPlainTableFqn("a.b.c.d")).toBe(false);
    expect(isPlainTableFqn("a..c")).toBe(false);
    expect(isPlainTableFqn(" . . ")).toBe(false);
  });

  test("rejects synthetic cross-table SQL-check keys", () => {
    // Not a real table — an entitlement probe could never open rows for it.
    expect(isPlainTableFqn("__sql_check__/my_check")).toBe(false);
  });

  test("rejects backtick-quoted parts", () => {
    // The runner writes source_table_fqn UNQUOTED; a quoted entitlement row
    // would never string-match it in the gated view.
    expect(isPlainTableFqn("`main`.`sales`.`orders`")).toBe(false);
  });
});

describe("selectFqnsToVerify", () => {
  test("keeps candidate order and drops null/blank entries", () => {
    expect(
      selectFqnsToVerify(["a.b.c", null, undefined, "", "d.e.f"], NONE),
    ).toEqual(["a.b.c", "d.e.f"]);
  });

  test("dedupes within the batch, first occurrence wins", () => {
    expect(selectFqnsToVerify(["a.b.c", "d.e.f", "a.b.c"], NONE)).toEqual([
      "a.b.c",
      "d.e.f",
    ]);
  });

  test("skips FQNs already attempted this session", () => {
    expect(
      selectFqnsToVerify(["a.b.c", "d.e.f"], new Set(["a.b.c"])),
    ).toEqual(["d.e.f"]);
  });

  test("filters non-plain FQNs (synthetic keys, quoted parts, fragments)", () => {
    expect(
      selectFqnsToVerify(
        ["__sql_check__/x", "`a`.`b`.`c`", "just-a-table", "a.b.c"],
        NONE,
      ),
    ).toEqual(["a.b.c"]);
  });

  test("caps at the given limit, counting only accepted FQNs", () => {
    const candidates = [
      "__sql_check__/x", // filtered, must not consume the cap
      "a.b.t1",
      "a.b.t2",
      "a.b.t3",
    ];
    expect(selectFqnsToVerify(candidates, NONE, 2)).toEqual(["a.b.t1", "a.b.t2"]);
  });

  test("a limit above the server cap is clamped to it", () => {
    const candidates = Array.from({ length: 60 }, (_, i) => `a.b.t${i}`);
    expect(selectFqnsToVerify(candidates, NONE, 999)).toHaveLength(
      VERIFY_REQUEST_MAX_FQNS,
    );
  });

  test("defaults to the server request cap", () => {
    const candidates = Array.from({ length: 60 }, (_, i) => `a.b.t${i}`);
    expect(selectFqnsToVerify(candidates, NONE)).toHaveLength(VERIFY_REQUEST_MAX_FQNS);
  });

  test("returns empty for empty or fully-attempted input (callers then skip the POST)", () => {
    expect(selectFqnsToVerify([], NONE)).toEqual([]);
    expect(selectFqnsToVerify(["a.b.c"], new Set(["a.b.c"]))).toEqual([]);
  });
});
