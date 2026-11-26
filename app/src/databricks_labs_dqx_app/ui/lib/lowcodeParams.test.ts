import { describe, expect, test } from "bun:test";
import { compileAstToSql, type LowcodeColumnRef } from "./lowcodeCompile";
import { collectAstColumnRefs, collectAstParamRefs, renameParamInAst, type AnyRow, type LowcodeAstV2 } from "./lowcodeAst";
import { parseSqlToLowcode } from "./sqlToLowcode";

const row = (over: Partial<AnyRow> = {}): AnyRow =>
  ({ kind: "row", combinator: null, column_ref: "amount", operator: "=", value: null, ...over }) as AnyRow;
const ast = (rows: AnyRow[]): LowcodeAstV2 => ({ rows, joins: [] });
const COLUMNS: LowcodeColumnRef[] = [
  { name: "amount", family: "NUMERIC" },
  { name: "tier", family: "TEXTUAL" },
];

describe("visual builder parameter references", () => {
  test("a parameter value compiles to its bare token for every operand kind", () => {
    expect(compileAstToSql(ast([row({ operator: ">", value: { $param: "limit" } })]))).toBe("{{amount}} > {{limit}}");
    expect(
      compileAstToSql(ast([row({ column_ref: "tier", operator: "in", value: ["Free", { $param: "allowed" }] })])),
    ).toBe("{{tier}} IN ('Free', {{allowed}})");
    expect(
      compileAstToSql(ast([row({ column_ref: "tier", operator: "contains", value: { $param: "needle" } })])),
    ).toBe("contains({{tier}}, {{needle}})");
  });

  test("parameters are never qualified with the input view", () => {
    expect(compileAstToSql(ast([row({ operator: "<", value: { $param: "limit" } })]), true)).toBe(
      "{{input_view}}.{{amount}} < {{limit}}",
    );
  });

  test("parameter refs are not counted as columns", () => {
    const a = ast([row({ operator: ">", value: { $param: "limit" } })]);
    expect([...collectAstColumnRefs(a)]).toEqual(["amount"]);
    expect([...collectAstParamRefs(a)]).toEqual(["limit"]);
  });

  test("renaming a parameter rewrites its references", () => {
    const a = ast([row({ operator: "in", value: [{ $param: "limit" }, 3] })]);
    expect(renameParamInAst(a, "limit", "cap").rows[0].value).toEqual([{ $param: "cap" }, 3]);
  });

  test("SQL import turns a declared parameter token into a parameter ref", () => {
    const { rows } = parseSqlToLowcode("{{amount}} > {{limit}}", COLUMNS, ["limit"]);
    expect(rows[0].value).toEqual({ $param: "limit" });
    // Round-trips back to the same SQL.
    expect(compileAstToSql(ast(rows))).toBe("{{amount}} > {{limit}}");
  });
});
