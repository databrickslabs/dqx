import { describe, expect, test } from "bun:test";
import { collectAstColumnRefs, type AnyRow, type LowcodeAstV2 } from "./lowcodeAst";

// Unit tests for the AST reference scan that the authoring form's
// "unused declared column(s)" gate rests on: a column the scan misses reads as
// unused, and Save & submit stays disabled on a rule that is in fact complete.

const ast = (rows: AnyRow[], joins: LowcodeAstV2["joins"] = []): LowcodeAstV2 => ({ rows, joins });

const row = (overrides: Partial<AnyRow> = {}): AnyRow =>
  ({
    kind: "row",
    combinator: null,
    column_ref: "ship_date",
    operator: "on or before",
    value: null,
    ...overrides,
  }) as AnyRow;

describe("collectAstColumnRefs", () => {
  test("collects a row's LHS column", () => {
    expect([...collectAstColumnRefs(ast([row()]))]).toEqual(["ship_date"]);
  });

  test("collects a column-vs-column RHS reference", () => {
    // The shape a "{{ship_date}} on or before {{delivered_date}}" rule builds:
    // both columns are referenced, so neither is unused.
    const refs = collectAstColumnRefs(ast([row({ value: { $col: "delivered_date" } })]));
    expect([...refs].sort()).toEqual(["delivered_date", "ship_date"]);
  });

  test("collects column references inside a between bound and an in list", () => {
    const refs = collectAstColumnRefs(
      ast([
        row({ operator: "between", value: [{ $col: "window_start" }, { $col: "window_end" }] }),
        row({ combinator: "AND", column_ref: "status", operator: "in", value: ["OPEN", { $col: "default_status" }] }),
      ]),
    );
    expect([...refs].sort()).toEqual(["default_status", "ship_date", "status", "window_end", "window_start"]);
  });

  test("collects an aggregated row's comparison spec column", () => {
    const aggregatedRow: AnyRow = {
      kind: "aggregated",
      combinator: null,
      aggregate: "sum",
      column_ref: "net_amount",
      operator: "<=",
      value: { aggregate: "sum", column_ref: "gross_amount" },
    };
    expect([...collectAstColumnRefs(ast([aggregatedRow]))].sort()).toEqual(["gross_amount", "net_amount"]);
  });

  test("collects join keys but not joined-table columns", () => {
    // A dotted name compiles to a raw identifier, not a {{slot}} placeholder, so
    // it is not a declared column and must not be reported as referenced.
    const joins: LowcodeAstV2["joins"] = [
      { join_type: "LEFT", target_table: "cat.sch.orders", keys: [{ joined_column: "id", column_ref: "order_id" }] },
    ];
    const refs = collectAstColumnRefs(ast([row({ value: { $col: "orders.delivered_date" } })], joins));
    expect([...refs].sort()).toEqual(["order_id", "ship_date"]);
  });

  test("ignores literal values and an empty AST", () => {
    expect([...collectAstColumnRefs(ast([row({ operator: "=", value: "SHIPPED" })]))]).toEqual(["ship_date"]);
    expect(collectAstColumnRefs(ast([])).size).toBe(0);
  });
});
