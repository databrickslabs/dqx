import { describe, expect, test } from "bun:test";
import { compileAstToSql, compileJoinsToSql, compileLowcodeBody, pruneStaleGroupByRefs } from "./lowcodeCompile";
import type { LowcodeColumnRef } from "./lowcodeCompile";
import type { AnyRow, JoinAst, LowcodeAstV2 } from "./lowcodeAst";

// Unit tests for the low-code -> SQL compiler. This is the sole guard against
// the class of bug that shipped the Advanced (joins / group-by) paths broken:
// the compiler emits the body that materializes and runs, so a wrong shape
// here becomes a runtime failure in the DQX runner. Run via `bun test` (see
// `make app-test-ui`).

const row = (over: Partial<AnyRow> = {}): AnyRow =>
  ({ kind: "row", combinator: null, column_ref: "email", operator: "is not null", value: null, ...over }) as AnyRow;

const ast = (rows: AnyRow[], joins: JoinAst[] = []): LowcodeAstV2 => ({ rows, joins });

describe("compileAstToSql — predicate composition", () => {
  test("single row compiles the pass-condition with a {{slot}} ref", () => {
    expect(compileAstToSql(ast([row()]))).toBe("{{email}} IS NOT NULL");
  });

  test("multiple rows join with their combinator (first row has none)", () => {
    const rows = [
      row({ column_ref: "a", operator: "is not null" }),
      row({ combinator: "AND", column_ref: "b", operator: "is null" }),
      row({ combinator: "OR", column_ref: "c", operator: "is not null" }),
    ];
    expect(compileAstToSql(ast(rows))).toBe("{{a}} IS NOT NULL AND {{b}} IS NULL OR {{c}} IS NOT NULL");
  });

  test("qualified (dotted) refs pass through raw; plain refs stay wrapped", () => {
    expect(compileAstToSql(ast([row({ column_ref: "orders.total", operator: ">", value: 5 })]))).toBe(
      "orders.total > 5",
    );
  });

  test("empty row stack compiles to an empty string", () => {
    expect(compileAstToSql(ast([]))).toBe("");
  });
});

describe("operator SQL", () => {
  test("LIKE value single-quotes are escaped (O'Brien cannot break the literal)", () => {
    expect(compileAstToSql(ast([row({ column_ref: "name", operator: "contains", value: "O'Brien" })]))).toBe(
      "{{name}} LIKE '%O''Brien%'",
    );
    expect(compileAstToSql(ast([row({ column_ref: "name", operator: "starts with", value: "O'B" })]))).toBe(
      "{{name}} LIKE 'O''B%'",
    );
  });

  test("equality quotes strings and passes numbers/booleans through", () => {
    expect(compileAstToSql(ast([row({ column_ref: "s", operator: "equals", value: "x" })]))).toBe("{{s}} = 'x'");
    expect(compileAstToSql(ast([row({ column_ref: "n", operator: "=", value: 3 })]))).toBe("{{n}} = 3");
    expect(compileAstToSql(ast([row({ column_ref: "b", operator: "is true", value: null })]))).toBe("{{b}} = TRUE");
  });

  test("between / in render list + range forms", () => {
    expect(compileAstToSql(ast([row({ column_ref: "n", operator: "between", value: [1, 10] })]))).toBe(
      "{{n}} BETWEEN 1 AND 10",
    );
    expect(compileAstToSql(ast([row({ column_ref: "n", operator: "in", value: ["a", "b"] })]))).toBe(
      "{{n}} IN ('a', 'b')",
    );
  });

  test("aggregated row compiles the aggregate expression", () => {
    const agg: AnyRow = {
      kind: "aggregated",
      combinator: null,
      aggregate: "count",
      column_ref: "id",
      operator: ">",
      value: 1,
    };
    expect(compileAstToSql(ast([agg]))).toBe("COUNT({{id}}) > 1");
  });
});

describe("compileJoinsToSql", () => {
  test("LEFT JOIN emits the qualified ON condition against a {{slot}} ref", () => {
    const joins: JoinAst[] = [
      { join_type: "LEFT", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "customer_id" }] },
    ];
    expect(compileJoinsToSql(joins)).toBe("LEFT JOIN c.s.dim ON c.s.dim.id = {{customer_id}}");
  });

  test("CROSS JOIN emits no ON clause", () => {
    expect(compileJoinsToSql([{ join_type: "CROSS", target_table: "c.s.dim", keys: [] }])).toBe("CROSS JOIN c.s.dim");
  });
});

describe("compileLowcodeBody — folding", () => {
  test("no joins & no group-by -> { predicate } only (no NOT wrapping)", () => {
    const body = compileLowcodeBody(ast([row()]), "");
    expect(body).toEqual({ predicate: "{{email}} IS NOT NULL" });
    expect(body.sql_query).toBeUndefined();
    expect(body.merge_columns).toBeUndefined();
  });

  test("group-by -> row-level sql_query with NOT(P) wrapping + merge_columns = group columns", () => {
    const agg: AnyRow = {
      kind: "aggregated",
      combinator: null,
      aggregate: "count",
      column_ref: "id",
      operator: ">",
      value: 1,
    };
    const body = compileLowcodeBody(ast([agg]), "{{region}}");
    expect(body.sql_query).toBe(
      "SELECT {{region}}, (NOT (COUNT({{id}}) > 1)) AS condition FROM {{input_view}} GROUP BY {{region}}",
    );
    expect(body.merge_columns).toEqual(["{{region}}"]);
    expect(body.predicate).toBeUndefined();
  });

  test("joins only -> row-level sql_query merging on the input-side join keys", () => {
    const joins: JoinAst[] = [
      { join_type: "LEFT", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "customer_id" }] },
    ];
    const body = compileLowcodeBody(ast([row({ column_ref: "customer_id", operator: "is not null" })], joins), "");
    expect(body.sql_query).toBe(
      "SELECT {{customer_id}}, (NOT ({{customer_id}} IS NOT NULL)) AS condition " +
        "FROM {{input_view}} LEFT JOIN c.s.dim ON c.s.dim.id = {{customer_id}}",
    );
    // merge_columns must exist on the input table -> the join keys, NOT absent
    // (absent would route DQX to the dataset-level 1-row path and fail at run).
    expect(body.merge_columns).toEqual(["{{customer_id}}"]);
  });

  test("joins + group-by -> group-by wins as the merge key", () => {
    const joins: JoinAst[] = [
      { join_type: "INNER", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "cid" }] },
    ];
    const agg: AnyRow = {
      kind: "aggregated",
      combinator: null,
      aggregate: "sum",
      column_ref: "amount",
      operator: ">",
      value: 0,
    };
    const body = compileLowcodeBody(ast([agg], joins), "{{region}}");
    expect(body.merge_columns).toEqual(["{{region}}"]);
    expect(body.sql_query).toContain("INNER JOIN c.s.dim ON c.s.dim.id = {{cid}}");
    expect(body.sql_query).toContain("GROUP BY {{region}}");
  });

  test("expression group-by (legacy raw string) splits at top-level commas only — no corruption", () => {
    const body = compileLowcodeBody(ast([row()]), "{{region}}, COALESCE({{country}}, 'XX')");
    // The COALESCE's inner comma must NOT split into invalid fragments.
    expect(body.merge_columns).toEqual(["{{region}}", "COALESCE({{country}}, 'XX')"]);
    expect(body.sql_query).toContain("GROUP BY {{region}}, COALESCE({{country}}, 'XX')");
  });

  test("CROSS-join-only (no keys, no group-by) -> dataset-level query, no merge_columns", () => {
    const body = compileLowcodeBody(ast([row()], [{ join_type: "CROSS", target_table: "c.s.dim", keys: [] }]), "");
    expect(body.sql_query).toBe("SELECT (NOT ({{email}} IS NOT NULL)) AS condition FROM {{input_view}} CROSS JOIN c.s.dim");
    expect(body.merge_columns).toBeUndefined();
  });
});

describe("pruneStaleGroupByRefs", () => {
  const col = (name: string): LowcodeColumnRef => ({ name, family: "textual" as LowcodeColumnRef["family"] });
  const declared: LowcodeColumnRef[] = [col("region"), col("country")];

  test("nothing stale -> returns the same value unchanged", () => {
    const value = "{{region}}, {{country}}";
    expect(pruneStaleGroupByRefs(value, declared)).toBe(value);
  });

  test("one stale token (column removed from Columns Used) is pruned", () => {
    // {{removed}} is no longer a declared column — e.g. dropped from "Columns Used"
    // after being used as a group-by key.
    expect(pruneStaleGroupByRefs("{{region}}, {{removed}}", declared)).toBe("{{region}}");
  });

  test("all tokens stale -> prunes to empty string", () => {
    expect(pruneStaleGroupByRefs("{{removed1}}, {{removed2}}", declared)).toBe("");
  });

  test("qualified (dotted) join-key refs are treated as-is, not slot-wrapped", () => {
    const withJoinKey: LowcodeColumnRef[] = [...declared, col("orders.total")];
    const value = "{{region}}, orders.total";
    expect(pruneStaleGroupByRefs(value, withJoinKey)).toBe(value);
  });
});
