import { describe, expect, test } from "bun:test";
import {
  buildSqlBody,
  compileAstToSql,
  compileJoinsToSql,
  compileLowcodeBody,
  lowcodeHasAdvancedShape,
  pruneStaleGroupByRefs,
} from "./lowcodeCompile";
import type { LowcodeColumnRef } from "./lowcodeCompile";
import type { AnyRow, JoinAst, LowcodeAstV2 } from "./lowcodeAst";
import { OPERATORS_BY_FAMILY } from "./lowcodeOperators";

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

  // Full operator catalogue ported from dqlake (item 3): every operator each
  // column family offers must compile to the exact SQL below. `left` is always
  // the `{{c}}` slot ref. A regression in any one arm shows up here rather than
  // only at run time.
  const OPERATOR_SQL: Array<[string, unknown, string]> = [
    // Comparison / equality (numeric, temporal, universal)
    ["=", 3, "{{c}} = 3"],
    ["!=", 3, "{{c}} != 3"],
    [">", 3, "{{c}} > 3"],
    [">=", 3, "{{c}} >= 3"],
    ["<", 3, "{{c}} < 3"],
    ["<=", 3, "{{c}} <= 3"],
    // Text
    ["equals", "x", "{{c}} = 'x'"],
    ["not equals", "x", "{{c}} != 'x'"],
    ["contains", "x", "{{c}} LIKE '%x%'"],
    ["does not contain", "x", "{{c}} NOT LIKE '%x%'"],
    ["starts with", "x", "{{c}} LIKE 'x%'"],
    ["ends with", "x", "{{c}} LIKE '%x'"],
    ["matches regex", "^a.*$", "{{c}} RLIKE '^a.*$'"],
    ["has leading or trailing whitespace", null, "{{c}} != TRIM({{c}})"],
    ["has no leading or trailing whitespace", null, "{{c}} = TRIM({{c}})"],
    ["is a valid", "int", "TRY_CAST({{c}} AS INT) IS NOT NULL"],
    ["is not a valid", "int", "TRY_CAST({{c}} AS INT) IS NULL"],
    // Range / set
    ["between", [1, 10], "{{c}} BETWEEN 1 AND 10"],
    ["in", ["a", "b"], "{{c}} IN ('a', 'b')"],
    ["not in", ["a", "b"], "{{c}} NOT IN ('a', 'b')"],
    // Universal null checks
    ["is null", null, "{{c}} IS NULL"],
    ["is not null", null, "{{c}} IS NOT NULL"],
    // Boolean
    ["is true", null, "{{c}} = TRUE"],
    ["is false", null, "{{c}} = FALSE"],
    // Temporal
    ["before", "2020-01-01", "{{c}} < '2020-01-01'"],
    ["after", "2020-01-01", "{{c}} > '2020-01-01'"],
    ["on or before", "2020-01-01", "{{c}} <= '2020-01-01'"],
    ["on or after", "2020-01-01", "{{c}} >= '2020-01-01'"],
    ["is in last", { number: 7, unit: "days" }, "{{c}} >= current_timestamp() - INTERVAL '7 days'"],
    // Length
    ["has length", 5, "length({{c}}) = 5"],
    ["is longer than", 3, "length({{c}}) > 3"],
    ["is shorter than", 8, "length({{c}}) < 8"],
    ["length between", [2, 4], "length({{c}}) BETWEEN 2 AND 4"],
    ["is not empty", null, "length(trim({{c}})) > 0"],
    ["is empty", null, "length(trim({{c}})) = 0"],
    // Text pattern / format
    ["does not match regex", "^a$", "NOT ({{c}} RLIKE '^a$')"],
    ["contains only digits", null, "{{c}} RLIKE '^[0-9]+$'"],
    ["is uppercase", null, "{{c}} = upper({{c}})"],
    ["is lowercase", null, "{{c}} = lower({{c}})"],
    [
      "is a valid uuid",
      null,
      "{{c}} RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'",
    ],
    [
      "is a valid ipv4",
      null,
      "{{c}} RLIKE '^((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])$'",
    ],
    // Numeric predicates
    ["is positive", null, "{{c}} > 0"],
    ["is negative", null, "{{c}} < 0"],
    ["is non-negative", null, "{{c}} >= 0"],
    ["is a whole number", null, "{{c}} = round({{c}})"],
    ["is a multiple of", 5, "mod({{c}}, 5) = 0"],
    // Temporal predicates
    ["is in the future", null, "{{c}} > current_timestamp()"],
    ["is in the past", null, "{{c}} < current_timestamp()"],
    ["is today", null, "to_date({{c}}) = current_date()"],
    // AI (Foundation Model)
    ["has positive sentiment", null, "ai_analyze_sentiment({{c}}) = 'positive'"],
    ["has negative sentiment", null, "ai_analyze_sentiment({{c}}) = 'negative'"],
  ];

  for (const [operator, value, expected] of OPERATOR_SQL) {
    test(`operator "${operator}" compiles to ${expected}`, () => {
      expect(compileAstToSql(ast([row({ column_ref: "c", operator, value })]))).toBe(expected);
    });
  }

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

  test("passes luhn check uses the built-in luhn_check with a normalized-digit length guard", () => {
    const sql = compileAstToSql(ast([row({ column_ref: "card", operator: "passes luhn check", value: null })]));
    // Non-digits are stripped (luhn_check returns false on any non-digit) and
    // the length guard rejects empty input (which would trivially pass Luhn).
    expect(sql).toBe("length(regexp_replace({{card}}, '[^0-9]', '')) > 0 AND luhn_check(regexp_replace({{card}}, '[^0-9]', ''))");
  });
});

// Locks the type-dependent catalogue ported from dqlake (item 3): each family
// offers only its family-appropriate operator set, and every operator listed
// has a compilation arm above. A drift here (added/removed/renamed operator)
// fails loudly rather than silently shipping an operator the compiler can't
// emit SQL for.
describe("OPERATORS_BY_FAMILY — ported dqlake catalogue", () => {
  test("each family exposes exactly its operator set", () => {
    expect(OPERATORS_BY_FAMILY.NUMERIC).toEqual([
      "between",
      "=",
      "!=",
      ">=",
      ">",
      "<=",
      "<",
      "in",
      "not in",
      "is positive",
      "is negative",
      "is non-negative",
      "is a whole number",
      "is a multiple of",
      "passes luhn check",
    ]);
    expect(OPERATORS_BY_FAMILY.TEXTUAL).toEqual([
      "equals",
      "not equals",
      "contains",
      "does not contain",
      "starts with",
      "ends with",
      "in",
      "not in",
      "matches regex",
      "does not match regex",
      "has length",
      "is longer than",
      "is shorter than",
      "length between",
      "is not empty",
      "is empty",
      "contains only digits",
      "is uppercase",
      "is lowercase",
      "is a valid uuid",
      "is a valid ipv4",
      "passes luhn check",
      "has leading or trailing whitespace",
      "has no leading or trailing whitespace",
      "is a valid",
      "is not a valid",
      "has positive sentiment",
      "has negative sentiment",
    ]);
    expect(OPERATORS_BY_FAMILY.TEMPORAL).toEqual([
      "on or after",
      "on or before",
      "after",
      "before",
      "between",
      "is in last",
      "is in the future",
      "is in the past",
      "is today",
      "=",
      "!=",
    ]);
    expect(OPERATORS_BY_FAMILY.BOOLEAN).toEqual(["is true", "is false"]);
    expect(OPERATORS_BY_FAMILY.ANY).toEqual([
      "is null",
      "is not null",
      "=",
      "!=",
      "in",
      "not in",
      "is not empty",
      "is empty",
    ]);
  });

  test("every catalogue operator compiles to non-empty SQL (no unhandled arm)", () => {
    const sampleValue = (op: string): unknown => {
      if (op === "between" || op === "length between") return [1, 2];
      if (op === "in" || op === "not in") return ["a"];
      if (op === "is in last") return { number: 1, unit: "days" };
      if (op === "is a valid" || op === "is not a valid") return "int";
      if (op === "has length" || op === "is longer than" || op === "is shorter than" || op === "is a multiple of")
        return 3;
      return "x";
    };
    const all = new Set(Object.values(OPERATORS_BY_FAMILY).flat());
    for (const op of all) {
      const sql = compileAstToSql(ast([row({ column_ref: "c", operator: op, value: sampleValue(op) })]));
      expect(sql.length, `operator "${op}" produced empty SQL`).toBeGreaterThan(0);
    }
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

describe("lowcodeHasAdvancedShape — Test-tab gating", () => {
  test("plain row predicate is NOT advanced (row-testable)", () => {
    expect(lowcodeHasAdvancedShape(ast([row()]), "")).toBe(false);
  });

  test("group-by makes the rule advanced (not row-testable)", () => {
    expect(lowcodeHasAdvancedShape(ast([row()]), "{{region}}")).toBe(true);
  });

  test("joins make the rule advanced (not row-testable)", () => {
    const joins: JoinAst[] = [
      { join_type: "LEFT", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "cid" }] },
    ];
    expect(lowcodeHasAdvancedShape(ast([row({ column_ref: "cid" })], joins), "")).toBe(true);
  });

  test("classification matches compileLowcodeBody's own sql_query shape", () => {
    const joins: JoinAst[] = [
      { join_type: "INNER", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "cid" }] },
    ];
    const advancedAst = ast([row({ column_ref: "cid" })], joins);
    expect(lowcodeHasAdvancedShape(advancedAst, "")).toBe(compileLowcodeBody(advancedAst, "").sql_query !== undefined);
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

describe("buildSqlBody — CRIT-2: cross-table sql_query round-trips without corruption", () => {
  const CROSS_TABLE_QUERY =
    "SELECT {{customer_id}}, (NOT ({{amount}} > 0)) AS condition " +
    "FROM {{input_view}} LEFT JOIN c.s.orders ON c.s.orders.cid = {{customer_id}}";

  test("loaded cross-table rule, unedited resave -> stays sql_query (current text)", () => {
    // No-edit resave reopens with sqlJoins=[] and the full SELECT in the editor.
    // It must be persisted as sql_query, NOT flipped into { predicate: <SELECT> }.
    const body = buildSqlBody({
      sqlPredicate: CROSS_TABLE_QUERY,
      sqlJoins: [],
      sqlQueryPassthrough: { merge_columns: ["{{customer_id}}"] },
    });
    expect(body.sql_query).toBe(CROSS_TABLE_QUERY);
    expect(body.predicate).toBeUndefined();
  });

  test("loaded cross-table rule, EDITED query text -> saves the edit AS sql_query (not sql_expression)", () => {
    // The literal edit+resave case: the author tweaks the loaded SELECT. As long
    // as the rule is still a loaded cross-table query, the edited text is saved
    // as sql_query — never downgraded to a broken sql_expression.
    const edited = CROSS_TABLE_QUERY.replace("> 0", "> 100");
    const body = buildSqlBody({
      sqlPredicate: edited,
      sqlJoins: [],
      sqlQueryPassthrough: { merge_columns: ["{{customer_id}}"] },
    });
    expect(body.sql_query).toBe(edited);
    expect(body.predicate).toBeUndefined();
    expect(body.merge_columns).toEqual(["{{customer_id}}"]);
  });

  test("passthrough preserves merge_columns (row-level merge, not dataset-level)", () => {
    const body = buildSqlBody({
      sqlPredicate: CROSS_TABLE_QUERY,
      sqlJoins: [],
      sqlQueryPassthrough: { merge_columns: ["{{customer_id}}"] },
    });
    expect(body.merge_columns).toEqual(["{{customer_id}}"]);
  });

  test("passthrough without merge_columns stays dataset-level (no merge_columns key)", () => {
    const q = "SELECT (NOT (COUNT(*) > 0)) AS condition FROM {{input_view}} CROSS JOIN c.s.dim";
    const body = buildSqlBody({ sqlPredicate: q, sqlJoins: [], sqlQueryPassthrough: {} });
    expect(body.sql_query).toBe(q);
    expect(body.merge_columns).toBeUndefined();
  });

  test("joins re-declared -> recompiles a fresh sql_query from predicate + joins (passthrough ignored)", () => {
    const joins: JoinAst[] = [
      { join_type: "LEFT", target_table: "c.s.dim", keys: [{ joined_column: "id", column_ref: "customer_id" }] },
    ];
    const body = buildSqlBody({
      sqlPredicate: "{{customer_id}} IS NOT NULL",
      sqlJoins: joins,
      sqlQueryPassthrough: { merge_columns: ["{{customer_id}}"] },
    });
    expect(body.sql_query).toBe(
      "SELECT {{customer_id}}, (NOT ({{customer_id}} IS NOT NULL)) AS condition " +
        "FROM {{input_view}} LEFT JOIN c.s.dim ON c.s.dim.id = {{customer_id}}",
    );
    expect(body.merge_columns).toEqual(["{{customer_id}}"]);
  });

  test("no joins, no passthrough -> plain single-table { predicate }", () => {
    const body = buildSqlBody({ sqlPredicate: "{{email}} IS NOT NULL", sqlJoins: [] });
    expect(body).toEqual({ predicate: "{{email}} IS NOT NULL" });
  });

  test("no joins, null passthrough (type changed away) -> falls back to { predicate }", () => {
    expect(buildSqlBody({ sqlPredicate: "x > 0", sqlJoins: [], sqlQueryPassthrough: null })).toEqual({
      predicate: "x > 0",
    });
  });

  // Item 55: a SQL predicate authored WITH a join must become a runnable
  // sql_query, not a bare predicate (which the backend renders as an
  // unrunnable sql_expression).
  test("bare boolean predicate + JOIN clause -> wraps into a dataset-level sql_query", () => {
    const body = buildSqlBody({
      sqlPredicate: "{{region}} IS NOT NULL\nLEFT JOIN c.s.dim a ON a.region = {{region}}",
      sqlJoins: [],
    });
    expect(body.sql_query).toBe(
      "SELECT (NOT ({{region}} IS NOT NULL)) AS condition FROM {{input_view}} LEFT JOIN c.s.dim a ON a.region = {{region}}",
    );
    expect(body.predicate).toBeUndefined();
  });

  test("a full SELECT … JOIN is persisted as sql_query as-is", () => {
    const q = "SELECT (NOT (x > 0)) AS condition FROM {{input_view}} INNER JOIN c.s.d d ON d.k = {{k}}";
    expect(buildSqlBody({ sqlPredicate: q, sqlJoins: [] })).toEqual({ sql_query: q });
  });

  test("bare 'JOIN' (defaults to INNER) is detected", () => {
    const body = buildSqlBody({
      sqlPredicate: "{{id}} IS NOT NULL JOIN c.s.d d ON d.id = {{id}}",
      sqlJoins: [],
    });
    expect(body.sql_query).toContain("FROM {{input_view}} JOIN c.s.d d ON d.id = {{id}}");
  });

  test("a plain predicate with no join stays a { predicate } (sql_expression)", () => {
    expect(buildSqlBody({ sqlPredicate: "{{amount}} > 0 AND {{amount}} < 1e9", sqlJoins: [] })).toEqual({
      predicate: "{{amount}} > 0 AND {{amount}} < 1e9",
    });
  });

  test("a '-- join ...' comment does NOT trip join detection", () => {
    expect(buildSqlBody({ sqlPredicate: "{{a}} > 0 -- consider a join here", sqlJoins: [] })).toEqual({
      predicate: "{{a}} > 0 -- consider a join here",
    });
  });

});

describe("item42: column-ref RHS in single-value operators", () => {
  test("item42: comparison RHS column-ref emits {{col}} not a quoted literal", () => {
    expect(
      compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: { $col: "credit_limit" } })])),
    ).toBe("{{amount}} >= {{credit_limit}}");
  });

  test("item42: equals with a column-ref RHS compiles to = {{col}}", () => {
    expect(
      compileAstToSql(ast([row({ column_ref: "a", operator: "equals", value: { $col: "b" } })])),
    ).toBe("{{a}} = {{b}}");
  });

  test("item42: temporal 'before' with a column-ref RHS compiles to < {{col}}", () => {
    expect(
      compileAstToSql(ast([row({ column_ref: "start_date", operator: "before", value: { $col: "end_date" } })])),
    ).toBe("{{start_date}} < {{end_date}}");
  });

  test("item42: a qualified (join) column RHS emits raw table.col", () => {
    expect(
      compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: { $col: "orders.total" } })])),
    ).toBe("{{amount}} >= orders.total");
  });

  test("item42: literal RHS is unchanged (regression)", () => {
    expect(compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: 100 })]))).toBe(
      "{{amount}} >= 100",
    );
  });
});
