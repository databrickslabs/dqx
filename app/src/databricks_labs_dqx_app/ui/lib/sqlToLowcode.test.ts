import { describe, expect, test } from "bun:test";
import { parseSqlToLowcode } from "./sqlToLowcode";
import { compileAstToSql, type LowcodeColumnRef } from "./lowcodeCompile";
import type { AnyRow } from "./lowcodeAst";
import { OPERATORS_BY_FAMILY, type Family } from "./lowcodeOperators";

// Tests for the best-effort SQL -> builder import. The core guarantee is not
// that a parse reproduces the author's exact AST — it is that whatever we DO
// map compiles back to the SQL it came from, so hopping to the builder and back
// can never silently change what the rule means. The round-trip suite below
// asserts exactly that for every operator the compiler can emit, so adding a
// case to `rowSql` without teaching the parser about it fails here.

const COLUMNS: LowcodeColumnRef[] = [
  { name: "amount", family: "NUMERIC" },
  { name: "email", family: "TEXTUAL" },
  { name: "created_at", family: "TEMPORAL" },
  { name: "is_active", family: "BOOLEAN" },
  { name: "anything", family: "ANY" },
  { name: "other_amount", family: "NUMERIC" },
];

const parse = (sql: string) => parseSqlToLowcode(sql, COLUMNS);

const row = (over: Partial<AnyRow>): AnyRow =>
  ({
    kind: "row",
    combinator: null,
    column_ref: "amount",
    operator: ">",
    value: 0,
    ...over,
  }) as AnyRow;

/** compile -> parse -> recompile must land on the same SQL. */
const roundTrips = (r: AnyRow): void => {
  const sql = compileAstToSql({ rows: [r], joins: [] });
  const parsed = parse(sql);
  expect({ op: r.operator, unmapped: parsed.unmapped }).toEqual({
    op: r.operator,
    unmapped: [],
  });
  expect(compileAstToSql({ rows: parsed.rows, joins: [] })).toBe(sql);
};

describe("parseSqlToLowcode — round trip through every operator", () => {
  // A representative value per operator, keyed by the operator name. Every
  // operator in OPERATORS_BY_FAMILY must appear here or in UNSUPPORTED, so the
  // catalog and the parser can't drift apart unnoticed.
  const VALUE_BY_OP: Record<string, { column: string; family: Family; value: unknown }> = {
    // ANY
    "is null": { column: "anything", family: "ANY", value: null },
    "is not null": { column: "anything", family: "ANY", value: null },
    "=": { column: "amount", family: "NUMERIC", value: 10 },
    "!=": { column: "amount", family: "NUMERIC", value: 10 },
    ">=": { column: "amount", family: "NUMERIC", value: 10 },
    ">": { column: "amount", family: "NUMERIC", value: 10 },
    "<=": { column: "amount", family: "NUMERIC", value: 10 },
    "<": { column: "amount", family: "NUMERIC", value: 10 },
    in: { column: "amount", family: "NUMERIC", value: [1, 2, 3] },
    "not in": { column: "amount", family: "NUMERIC", value: [1, 2, 3] },
    "is not empty": { column: "email", family: "TEXTUAL", value: null },
    "is empty": { column: "email", family: "TEXTUAL", value: null },
    // NUMERIC
    between: { column: "amount", family: "NUMERIC", value: [1, 5] },
    "is positive": { column: "amount", family: "NUMERIC", value: null },
    "is negative": { column: "amount", family: "NUMERIC", value: null },
    "is non-negative": { column: "amount", family: "NUMERIC", value: null },
    "is a whole number": { column: "amount", family: "NUMERIC", value: null },
    "is a multiple of": { column: "amount", family: "NUMERIC", value: 5 },
    "passes luhn check": { column: "email", family: "TEXTUAL", value: null },
    // TEXTUAL
    equals: { column: "email", family: "TEXTUAL", value: "a@b.com" },
    "not equals": { column: "email", family: "TEXTUAL", value: "a@b.com" },
    contains: { column: "email", family: "TEXTUAL", value: "@" },
    "does not contain": { column: "email", family: "TEXTUAL", value: "@" },
    "starts with": { column: "email", family: "TEXTUAL", value: "a" },
    "ends with": { column: "email", family: "TEXTUAL", value: ".com" },
    "matches regex": { column: "email", family: "TEXTUAL", value: "^[a-z]+$" },
    "does not match regex": {
      column: "email",
      family: "TEXTUAL",
      value: "^[a-z]+$",
    },
    "has length": { column: "email", family: "TEXTUAL", value: 8 },
    "is longer than": { column: "email", family: "TEXTUAL", value: 8 },
    "is shorter than": { column: "email", family: "TEXTUAL", value: 8 },
    "length between": { column: "email", family: "TEXTUAL", value: [3, 8] },
    "contains only digits": { column: "email", family: "TEXTUAL", value: null },
    "is uppercase": { column: "email", family: "TEXTUAL", value: null },
    "is lowercase": { column: "email", family: "TEXTUAL", value: null },
    "is a valid uuid": { column: "email", family: "TEXTUAL", value: null },
    "is a valid ipv4": { column: "email", family: "TEXTUAL", value: null },
    "has leading or trailing whitespace": {
      column: "email",
      family: "TEXTUAL",
      value: null,
    },
    "has no leading or trailing whitespace": {
      column: "email",
      family: "TEXTUAL",
      value: null,
    },
    "is a valid": { column: "email", family: "TEXTUAL", value: "int" },
    "is not a valid": { column: "email", family: "TEXTUAL", value: "int" },
    "has positive sentiment": {
      column: "email",
      family: "TEXTUAL",
      value: null,
    },
    "has negative sentiment": {
      column: "email",
      family: "TEXTUAL",
      value: null,
    },
    // TEMPORAL
    "on or after": {
      column: "created_at",
      family: "TEMPORAL",
      value: "2024-01-01",
    },
    "on or before": {
      column: "created_at",
      family: "TEMPORAL",
      value: "2024-01-01",
    },
    after: { column: "created_at", family: "TEMPORAL", value: "2024-01-01" },
    before: { column: "created_at", family: "TEMPORAL", value: "2024-01-01" },
    "is in last": {
      column: "created_at",
      family: "TEMPORAL",
      value: { number: 7, unit: "days" },
    },
    "is in the future": {
      column: "created_at",
      family: "TEMPORAL",
      value: null,
    },
    "is in the past": { column: "created_at", family: "TEMPORAL", value: null },
    "is today": { column: "created_at", family: "TEMPORAL", value: null },
    // BOOLEAN
    "is true": { column: "is_active", family: "BOOLEAN", value: null },
    "is false": { column: "is_active", family: "BOOLEAN", value: null },
  };

  const allOperators = Array.from(new Set(Object.values(OPERATORS_BY_FAMILY).flat()));

  test("every catalog operator has a round-trip case", () => {
    expect(allOperators.filter((op) => !(op in VALUE_BY_OP))).toEqual([]);
  });

  for (const op of Object.keys(VALUE_BY_OP)) {
    const spec = VALUE_BY_OP[op];
    test(`"${op}" survives compile -> parse -> compile`, () => {
      roundTrips(row({ column_ref: spec.column, operator: op, value: spec.value }));
    });
  }

  test("a temporal 'between' keeps its own AND out of the row split", () => {
    roundTrips(
      row({
        column_ref: "created_at",
        operator: "between",
        value: ["2024-01-01", "2024-12-31"],
      }),
    );
  });

  test("a column-vs-column comparison keeps the reference", () => {
    roundTrips(
      row({
        column_ref: "amount",
        operator: "<",
        value: { $col: "other_amount" },
      }),
    );
  });

  test("aggregated rows round-trip", () => {
    for (const aggregate of ["count", "count_distinct", "sum", "avg", "min", "max", "median", "null_rate"]) {
      roundTrips({
        kind: "aggregated",
        combinator: null,
        aggregate,
        column_ref: "amount",
        operator: ">",
        value: 3,
      });
    }
  });

  test("a percentile aggregate keeps its quantile", () => {
    roundTrips({
      kind: "aggregated",
      combinator: null,
      aggregate: "percentile",
      column_ref: "amount",
      operator: ">",
      value: 3,
      aggregate_param: 0.9,
    });
  });

  test("a multi-row stack round-trips with its combinators", () => {
    const rows: AnyRow[] = [
      row({ column_ref: "email", operator: "is not null", value: null }),
      row({
        combinator: "AND",
        column_ref: "amount",
        operator: "between",
        value: [1, 5],
      }),
      row({
        combinator: "OR",
        column_ref: "email",
        operator: "contains",
        value: "@",
      }),
    ];
    const sql = compileAstToSql({ rows, joins: [] });
    const parsed = parse(sql);
    expect(parsed.unmapped).toEqual([]);
    expect(compileAstToSql({ rows: parsed.rows, joins: [] })).toBe(sql);
  });
});

describe("parseSqlToLowcode — hand-written SQL", () => {
  test("maps a plain predicate a human would type", () => {
    const parsed = parse("{{amount}} > 0 AND {{email}} IS NOT NULL");
    expect(parsed.unmapped).toEqual([]);
    expect(parsed.rows).toEqual([
      {
        kind: "row",
        combinator: null,
        column_ref: "amount",
        operator: ">",
        value: 0,
      },
      {
        kind: "row",
        combinator: "AND",
        column_ref: "email",
        operator: "is not null",
        value: null,
      },
    ]);
  });

  test("tolerates the {{input_view}}-qualified form the compiler emits with joins", () => {
    const parsed = parse("{{input_view}}.{{amount}} > 0");
    expect(parsed.rows).toEqual([
      {
        kind: "row",
        combinator: null,
        column_ref: "amount",
        operator: ">",
        value: 0,
      },
    ]);
  });

  test("keeps what it understands and reports the rest verbatim", () => {
    const parsed = parse("{{amount}} > 0 AND weird_udf({{amount}}) = 3");
    expect(parsed.rows).toHaveLength(1);
    expect(parsed.unmapped).toEqual(["weird_udf({{amount}}) = 3"]);
  });

  test("the first surviving row never keeps a dangling combinator", () => {
    const parsed = parse("weird_udf({{amount}}) = 3 AND {{amount}} > 0");
    expect(parsed.rows).toEqual([
      {
        kind: "row",
        combinator: null,
        column_ref: "amount",
        operator: ">",
        value: 0,
      },
    ]);
    expect(parsed.unmapped).toEqual(["weird_udf({{amount}}) = 3"]);
  });

  test("a nested boolean group is left to SQL rather than flattened", () => {
    const parsed = parse("({{amount}} > 0 AND {{amount}} < 10) OR {{email}} IS NULL");
    expect(parsed.unmapped).toEqual(["({{amount}} > 0 AND {{amount}} < 10)"]);
    expect(parsed.rows).toHaveLength(1);
  });

  test("a whole SELECT is reported as one block, not shredded", () => {
    const sql = "SELECT ({{amount}} > 0) AS condition FROM {{input_view}}";
    expect(parse(sql)).toEqual({ rows: [], unmapped: [sql] });
  });

  test("a condition on an undeclared column is not invented into a row", () => {
    const parsed = parse("{{not_declared}} > 0");
    expect(parsed.rows).toEqual([]);
    expect(parsed.unmapped).toEqual(["{{not_declared}} > 0"]);
  });

  test("a joined-table column resolves even before the join reaches the builder", () => {
    const parsed = parse("c.tier = 'gold'");
    expect(parsed.rows).toEqual([
      {
        kind: "row",
        combinator: null,
        column_ref: "c.tier",
        operator: "=",
        value: "gold",
      },
    ]);
  });

  test("an operator the column's type does not offer is left unmapped", () => {
    const parsed = parse("{{amount}} LIKE '%x%'");
    expect(parsed.rows).toEqual([]);
    expect(parsed.unmapped).toEqual(["{{amount}} LIKE '%x%'"]);
  });

  test("comments are ignored", () => {
    const parsed = parse("-- explain the rule\n{{amount}} > 0");
    expect(parsed.rows).toHaveLength(1);
    expect(parsed.unmapped).toEqual([]);
  });

  test("empty SQL maps to nothing at all", () => {
    expect(parse("   ")).toEqual({ rows: [], unmapped: [] });
  });

  test("a quoted AND inside a literal is not a split point", () => {
    const parsed = parse("{{email}} = 'a AND b'");
    expect(parsed.rows).toEqual([
      {
        kind: "row",
        combinator: null,
        column_ref: "email",
        operator: "equals",
        value: "a AND b",
      },
    ]);
  });

  test("lowercase keywords parse the same as upper", () => {
    const parsed = parse("{{amount}} between 1 and 5 and {{email}} is not null");
    expect(parsed.unmapped).toEqual([]);
    expect(parsed.rows).toHaveLength(2);
  });
});
