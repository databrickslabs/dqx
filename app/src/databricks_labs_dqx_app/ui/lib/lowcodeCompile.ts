import { isColumnRef, type AnyRow, type JoinAst, type LowcodeAstV2 } from "./lowcodeAst";
import { VALIDITY_SQL_TYPE, type Family } from "./lowcodeOperators";
import { stripSqlLineComments } from "./sqlComments";
import type { RuleSlotFamily } from "@/lib/api";

// Client-side compilation of a low-code AST to SQL. Ported from dqlake's
// `ui/lib/lowcodeCompile.ts` (which itself mirrors the backend lowcode
// compiler rows.py + joins.py + compile_v2.py).
//
// Unlike dqlake — which kept the predicate, joins and group-by as SEPARATE
// fields consumed by its own runner — DQX's runner consumes ONE SQL string
// with sql_check semantics. `compileLowcodeBody` therefore FOLDS the row
// predicate, joins and group-by into a single body payload:
//
//   • no joins & no group-by  →  { predicate }        (sql_expression path)
//   • joins and/or group-by   →  { sql_query, merge_columns? }  (sql_query path)
//
// The compiled `NOT (<predicate>)` condition is what the materializer's
// existing sql-mode path runs; polarity is applied by `render_check`'s
// `negate` argument, exactly as for a hand-written sql-mode rule.

const AGG_SQL: Record<string, (col: string, param?: number | null) => string> = {
  count: (c) => `COUNT(${c})`,
  count_distinct: (c) => `COUNT(DISTINCT ${c})`,
  approx_count_distinct: (c) => `APPROX_COUNT_DISTINCT(${c})`,
  null_rate: (c) => `(SUM(CASE WHEN ${c} IS NULL THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))`,
  sum: (c) => `SUM(${c})`,
  avg: (c) => `AVG(${c})`,
  min: (c) => `MIN(${c})`,
  max: (c) => `MAX(${c})`,
  stddev: (c) => `STDDEV_POP(${c})`,
  stddev_samp: (c) => `STDDEV_SAMP(${c})`,
  variance: (c) => `VAR_POP(${c})`,
  var_samp: (c) => `VAR_SAMP(${c})`,
  median: (c) => `MEDIAN(${c})`,
  percentile: (c, p) => `PERCENTILE(${c}, ${p ?? 0.5})`,
  percentile_approx: (c, p) => `PERCENTILE_APPROX(${c}, ${p ?? 0.5})`,
  bool_and: (c) => `BOOL_AND(${c})`,
  bool_or: (c) => `BOOL_OR(${c})`,
  any_value: (c) => `ANY_VALUE(${c})`,
  mode: (c) => `MODE(${c})`,
};

// Qualified refs (containing a dot) name a joined-table column and are
// emitted as raw SQL. Plain refs name a declared slot and stay wrapped as
// `{{name}}` placeholders the materializer substitutes with the real column.
const ref = (c: string) => (c.includes(".") ? c : `{{${c}}}`);

function quote(v: unknown): string {
  if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
  if (typeof v === "number") return String(v);
  if (v === null || v === undefined) return "NULL";
  const s = String(v).replaceAll("'", "''");
  return `'${s}'`;
}

// A comparison RHS is EITHER a column reference (item 42 — emit ref(), so a
// plain name becomes {{name}} and a joined-table col emits raw) OR a literal
// (quote() as before). This is the only place the literal-vs-column decision
// is made for scalar operands.
function valueSql(value: unknown): string {
  return isColumnRef(value) ? ref(value.$col) : quote(value);
}

function quoteList(values: unknown[]): string {
  return values.map(quote).join(", ");
}

// Escape a value for embedding INSIDE a single-quoted SQL LIKE pattern
// (e.g. `'%<value>%'`). Only the quote needs doubling so a value like
// `O'Brien` can't terminate the literal early and break the SQL — matching
// how `quote()` escapes ordinary string literals.
function likeLiteral(value: unknown): string {
  return String(value).replaceAll("'", "''");
}

// Split a comma-separated group-by / column-ref string at TOP-LEVEL commas
// only — commas nested inside parentheses (e.g. `COALESCE({{c}}, 'XX')`) or
// inside single-quoted string literals are NOT split points. The structured
// GroupByField only ever emits clean single-token column refs, but a rule
// hydrated from a legacy raw group-by string (or a dqlake import) may still
// carry an expression; a naive `split(",")` would shred it into invalid
// merge-column fragments, so we parse structurally instead.
function splitTopLevelCommas(value: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let inQuote = false;
  let start = 0;
  for (let i = 0; i < value.length; i++) {
    const ch = value[i];
    if (inQuote) {
      // Doubled '' is an escaped quote inside the literal — stay in-quote.
      if (ch === "'" && value[i + 1] === "'") {
        i++;
        continue;
      }
      if (ch === "'") inQuote = false;
      continue;
    }
    if (ch === "'") inQuote = true;
    else if (ch === "(") depth++;
    else if (ch === ")") depth = Math.max(0, depth - 1);
    else if (ch === "," && depth === 0) {
      out.push(value.slice(start, i));
      start = i + 1;
    }
  }
  out.push(value.slice(start));
  return out.map((s) => s.trim()).filter(Boolean);
}

// The distinct input-side merge keys for a set of joins: the `{{column_ref}}`
// tokens each (non-CROSS) join equates against its target table. These are the
// only columns present on BOTH the joined result and the monitored input table,
// so they are what a joins-only row-level `sql_query` merges its per-row result
// back on (see `compileLowcodeBody`).
function joinKeyRefs(joins: JoinAst[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const j of joins ?? []) {
    if (j.join_type === "CROSS" || !j.target_table) continue;
    for (const k of j.keys ?? []) {
      if (!k.column_ref) continue;
      const token = ref(k.column_ref);
      if (seen.has(token)) continue;
      seen.add(token);
      out.push(token);
    }
  }
  return out;
}

function aggExpr(spec: { aggregate?: string; column_ref?: string; aggregate_param?: number | null }): string {
  const agg = spec.aggregate;
  const col = spec.column_ref;
  if (!agg || !(agg in AGG_SQL)) return "";
  if (!col) return "";
  return AGG_SQL[agg](ref(col), spec.aggregate_param);
}

function rowSql(left: string, operator: string, value: unknown): string {
  const op = operator;
  if (["=", "!=", "<", "<=", ">", ">="].includes(op)) return `${left} ${op} ${valueSql(value)}`;
  if (op === "equals") return `${left} = ${valueSql(value)}`;
  if (op === "not equals") return `${left} != ${valueSql(value)}`;
  if (op === "contains") return `${left} LIKE '%${likeLiteral(value)}%'`;
  if (op === "does not contain") return `${left} NOT LIKE '%${likeLiteral(value)}%'`;
  if (op === "starts with") return `${left} LIKE '${likeLiteral(value)}%'`;
  if (op === "ends with") return `${left} LIKE '%${likeLiteral(value)}'`;
  if (op === "matches regex") return `${left} RLIKE ${quote(value)}`;
  if (op === "between") {
    const [lo, hi] = Array.isArray(value) ? (value as unknown[]) : [null, null];
    return `${left} BETWEEN ${quote(lo)} AND ${quote(hi)}`;
  }
  if (op === "in") return `${left} IN (${quoteList((value as unknown[]) ?? [])})`;
  if (op === "not in") return `${left} NOT IN (${quoteList((value as unknown[]) ?? [])})`;
  if (op === "is null") return `${left} IS NULL`;
  if (op === "is not null") return `${left} IS NOT NULL`;
  if (op === "is true") return `${left} = TRUE`;
  if (op === "is false") return `${left} = FALSE`;
  if (op === "before") return `${left} < ${valueSql(value)}`;
  if (op === "after") return `${left} > ${valueSql(value)}`;
  if (op === "on or before") return `${left} <= ${valueSql(value)}`;
  if (op === "on or after") return `${left} >= ${valueSql(value)}`;
  if (op === "is in last") {
    const obj = (value && typeof value === "object" ? value : {}) as { number?: number; unit?: string };
    return `${left} >= current_timestamp() - INTERVAL '${obj.number ?? 0} ${obj.unit ?? "days"}'`;
  }
  if (op === "is a valid" || op === "is not a valid") {
    const asType = VALIDITY_SQL_TYPE[String(value)];
    if (!asType) return "";
    const nullCheck = op === "is a valid" ? "IS NOT NULL" : "IS NULL";
    return `TRY_CAST(${left} AS ${asType}) ${nullCheck}`;
  }
  if (op === "has leading or trailing whitespace") return `${left} != TRIM(${left})`;
  if (op === "has no leading or trailing whitespace") return `${left} = TRIM(${left})`;
  // --- Length ------------------------------------------------------------
  if (op === "has length") return `length(${left}) = ${quote(value)}`;
  if (op === "is longer than") return `length(${left}) > ${quote(value)}`;
  if (op === "is shorter than") return `length(${left}) < ${quote(value)}`;
  if (op === "length between") {
    const [lo, hi] = Array.isArray(value) ? (value as unknown[]) : [null, null];
    return `length(${left}) BETWEEN ${quote(lo)} AND ${quote(hi)}`;
  }
  if (op === "is not empty") return `length(trim(${left})) > 0`;
  if (op === "is empty") return `length(trim(${left})) = 0`;
  // --- Text pattern / format --------------------------------------------
  if (op === "does not match regex") return `NOT (${left} RLIKE ${quote(value)})`;
  if (op === "contains only digits") return `${left} RLIKE '^[0-9]+$'`;
  if (op === "is uppercase") return `${left} = upper(${left})`;
  if (op === "is lowercase") return `${left} = lower(${left})`;
  // The `\\.` in these TS literals emits a single backslash-dot (`\.`) into the
  // SQL — the literal-dot RLIKE escape. These patterns are hardcoded (not user
  // input), so they are NOT run through quote().
  if (op === "is a valid uuid")
    return `${left} RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'`;
  if (op === "is a valid ipv4")
    return `${left} RLIKE '^((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])$'`;
  // --- Numeric predicates -----------------------------------------------
  if (op === "is positive") return `${left} > 0`;
  if (op === "is negative") return `${left} < 0`;
  if (op === "is non-negative") return `${left} >= 0`;
  if (op === "is a whole number") return `${left} = round(${left})`;
  if (op === "is a multiple of") return `mod(${left}, ${quote(value)}) = 0`;
  // --- Temporal predicates ----------------------------------------------
  if (op === "is in the future") return `${left} > current_timestamp()`;
  if (op === "is in the past") return `${left} < current_timestamp()`;
  if (op === "is today") return `to_date(${left}) = current_date()`;
  // --- AI (Foundation Model) checks — per-row cost + latency ------------
  if (op === "has positive sentiment") return `ai_analyze_sentiment(${left}) = 'positive'`;
  if (op === "has negative sentiment") return `ai_analyze_sentiment(${left}) = 'negative'`;
  // --- Luhn checksum (credit cards / IMEI / national ids) ---------------
  // Databricks has a built-in luhn_check(numStr) -> BOOLEAN (DBR 13.3+). It
  // returns false for ANY non-digit character, so normalize formatted inputs
  // (spaces/dashes) first. The leading length guard is MANDATORY: an empty
  // digit string (all-non-digit or empty input) trivially passes Luhn, so
  // without it such rows would be wrongly marked valid.
  if (op === "passes luhn check") {
    const digits = `regexp_replace(${left}, '[^0-9]', '')`;
    return `length(${digits}) > 0 AND luhn_check(${digits})`;
  }
  return "";
}

function compileRow(row: AnyRow): string {
  if (row.kind === "row") {
    if (!row.column_ref) return "";
    return rowSql(ref(row.column_ref), row.operator, row.value);
  }
  const left = aggExpr(row);
  if (!left) return "";
  const op = row.operator;
  if (["=", "!=", "<", "<=", ">", ">="].includes(op)) {
    const right =
      row.value && typeof row.value === "object" && "aggregate" in (row.value as Record<string, unknown>)
        ? aggExpr(row.value as { aggregate?: string; column_ref?: string })
        : quote(row.value);
    return `${left} ${op} ${right}`;
  }
  if (op === "is null") return `${left} IS NULL`;
  if (op === "is not null") return `${left} IS NOT NULL`;
  if (op === "between") {
    const [lo, hi] = Array.isArray(row.value) ? (row.value as unknown[]) : [null, null];
    return `${left} BETWEEN ${quote(lo)} AND ${quote(hi)}`;
  }
  return "";
}

/** Compile the row stack into a single boolean WHERE/HAVING expression (the
 *  "pass" condition — IF this holds THEN the row passes, by default). */
export function compileAstToSql(ast: LowcodeAstV2): string {
  if (!ast.rows?.length) return "";
  const parts: string[] = [];
  ast.rows.forEach((row, i) => {
    const frag = compileRow(row);
    if (!frag) return;
    if (i === 0) parts.push(frag);
    else parts.push(`${row.combinator ?? "AND"} ${frag}`);
  });
  return parts.join(" ");
}

/** Compile the joins into a `LEFT JOIN … ON …` FROM-clause fragment. */
export function compileJoinsToSql(joins: JoinAst[]): string {
  if (!joins?.length) return "";
  const typeSql: Record<string, string> = {
    INNER: "INNER JOIN",
    LEFT: "LEFT JOIN",
    RIGHT: "RIGHT JOIN",
    FULL: "FULL OUTER JOIN",
    "LEFT SEMI": "LEFT SEMI JOIN",
    "LEFT ANTI": "LEFT ANTI JOIN",
    CROSS: "CROSS JOIN",
  };
  return joins
    .filter((j) => j.target_table && (j.join_type === "CROSS" || j.keys?.length))
    .map((j) => {
      const head = `${typeSql[j.join_type] ?? "INNER JOIN"} ${j.target_table}`;
      if (j.join_type === "CROSS") return head;
      const conds = j.keys
        .filter((k) => k.joined_column && k.column_ref)
        .map((k) => `${j.target_table}.${k.joined_column} = ${ref(k.column_ref)}`);
      return `${head} ON ${conds.join(" AND ")}`;
    })
    .join(" ");
}

/** Map a Rules-Registry slot family (lowercase) to the low-code builder's
 *  UPPERCASE Family vocabulary. */
export function slotFamilyToLowcode(family: RuleSlotFamily | string): Family {
  switch (family) {
    case "numeric":
      return "NUMERIC";
    case "text":
      return "TEXTUAL";
    case "temporal":
      return "TEMPORAL";
    case "boolean":
      return "BOOLEAN";
    default:
      return "ANY";
  }
}

/** A column available to the low-code builder — a declared `{{slot}}` (plain
 *  name) or a joined-table column (qualified `<table>.<col>`). */
export interface LowcodeColumnRef {
  name: string;
  family: Family;
}

function groupByTokenToRefName(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  const m = /^\{\{(.+?)\}\}$/.exec(trimmed);
  return m ? m[1] : trimmed;
}

/**
 * Drop any `{{slot}}` / `<table>.<col>` token from a comma-joined group-by
 * `value` that no longer names a declared column (e.g. the column was
 * removed from "Columns Used" after being picked as a group-by key). Parses
 * every raw token in `value` first — unlike the UI's rendering-only parser,
 * this must NOT pre-filter by `declaredColumns`, or a stale token is
 * silently dropped from consideration and never detected as stale.
 *
 * Returns `value` unchanged (same reference) when nothing is stale, so
 * callers can skip a state update and avoid an effect loop.
 */
export function pruneStaleGroupByRefs(value: string, declaredColumns: LowcodeColumnRef[]): string {
  const declaredSet = new Set(declaredColumns.map((c) => c.name));
  const refs = value.split(",").map(groupByTokenToRefName).filter(Boolean);
  const stillValid = refs.filter((ref) => declaredSet.has(ref));
  if (stillValid.length === refs.length) return value;
  return stillValid.map((name) => (name.includes(".") ? name : `{{${name}}}`)).join(", ");
}

export interface CompiledLowcodeBody {
  /** Simple row stack (no joins, no group-by): the sql_expression predicate. */
  predicate?: string;
  /** Advanced (joins and/or group-by): the full sql_query referencing `{{input_view}}`. */
  sql_query?: string;
  /** Group-by columns to join aggregate results back on (row-level check). */
  merge_columns?: string[];
}

/**
 * Fold the row predicate, joins and group-by into the single body payload the
 * materializer's existing sql-mode path consumes.
 *
 * Composition rule: the row stack compiles to the pass-condition `P`; the
 * emitted fail-condition is `NOT (P)`. Every folded form is ROW-LEVEL — DQX's
 * `sql_query` check joins the per-row result back onto the monitored table via
 * `merge_columns`, which MUST be columns that exist on the input DataFrame
 * (see the `sql_query` docstring / `quality_checks.mdx`).
 *
 *   • no joins & no group-by  →  `{ predicate: P }`  (sql_expression path)
 *   • group-by present        →  `SELECT <gb>, (NOT (P)) AS condition
 *                                  FROM {{input_view}} [<joins>] GROUP BY <gb>`
 *                                 with `merge_columns` = the group-by columns.
 *   • joins only (no gb)      →  `SELECT <keys>, (NOT (P)) AS condition
 *                                  FROM {{input_view}} <joins>` with
 *                                 `merge_columns` = the input-side join keys.
 *                                 DQX collapses join fan-out internally by
 *                                 grouping on `merge_columns` and taking the
 *                                 max condition (fail if any joined row
 *                                 violates), matching the canonical join
 *                                 example in `quality_checks.mdx`.
 *
 * Group-by / merge keys are always clean single-token column refs (the
 * structured GroupByField and the join-key pickers only ever emit
 * `{{slot}}` / `<table>.<col>` tokens), split at top-level commas so an
 * expression such as `COALESCE({{c}}, 'XX')` in a legacy raw group-by string
 * is never shredded into invalid fragments. Only the degenerate case with no
 * usable row key (e.g. a CROSS-join-only rule with no group-by) falls back to
 * the dataset-level single-row query. Polarity is NOT baked in — `render_check`'s
 * `negate` applies it, uniform with a hand-written sql-mode rule.
 */
/**
 * Whether a low-code rule folds joins and/or group-by into a dataset-level
 * `sql_query` (rather than a plain row predicate). Derived from
 * `compileLowcodeBody`'s own classification so the two can never drift.
 *
 * Only the row predicate is testable via the Rules Registry "Test" tab, so an
 * advanced rule would produce a MISLEADING verdict (it tests the row predicate
 * in isolation, ignoring the joins/grouping that define its real semantics).
 * The Test tab therefore hides its surface for such rules, and the run route
 * rejects them as belt-and-braces.
 */
export function lowcodeHasAdvancedShape(ast: LowcodeAstV2, groupBy: string): boolean {
  return compileLowcodeBody(ast, groupBy).sql_query !== undefined;
}

export function compileLowcodeBody(ast: LowcodeAstV2, groupBy: string): CompiledLowcodeBody {
  const predicate = compileAstToSql(ast);
  const joinsSql = compileJoinsToSql(ast.joins);
  const gbColumns = splitTopLevelCommas(groupBy ?? "");

  if (!joinsSql && gbColumns.length === 0) {
    return { predicate };
  }

  const failCond = `NOT (${predicate})`;
  const from = `{{input_view}}${joinsSql ? ` ${joinsSql}` : ""}`;

  if (gbColumns.length > 0) {
    const gbList = gbColumns.join(", ");
    return {
      sql_query: `SELECT ${gbList}, (${failCond}) AS condition FROM ${from} GROUP BY ${gbList}`,
      merge_columns: gbColumns,
    };
  }

  // Joins only: run row-level, merging the per-row result back on the
  // input-side join keys (the only columns present on the input table).
  const keyRefs = joinKeyRefs(ast.joins);
  if (keyRefs.length > 0) {
    return {
      sql_query: `SELECT ${keyRefs.join(", ")}, (${failCond}) AS condition FROM ${from}`,
      merge_columns: keyRefs,
    };
  }

  // No usable row key (e.g. CROSS-join-only) — dataset-level single-row query.
  return { sql_query: `SELECT (${failCond}) AS condition FROM ${from}` };
}

/**
 * Build the SQL-mode rule body (`{ predicate }` or `{ sql_query, merge_columns? }`)
 * from the SQL editor's raw predicate + declared joins.
 *
 * The body TYPE is derived from join presence (mirroring
 * {@link compileLowcodeBody}'s classification — no separate stored flag):
 *
 *   • joins declared     →  compile predicate + joins into a `sql_query`
 *                           (`merge_columns` = input-side join keys, or absent
 *                           for a CROSS-join-only dataset-level query);
 *   • no joins, but the  →  emit the CURRENT editor text as `sql_query`,
 *     editor holds a         preserving the loaded body's `merge_columns`
 *     loaded cross-table      (*sqlQueryPassthrough* non-null). This is the
 *     query                   CRIT-2 fix: joins are not round-trippable from a
 *                            raw `sql_query` string, so a cross-table rule
 *                            reopens with `sqlJoins = []` and its whole
 *                            `SELECT … FROM {{input_view}} … JOIN …` sitting in
 *                            the predicate editor. Without this branch that body
 *                            would be mis-emitted as `{ predicate: <full SELECT> }`,
 *                            flipping the rule into a broken `sql_expression`.
 *                            Using the CURRENT predicate text (not a frozen
 *                            snapshot) means editing the query — the literal
 *                            "edit + resave" case — keeps it a valid `sql_query`;
 *   • otherwise          →  a plain single-table `{ predicate }`.
 *
 * *sqlQueryPassthrough* is non-null exactly while the editor still holds a rule
 * loaded as a cross-table `sql_query` (see `loadedSqlQueryRef`). It carries the
 * loaded `merge_columns` to preserve (dropping them would flip the runtime from
 * a row-level merge to a dataset-level single-row query). The caller drops it
 * the moment the author changes the rule TYPE (the decision-point re-pick), so
 * an intentional conversion to another mode is honoured; re-declaring joins
 * takes the recompile branch above regardless.
 */
export function buildSqlBody(params: {
  sqlPredicate: string;
  sqlJoins: JoinAst[];
  sqlQueryPassthrough?: { merge_columns?: string[] } | null;
}): CompiledLowcodeBody {
  const { sqlPredicate, sqlJoins, sqlQueryPassthrough } = params;
  const joinsSql = compileJoinsToSql(sqlJoins);
  const pred = sqlPredicate.trim();
  if (joinsSql) {
    const failCond = `NOT (${pred})`;
    const from = `{{input_view}} ${joinsSql}`;
    const keyRefs = joinKeyRefs(sqlJoins);
    if (keyRefs.length > 0) {
      return {
        sql_query: `SELECT ${keyRefs.join(", ")}, (${failCond}) AS condition FROM ${from}`,
        merge_columns: keyRefs,
      };
    }
    // CROSS-join-only: dataset-level single-row query (no usable row key).
    return { sql_query: `SELECT (${failCond}) AS condition FROM ${from}` };
  }
  if (sqlQueryPassthrough && pred) {
    // The editor holds a loaded cross-table sql_query. Persist the CURRENT text
    // as sql_query (so edits are saved, not corrupted), preserving merge_columns.
    const body: CompiledLowcodeBody = { sql_query: pred };
    if (sqlQueryPassthrough.merge_columns && sqlQueryPassthrough.merge_columns.length > 0) {
      body.merge_columns = sqlQueryPassthrough.merge_columns;
    }
    return body;
  }
  // Item 55: a SQL rule authored with a JOIN (per the predicate placeholder's
  // "LEFT JOIN catalog.schema.table a ON a.column = {{region}}" hint) must
  // become a cross-table `sql_query` check, NOT a bare `sql_expression` — the
  // latter can't run a join. Detect it on the comment-stripped text so a `--`
  // note can't trip the scan, then:
  //   • a full `SELECT … FROM … JOIN …` is already a query → persist as-is;
  //   • a bare boolean predicate followed by JOIN clause(s) is wrapped into a
  //     dataset-level query `SELECT (NOT (<pred>)) AS condition FROM
  //     {{input_view}} <joins>` (same NOT-condition convention as the
  //     structured-joins branch above; no merge_columns — raw JOIN text has no
  //     pickable input-side key, so the check applies dataset-wide).
  const scan = stripSqlLineComments(pred).trim();
  if (/^select\b/i.test(scan)) {
    return { sql_query: pred };
  }
  const joinMatch = SQL_JOIN_RE.exec(scan);
  if (joinMatch) {
    const boolExpr = scan.slice(0, joinMatch.index).trim();
    const joinClause = scan.slice(joinMatch.index).trim();
    if (boolExpr) {
      return { sql_query: `SELECT (NOT (${boolExpr})) AS condition FROM {{input_view}} ${joinClause}` };
    }
  }
  return { predicate: pred };
}

/** Matches a JOIN clause introducer (typed forms + bare `JOIN`) as a whole
 *  word, used to detect that a SQL predicate is really a cross-table query
 *  (item 55). Bare `JOIN` is included since it's valid SQL (defaults to INNER);
 *  a plain boolean predicate practically never contains the standalone token. */
const SQL_JOIN_RE =
  /\b(?:inner|left(?:\s+(?:outer|semi|anti))?|right(?:\s+outer)?|full(?:\s+outer)?|cross)\s+join\b|\bjoin\b/i;
