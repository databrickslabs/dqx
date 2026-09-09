/**
 * Best-effort SQL -> low-code builder import: the inverse of
 * `lowcodeCompile.compileAstToSql`, used when the author hops from the SQL tab
 * to the Visual builder after hand-editing the query.
 *
 * The exact round trip (builder -> SQL -> builder, untouched) is served by the
 * dialog's AST cache and never reaches this module. What lands here is SQL a
 * human (or the AI) wrote, so a faithful parse is impossible in general: this
 * recognises the shapes the compiler itself emits plus their common hand-written
 * equivalents, and reports everything else VERBATIM as unmapped rather than
 * guessing. The caller shows what was dropped — an import that silently loses a
 * condition is worse than one that admits it.
 *
 * Deliberately NOT parsed (reported unmapped):
 *   • whole `SELECT` queries and JOIN clauses — the builder expresses those
 *     through its joins/group-by controls, not its row stack;
 *   • nested boolean groups — the row stack is flat, so `(a AND b) OR c` has no
 *     representation and must not be flattened into a different rule;
 *   • anything referencing a column the rule has not declared.
 *
 * Every operator the compiler can emit is covered by a round-trip test
 * (`sqlToLowcode.test.ts`): parse(compile(row)) must recompile to the same SQL,
 * which is what keeps this file honest as `rowSql` grows.
 */
import type { AggregatedRowAst, AnyRow, ColumnRefValue, Combinator, RowAst } from "./lowcodeAst";
import { splitTopLevelCommas, type LowcodeColumnRef } from "./lowcodeCompile";
import { operatorValidForFamily, VALIDITY_TYPES, type Family } from "./lowcodeOperators";
import { stripSqlLineComments } from "./sqlComments";

/** A recognised condition before its combinator is decided by its position.
 *  Spelled per-variant because `Omit` over a union keeps only shared keys. */
type ParsedRow = Omit<RowAst, "combinator"> | Omit<AggregatedRowAst, "combinator">;

export interface SqlImportResult {
  /** Conditions recognised, in source order. Empty when nothing mapped. */
  rows: AnyRow[];
  /** Fragments left behind, verbatim, for the caller to show the author. */
  unmapped: string[];
}

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";
// A column on the left of a condition: a declared slot (optionally qualified
// with the `{{input_view}}` marker the compiler adds when the rule joins) or a
// joined-table column. Sub-groups are non-capturing so callers can wrap the
// whole thing in one capture.
const LHS = `(?:\\{\\{\\s*input_view\\s*\\}\\}\\.)?\\{\\{\\s*${IDENT}\\s*\\}\\}|${IDENT}\\.${IDENT}`;
const SQL_TYPE_TO_VALIDITY = new Map(VALIDITY_TYPES.map((t) => [t.sqlType.toUpperCase(), t.value as string]));

// The three RLIKE patterns `rowSql` hardcodes for named validators. Matched
// literally so `{{a}} RLIKE '^[0-9]+$'` reopens as "contains only digits"
// rather than as a raw regex the author then has to recognise.
const DIGITS_PATTERN = "^[0-9]+$";
const UUID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
const IPV4_PATTERN = "^((25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\\.){3}(25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])$";

// SQL function name -> aggregate in the builder's vocabulary (the inverse of
// `AGG_SQL`). `COUNT(DISTINCT c)` is handled by the DISTINCT capture below.
const AGG_BY_SQL_NAME: Record<string, string> = {
  COUNT: "count",
  APPROX_COUNT_DISTINCT: "approx_count_distinct",
  SUM: "sum",
  AVG: "avg",
  MIN: "min",
  MAX: "max",
  STDDEV_POP: "stddev",
  STDDEV_SAMP: "stddev_samp",
  VAR_POP: "variance",
  VAR_SAMP: "var_samp",
  MEDIAN: "median",
  BOOL_AND: "bool_and",
  BOOL_OR: "bool_or",
  ANY_VALUE: "any_value",
  MODE: "mode",
};

const rx = (body: string): RegExp => new RegExp(`^${body}$`, "i");

/** Strip one or more layers of parentheses that wrap the WHOLE expression. */
function unwrapOuterParens(text: string): string {
  let out = text.trim();
  while (out.startsWith("(") && out.endsWith(")")) {
    let depth = 0;
    let inQuote = false;
    let wrapsAll = true;
    for (let i = 0; i < out.length; i++) {
      const ch = out[i];
      if (inQuote) {
        if (ch === "'" && out[i + 1] === "'") i++;
        else if (ch === "'") inQuote = false;
        continue;
      }
      if (ch === "'") inQuote = true;
      else if (ch === "(") depth++;
      else if (ch === ")") {
        depth--;
        // Closed the opening paren before the end: it wraps only a prefix.
        if (depth === 0 && i < out.length - 1) {
          wrapsAll = false;
          break;
        }
      }
    }
    if (!wrapsAll) break;
    out = out.slice(1, -1).trim();
  }
  return out;
}

const isWordStart = (text: string, i: number): boolean => i === 0 || !/[A-Za-z0-9_]/.test(text[i - 1]);

interface Fragment {
  combinator: Combinator | null;
  text: string;
}

/**
 * Split a boolean expression at TOP-LEVEL `AND` / `OR`, keeping each fragment's
 * leading combinator. Quote- and paren-aware, and `BETWEEN x AND y` consumes its
 * own `AND` so a range condition survives as one fragment.
 */
function splitFragments(text: string): Fragment[] {
  const out: Fragment[] = [];
  let depth = 0;
  let inQuote = false;
  let start = 0;
  let pendingBetween = false;
  let combinator: Combinator | null = null;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuote) {
      if (ch === "'" && text[i + 1] === "'") i++;
      else if (ch === "'") inQuote = false;
      continue;
    }
    if (ch === "'") {
      inQuote = true;
      continue;
    }
    if (ch === "(") {
      depth++;
      continue;
    }
    if (ch === ")") {
      depth = Math.max(0, depth - 1);
      continue;
    }
    if (depth > 0 || !isWordStart(text, i)) continue;
    const rest = text.slice(i);
    if (/^between\b/i.test(rest)) {
      pendingBetween = true;
      i += 6;
      continue;
    }
    if (/^and\b/i.test(rest)) {
      if (pendingBetween) {
        pendingBetween = false;
        i += 2;
        continue;
      }
      out.push({ combinator, text: text.slice(start, i) });
      combinator = "AND";
      i += 2;
      start = i + 1;
      continue;
    }
    if (/^or\b/i.test(rest)) {
      out.push({ combinator, text: text.slice(start, i) });
      combinator = "OR";
      i += 1;
      start = i + 1;
    }
  }
  out.push({ combinator, text: text.slice(start) });
  return out.map((f) => ({ ...f, text: f.text.trim() })).filter((f) => f.text.length > 0);
}

/** The builder's name for a column as written in SQL, or null if it isn't one. */
function parseColumnRef(text: string): string | null {
  const t = text.trim();
  const slot = rx(`(?:\\{\\{\\s*input_view\\s*\\}\\}\\.)?\\{\\{\\s*(${IDENT})\\s*\\}\\}`).exec(t);
  if (slot) return slot[1];
  return rx(`${IDENT}\\.${IDENT}`).test(t) ? t : null;
}

type Parsed<T> = { ok: true; value: T } | { ok: false };
const ok = <T>(value: T): Parsed<T> => ({ ok: true, value });
const fail: Parsed<never> = { ok: false };

/** A literal operand, or a `{ $col }` reference when the operand is a column. */
function parseValue(text: string): Parsed<unknown> {
  const t = text.trim();
  if (!t) return fail;
  if (t.startsWith("'") && t.endsWith("'") && t.length >= 2) {
    const inner = t.slice(1, -1);
    // A lone quote inside means the literal ended early — not a single literal.
    if (/(^|[^'])'(?!')/.test(inner)) return fail;
    return ok(inner.replaceAll("''", "'"));
  }
  if (/^-?\d+(\.\d+)?$/.test(t)) return ok(Number(t));
  if (/^true$/i.test(t)) return ok(true);
  if (/^false$/i.test(t)) return ok(false);
  if (/^null$/i.test(t)) return ok(null);
  const col = parseColumnRef(t);
  return col ? ok({ $col: col } satisfies ColumnRefValue) : fail;
}

/** The literal inside a single-quoted SQL string, or null. */
function stringLiteral(text: string): string | null {
  const parsed = parseValue(text);
  return parsed.ok && typeof parsed.value === "string" ? parsed.value : null;
}

interface Ctx {
  familyOf: (column: string) => Family | null;
}

/** Map a comparison symbol onto the vocabulary the column's family offers. */
function comparisonOperator(symbol: string, family: Family, value: unknown): string {
  const sym = symbol === "<>" ? "!=" : symbol;
  if (sym === "=" && family === "BOOLEAN" && typeof value === "boolean") return value ? "is true" : "is false";
  if (family === "TEXTUAL") {
    if (sym === "=") return "equals";
    if (sym === "!=") return "not equals";
  }
  if (family === "TEMPORAL") {
    if (sym === "<") return "before";
    if (sym === ">") return "after";
    if (sym === "<=") return "on or before";
    if (sym === ">=") return "on or after";
  }
  return sym;
}

/** Recognise one condition, or null when it has no builder representation. */
function parseCondition(fragment: string, ctx: Ctx): ParsedRow | null {
  const text = unwrapOuterParens(fragment);
  // A group with its own boolean operators can't be flattened into the row
  // stack without changing precedence — leave it to the SQL tab.
  if (splitFragments(text).length > 1) return null;

  const row = (column: string, operator: string, value: unknown): ParsedRow | null => {
    const family = ctx.familyOf(column);
    if (family === null) return null;
    if (!operatorValidForFamily(operator, family)) return null;
    return { kind: "row", column_ref: column, operator, value };
  };
  // Both sides of a self-referential form (`c = upper(c)`) must name the same
  // column, or it isn't that form at all.
  const sameColumn = (a: string, b: string): string | null => {
    const left = parseColumnRef(a);
    return left && left === parseColumnRef(b) ? left : null;
  };

  let m: RegExpExecArray | null;

  // Marker planted by `foldLuhnPairs` for the two-conjunct checksum form.
  if ((m = rx(`__luhn__\\((${LHS})\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column ? row(column, "passes luhn check", null) : null;
  }

  if ((m = rx(`TRY_CAST\\(\\s*(${LHS})\\s+AS\\s+(${IDENT})\\s*\\)\\s+IS\\s+(NOT\\s+)?NULL`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const type = SQL_TYPE_TO_VALIDITY.get(m[2].toUpperCase());
    if (column && type) return row(column, m[3] ? "is a valid" : "is not a valid", type);
    return null;
  }

  // --- length ------------------------------------------------------------
  if ((m = rx(`length\\(\\s*trim\\(\\s*(${LHS})\\s*\\)\\s*\\)\\s*(>|=)\\s*0`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column ? row(column, m[2] === ">" ? "is not empty" : "is empty", null) : null;
  }
  if ((m = rx(`length\\(\\s*(${LHS})\\s*\\)\\s+BETWEEN\\s+(.+?)\\s+AND\\s+(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const lo = parseValue(m[2]);
    const hi = parseValue(m[3]);
    if (column && lo.ok && hi.ok) return row(column, "length between", [lo.value, hi.value]);
    return null;
  }
  if ((m = rx(`length\\(\\s*(${LHS})\\s*\\)\\s*(=|>|<)\\s*(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = parseValue(m[3]);
    const operator = m[2] === "=" ? "has length" : m[2] === ">" ? "is longer than" : "is shorter than";
    if (column && value.ok) return row(column, operator, value.value);
    return null;
  }

  // --- regex / pattern ---------------------------------------------------
  if ((m = rx(`NOT\\s*\\(\\s*(${LHS})\\s+RLIKE\\s+(.+?)\\s*\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const pattern = stringLiteral(m[2]);
    if (column && pattern !== null) return row(column, "does not match regex", pattern);
    return null;
  }
  if ((m = rx(`(${LHS})\\s+RLIKE\\s+(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const pattern = stringLiteral(m[2]);
    if (!column || pattern === null) return null;
    if (pattern === DIGITS_PATTERN) return row(column, "contains only digits", null);
    if (pattern === UUID_PATTERN) return row(column, "is a valid uuid", null);
    if (pattern === IPV4_PATTERN) return row(column, "is a valid ipv4", null);
    return row(column, "matches regex", pattern);
  }
  // Spark string builtins (preferred compile form — %/_ stay literal).
  if ((m = rx(`NOT\\s+contains\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = stringLiteral(m[2]);
    if (column && value !== null) return row(column, "does not contain", value);
    return null;
  }
  if ((m = rx(`contains\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = stringLiteral(m[2]);
    if (column && value !== null) return row(column, "contains", value);
    return null;
  }
  if ((m = rx(`startswith\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = stringLiteral(m[2]);
    if (column && value !== null) return row(column, "starts with", value);
    return null;
  }
  if ((m = rx(`endswith\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = stringLiteral(m[2]);
    if (column && value !== null) return row(column, "ends with", value);
    return null;
  }
  // Legacy LIKE forms (older compiled SQL / hand-written predicates).
  if ((m = rx(`(${LHS})\\s+(NOT\\s+)?LIKE\\s+(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const pattern = stringLiteral(m[3]);
    if (!column || pattern === null) return null;
    const negated = !!m[2];
    const starts = pattern.startsWith("%");
    const ends = pattern.endsWith("%");
    if (starts && ends && pattern.length >= 2)
      return row(column, negated ? "does not contain" : "contains", pattern.slice(1, -1));
    // Only the symmetric `%…%` form has a negated counterpart in the builder.
    if (negated) return null;
    if (ends) return row(column, "starts with", pattern.slice(0, -1));
    if (starts) return row(column, "ends with", pattern.slice(1));
    return null;
  }

  // --- self-referential shapes ------------------------------------------
  if ((m = rx(`(${LHS})\\s*(=|!=)\\s*TRIM\\(\\s*(${LHS})\\s*\\)`).exec(text))) {
    const column = sameColumn(m[1], m[3]);
    const operator = m[2] === "=" ? "has no leading or trailing whitespace" : "has leading or trailing whitespace";
    return column ? row(column, operator, null) : null;
  }
  if ((m = rx(`(${LHS})\\s*=\\s*(upper|lower|round)\\(\\s*(${LHS})\\s*\\)`).exec(text))) {
    const column = sameColumn(m[1], m[3]);
    const fn = m[2].toLowerCase();
    const operator = fn === "upper" ? "is uppercase" : fn === "lower" ? "is lowercase" : "is a whole number";
    return column ? row(column, operator, null) : null;
  }
  if ((m = rx(`mod\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)\\s*=\\s*0`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = parseValue(m[2]);
    return column && value.ok ? row(column, "is a multiple of", value.value) : null;
  }

  // --- temporal ----------------------------------------------------------
  if ((m = rx(`to_date\\(\\s*(${LHS})\\s*\\)\\s*=\\s*current_date\\(\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column ? row(column, "is today", null) : null;
  }
  if ((m = rx(`(${LHS})\\s*(>|<)\\s*current_timestamp\\(\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column ? row(column, m[2] === ">" ? "is in the future" : "is in the past", null) : null;
  }
  if ((m = rx(`(${LHS})\\s*>=\\s*current_timestamp\\(\\)\\s*-\\s*INTERVAL\\s*'(\\d+)\\s+(${IDENT})'`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column
      ? row(column, "is in last", {
          number: Number(m[2]),
          unit: m[3].toLowerCase(),
        })
      : null;
  }

  // --- AI sentiment ------------------------------------------------------
  if ((m = rx(`ai_analyze_sentiment\\(\\s*(${LHS})\\s*\\)\\s*=\\s*'(positive|negative)'`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const operator = m[2].toLowerCase() === "positive" ? "has positive sentiment" : "has negative sentiment";
    return column ? row(column, operator, null) : null;
  }

  // --- aggregates --------------------------------------------------------
  const aggregated = parseAggregated(text, ctx);
  if (aggregated) return aggregated;

  // --- plain comparisons -------------------------------------------------
  if ((m = rx(`(${LHS})\\s+IS\\s+(NOT\\s+)?NULL`).exec(text))) {
    const column = parseColumnRef(m[1]);
    return column ? row(column, m[2] ? "is not null" : "is null", null) : null;
  }
  if ((m = rx(`(${LHS})\\s+(NOT\\s+)?IN\\s*\\((.*)\\)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    if (!column) return null;
    const entries = splitTopLevelCommas(m[3]).map(parseValue);
    if (entries.length === 0 || entries.some((e) => !e.ok)) return null;
    const values = entries.map((e) => (e as { ok: true; value: unknown }).value);
    return row(column, m[2] ? "not in" : "in", values);
  }
  if ((m = rx(`(${LHS})\\s+BETWEEN\\s+(.+?)\\s+AND\\s+(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const lo = parseValue(m[2]);
    const hi = parseValue(m[3]);
    if (column && lo.ok && hi.ok) return row(column, "between", [lo.value, hi.value]);
    return null;
  }
  if ((m = rx(`(${LHS})\\s*(=|!=|<>|<=|>=|<|>)\\s*(.+)`).exec(text))) {
    const column = parseColumnRef(m[1]);
    const value = parseValue(m[3]);
    if (!column || !value.ok) return null;
    const family = ctx.familyOf(column);
    if (family === null) return null;
    const operator = comparisonOperator(m[2], family, value.value);
    const noOperand = operator === "is true" || operator === "is false";
    return row(column, operator, noOperand ? null : value.value);
  }
  return null;
}

/** `COUNT({{c}}) > 10` and friends — the aggregated row shape. */
function parseAggregated(text: string, ctx: Ctx): ParsedRow | null {
  const CMP = "=|!=|<>|<=|>=|<|>";
  const aggRow = (
    aggregate: string,
    column: string,
    operator: string,
    value: unknown,
    aggregate_param?: number,
  ): ParsedRow | null => {
    if (ctx.familyOf(column) === null) return null;
    const sym = operator === "<>" ? "!=" : operator;
    return {
      kind: "aggregated",
      aggregate,
      column_ref: column,
      operator: sym,
      value,
      ...(aggregate_param === undefined ? {} : { aggregate_param }),
    };
  };

  let m: RegExpExecArray | null;
  // null_rate carries its own parenthesised expression, so it is matched before
  // the generic `FUNC(col)` shape below.
  if (
    (m = rx(
      `\\(\\s*SUM\\(\\s*CASE\\s+WHEN\\s+(${LHS})\\s+IS\\s+NULL\\s+THEN\\s+1\\s+ELSE\\s+0\\s+END\\s*\\)\\s*\\*\\s*1\\.0\\s*/\\s*NULLIF\\(\\s*COUNT\\(\\*\\)\\s*,\\s*0\\s*\\)\\s*\\)\\s*(${CMP})\\s*(.+)`,
    ).exec(text))
  ) {
    const column = parseColumnRef(m[1]);
    const value = parseValue(m[3]);
    return column && value.ok ? aggRow("null_rate", column, m[2], value.value) : null;
  }
  if ((m = rx(`(PERCENTILE|PERCENTILE_APPROX)\\(\\s*(${LHS})\\s*,\\s*(.+?)\\s*\\)\\s*(${CMP})\\s*(.+)`).exec(text))) {
    const column = parseColumnRef(m[2]);
    const quantile = Number(m[3]);
    const value = parseValue(m[5]);
    if (!column || !value.ok || Number.isNaN(quantile)) return null;
    return aggRow(m[1].toLowerCase(), column, m[4], value.value, quantile);
  }
  if ((m = rx(`(${IDENT})\\(\\s*(DISTINCT\\s+)?(${LHS})\\s*\\)\\s*(${CMP})\\s*(.+)`).exec(text))) {
    const fn = m[1].toUpperCase();
    const distinct = !!m[2];
    const aggregate = distinct ? (fn === "COUNT" ? "count_distinct" : null) : (AGG_BY_SQL_NAME[fn] ?? null);
    const column = parseColumnRef(m[3]);
    const value = parseValue(m[5]);
    if (!aggregate || !column || !value.ok) return null;
    return aggRow(aggregate, column, m[4], value.value);
  }
  return null;
}

// "passes luhn check" is the one operator the compiler emits as TWO conjuncts
// (a length guard AND the checksum), so it arrives here already split. Folding
// the pair back before matching keeps it a single row instead of two fragments
// the author is told were dropped.
const LUHN_DIGITS = `regexp_replace\\(\\s*(${LHS})\\s*,\\s*'\\[\\^0-9\\]'\\s*,\\s*''\\s*\\)`;
const LUHN_GUARD = rx(`length\\(\\s*${LUHN_DIGITS}\\s*\\)\\s*>\\s*0`);
const LUHN_CHECK = rx(`luhn_check\\(\\s*${LUHN_DIGITS}\\s*\\)`);

function foldLuhnPairs(fragments: Fragment[]): Fragment[] {
  const out: Fragment[] = [];
  for (let i = 0; i < fragments.length; i++) {
    const guard = LUHN_GUARD.exec(fragments[i].text);
    const next = fragments[i + 1];
    const check = next && next.combinator === "AND" ? LUHN_CHECK.exec(next.text) : null;
    if (guard && check && parseColumnRef(guard[1]) === parseColumnRef(check[1])) {
      out.push({
        combinator: fragments[i].combinator,
        text: `__luhn__(${guard[1]})`,
      });
      i++;
      continue;
    }
    out.push(fragments[i]);
  }
  return out;
}

/**
 * Parse a boolean SQL predicate into builder rows, reporting what could not be
 * represented. *columns* are the rule's declared slots (plus any joined-table
 * columns): a condition on anything else is left unmapped, since the builder
 * has no column to point the row at.
 */
export function parseSqlToLowcode(sql: string, columns: LowcodeColumnRef[]): SqlImportResult {
  const text = unwrapOuterParens(stripSqlLineComments(sql).trim());
  if (!text) return { rows: [], unmapped: [] };
  // A whole query or a JOIN belongs to the builder's other controls, not to its
  // row stack; reported as one unmapped block rather than shredded into pieces.
  if (/^select\b/i.test(text)) return { rows: [], unmapped: [text] };

  const byName = new Map(columns.map((c) => [c.name, c.family]));
  const ctx: Ctx = {
    // A joined-table column (`c.tier`) is legitimate even when the join lives on
    // the SQL side and hasn't been carried into the builder yet, so it resolves
    // as ANY rather than failing the whole condition.
    familyOf: (column) => byName.get(column) ?? (column.includes(".") ? "ANY" : null),
  };

  const rows: AnyRow[] = [];
  const unmapped: string[] = [];
  for (const fragment of foldLuhnPairs(splitFragments(text))) {
    const parsed = parseCondition(fragment.text, ctx);
    if (!parsed) {
      unmapped.push(fragment.text);
      continue;
    }
    // The first surviving row anchors the stack: a leading `OR` left over from a
    // dropped fragment would read as "OR" against nothing.
    rows.push({
      ...parsed,
      combinator: rows.length === 0 ? null : (fragment.combinator ?? "AND"),
    } as AnyRow);
  }
  return { rows, unmapped };
}
