/**
 * Low-code rule AST — the structured, re-editable representation of a
 * Rules-Registry rule authored in the visual Low-Code builder. Ported from
 * dqlake's `ui/lib/lowcodeAst.ts` (the `LowcodeAstV2` shape) verbatim, since
 * it is framework-agnostic.
 *
 * A lowcode registry rule stores BOTH this AST (under `body.lowcode_ast`, so
 * the builder can rehydrate the exact rows/joins the author built) AND the
 * compiled SQL (`body.predicate` for a simple row stack, or `body.sql_query`
 * + `body.merge_columns` when joins / a group-by are present) — see
 * `lowcodeCompile.ts`. The compiled SQL is what actually materializes and
 * runs, exactly like an sql-mode rule; the AST is display/edit-only.
 */
export type Combinator = "AND" | "OR";
export type RowKind = "row" | "aggregated";
export type JoinType = "INNER" | "LEFT" | "RIGHT" | "FULL" | "LEFT SEMI" | "LEFT ANTI" | "CROSS";

export interface RowAst {
  kind: "row";
  combinator: Combinator | null;
  column_ref: string;
  operator: string;
  value: unknown;
}

export interface AggregatedRowAst {
  kind: "aggregated";
  combinator: Combinator | null;
  aggregate: string;
  column_ref: string;
  operator: string;
  value: unknown;
  // Used by percentile / percentile_approx — the quantile (0..1) the
  // aggregate computes. Ignored by aggregates that don't take a param.
  aggregate_param?: number | null;
}

export type AnyRow = RowAst | AggregatedRowAst;

/**
 * A condition value that references another COLUMN instead of holding a
 * literal (item 42). `$col` is the referenced column's name — a declared slot
 * (plain name → `{{name}}` at compile time) or a joined-table column
 * (`table.col`, emitted raw). A value box holds EITHER a literal (bare scalar /
 * array / object, as before) OR one of these — never a mix. Additive: literal
 * values stay bare scalars, so existing ASTs are unchanged.
 */
export interface ColumnRefValue {
  $col: string;
}

/** True when a condition value (or a `between` bound / `in` entry) is a column
 *  reference rather than a literal. */
export function isColumnRef(v: unknown): v is ColumnRefValue {
  return (
    typeof v === "object" &&
    v !== null &&
    typeof (v as Record<string, unknown>).$col === "string" &&
    (v as ColumnRefValue).$col.length > 0
  );
}

export interface JoinKeyAst {
  joined_column: string;
  column_ref: string;
}

export interface JoinAst {
  join_type: JoinType;
  target_table: string;
  keys: JoinKeyAst[];
}

export interface LowcodeAstV2 {
  rows: AnyRow[];
  joins: JoinAst[];
}

export const EMPTY_LOWCODE_AST: LowcodeAstV2 = { rows: [], joins: [] };

export function isV2Ast(ast: unknown): ast is LowcodeAstV2 {
  if (!ast || typeof ast !== "object") return false;
  const o = ast as Record<string, unknown>;
  return Array.isArray(o.rows) && Array.isArray(o.joins);
}

export function hashAst(ast: LowcodeAstV2): string {
  return JSON.stringify(ast, Object.keys(ast).sort());
}
