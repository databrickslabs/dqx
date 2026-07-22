# Low-code join rules: qualify own-table columns to remove AMBIGUOUS_REFERENCE — Design

**Date:** 2026-07-22
**Worktree:** `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4`
**Branch:** new worktree branch off `dqx-dqlake-integration` (created at execution time)

## Problem

A condition-builder (low-code) rule that declares a JOIN fails at run time with:

```
[AMBIGUOUS_REFERENCE] Reference `customer_id` is ambiguous, could be:
[`customer_id_query_condition_violation_input_view_<hex>`.`customer_id`,
 `dqx`.`dqx_studio_demo`.`customers`.`customer_id`]. SQLSTATE: 42704
```

Root cause (confirmed in code):

- The low-code compiler's `_ref(column)` (`backend/lowcode_compile.py:184`) compiles an
  OWN-table column to a bare `{{column}}` slot placeholder (a joined-table column,
  which contains a `.`, is emitted raw). The materializer's `_substitute_text`
  (`backend/services/materializer.py:217`) replaces `{{column}}` with the real,
  UNQUALIFIED column name (e.g. `customer_id`).
- `compile_lowcode_body` builds `... FROM {{input_view}} <JOIN ...>` and the DQX
  library's `sql_query` check (`check_funcs.py`) substitutes `{{input_view}}` with
  a unique temp-view name that becomes the effective alias
  (`customer_id_query_condition_violation_input_view_<hex>`).
- So the generated SQL is `SELECT ... FROM <input_view> JOIN customers ... WHERE customer_id ...`.
  When the JOINED table ALSO has a `customer_id` column, the bare `customer_id`
  reference is ambiguous → the error.

The frontend TS compiler (`ui/lib/lowcodeCompile.ts`) mirrors this Python compiler
and has the SAME bug (its `ref()` emits a bare `{{col}}`), so a rule authored/tested
in the UI compiles identically.

## Goal

When a low-code rule has ONE OR MORE joins, qualify every OWN-table column
reference to the input view so it is unambiguous, in BOTH compilers:

- `{{column}}` → `{{input_view}}.{{column}}` (join present)
- Joined-table columns (already qualified, e.g. `orders.total`) are unchanged.
- Rules with NO joins are byte-identical to today (no qualification added).

## Why `{{input_view}}.{{column}}` works (verified end-to-end)

`_substitute_text` replaces each `{{slot}}` independently by string replace. Given
`{{input_view}}.{{customer_id}}`:
1. The APP materializer replaces the column slot: `{{customer_id}}` → `customer_id`,
   leaving `{{input_view}}.customer_id`. (`{{input_view}}` is NOT an app slot, so it
   is left intact by the app.)
2. The DQX library's `sql_query` check replaces `{{input_view}}` → the unique
   temp-view name → `<input_view>.customer_id` — fully qualified, unambiguous.

The `{{input_view}}` marker is the reserved token both layers already understand
(`extract_slot_tokens` explicitly skips it; the library substitutes it). So we reuse
the existing mechanism — no new placeholder, no materializer change.

## Non-goals

- No change to rules WITHOUT joins (the vast majority) — they must compile
  byte-identically.
- No schema-awareness / collision detection (we qualify ALL own-table columns when
  a join is present, not just colliding ones — simpler and always-correct).
- No change to how joined-table columns (already `.`-qualified) are emitted, nor to
  the `{{input_view}}` FROM-clause, join-key refs, or group-by handling beyond the
  own-column qualification.
- No materializer or DQX-library change.

## Global Constraints

- **Dual-compiler parity:** `backend/lowcode_compile.py` (Python) and
  `ui/lib/lowcodeCompile.ts` (TypeScript) MUST produce equivalent SQL. Every change
  to one is mirrored in the other; both have unit tests that must assert the new
  qualified output.
- Backend type hints; no `Any`; `make app-check` clean modulo the 5 known
  pre-existing errors. Frontend `bun tsc -b` clean; `bun test` green.
- SQL safety unchanged — the compiled body still flows through `is_sql_query_safe`
  in the materializer; qualification adds only a `{{input_view}}.` prefix to an
  existing slot, introducing no new injection surface (column names are
  slot-substituted, not interpolated).
- **Values/behaviour unchanged for non-join rules** — regression-guard with tests.

## Approach

### The qualification decision point

Both compilers know, at compile time, whether the AST has joins (`ast.joins`).
Thread that fact into the row/predicate compilation so `_ref`/`ref` can qualify:

- **Python** (`lowcode_compile.py`): `compile_ast_to_sql` already reads the whole
  ast. Determine `has_joins = bool(ast.get("joins"))` once, and pass a
  `qualify_own_columns: bool` flag down through `_compile_row` → `_row_sql`/
  aggregate handling → `_ref`. When true, `_ref(column)` for an own-table column
  (no `.`) returns `{{input_view}}.{{column}}` instead of `{{column}}`. Joined-table
  columns (with `.`) are returned raw as today. The join-key `ON` conditions
  (`compile_joins_to_sql`) already qualify the joined side (`target.joined_column`)
  and use `_ref(column_ref)` for the own side — those own-side refs must ALSO be
  qualified (they live in a join context by definition), so route them through the
  same qualified `_ref`.
- **TypeScript** (`lowcodeCompile.ts`): mirror exactly — compute `hasJoins` from the
  AST, thread it into `rowSql`/`ref`, emit `{{input_view}}.{{col}}` for own columns
  when joins exist.

### Column-ref values (item 42) interaction

Item 42 lets a condition VALUE reference another column (`{"$col": "b"}` → `_ref("b")`).
Those own-column value refs must be qualified under the SAME rule (join present →
`{{input_view}}.{{b}}`), since they'd hit the identical ambiguity. Both compilers'
`$col` value path routes through the same qualified `_ref`, so it's covered by
threading the flag — verify with a test.

### What stays bare

- No-join rules: `_ref` returns `{{column}}` exactly as today.
- Joined-table columns (`orders.total`): returned raw in both cases.
- Group-by columns / merge columns: these are emitted for the SELECT/GROUP BY of the
  dataset query; confirm whether they need qualification too when a join is present
  (a group-by on an own column in a joined query is equally ambiguous). If so, apply
  the same qualification; if the group-by list is only ever own-table columns and the
  SELECT aliases them, qualify them consistently. The plan will pin this down against
  the actual `compile_lowcode_body` SELECT/GROUP BY shape and add a test.

## Testing

- Python (`tests/` for lowcode_compile): a rule WITH a join qualifies own columns
  (`{{input_view}}.{{customer_id}}`) in the predicate, join-key own side, and any
  `$col` value; a rule WITHOUT a join is byte-identical to today (bare `{{column}}`);
  joined-table columns (`orders.total`) stay raw.
- TypeScript (`lowcodeCompile.test.ts`): the mirrored assertions — same qualified
  output for join rules, unchanged output for non-join rules.
- End-to-end sanity (documented, not necessarily automated): the compiled body for
  the repro (own `customer_id` + join to `customers.customer_id`) now yields
  `... WHERE {{input_view}}.customer_id ...` → after substitution
  `<view>.customer_id` → no AMBIGUOUS_REFERENCE.
- Regression: existing lowcode_compile / lowcodeCompile tests pass unchanged for
  non-join rules.

## Deploy / verification

- `make app-check` clean (modulo known); `make app-test` + `bun test` green; `bun tsc -b` clean.
- Build wheel with `.cloud` pypi-proxy fallback; deploy to `fe-sandbox-dq-demo-2`.
- Verify on fe-sandbox: re-run the failing condition-builder-with-join rule (own
  `customer_id` + join to a `customer_id`-bearing table) — it runs without
  AMBIGUOUS_REFERENCE; a no-join rule still runs correctly.
- Report for user verification BEFORE squash-merge.
