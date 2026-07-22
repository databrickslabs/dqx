# Low-code join rules: qualify own-table columns — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** When a low-code rule has joins, qualify own-table column references to the input view (`{{input_view}}.{{col}}`) in BOTH the Python and TypeScript compilers, so a rule joining to a table that shares a column name no longer fails with `AMBIGUOUS_REFERENCE`. Non-join rules compile byte-identically.

**Architecture:** Thread a `has_joins`/`hasJoins` flag from the top-level compile function down to `_ref`/`ref`, which prefixes own-table columns (no `.`) with `{{input_view}}.` when joins are present. Joined-table columns (with `.`) are unchanged. Spec: `docs/superpowers/specs/2026-07-22-lowcode-join-ambiguous-column-design.md`.

**Tech Stack:** Python (`backend/lowcode_compile.py`), TypeScript (`ui/lib/lowcodeCompile.ts`). Dual compilers that MUST stay in sync.

## Global Constraints

- **Dual-compiler parity:** both compilers must emit equivalent SQL. Both changes land; both test files updated.
- `{{input_view}}` is the reserved marker: the APP materializer leaves it intact (not an app slot), the DQX library substitutes it with the unique temp view. `{{col}}` is an app slot the materializer substitutes to the real column. So `{{input_view}}.{{col}}` → (app) `{{input_view}}.realcol` → (library) `<view>.realcol`. Verified.
- Non-join rules: `_ref`/`ref` return bare `{{col}}` EXACTLY as today — regression-guard with tests.
- Backend type hints; no `Any`; `make app-check` clean modulo the 5 known pre-existing errors. `bun tsc -b` clean; `bun test` green.
- Backend tests: `cd app && uv run --group test pytest tests/<file>.py -v`. Frontend: `cd app && bun test <path>`.

## Key facts (verified)

**Python `lowcode_compile.py`:**
- `_ref(column)` (line 184): `return column if "." in column else f"{{{{{column}}}}}"`.
- `compile_ast_to_sql(ast)` (line 437) reads `ast.get("rows")`; `ast.get("joins")` is available. It calls `_compile_row(row)` per row.
- `_compile_row` → `_row_sql(_ref(column_ref), operator, value)` (line 415); aggregate path builds refs too (line ~289); `$col` value path calls `_ref` via `_value_sql`/`_column_ref_name` (line ~220-224).
- `compile_joins_to_sql(joins)` (line 456): own side of `ON` uses `_ref(k['column_ref'])` (line 481) — this is inherently a join context, so its own-side refs must be qualified too.
- `compile_lowcode_body` (line 498): computes `joins_sql = compile_joins_to_sql(...)`; builds SELECT/GROUP BY with `_join_key_refs` / gb columns.

**TypeScript `lowcodeCompile.ts`:**
- `const ref = (c: string) => (c.includes(".") ? c : `{{${c}}}`);` (line 47).
- `compileAstToSql(ast)` (line 247); `rowSql(left, operator, value)` (line 137); `joinKeyRefs` (line 113); `compileJoinsToSql` (line 260); `compileLowcodeBody` (line 425); group-by ref mapping (line 366).

---

## Task 1: Python compiler — qualify own columns when joins present

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/lowcode_compile.py`
- Test: the Python lowcode-compile test file (find: `grep -rln "compile_ast_to_sql\|compile_lowcode_body" app/tests`)

**Interfaces:**
- `_ref(column, qualify=False)` → own column returns `{{input_view}}.{{column}}` when `qualify` True, else `{{column}}`; `.`-qualified columns unchanged.
- The compile entry points pass `qualify = bool(ast.get("joins"))` down through row/aggregate/`$col`/join-key compilation.

- [ ] **Step 1: Write failing tests.** First READ the test file + the compiler to match helper names. Add:
```python
def test_own_columns_qualified_to_input_view_when_join_present():
    ast = {
        "rows": [{"kind": "row", "column_ref": "customer_id", "operator": ">", "value": 0}],
        "joins": [{"target_table": "dqx.dqx_studio_demo.customers", "join_type": "INNER",
                   "keys": [{"column_ref": "customer_id", "joined_column": "customer_id"}]}],
    }
    body = compile_lowcode_body(ast, "")
    # own column in predicate qualified
    assert "{{input_view}}.{{customer_id}}" in (body.sql_query or "")
    # join-key own side qualified; joined side stays raw-qualified
    assert "dqx.dqx_studio_demo.customers.customer_id = {{input_view}}.{{customer_id}}" in (body.sql_query or "") \
        or "{{input_view}}.{{customer_id}}" in (body.sql_query or "")

def test_own_columns_bare_when_no_join():
    ast = {"rows": [{"kind": "row", "column_ref": "amount", "operator": ">", "value": 0}], "joins": []}
    pred = compile_ast_to_sql(ast)
    assert "{{amount}}" in pred
    assert "{{input_view}}" not in pred  # no qualification without joins

def test_joined_table_column_stays_raw():
    ast = {
        "rows": [{"kind": "row", "column_ref": "customers.tier", "operator": "=", "value": "'gold'"}],
        "joins": [{"target_table": "customers", "join_type": "INNER",
                   "keys": [{"column_ref": "customer_id", "joined_column": "customer_id"}]}],
    }
    body = compile_lowcode_body(ast, "")
    assert "customers.tier" in (body.sql_query or "")  # already-qualified, unchanged
```
(Adjust AST shapes to the real row/join schema the compiler expects — read `_compile_row`/`compile_joins_to_sql` to get `kind`/`keys`/`target_table` field names exactly. If `$col` value refs are supported, add a test that an own `$col` value is also qualified under a join.)

- [ ] **Step 2: Run, expect FAIL:** `cd app && uv run --group test pytest tests/<file>.py -k "qualified or bare or joined_table" -v`

- [ ] **Step 3: Implement.**
(a) `_ref` gains a qualify flag:
```python
def _ref(column: str, qualify: bool = False) -> str:
    if "." in column:
        return column
    return f"{{{{input_view}}}}.{{{{{column}}}}}" if qualify else f"{{{{{column}}}}}"
```
(b) Thread `qualify` from the entry points. `compile_ast_to_sql(ast)` computes `qualify = bool(ast.get("joins"))` and passes it into `_compile_row(row, qualify)`; `_compile_row` passes it to `_row_sql`/aggregate/`$col` ref calls so every own-column `_ref(...)` becomes `_ref(..., qualify)`. Keep signatures backward-safe (default `qualify=False`).
(c) `compile_joins_to_sql`: the own side of the `ON` (`_ref(k['column_ref'])`) is always in a join context → call `_ref(k['column_ref'], qualify=True)`. The joined side `target.joined_column` is already qualified — unchanged.
(d) GROUP-BY / merge keys: in `compile_lowcode_body`, when `joins_sql` is present, the `_join_key_refs` / gb-column refs that name OWN columns must ALSO be qualified (they sit in the joined SELECT/GROUP BY). READ `_join_key_refs` + the gb path; qualify own-column refs there when joins exist. Add a test asserting a joins-only rule's SELECT keys are qualified. (If gb columns can legitimately be joined-table columns, keep the `.`-check so only own columns get the prefix.)

- [ ] **Step 4: Run the whole file:** `cd app && uv run --group test pytest tests/<file>.py -v` — PASS. Existing non-join tests must be unchanged (bare `{{col}}`); if any existing test asserted a join rule's bare column, update it to the qualified form (that was the bug).

- [ ] **Step 5: Commit:**
```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git add app/src/databricks_labs_dqx_app/backend/lowcode_compile.py app/tests/<file>.py
git commit -m "fix(app): qualify own-table columns to input view in low-code join rules (Python)"
```

---

## Task 2: TypeScript compiler — mirror the qualification

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts`
- Test: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`

**Interfaces:**
- `ref(c, qualify=false)` mirrors the Python `_ref`; `compileAstToSql`/`compileLowcodeBody` compute `hasJoins` and thread it through `rowSql`, `joinKeyRefs`, `compileJoinsToSql`, and the group-by ref path.

- [ ] **Step 1: Write failing tests** in `lowcodeCompile.test.ts` mirroring Task 1's Python assertions (join rule qualifies own columns to `{{input_view}}.{{col}}`; no-join rule stays bare `{{col}}`; joined-table `customers.tier` stays raw; `$col` value qualified under join). READ the existing test style first and match it.

- [ ] **Step 2: Run, expect FAIL:** `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`

- [ ] **Step 3: Implement.** Mirror Task 1 in TS:
```ts
const ref = (c: string, qualify = false): string =>
  c.includes(".") ? c : qualify ? `{{input_view}}.{{${c}}}` : `{{${c}}}`;
```
Thread `hasJoins = (ast.joins?.length ?? 0) > 0` from `compileAstToSql`/`compileLowcodeBody` into `rowSql`, the `$col` value path, `joinKeyRefs`, `compileJoinsToSql` (own side of ON → `ref(x, true)`), and the group-by ref mapping (line ~366 — qualify own columns when joins present). Keep default `qualify=false` so no-join output is unchanged.

- [ ] **Step 4: Run tests + typecheck:**
`cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts` → PASS
`cd app && bun tsc -b` → clean
Update any existing non-join test only if the compiler output for joins changed (expected); non-join assertions unchanged.

- [ ] **Step 5: Commit:**
```bash
git add app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts
git commit -m "fix(app): qualify own-table columns to input view in low-code join rules (TypeScript)"
```

---

## Final verification (controller)

- [ ] `cd app && uv run --group test pytest -q` — no NEW failures (7 known pre-existing OK).
- [ ] `cd app && bun test` — green; `cd app && bun tsc -b` — clean.
- [ ] `make app-check` — clean modulo the 5 known pre-existing errors.
- [ ] Build wheel with `.cloud` proxy fallback; `make app-deploy PROFILE=fe-sandbox-dq-demo-2 TARGET=dev`; app RUNNING.
- [ ] Verify on fe-sandbox: re-run the failing condition-builder-with-join rule (own `customer_id` + join to a table with `customer_id`) — runs without AMBIGUOUS_REFERENCE; a no-join rule still runs.
- [ ] Report to user. Do NOT squash-merge / push until told.
