# Low-code join rules: qualify own-table columns to remove AMBIGUOUS_REFERENCE — Design (v2, corrected)

**Date:** 2026-07-22
**Worktree:** `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4`
**Branch:** `dqx/bug-bash-v4`

## History / why v2

v1 of this fix qualified **all** own-column references to `{{input_view}}.{{col}}`,
including the SELECT-list keys, `merge_columns`, and group-by columns. It was
deployed and FAILED at runtime with:

```
[INVALID_IDENTIFIER] The unquoted identifier
{{input_view}}.customer_id_query_condition_violation_input_view_<hex> is invalid…
```

Root cause of the v1 failure, proven by simulating BOTH substitution layers:

- The APP materializer (`services/materializer.py::_substitute_text`) substitutes
  `{{col}}` slots in the query text AND in `merge_columns`, but it does NOT touch
  `{{input_view}}` (not an app slot).
- The DQX library's `sql_query` check (`check_funcs.py`) substitutes `{{input_view}}`
  in the **query text only** — it never rewrites `merge_columns`.
- So a `merge_columns` entry of `{{input_view}}.{{customer_id}}` becomes
  `{{input_view}}.customer_id` and is passed VERBATIM to Spark's
  `df.select(*merge_columns, …)` / `df.join(on=merge_columns)`. Spark treats the
  whole thing as one unquoted column identifier → `[INVALID_IDENTIFIER]`.

So `merge_columns` (and the SELECT-list keys that feed it, and group-by columns)
must remain BARE. Qualification is only valid inside the query TEXT, where
`{{input_view}}` is substituted.

## Problem (unchanged from v1)

A condition-builder (low-code) rule with a JOIN fails at run time with
`[AMBIGUOUS_REFERENCE] Reference 'customer_id' is ambiguous` when the rule's own
table AND the joined table both have a `customer_id` column, because the compiler
emits own-table columns as bare `{{customer_id}}` → unqualified `customer_id` in a
joined query.

## The compiled body (what `sql_query` mode produces)

For a join rule, `compile_lowcode_body` emits (row-level, keys form merge_columns):

```
sql_query    = SELECT <key_refs>, (NOT (<predicate>)) AS condition FROM {{input_view}} <JOINS>
merge_columns = <key_refs>
```

and for a group-by rule:

```
sql_query    = SELECT <gb_list>, (NOT (<predicate>)) AS condition FROM {{input_view}} <JOINS?> GROUP BY <gb_list>
merge_columns = <gb_columns>
```

Ambiguity can appear in THREE positions when a join is present: the predicate, the
join ON-clause own-side, and the SELECT-list key references. `merge_columns`,
`GROUP BY`, and the SELECT OUTPUT column NAMES must stay bare.

## Correct qualification rules (per position)

Let `IV = {{input_view}}`. When (and only when) the rule has ≥1 join:

1. **Predicate** (inside `NOT(...)`): own columns → `IV.{{col}}`. Lives only in the
   query text; `{{input_view}}` is substituted by the library; unambiguous. ✅
   (This is the position that actually causes the reported AMBIGUOUS_REFERENCE.)
2. **Join ON-clause own side** (`compile_joins_to_sql`): own side → `IV.{{col}}`.
   Query text only; correct. ✅
3. **SELECT-list key references** (`_join_key_refs` output used in the SELECT): must
   emit `IV.{{col}} AS {{col}}` — the SOURCE is qualified (unambiguous) but the
   OUTPUT column is aliased back to the bare name so it matches `merge_columns`. ✅
4. **`merge_columns`**: BARE `{{col}}` (→ substitutes to `customer_id`). Passed to
   `df.select`/`df.join(on=…)`; must be a bare column name. **Never `IV.`-prefixed.**
5. **GROUP BY list**: BARE `{{col}}` (grouping references the SELECT output columns).
   The SELECT for a group-by rule likewise emits `IV.{{col}} AS {{col}}` for own
   columns so the projected/grouped name is bare and unambiguous. (Confirm GROUP BY
   references the bare output name; if Spark requires the grouping expr to match the
   SELECT source, group by `IV.{{col}}` while the projection aliases — pin this in
   the plan against a real compile + a runtime check.)
6. Joined-table columns (already dotted, e.g. `customers.tier`) — unchanged everywhere.
7. No-join rules — unchanged everywhere (bare `{{col}}`, no `IV.`).

### Net: `merge_columns` stays bare; the SELECT list qualifies-source-and-aliases.

So `_join_key_refs` must return TWO forms: the SELECT projection (`IV.{{col}} AS {{col}}`
under join) and the merge-column list (bare `{{col}}`). Today it returns one list used
for both — the plan splits them.

## Why this is correct end-to-end (to verify with a simulation test)

For the repro (own `customer_id` + join to `customers.customer_id`), the fixed body:
```
sql_query    = SELECT {{input_view}}.{{customer_id}} AS {{customer_id}},
                      (NOT ({{input_view}}.{{customer_id}} > 0)) AS condition
               FROM {{input_view}} INNER JOIN dqx.dqx_studio_demo.customers
                 ON dqx.dqx_studio_demo.customers.customer_id = {{input_view}}.{{customer_id}}
merge_columns = ['{{customer_id}}']
```
After APP substitution (`{{customer_id}}`→`customer_id`) then DQX substitution
(`{{input_view}}`→`<view>`):
```
sql_query    = SELECT <view>.customer_id AS customer_id, (NOT (<view>.customer_id > 0)) AS condition
               FROM <view> INNER JOIN dqx.dqx_studio_demo.customers
                 ON dqx.dqx_studio_demo.customers.customer_id = <view>.customer_id
merge_columns = ['customer_id']   # bare, valid for .select / .join(on=)
```
No AMBIGUOUS_REFERENCE (all own refs qualified), no INVALID_IDENTIFIER
(merge_columns bare, SELECT output aliased to bare `customer_id`).

## Goal

Both compilers (`backend/lowcode_compile.py` + `ui/lib/lowcodeCompile.ts`), when a
rule has joins:
- qualify own columns in the predicate + join ON-clause own side;
- qualify-and-alias own columns in the SELECT projection (`IV.{{col}} AS {{col}}`);
- keep `merge_columns` and GROUP BY bare;
- leave joined-table columns and no-join rules byte-identical.

## Non-goals

- No materializer or DQX-library change.
- No change to non-join rules (byte-identical).
- No schema-aware collision detection (qualify all own columns under a join).

## Global Constraints

- **Dual-compiler parity:** Python + TS emit equivalent SQL + merge_columns.
- **The runtime is the real test.** A pure-string unit test is necessary but NOT
  sufficient (v1 passed its unit tests and still failed at runtime). The plan MUST
  include a substitution-simulation test that reproduces BOTH layers (app `{{col}}`
  subst on query+merge_columns, then DQX `{{input_view}}` subst on query only) and
  asserts the final `merge_columns` are bare and the final query has no residual
  `{{` and no ambiguous bare own-column. AND a live post-deploy verification runs the
  actual repro rule.
- Backend type hints; no `Any`; `make app-check` clean modulo 5 known pre-existing.
  `bun tsc -b` clean; `bun test` green.
- SQL safety: the body still passes `is_sql_query_safe` post-substitution; aliasing
  adds no injection surface (slot-substituted names only).

## Testing

- **Substitution simulation (the test v1 lacked):** for a join rule, apply the app
  `{{col}}` substitution to `sql_query` AND `merge_columns`, then the DQX
  `{{input_view}}` regex substitution to the query; assert: final `merge_columns`
  contain NO `{{`, NO `.`-qualified `input_view`, are bare column names; final query
  contains `<view>.customer_id` (qualified) and `AS customer_id`; no residual `{{`.
- Compiler unit tests (Python + TS): predicate + ON own-side qualified; SELECT keys
  `IV.{{col}} AS {{col}}`; `merge_columns` bare `{{col}}`; group-by bare; joined-table
  cols raw; no-join byte-identical.
- Regression: existing non-join tests unchanged.

## Deploy / verification

- Gates green; build wheel with `.cloud` proxy fallback; deploy to fe-sandbox.
- **Live verification (mandatory before hand-off):** re-run the actual failing
  condition-builder-with-join rule on fe-sandbox and confirm it completes with no
  AMBIGUOUS_REFERENCE and no INVALID_IDENTIFIER. A green unit suite is not enough —
  v1 taught us the failure only shows at runtime.
- Report for user verification BEFORE squash-merge.
