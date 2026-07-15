# Underlying rule-name display + SQL-query column attribution — design

**Date:** 2026-07-15
**Branch:** `dqx/demo` (worktree `.claude/worktrees/demo`)

## Problem

Two tightly-related defects in results/Genie column attribution, both stemming
from the per-column check-name work (`28d52c35`) and the SQL-columns work
(`ed388f01`):

### A. Results/Genie show the dedup check name, not the rule name

A rule applied to N columns renders N checks whose **top-level `check["name"]`**
is suffixed with the column (`Column is not null (email_address)`) so that
attribution and per-column failure counts stay distinct (DQX keys both the
metrics observer and the attribution view on `check_name`). That suffix is
correct and must stay. But the **display** surfaces read `check_name`:

- Results "by rule": `_by_rule_rows` labels each group with
  `MAX/newest check_name` → shows `Column is not null (state)`.
- Genie by-rule / rules-applied / drift queries: `MAX(check_name) AS rule_name`
  → same suffixed name, and the fan-out inflates what the steward sees.

The grouping is already correct (by `registry_rule_id`), so it IS one rule —
only the **label** is wrong.

**Key fact (verified on live data):** every rendered check already carries the
ORIGINAL rule name in `user_metadata['name']` ("Column is not null"), frozen
as-of-run — `_build_user_metadata` copies the rule's reserved `name` tag, and
the suffix logic only touched top-level `check["name"]`, never
`user_metadata['name']`. So the base name is already in the data; the fix is to
DISPLAY from `user_metadata['name']` instead of `check_name`.

### B. `sql_query` (low-code group-by) checks fail validation

`ed388f01` added `arguments["columns"]` to BOTH the `sql_expression` and
`sql_query` render branches. `sql_expression` DECLARES a `columns` parameter, so
that is valid and useful (DQX uses it for validation/reporting/name-prefix).
But `sql_query` does NOT — and DQX's `ChecksValidator._validate_func_args`
rejects any argument absent from the function signature BEFORE the resolver's
signature-filter runs (my earlier assumption that it was silently dropped was
wrong). Result: every low-code group-by / cross-table `sql_query` check now
fails at run time:

> Unexpected argument 'columns' for function 'sql_query' … Expected arguments
> are: ['query', 'merge_columns', 'msg', 'name', 'negate', 'condition_column',
> 'input_placeholder', 'row_filter']

Reproduced by the user's "Unique value check" (`sql_query` group-by on `city`).

## Goals

1. Results "by rule" and Genie display the **underlying rule name**
   (`user_metadata['name']`), while attribution/counts keep using the distinct
   per-column `check_name`. Grouping stays by `registry_rule_id`. Genie keeps
   its `MAX(...)`/`COUNT(DISTINCT check_name)`/`col_set` rollup shape and gains
   instruction text so it explains the per-column fan-out under the one rule
   name.
2. `sql_query` checks never carry an unsupported `columns` argument (fix the
   validation error), while `sql_query`/low-code group-by rules STILL attribute
   to their columns in results/by-column.
3. No regression to `sql_expression` (keep its valid `arguments.columns`), to
   rule identity grouping, or to Genie's existing question set.

Non-goals: changing the metrics observer, the runner, or the per-column suffix
itself (it is correct). No change to how `dqx_native` checks attribute columns
(they already carry `arguments.column`/`columns`).

## Design

### Column attribution source of truth (fixes B, unifies A's data)

Introduce a validator-safe, mode-agnostic carrier for a check's mapped columns:
stamp them into `user_metadata` as a JSON-encoded array under a new reserved
key **`mapped_columns`** in `render_check`, for every mode. `user_metadata` is a
free-form `MAP<STRING,STRING>` the DQX validator never inspects, so it is safe
for `sql_query`. Then:

- **`sql_expression`**: KEEP `arguments["columns"]` (declared param, engine-useful).
- **`sql_query`**: REMOVE `arguments["columns"]` (validation error). Its columns
  ride in `user_metadata['mapped_columns']` instead.
- **`dqx_native`**: unchanged (`arguments.column`/`columns` already present); it
  also gets `user_metadata['mapped_columns']` for uniformity, harmlessly.

The attribution view resolves `columns` as
`COALESCE(arg_columns, array(arg_column), from_json(user_metadata['mapped_columns']))`
so every mode attributes correctly from one consistent place.

Rationale for choosing `user_metadata` over `merge_columns`: `merge_columns` is
only present on ROW-LEVEL `sql_query` rules (group-by/joins), is semantically
the join key (not necessarily every referenced column), and is absent on
dataset-level queries — so it is not a reliable column source. `user_metadata`
is uniform and validator-safe.

### Display the underlying rule name (fixes A)

`v_dq_check_attribution` / `v_dq_check_results` expose a new column
`rule_name = user_metadata['name']` alongside the existing `check_name`.

- **Results** (`dq_results_service.py`): `CheckResultRow` gains `rule_name`;
  `parse_check_rows` reads it. `_by_rule_rows` labels each group with the
  newest run's `rule_name`, falling back to `check_name` when the metadata name
  is absent (legacy/hand-authored auto-named checks). Grouping key unchanged.
- **Genie** (`genie_space_service.py`): the by-rule / rules-applied / drift
  CTEs select `MAX(\`rule_name\`) AS rule_name` (the metadata name) rather than
  `MAX(\`check_name\`)`; `COUNT(DISTINCT \`check_name\`)` and `col_set` stay so
  the fan-out/rollup is preserved. `TEXT_INSTRUCTIONS` gains a sentence: a rule
  applied to multiple columns fans out into one check per column that share the
  rule's name and `registry_rule_id`; report the rule by its name and treat the
  per-column checks as its rollup (`check_count` = columns covered).

### Views recreate at deploy

Both views are `CREATE OR REPLACE` at app startup (`_ensure_score_views`), so
redeploy applies the schema change to existing and new runs. The user will
reset + reseed, so historical compatibility is moot, but the fallback
(`COALESCE(... , check_name)`) keeps any pre-existing run label sensible.

## Testing

- **Materializer**: `sql_query` render has NO `arguments.columns` (regression
  guard for the validation error); `sql_expression` still has it;
  `user_metadata['mapped_columns']` is the JSON of the mapped columns for all
  modes; tableless no-slot query has empty/absent mapped_columns.
- **Attribution/score view SQL** (string-shape tests, as existing): assert the
  view selects `user_metadata['name'] AS rule_name` and the `columns` COALESCE
  includes the `from_json(user_metadata['mapped_columns'])` fallback.
- **Results service**: `_by_rule_rows` labels with `rule_name`, stays ONE group
  per `registry_rule_id` across N per-column checks; falls back to `check_name`
  when `rule_name` is None; by-column still explodes columns correctly.
- **Genie SQL**: the by-rule/applied/drift snippets select `rule_name` from the
  metadata name; instruction text includes the fan-out explanation.
- **DQX validator** (sanity, no code change): a `sql_query` check with a
  `columns` arg is rejected — documents WHY the fix is needed.

## Files

- `app/src/databricks_labs_dqx_app/backend/services/materializer.py`
  (`render_check`: mapped_columns → user_metadata; drop sql_query
  arguments.columns) + `test_materializer.py`
- `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py`
  (attribution + shaping views: `rule_name`, columns COALESCE) + its tests
- `app/src/databricks_labs_dqx_app/backend/services/dq_results_service.py`
  (`CheckResultRow.rule_name`, `_by_rule_rows` label) + `test_dq_results*.py`
- `app/src/databricks_labs_dqx_app/backend/services/genie_space_service.py`
  (rule_name from metadata; instruction text) + `test_genie*.py`

## Global constraints

- Do NOT change the metrics observer, the runner, the DQX library, or the
  per-column suffix (`28d52c35` stays).
- Keep Genie's grouping (`COALESCE(registry_rule_id, check_name)`), its
  `MAX(...)`/`COUNT(DISTINCT check_name)`/`col_set` aggregation shape, and its
  existing question set intact — only swap the DISPLAY name source and add
  instruction text.
- `user_metadata` is `MAP<STRING,STRING>` — `mapped_columns` must be a
  JSON-encoded string, parsed with `from_json(..., 'ARRAY<STRING>')` in SQL.
- All interpolated SQL identifiers/literals go through `sql_utils`
  (`escape_sql_string` / `quote_*`); no raw f-string user input.
- Every user-facing string stays English-only per project test-pass rules;
  Genie instruction text is backend prose, not i18n.
