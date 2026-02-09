# Updating DQX Agent Skills

This document describes how to reconcile and update the SKILL.md files (and their companion
examples) so they stay in sync with the actual source code. Treat the examples as tests: if
an example cannot run, the skill it references is wrong and must be fixed.

---

## Prerequisites

See `AGENTS.md` for Databricks connectivity setup (environment variables, venv activation).
Run that setup in every terminal session before executing any examples.

---

## Repository Layout

| Path | Purpose |
|------|---------|
| `src/databricks/labs/dqx/check_funcs.py` | Row-level and dataset-level check function definitions |
| `src/databricks/labs/dqx/geo/check_funcs.py` | Geospatial check function definitions |
| `src/databricks/labs/dqx/profiler/profiler.py` | `DQProfiler` class (profile, profile_table, profile_tables_for_patterns) |
| `src/databricks/labs/dqx/resources/skills/dqx/SKILL.md` | Top-level DQX skill |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/SKILL.md` | Authoring/applying checks skill |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/row-level/SKILL.md` | Row-level check functions reference |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/dataset-level/SKILL.md` | Dataset-level check functions reference |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/custom/SKILL.md` | Custom checks (SQL expressions, Python) |
| `src/databricks/labs/dqx/resources/skills/dqx/profiling/SKILL.md` | Data profiling skill |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/geospatial/SKILL.md` | Geospatial check functions reference |
| `src/databricks/labs/dqx/resources/skills/dqx/checks/examples/` | Runnable example scripts (01-25) |

---

## Procedure Overview

For each skill area below, perform **three passes**:

1. **Reconcile** -- diff the source code against the SKILL.md to find new, removed, or
   changed functions/parameters.
2. **Update SKILL.md** -- add/remove/edit the function tables, YAML examples, and runnable
   example references. Always quote file paths and line numbers in examples.
3. **Validate examples** -- run every example script that the SKILL.md references. If an
   example fails, fix the example *and* re-check the SKILL.md for consistency.

### Discovering Undocumented Rule Types

During reconciliation you may find **public check functions that have no corresponding
SKILL.md at all** (e.g. functions in a new module like `geo/check_funcs.py`, or a new
category of row/dataset rules added to `check_funcs.py` that falls outside every existing
skill). When this happens:

1. **Do not silently skip them.** Collect the full list of undocumented functions (name,
   source file, line number, parameters, and whether they are row-level or dataset-level).
2. **Ask the user** before proceeding: present the list and ask whether a new SKILL.md
   (and corresponding examples) should be created, or whether the functions should be folded
   into an existing skill.
3. If the user says to create a new skill, follow the same reconcile/update/validate pattern
   used for the existing skills: create the SKILL.md with parameter tables + YAML examples +
   runnable example scripts, add the new skill to the Sub-Skills Reference table in
   `dqx/SKILL.md`, and add its examples to the Runnable Examples table in `checks/SKILL.md`.
4. Add a corresponding entry to the **Summary Checklist** at the bottom of this document.

---

## 1. Row-Level Check Functions

### 1a. Reconcile

Compare the public functions decorated with `@register_rule("row")` (or returning a single
`Column`) in `src/databricks/labs/dqx/check_funcs.py` against the function tables in:

- `src/databricks/labs/dqx/resources/skills/dqx/checks/row-level/SKILL.md`

**How to reconcile:**

1. Extract all row-level functions from source. These are functions that:
   - Are decorated with `@register_rule("row")`, OR
   - Return `-> Column` (not `-> tuple[Column, Callable]`)
   - AND are public (no leading underscore)

   At the time of writing, the row-level functions defined in `check_funcs.py` are:

   | Function | Line | Category |
   |----------|------|----------|
   | `is_not_null_and_not_empty` | 120 | Null/Empty |
   | `is_not_empty` | 140 | Null/Empty |
   | `is_not_null` | 158 | Null/Empty |
   | `is_null` | 172 | Null/Empty |
   | `is_empty` | 188 | Null/Empty |
   | `is_null_or_empty` | 206 | Null/Empty |
   | `is_not_null_and_is_in_list` | 228 | List Membership |
   | `is_in_list` | 282 | List Membership |
   | `is_not_in_list` | 340 | List Membership |
   | `sql_expression` | 397 | SQL Expression |
   | `is_older_than_col2_for_n_days` | 439 | Date/Time |
   | `is_older_than_n_days` | 489 | Date/Time |
   | `is_not_in_future` | 540 | Date/Time |
   | `is_not_in_near_future` | 574 | Date/Time |
   | `is_equal_to` | 611 | Comparison |
   | `is_not_equal_to` | 672 | Comparison |
   | `is_not_less_than` | 734 | Comparison |
   | `is_not_greater_than` | 764 | Comparison |
   | `is_in_range` | 794 | Range |
   | `is_not_in_range` | 832 | Range |
   | `regex_match` | 870 | Pattern |
   | `is_not_null_and_not_empty_array` | 893 | Null/Empty |
   | `is_valid_date` | 910 | Date/Time |
   | `is_valid_timestamp` | 934 | Date/Time |
   | `is_valid_ipv4_address` | 962 | Network |
   | `is_ipv4_address_in_cidr` | 975 | Network |
   | `is_valid_ipv6_address` | 1026 | Network |
   | `is_ipv6_address_in_cidr` | 1057 | Network |
   | `is_data_fresh` | 1117 | Date/Time |
   | `is_valid_json` | 2176 | JSON |
   | `has_json_keys` | 2200 | JSON |
   | `has_valid_json_schema` | 2252 | JSON |

2. Compare this list against the tables in `row-level/SKILL.md` (lines 16-95).
3. For every function in source but **missing** from the skill: add it to the appropriate
   table section, including Required and Optional parameters (read the function signature
   and docstring).
4. For every function in the skill but **removed** from source: delete it.
5. For every function where parameters changed: update the table row.

### 1b. Update SKILL.md

- Edit `src/databricks/labs/dqx/resources/skills/dqx/checks/row-level/SKILL.md`.
- Update the Parameter Reference tables (lines 16-95 at the time of writing).
- Update the YAML examples section (lines 99-337) to cover any new functions.
- Update the Runnable Examples table (lines 339-352) if new example files were added.

### 1c. Validate Examples

Run each row-level example. Every example file must have a comment on line 1 pointing back
to the SKILL.md section it demonstrates, e.g.:

```
# based on checks from dqx/checks/row-level/SKILL.md:14-45
```

Run:

```bash
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/01_row_null_empty.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/02_row_list.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/03_row_comparison.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/04_row_range.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/05_row_regex.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/06_row_datetime.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/07_row_network.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/08_row_sql_expression.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/09_row_complex.py
```

If any example fails:
1. Fix the example script.
2. Re-read the SKILL.md section referenced in the example's line-1 comment.
3. If the SKILL.md is also wrong, fix it.
4. Ensure the line numbers in the example's comment still match the SKILL.md.
5. Re-run the example to confirm.

---

## 2. Dataset-Level Check Functions

### 2a. Reconcile

Compare the public functions that return `-> tuple[Column, Callable]` (or are decorated
with `@register_rule("dataset")`) in `src/databricks/labs/dqx/check_funcs.py` against:

- `src/databricks/labs/dqx/resources/skills/dqx/checks/dataset-level/SKILL.md`

At the time of writing, the dataset-level functions are:

| Function | Line |
|----------|------|
| `has_no_outliers` | 1159 |
| `is_unique` | 1238 |
| `foreign_key` | 1335 |
| `sql_query` | 1446 |
| `is_aggr_not_greater_than` | 1582 |
| `is_aggr_not_less_than` | 1627 |
| `is_aggr_equal` | 1672 |
| `is_aggr_not_equal` | 1723 |
| `compare_datasets` | 1774 |
| `is_data_fresh_per_time_window` | 1943 |
| `has_valid_schema` | 2058 |

Follow the same reconcile steps as 1a above.

### 2b. Update SKILL.md

- Edit `src/databricks/labs/dqx/resources/skills/dqx/checks/dataset-level/SKILL.md`.
- Update Parameter Reference tables (lines 16-73 at the time of writing).
- Update YAML examples section (lines 77-286).
- Update Runnable Examples table (lines 288-299).

### 2c. Validate Examples

```bash
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/10_dataset_unique.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/11_dataset_aggr.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/12_dataset_fk.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/13_dataset_compare.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/14_dataset_freshness.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/15_dataset_schema.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/16_dataset_sql_query.py
```

Apply the same fix loop as described in 1c.

---

## 3. Custom Checks (SQL Expressions, Python)

### 3a. Reconcile

The custom checks skill covers `sql_expression` and `sql_query` (already in check_funcs.py)
plus the patterns for user-defined Python row-level and dataset-level checks using
`@register_rule("row")` and `@register_rule("dataset")`.

Compare:
- `src/databricks/labs/dqx/resources/skills/dqx/checks/custom/SKILL.md`

against the `make_condition` helper (line 74 of `check_funcs.py`), the `register_rule`
decorator from `src/databricks/labs/dqx/rule.py`, and the `sql_expression` / `sql_query`
function signatures.

Check that:
1. The `make_condition` signature and usage in the skill matches source.
2. The `register_rule` decorator usage matches source.
3. The `sql_expression` and `sql_query` parameter tables match their function signatures.
4. The return type conventions (row: `Column`, dataset: `tuple[Column, Callable]`) are
   accurately described.

### 3b. Update SKILL.md

- Edit `src/databricks/labs/dqx/resources/skills/dqx/checks/custom/SKILL.md`.
- Update parameter tables (lines 8-13 at the time of writing).
- Update code examples if function signatures changed.
- Update Runnable Examples table (lines 185-192).

### 3c. Validate Examples

```bash
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/17_custom_sql_window.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/18_custom_python_row.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/19_custom_python_dataset.py
```

Apply the same fix loop as described in 1c.

---

## 4. Profiler

### 4a. Reconcile

Compare the public methods on the `DQProfiler` class in
`src/databricks/labs/dqx/profiler/profiler.py` against:

- `src/databricks/labs/dqx/resources/skills/dqx/profiling/SKILL.md`

At the time of writing, the relevant public methods are:

| Method | Line | Description |
|--------|------|-------------|
| `DQProfiler.__init__` | 51 | Constructor (workspace_client, spark, llm_model_config) |
| `DQProfiler.profile` | 103 | Profile a DataFrame (options, columns) |
| `DQProfiler.profile_table` | 139 | Profile a table by name |
| `DQProfiler.profile_tables_for_patterns` | 164 | Profile multiple tables by glob patterns |
| `DQProfiler.detect_primary_keys_with_llm` | 207 | LLM-based PK detection |

Also check the `DQProfile` dataclass (line 40) for any field changes, since the skill shows
how to interpret profiler output.

Check that:
1. Constructor parameters match the skill's examples.
2. The `profile()` method signature and `options` dict keys match what the skill documents.
3. The `profile_table()` and `profile_tables_for_patterns()` usage matches.
4. The `DQProfile` fields (`name`, `column`, `description`, `parameters`, `filter`) are
   accurately described.
5. Any new methods (e.g., `detect_primary_keys_with_llm`) are documented if user-facing.

### 4b. Update SKILL.md

- Edit `src/databricks/labs/dqx/resources/skills/dqx/profiling/SKILL.md`.
- Update code examples if method signatures or option names changed.
- Update Runnable Examples table (lines 112-117 at the time of writing).

### 4c. Validate Examples

```bash
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/20_profiling_dataframe.py
python src/databricks/labs/dqx/resources/skills/dqx/checks/examples/21_profiling_specific_columns.py
```

Apply the same fix loop as described in 1c.

---

## 5. Top-Level and Checks Authoring Skills

### 5a. Reconcile

These two skills are higher-level and reference the sub-skills above:

- `src/databricks/labs/dqx/resources/skills/dqx/SKILL.md` -- top-level overview
- `src/databricks/labs/dqx/resources/skills/dqx/checks/SKILL.md` -- authoring/applying

After updating the sub-skills (steps 1-4), review these for:

1. **Sub-Skills Reference table** in `dqx/SKILL.md` (lines 112-119): verify all sub-skill
   paths are correct and no new sub-skills need to be listed.
2. **Quick Start example** in `dqx/SKILL.md` (lines 19-49): verify `DQEngine` constructor
   and method signatures still match `src/databricks/labs/dqx/engine.py`.
3. **Check YAML Format** in `dqx/SKILL.md` (lines 51-66): verify the YAML schema is still
   accurate (criticality, name, filter, check, function, arguments, for_each_column).
4. **Python API Alternative** in `dqx/SKILL.md` (lines 121-135): verify `DQRowRule` and
   `DQDatasetRule` constructors match `src/databricks/labs/dqx/rule.py`.
5. **Applying Checks** in `checks/SKILL.md` (lines 8-32): verify `DQEngine` API.
6. **Reference DataFrames** in `checks/SKILL.md` (lines 79-114): verify `ref_dfs` and
   `ref_table` usage.
7. **Runnable Examples table** in `checks/SKILL.md` (lines 136-143): verify example file
   references are correct and complete.

### 5b. Update

Edit the two SKILL.md files as needed. No dedicated examples to run for these -- they are
validated transitively by the sub-skill examples.

---

## 6. Scan for Undocumented Rule Types

After completing steps 1-5, scan the codebase for public check functions that are **not
covered by any existing SKILL.md**. Known places to look:

- `src/databricks/labs/dqx/geo/check_funcs.py` -- geospatial checks (25 functions at the
  time of writing, starting at line 21). No dedicated SKILL.md exists yet.
- Any new `**/check_funcs.py` modules that may have been added since this document was
  last updated.
- New functions in `src/databricks/labs/dqx/check_funcs.py` that don't fit into the
  existing row-level, dataset-level, or custom skill categories.

For every group of undocumented functions found, follow the **Discovering Undocumented Rule
Types** procedure in the Procedure Overview above: collect the list, present it to the user,
and ask whether to create a new SKILL.md or fold them into an existing skill. **Do not skip
undocumented functions silently.**

---

## Example Comment Convention

Every example file **must** start with a comment referencing the SKILL.md section it
demonstrates, using relative paths and line numbers:

```python
# based on checks from dqx/checks/row-level/SKILL.md:14-45
```

This acts as a traceability link. When updating a SKILL.md, you **must** also update the
line numbers in any example that references the changed section.

**Verification step:** After updating a SKILL.md, grep all example files for references to
it and confirm the line numbers still point to the correct content:

```bash
rg "row-level/SKILL.md" src/databricks/labs/dqx/resources/skills/dqx/checks/examples/
rg "dataset-level/SKILL.md" src/databricks/labs/dqx/resources/skills/dqx/checks/examples/
rg "custom/SKILL.md" src/databricks/labs/dqx/resources/skills/dqx/checks/examples/
rg "profiling/SKILL.md" src/databricks/labs/dqx/resources/skills/dqx/checks/examples/
```

---

## Summary Checklist

- [ ] **Row-level:** Reconcile `check_funcs.py` (row functions) vs `checks/row-level/SKILL.md`
- [ ] **Row-level:** Update SKILL.md if needed
- [ ] **Row-level:** Run examples 01-09, fix any failures, update SKILL.md back if needed
- [ ] **Dataset-level:** Reconcile `check_funcs.py` (dataset functions) vs `checks/dataset-level/SKILL.md`
- [ ] **Dataset-level:** Update SKILL.md if needed
- [ ] **Dataset-level:** Run examples 10-16, fix any failures, update SKILL.md back if needed
- [ ] **Custom:** Reconcile `check_funcs.py` + `rule.py` vs `checks/custom/SKILL.md`
- [ ] **Custom:** Update SKILL.md if needed
- [ ] **Custom:** Run examples 17-19, fix any failures, update SKILL.md back if needed
- [ ] **Profiler:** Reconcile `profiler/profiler.py` vs `profiling/SKILL.md`
- [ ] **Profiler:** Update SKILL.md if needed
- [ ] **Profiler:** Run examples 20-21, fix any failures, update SKILL.md back if needed
- [ ] **Top-level:** Reconcile `dqx/SKILL.md` and `checks/SKILL.md` sub-skill refs
- [ ] **Top-level:** Update if needed
- [ ] **Geospatial:** Reconcile `geo/check_funcs.py` vs `checks/geospatial/SKILL.md`
- [ ] **Geospatial:** Update SKILL.md if needed
- [ ] **Geospatial:** Run examples 22-25, fix any failures, update SKILL.md back if needed
- [ ] **Undocumented rules:** Scan for functions not covered by any SKILL.md, present to user, create new skills if requested
- [ ] **Line numbers:** Verify all example comment line numbers match their SKILL.md sections
