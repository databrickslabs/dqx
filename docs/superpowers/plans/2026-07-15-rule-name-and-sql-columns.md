# Underlying rule-name display + sql_query column attribution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Results and Genie display the underlying rule name (not the suffixed per-column check name), and `sql_query` checks attribute their mapped columns to the results by-column breakdown without hitting DQX arg-validation.

**Architecture:** The materializer stamps each check's mapped columns into `user_metadata['mapped_columns']` (a JSON array string — validator-safe for all modes) and stops putting `columns` in `sql_query` arguments. The attribution view resolves `columns` from `arguments.columns` OR the metadata fallback, and exposes `rule_name = user_metadata['name']`. `rule_name` threads through the shaping + asof views. Results by-rule and Genie label from `rule_name` (fallback `check_name`); the per-column `check_name` suffix stays for attribution/counts.

**Tech Stack:** Python 3.12, FastAPI, Databricks SQL views (Spark SQL DDL as Python strings), pytest, bun test (frontend untouched here).

## Global Constraints

- Do NOT change the metrics observer, the runner, the DQX library, or the per-column check-name suffix (`_suffix_check_name_with_columns`, commit `28d52c35`) — it is correct and must stay.
- Keep Genie's grouping key `COALESCE(registry_rule_id, check_name)`, its `MAX(...)` / `COUNT(DISTINCT check_name)` / `col_set` aggregation shape, and its existing question set intact. Only swap the DISPLAY name source (`MAX(check_name)` → `MAX(rule_name)`) and add instruction text.
- `sql_query` MUST NOT carry a `columns` argument (DQX `ChecksValidator._validate_func_args` rejects unknown args). `sql_expression` KEEPS `arguments.columns` (declared param).
- `user_metadata` is `MAP<STRING,STRING>`; `mapped_columns` is a JSON-encoded array string, parsed in SQL with `from_json(..., 'ARRAY<STRING>')`.
- Column attribution MUST reach `v_dq_check_results.columns` for every mode (the load-bearing requirement) — verified by an explicit end-to-end task.
- All interpolated SQL goes through existing helpers; no new raw user-input interpolation.
- `RESERVED_NAME_KEY = "name"` and `get_reserved_tag`/`set_reserved_tag` live in `registry_models.py`. Add `RESERVED_MAPPED_COLUMNS_KEY = "mapped_columns"` there.

---

### Task 1: Reserve the `mapped_columns` metadata key

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/registry_models.py` (near line 424, the `RESERVED_*_KEY` block)
- Test: `app/tests/test_registry_models.py`

**Interfaces:**
- Produces: `RESERVED_MAPPED_COLUMNS_KEY = "mapped_columns"` — the `user_metadata` key holding a check's mapped columns as a JSON array string. Consumed by Task 2 (materializer stamps it) and Task 3 (attribution view reads it).

- [ ] **Step 1: Add the constant**

In `registry_models.py`, after `RESERVED_SLOT_TAGS_KEY = "slot_tags"` (line 428):

```python
# The reserved user_metadata key holding a materialized check's mapped columns
# as a JSON-encoded array string (e.g. '["city"]'). Populated by the
# materializer for EVERY mode so the results attribution view can recover a
# check's columns uniformly — critically for sql_query, whose DQX check
# function rejects a `columns` argument, so its columns cannot live in
# `arguments`. Read in SQL via from_json(user_metadata['mapped_columns'],
# 'ARRAY<STRING>').
RESERVED_MAPPED_COLUMNS_KEY = "mapped_columns"
```

- [ ] **Step 2: Write the test**

In `test_registry_models.py`:

```python
def test_reserved_mapped_columns_key_value():
    from databricks_labs_dqx_app.backend.registry_models import RESERVED_MAPPED_COLUMNS_KEY

    assert RESERVED_MAPPED_COLUMNS_KEY == "mapped_columns"
```

- [ ] **Step 3: Run**

Run: `.venv/bin/python -m pytest tests/test_registry_models.py -q`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/registry_models.py app/tests/test_registry_models.py
git commit -m "Add RESERVED_MAPPED_COLUMNS_KEY for check column attribution"
```

---

### Task 2: Materializer — stamp mapped columns in user_metadata; drop sql_query arguments.columns

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/materializer.py` (`render_check`, lines ~205-366)
- Test: `app/tests/test_materializer.py`

**Interfaces:**
- Consumes: `RESERVED_MAPPED_COLUMNS_KEY` (Task 1), existing `_mapped_columns(group, slots)`.
- Produces: every rendered check's `user_metadata` carries `mapped_columns` (JSON array string) when the check maps ≥1 column; `sql_query` checks no longer have `arguments["columns"]`; `sql_expression` still does.

- [ ] **Step 1: Write failing tests**

In `test_materializer.py` (the `TestRenderCheckSqlMode` / lowcode classes). Replace the group-by assertion that currently expects `args["columns"] == ["region"]`:

```python
def test_group_by_sql_query_has_no_columns_arg_but_metadata_carries_them(self):
    # sql_query does NOT declare a `columns` parameter — DQX's validator rejects
    # any unknown arg, so `columns` must NOT be in arguments. The mapped columns
    # instead ride in user_metadata['mapped_columns'] (JSON), which the
    # attribution view reads so by-column still populates for sql_query rules.
    body = {
        "lowcode_ast": {"rows": [], "joins": []},
        "group_by": "{{column}}",
        "sql_query": "SELECT {{column}}, (NOT (COUNT(*) > 1)) AS condition FROM {{input_view}} GROUP BY {{column}}",
        "merge_columns": ["{{column}}"],
    }
    definition = self._lowcode_definition(body)
    version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
    check, _ = render_check(
        mode="lowcode",
        version=version,
        group={"column": "region"},
        effective_severity="High",
        per_application_tags={},
        registry_rule_id="r1",
        registry_version=1,
        applied_rule_id="ar1",
        app_settings=_app_settings_stub(),
    )
    assert check["check"]["function"] == "sql_query"
    assert "columns" not in check["check"]["arguments"]
    assert check["check"]["arguments"]["merge_columns"] == ["region"]
    import json as _json
    assert _json.loads(check["user_metadata"]["mapped_columns"]) == ["region"]


def test_sql_expression_keeps_columns_arg_and_metadata(self):
    # sql_expression DECLARES a columns param — keep arguments.columns AND also
    # stamp the metadata carrier (uniform across modes).
    definition = RuleDefinition.model_validate(
        {
            "body": {"predicate": "{{start}} <= {{end}}"},
            "slots": [
                {"name": "start", "family": "any", "position": 0, "cardinality": "one"},
                {"name": "end", "family": "any", "position": 1, "cardinality": "one"},
            ],
            "parameters": [],
        }
    )
    version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass", user_metadata={})
    check, _ = render_check(
        mode="sql",
        version=version,
        group={"start": "shipped_at", "end": "delivered_at"},
        effective_severity="Medium",
        per_application_tags={},
        registry_rule_id="r1",
        registry_version=1,
        applied_rule_id="ar1",
        app_settings=_app_settings_stub(),
    )
    import json as _json
    assert check["check"]["arguments"]["columns"] == ["shipped_at", "delivered_at"]
    assert _json.loads(check["user_metadata"]["mapped_columns"]) == ["shipped_at", "delivered_at"]


def test_dqx_native_stamps_mapped_columns_metadata(self):
    # dqx_native single-column check: arguments.column unchanged, metadata carrier added.
    version = RuleVersion(rule_id="r1", version=1, definition=_is_not_null_definition(), user_metadata={"name": "x"})
    check, _ = render_check(
        mode="dqx_native",
        version=version,
        group={"column": "customer_id"},
        effective_severity="High",
        per_application_tags={},
        registry_rule_id="r1",
        registry_version=1,
        applied_rule_id="ar1",
        app_settings=_app_settings_stub(),
    )
    import json as _json
    assert check["check"]["arguments"]["column"] == "customer_id"
    assert _json.loads(check["user_metadata"]["mapped_columns"]) == ["customer_id"]


def test_tableless_sql_query_no_mapped_columns_metadata(self):
    definition = RuleDefinition.model_validate(
        {"body": {"sql_query": "SELECT COUNT(*) > 100 AS condition FROM t"}, "slots": [], "parameters": []}
    )
    version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="fail", user_metadata={})
    check, is_tableless = render_check(
        mode="sql", version=version, group={}, effective_severity="Medium",
        per_application_tags={}, registry_rule_id="r1", registry_version=1,
        applied_rule_id="ar1", app_settings=_app_settings_stub(),
    )
    assert is_tableless is True
    assert "columns" not in check["check"]["arguments"]
    assert "mapped_columns" not in check["user_metadata"]
```

Delete/replace the old `test_group_by_renders_sql_query_with_substituted_merge_columns` assertion `assert args["columns"] == ["region"]` (it now asserts the opposite) — keep the rest of that test.

- [ ] **Step 2: Run to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_materializer.py -q -k "sql_query or sql_expression or mapped_columns or group_by"`
Expected: FAIL (columns still in sql_query args; no metadata key yet)

- [ ] **Step 3: Implement**

In `materializer.py`:

(a) Import the new key — add to the `registry_models` import block (near line 67):
```python
    RESERVED_MAPPED_COLUMNS_KEY,
```

(b) In the `sql_query` branch, REMOVE the `arguments["columns"]` block (current lines 293-305, the comment + `mapped_columns = _mapped_columns(...)` + `if mapped_columns: arguments["columns"] = mapped_columns`). Leave the `merge_columns` handling intact.

(c) In the `sql_expression` branch (current lines 331-337), KEEP `arguments["columns"]` as-is.

(d) After `check_dict` is built and `name`/`error_message` set (after line 362, before `return`), stamp the metadata carrier for ALL modes:
```python
    # Stamp the mapped columns into user_metadata as a JSON array so the results
    # attribution view can recover a check's columns uniformly across modes —
    # essential for sql_query, whose check function rejects a `columns` argument
    # (so its columns can't live in `check.arguments`). Only when non-empty, so a
    # tableless/no-column check's metadata is unchanged.
    mapped_columns = _mapped_columns(group, version.definition.slots)
    if mapped_columns:
        check_dict["user_metadata"][RESERVED_MAPPED_COLUMNS_KEY] = json.dumps(mapped_columns)
```
(`json` is already imported at top of file.)

- [ ] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_materializer.py -q`
Expected: PASS (all, including the updated group-by test)

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/materializer.py app/tests/test_materializer.py
git commit -m "Materializer: mapped columns in user_metadata; drop sql_query arguments.columns"
```

---

### Task 2b: DQX validator sanity guard (documents WHY sql_query drops columns)

**Files:**
- Test only: `app/tests/test_materializer.py`

- [ ] **Step 1: Add a guard test asserting DQX rejects columns on sql_query**

```python
def test_dqx_rejects_columns_arg_on_sql_query():
    # Documents the reason Task 2 removed arguments.columns from sql_query: DQX's
    # ChecksValidator rejects any argument absent from the function signature.
    from databricks.labs.dqx.checks_validator import ChecksValidator
    from databricks.labs.dqx import check_funcs

    check = {
        "criticality": "error",
        "check": {"function": "sql_query", "arguments": {"query": "SELECT 1 AS condition", "columns": ["city"]}},
    }
    errors = ChecksValidator._validate_func_args(
        check["check"]["arguments"], check_funcs.sql_query, check,
        __import__("inspect").signature(check_funcs.sql_query).parameters,
    )
    assert any("Unexpected argument 'columns'" in e for e in errors)
```

- [ ] **Step 2: Run**

Run: `.venv/bin/python -m pytest tests/test_materializer.py::test_dqx_rejects_columns_arg_on_sql_query -q`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add app/tests/test_materializer.py
git commit -m "Test: document DQX rejects a columns arg on sql_query"
```

---

### Task 3: Attribution view — columns fallback + expose rule_name

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py` (`attribution_view_ddl`, lines ~185-213; `_CHECKS_JSON_SCHEMA` line ~103)
- Test: `app/tests/test_score_view_service.py`

**Interfaces:**
- Produces: `v_dq_check_attribution` gains `rule_name` (= `user_metadata['name']`) and its `columns` COALESCE includes `from_json(user_metadata['mapped_columns'], 'ARRAY<STRING>')`. Consumed by Task 4 (shaping/asof views).

- [ ] **Step 1: Write failing tests**

In `test_score_view_service.py` (the attribution-view test class):

```python
def test_attribution_view_exposes_rule_name_from_metadata(self, svc):
    ddl = svc.attribution_view_ddl()
    assert "user_metadata['name'] AS rule_name" in ddl

def test_attribution_view_columns_fall_back_to_metadata_mapped_columns(self, svc):
    ddl = svc.attribution_view_ddl()
    # sql_query checks carry columns only in user_metadata['mapped_columns'];
    # the COALESCE must recover them so by-column populates for sql_query.
    assert "from_json(user_metadata['mapped_columns'], 'ARRAY<STRING>')" in ddl
    # existing arg-based sources still come first
    assert "arg_columns" in ddl and "arg_column" in ddl
```

- [ ] **Step 2: Run to verify fail**

Run: `.venv/bin/python -m pytest tests/test_score_view_service.py -q -k "rule_name or mapped_columns"`
Expected: FAIL

- [ ] **Step 3: Implement**

In `score_view_service.py`, `attribution_view_ddl`:

Add `user_metadata` is already selected (it's `c.col.user_metadata AS user_metadata` at line 192). In the final SELECT (lines 201-212), add a `rule_name` column after `registry_rule_id`:
```python
            "  user_metadata['registry_rule_id'] AS registry_rule_id,\n"
            "  user_metadata['name'] AS rule_name,\n"
```
And change the columns COALESCE (line 209) to add the metadata fallback:
```python
            "  COALESCE(\n"
            "    arg_columns,\n"
            "    CASE WHEN arg_column IS NOT NULL THEN array(arg_column) END,\n"
            "    from_json(user_metadata['mapped_columns'], 'ARRAY<STRING>')\n"
            "  ) AS columns\n"
```

(`_CHECKS_JSON_SCHEMA` already includes `user_metadata: MAP<STRING, STRING>`, so no schema change is needed — `mapped_columns` and `name` are read from that map.)

- [ ] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_score_view_service.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/score_view_service.py app/tests/test_score_view_service.py
git commit -m "Attribution view: expose rule_name + recover sql_query columns from user_metadata"
```

---

### Task 4: Thread rule_name through shaping + asof views

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py` (`shaping_view_ddl` SELECT ~308-319; `asof_view_ddl` final SELECT ~395-407)
- Test: `app/tests/test_score_view_service.py`

**Interfaces:**
- Consumes: `v_dq_check_attribution.rule_name` (Task 3).
- Produces: `v_dq_check_results` and `v_dq_check_results_asof` both carry `rule_name`. Consumed by Task 5 (results) and Task 6 (Genie).

- [ ] **Step 1: Write failing tests**

```python
def test_shaping_view_carries_rule_name(self, svc):
    assert "a.rule_name" in svc.shaping_view_ddl()

def test_asof_view_carries_rule_name(self, svc):
    assert "c.rule_name" in svc.asof_view_ddl()
```

- [ ] **Step 2: Run to verify fail**

Run: `.venv/bin/python -m pytest tests/test_score_view_service.py -q -k "carries_rule_name"`
Expected: FAIL

- [ ] **Step 3: Implement**

In `shaping_view_ddl`, the final SELECT joins attribution as `a` and selects `a.columns` (line 314). Add `a.rule_name`:
```python
            "  a.registry_rule_id,\n"
            "  a.rule_name,\n"
            "  a.columns\n"
```

In `asof_view_ddl`, the final SELECT selects `c.columns` (line 403) from the shaping view aliased `c`. Add `c.rule_name`:
```python
            "  c.registry_rule_id,\n"
            "  c.rule_name,\n"
            "  c.columns\n"
```

- [ ] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_score_view_service.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/score_view_service.py app/tests/test_score_view_service.py
git commit -m "Thread rule_name through shaping + asof score views"
```

---

### Task 5: Results service — label by-rule with rule_name

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/dq_results_service.py` (`CheckResultRow` ~line 77-100; `parse_check_rows` ~120-150; `_by_rule_rows` ~233-261; the two fetch SELECTs in `routes/v1/dq_results.py` ~188-196, 231-233)
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/dq_results.py` (add `rule_name` to the two `SELECT ... FROM {view}` statements)
- Test: `app/tests/test_dq_results_service.py`

**Interfaces:**
- Consumes: `v_dq_check_results.rule_name` (Task 4).
- Produces: by-rule group label = newest run's `rule_name` (fallback `check_name`); grouping key unchanged (`registry_rule_id`).

- [ ] **Step 1: Write failing tests**

In `test_dq_results_service.py`:

```python
def test_by_rule_labels_with_rule_name_not_suffixed_check_name():
    from databricks_labs_dqx_app.backend.services.dq_results_service import CheckResultRow, compute_entity_results  # adjust import to the by-rule entry actually used
    # Two per-column checks of ONE rule: distinct suffixed check_names, same
    # registry_rule_id, same underlying rule_name.
    rows = [
        CheckResultRow(table_fqn="c.s.t", run_id="r1", run_date="2026-07-15", check_name="Column is not null (a)",
                       failed=2, total=100, severity="High", dimension="Completeness", columns=("a",),
                       rule_id="rid1", run_mode="published", rule_name="Column is not null"),
        CheckResultRow(table_fqn="c.s.t", run_id="r1", run_date="2026-07-15", check_name="Column is not null (b)",
                       failed=0, total=100, severity="High", dimension="Completeness", columns=("b",),
                       rule_id="rid1", run_mode="published", rule_name="Column is not null"),
    ]
    from databricks_labs_dqx_app.backend.services.dq_results_service import _by_rule_rows
    groups = _by_rule_rows(rows)
    assert len(groups) == 1
    assert groups[0].label == "Column is not null"
    assert groups[0].rule_id == "rid1"

def test_by_rule_falls_back_to_check_name_when_rule_name_absent():
    from databricks_labs_dqx_app.backend.services.dq_results_service import CheckResultRow, _by_rule_rows
    rows = [CheckResultRow(table_fqn="c.s.t", run_id="r1", run_date="2026-07-15", check_name="a_is_null",
                           failed=1, total=10, severity=None, dimension=None, columns=("a",),
                           rule_id=None, run_mode="published", rule_name=None)]
    groups = _by_rule_rows(rows)
    assert groups[0].label == "a_is_null"
```

(Confirm exact `CheckResultRow` field order/names against the dataclass; adjust kwargs accordingly.)

- [ ] **Step 2: Run to verify fail**

Run: `.venv/bin/python -m pytest tests/test_dq_results_service.py -q -k "rule_name or falls_back"`
Expected: FAIL (`CheckResultRow` has no `rule_name`; label uses check_name)

- [ ] **Step 3: Implement**

(a) `CheckResultRow` (dataclass ~line 77): add field `rule_name: str | None = None`.

(b) `parse_check_rows` (~line 135): add `rule_name=row.get("rule_name")` to the `CheckResultRow(...)` construction.

(c) `_by_rule_rows` (~246-260): track newest `rule_name` alongside check_name and prefer it for the label:
```python
    newest: dict[str, tuple[str, str, str | None]] = {}  # key -> (run_date, check_name, rule_name)
    identity_ids: dict[str, str] = {}
    for row in rows:
        key = _rule_key(row)
        candidate = (row.run_date or "", row.check_name, row.rule_name)
        if key not in newest or candidate[0] > newest[key][0]:
            newest[key] = candidate
        if row.rule_id is not None:
            identity_ids[key] = row.rule_id
    out = _group_rows(rows, _rule_key)
    for group in out:
        if group.label is None:
            continue
        group.rule_id = identity_ids.get(group.label)
        _run_date, check_name, rule_name = newest[group.label]
        group.label = rule_name or check_name
    return out
```

(d) `routes/v1/dq_results.py`: in BOTH fetch SELECTs (the `_fetch_check_rows` at ~188 and `_fetch_asof_check_rows` at ~231), add `rule_name` to the column list, e.g.:
```python
        f"severity, dimension, registry_rule_id, rule_name, to_json(columns) AS columns_json "
```

- [ ] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_dq_results_service.py tests/test_dq_results_route.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/dq_results_service.py app/src/databricks_labs_dqx_app/backend/routes/v1/dq_results.py app/tests/test_dq_results_service.py
git commit -m "Results by-rule: label with underlying rule_name (fallback check_name)"
```

---

### Task 6: Genie — label from rule_name + rollup instruction

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/genie_space_service.py` (the by-rule/applied/drift CTEs using `MAX(\`check_name\`) AS rule_name`, ~lines 514/533; `TEXT_INSTRUCTIONS` ~133)
- Test: `app/tests/test_genie_space_service.py`

**Interfaces:**
- Consumes: `v_dq_check_results.rule_name` (Task 4).
- Produces: Genie's rule-labelled queries display the underlying rule name; instruction text explains the per-column fan-out.

- [ ] **Step 1: Write failing tests**

In `test_genie_space_service.py`:

```python
def test_genie_rule_queries_label_from_rule_name():
    # The by-rule/applied/drift SQL must display MAX(`rule_name`) (the underlying
    # rule name), not MAX(`check_name`) (the per-column suffixed name).
    from databricks_labs_dqx_app.backend.services.genie_space_service import build_genie_space_definition  # adjust to the actual builder
    definition = build_genie_space_definition(catalog="c", schema="s")  # adjust signature
    sql_text = str(definition)  # serialise all SQL snippets
    assert "MAX(`rule_name`) AS rule_name" in sql_text
    # identity grouping + rollup shape preserved
    assert "COALESCE(`registry_rule_id`, `check_name`)" in sql_text
    assert "COUNT(DISTINCT `check_name`)" in sql_text

def test_genie_instructions_explain_column_fanout():
    from databricks_labs_dqx_app.backend.services.genie_space_service import TEXT_INSTRUCTIONS
    joined = " ".join(TEXT_INSTRUCTIONS)
    assert "one check per column" in joined
    assert "registry_rule_id" in joined
```

(Adjust the builder call + serialisation to how `test_genie_space_service.py` already inspects SQL — mirror an existing test in that file.)

- [ ] **Step 2: Run to verify fail**

Run: `.venv/bin/python -m pytest tests/test_genie_space_service.py -q -k "rule_name or fanout"`
Expected: FAIL

- [ ] **Step 3: Implement**

(a) In the `cur`/`prev` CTEs (and any other rule-grain query), change `MAX(\`check_name\`) AS rule_name` → `MAX(\`rule_name\`) AS rule_name`. Leave `COALESCE(registry_rule_id, check_name)` grouping and `COUNT(DISTINCT check_name)` / `col_set` unchanged.

(b) Add one `TEXT_INSTRUCTIONS` element (prose):
```python
    (
        "A rule applied to several columns fans out into one check per column: "
        "those checks share the rule's name (the `rule_name` field) and its "
        "`registry_rule_id`, differing only in `check_name` (suffixed with the "
        "column). Report such a rule ONCE by its `rule_name`, and treat the "
        "per-column checks as its rollup — `COUNT(DISTINCT check_name)` is how "
        "many columns it covers and the exploded `columns` array attributes "
        "failures to each. Never present the suffixed `check_name` as a separate "
        "rule.\n"
    ),
```

- [ ] **Step 4: Run**

Run: `.venv/bin/python -m pytest tests/test_genie_space_service.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/genie_space_service.py app/tests/test_genie_space_service.py
git commit -m "Genie: label rules by rule_name + explain per-column fan-out"
```

---

### Task 7: End-to-end attribution guarantee (the load-bearing requirement)

**Files:**
- Test: `app/tests/test_materializer.py` (or a new `app/tests/test_column_attribution_flow.py`)

**Interfaces:**
- Consumes: Task 2 (materializer output shape) + Task 3 (attribution SQL). Pure-logic test — no live warehouse.

- [ ] **Step 1: Write the test**

Prove the sql_query columns survive from render → the shape the attribution view parses. Since the view runs in Spark, assert the *contract* the view relies on: a rendered `sql_query` check exposes its columns through `user_metadata['mapped_columns']` as valid JSON that `from_json(..., 'ARRAY<STRING>')` would parse, and NOT through `arguments`:

```python
def test_sql_query_columns_recoverable_by_attribution_contract():
    import json as _json
    definition = RuleDefinition.model_validate(
        {
            "body": {
                "sql_query": "SELECT {{col}}, (NOT (COUNT({{col}}) = 1)) AS condition FROM {{input_view}} GROUP BY {{col}}",
                "merge_columns": ["{{col}}"],
            },
            "slots": [{"name": "col", "family": "any", "position": 0, "cardinality": "one"}],
            "parameters": [],
        }
    )
    version = RuleVersion(rule_id="r1", version=1, definition=definition, polarity="pass",
                          user_metadata={"name": "Unique value check"})
    check, _ = render_check(
        mode="lowcode", version=version, group={"col": "city"}, effective_severity="High",
        per_application_tags={}, registry_rule_id="r1", registry_version=1,
        applied_rule_id="ar1", app_settings=_app_settings_stub(),
    )
    # 1. No columns arg (would fail DQX validation)
    assert "columns" not in check["check"]["arguments"]
    # 2. Columns are recoverable via the metadata carrier the attribution view reads
    recovered = _json.loads(check["user_metadata"]["mapped_columns"])
    assert recovered == ["city"]
    # 3. The underlying rule name is available for display
    assert check["user_metadata"]["name"] == "Unique value check"
```

- [ ] **Step 2: Run**

Run: `.venv/bin/python -m pytest tests/test_materializer.py -q -k "attribution_contract"`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add app/tests/
git commit -m "Test: sql_query columns recoverable via metadata attribution contract"
```

---

### Task 8: Full suite + typecheck + lint

- [ ] **Step 1: Backend suite**

Run: `.venv/bin/python -m pytest tests/ -q -k "materializer or score_view or dq_results or genie or registry or scheduler or demo or contract or custom_metrics or rule_test"`
Expected: PASS (all)

- [ ] **Step 2: Lint + typecheck**

Run: `make app-check` and `.venv/bin/python -m ruff check` + `ruff format` on the four modified backend files.
Expected: clean.

- [ ] **Step 3: Commit any format fixes**

```bash
git add -A && git commit -m "Format/lint after rule-name + sql-columns fixes"
```
