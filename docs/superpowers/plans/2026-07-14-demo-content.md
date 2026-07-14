# Demo content for DQX Studio — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a rich, believable e-commerce DQ demo for DQX Studio — seeded source data, ~15 well-named rules across all dimensions/severities, a 9-week real-runs-first quality story, governed-tag auto-apply — driven by an ADMIN-only in-app "Deploy demo content" action (plus a committed CLI wrapper).

**Architecture:** A shared backend package `backend/demo/` holds the demo definition (manifest), the deterministic datagen, and a `DemoSeedService` orchestrator that calls the backend service layer **directly, in-process** (not over HTTP). Runs are genuine engine runs (async serverless Jobs) whose `dq_metrics`/`dq_validation_runs` (Delta) and `dq_score_history`/`dq_score_cache` (Lakebase OLTP) rows are **re-dated** to weekly instants so every number on screen is engine-computed. An ADMIN-gated route launches the seed on a daemon thread (fire-and-forget) with status persisted in `dq_app_settings`; a committed CLI wraps the same service for dev iteration. A config-page card + confirmation modal trigger it.

**Tech Stack:** Python 3.12 / FastAPI / Pydantic 2 / Databricks SDK / `SqlExecutor` (Statement Execution API) / `PgExecutor` (Lakebase) / React 19 + TanStack Query + shadcn/ui + react-i18next / orval.

## Global Constraints

- **Reference source of truth for the pattern:** `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch` — `scripts/seed_demo.py`, `scripts/demo_datagen.py` (adapt, don't copy blindly — DQX's data model differs).
- **Design spec:** `docs/superpowers/specs/2026-07-14-demo-content-design.md`. Every task serves it.
- **Rule copy:** name = short human-readable, ≤80 chars; description = one declarative sentence. **Minimal phrasing, NO leading articles ("A"/"The")** — "Amount is not negative", not "The amount must be zero or greater". No "This rule…" preamble.
- **Rule storage:** name/description/dimension/severity/slot_tags live in `user_metadata` reserved keys (`RESERVED_NAME_KEY="name"`, `RESERVED_DESCRIPTION_KEY="description"`, `RESERVED_DIMENSION_KEY="dimension"`, `RESERVED_SEVERITY_KEY="severity"`, `RESERVED_SLOT_TAGS_KEY="slot_tags"`). Set via `set_reserved_tag(md, key, val)` / `set_slot_tags(md, mapping)`. Rule LOGIC lives in `definition`; never change logic for a naming task.
- **Source data location:** `dqx.dqx_studio_demo` — an SP-owned schema in the app's own catalog (`conf.catalog` default `"dqx"`). The app SP has NO grant on arbitrary user catalogs, so demo data lives where the SP already has rights.
- **Run identity:** the seed builds `ViewService(sql=<SP SqlExecutor>, sp_sql=<SP SqlExecutor>)` so temp views are created as the SP (no user OBO token in a background job). SP owns `dqx_studio_demo`, so it has SELECT.
- **Trend construction:** REAL runs then RE-DATE — never fabricate metric values. `dq_metrics`/`dq_validation_runs` (Delta) and `dq_score_history`/`dq_score_cache` (OLTP) all have MUTABLE timestamps.
- **Access control:** ADMIN only — router-level `require_role(UserRole.ADMIN)` AND the UI card only renders/enables for admins.
- **SQL safety:** every interpolated identifier via `validate_fqn` + `quote_fqn`/`quote_ident`; every literal via `escape_sql_string` (ANSI doubled quotes, never backslash); any templated rule body through `is_sql_query_safe()` (raise `UnsafeSqlQueryError`). Governed-tag keys validated `class.*` + escaped before `SET TAGS`.
- **i18n:** every user-facing string via `t()`, added to ALL of `ui/lib/i18n/locales/{en,pt-BR,it,es}.json` (`en` source of truth). Parity test: `app/tests/test_i18n_locale_parity.py`.
- **No lint suppression:** no `# type: ignore` / `# noqa` / `@ts-ignore` / `eslint-disable`. Fix the code. Type hints on every param/return (`str | None`, `list[str]`, `collections.abc.Callable`).
- **Commits:** GPG-signed, trailer `Co-authored-by: Isaac`. `git add` by explicit path only — never `-A`/`.`. **Never** stage `app/uv.lock` or `uv.lock` (`git checkout -- app/uv.lock uv.lock` before every commit).
- **Gates:** `make app-check` (tsc + basedpyright + bun UI tests) and `make app-test` (backend pytest) must stay green. Baseline: 3033 backend + 395 UI tests pass; 0 basedpyright errors.
- **After backend response-model changes:** `make app-regen-api` to regenerate `ui/lib/api.ts` (orval).

---

## File Structure

**New backend package `app/src/databricks_labs_dqx_app/backend/demo/`:**
- `__init__.py` — package marker + public exports.
- `manifest.py` — the demo DEFINITION as pure data: 5 tables + row counts + per-column seeded issue rates; ~15 rules (key, name, description, dimension, severity, mode, `definition` body/slots, optional `slot_tags`); 5 bindings (rule-key → list of slot→column mapping groups); 2 data products; column-tag assignments; the 9-week story (per-table per-week target fail levels, rule lifecycle add/retire windows, tighten week). No I/O.
- `datagen.py` — deterministic source-table SQL builders (CTAS from `range(N)` with `pmod(hash(pk,salt),1000)` issue seeding), the per-week mutation SQL builders, and `ALTER TABLE … ALTER COLUMN … SET TAGS` builders. Pure string builders + thin `SqlExecutor.execute` calls.
- `redate.py` — pure builders for the re-date SQL: Delta UPDATE of `dq_metrics`/`dq_validation_runs` `run_time`/`created_at`/`updated_at` by `run_id`; OLTP INSERT of back-dated `dq_score_history` rows and UPDATE of `dq_score_cache`.
- `status.py` — `DemoStatusStore`: read/write a JSON status blob in `dq_app_settings` (phase, state, started_at, message).
- `seed_service.py` — `DemoSeedService`: the orchestrator. Constructed from injected executors + internal services; runs the full pipeline; writes status at each phase.

**Modified backend:**
- `routes/v1/admin.py` — add `POST /demo/deploy` (`deployDemoContent`) + `GET /demo/status` (`demoContentStatus`) on the existing ADMIN-gated router; launch on a daemon thread.
- `models.py` — `DeployDemoContentIn` / `DeployDemoContentOut` / `DemoContentStatusOut`.
- `dependencies.py` — `get_demo_seed_service` provider (builds the SP-only service graph incl. `ViewService(sql=sp_sql, sp_sql=sp_sql)`), `get_demo_status_store`.

**New CLI:**
- `app/scripts/seed_demo.py` — thin wrapper: build a `WorkspaceClient(profile=…)`, resolve a warehouse, construct the executors + `DemoSeedService`, run it. For dev iteration.

**Modified UI:**
- `ui/routes/_sidebar/config.tsx` — a "Deploy demo content" card (ADMIN-only) + confirmation modal (wipe-first checkbox, ~1hr warning) + status polling.
- `ui/lib/i18n/locales/{en,pt-BR,it,es}.json` — new keys.
- `ui/lib/api.ts` — regenerated by orval (do not hand-edit).

**Tests:** `app/tests/test_demo_manifest.py`, `test_demo_datagen.py`, `test_demo_redate.py`, `test_demo_status.py`, `test_demo_seed_service.py`, `test_demo_admin_routes.py`.

---

## Task 1: Demo manifest (the definition)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/demo/__init__.py`
- Create: `app/src/databricks_labs_dqx_app/backend/demo/manifest.py`
- Test: `app/tests/test_demo_manifest.py`

**Interfaces:**
- Produces (consumed by datagen, redate, seed_service):
  - `SOURCE_CATALOG_ENV_DEFAULT = "dqx"`, `SOURCE_SCHEMA = "dqx_studio_demo"`, `WEEKS_DEFAULT = 9`.
  - `@dataclass(frozen=True) TableSpec(name: str, row_count: int, primary_key: str, columns: tuple[str, ...])`.
  - `@dataclass(frozen=True) RuleSpec(key: str, name: str, description: str, dimension: str, severity: str, mode: str, body: dict[str, object], slots: tuple[SlotSpec, ...], slot_tags: dict[str, tuple[str, ...]] = {})`.
  - `@dataclass(frozen=True) SlotSpec(name: str, family: str, arg_key: str | None = None)`.
  - `@dataclass(frozen=True) BindingSpec(table: str, display_name: str, mappings: dict[str, tuple[dict[str, str], ...]])` — rule-key → tuple of `{slot_name: column}` groups.
  - `@dataclass(frozen=True) DataProductSpec(name: str, description: str, members: tuple[str, ...])`.
  - `@dataclass(frozen=True) ColumnTagSpec(table: str, column: str, tag: str)`.
  - Module constants: `TABLES: tuple[TableSpec, ...]`, `RULES: tuple[RuleSpec, ...]`, `RULES_BY_KEY: dict[str, RuleSpec]`, `BINDINGS: tuple[BindingSpec, ...]`, `DATA_PRODUCTS: tuple[DataProductSpec, ...]`, `COLUMN_TAGS: tuple[ColumnTagSpec, ...]`.
  - Story: `RULE_LIFECYCLE: dict[tuple[str, str], tuple[int, int]]` (`(table, rule_key) -> (start_week, end_week)`), `TIGHTEN_WEEK: int`, `EXPECT: dict[tuple[str, str], float]` (baseline week-0 fail rate by `(table, column)`), `UNIQUE_EXPECT_ROWS: tuple[int, int]`.
  - `active_mapping(binding: BindingSpec, week: int) -> dict[str, tuple[dict[str, str], ...]]`.

- [ ] **Step 1: Write the failing test**

```python
# app/tests/test_demo_manifest.py
from databricks_labs_dqx_app.backend.demo import manifest as m


def test_every_rule_has_contract_compliant_name_and_description():
    assert m.RULES, "expected a non-empty rule set"
    for r in m.RULES:
        assert r.name and len(r.name) <= 80, f"{r.key}: name empty or >80 chars"
        # minimal phrasing: no leading article
        first = r.name.split()[0].lower()
        assert first not in {"a", "an", "the"}, f"{r.key}: name starts with an article: {r.name!r}"
        assert r.description.endswith("."), f"{r.key}: description not one sentence: {r.description!r}"
        assert r.description.count(".") == 1, f"{r.key}: description has >1 sentence: {r.description!r}"
        dfirst = r.description.split()[0].lower()
        assert dfirst not in {"a", "an", "the"}, f"{r.key}: description starts with an article"
        assert r.dimension in {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
        assert r.severity in {"Low", "Medium", "High", "Critical"}
        assert r.mode in {"dqx_native", "lowcode", "sql"}


def test_all_six_dimensions_and_all_severities_are_covered():
    dims = {r.dimension for r in m.RULES}
    sevs = {r.severity for r in m.RULES}
    assert dims == {"Validity", "Completeness", "Accuracy", "Consistency", "Uniqueness", "Timeliness"}
    assert sevs == {"Low", "Medium", "High", "Critical"}


def test_binding_mappings_reference_real_rules_and_fill_declared_slots():
    for b in m.BINDINGS:
        table = next(t for t in m.TABLES if t.name == b.table)
        for rule_key, groups in b.mappings.items():
            rule = m.RULES_BY_KEY[rule_key]  # KeyError => bad manifest
            slot_names = {s.name for s in rule.slots}
            for group in groups:
                assert set(group.keys()) == slot_names, f"{b.table}/{rule_key}: group {group} != slots {slot_names}"
                for col in group.values():
                    assert col in table.columns, f"{b.table}/{rule_key}: column {col} not in {table.name}"


def test_slot_tags_are_class_namespaced_and_reference_real_slots():
    for r in m.RULES:
        for slot_name, tags in r.slot_tags.items():
            assert slot_name in {s.name for s in r.slots}, f"{r.key}: slot_tags names unknown slot {slot_name}"
            for tag in tags:
                assert tag.startswith("class."), f"{r.key}: slot tag not class.*: {tag}"


def test_column_tags_target_real_columns():
    for ct in m.COLUMN_TAGS:
        table = next(t for t in m.TABLES if t.name == ct.table)
        assert ct.column in table.columns
        assert ct.tag.startswith("class.")


def test_data_products_reference_real_tables():
    names = {t.name for t in m.TABLES}
    for dp in m.DATA_PRODUCTS:
        assert dp.members, f"{dp.name}: no members"
        assert set(dp.members) <= names


def test_active_mapping_honours_lifecycle_windows():
    ship = next(b for b in m.BINDINGS if b.table == "shipments")
    # min_len is added at week 5 per the story
    assert ("shipments", "min_len") in m.RULE_LIFECYCLE
    start, end = m.RULE_LIFECYCLE[("shipments", "min_len")]
    assert "min_len" not in m.active_mapping(ship, start - 1)
    assert "min_len" in m.active_mapping(ship, start)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_manifest.py -q`
Expected: FAIL — `ModuleNotFoundError: databricks_labs_dqx_app.backend.demo`.

- [ ] **Step 3: Create the package marker**

```python
# app/src/databricks_labs_dqx_app/backend/demo/__init__.py
"""Demo-content seeding for DQX Studio (manifest, datagen, orchestrator)."""
```

- [ ] **Step 4: Write `manifest.py`**

Model the dataclasses and constants exactly as the Interfaces block above. Port the rule INTENTS and the 9-week story from the dqlake reference (`databricks-dqwatch/scripts/seed_demo.py` `RULES`, `BINDINGS`, `RULE_LIFECYCLE`, `EXPECT`, `TIGHTEN_WEEK`, `active_mapping`) — but **rewrite every name/description to the minimal-phrasing contract** and express `body`/`slots` in DQX's `dqx_native`/`sql` shape (function + `{{slot}}` arguments, or `{predicate}`). Representative entries (author the rest to match):

```python
from __future__ import annotations
from dataclasses import dataclass, field

SOURCE_CATALOG_ENV_DEFAULT = "dqx"
SOURCE_SCHEMA = "dqx_studio_demo"
WEEKS_DEFAULT = 9
TIGHTEN_WEEK = 6
UNIQUE_EXPECT_ROWS = (0, 700)


@dataclass(frozen=True)
class SlotSpec:
    name: str
    family: str            # numeric|text|temporal|boolean|array|any
    arg_key: str | None = None


@dataclass(frozen=True)
class RuleSpec:
    key: str
    name: str
    description: str
    dimension: str
    severity: str
    mode: str              # dqx_native | sql
    body: dict[str, object]
    slots: tuple[SlotSpec, ...]
    slot_tags: dict[str, tuple[str, ...]] = field(default_factory=dict)


# ... TableSpec, BindingSpec, DataProductSpec, ColumnTagSpec as in Interfaces ...

RULES: tuple[RuleSpec, ...] = (
    RuleSpec(
        key="present",
        name="Value is present",
        description="Value is not null.",
        dimension="Completeness", severity="High", mode="dqx_native",
        body={"function": "is_not_null", "arguments": {"col": "{{col}}"}},
        slots=(SlotSpec("col", "any", arg_key="col"),),
    ),
    RuleSpec(
        key="nonneg",
        name="Amount is not negative",
        description="Numeric amount is zero or greater.",
        dimension="Accuracy", severity="Medium", mode="dqx_native",
        body={"function": "is_not_negative", "arguments": {"column": "{{number}}"}},
        slots=(SlotSpec("number", "numeric", arg_key="column"),),
    ),
    RuleSpec(
        key="pct_range",
        name="Discount is 0 to 100",
        description="Discount percentage falls between 0 and 100 inclusive.",
        dimension="Accuracy", severity="Low", mode="dqx_native",
        body={"function": "is_in_range", "arguments": {"column": "{{pct}}", "min_limit": 0, "max_limit": 100}},
        slots=(SlotSpec("pct", "numeric", arg_key="column"),),
    ),
    RuleSpec(
        key="country_set",
        name="Country is a known ISO code",
        description="Country code is one of the supported ISO codes.",
        dimension="Validity", severity="Medium", mode="dqx_native",
        body={"function": "is_in_list",
              "arguments": {"column": "{{country}}",
                            "allowed": ["US","GB","DE","FR","ES","IT","NL","CA","AU","JP","IN","BR"]}},
        slots=(SlotSpec("country", "text", arg_key="column"),),
        slot_tags={"country": ("class.country",)},   # a tag showcase (task #3)
    ),
    RuleSpec(
        key="not_future",
        name="Timestamp is not in the future",
        description="Event timestamp is no later than now.",
        dimension="Timeliness", severity="Medium", mode="dqx_native",
        body={"function": "is_not_in_future", "arguments": {"column": "{{ts}}"}},
        slots=(SlotSpec("ts", "temporal", arg_key="column"),),
    ),
    RuleSpec(
        key="unique",
        name="Key is unique",
        description="Key value appears at most once in the table.",
        dimension="Uniqueness", severity="High", mode="dqx_native",
        body={"function": "is_unique", "arguments": {"columns": ["{{key}}"]}},
        slots=(SlotSpec("key", "any", arg_key="columns"),),
    ),
    RuleSpec(
        key="card_format",
        name="Card last-four is four digits",
        description="Stored last four card digits are exactly four digits.",
        dimension="Validity", severity="Medium", mode="dqx_native",
        body={"function": "regex_match", "arguments": {"column": "{{code}}", "regex": "^[0-9]{4}$"}},
        slots=(SlotSpec("code", "text", arg_key="column"),),
    ),
    RuleSpec(
        key="email_present",
        name="Email is present and well-formed",
        description="Email is a valid, non-empty address.",
        dimension="Validity", severity="High", mode="dqx_native",
        body={"function": "is_valid_email", "arguments": {"column": "{{email}}"}},
        slots=(SlotSpec("email", "text", arg_key="column"),),
        slot_tags={"email": ("class.email_address",)},   # tag showcase
    ),
    RuleSpec(
        key="min_len",
        name="Tracking number is long enough",
        description="Tracking number is at least five characters.",
        dimension="Validity", severity="Low", mode="sql",
        body={"predicate": "length({{text}}) >= 5"},
        slots=(SlotSpec("text", "text"),),
    ),
    RuleSpec(
        key="end_after_start",
        name="End is not before start",
        description="End timestamp is on or after start timestamp.",
        dimension="Consistency", severity="Medium", mode="sql",
        body={"predicate": "{{end_ts}} >= {{start_ts}}"},
        slots=(SlotSpec("start_ts", "temporal"), SlotSpec("end_ts", "temporal")),
    ),
    RuleSpec(
        key="amount_and_discount",
        name="Amount positive and discount valid",
        description="Amount is above zero and any discount percentage is between 0 and 100.",
        dimension="Accuracy", severity="Critical", mode="sql",
        body={"predicate": "{{amount}} > 0 AND ({{discount_pct}} IS NULL OR {{discount_pct}} BETWEEN 0 AND 100)"},
        slots=(SlotSpec("amount", "numeric"), SlotSpec("discount_pct", "numeric")),
    ),
    # Author the remaining allowed-set rules (tier_set, status_set, method_set,
    # category_set) and card_when_card to the same shape + minimal-phrasing names,
    # so all six dimensions and all four severities are covered (see the two
    # coverage tests). Names e.g. "Account tier is Free, Pro or Enterprise",
    # "Order status is a known status", "Payment method is a known method",
    # "Product category is a known category", "Card details present for card payments".
)
```

**Notes carried from the dqlake reference (respect these):**
- **Uniqueness** uses the native `is_unique` check (columns list), NOT a hand-built count=count_distinct — the latter misfires and flags every row. Assert the dup-row band `UNIQUE_EXPECT_ROWS`.
- **Foreign-key / referential rules are OUT** — the DQX engine runs row checks against the source table only and won't inline a join; dqlake documented this. Reuse story is carried by `present`, `not_future`, `nonneg` appearing on multiple bindings.
- `BINDINGS`, `RULE_LIFECYCLE`, `EXPECT` mirror the dqlake plan (customers improving; orders incident dip+recover; payments tightened at week 6; shipments `min_len` added week 5). `active_mapping` filters a binding's rule keys by the lifecycle `[start, end)` window at a given week.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd app && uv run --group test pytest tests/test_demo_manifest.py -q`
Expected: PASS (all manifest-integrity tests green).

- [ ] **Step 6: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/demo/__init__.py \
        app/src/databricks_labs_dqx_app/backend/demo/manifest.py \
        app/tests/test_demo_manifest.py
git commit -S -m "$(printf 'Add demo-content manifest (tables, rules, story)\n\nCo-authored-by: Isaac')"
```

---

## Task 2: Datagen — deterministic source SQL + weekly mutations + tag assignment

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/demo/datagen.py`
- Test: `app/tests/test_demo_datagen.py`

**Interfaces:**
- Consumes: `manifest` (TableSpec, ColumnTagSpec, EXPECT, TIGHTEN_WEEK, SOURCE_SCHEMA).
- Produces (consumed by seed_service):
  - `build_create_table_sql(table: str, catalog: str, schema: str) -> str` — CTAS from `range(N)` with seeded issues (adapt the dqlake `demo_datagen.py` per-table SQL to the e-commerce columns; deterministic via `pmod(hash(id, salt), 1000) < threshold`).
  - `build_column_comment_sql(table, column, comment, catalog, schema) -> str`.
  - `build_mutation_sql(table: str, week: int, weeks: int, catalog: str, schema: str) -> list[str]` — the per-week `UPDATE … SET col = CASE …` list driving each column to its target week rate (port `mutate_week`). Deterministic + idempotent (full-column overwrite).
  - `build_baseline_reset_sql(catalog, schema) -> list[str]` — week-0 mutation (`build_mutation_sql(·, 0, 1, …)` for every table) to reset to a known baseline.
  - `build_set_column_tag_sql(tag: ColumnTagSpec, catalog: str, schema: str) -> str` — `ALTER TABLE <fqn> ALTER COLUMN <col> SET TAGS ('<key>' = '<value>')` (bare key → empty value `''`), identifiers validated/quoted, tag key+value escaped.
  - `create_schema_sql(catalog: str, schema: str) -> str`, `drop_schema_sql(catalog, schema) -> str`.
- All builders are PURE (return SQL strings); seed_service executes them via `SqlExecutor.execute`.

- [ ] **Step 1: Write the failing test** (assert safety + shape, not warehouse behaviour)

```python
# app/tests/test_demo_datagen.py
import re
from databricks_labs_dqx_app.backend.demo import datagen as d

CAT, SCH = "dqx", "dqx_studio_demo"


def test_create_table_sql_is_fully_qualified_and_deterministic():
    sql = d.build_create_table_sql("customers", CAT, SCH)
    assert "CREATE OR REPLACE TABLE" in sql
    assert f"{CAT}.{SCH}.customers" in sql
    assert "range(" in sql            # server-side generation
    assert "pmod(hash(" in sql        # deterministic seeded issues


def test_mutation_sql_is_a_list_of_full_column_overwrites():
    stmts = d.build_mutation_sql("orders", week=4, weeks=9, catalog=CAT, schema=SCH)
    assert isinstance(stmts, list) and stmts
    for s in stmts:
        assert s.strip().upper().startswith("UPDATE")
        assert f"{CAT}.{SCH}.orders" in s


def test_baseline_reset_covers_every_table():
    stmts = d.build_baseline_reset_sql(CAT, SCH)
    joined = "\n".join(stmts)
    for t in ("customers", "orders", "payments", "products", "shipments"):
        assert f"{CAT}.{SCH}.{t}" in joined


def test_set_column_tag_sql_escapes_and_quotes():
    from databricks_labs_dqx_app.backend.demo.manifest import ColumnTagSpec
    sql = d.build_set_column_tag_sql(ColumnTagSpec("customers", "email", "class.email_address"), CAT, SCH)
    assert "ALTER TABLE" in sql and "ALTER COLUMN" in sql and "SET TAGS" in sql
    assert "class.email_address" in sql


def test_set_column_tag_rejects_non_class_namespace():
    from databricks_labs_dqx_app.backend.demo.manifest import ColumnTagSpec
    import pytest
    with pytest.raises(ValueError):
        d.build_set_column_tag_sql(ColumnTagSpec("customers", "email", "pii.email"), CAT, SCH)


def test_identifiers_are_validated_no_injection():
    import pytest
    with pytest.raises(ValueError):
        d.build_create_table_sql("customers; DROP TABLE x", CAT, SCH)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_datagen.py -q`
Expected: FAIL — module/functions not defined.

- [ ] **Step 3: Implement `datagen.py`**

Port the five CTAS blocks + `mutate_week` from `databricks-dqwatch/scripts/demo_datagen.py` and `seed_demo.py` to functions that take `(catalog, schema)` and build fully-qualified SQL. Use `validate_fqn`/`quote_fqn` from `backend.sql_utils` on the table FQN and `escape_sql_string` on comment/tag literals. `build_set_column_tag_sql` raises `ValueError` if the tag key doesn't start with `class.`. Example skeleton:

```python
from __future__ import annotations
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string, quote_fqn, validate_fqn
from databricks_labs_dqx_app.backend.demo.manifest import ColumnTagSpec, TIGHTEN_WEEK

def _fqn(catalog: str, schema: str, table: str) -> str:
    fqn = f"{catalog}.{schema}.{table}"
    validate_fqn(fqn)            # raises ValueError on injection / bad identifier
    return quote_fqn(fqn)

def create_schema_sql(catalog: str, schema: str) -> str:
    validate_fqn(f"{catalog}.{schema}.x")
    return f"CREATE SCHEMA IF NOT EXISTS {quote_fqn(f'{catalog}.{schema}')}"

def build_create_table_sql(table: str, catalog: str, schema: str) -> str:
    fqn = _fqn(catalog, schema, table)
    return _TABLE_CTAS[table].format(fqn=fqn)   # _TABLE_CTAS: dict[str, str] of the 5 CTAS templates

def build_set_column_tag_sql(tag: ColumnTagSpec, catalog: str, schema: str) -> str:
    if not tag.tag.startswith("class."):
        raise ValueError(f"governed demo tags must be class.*: {tag.tag}")
    fqn = _fqn(catalog, schema, tag.table)
    key, _, value = tag.tag.partition("=")
    return (f"ALTER TABLE {fqn} ALTER COLUMN {tag.column} "
            f"SET TAGS ('{escape_sql_string(key)}' = '{escape_sql_string(value)}')")
# ... build_mutation_sql / build_baseline_reset_sql / build_column_comment_sql ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && uv run --group test pytest tests/test_demo_datagen.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/demo/datagen.py app/tests/test_demo_datagen.py
git commit -S -m "$(printf 'Add demo datagen: seeded source SQL, weekly mutations, column tags\n\nCo-authored-by: Isaac')"
```

---

## Task 3: Re-date SQL builders (Delta runs + OLTP score trend)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/demo/redate.py`
- Test: `app/tests/test_demo_redate.py`

**Interfaces:**
- Produces (consumed by seed_service):
  - `build_redate_metrics_sql(metrics_fqn: str, run_id: str, target_iso: str) -> str` — `UPDATE <dq_metrics> SET run_time = CAST('<iso>' AS TIMESTAMP) WHERE run_id = '<esc>'`.
  - `build_redate_runs_sql(runs_fqn: str, run_id: str, target_iso: str) -> str` — `UPDATE <dq_validation_runs> SET created_at = …, updated_at = … WHERE run_id = '<esc>'`.
  - `build_insert_history_sql(history_fqn: str, scope_type: str, scope_key: str, *, score: float, failed_tests: int | None, total_tests: int | None, target_iso: str) -> str` — back-dated `INSERT INTO dq_score_history (… , run_time, computed_at) VALUES (…, CAST('<iso>' AS TIMESTAMP), CAST('<iso>' AS TIMESTAMP))`.
  - `build_redate_latest_history_sql(history_fqn, scope_type, scope_key, target_iso) -> str` — UPDATE the just-appended (`computed_at = now()`) history row of a scope to `target_iso` (used when re-dating a `ScoreCacheService`-appended point rather than inserting).
  - `iso(dt: datetime) -> str` — format a UTC datetime as `YYYY-MM-DD HH:MM:SS`.
- All builders PURE; scope_type ∈ `{"table","product","global"}`; `run_id`, `scope_key`, `scope_type` escaped via `escape_sql_string`; numeric values rendered as literals (`float(score)`, `int(...)` or `NULL`). FQNs are passed in already-qualified from the caller (built once from the executor's catalog/schema).

- [ ] **Step 1: Write the failing test**

```python
# app/tests/test_demo_redate.py
from datetime import datetime, timezone
from databricks_labs_dqx_app.backend.demo import redate as r

M = "dqx.dqx_studio.dq_metrics"
RUNS = "dqx.dqx_studio.dq_validation_runs"
H = "dqx.dqx_studio.dq_score_history"


def test_iso_formats_utc():
    assert r.iso(datetime(2026, 5, 1, 9, 30, 0, tzinfo=timezone.utc)) == "2026-05-01 09:30:00"


def test_redate_metrics_targets_run_id_and_casts_timestamp():
    sql = r.build_redate_metrics_sql(M, "abc123", "2026-05-01 09:30:00")
    assert sql.startswith("UPDATE")
    assert M in sql and "run_time" in sql
    assert "CAST('2026-05-01 09:30:00' AS TIMESTAMP)" in sql
    assert "run_id = 'abc123'" in sql


def test_insert_history_backdates_both_timestamps():
    sql = r.build_insert_history_sql(H, "global", "global", score=0.91, failed_tests=12, total_tests=1000,
                                     target_iso="2026-05-01 09:30:00")
    assert sql.startswith("INSERT INTO")
    assert "0.91" in sql and "12" in sql and "1000" in sql
    assert sql.count("CAST('2026-05-01 09:30:00' AS TIMESTAMP)") == 2  # run_time + computed_at


def test_insert_history_null_counts_render_null():
    sql = r.build_insert_history_sql(H, "table", "dqx.dqx_studio_demo.orders", score=0.5,
                                     failed_tests=None, total_tests=None, target_iso="2026-05-01 09:30:00")
    assert "NULL" in sql


def test_run_id_is_escaped_against_injection():
    sql = r.build_redate_metrics_sql(M, "a'b", "2026-05-01 09:30:00")
    assert "'a''b'" in sql  # ANSI doubled-quote escaping
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_redate.py -q`
Expected: FAIL — module not defined.

- [ ] **Step 3: Implement `redate.py`**

```python
from __future__ import annotations
from datetime import datetime
from databricks_labs_dqx_app.backend.sql_utils import escape_sql_string

def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _ts(target_iso: str) -> str:
    return f"CAST('{escape_sql_string(target_iso)}' AS TIMESTAMP)"

def build_redate_metrics_sql(metrics_fqn: str, run_id: str, target_iso: str) -> str:
    return (f"UPDATE {metrics_fqn} SET run_time = {_ts(target_iso)} "
            f"WHERE run_id = '{escape_sql_string(run_id)}'")

def build_redate_runs_sql(runs_fqn: str, run_id: str, target_iso: str) -> str:
    return (f"UPDATE {runs_fqn} SET created_at = {_ts(target_iso)}, updated_at = {_ts(target_iso)} "
            f"WHERE run_id = '{escape_sql_string(run_id)}'")

def build_insert_history_sql(history_fqn: str, scope_type: str, scope_key: str, *,
                             score: float, failed_tests: int | None, total_tests: int | None,
                             target_iso: str) -> str:
    f = str(int(failed_tests)) if failed_tests is not None else "NULL"
    t = str(int(total_tests)) if total_tests is not None else "NULL"
    return (f"INSERT INTO {history_fqn} "
            f"(scope_type, scope_key, score, failed_tests, total_tests, run_time, computed_at) VALUES "
            f"('{escape_sql_string(scope_type)}', '{escape_sql_string(scope_key)}', {float(score)}, "
            f"{f}, {t}, {_ts(target_iso)}, {_ts(target_iso)})")

def build_redate_latest_history_sql(history_fqn: str, scope_type: str, scope_key: str, target_iso: str) -> str:
    e_type, e_key = escape_sql_string(scope_type), escape_sql_string(scope_key)
    return (f"UPDATE {history_fqn} SET computed_at = {_ts(target_iso)}, run_time = {_ts(target_iso)} "
            f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}' AND computed_at = ("
            f"SELECT MAX(computed_at) FROM {history_fqn} "
            f"WHERE scope_type = '{e_type}' AND scope_key = '{e_key}')")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && uv run --group test pytest tests/test_demo_redate.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/demo/redate.py app/tests/test_demo_redate.py
git commit -S -m "$(printf 'Add demo re-date SQL builders (Delta runs + OLTP score trend)\n\nCo-authored-by: Isaac')"
```

---

## Task 4: Demo status store (settings-backed job status)

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/demo/status.py`
- Test: `app/tests/test_demo_status.py`

**Interfaces:**
- Consumes: `AppSettingsService` (has `get_setting(key) -> str | None` / `save_setting(key, value, *, user_email)` — verify exact names when implementing; use the same key/value API the governance settings use).
- Produces:
  - `DEMO_STATUS_KEY = "demo_content_status"`.
  - `@dataclass DemoStatus(state: str, phase: str, message: str, started_at: str, updated_at: str)` — `state` ∈ `{"idle","running","succeeded","failed"}`.
  - `class DemoStatusStore` with `__init__(self, app_settings: AppSettingsService)`, `get() -> DemoStatus` (returns an `idle` default when unset or unparseable — graceful), `set(status: DemoStatus, *, user_email: str) -> None` (persists JSON), and `is_running() -> bool`.
- JSON parse failures degrade to `idle` (never raise), so a corrupt blob can't wedge the UI.

- [ ] **Step 1: Write the failing test**

```python
# app/tests/test_demo_status.py
from unittest.mock import create_autospec
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService
from databricks_labs_dqx_app.backend.demo.status import DemoStatus, DemoStatusStore, DEMO_STATUS_KEY


def test_get_returns_idle_when_unset():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = None
    store = DemoStatusStore(settings)
    assert store.get().state == "idle"
    assert store.is_running() is False


def test_set_then_get_round_trips():
    settings = create_autospec(AppSettingsService, instance=True)
    stored: dict[str, str] = {}
    settings.save_setting.side_effect = lambda k, v, **kw: stored.__setitem__(k, v)
    settings.get_setting.side_effect = lambda k: stored.get(k)
    store = DemoStatusStore(settings)
    store.set(DemoStatus(state="running", phase="datagen", message="building tables",
                         started_at="2026-07-14 10:00:00", updated_at="2026-07-14 10:00:05"),
              user_email="admin@example.com")
    got = store.get()
    assert got.state == "running" and got.phase == "datagen"
    assert store.is_running() is True
    assert stored[DEMO_STATUS_KEY]  # JSON persisted under the key


def test_corrupt_blob_degrades_to_idle():
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.return_value = "not-json{{"
    assert DemoStatusStore(settings).get().state == "idle"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_status.py -q`
Expected: FAIL — module not defined.

- [ ] **Step 3: Implement `status.py`**

Use `dataclasses.asdict` + `json.dumps` to serialize, `json.loads` guarded by `try/except (ValueError, TypeError)` returning the idle default. **When implementing, open `app_settings_service.py` and confirm the exact getter/setter method names + their `user_email` kwarg**; adapt the calls to match (the test mocks `get_setting`/`save_setting` — rename in both test and impl if the real API differs, keeping the mock aligned).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && uv run --group test pytest tests/test_demo_status.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/demo/status.py app/tests/test_demo_status.py
git commit -S -m "$(printf 'Add settings-backed demo job status store\n\nCo-authored-by: Isaac')"
```

---

## Task 5: DemoSeedService orchestrator

**Files:**
- Create: `app/src/databricks_labs_dqx_app/backend/demo/seed_service.py`
- Test: `app/tests/test_demo_seed_service.py`

**Interfaces:**
- Consumes (constructor — all injected, DI-friendly):
  ```python
  def __init__(
      self,
      *,
      demo_sql: SqlExecutor,            # bound to dqx.dqx_studio_demo (datagen + Delta re-date)
      app_sql: SqlExecutor,             # bound to dqx.dqx_studio (dq_metrics/dq_validation_runs re-date)
      oltp: OltpExecutorProtocol,       # dq_score_history / dq_score_cache
      registry: RegistryService,
      monitored_tables: MonitoredTableService,
      apply_rules: ApplyRulesService,
      materializer: Materializer,
      rules_catalog: RulesCatalogService,
      version_service: MonitoredTableVersionService,
      data_products: DataProductService,
      binding_run: BindingRunService,   # built with ViewService(sql=SP, sp_sql=SP)
      score_cache: ScoreCacheService,
      status: DemoStatusStore,
      reset_service: DatabaseResetService | None = None,
      catalog: str = "dqx",
  ) -> None
  ```
- Produces:
  - `run(self, *, user_email: str, wipe_first: bool, weeks: int = WEEKS_DEFAULT) -> DemoSeedResult` — the full pipeline (§4 of the spec). Writes status at each phase; returns a summary `@dataclass DemoSeedResult(rules: int, tables: int, products: int, weeks: int, trend_points: int)`.
  - `RuleDefinition`/`RuleSlot` are built from each `RuleSpec` via a private `_definition_for(spec) -> RuleDefinition` and `_metadata_for(spec) -> dict` (using `set_reserved_tag` + `set_slot_tags`).
- Idempotent: rule creation uses `registry.match_or_create_approved_rule` (by fingerprint); binding/product/table creation catch the "already exists" domain errors and reuse.

**Implementation notes (grounded in verified signatures):**
- **Build rules:** for each `RuleSpec`, `definition = RuleDefinition(body=spec.body, slots=[RuleSlot(name=s.name, family=s.family, arg_key=s.arg_key, position=i) for i,s in enumerate(spec.slots)])`; `md = set_slot_tags(set_reserved_tag(... name/description/dimension/severity ...), spec.slot_tags)`; `registry.match_or_create_approved_rule(definition, md, user_email)`. Keep a `rule_key -> rule_id` map.
- **Bindings:** `monitored_tables.register(table_fqn, user_email)` (catch `DuplicateMonitoredTableError`, then `get` by fqn); build `list[DesiredAppliedRule]` from `active_mapping(binding, 0)` (each mapping group → `ColumnMappingGroup` dict); `apply_rules.save_applied_rules(binding_id, desired, user_email)`; then **materialize→submit→approve** by replicating the route helper sequence: `materializer.materialize_binding(binding_id)` → transition materialized checks `draft→pending_approval` → transition `pending_approval→approved` → `monitored_tables.set_status(binding_id,"approved",…)` → `version_service.freeze_new_version(binding_id, user_email)`. (Import/reuse `_transition_binding_checks`/`_approve_binding_checks` from `routes/v1/monitored_tables.py` if practical, else replicate the 3 short calls using `rules_catalog.set_status` over `monitored_tables.list_materialized_rule_statuses(binding_id)`.)
- **Data products:** `data_products.create(name, description, None, user_email)`; per member `data_products.add_member(product_id, binding_id, None, user_email)`; `data_products.submit(...)`; `data_products.approve(...)`.
- **Validation gate:** run `datagen.build_baseline_reset_sql`; `binding_run.run_binding(binding_id, "approved", None, user_email)` per binding; wait terminal (poll `dq_validation_runs.status` via `app_sql.query_dicts` by `run_id` until `SUCCESS`/`FAILED`, generous timeout); read per-check `failed_tests` from `dq_metrics` and compare to `EXPECT` — hard-fail (raise) a rate>0.985 where a low rate was expected, or a `unique` count outside `UNIQUE_EXPECT_ROWS`.
- **Weekly loop (weeks):** for `wi` in range: apply lifecycle add/retire (re-`save_applied_rules` + re-approve only the changed bindings, using `active_mapping(binding, wi)`); run `datagen.build_mutation_sql(table, wi, weeks, …)`; `binding_run.run_binding(...)` per binding; wait terminal; compute weekly instant (irregular spacing like dqlake `GAP_DAYS`/`HOURS`; final week = one shared recent instant); for each binding: re-date `dq_metrics` + `dq_validation_runs` (`app_sql.execute(redate.build_redate_*_sql(...))`), call `score_cache.refresh_for_tables([fqn])` (engine-computed score), then `app_sql`/`oltp` re-date the just-appended `dq_score_history` row via `redate.build_redate_latest_history_sql`. After all bindings for the week: `score_cache.refresh_product(pid)` + `score_cache.refresh_global()`, then re-date those appended product/global history rows to the same instant.
- **Finish:** `score_cache.refresh_all_for_tables([...])` for a truthful "now"; write `succeeded` status.
- **Timestamps:** `datetime.now(timezone.utc)` is fine here (this is a service, not a Workflow script).
- **Status:** wrap the whole `run` in try/except; on exception write `failed` status (message = sanitized `str(e)`, newlines stripped) and re-raise.

- [ ] **Step 1: Write the failing test** (unit — all deps mocked; assert orchestration, not warehouse)

```python
# app/tests/test_demo_seed_service.py
from unittest.mock import create_autospec, MagicMock
import pytest
from databricks_labs_dqx_app.backend.demo.seed_service import DemoSeedService
from databricks_labs_dqx_app.backend.demo.status import DemoStatusStore
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _svc(**over):
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
    from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
    from databricks_labs_dqx_app.backend.services.materializer import Materializer
    from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
    from databricks_labs_dqx_app.backend.services.monitored_table_version_service import MonitoredTableVersionService
    from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
    from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
    from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService
    deps = dict(
        demo_sql=create_autospec(SqlExecutor, instance=True),
        app_sql=create_autospec(SqlExecutor, instance=True),
        oltp=MagicMock(),
        registry=create_autospec(RegistryService, instance=True),
        monitored_tables=create_autospec(MonitoredTableService, instance=True),
        apply_rules=create_autospec(ApplyRulesService, instance=True),
        materializer=create_autospec(Materializer, instance=True),
        rules_catalog=create_autospec(RulesCatalogService, instance=True),
        version_service=create_autospec(MonitoredTableVersionService, instance=True),
        data_products=create_autospec(DataProductService, instance=True),
        binding_run=create_autospec(BindingRunService, instance=True),
        score_cache=create_autospec(ScoreCacheService, instance=True),
        status=create_autospec(DemoStatusStore, instance=True),
    )
    deps.update(over)
    return DemoSeedService(**deps), deps


def test_run_creates_all_rules_and_writes_terminal_status():
    svc, deps = _svc()
    # registry returns a fake approved rule with an id for every create
    rule = MagicMock(); rule.rule_id = "r1"
    deps["registry"].match_or_create_approved_rule.return_value = (rule, True)
    # make binding registration + run + score reads no-op-friendly
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    # short-circuit the wait+validate+weekly loop via a 0-week run for the unit test
    result = svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    assert deps["registry"].match_or_create_approved_rule.call_count >= 10
    # terminal status written
    assert deps["status"].set.called
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state in {"succeeded", "failed"}


def test_wipe_first_calls_reset_service():
    reset = MagicMock()
    svc, deps = _svc(reset_service=reset)
    deps["registry"].match_or_create_approved_rule.return_value = (MagicMock(rule_id="r1"), True)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    svc.run(user_email="admin@example.com", wipe_first=True, weeks=0)
    reset.reset_all_data.assert_called_once()


def test_run_marks_failed_status_and_reraises_on_error():
    svc, deps = _svc()
    deps["registry"].match_or_create_approved_rule.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "failed"
```

> Implementer note: design `run(weeks=0)` to build rules/bindings/products + write terminal status but skip the weekly history loop, so the orchestration is unit-testable without a warehouse. The real 9-week path is exercised in the sandbox deploy (Task 9).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_seed_service.py -q`
Expected: FAIL — module not defined.

- [ ] **Step 3: Implement `seed_service.py`** per the Implementation notes above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && uv run --group test pytest tests/test_demo_seed_service.py -q`
Expected: PASS.

- [ ] **Step 5: Run the whole demo unit suite + type check**

Run: `cd app && uv run --group test pytest tests/test_demo_*.py -q && cd .. && make app-check`
Expected: PASS; 0 basedpyright errors.

- [ ] **Step 6: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/demo/seed_service.py app/tests/test_demo_seed_service.py
git commit -S -m "$(printf 'Add DemoSeedService orchestrator (real-runs-first + re-date)\n\nCo-authored-by: Isaac')"
```

---

## Task 6: DI providers (dependencies.py)

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/dependencies.py`
- Test: `app/tests/test_demo_seed_service.py` (add a construction smoke test) — or a focused `test_demo_dependencies.py`.

**Interfaces:**
- Produces: `get_demo_status_store(app_settings) -> DemoStatusStore`, and `get_demo_seed_service(...) -> DemoSeedService` that assembles the SP-only graph. CRUCIAL: it must build `demo_sql = SqlExecutor(ws=sp_ws, warehouse_id=<wh>, catalog=conf.catalog, schema="dqx_studio_demo")` and a `BindingRunService` whose `ViewService(sql=<SP SqlExecutor>, sp_sql=<SP SqlExecutor>)` — NOT the OBO view service — so it runs with no user token.

- [ ] **Step 1: Write the failing test**

```python
# append to app/tests/test_demo_seed_service.py
def test_get_demo_seed_service_builds_sp_only_view_service(monkeypatch):
    # Construction-only smoke: the provider assembles without a request/OBO token.
    # (Heavier wiring is covered by the route test; this asserts the provider exists
    # and yields a DemoSeedService.)
    import inspect
    from databricks_labs_dqx_app.backend import dependencies
    assert hasattr(dependencies, "get_demo_seed_service")
    assert hasattr(dependencies, "get_demo_status_store")
    sig = inspect.signature(dependencies.get_demo_seed_service)
    assert sig.return_annotation.__name__ == "DemoSeedService" or "DemoSeedService" in str(sig.return_annotation)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_seed_service.py::test_get_demo_seed_service_builds_sp_only_view_service -q`
Expected: FAIL — attributes not defined.

- [ ] **Step 3: Implement the providers**

Add to `dependencies.py`, following the existing `get_view_service`/`get_binding_run_service`/`get_score_cache_service` patterns but with the SP executor in BOTH ViewService slots. Sketch:

```python
async def get_demo_status_store(
    app_settings: Annotated[AppSettingsService, Depends(get_app_settings_service)],
) -> DemoStatusStore:
    return DemoStatusStore(app_settings)


async def get_demo_seed_service(
    sp_ws: Annotated[WorkspaceClient, Depends(get_sp_ws)],
    sp_sql: Annotated[SqlExecutor, Depends(get_sp_sql_executor)],
    oltp: Annotated[OltpExecutorProtocol, Depends(get_sp_oltp_executor)],
    registry: Annotated[RegistryService, Depends(get_registry_service)],
    monitored_tables: Annotated[MonitoredTableService, Depends(get_monitored_table_service)],
    apply_rules: Annotated[ApplyRulesService, Depends(get_apply_rules_service)],
    materializer: Annotated[Materializer, Depends(get_materializer)],
    rules_catalog: Annotated[RulesCatalogService, Depends(get_rules_catalog_service)],
    version_service: Annotated[MonitoredTableVersionService, Depends(get_monitored_table_version_service)],
    data_products: Annotated[DataProductService, Depends(get_data_product_service)],
    score_cache: Annotated[ScoreCacheService, Depends(get_score_cache_service)],
    materializer2: ... ,  # reuse the same materializer instance
    status: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    reset_service: Annotated[DatabaseResetService, Depends(get_database_reset_service)],
) -> DemoSeedService:
    warehouse_id = _get_warehouse_id()
    demo_sql = SqlExecutor(ws=sp_ws, warehouse_id=warehouse_id, catalog=conf.catalog, schema="dqx_studio_demo")
    sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)  # SP-only view creation for background runs
    binding_run = BindingRunService(
        monitored_tables=monitored_tables, version_service=version_service, materializer=materializer,
        view_service=sp_view, job_service=<get_job_service dep>, run_set_service=<get_run_set_service dep>,
        settings_service=<app_settings dep>, runs_table=sp_sql.fqn("dq_validation_runs"),
    )
    return DemoSeedService(
        demo_sql=demo_sql, app_sql=sp_sql, oltp=oltp, registry=registry, monitored_tables=monitored_tables,
        apply_rules=apply_rules, materializer=materializer, rules_catalog=rules_catalog,
        version_service=version_service, data_products=data_products, binding_run=binding_run,
        score_cache=score_cache, status=status, reset_service=reset_service, catalog=conf.catalog,
    )
```

When implementing, resolve the exact peer providers (`get_job_service`, `get_run_set_service`, `get_app_settings_service`) already used by `get_binding_run_service` (dependencies.py:717-743) and add them as `Depends` params instead of the `<…>` placeholders above. Import the new symbols at the top of the file.

- [ ] **Step 4: Run test + type check**

Run: `cd app && uv run --group test pytest tests/test_demo_seed_service.py -q && cd .. && make app-check`
Expected: PASS; 0 errors.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/dependencies.py app/tests/test_demo_seed_service.py
git commit -S -m "$(printf 'Wire demo seed service DI (SP-only view path)\n\nCo-authored-by: Isaac')"
```

---

## Task 7: Admin routes + daemon-thread launch

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/admin.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/models.py`
- Test: `app/tests/test_demo_admin_routes.py`

**Interfaces:**
- Consumes: `get_demo_seed_service`, `get_demo_status_store`, `require_role(UserRole.ADMIN)` (router already gated), `get_obo_ws` (for actor email).
- Produces:
  - `models.py`: `DeployDemoContentIn(BaseModel)` = `{ wipe_first: bool = False }`; `DeployDemoContentOut(BaseModel)` = `{ status: str, started_at: str }`; `DemoContentStatusOut(BaseModel)` = `{ state: str, phase: str, message: str, started_at: str, updated_at: str }`.
  - `admin.py`: `POST /demo/deploy` (`operation_id="deployDemoContent"`) and `GET /demo/status` (`operation_id="demoContentStatus"`).
- Behaviour: `deploy` returns 409 if a seed is already `running`; otherwise sets `running` status, launches `DemoSeedService.run(...)` on a **named daemon thread** (mirror `config.py`'s `_fire_and_forget_ensure_vector_store`: `threading.Thread(target=_run, name="dqx-demo-seed", daemon=True).start()`, where `_run` calls the service and lets the service write terminal status), and returns immediately with `{status:"running", started_at}`.

- [ ] **Step 1: Write the failing test**

```python
# app/tests/test_demo_admin_routes.py
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
# Reuse the app's existing admin-route test harness/fixtures (see tests/test_admin_routes.py
# or the resetDatabase route test) for building a TestClient with an ADMIN identity and
# dependency_overrides. Follow that file's setup verbatim.


def test_deploy_requires_admin(client_as_viewer):
    resp = client_as_viewer.post("/api/v1/admin/demo/deploy", json={"wipe_first": False})
    assert resp.status_code == 403


def test_deploy_launches_and_returns_running(client_as_admin, demo_seed_service_mock, demo_status_store_mock):
    demo_status_store_mock.is_running.return_value = False
    resp = client_as_admin.post("/api/v1/admin/demo/deploy", json={"wipe_first": True})
    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    # the thread target eventually calls run(...) with wipe_first=True
    # (join the launched thread in the test, or assert the mock was scheduled)


def test_deploy_conflict_when_already_running(client_as_admin, demo_status_store_mock):
    demo_status_store_mock.is_running.return_value = True
    resp = client_as_admin.post("/api/v1/admin/demo/deploy", json={"wipe_first": False})
    assert resp.status_code == 409


def test_status_endpoint_returns_state(client_as_admin, demo_status_store_mock):
    from databricks_labs_dqx_app.backend.demo.status import DemoStatus
    demo_status_store_mock.get.return_value = DemoStatus("running", "weekly", "week 3/9",
                                                         "2026-07-14 10:00:00", "2026-07-14 10:20:00")
    resp = client_as_admin.get("/api/v1/admin/demo/status")
    assert resp.status_code == 200 and resp.json()["phase"] == "weekly"
```

> Implementer: model `client_as_admin`/`client_as_viewer`/`*_mock` fixtures on the EXISTING admin/reset-route test in `app/tests/` (grep for `resetDatabase` or `reset-database` in tests to find the harness). Use FastAPI `app.dependency_overrides` to inject the mocked seed service + status store and an ADMIN role. For the launch test, make the daemon thread synchronous in-test (override the launcher to call the target inline, or `thread.join()`), so the assertion is deterministic — no real sleeps.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_admin_routes.py -q`
Expected: FAIL — routes/models not defined.

- [ ] **Step 3: Add the models** to `models.py` (place near `ResetDatabaseIn`/`ResetDatabaseOut`, ~line 2310).

- [ ] **Step 4: Add the routes** to `admin.py`:

```python
import threading
from databricks_labs_dqx_app.backend.demo.status import DemoStatus
# ... existing imports ...

@router.post("/demo/deploy", response_model=DeployDemoContentOut, operation_id="deployDemoContent")
def deploy_demo_content(
    body: DeployDemoContentIn,
    seeder: Annotated[DemoSeedService, Depends(get_demo_seed_service)],
    status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
    obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)],
) -> DeployDemoContentOut:
    if status_store.is_running():
        raise HTTPException(status_code=409, detail="A demo deployment is already in progress.")
    try:
        performed_by = obo_ws.current_user.me().user_name or "unknown"
    except Exception:
        performed_by = "unknown"
    started_at = _utc_now_str()  # helper: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    status_store.set(DemoStatus("running", "starting", "queued", started_at, started_at), user_email=performed_by)

    def _run() -> None:
        try:
            seeder.run(user_email=performed_by, wipe_first=body.wipe_first)
        except Exception:
            logger.error("Demo content deployment failed", exc_info=True)  # service already wrote 'failed'

    threading.Thread(target=_run, name="dqx-demo-seed", daemon=True).start()
    return DeployDemoContentOut(status="running", started_at=started_at)


@router.get("/demo/status", response_model=DemoContentStatusOut, operation_id="demoContentStatus")
def demo_content_status(
    status_store: Annotated[DemoStatusStore, Depends(get_demo_status_store)],
) -> DemoContentStatusOut:
    s = status_store.get()
    return DemoContentStatusOut(state=s.state, phase=s.phase, message=s.message,
                                started_at=s.started_at, updated_at=s.updated_at)
```

- [ ] **Step 5: Run tests + regen API + type check**

Run: `cd app && uv run --group test pytest tests/test_demo_admin_routes.py -q && cd .. && make app-regen-api && make app-check`
Expected: tests PASS; `ui/lib/api.ts` gains `useDeployDemoContent` + `useDemoContentStatus`; app-check green.

- [ ] **Step 6: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/backend/routes/v1/admin.py \
        app/src/databricks_labs_dqx_app/backend/models.py \
        app/tests/test_demo_admin_routes.py \
        app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -S -m "$(printf 'Add ADMIN-only demo deploy + status routes (daemon-thread launch)\n\nCo-authored-by: Isaac')"
```

---

## Task 8: CLI wrapper (dev iteration)

**Files:**
- Create: `app/scripts/seed_demo.py`
- (No new unit test — the module logic is covered by Tasks 1–5; this is a thin dependency-assembly wrapper. A smoke import is enough.)

**Interfaces:**
- `main() -> int` — argparse `--profile` (default from env), `--warehouse-id`, `--weeks` (default 9), `--wipe-first`, `--catalog` (default `dqx`). Builds `WorkspaceClient(profile=…)`, resolves a warehouse, constructs the executors + services (the same graph as `get_demo_seed_service`, but from a standalone `WorkspaceClient` instead of FastAPI DI), and calls `DemoSeedService.run(...)`. Line-buffers stdout.

- [ ] **Step 1: Write a smoke test**

```python
# app/tests/test_demo_cli_smoke.py
def test_cli_module_imports_and_has_main():
    import importlib.util, pathlib
    p = pathlib.Path(__file__).parents[1] / "scripts" / "seed_demo.py"
    spec = importlib.util.spec_from_file_location("demo_cli", p)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    assert callable(mod.main)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run --group test pytest tests/test_demo_cli_smoke.py -q`
Expected: FAIL — file not found.

- [ ] **Step 3: Write `app/scripts/seed_demo.py`** assembling the same service graph as Task 6 from a standalone `WorkspaceClient`; guard the OLTP executor construction (Lakebase) behind the app's existing helper if importable, else document that the CLI targets the Delta-OLTP-fallback path. `if __name__ == "__main__": sys.exit(main())`.

- [ ] **Step 4: Run smoke test**

Run: `cd app && uv run --group test pytest tests/test_demo_cli_smoke.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/scripts/seed_demo.py app/tests/test_demo_cli_smoke.py
git commit -S -m "$(printf 'Add seed_demo CLI wrapper (dev iteration)\n\nCo-authored-by: Isaac')"
```

---

## Task 9: Admin UI — "Deploy demo content" card + modal + i18n

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx`
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/{en,pt-BR,it,es}.json`

**Interfaces:**
- Consumes: `useDeployDemoContent()` + `useDemoContentStatus()` (orval, from Task 7), `usePermissions().isAdmin`, existing shadcn `Card`/`Dialog`/`Button`/`Checkbox`/`Label`.
- Produces: a `DeployDemoCard()` component registered in the settings card list (same list as `DangerZoneCard`, ~config.tsx:2674), rendered only for admins.

- [ ] **Step 1: Add i18n keys to `en.json`** (and translate into `pt-BR`/`it`/`es`). Keys under a `config.*` namespace:
  - `demoTitle`: "Deploy demo content"
  - `demoBody`: "Populate this workspace with a realistic sample data-quality deployment — sample tables, rules across every quality dimension, and several weeks of run history."
  - `demoButton`: "Deploy demo content"
  - `demoAdminOnly`: "Only admins can deploy demo content."
  - `demoDialogTitle`: "Deploy demo content?"
  - `demoWipeLabel`: "Wipe existing data first (recommended)"
  - `demoWarning`: "This takes about an hour to populate. Don't interact with the app while it runs."
  - `demoConfirm`: "Start deployment"
  - `demoCancel`: "Cancel"
  - `demoStarted`: "Demo deployment started. It will take about an hour."
  - `demoInProgress`: "Deploying demo content…"
  - `demoRunningBanner`: "Demo deployment in progress ({{phase}})"
  - `demoFailed`: "Demo deployment failed. See server logs."
  - `kwDemo`: "demo sample seed populate" (search keywords)

- [ ] **Step 2: Add the card component** to `config.tsx`, modelled on `DangerZoneCard` (config.tsx:2488-2612) but non-destructive styling. Gate the button on `isAdmin`; render an `AlertDialog`/`Dialog` with a `Checkbox` (default checked) for wipe-first and the warning text; on confirm call `deployMutation.mutate({ data: { wipe_first } })`, toast `demoStarted`, and start polling `useDemoContentStatus` (TanStack `refetchInterval` while `state === "running"`). Show a subtle running banner from the status query. Register it in the card list array near line 2674:
  ```tsx
  { id: "deployDemo", tab: "danger", title: t("config.demoTitle"), keywords: t("config.kwDemo"), render: () => <DeployDemoCard /> },
  ```
  All display text via `t()`.

- [ ] **Step 3: Type-check + i18n parity + UI tests**

Run: `cd .. && make app-check`
Expected: tsc clean; the i18n parity test (`app/tests/test_i18n_locale_parity.py`) green (run `cd app && uv run --group test pytest tests/test_i18n_locale_parity.py -q`); 395+ UI tests pass.

- [ ] **Step 4: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/pt-BR.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/it.json \
        app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/es.json
git commit -S -m "$(printf 'Add Deploy-demo-content admin card + modal (i18n 4-locale)\n\nCo-authored-by: Isaac')"
```

---

## Task 10: Deploy to sandbox + verify (end-to-end)

**Files:** none (verification task).

- [ ] **Step 1: Full local gates**

Run: `make app-check && make app-test`
Expected: app-check 0 errors + UI tests green; app-test ≥3033 + new demo tests pass.

- [ ] **Step 2: Deploy (capture the REAL exit code)**

Run:
```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/demo
git checkout -- app/uv.lock uv.lock 2>/dev/null || true
make app-deploy PROFILE=fe-sandbox-dq-demo TARGET=dev > /tmp/deploy.log 2>&1; echo "EXIT: $?"; tail -30 /tmp/deploy.log
```
Expected: `EXIT: 0` AND "Deployment complete!" AND "App started successfully". ("app: RUNNING" alone is NOT proof.)

- [ ] **Step 3: Trigger the demo seed** on the sandbox (as an admin) — either the Config-page "Deploy demo content" button (wipe-first checked), or the CLI:
  `cd app && uv run python scripts/seed_demo.py --profile fe-sandbox-dq-demo --wipe-first`
  Then poll `GET /api/v1/admin/demo/status` (or watch the UI banner) until `succeeded` (~1 hour). Use the apx MCP `databricks_apps_logs` tool (`app_name="dqx-studio"`, `profile="fe-sandbox-dq-demo"`) to watch backend logs while it runs.

- [ ] **Step 4: Verify the demo reads correctly** on the deployed app:
  - Homepage `/api/v1/home/stats`: `rule_count` ~15, `monitored_table_count` 5, `table_space_count` 2, non-null `score`, and a multi-point `score_trend` that moves (not flat).
  - Monitored tables show real failed records; drilldowns show per-dimension/severity breakdowns.
  - Data products (Customer 360, Fulfillment) show rollup scores + trends.
  - Apply-Rules screen on a tagged table (e.g. customers) surfaces tag-matched rule SUGGESTIONS (tag_auto_apply OFF), matching a governed tag (e.g. `class.email_address`).
  - Trend spans ~9 points with the story shape (orders dip+recover; customers improving; shipments' new rule appears mid-history).

- [ ] **Step 5: Report back to the user** with the deploy exit status, the seed outcome, and the verified homepage numbers. Do NOT merge or push — wait for the user's go-ahead. Visual QA is owed to the user (agents can't drive the authed browser).

---

## Self-review notes (author → implementer)

- **Spec coverage:** task #1 domain/source-data → Tasks 1–2; rule naming (#2) → Task 1 (+contract tests); tags (#3) → Task 1 (slot_tags) + Task 2 (SET TAGS) + Task 9 (suggestions visible); real-runs-first + re-date trend → Tasks 3 + 5; ADMIN-only in-app action + wipe-first modal + ~1hr warning → Tasks 6–7 + 9; CLI → Task 8; deploy+verify → Task 10.
- **SP-view risk (spec §5):** resolved in Task 6 — `ViewService(sql=SP, sp_sql=SP)`; no `BindingRunService` change needed.
- **Materialize/approve** is not a single service call — Task 5 replicates the route helper sequence (`materialize_binding` → transition draft→pending→approved → `freeze_new_version`). Verify the helper names against `routes/v1/monitored_tables.py` when implementing.
- **AppSettingsService getter/setter names** (Task 4) and **peer providers** (`get_job_service`/`get_run_set_service`, Task 6) must be confirmed against source at implementation time — the plan flags both explicitly.
