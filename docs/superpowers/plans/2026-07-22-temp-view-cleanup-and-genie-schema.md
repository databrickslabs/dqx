# Temp-view cleanup + Genie schema move — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop temp views (`tmp_view_*`) from leaking in Unity Catalog, and move the 7 Genie-derived views/dims into a dedicated `genie` schema.

**Architecture:** Backend-only (Python/FastAPI). Part A fixes the broken orphan-view GC query, fixes a demo path that creates views in the wrong schema, and adds prompt drop-on-completion piggybacked on the scheduler's existing 60s terminal-run tick. Part B introduces a `genie` schema and repoints the SP-owned derived views/dims to it while their view bodies still read the base Delta tables in `dqx_studio`.

**Tech Stack:** Python 3.12, FastAPI, Databricks SDK, `SqlExecutor` (Statement Execution API), pytest, basedpyright. Full spec: `docs/superpowers/specs/2026-07-22-temp-view-cleanup-and-genie-schema-design.md`.

## Global Constraints

- SQL identifiers: `validate_fqn` + `quote_fqn` (`backend/sql_utils.py`); string literals: `escape_sql_string` (ANSI doubled quotes, never backslash). No user input reaches these paths.
- Type hints on every param + return; `str | None` not `Optional`; `list`/`dict` generics; no `Any`. `make app-check` (bun tsc -b + basedpyright) must stay clean (the 5 pre-existing `monitored_table_service.py:1062` basedpyright errors are unrelated and out of scope — do not touch that file).
- Dropping a view is best-effort: `DROP VIEW IF EXISTS`, wrapped in try/except, logged (not raised). A permission failure on an OBO-owned view is expected and benign.
- Never drop a view whose run row is still `RUNNING`. Only drop when the run is non-`RUNNING`.
- Only-drop `tmp_view_`-named FQNs. Skip synthetic cross-table `view_fqn` (which is a quoted source-table name, not a temp view).
- Tests must assert real SQL shape (object names / columns), not just call counts.
- Run `make app-test` from repo root; single test: `.venv-app` is not used — use `cd app && uv run pytest tests/<file>.py::<test> -v` (backend tests live in `app/tests/`).
- Preserve existing scheduler behaviour (score-cache refresh, timestamp denorm) unchanged.
- Backend tests live in `app/tests/`. Run one file: `cd app && uv run pytest tests/test_scheduler_service.py -v`.

---

## File Structure

**Part A (temp-view cleanup):**
- Modify `app/src/databricks_labs_dqx_app/backend/services/scheduler_service.py` — fix GC query; add drop-on-completion.
- Modify `app/src/databricks_labs_dqx_app/backend/dependencies.py` — fix demo `sp_view` schema.
- Modify `app/tests/test_scheduler_service.py` — GC SQL-shape + drop-on-completion tests.
- Modify `app/tests/test_demo_seed_service.py` (or nearest demo-wiring test) — assert `sp_view` uses tmp schema.

**Part B (genie schema):**
- Modify `app/src/databricks_labs_dqx_app/backend/config.py` — add `genie_schema_name`.
- Modify `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py` — genie schema for object names, main schema for base-table refs.
- Modify `app/src/databricks_labs_dqx_app/backend/services/metadata_dim_service.py` — same split.
- Modify `app/src/databricks_labs_dqx_app/backend/services/entitlement_service.py` — `v_dq_failing_rows` in genie.
- Modify `app/src/databricks_labs_dqx_app/backend/services/genie_space_service.py` — curated SQL / data sources use genie schema.
- Modify `app/src/databricks_labs_dqx_app/backend/app.py` — ensure genie schema; wire genie schema into the ensure functions + genie space.
- Modify `app/src/databricks_labs_dqx_app/backend/dependencies.py` — wire genie schema into services.
- Modify `app/src/databricks_labs_dqx_app/backend/services/database_reset_service.py` — target genie schema for the 7 objects.
- Modify `app/databricks.yml` — `genie` schema resource + grants + `DQX_GENIE_SCHEMA` env.
- Modify the reader services/routes that resolve these object FQNs (`dq_results_service.py`, `score_cache_service.py`, `run_sets.py`, `table_data_service.py`, `genie_chat_service.py`, `routes/v1/dq_results.py`, `routes/v1/dq_score.py`, `routes/v1/genie.py`).
- Modify tests: `test_score_view_service.py`, `test_metadata_dim_service.py`, `test_entitlement_service.py`, `test_genie_space_service.py`, `test_config*.py`.

---

## Part A: Temp-view cleanup

### Task A1: Fix the orphan-view GC query

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/scheduler_service.py` (`_gc_orphan_views`, the `list_sql` string ~line 1950)
- Test: `app/tests/test_scheduler_service.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: no signature change. `_gc_orphan_views` still lists `tmp_view_*` candidates in `self._tmp_schema` older than `_GC_AGE_HOURS` and drops them.

- [ ] **Step 1: Write the failing test** — assert the emitted list SQL targets the correct catalog object + column. Add to the `TestGcOrphanViews` class:

```python
def test_list_query_targets_information_schema_tables_created(self, gc_scheduler):
    svc, mocks = gc_scheduler
    mocks.tmp.query.return_value = []
    svc._gc_orphan_views()
    list_sql = mocks.tmp.query.call_args_list[0].args[0]
    # Must query information_schema.tables (not .views — which has no created column)
    assert "information_schema.tables" in list_sql
    assert "table_type = 'VIEW'" in list_sql
    # The creation timestamp column is `created`, never `created_at`
    assert "created_at" not in list_sql
    assert "created <" in list_sql
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run pytest tests/test_scheduler_service.py::TestGcOrphanViews::test_list_query_targets_information_schema_tables_created -v`
Expected: FAIL — current SQL says `information_schema.views` and `created_at`.

- [ ] **Step 3: Fix the query.** Replace the `list_sql` assignment in `_gc_orphan_views`:

```python
        list_sql = (
            f"SELECT table_name "
            f"FROM `{self._catalog}`.information_schema.tables "
            f"WHERE table_schema = '{escape_sql_string(self._tmp_schema)}' "
            f"  AND table_type = 'VIEW' "
            f"  AND table_name LIKE 'tmp\\_view\\_%' ESCAPE '\\\\' "
            f"  AND created < current_timestamp() - INTERVAL {_GC_AGE_HOURS} HOUR "
            f"ORDER BY created ASC "
            f"LIMIT {_GC_MAX_DROPS_PER_RUN}"
        )
```

(Only `information_schema.views`→`.tables`, added `table_type = 'VIEW'`, `created_at`→`created` in both WHERE and ORDER BY. Everything else in the method is unchanged.)

- [ ] **Step 4: Run to verify it passes** (plus the existing `TestGcOrphanViews` cases still pass)

Run: `cd app && uv run pytest tests/test_scheduler_service.py::TestGcOrphanViews -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/scheduler_service.py app/tests/test_scheduler_service.py
git commit -m "fix(app): orphan-view GC queried a non-existent created_at column"
```

---

### Task A2: Drop-on-completion in the scheduler tick

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/scheduler_service.py` (`_collect_completed_score_run_fqns` ~line 1276, `_refresh_scores_for_completed_runs` ~line 1172; add `_drop_completed_run_views`)
- Test: `app/tests/test_scheduler_service.py`

**Interfaces:**
- Consumes: `self._tmp_sql` (SP tmp-schema executor, already used by the GC), `self._runs_table`, `self._pending_score_runs`, `_SQL_CHECK_PREFIX`, `quote_fqn`, `validate_fqn`.
- Produces:
  - `_collect_completed_score_run_fqns(now)` return type UNCHANGED (`set[str]` of source FQNs — the reconcile caller relies on it). It gains a **side effect**: it appends each completed run's `view_fqn` to a new instance list `self._completed_view_fqns_buffer: list[str]` (cleared at the start of each call), consumed immediately after by `_refresh_scores_for_completed_runs`.
  - New `_drop_completed_run_views(self, view_fqns: list[str]) -> None`.

- [ ] **Step 1: Write the failing tests.** Add a `TestDropCompletedRunViews` class:

```python
class TestDropCompletedRunViews:
    def test_drops_tmp_view_for_completed_run(self, gc_scheduler):
        svc, mocks = gc_scheduler
        svc._drop_completed_run_views(["dqx.dqx_studio_tmp.tmp_view_abc123def456"])
        executed = [c.args[0] for c in mocks.tmp.execute.call_args_list]
        assert any("DROP VIEW IF EXISTS" in s and "tmp_view_abc123def456" in s for s in executed)

    def test_skips_non_tmp_view_fqn(self, gc_scheduler):
        svc, mocks = gc_scheduler
        # Synthetic cross-table runs store the quoted SOURCE table as view_fqn.
        svc._drop_completed_run_views(["`dqx`.`sales`.`orders`"])
        mocks.tmp.execute.assert_not_called()

    def test_drop_failure_is_swallowed(self, gc_scheduler):
        svc, mocks = gc_scheduler
        mocks.tmp.execute.side_effect = RuntimeError("PERMISSION_DENIED")
        # Must not raise.
        svc._drop_completed_run_views(["dqx.dqx_studio_tmp.tmp_view_abc123def456"])
```

Add a test that `_collect_completed_score_run_fqns` buffers the completed run's `view_fqn` (extend the existing collect test fixture to return a 3-col row):

```python
    def test_collect_buffers_view_fqn_of_completed_runs(self, gc_scheduler):
        svc, mocks = gc_scheduler
        svc._pending_score_runs = {"r1": _some_past_dt(svc)}
        # runs-table query now returns (run_id, source_table_fqn, view_fqn)
        mocks.sql.query.return_value = [("r1", "dqx.sales.orders", "dqx.dqx_studio_tmp.tmp_view_r1aaaa")]
        fqns = svc._collect_completed_score_run_fqns(_now(svc))
        assert "dqx.sales.orders" in fqns
        assert "dqx.dqx_studio_tmp.tmp_view_r1aaaa" in svc._completed_view_fqns_buffer
```

(Use the existing helpers/fixtures in the file for `_now` / past datetimes; if none exist, construct `datetime.now(timezone.utc)` and a value older than `_SCORE_REFRESH_TTL` inline, matching how `test_scheduler_service.py` already builds times.)

- [ ] **Step 2: Run to verify they fail**

Run: `cd app && uv run pytest tests/test_scheduler_service.py::TestDropCompletedRunViews -v`
Expected: FAIL — `_drop_completed_run_views` and `_completed_view_fqns_buffer` don't exist; collect query selects 2 columns.

- [ ] **Step 3: Implement.**

(a) In `__init__`, initialise the buffer near the other run-tracking state (`self._pending_score_runs`):

```python
        # Views of runs observed terminal this tick, drained by
        # _refresh_scores_for_completed_runs into _drop_completed_run_views.
        self._completed_view_fqns_buffer: list[str] = []
```

(b) In `_collect_completed_score_run_fqns`, clear the buffer at the top and select+buffer `view_fqn`:

```python
        self._completed_view_fqns_buffer = []
        ...
        sql = (
            f"SELECT DISTINCT run_id, source_table_fqn, view_fqn FROM {self._runs_table} "  # noqa: S608
            f"WHERE run_id IN ({in_list}) AND UPPER(status) <> 'RUNNING'"
        )
        rows = self._sql.query(sql)

        fqns: set[str] = set()
        for row in rows:
            run_id = row[0] if row else None
            if not run_id:
                continue
            self._pending_score_runs.pop(run_id, None)
            fqn = row[1]
            if fqn and not fqn.startswith(_SQL_CHECK_PREFIX):
                fqns.add(fqn)
            view_fqn = row[2] if len(row) > 2 else None
            if isinstance(view_fqn, str) and view_fqn:
                self._completed_view_fqns_buffer.append(view_fqn)
        return fqns
```

(c) Add the drop helper:

```python
    def _drop_completed_run_views(self, view_fqns: list[str]) -> None:
        """Best-effort drop of temp views for runs observed terminal this tick.

        Runs as the SP against the tmp-schema executor. Only drops
        ``tmp_view_*`` FQNs (synthetic cross-table runs store the quoted
        source table here, which must never be dropped). Each drop is
        isolated: a failure — e.g. an OBO-owned view the SP lacks MANAGE on,
        already covered by the browser status-poll — is logged, not raised,
        so one bad drop can't wedge the tick or block score refresh.
        """
        from databricks_labs_dqx_app.backend.sql_utils import quote_fqn, validate_fqn

        for view_fqn in view_fqns:
            if "tmp_view_" not in view_fqn:
                continue
            try:
                validate_fqn(view_fqn)
            except Exception:
                logger.warning("Skipping drop of malformed completed-run view fqn: %s", view_fqn)
                continue
            try:
                self._tmp_sql.execute(f"DROP VIEW IF EXISTS {quote_fqn(view_fqn)}")
            except Exception as exc:
                logger.warning("Drop-on-completion failed for %s: %s", view_fqn, exc)
```

(d) In `_refresh_scores_for_completed_runs`, drain the buffer right after collecting (whether or not reconcile short-circuits — the views should be dropped in both branches). Change:

```python
        fqns = self._collect_completed_score_run_fqns(now)
        self._drop_completed_run_views(self._completed_view_fqns_buffer)

        if self._reconcile_due():
            self._reconcile_scores(fqns)
            return
        ...
```

- [ ] **Step 4: Run to verify they pass** (plus the whole scheduler suite)

Run: `cd app && uv run pytest tests/test_scheduler_service.py -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/scheduler_service.py app/tests/test_scheduler_service.py
git commit -m "feat(app): drop temp views on run completion via scheduler tick"
```

---

### Task A3: Fix the demo `sp_view` schema

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/dependencies.py` (`get_demo_seed_service`, ~line 949)
- Test: `app/tests/test_demo_seed_service.py`

**Interfaces:**
- Consumes: `conf.tmp_schema_name`, `SqlExecutor`, `sp_ws`, `warehouse_id`, `conf.catalog`.
- Produces: `sp_view` (the `BindingRunService`'s `ViewService`) now creates temp views in `dqx_studio_tmp`.

- [ ] **Step 1: Write the failing test.** In `test_demo_seed_service.py` (or a new `test_dependencies_demo_wiring.py` if the demo-seed test can't reach the factory), assert the binding-run ViewService targets the tmp schema. The simplest robust assertion is at the unit boundary of `ViewService`: construct it the way the fix will and assert the created view name contains the tmp schema. If the factory is hard to invoke in a unit test, add a focused test on `ViewService`:

```python
def test_view_service_uses_its_sql_executors_schema_for_view_name():
    from unittest.mock import create_autospec
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.view_service import ViewService, mark_tmp_schema_ready
    sql = create_autospec(SqlExecutor, instance=True)
    sql.catalog = "dqx"
    sql.schema = "dqx_studio_tmp"
    sp = create_autospec(SqlExecutor, instance=True)
    mark_tmp_schema_ready()  # skip the CREATE SCHEMA path
    svc = ViewService(sql=sql, sp_sql=sp)
    name = svc.create_view("dqx.sales.orders")
    assert ".dqx_studio_tmp.tmp_view_" in name
```

(This locks the invariant the fix depends on: `ViewService` names views from `self._sql.schema`, so pointing that executor at the tmp schema is what fixes the demo path. Reset `reset_tmp_schema_ready()` in a teardown if the module global leaks across tests — check existing `view_service` tests for the pattern.)

- [ ] **Step 2: Run to verify current behaviour.**

Run: `cd app && uv run pytest tests/test_demo_seed_service.py -v` (or the new test file)
Expected: the new ViewService invariant test PASSES already (it documents the mechanism). The demo bug itself is a wiring bug, fixed in Step 3; if a factory-level test is feasible it FAILS pre-fix. If only the invariant test is feasible, note in the commit that the wiring fix has no direct unit seam and is covered by the invariant + manual verification.

- [ ] **Step 3: Fix the wiring.** In `get_demo_seed_service`, replace:

```python
    sp_view = ViewService(sql=sp_sql, sp_sql=sp_sql)
```

with an executor bound to the tmp schema (mirroring the `profiler_view` block just below):

```python
    sp_view = ViewService(
        sql=SqlExecutor(
            ws=sp_ws,
            warehouse_id=warehouse_id,
            catalog=conf.catalog,
            schema=conf.tmp_schema_name,
        ),
        sp_sql=sp_sql,
    )
```

- [ ] **Step 4: Run tests.**

Run: `cd app && uv run pytest tests/test_demo_seed_service.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/dependencies.py app/tests/test_demo_seed_service.py
git commit -m "fix(app): demo binding runs created temp views in main schema, not tmp"
```

---

## Part B: Move Genie derived views/dims to a `genie` schema

### Task B1: Add `genie_schema_name` config + bundle schema

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/config.py` (~line 31, next to `tmp_schema_name`)
- Modify: `app/databricks.yml` (vars ~line 14-19; `resources.schemas` ~line 219; app env ~line 105-111)
- Test: `app/tests/test_config.py` (or the nearest existing config test — find with `grep -rln "tmp_schema_name" app/tests`)

**Interfaces:**
- Produces: `AppConfig.genie_schema_name: str` (default `"genie"`, env `DQX_GENIE_SCHEMA`).

- [ ] **Step 1: Write the failing test.**

```python
def test_genie_schema_name_default_and_env(monkeypatch):
    from databricks_labs_dqx_app.backend.config import AppConfig
    assert AppConfig().genie_schema_name == "genie"
    monkeypatch.setenv("DQX_GENIE_SCHEMA", "custom_genie")
    assert AppConfig().genie_schema_name == "custom_genie"
```

(Match the construction pattern the existing config tests use — if they instantiate `AppConfig` differently, mirror that. `monkeypatch` on a real env var is allowed here per AGENTS.md since it's the settings boundary.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run pytest tests/test_config.py -k genie_schema -v`
Expected: FAIL — attribute doesn't exist.

- [ ] **Step 3: Add the field** in `config.py` right after `tmp_schema_name`:

```python
    genie_schema_name: str = Field(default="genie", validation_alias="DQX_GENIE_SCHEMA")
```

- [ ] **Step 4: Add the bundle wiring** in `app/databricks.yml`:

Under `variables:` (near `tmp_schema_name`):
```yaml
  genie_schema_name:
    description: "Schema for Genie-facing derived views/dims"
    default: "genie"
```
Under the app resource `env:` list (near the `DQX_TMP_SCHEMA` entry):
```yaml
        - name: "DQX_GENIE_SCHEMA"
          value: "${var.genie_schema_name}"
```
Under `resources.schemas:` (mirror `main_schema`'s grants block exactly, substituting the name/comment):
```yaml
    genie_schema:
      catalog_name: ${var.catalog_name}
      name: ${var.genie_schema_name}
      comment: "DQX Studio Genie-facing derived views + dims"
      grants:
        # COPY the identical grants list from main_schema (app SP + task-runner SP + account users)
```

(Open `databricks.yml`, copy `main_schema.grants` verbatim into `genie_schema.grants`. Add `lifecycle: { prevent_destroy: true }` if `main_schema` has it.)

- [ ] **Step 5: Run test + validate bundle parses**

Run: `cd app && uv run pytest tests/test_config.py -k genie_schema -v` → PASS
Run: `cd app && databricks bundle validate -t dev -p fe-sandbox-dq-demo-2` → no schema-parse error (network permitting; if offline, at minimum `python -c "import yaml,sys; yaml.safe_load(open('databricks.yml'))"` succeeds).

- [ ] **Step 6: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/config.py app/databricks.yml app/tests/test_config.py
git commit -m "feat(app): add genie schema config + bundle resource"
```

---

### Task B2: `ScoreViewService` — genie schema for object names, main for base tables

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/score_view_service.py`
- Test: `app/tests/test_score_view_service.py`

**Interfaces:**
- Consumes: `AppConfig.genie_schema_name` (passed in by the constructor caller).
- Produces:
  - `ScoreViewService.__init__(self, sql: SqlExecutor, genie_schema: str)` — `sql` stays the main-schema executor (runs DDL + builds base-table FQNs); `genie_schema` names the 4 views.
  - The 4 `*_view_fqn_quoted` properties resolve in the `genie` schema.
  - Base-table refs inside DDL (`dq_validation_runs`, `dq_metrics`) resolve in `sql.schema` (main).
  - Module helper `metric_view_fqn(catalog, schema)` unchanged in signature (callers pass the genie schema).

- [ ] **Step 1: Write the failing test.**

```python
def test_view_names_use_genie_schema_but_base_tables_use_main():
    from unittest.mock import create_autospec
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.score_view_service import ScoreViewService
    sql = create_autospec(SqlExecutor, instance=True)
    sql.catalog = "dqx"; sql.schema = "dqx_studio"
    sql.q = lambda s: f"`{s}`"
    svc = ScoreViewService(sql=sql, genie_schema="genie")
    assert "`genie`.`mv_dq_scores`" in svc.metric_view_fqn_quoted
    ddl = svc.attribution_view_ddl()
    # view name in genie, base table in main
    assert "`genie`.`v_dq_check_attribution`" in ddl
    assert "`dqx_studio`.dq_validation_runs" in ddl or "`dqx_studio`.`dq_validation_runs`" in ddl
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run pytest tests/test_score_view_service.py -k genie -v`
Expected: FAIL — constructor takes no `genie_schema`; views resolve in `dqx_studio`.

- [ ] **Step 3: Implement.** Update `__init__` and the fqn properties:

```python
    def __init__(self, sql: SqlExecutor, genie_schema: str) -> None:
        self._sql = sql
        self._catalog_q = sql.q(sql.catalog)
        self._schema_q = sql.q(sql.schema)            # main schema — base tables
        self._genie_schema_q = sql.q(genie_schema)    # genie schema — derived views
```

Change the 4 view-fqn properties to use `self._genie_schema_q` instead of `self._schema_q` (e.g. `f"{self._catalog_q}.{self._genie_schema_q}.{ATTRIBUTION_VIEW_NAME}"`). Leave the base-table FQNs (`validation_runs = f"{self._catalog_q}.{self._schema_q}.dq_validation_runs"`, `metrics_table = f"...{self._schema_q}.dq_metrics"`) on `self._schema_q`.

- [ ] **Step 4: Run test + full service suite**

Run: `cd app && uv run pytest tests/test_score_view_service.py -v`
Expected: PASS (update any existing tests that asserted `dqx_studio.mv_dq_scores` to expect `genie.mv_dq_scores`).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/score_view_service.py app/tests/test_score_view_service.py
git commit -m "feat(app): score views resolve in genie schema, base tables in main"
```

---

### Task B3: `MetadataDimService` + `EntitlementService` — genie schema

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/metadata_dim_service.py`
- Modify: `app/src/databricks_labs_dqx_app/backend/services/entitlement_service.py`
- Test: `app/tests/test_metadata_dim_service.py`, `app/tests/test_entitlement_service.py`

**Interfaces:**
- Produces:
  - `MetadataDimService.__init__(self, sp_sql, registry, monitored_tables, genie_schema: str)` — dims created in `genie`; any base-table reads use `sp_sql.schema` (main).
  - `EntitlementService.__init__(self, sql, genie_schema: str)` — `v_dq_failing_rows` created in `genie`; reads `dq_quarantine_records` + `dq_user_table_entitlements` from `sql.schema` (main). The entitlements TABLE (`dq_user_table_entitlements`) STAYS in main schema.

- [ ] **Step 1: Write failing tests.**

`test_metadata_dim_service.py`:
```python
def test_dims_created_in_genie_schema():
    from unittest.mock import create_autospec
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.metadata_dim_service import MetadataDimService
    from databricks_labs_dqx_app.backend.services.registry_service import RegistryService
    from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
    sp = create_autospec(SqlExecutor, instance=True); sp.catalog="dqx"; sp.schema="dqx_studio"
    reg = create_autospec(RegistryService, instance=True); reg.list_rules.return_value=[]
    mt = create_autospec(MonitoredTableService, instance=True); mt.list_monitored_tables.return_value=[]
    svc = MetadataDimService(sp_sql=sp, registry=reg, monitored_tables=mt, genie_schema="genie")
    svc.refresh()
    created = " ".join(c.args[0] for c in sp.execute.call_args_list)
    assert "genie" in created and "dim_dq_rules" in created
```

`test_entitlement_service.py`:
```python
def test_failing_rows_view_in_genie_reads_quarantine_from_main():
    from unittest.mock import create_autospec
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.entitlement_service import EntitlementService
    sql = create_autospec(SqlExecutor, instance=True); sql.catalog="dqx"; sql.schema="dqx_studio"
    sql.q = lambda s: f"`{s}`"
    svc = EntitlementService(sql=sql, genie_schema="genie")
    ddl = svc.failing_rows_view_ddl()  # use the actual DDL accessor name in the file
    assert "genie" in ddl and "v_dq_failing_rows" in ddl
    assert "dq_quarantine_records" in ddl  # base table stays in main
```

(Adjust accessor/method names to match the file — read the service first to find the DDL method + how `dim_*` FQNs are built.)

- [ ] **Step 2: Run to verify they fail**

Run: `cd app && uv run pytest tests/test_metadata_dim_service.py tests/test_entitlement_service.py -k "genie" -v`
Expected: FAIL — constructors take no `genie_schema`.

- [ ] **Step 3: Implement.** `MetadataDimService`: add `genie_schema` param; set `self._genie_schema = genie_schema`; change the two `quote_object_fqn(self._catalog, self._schema, DIM_*)` calls to use `self._genie_schema`; keep any base-table reads on `self._schema`. `EntitlementService`: add `genie_schema`; the `v_dq_failing_rows` object FQN uses genie; the `dq_quarantine_records` / `dq_user_table_entitlements` references stay on the main schema (`sql.schema`). Read each file to place edits precisely.

- [ ] **Step 4: Run tests**

Run: `cd app && uv run pytest tests/test_metadata_dim_service.py tests/test_entitlement_service.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/metadata_dim_service.py app/src/databricks_labs_dqx_app/backend/services/entitlement_service.py app/tests/test_metadata_dim_service.py app/tests/test_entitlement_service.py
git commit -m "feat(app): dims + failing-rows view resolve in genie schema"
```

---

### Task B4: Genie space payload + all readers use the genie schema

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/genie_space_service.py` (callers already take `catalog, schema` — pass genie schema)
- Modify: `app/src/databricks_labs_dqx_app/backend/app.py` (`_ensure_score_views`, `_ensure_metadata_dims`, `_ensure_entitlement_view`, `_ensure_genie_space`, lifespan: ensure genie schema; wire `conf.genie_schema_name` into the service constructors + `ensure_dq_genie_space(schema=...)`)
- Modify: `app/src/databricks_labs_dqx_app/backend/dependencies.py` (any place that builds `ScoreViewService` / `MetadataDimService` / `EntitlementService` — pass `conf.genie_schema_name`)
- Modify reader services/routes that resolve these object FQNs: `services/dq_results_service.py`, `services/score_cache_service.py`, `services/run_sets.py`, `services/table_data_service.py`, `services/genie_chat_service.py`, `routes/v1/dq_results.py`, `routes/v1/dq_score.py`, `routes/v1/genie.py`
- Test: `app/tests/test_genie_space_service.py`

**Interfaces:**
- Consumes: `conf.genie_schema_name`, the B2/B3 constructor signatures.
- Produces: all 7 objects created + read in `genie`; base tables still read in `dqx_studio`.

- [ ] **Step 1: Write the failing test.** In `test_genie_space_service.py`, assert the built payload references the genie schema:

```python
def test_serialized_space_uses_genie_schema():
    from databricks_labs_dqx_app.backend.services.genie_space_service import build_serialized_space
    space = build_serialized_space("dqx", "genie")
    blob = str(space)
    assert "dqx.genie.mv_dq_scores" in blob
    assert "dqx.genie.dim_dq_rules" in blob
```

- [ ] **Step 2: Run to verify it fails / passes.** Since `build_serialized_space(catalog, schema)` already parameterises the schema, this test PASSES once callers pass `genie`. The real work is repointing callers. Run:

Run: `cd app && uv run pytest tests/test_genie_space_service.py::test_serialized_space_uses_genie_schema -v`
Expected: PASS (the function is already schema-parameterised) — this test guards against a regression where a caller hardcodes the main schema.

- [ ] **Step 3: Repoint callers.**
  (a) `app.py` lifespan: before ensuring the genie objects, ensure the genie schema exists (mirror the tmp-schema `CREATE SCHEMA IF NOT EXISTS` block, using `conf.genie_schema_name`):
```python
    try:
        gen_sch = conf.genie_schema_name.replace("`", "")
        sp_sql.execute_no_schema(f"CREATE SCHEMA IF NOT EXISTS `{conf.catalog}`.`{gen_sch}`")
    except Exception as gen_e:
        logger.warning("Could not create genie schema %s.%s: %s", conf.catalog, conf.genie_schema_name, gen_e)
```
  (b) `_ensure_score_views(sp_sql)` → `ScoreViewService(sql=sp_sql, genie_schema=conf.genie_schema_name)`.
  (c) `_ensure_metadata_dims(...)` → `MetadataDimService(..., genie_schema=conf.genie_schema_name)`.
  (d) `_ensure_entitlement_view(...)` → `EntitlementService(sql=sp_sql, genie_schema=conf.genie_schema_name)`.
  (e) `_ensure_genie_space(...)` → `ensure_dq_genie_space(..., schema=conf.genie_schema_name)`.
  (f) `dependencies.py`: every `ScoreViewService(...)`, `MetadataDimService(...)`, `EntitlementService(...)` construction gets `genie_schema=conf.genie_schema_name`.
  (g) Reader services/routes: anywhere an FQN for one of the 7 objects is built from `conf.schema_name` (or a main-schema executor's `.schema`), switch to `conf.genie_schema_name`. Grep each file for the object constants (`METRIC_VIEW_NAME`, `SHAPING_VIEW_NAME`, `ASOF_VIEW_NAME`, `ATTRIBUTION_VIEW_NAME`, `FAILING_ROWS_VIEW_NAME`, `DIM_RULES_TABLE_NAME`, `DIM_MONITORED_TABLES_TABLE_NAME`, `metric_view_fqn`) and repoint the schema argument. Base-table FQNs (`dq_validation_runs`, `dq_metrics`, `dq_quarantine_records`, `dq_profiling_results`, `dq_score_cache`) stay on the main schema.

  For each reader, add/extend a test that asserts it resolves the genie object in the genie schema (mirror the assertion style already in that file). If a reader has no unit seam, note it and rely on the deploy smoke-test.

- [ ] **Step 4: Run the full backend suite**

Run: `cd app && uv run pytest -q`
Expected: PASS. Fix any test that hardcoded `dqx_studio.<genie object>` to expect `genie.<object>`.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend app/tests
git commit -m "feat(app): route Genie space + readers to the genie schema"
```

---

### Task B5: Reset service targets the genie schema

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/database_reset_service.py`
- Test: `app/tests/test_database_reset_service.py` (find with `grep -rln reset app/tests`)

**Interfaces:**
- Consumes: `conf.genie_schema_name` (wire via the reset-service constructor / its caller in `dependencies.py`).
- Produces: reset drops/recreates the 7 genie objects in the `genie` schema (not `dqx_studio`).

- [ ] **Step 1: Read** `database_reset_service.py` to see how it references the derived objects (it currently DROPs/recreates by main-schema name, or delegates to the ensure functions). If it only DELETEs base-table rows and delegates genie-object recreation to `genie_reprovision` / the ensure functions, then B4 already covers it — in that case add a test asserting reset does NOT reference `dqx_studio.<genie object>` and STOP (no code change). Otherwise proceed.

- [ ] **Step 2: Write the failing test** asserting any genie-object DROP/DDL uses the genie schema.

```python
def test_reset_targets_genie_schema_for_derived_objects(...):
    # arrange reset service with genie_schema="genie", capture executed SQL,
    # assert genie-object statements reference `genie`, not `dqx_studio`
```

(Match the file's existing test fixtures.)

- [ ] **Step 3: Run to verify it fails.**

Run: `cd app && uv run pytest tests/test_database_reset_service.py -k genie -v`
Expected: FAIL (if reset references the objects) — else this task is a no-op documented in Step 1.

- [ ] **Step 4: Implement** — thread `genie_schema` through and use it for the 7 objects; base-table DELETEs stay on main.

- [ ] **Step 5: Run tests**

Run: `cd app && uv run pytest tests/test_database_reset_service.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/database_reset_service.py app/tests/test_database_reset_service.py
git commit -m "feat(app): database reset targets genie schema for derived objects"
```

---

## Final verification (controller runs after all tasks)

- [ ] `cd app && uv run pytest -q` — full backend suite green.
- [ ] `make app-check` — clean (except the 5 pre-existing `monitored_table_service.py:1062` errors).
- [ ] Build wheel with `.cloud` pypi-proxy fallback env vars; `make app-deploy PROFILE=fe-sandbox-dq-demo-2 TARGET=dev`; app RUNNING.
- [ ] One-off cleanup via warehouse `43a1704b9dfa9ebc`: `DROP VIEW IF EXISTS` the ~17 existing `tmp_view_*` in both `dqx_studio` and `dqx_studio_tmp`; `DROP` the stale 7 Genie objects from `dqx_studio` (they've been recreated in `genie` by startup).
- [ ] Report to user for verification. Do NOT squash-merge / push until told.
