# Collections list speed: `applied_check_count` on summary + lazy runs fetch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make the Collections overview list load as fast as the Tables overview by reading each approved member's frozen-snapshot check count from a LEFT-JOIN field (`MonitoredTableSummary.applied_check_count`) instead of rendering every member's rules; render only never-approved drafts. Plus stop the run-failure-toast hook fetching `dryrun/runs` + `profiler/runs` on every navigation.

**Architecture:** Backend adds one summary field populated by extending the existing `list_monitored_tables` query with a LEFT JOIN to `dq_monitored_table_versions` (frozen `state_json.check_count`, parsed in Python — mirroring `snapshot_counts_many`). `DataProductService.list_products` reads it for approved members and renders only drafts. Frontend pins two run-list queries to a long staleTime. No new column/migration/write-path/fan-out.

**Tech Stack:** Python 3.12 / FastAPI / SqlExecutor (Lakebase PG + Delta fallback), React 19 + TanStack Query. Full spec: `docs/superpowers/specs/2026-07-22-collections-check-count-denorm-design.md`.

## Global Constraints

- Backend type hints on every param/return; `str | None`; no `Any`. `make app-check` must stay clean modulo the 5 KNOWN pre-existing errors (`monitored_table_service.py:1062` x2, `apply_rules_service.py:73` x2, `seed_demo.py:264` x1) — do not touch those files' unrelated lines.
- **VALUES MUST STAY BYTE-IDENTICAL** for approved / draft / pinned members. This is a performance change only. Every existing count test (item-44, pinned, monitored-tables overview) must pass unchanged.
- Portable SQL: JSON is extracted by selecting the whole `state_json` as text via the executor's `select_json_text(...)` helper (exists on both SqlExecutor Delta + PgExecutor) and parsing `check_count` in Python — NEVER an in-SQL `::jsonb`/`get_json_object`. Mirror `MonitoredTableVersions.snapshot_counts_many` exactly.
- Reuse batched helpers; never a per-member round-trip.
- Backend tests live in `app/tests/`; run one file: `cd app && uv run pytest tests/<file>.py -v`.

## Key facts (verified against current code)

- `MonitoredTableSummary` (dataclass in `services/monitored_table_service.py`) has `applied_rule_count: int = 0` and `check_count: int = 0`. We ADD `applied_check_count: int | None = None`.
- `list_monitored_tables` builds `sql = SELECT {_build_select_cols('mt.')}, sc.score, sc.failed_tests, sc.total_tests, {ts} AS score_computed_at FROM {self._table} mt LEFT JOIN {self._score_cache_table} sc ON ...`. Base cols are row[0..13] (schedule_kind=13); score-cache cols are row[14..17]. `version` is `mt.version` = row[4]. We add ONE more LEFT JOIN + ONE more selected column (`state_json` text) → row[18].
- `self._versions_table = sql.fqn("dq_monitored_table_versions")` already exists on the service.
- `snapshot_counts_many` parses: `state = json.loads(text); check_count = state.get("check_count"); fallback = len(state.get("rule_refs")) if list`. We copy that parse for the join column.
- `DataProductService._member_counts` returns `(rules_count, checks_count)`; unpinned currently does `live_check_counts.get(binding_id, summary.check_count)`. `_live_check_counts` renders ALL unpinned members. `_pinned_snapshot_counts` handles pinned. `summary.table.version` gives approval state (`> 0` = approved).
- Frontend runs hook: `ui/hooks/use-run-failure-toasts.ts` calls `useListValidationRuns({query:{refetchInterval}})` + `useListProfileRuns(undefined,{query:{refetchInterval}})`. No staleTime set → rides the 5-min global default, refetches per mount after it lapses.

---

## Task 1: Add `applied_check_count` to the summary via a version-snapshot LEFT JOIN

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/monitored_table_service.py` (`MonitoredTableSummary` dataclass; `list_monitored_tables` query + row mapping ~lines 336-369)
- Test: `app/tests/test_monitored_table_service.py`

**Interfaces:**
- Produces: `MonitoredTableSummary.applied_check_count: int | None` — the approved binding's frozen-snapshot `check_count` (non-NULL when `version > 0` and a snapshot row exists), else `None`.

- [ ] **Step 1: Write the failing test.** In `test_monitored_table_service.py`, add a test that the list query LEFT-JOINs the versions table and that an approved binding's `applied_check_count` is populated from the snapshot while a draft's is None. FIRST read the file's existing `list_monitored_tables` test + its executor mock (it likely stubs `.query` to return rows and `.ts_text`/`.q`/`.select_json_text`/`.fqn`). Mirror that. Example shape (ADAPT to the real mock/fixtures):

```python
def test_list_includes_applied_check_count_from_version_snapshot(<existing fixtures>):
    # Arrange: mock the executor .query to return one binding row (version=3)
    # with the joined state_json text column carrying {"check_count": 7, "rule_refs":[...]}.
    # Assert: the emitted SELECT references dq_monitored_table_versions and select_json_text("...state_json...");
    #         the returned summary.applied_check_count == 7.
    ...
    summaries = svc.list_monitored_tables()
    assert "dq_monitored_table_versions" in captured_sql
    assert summaries[0].applied_check_count == 7
```
Also assert a draft binding (version=0, no snapshot row → joined text NULL/None) yields `applied_check_count is None`.

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run pytest tests/test_monitored_table_service.py -k applied_check_count -v`
Expected: FAIL (field/JOIN absent).

- [ ] **Step 3: Implement.**
(a) Add to the dataclass (after `applied_rule_count`):
```python
    applied_check_count: int | None = None
```
(b) In `list_monitored_tables`, extend the query. After the score-cache LEFT JOIN, add a versions LEFT JOIN on the binding's CURRENT version and select its state_json text as the last column:
```python
        state_json_text = self._sql.select_json_text("v.state_json")
        sql = (
            f"SELECT {self._build_select_cols('mt.')}, "
            f"sc.score, sc.failed_tests, sc.total_tests, {score_computed_at} AS score_computed_at, "
            f"{state_json_text} AS version_state_json "
            f"FROM {self._table} mt "
            f"LEFT JOIN {self._score_cache_table} sc "
            f"ON sc.scope_type = 'table' AND sc.scope_key = mt.table_fqn "
            f"LEFT JOIN {self._versions_table} v "
            f"ON v.binding_id = mt.binding_id AND v.version = mt.version"
        )
```
(Keep the existing `WHERE`/`ORDER BY`/`LIMIT` appended after — confirm they still attach to this string. `select_json_text` takes a column ref; confirm it accepts a table-qualified `v.state_json` — read its impl; if it only quotes a bare name, pass the form it expects and qualify in the SELECT accordingly.)
(c) In the row mapping, parse the new last column (row[18]) into `applied_check_count`. Add a small helper mirroring `snapshot_counts_many`'s parse:
```python
        # row[18] = joined frozen-version state_json text (NULL for a draft binding
        # with no snapshot). Parse check_count the SAME way snapshot_counts_many does.
        tables = [
            (self._row_to_table(row), parse_cached_score(row[14], row[15], row[16], row[17]), _parse_snapshot_check_count(row[18]))
            for row in rows
        ]
```
where `_parse_snapshot_check_count(text)` returns `int | None`:
```python
def _parse_snapshot_check_count(state_text: object) -> int | None:
    if not isinstance(state_text, str) or not state_text:
        return None
    try:
        state = json.loads(state_text)
    except (ValueError, TypeError):
        return None
    cc = state.get("check_count")
    if isinstance(cc, int):
        return cc
    refs = state.get("rule_refs")
    return len(refs) if isinstance(refs, list) else None
```
(Place it as a module-level helper near `parse_cached_score`; `import json` if not already imported. Match the EXACT `check_count`/`rule_refs` fallback of `snapshot_counts_many` so values agree.)
(d) Thread the parsed value into the `MonitoredTableSummary(...)` construction (the filters over `tables` unpack 2-tuples today — update them to 3-tuples, or restructure so the third element rides along; keep the catalog/schema/name Python filters working):
```python
                applied_check_count=snap_cc,
```

- [ ] **Step 4: Run to verify it passes** (+ the whole file)

Run: `cd app && uv run pytest tests/test_monitored_table_service.py -v`
Expected: PASS. Update any existing `list_monitored_tables` test that asserted an exact SELECT string to include the new join/column.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git add app/src/databricks_labs_dqx_app/backend/services/monitored_table_service.py app/tests/test_monitored_table_service.py
git commit -m "feat(app): add applied_check_count to monitored-table summary via version-snapshot join"
```

---

## Task 2: Collections list reads `applied_check_count`, renders only drafts

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/data_product_service.py` (`_live_check_counts`, `_member_counts`, and `list_products`'s call to them)
- Test: `app/tests/test_data_products.py`

**Interfaces:**
- Consumes: `MonitoredTableSummary.applied_check_count` (Task 1); `summary.table.version` for approval state.
- Produces: `list_products` passes ONLY never-approved unpinned members (`version == 0`) to `render_binding_checks_counts_many`; approved unpinned members get their count from `applied_check_count`; pinned unchanged.

- [ ] **Step 1: Write the failing test.** In `test_data_products.py`, add a test asserting the render is called with only draft members and approved members use `applied_check_count`. Read the existing list_products tests + how they mock the materializer/monitored-tables first; mirror. Shape:

```python
def test_list_renders_only_draft_members_uses_snapshot_for_approved(<fixtures>):
    # Two unpinned members: A approved (version=2, applied_check_count=9),
    # B draft (version=0, applied_check_count=None). Mock materializer.render_binding_checks_counts_many
    # to record its input and return {B_binding: 4}.
    products = svc.list_products()
    # render called with ONLY the draft binding
    assert render_mock.call_args.args[0] == [(B_binding, B_fqn)]  # or the recorded live_bindings
    # approved member A's checks_count == 9 (from applied_check_count), NOT rendered
    # draft member B's checks_count == 4 (rendered)
```
(Adapt binding shapes to the real `MonitoredTableSummary`/`_MemberRow` fixtures. Also keep/verify an existing pinned-member test still passes.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd app && uv run pytest tests/test_data_products.py -k renders_only_draft -v`
Expected: FAIL (today renders all unpinned members).

- [ ] **Step 3: Implement.**
(a) `_live_check_counts`: restrict `live_bindings` to never-approved members. Add the version gate to the existing skip logic:
```python
        for row in member_rows:
            if row.pinned_version is not None and pinned_counts.get((row.binding_id, row.pinned_version)) is not None:
                continue
            if row.binding_id in seen:
                continue
            summary = table_map.get(row.binding_id)
            if summary is None:
                continue
            # Approved bindings (version > 0) read their frozen snapshot count from
            # the summary (applied_check_count) — no render. Only never-approved
            # drafts (version == 0) need a live render (item 44). Mirrors the Tables
            # overview's _apply_snapshot_check_counts split.
            if summary.table.version > 0:
                continue
            seen.add(row.binding_id)
            live_bindings.append((row.binding_id, summary.table.table_fqn))
```
(b) `_member_counts`: for an unpinned member, prefer the rendered count (drafts) then `applied_check_count` (approved) then the old `summary.check_count` fallback:
```python
        # unpinned:
        rendered = live_check_counts.get(summary.table.binding_id)
        if rendered is not None:
            checks_count = rendered
        elif summary.applied_check_count is not None:
            checks_count = summary.applied_check_count
        else:
            checks_count = summary.check_count
        return summary.applied_rule_count, checks_count
```
(Keep the pinned branch above it exactly as-is.)

- [ ] **Step 4: Run to verify it passes** (+ whole file)

Run: `cd app && uv run pytest tests/test_data_products.py -v`
Expected: PASS. Existing item-44 draft test still passes (draft still renders); pinned test unchanged.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/data_product_service.py app/tests/test_data_products.py
git commit -m "perf(app): Collections list reads snapshot check-count for approved members, renders only drafts"
```

---

## Task 3: Lazy runs fetch in the run-failure-toast hook

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/hooks/use-run-failure-toasts.ts`
- Test: `app/src/databricks_labs_dqx_app/ui/hooks/` — add/extend a hook test if one exists; else a focused test on the query options.

**Interfaces:**
- Produces: both run-list queries pinned to a long `staleTime` so they're served from the shared React Query cache across navigations instead of refetching per mount.

- [ ] **Step 1: Write/adjust the test.** If a test for this hook exists, extend it; otherwise the change is small and asserted via the query options. Search first: `grep -rl "use-run-failure-toasts\|useRunFailureToasts" app/src/databricks_labs_dqx_app/ui/**/*.test.* 2>/dev/null`. If a test harness for hooks exists in the repo, add:
```ts
// assert both list queries are configured with a long staleTime (served from cache across mounts)
```
If NO hook-test harness exists (likely — this repo's bun tests are mostly pure-function), SKIP a bespoke test and rely on `bun tsc -b` + manual verification; note this in the report. Do NOT stand up a new React-testing harness for this.

- [ ] **Step 2: Implement.** In `use-run-failure-toasts.ts`, add a long `staleTime` to both query option objects (keep the existing `refetchInterval`):
```ts
  const STALE = 5 * 60 * 1000; // 5 min — served from shared cache across navigations; polling still fires while a run is RUNNING
  const { data: validationResp } = useListValidationRuns({
    query: { refetchInterval, staleTime: STALE },
  });
  const { data: profileResp } = useListProfileRuns(undefined, {
    query: { refetchInterval, staleTime: STALE },
  });
```
(If the global default already provides 5 min, use a longer value — e.g. `30 * 60 * 1000` — so the hook genuinely stops refetching on every navigation within a session. Pick the value that makes the per-navigation refetch stop while keeping the RUNNING poll intact — the `refetchInterval` gate is unchanged and still drives live polling.)

- [ ] **Step 3: Type-check.**

Run: `cd app && bun tsc -b`
Expected: clean (no new errors).

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/hooks/use-run-failure-toasts.ts
git commit -m "perf(app): serve run-failure-toast run lists from cache, stop per-navigation refetch"
```

---

## Final verification (controller, after all tasks)

- [ ] `cd app && uv run pytest -q` — backend green (modulo the 7 known pre-existing failures in test_export_routes / test_lint_policy / test_tag_auto_suppressions_schema — confirm no NEW failures).
- [ ] `make app-check` — clean modulo the 5 known pre-existing errors.
- [ ] Build wheel with `.cloud` pypi-proxy fallback; `make app-deploy PROFILE=fe-sandbox-dq-demo-2 TARGET=dev`; app RUNNING.
- [ ] Verify on fe-sandbox: Collections list paints as fast as Tables; `# Checks` values unchanged for the v1 collections; `dryrun/runs`+`profiler/runs` no longer fire on every navigation.
- [ ] Report to user for verification. Do NOT squash-merge / push until told.
