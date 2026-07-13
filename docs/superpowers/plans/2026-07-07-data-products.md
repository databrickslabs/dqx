# Data Products Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Versioned monitored tables with frozen-per-version rule snapshots, runnable (approved
version or draft) individually and in dqlake-style "Data Products" groupings with GUIDs,
publish-bumped versions, per-product scheduling, and run-set tracking — with the job runner and
`dq_quality_rules` spec provably unchanged.

**Architecture:** Approval bumps `dq_monitored_tables.version` and freezes the approved checks
into `dq_monitored_table_versions.checks_json`; runs resolve a frozen snapshot (or a read-only
draft render) and feed it to the EXISTING per-table `JobService.submit_run` path. New
`dq_data_products`/`dq_data_product_members` model the grouping; every trigger mints a
`dq_run_sets` row. The in-app scheduler gains product ticks. UI is a close port of dqlake's
`/products` surfaces.

**Tech Stack:** FastAPI + Pydantic 2 backend, appended Delta/PG migrations, React 19 +
TanStack Router/Query + shadcn + orval hooks, i18next (4 locales), dqlake source at
`/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch/src/dqlake/`.

## Global Constraints

- `dq_quality_rules` table spec UNCHANGED. `app/tasks/`, `backend/services/job_service.py`,
  and the `jobs.run_now(DQX_JOB_ID, job_parameters={...config_json...})` contract UNCHANGED.
  Proof gate: `git diff <base>.. -- app/tasks/ '*job_service*'` empty.
- `dq_validation_runs` schema + `GET /api/v1/dryrun/runs` contract UNCHANGED (run sets are
  separate tables).
- Migrations: APPEND new versions (PG v6+ / Delta v10+); never edit shipped versions.
- Worktree: `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/rules-registry`;
  never touch `main`. Commits `--no-gpg-sign`, message ends `Co-authored-by: Isaac`.
- Every user-facing string via `t()` in en, pt-BR, it, es (real translations; en = source of
  truth; plurals `_one`/`_other`). No lint suppressions. Generated `ui/lib/api.ts` /
  `routeTree.gen.ts` via `make app-regen-api` only.
- Verify per task: `make app-stop-dev` first, `make app-check` (0 errors), `make app-test`
  (full backend; baseline 1210 at plan time), `make app-test K=i18n`. UI tasks: browser-verify
  on local dev (`make app-start-dev`, :9001) — this machine reaches <your-profile>.
- If lockfiles churn with internal-proxy URLs: `git checkout --` them.
- Copy discipline: plain declarative product language; no exclamation marks, no AI-magic
  speak.
- dqlake ports: COPY the named dqlake file and adapt ONLY imports/data hooks/i18n — do not
  re-approximate layout/classes (hard-learned Phase-11 lesson).
- Spec (binding requirements): `docs/superpowers/specs/2026-07-07-data-products-design.md`.

---

### Task 0: Adopt & finish the P17-C WIP (UX fixes + copy)

**Files:**
- Modify (WIP already in tree, UNCOMMITTED, adopt it): `ui/components/apply-rules/{MappingChips,RuleConfigCard,AiSuggestionDialog,RulesPicker}.tsx`, `ui/components/{RegistryRuleBadges,RegistryRuleFormDialog}.tsx`, `ui/components/rules/RulesTable.tsx`, `ui/lib/i18n/locales/*.json`
  (all under `app/src/databricks_labs_dqx_app/`)
- Reference brief with full requirements: `/private/tmp/claude-502/-Users-oliver-gordon-Documents-Code-Other-dqx/af2854b5-244b-4db5-9fa0-0de8874c54de/scratchpad/task-p17c-brief.md`

**Interfaces:** none consumed downstream; purely UX.

- [ ] Step 1: `git diff` the uncommitted WIP; judge it against the brief. Requirements: (1) "+ apply
  to another column" = one-click — pressing it opens the column picker directly and clicking a
  column adds the mapping group (multi-slot rules: slot-by-slot auto-opening chips, no modal,
  no confirm button — the WIP's `PendingSlotChip` is this approach, half-done); (2) unset
  severity renders localized "None", never an empty colour dot (all severity render sites);
  (3) align FE `canSaveDraft` SQL-keyword list in `RegistryRuleFormDialog.tsx` with backend
  `is_sql_query_safe()` (`src/databricks/labs/dqx/utils.py`) — mirror verbatim + comment
  naming the source of truth; (4) lowcode disabled-Save tooltip; (5) generate-checks copy
  de-cringe ("AI-Assisted Rules Generation", "Checks generated successfully!", "AI Rules
  Assistant", "AI-powered rule generation" → plain language, 4 locales).
- [ ] Step 2: finish/fix each item; keep edits STAGED-local (no network on mapping edits).
- [ ] Step 3: verify (global constraints) + browser-verify items 1–2 on local dev.
- [ ] Step 4: commit.

### Task 1: Migrations + domain models

**Files:**
- Modify: `backend/migrations/postgres.py` (append v6), `backend/migrations/__init__.py`
  (append Delta v10, `oltp_fallback=True` for OLTP tables; run-set tables are OLTP too),
  `backend/registry_models.py`, `backend/models.py`
- Test: `app/tests/test_pg_migration_runner.py`, `app/tests/test_migration_runner.py`,
  `app/tests/test_monitored_tables_schema.py` (follow 00c3e01's appended-version test pattern)

**Interfaces (produces — later tasks rely on these exact names):**
- `dq_monitored_tables` += `version INT NOT NULL DEFAULT 0`
- `dq_monitored_table_versions(id TEXT PK, binding_id TEXT, version INT, checks_json JSON,
  state_json JSON, created_by TEXT, created_at TS, refrozen_at TS NULL,
  UNIQUE(binding_id, version))`
- `dq_data_products(product_id TEXT PK, name TEXT UNIQUE, description TEXT NULL, steward TEXT,
  schedule_cron TEXT NULL, schedule_tz TEXT NULL, status TEXT CHECK draft|published,
  version INT NOT NULL DEFAULT 0, created_by/created_at/updated_by/updated_at)`
- `dq_data_product_members(id TEXT PK, product_id TEXT, binding_id TEXT,
  pinned_version INT NULL, UNIQUE(product_id, binding_id))`
- `dq_run_sets(run_set_id TEXT PK, product_id TEXT NULL, product_version INT NULL,
  source TEXT CHECK approved|draft, trigger TEXT CHECK manual|scheduled, created_by TEXT,
  created_at TS)`
- `dq_run_set_members(id TEXT PK, run_set_id TEXT, run_id TEXT, binding_id TEXT,
  binding_version INT NULL)`
- Domain models (registry_models.py): `MonitoredTable.version: int = 0`;
  `MonitoredTableVersion`, `DataProduct`, `DataProductMember`, `RunSet`, `RunSetMember`
  dataclass-style models mirroring the columns; `DataProductStatus = Literal["draft","published"]`.

Steps: failing schema tests (table/column/CHECK presence, appended-past-baseline, oltp_fallback
flags) → implement → suite green → commit.

### Task 2: Version freeze service + approve integration + draft render

**Files:**
- Create: `backend/services/monitored_table_versions.py`
- Modify: `backend/services/materializer.py` (ADD a read-only public method ONLY — no change
  to persisted-materialization behavior; parity tests must pass untouched),
  `backend/routes/v1/monitored_tables.py` (approve route + new versions endpoint),
  `backend/services/registry_service.py` OR the rules approve/reject route (re-freeze hooks —
  find where per-rule status changes + auto-upgrade land and hook AFTER them)
- Test: create `app/tests/test_monitored_table_versions.py`; extend
  `app/tests/test_monitored_tables_routes.py`

**Interfaces:**
- Consumes: Task 1 tables/models.
- Produces:
  - `class MonitoredTableVersionService:`
    - `freeze_new_version(binding_id: str, user_email: str) -> int` — reads the binding's
      CURRENT approved `dq_quality_rules` rows (via existing
      `RulesCatalogService.get_approved_checks_for_table` on the binding's table_fqn,
      restricted to rows with this binding's `applied_rule_id`s), bumps
      `dq_monitored_tables.version`, inserts the snapshot, returns the new version.
    - `refreeze_current(binding_id: str) -> None` — no-op if version==0; else rewrites vN's
      `checks_json`/`state_json` from current approved rows and stamps `refrozen_at`.
    - `list_versions(binding_id: str) -> list[MonitoredTableVersion]` (checks_json omitted)
    - `get_checks(binding_id: str, version: int) -> list[dict]` — raises `LookupError` if the
      snapshot doesn't exist (route maps to 422).
  - `Materializer.render_binding_checks(binding_id: str) -> list[dict]` — read-only render of
    the binding's CURRENT persisted applied-rules state through the same rendering code as
    materialization, without writing (the draft-run source).
  - Route `GET /monitored-tables/{binding_id}/versions` (operation_id
    `listMonitoredTableVersions`, VIEWER+).
  - Approve route: after rows flip approved → `freeze_new_version`; response includes new
    version. Re-freeze hooks: (a) `Materializer.rematerialize_for_rule` outcome where rows
    stayed approved (auto-upgrade ON), (b) per-rule approve/reject in `routes/v1/rules.py`
    for rows with non-null `applied_rule_id` → `refreeze_current(binding_id)` for each
    affected binding (resolve binding via the row's `applied_rule_id`→`dq_applied_rules.binding_id`).

Tests: approve bumps 1→2 and freezes exactly the approved checks; refreeze updates in place
(version stable, refrozen_at set); per-rule approval triggers refreeze; get_checks missing →
LookupError; render_binding_checks equals materialized output for an approved binding (reuse
the parity fixtures' shapes); version 0 refreeze no-op.

### Task 3: Run sets + monitored-table run endpoint

**Files:**
- Create: `backend/services/run_sets.py`, `backend/services/binding_run_service.py`
- Modify: `backend/routes/v1/monitored_tables.py` (run endpoint),
  create `backend/routes/v1/run_sets.py` (queries), register router in the v1 `__init__`/app
- Test: create `app/tests/test_run_sets.py`, `app/tests/test_binding_runs.py`

**Interfaces:**
- Consumes: Task 2 `MonitoredTableVersionService.get_checks` / `Materializer.render_binding_checks`;
  EXISTING `JobService.submit_run`, view service (`create_view`), and
  `record_dryrun_started` exactly as `routes/v1/dryrun.py:batch_run_from_catalog` uses them —
  copy that call pattern, do not modify it.
- Produces:
  - `class RunSetService:` `create(product_id, product_version, source, trigger, created_by) -> str`;
    `add_member(run_set_id, run_id, binding_id, binding_version) -> None`;
    `list_for_product(product_id, limit=50) -> list[RunSetSummary]` (member counts + aggregated
    status from `dq_validation_runs` join: running if any RUNNING, failed if any FAILED, …);
    `get(run_set_id) -> RunSetDetail` (per-member: table_fqn, binding_version|None, run_id,
    status, counts from the joined validation-run row).
  - `class BindingRunService:` `run_binding(binding_id, source: Literal["approved","draft"],
    version: int | None, user_email: str, trigger: str = "manual",
    run_set_id: str | None = None) -> BindingRunResult{run_set_id, run_id, job_run_id, view_fqn}`
    — resolves checks per the spec §4.1 matrix (draft → render_binding_checks; version → that
    snapshot; None+approved → latest, 409 if binding.version==0), mints a run set when
    run_set_id is None, submits via the existing path, records the member row.
  - Routes: `POST /monitored-tables/{binding_id}/run` (operation_id `runMonitoredTable`,
    RUNNER gate — reuse the exact `require_role`/permission dependency the dryrun batch route
    uses), body `{source: "approved"|"draft", version?: int}`;
    `GET /run-sets/{run_set_id}` (`getRunSet`); `GET /run-sets?product_id=` (`listRunSets`).

Tests: resolution matrix incl. 409 never-approved + 422 missing snapshot; run set of one
minted; member row carries binding_version (null on draft); RBAC (non-runner 403); submit path
receives EXACTLY the frozen checks (assert on the mocked JobService config payload);
aggregated status logic.

### Task 4: Data products service + routes

**Files:**
- Create: `backend/services/data_product_service.py`, `backend/routes/v1/data_products.py`
  (register router); Modify: `backend/models.py` (Pydantic In/Out models)
- Test: create `app/tests/test_data_products.py`
- Then: `make app-regen-api`.

**Interfaces:**
- Consumes: Task 3 `BindingRunService.run_binding` + `RunSetService`.
- Produces (operation_ids → orval hooks used by UI tasks):
  `listDataProducts`, `createDataProduct`, `getDataProduct`, `updateDataProduct`,
  `deleteDataProduct`, `addDataProductMember`, `removeDataProductMember`,
  `publishDataProduct`, `runDataProduct`, plus Task 3's `listRunSets`/`getRunSet`/
  `runMonitoredTable`/`listMonitoredTableVersions`.
  - `DataProductOut`: product_id, name, description, steward, schedule_cron, schedule_tz,
    status, version, display_status ("published"|"modified"|"draft" — dqlake's logic:
    published→"published"; draft with version>0→"modified"; else "draft"), members:
    list[`DataProductMemberOut{id, binding_id, table_fqn, binding_status, binding_version,
    pinned_version, rules_count, checks_count, runnable}`] (runnable = binding approved &&
    version>0), member_count, runnable_count, last_run_at (from run sets).
  - Semantics (dqlake-exact): PATCH/member changes flip published→draft, never bump version;
    `publish` bumps version+1 & sets published; `addDataProductMember` upserts by binding_id
    (pin change = update in place); name unique → 409; run body `{source}` → resolves members
    (skip never-approved unless draft source), one shared run set,
    `{run_set_id, submitted: […], skipped: […]}`, 409 when zero runnable.
  - RBAC: list/get VIEWER+; create/update/delete/members/publish RULE_AUTHOR+; run = RUNNER
    gate (same dependency as Task 3).

Tests: CRUD + uniqueness; publish bump + display_status transitions; member upsert/pin;
run fan-out (mock BindingRunService; assert shared run_set_id, skipped list, 409); RBAC matrix.

### Task 5: Scheduler product ticks

**Files:**
- Modify: `backend/services/scheduler_service.py` (additive — existing scope-config path
  untouched), maybe `backend/services/schedule_config_service.py` (read helper)
- Test: extend `app/tests/test_scheduler*.py` (existing scheduler test file)

**Interfaces:**
- Consumes: Task 4 `DataProductService` (list active published products with schedule_cron),
  Task 3 run services.
- Produces: in `_tick`, after existing config processing: for each product with
  `schedule_cron IS NOT NULL AND status='published'`, compute due-ness with the SAME cron
  machinery existing schedules use (5-field cron dialect — the UI task converts dqlake's
  helpers to 5-field), bookkeeping row in `dq_schedule_runs` keyed
  `schedule_name = f"product:{product_id}"`, fire `DataProductService.run(product_id,
  source="approved", trigger="scheduled", created_by="scheduler")`, best-effort per product
  (log + continue). Timezone: interpret cron in `schedule_tz` (document; existing configs'
  behavior unchanged).

Tests: due product fires once + bookkeeping row advances; not-due skipped; draft product
skipped; run failure doesn't break the tick loop; existing config schedules unaffected.

### Task 6: UI — nav rename, redirects, Data Products list + create

**Files:**
- Modify: `ui/routes/_sidebar/route.tsx` (nav: "Run Rules"→"Data Products", icon `Boxes`),
  `ui/routes/_sidebar/runs.tsx` → gut to a redirect to `/data-products` (keep file for old
  bookmarks, Phase-5 pattern; delete the Execute/Schedules UI code)
- Create: `ui/routes/_sidebar/data-products.index.tsx`, `ui/routes/_sidebar/data-products.new.tsx`,
  `ui/components/data-products/DataProductsTable.tsx`
- dqlake sources to COPY: `ui/routes/_sidebar/products/index.tsx`, `products/new.tsx`,
  `ui/components/products/DataProductsTable.tsx`

**Interfaces:** Consumes Task 4 hooks (`useListDataProducts`, `useCreateDataProduct`).
Columns: Name, Description, Status (Published/Modified since publish/Draft badges), Steward,
`# Tables` (+"(N not ready)"), `# Rules`, `# Checks`, Last Run (relative), Schedule (icon).
NO DQ Score column (spec §8). Edit Columns w/ dnd-kit drag reorder + localStorage persist
(`dqx.products.layout.v1`), header sort, search + steward filter, client pagination (50).
Create page: tab shell w/ only About enabled → Name/Description → navigate to detail.
i18n every string ×4. Browser-verify list/create/navigate. `.index.tsx` route pattern
(8A lesson: list pages must not be implicit parents without Outlet).

### Task 7: UI — product detail shell, About/Sharing, edit state, header actions

**Files:**
- Create: `ui/routes/_sidebar/data-products.$productId.tsx`,
  `ui/components/data-products/{ProductTabsShell,ProductHeader,ProductAboutTab,ProductSharingTab}.tsx`,
  `ui/routes/_sidebar/useEditProductState.ts` (or colocated hook)
- dqlake sources: `products/$productId.tsx`, `components/products/{ProductTabsShell,ProductAboutTab,ProductSharingTab}.tsx`, `products/useEditProductState.ts`

**Interfaces:** Consumes Task 4 hooks. Tab strip (spec §6, History CUT, Runs VISIBLE):
`About | Sharing, Tables | Runs ‖ Schedule` with dqlake's divider grouping; `?tab=` URL-driven;
lazy per-tab Suspense+ErrorBoundary. Header: name + `vN` badge (published only) +
Save as draft / Save & publish / Publish (dqlake publish-only-state logic) + Run now (busy/
active-poll/zero-runnable disabled states; poll via `listRunSets` for a RUNNING set) + ⋮ menu:
Run draft, Delete (confirm dialog, dqlake copy adapted, i18n'd). Edit-state hook: buffer
name/description/steward/schedule/members incl. pin-only dirtiness; persist = PATCH + member
add/remove/pin reconcile; wire existing unsaved-changes nav guard. Tables/Runs/Schedule tabs
render placeholders ("wired in Tasks 8/9/10") only if those tasks aren't merged yet — this
task must still leave app-check green.

### Task 8: UI — Tables tab, AddTablesDialog, TablesPicker, MemberVersionPin

**Files:**
- Create: `ui/components/data-products/{ProductTablesTab,AddTablesDialog,TablesPicker,MemberVersionPin}.tsx`
- dqlake sources: `components/products/{ProductTablesTab,AddTablesDialog,TablesPicker,MemberVersionPin}.tsx`

**Interfaces:** Consumes Task 7 edit-state buffer + Task 4 member models + Task 2
`useListMonitoredTableVersions` (pin dropdown lists real versions). Member rows: Table FQN
(link to monitored-table detail; "not ready" helper for unapproved), #Rules, #Checks, Version
(`MemberVersionPin`: "Latest (vN)" / "vN" badge, amber stale triangle+tooltip when pinned <
current, dropdown "Use latest (vN)" + descending vN..v1), hover-X remove + confirm. No score
footer. AddTablesDialog: embedded TablesPicker (checkbox rows, select-all w/ indeterminate,
search + catalog/schema/steward filters, already-member rows checked+disabled, pagination 25)
sourced from monitored tables (all statuses; unapproved marked "not ready", addable) + per-pick
"Version to track" pin list + "Add N tables" buffered into edit state. Browser-verify add/pin/
remove/save.

### Task 9: UI — Schedule tab

**Files:**
- Create: `ui/components/data-products/ProductSchedulingTab.tsx`,
  `ui/components/common/SchedulePicker.tsx`, `ui/components/common/TimezonePicker.tsx`,
  `ui/lib/cron.ts`
- dqlake sources: `components/products/ProductSchedulingTab.tsx`,
  `components/common/SchedulePicker.tsx` + its `ui/lib/cron.ts`

**Interfaces:** Consumes Task 7 edit-state (schedule buffer). Port dqlake exactly BUT convert
the cron dialect to the 5-field format the DQX scheduler evaluates (Task 5): adapt
`simpleToCron`/`cronToSimple`/`cronHint`/validation to 5-field; simple mode (Every
Day/Hour/Week at HH:MM) ⟷ "Show cron syntax" checkbox → raw cron input + live human hint +
inline invalid-cron error; Timezone section; "Remove schedule"; footer note reworded: schedule
applies when you save (our scheduler is in-app — no publish needed). i18n ×4. Browser-verify
both modes round-trip.

### Task 10: UI — Runs tab, monitored-table Run action + version badges, Runs History filter

**Files:**
- Create: `ui/components/data-products/ProductRunsTab.tsx`
- Modify: `ui/routes/_sidebar/monitored-tables.$bindingId.tsx` (Run split-action + version
  badge), `ui/components/monitored-tables/MonitoredTablesTable.tsx` (version column),
  `ui/routes/_sidebar/runs-history.tsx` (additive run-set filter + `?runSetId=` deep link)
- dqlake visual source: `components/products/ProductRunsTab.tsx` + `components/runs/RunSubtasks.tsx`

**Interfaces:** Consumes Task 3 `useListRunSets`/`useGetRunSet`, Task 2 versions hook, Task 3
`useRunMonitoredTable`. Runs tab: run-set rows (source badge approved/draft, trigger, started,
live-ticking duration while any member RUNNING, member count, aggregate status badge) →
expand: per-member indented rows (FQN, `vN`|"draft", status badge, link to
`/runs-history?runSetId=…` row); poll 4s while active; pagination 25. Monitored-table detail:
header `vN` / "Modified since vN" (persisted diff vs snapshot presence of pending/draft rows —
use binding status + version) / no badge at v0; Run split-button: "Run now (vN)" default, menu
= approved versions (from versions hook) + "Run draft" (v0 tables: draft only). Runs History:
optional run-set select filter (i18n'd, additive; page contract otherwise untouched).
Browser-verify: run a draft table end-to-end locally, watch the run set appear.

### Task 11: Final whole-phase review

Most capable model. Review package `MERGE_BASE=<Task 0 base commit>`..HEAD. Verify the Global
Constraints proof gates (runner diff empty, dq_quality_rules/dq_validation_runs untouched,
migrations appended-only), lifecycle coherence (approve→freeze→pin→run; refreeze paths;
product publish/modified), RBAC, security (SQL escaping in all new queries), i18n parity,
accumulated Minors triage. Fix round via ONE fixer if findings.

### Task 12: UI-driven test pass + redeploy

**User-requested: "a bunch of UI driven tests".**
- On local dev (`make app-start-dev`, browser MCP tools), execute and EVIDENCE (DOM
  text/screenshots in the report) at minimum:
  1. Registry: create rule → edit existing draft (P17-A regression) → publish.
  2. Monitored table: add table → apply rule to 2 columns one-click (Task 0) → Save →
     Submit → approve (as admin) → header shows v1 → Run now (v1) → run set appears →
     Runs History shows the run + run-set filter.
  3. Edit mappings → "Modified since v1" → Run draft → re-submit → approve → v2; pin
     behaviors in a product.
  4. Data product: create → add 2 tables (one pinned v1, one Latest) → Save & publish (v1) →
     Run now → run set with 2 members, statuses tick → edit member set → "Modified since
     publish" → publish → v2.
  5. Schedule: set simple daily schedule → verify scheduler bookkeeping row appears
     (`dq_schedule_runs` keyed product:<id>) — may shorten cron for a live tick test.
  6. Suggest rules dialog states (P17-B): configured + empty-registry reason rendering.
  7. Keyboard nav spot-checks (P16-E): function picker, column picker, multiselect.
- Fix small breakages found (or dispatch a fixer for real bugs), re-run the failing flow.
- Then redeploy: temp `run_as: user_name` edit → `UV_OFFLINE=1 make app-deploy
  PROFILE=<your-profile> TARGET=dev BUNDLE_VARS='--var=catalog_name=dqx
  --var=sql_warehouse_id=<your-warehouse-id>
  --var=dqx_service_principal_application_id=<your-sp-application-id>
  --var=lakebase_instance_name=dqx-studio-lakebase --var=admin_group=admins'` → revert edit →
  verify app RUNNING + smoke the deployed URL.
