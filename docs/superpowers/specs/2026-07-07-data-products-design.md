# Data Products — versioned monitored tables, grouped runs, per-product scheduling

Date: 2026-07-07 · Branch: `rules-registry` · Status: approved by user (pin semantics = real
execution; version bump = on approval; Run Rules replaced; NO History tab; draft runs required)

## 1. Goal

Port dqlake's "Data Products" concept into DQX Studio: monitored tables become **versioned**;
approved rule sets are **frozen per version** and runnable as of any approved version (or as a
draft); **Data Products** group multiple monitored tables (each pinned to a version or following
latest) behind a GUID, a publish-bumped version, an openable dqlake-style screen, one-click
simultaneous runs, and a per-product schedule using dqlake's schedule picker UI. The existing
"Run Rules" page is replaced.

## 2. Invariants (hard constraints)

- `dq_quality_rules` table spec: UNCHANGED (no columns added/removed/retyped).
- Job/runner path UNCHANGED: `app/tasks/` (dqx_task_runner), `job_service.py`, and the
  `jobs.run_now(DQX_JOB_ID, job_parameters={... config_json ...})` contract stay byte-identical.
  Enabler: the runner executes exactly the checks handed to it — `runner.py:324`
  `checks = config.get("checks", [])`. All version/draft resolution is app-side.
- Runs History (`/runs-history`, `GET /api/v1/dryrun/runs`, `ValidationRunSummaryOut`,
  `dq_validation_runs` schema) keeps working untouched. Run-set tracking is ADDITIVE via new
  tables; no column changes to `dq_validation_runs`.
- Scheduler's existing scope-based configs (`dq_schedule_configs`) keep ticking (back-compat);
  only the UI for authoring them is retired with the page.
- Migrations are APPENDED versions (PG v6+/Delta v10+), never in-place edits of shipped
  versions (the 00c3e01 lesson).

## 3. Data model (all new/additive)

### 3.1 `dq_monitored_tables` — add `version`
`version INT NOT NULL DEFAULT 0` (0 = never approved). Appended migration converges live DBs.

### 3.2 NEW `dq_monitored_table_versions` — frozen approved snapshots
| column | type | semantics |
|---|---|---|
| id | TEXT/UUID PK | |
| binding_id | FK dq_monitored_tables | |
| version | INT | UNIQUE(binding_id, version) |
| checks_json | JSON | the frozen list of DQX check dicts, exactly the shape the runner consumes (same shape as `get_approved_checks_for_table` output) |
| state_json | JSON | display metadata: applied rules with registry ids/versions/pins/severities/mappings at freeze time |
| created_by, created_at, refrozen_at | audit | `refrozen_at` set when vN is re-frozen without a bump |

**Freeze rule:** snapshot vN always mirrors the binding's CURRENT approved rule set.
- Table **approval** (`POST /monitored-tables/{id}/approve`): after rows flip approved →
  `version = version + 1`, write snapshot vN from the binding's approved `dq_quality_rules`
  rows (their check definitions).
- **Re-freeze without bump** (dqlake's reconcile-without-bump): any event that changes the
  binding's approved rule set WITHOUT a table re-approval updates the vN row in place and
  stamps `refrozen_at`: (a) auto-upgrade (`auto_upgrade_without_approval=ON`, unpinned content
  change stays approved), (b) a per-rule approval in Drafts & Review affecting this binding's
  materialized rows, (c) per-rule rejection/deprecation removing a row from the approved set.
- Edits after approval (apply-rules save, mapping/severity/pin changes) do NOT touch vN; the
  table shows "Modified since vN" until re-submitted + re-approved (mints vN+1).

### 3.3 NEW `dq_data_products`
| column | semantics |
|---|---|
| product_id TEXT/UUID PK | the grouping GUID |
| name | UNIQUE, required |
| description | optional |
| steward | user email (defaults to creator) |
| schedule_cron, schedule_tz | nullable; Quartz-style cron + IANA tz (dqlake shape) |
| status | CHECK draft\|published (NO approver gate — members are already approval-gated) |
| version INT DEFAULT 0 | bumped ONLY on Publish (`v+1`); edits flip published→draft ("Modified since publish" display state, dqlake `display_status` logic) |
| created_by/at, updated_by/at | audit |

### 3.4 NEW `dq_data_product_members`
| column | semantics |
|---|---|
| id PK | |
| product_id FK | |
| binding_id FK | members reference bindings by id (stronger than dqlake's FQN matching) |
| pinned_version INT NULL | NULL = follow latest approved; INT = **really executes** frozen snapshot vN (deliberate upgrade over dqlake's display-only pin) |
| UNIQUE(product_id, binding_id) | |

### 3.5 NEW `dq_run_sets` + `dq_run_set_members`
`dq_run_sets`: run_set_id (UUID PK), product_id (nullable — null for single-table runs),
product_version (nullable), source CHECK approved|draft, trigger CHECK manual|scheduled,
created_by, created_at.
`dq_run_set_members`: run_set_id FK, run_id (the per-table app run id in
`dq_validation_runs`), binding_id, binding_version (nullable — null for draft-source runs).
Every run submission (product, single table, scheduled product) mints a run_set — this
finally provides the unique run-set GUID (ledger 11-D). Runs History joins these tables
optionally (additive filter/column); its existing contract is untouched.

### 3.6 Dropped from the earlier draft
No `dq_data_product_snapshots` and NO History tab (user cut). Product `version` int alone
carries governance.

## 4. Execution semantics (runner untouched)

### 4.1 Check resolution per member table
```
resolve_checks(binding, pinned_version, source):
  if source == draft:
      render the binding's CURRENT persisted applied-rules state through the SAME rendering
      code the materializer uses (exposed as a read-only render — no writes to
      dq_quality_rules); binding_version recorded as NULL
  elif pinned_version is not None:
      dq_monitored_table_versions[binding, pinned_version].checks_json   (404→422 if missing)
  else:  # follow latest approved
      dq_monitored_table_versions[binding, binding.version].checks_json  (requires version>0)
```
Then per table: create view (existing `view_svc`), `job_svc.submit_run(task_type="dryrun",
config={checks: <resolved>, ...}, run_id=uuid4().hex[:16], ...)` — the EXISTING submission
path; one job run per table exactly as today.

### 4.2 Run sources
- **Monitored table** detail gets a Run action: default "Run now" = latest approved snapshot;
  a version picker (dqlake MemberVersionPin-style dropdown) allows any approved vK; "Run
  draft" runs the current persisted draft state (staged-but-unsaved browser edits are NOT
  included — save first; the button copy says so). Tables with version 0 (never approved)
  can ONLY draft-run.
- **Data Product** "Run now" (published source): resolves every member — pinned → frozen vK;
  unpinned → latest approved; members whose table has no approved version are skipped and
  surfaced in the response/UI ("not ready", dqlake parity). "Run draft": uses the product's
  current persisted member set (even if product status=draft/modified); members resolve as
  above EXCEPT never-approved tables run their draft-rendered checks instead of being
  skipped. Zero-runnable-members → 409.
- All member submissions of one trigger share the minted run_set_id; single-table runs are a
  run set of one. `dq_validation_runs.run_type` uses existing values (`dryrun` for manual,
  `scheduled` for scheduler ticks) — no CHECK change.

### 4.3 Scheduling
Per-product schedule (cron+tz on the product row). The existing in-app scheduler loop gains a
second source: due active published products → run with source=approved, trigger=scheduled.
Bookkeeping reuses `dq_schedule_runs` keyed by product_id. Existing scope-config schedules
continue to tick unchanged. Schedule edits apply on save (no publish needed — our scheduler is
in-app; note this diverges from dqlake where the Databricks job is only rebuilt on publish).

## 5. RBAC
- View products/runs: VIEWER+. Create/edit/publish/delete products: RULE_AUTHOR+ (no
  approver gate). Run (product, table, draft): RUNNER (admins implicit) — same gate as
  today's Run Rules. Backend `require_role` is the gate; FE mirrors it.

## 6. UI (close dqlake match; source references)

dqlake source root: `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch/src/dqlake/ui/`.

- **Nav**: "Run Rules" → **"Data Products"** (same position; pick a fitting lucide icon,
  e.g. Boxes/Package). `/runs` and `/runs?tab=…` redirect to `/data-products` (Phase-5
  redirect pattern). Runs History nav unchanged.
- **List** `/data-products`: port `components/products/DataProductsTable.tsx` — columns
  Name, Description, Status badge (Published / Modified since publish / Draft), Steward,
  #Tables (with "(N not ready)" suffix), #Rules, #Checks, Last Run (relative), Schedule
  (check/x icon); Edit Columns dropdown with drag-reorder + persisted layout; click-header
  sort; search + steward filter; client pagination; row → detail. (DQ Score column omitted —
  no score pipeline; see §8.)
- **Create** `/data-products/new`: port `products/new.tsx` — tab shell with only About
  enabled; Name + Description; create → navigate to detail.
- **Detail** `/data-products/$productId` (`?tab=` URL-driven): port `ProductTabsShell` minus
  History → strip: `About | Sharing, Tables | Runs ‖ Schedule` (History cut; since dqlake
  hides Runs in the ⋮ menu and we have no Results dashboard, Runs takes the Results slot as a
  visible tab). Header: name + `vN` badge when published + Save as draft / Save & publish /
  Publish (publish-only state logic ported from `useEditProductState`) + **Run now** button
  state machine (busy/active-run polling/disabled at 0 runnable) + ⋮ menu (Run draft, Delete
  with dqlake's confirm copy). Buffered edit state hook ported (`useEditProductState`
  pattern: dirty tracking incl. pin-only changes, persist = PATCH + member add/remove
  reconcile).
- **About / Sharing tabs**: straight ports (Name+Description; StewardPicker equivalent using
  our existing steward control).
- **Tables tab**: port `ProductTablesTab` — member rows: Table (FQN, links to monitored-table
  detail; "not ready" helper when unapproved), #Rules, #Checks, Version
  (**MemberVersionPin** port: "Latest (vN)" / "vN" badge, amber stale triangle + tooltip when
  pinned < current, dropdown "Use latest (vN)" + vN..v1), hover-X remove with confirm.
  Footer summary row omitted (score). "Add tables" → port `AddTablesDialog`: embedded picker
  + "Version to track" per-pick pin section + "Add N tables" (buffered locally, flushed on
  Save). Picker = dqlake `TablesPicker` structure (checkbox rows, select-all, search +
  catalog/schema/steward filters, already-member rows checked+disabled, client pagination)
  blended with DQX Execute-tab patterns (catalog/schema grouping, LabelsBadges) — source list
  = monitored tables (all statuses shown; unapproved rows marked "not ready", still addable).
- **Schedule tab**: port `SchedulePicker` + `ProductSchedulingTab` exactly — empty state
  ("runs on demand only…" + Add schedule), simple mode (Every Day/Hour/Week at HH:MM selects)
  ⟷ "Show cron syntax" checkbox → raw cron input with live human-readable hint + validation,
  Timezone picker, Remove schedule, footer note.
- **Runs tab**: dqlake `ProductRunsTab` visual (expandable rows, status badges, live-ticking
  durations, pagination) backed by our `dq_run_sets` join (NOT the Jobs API): row =
  run set (source badge draft/approved, trigger, started, duration, member count); expand →
  per-member rows (table FQN, binding_version or "draft", per-run status from
  `dq_validation_runs`, link into Runs History row).
- **Monitored table detail**: header version badge (`vN` / "Modified since vN" /
  no badge when never approved) + Run split-action (Run now (vN) default; menu: pick version,
  Run draft). Applied-rules pin UI already exists; align its badge visuals with
  MemberVersionPin.
- **Runs History**: unchanged page + an additive "Run set" filter (dropdown/chip) and an
  optional run-set column populated via the new join; deep-linkable
  `?runSetId=` for the product Runs tab links.

## 7. API surface (new routes, `routes/v1/data_products.py` + additions)

- `GET/POST /data-products`, `GET/PATCH/DELETE /data-products/{id}`
- `POST /data-products/{id}/members` (upsert by binding_id incl. pin change),
  `DELETE /data-products/{id}/members/{member_id}`
- `POST /data-products/{id}/publish` (bumps version)
- `POST /data-products/{id}/run` body `{source: "approved"|"draft"}` → `{run_set_id, submitted:[…], skipped:[…]}`
- `GET /data-products/{id}/run-sets` (+ `GET /run-sets/{id}` detail w/ member run statuses)
- `POST /monitored-tables/{binding_id}/run` body `{source, version?}` → run set of one
- `GET /monitored-tables/{binding_id}/versions` (list approved versions for pickers)
- Approve route (existing) extended: bump + freeze (§3.2). Materializer exposes a read-only
  render for draft runs (no persisted-materialization behavior change).
- Existing dryrun/batch-from-catalog endpoint remains for back-compat but the UI no longer
  calls it.

## 8. Out of scope (explicit)

History tab + product snapshots (user cut). DQ Score columns/dashboard (dqlake's
Results/score pipeline — no equivalent data source; revisit later). dqlake's per-binding
Databricks jobs/pipelines model (we keep the single DQX_JOB_ID runner). SCHEMA-kind members
(dqlake enum exists but is unwired there too). Migrating existing scope-schedules into
products.

## 9. Testing

Backend (pytest, no Spark): version bump + freeze on approve; re-freeze without bump on
auto-upgrade and per-rule approval; resolve_checks matrix (pinned/latest/draft ×
approved/never-approved; missing snapshot → 422); product CRUD + publish version bump +
modified-since status; member upsert/pin change; run endpoints (run-set minting, skipped
members, 409 zero-runnable, RBAC incl. RUNNER gate); scheduler product-tick (due resolution,
bookkeeping row, source=approved trigger=scheduled); run-set queries powering the Runs tab;
migrations (appended-version tests per 00c3e01 pattern). Frontend: i18n parity; browser
verification of list/detail/add-tables/pin/run/schedule flows (this machine reaches the
workspace). The runner-invariant proof stays `git diff -- app/tasks/ '*job_service*'` = empty
+ dq_quality_rules schema untouched.
