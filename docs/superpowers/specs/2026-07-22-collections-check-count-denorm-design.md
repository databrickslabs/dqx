# Collections list speed: `applied_check_count` on the summary + lazy runs fetch — Design

**Date:** 2026-07-22
**Worktree:** `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4`
**Branch:** new worktree branch off `dqx-dqlake-integration` (created at execution time)

## Problem

Two independent issues make the **Collections** overview (the list of all
collections — `# Tables / # Rules / # Checks / DQ Score / Version / Last Run`)
feel slow:

**1. The Collections list RENDERS the check-count for every member on every load.**
`DataProductService.list_products` calls `_live_check_counts` →
`Materializer.render_binding_checks_counts_many` for every UNPINNED member — it
fetches each member's applied rules + rule versions and expands them in memory,
regardless of whether the member is approved. The collection-row `# Checks` (e.g.
13, 19) is the frontend sum of each member's rendered `checks_count`
(`DataProductsTable.tsx` reduces `members[].checks_count`).

The **Tables overview does NOT render for approved bindings.**
`routes/v1/monitored_tables.py:_apply_snapshot_check_counts` splits by approval:
- **approved (`version > 0`)** → the CACHED snapshot check-count from the frozen
  `dq_monitored_table_versions.state_json.check_count` (one batched metadata query,
  no render);
- **never-approved (`version == 0`, draft)** → live render (only these).

On a mostly-approved workspace almost nothing renders, so Tables is fast.
Collections renders ALL unpinned members (approved included) — that render fan-out
is the slowness. Git confirms it entered `list_products` in `1f25c048` (item 44),
which applied the DRAFT branch of the logic to every unpinned member.

**2. `dryrun/runs` + `profiler/runs` fetched on EVERY page navigation.**
The app-wide run-failure-toast hook (`hooks/use-run-failure-toasts.ts`, item 58),
mounted in the sidebar layout, eagerly fetches both run lists on the initial mount
of every page. Polling is already gated (only while a run is RUNNING + visible),
but the initial fetch rides every navigation.

## Why no new stored column / write-path refresh / rule-save fan-out is needed

An earlier draft proposed a denormalized `rendered_check_count` column with
write-path refresh + rule-edit fan-out. Dropped — over-engineered. The reliable
cheap value ALREADY EXISTS: `dq_monitored_table_versions.state_json.check_count`,
frozen at approval time and immutable for that version. `# Rules` is already cheap
(`COUNT(*)` over `dq_applied_rules` → `summary.applied_rule_count`); `# Checks`
just needs the SYMMETRIC field — `summary.applied_check_count` — sourced from that
frozen snapshot in the SAME summary query. No new table column, no migration, no
refresh-on-write, no fan-out: editing a rule bumps a binding's version, and the
next approval re-freezes a fresh snapshot count; drafts (the only mutable case)
still render fresh every load.

## Goals

- Add `applied_check_count` to `MonitoredTableSummary`, populated by the existing
  `list_monitored_tables` query via a LEFT JOIN to the frozen version snapshot —
  one Lakebase round-trip, sitting beside `applied_rule_count`.
- The Collections list reads `summary.applied_check_count` for approved members
  (cheap) and renders ONLY never-approved draft members — matching the Tables
  overview — so it's as fast as Tables on a mostly-approved workspace.
- `# Checks` VALUES are unchanged for every member state (approved → its frozen
  snapshot count, identical to today's render-at-approval; draft → live render,
  item-44 preserved; pinned → pinned snapshot, unchanged).
- The Tables overview can read `summary.applied_check_count` directly too,
  simplifying its approved branch (it currently makes a separate
  `snapshot_counts_many` call for approved bindings — that can fold into the
  summary) — as long as VALUES stay identical.
- The run-failure-toast hook no longer eagerly fetches the two run lists on every
  navigation.

## Non-goals

- No new OLTP column, migration, backfill, write-path refresh, or rule-save
  fan-out.
- No change to the score cache, the materializer's render logic, the Collections
  DETAIL page semantics, or the runner path.
- No change to `# Tables` (`len(members)`) or `# Rules`
  (`summary.applied_rule_count`) — already cheap, untouched.

## Global Constraints

- Backend type hints on every param/return; `str | None`; no `Any`. `make app-check`
  clean (the 5 pre-existing errors in `monitored_table_service.py:1062` /
  `apply_rules_service.py:73` / `seed_demo.py:264` are out of scope).
- Portable SQL via the executor dialect helpers — the JSON extraction of
  `state_json.check_count` MUST use the executor's JSON helper
  (`select_json_text` / the same helper `snapshot_counts_many` uses), never a
  hard-coded `::jsonb` / `get_json_object`, so it works on BOTH Lakebase Postgres
  and the Delta OLTP fallback. Confirm the helper both backends expose before
  writing the JOIN.
- Reuse batched, query-bounded helpers; never introduce a per-member round-trip.
- VALUES must stay byte-identical for approved / draft / pinned members. This is a
  performance change, not a semantics change — every existing count test must pass
  unchanged.
- Tests: assert the list renders ONLY draft (`version == 0`, unpinned) members;
  approved members' `# Checks` comes from `applied_check_count`; existing item-44 /
  pinned / Tables-overview count tests still pass unchanged.

## Approach

### `MonitoredTableSummary.applied_check_count` (the symmetry)

Add `applied_check_count: int | None` to `MonitoredTableSummary` (next to
`applied_rule_count`). Populate it in `MonitoredTableService.list_monitored_tables`
by extending that method's existing SELECT with a LEFT JOIN to
`dq_monitored_table_versions` on `(binding_id = mt.binding_id AND version =
mt.version)` and extracting `state_json.check_count` via the executor's JSON text
helper. This resolves in the SAME round-trip that already LEFT-JOINs
`dq_score_cache` — no extra query.

- Approved binding (`version > 0`) → the JOIN finds its frozen snapshot → non-NULL
  cached check count.
- Never-approved binding (`version == 0`) → no snapshot row → NULL (the signal that
  a live render is needed for this one).

### Collections list — read the field, render only drafts

In `DataProductService.list_products` / `_member_counts`:
- **pinned member** → unchanged (frozen pinned-snapshot count via
  `_pinned_snapshot_counts`).
- **approved unpinned (`version > 0`)** → `summary.applied_check_count` (the new
  field). No render.
- **never-approved unpinned (`version == 0`)** → the ONLY members passed to
  `render_binding_checks_counts_many` (one batched call, as today). If the render
  yields nothing, fall back to `applied_check_count` (NULL→0) — never crash.

`_live_check_counts` shrinks from "all unpinned members" to "only draft members."
`_table_summary_map()` (the single cheap `list_monitored_tables` call) stays.

### Tables overview — optionally fold its approved branch onto the field

`_apply_snapshot_check_counts` currently makes a separate `snapshot_counts_many`
call for approved bindings. Since `applied_check_count` now carries that same
frozen count on the summary, the approved branch can read it directly and the
separate call for the approved (non-pinned-in-list) case can be dropped — PROVIDED
the resulting values are identical (the pinned-in-list `version > 0` case already
uses the current version's snapshot, which is exactly what the JOIN returns). If
any value would differ, leave the Tables path as-is; the primary win is Collections.
The draft branch (live render) is unchanged either way.

### Runs fetch — make the toast hook lazy/shared

In `hooks/use-run-failure-toasts.ts`: pin `useListValidationRuns` +
`useListProfileRuns` to a long `staleTime` so the shared React Query cache serves
them across navigations (one fetch per session window instead of per mount).
Failure-toast detection and the RUNNING+visible polling gate are unchanged.
Self-contained frontend change, independent of the count work.

## Testing

- `monitored_table_service`: `list_monitored_tables` emits the version-snapshot
  LEFT JOIN + JSON extraction; `applied_check_count` is non-NULL for an approved
  binding (equals its snapshot count) and NULL for a draft binding; the JSON helper
  used is the portable one.
- `data_product_service`: `list_products` calls `render_binding_checks_counts_many`
  with ONLY never-approved unpinned members (assert approved members absent from
  render input); approved member `# Checks` == `applied_check_count`; pinned
  unchanged; NULL count → 0.
- Values unchanged: existing item-44, pinned-member, and (if the Tables branch is
  folded) monitored-tables-overview count tests pass unchanged.
- Frontend: the run-failure-toast hook no longer fetches on every mount (assert via
  hook query options / staleTime); toasts still fire on a newly-failed run.

## Deploy / verification

- `make app-check` clean (modulo known pre-existing); `make app-test` green.
- Build wheel with the `.cloud` pypi-proxy fallback; deploy to `fe-sandbox-dq-demo-2`;
  app RUNNING. Verify: Collections list paints as fast as Tables; `# Checks` values
  unchanged for approved/draft/pinned; the two `runs` calls no longer fire on every
  navigation.
- Report for user verification BEFORE squash-merge.
