# Bug-bash-5 — Design

**Date:** 2026-07-22
**Branch:** `dqx/bug-bash-5` (off `dqx-dqlake-integration` @ `c8d1183f`)
**Scope:** DQX Studio app (`app/`) — a batch of roles/permissions fixes, a profiler→apply-rules UX fix, run-status fixes, and two small UI changes.

## Summary

Fourteen items grouped into five workstreams. Workstream B (object permissions) is the keystone — it rewrites how default grants are stored and adds an `EXECUTE` privilege — so it lands first. The others build on it.

The unifying principle for A + B: **roles are a hard ceiling; object grants only refine *within* a role, never elevate above it.** A Viewer granted `MODIFY`/`EXECUTE` on an object is still capped at Viewer because `require_role` rejects the route before any object-grant check runs. This is already pinned by `tests/test_entitlements_hard_boundary.py`; we extend it.

## Item → workstream map

| # | Item | Workstream |
|---|------|-----------|
| 1 | Viewer can click Run Profile → 403; should be disabled in UI | A |
| 3 | Profiling disabled for viewers (UI gate) | A |
| 14 | Remove `RUNNER` role; fold `run_rules` into `RULE_AUTHOR` | A |
| 2 | Add `EXECUTE` privilege to tables & collections | B |
| 5 | Permissions can't be deleted (owner/all-users); entitlements are an un-overridable boundary | B |
| 6 | Auto-select `SELECT` when any other privilege is checked | B |
| 4 | Show current role in top-right username dropdown | A (UI) |
| 7 | Profiler-applied checks need refresh + bypass approval | C |
| 11 | Runs-history refresh button | D |
| 12 | Runs-history stopped live-updating when a job ends | D |
| 13 | Runs started from overview/collection don't show "Running" spinners on the table | D |
| 8 | Dimensions dropdown native hover tooltip with descriptions | E |
| 10 | Rename "Slots" → "Column Type(s)" | E |

("Apply Rules → Rules" tab rename was considered and **dropped** per user preference.)

---

## Workstream A — Roles & run-gating (items 1, 3, 4, 14)

### A1. Remove the `RUNNER` role

`backend/common/authorization.py`:
- Delete `UserRole.RUNNER` from the enum.
- Remove it from `ROLE_PRIORITY` (it was already off the priority ladder).
- Remove the runner-only permission set from the matrix.

`backend/services/role_service.py`:
- Delete `has_runner_role()` and any runner-specific resolution branch. `resolve_role` no longer special-cases runner.

`backend/routes/v1/me.py`:
- Delete the runner-layer append (`if effective_runner and "run_rules" not in permissions: ...`). `run_rules` now comes from the role matrix directly.

**No migration.** Per user decision, stale `runner` rows in `dq_role_mappings` are simply ignored — `resolve_role` won't match a role that no longer exists, so those groups fall back to `VIEWER`. Known tradeoff: a group mapped *only* to runner loses run ability until an admin re-maps it to author. Called out to the user; accepted.

Frontend: remove `"runner"` from the role picker options in `RoleManagement` (Settings → Entitlements).

### A2. Revised permission matrix

Approvers approve; they do not author or run. New matrix in `authorization.py`:

| Role | Permissions |
|------|-------------|
| `ADMIN` | view_rules, create_rules, edit_rules, generate_rules, submit_rules, approve_rules, export_rules, configure_storage, manage_roles, **run_rules** |
| `RULE_APPROVER` | view_rules, approve_rules, export_rules, configure_storage *(+ view quarantine)* |
| `RULE_AUTHOR` | view_rules, create_rules, edit_rules, generate_rules, submit_rules, **run_rules** |
| `VIEWER` | view_rules |

Notes:
- `RULE_APPROVER` **loses** create/edit/generate/submit and does **not** gain run_rules.
- `RULE_AUTHOR` **gains** run_rules (absorbs the old orthogonal runner ability).
- Only `VIEWER` lacks `run_rules` → viewers can't run, exactly as desired.

**Consequence for object-grant bypass:** today `_ROLE_BYPASS` = {ADMIN, RULE_APPROVER} (approvers bypass object grants like admins). Approver now has a much narrower permission set; the bypass stays as-is for the privileges the approver still holds (view/approve/export/configure). Because object grants only gate `MODIFY`/`APPLY`/`EXECUTE` — none of which the approver has role-permission for — the bypass is effectively inert for authoring/running. Keep `_ROLE_BYPASS` unchanged (admin+approver) to avoid regressing approver's ability to *see* everything for review; extend the boundary test to confirm an approver still can't reach an author-gated route.

### A3. Run-route role gate

Routes that submit profiler/validation runs currently guard with `require_role(*_ALL_ROLES)` (any authenticated user, incl. viewer). Change to a non-viewer set that holds `run_rules`: `require_role(UserRole.ADMIN, UserRole.RULE_AUTHOR)`. (Approver excluded — no run_rules.) Applies to:
- `backend/routes/v1/profiler.py` — submit/poll/cancel profiler jobs
- `backend/routes/v1/dryrun.py` — submit run (line ~82, currently `_ALL_ROLES`)

Define a shared constant (e.g. `_CAN_RUN = (UserRole.ADMIN, UserRole.RULE_AUTHOR)`) rather than repeating the tuple.

### A4. UI run-gating (items 1, 3)

The "Run Profile" and run-validation buttons must be disabled for users lacking `run_rules`, instead of firing a request that 403s.
- Source of truth: `GET /api/v1/current-user/role` returns `permissions: string[]`. Gate on `permissions.includes("run_rules")`.
- Disable the button + show a tooltip explaining why (i18n key, e.g. `permissions.cannotRunTooltip` = "Viewers can't run profiling.").
- Find all run/profile trigger buttons (profiler launch, run-validation, overview actions bar, collection run) and apply the same gate via a small shared hook (e.g. `useCanRun()` reading the role query).

### A5. Show current role in username dropdown (item 4)

`ui/components/layout/HeaderUserMenu.tsx`: add a line/badge under the user's name+email showing the resolved role (already fetched — the menu reads `role` for the admin-gated Settings link). Display humanized role label (i18n). Read-only.

---

## Workstream B — Object permissions rework (items 2, 5, 6)

### B1. Materialize real default grant rows

**Problem being fixed:** the users-group default (`SELECT+APPLY`) and owner default (`ALL_PRIVILEGES`) rows are synthesized at read time, not stored. Deleting them uses a *separate* code path (`handleRevokeDefault` → write an empty-privilege "revoked marker") from real grants (`handleRemove` → SQL `DELETE`). The dual path is the deletion bug.

**Fix:** on object creation, write **actual** grant rows:
- users-group: `SELECT + APPLY + EXECUTE` (see B2 for EXECUTE-in-default)
- owner/creator: `ALL_PRIVILEGES`

Then every row — default or not — is edited/deleted through **one** uniform path (`remove_grant` → `DELETE`; `set_grant` → upsert). `handleRevokeDefault`, `is_default` synthesis, and the revoke-marker concept are removed.

**Backend (`permissions_service.py` + `common/permissions.py`):**
- On object create (registry rule, monitored table, data product), insert the two default rows. Identify the creation call sites for each object type and add the grant seeding there (in the service that creates the object, within the same logical operation).
- `effective_privileges()` no longer injects synthetic users-group/owner defaults — it reads only stored rows (plus inheritance). The owner's access now comes entirely from a **real stored `ALL_PRIVILEGES` row** written at creation, so **remove** the "implicit owner ALL_PRIVILEGES unless overridden" branch entirely. There is no implicit owner path after this change. The admin/approver `_ROLE_BYPASS` is the only non-stored access path and remains, so an object can never be orphaned (an admin can always re-grant even if every stored row is deleted).
- `remove_grant` stays a plain `DELETE`. Deleting the owner row or users-group row now just works; admin/approver bypass keeps the object manageable.

**Migration / backfill:** existing objects have no stored default rows. A migration backfills, for every existing registry_rule / monitored_table / data_product, the users-group row (`SELECT,APPLY,EXECUTE`) and an owner row (`ALL_PRIVILEGES`, keyed to the stored owner/creator email where known). Where owner is unknown, skip the owner row (admin bypass still covers manageability). Idempotent: only insert when no row exists for that (object, principal).

**Data model note:** `dq_object_grants` already stores arbitrary principals + privilege strings, so no schema change — the migration only inserts rows. `is_default` column (if present) becomes vestigial; keep it nullable/ignored or drop it in the migration (decide during implementation — dropping is cleaner but touches more).

### B2. Add `EXECUTE` privilege (item 2)

`common/permissions.py`:
- Add `Privilege.EXECUTE = "EXECUTE"`.
- Add to `_CONCRETE_PRIVILEGES` → `{SELECT, MODIFY, APPLY, EXECUTE}`.
- `ALL_PRIVILEGES` now expands to include EXECUTE.
- `serialize_privileges` / `normalize_privileges` ordering: `[SELECT, MODIFY, APPLY, EXECUTE]`.
- `DEFAULT_USERS_GROUP_PRIVILEGES` → `{SELECT, APPLY, EXECUTE}` (EXECUTE in the day-one default per user decision — preserves "any author-and-above can run" while making it narrowable per asset).

**Semantics:** `EXECUTE` = "run profiling / validation runs on this table or collection." It gates the run path for `monitored_table` and `data_product`. It is meaningless on `registry_rule` (rules aren't run directly) — the grant dialog omits EXECUTE for registry rules, mirroring how the inherit toggle is omitted there.

**Enforcement — the boundary (item 5):** the run routes (A3) gain, *after* the `require_role` gate, a `PermissionsService.require(object_type, object_id, Privilege.EXECUTE, role=..., principal_ids=...)` check. This is the layer that makes per-asset entitlements bite: a role with run_rules can still be denied EXECUTE on a *specific* table if an admin narrowed it. Admin/approver bypass object grants (so admin always can; approver has no run_rules so never reaches here). This wires per-asset permissions into the run path, which today only checks role.

**Confirming the ceiling (user's Viewer+MODIFY example):** a Viewer with a `MODIFY` grant is still blocked from editing because `require_role` on the edit route excludes Viewer and runs *first*. The object grant is inert above the role ceiling. Object grants can only *subtract* (narrow users-group/owner defaults), never *add* above the role. Extend `test_entitlements_hard_boundary.py` with: (a) Viewer+MODIFY still can't edit, (b) Viewer+EXECUTE still can't run, (c) Approver can't reach author/run routes, (d) Author denied EXECUTE on a specific narrowed table can't run it.

### B3. Auto-select SELECT (item 6)

`ui/components/permissions/PermissionsTab.tsx` `GrantDialog`:
- When MODIFY, APPLY, or EXECUTE is checked, force `view` (SELECT) true and prevent unchecking it while any other privilege is set ("can't modify what you can't see").
- Add an EXECUTE checkbox (hidden for `objectType === "registry_rule"`), with hint copy.
- Keep the existing "users-group row may be saved with empty privileges" affordance? With materialized rows, an empty-privilege users-group grant is just a normal narrowing (revoke everything for the group). Keep allowing empty for the users group; other principals still require ≥1 privilege.

Update `permissions-utils.ts`: add `PRIV_EXECUTE`, include in `isAllPrivileges`, and the ordering. Add bun tests for the auto-select logic (pure helper: given a set of checked boxes, SELECT is forced on).

---

## Workstream C — Profiler apply → draft (item 7)

**Desired behavior (user):** clicking Apply on profiler suggestions must (a) make the mappings show up in the Apply Rules tab immediately (no refresh), and (b) not silently persist anything behind approvals. Instead it populates the tab's working/draft state; the user saves via the normal path when ready.

**Current behavior:** `ProfilingSuggestionService.apply_suggestions` calls `match_or_create_approved_rule` (auto-approves the rule template — keep this) **and** `apply_rules.apply_rule(...)` which immediately persists the binding server-side (this is the silent bypass + the reason a refresh is needed).

**The mechanism already exists.** The Apply Rules tab (`monitored-tables.$bindingId.tsx`) already tracks an unsaved working selection: `stagedRows` (the current, possibly-dirty selection) vs `baseline` (last saved), wired to the Save-as-draft button and the unsaved-changes nav guard. When a user picks rules by hand, they land in `stagedRows` and show as unsaved until Save. **Profiler apply should do the identical thing** — push the profiler's rules into `stagedRows` — so the result is exactly "as if the user had selected these rules but not yet pressed Save."

**Change:**
- Keep rule-template creation (`match_or_create_approved_rule`) — profiler rules auto-approve as templates, as today. That's fine; templates aren't table mappings.
- **Stop** the server-side `apply_rule` binding call in the profiler-apply path.
- The apply endpoint returns the resolved rule id(s) + column mapping(s) as data (shaped as `AppliedRuleOut`, the same type `stagedRows` holds).
- Frontend `ProfileSuggestionsCard`: on apply, instead of invalidating and expecting the server to have persisted, call the tab's existing "add rules to staged selection" handler (the same one manual selection uses — pass the returned rows up via a callback/prop) so they append to `stagedRows`. They render immediately and the tab goes dirty. The user hits Save exactly as with hand-picked rules; that goes through the normal monitored-table save path (where approval, if any, applies the normal way). No new draft layer, no separate persistence path.

**Verify at runtime:** profiler apply → rules appear in Apply Rules tab immediately without refresh and the tab shows unsaved/dirty → Save persists them → reload shows them.

---

## Workstream D — Runs status (items 11, 12, 13)

### D1. Refresh button (item 11)

`ui/routes/_sidebar/runs-history.tsx`: add a Refresh button to the header toolbar (lines ~621–626) that calls `refetchValidation()` + `refetchProfile()`. Use existing icon (RefreshCw) + i18n label. Disable/spin while fetching.

### D2. Live-update stops when job ends (item 12)

**Root cause:** the polling `useEffect` (lines ~428–436) depends on `hasRunning`; when a RUNNING run settles, `hasRunning` flips false and the interval is cleared *before* a refetch reflecting the final status necessarily lands — polling stops one tick too early, so the row can be left showing RUNNING until a manual refresh.

**Fix approach:** keep polling until a refetch has *confirmed* no in-flight runs, rather than clearing on the derived flag mid-flight. Concretely: after `hasRunning` becomes false, perform one final refetch (or keep the interval alive for one extra confirmed-empty cycle). Simplest robust form: drive polling with React Query's `refetchInterval` set to a function that returns the interval while any run is RUNNING **based on the freshly-fetched data** and `false` otherwise — React Query evaluates `refetchInterval` against the latest data after each fetch, so it naturally does one more poll that observes the settled status and then stops. Prefer moving polling into the query's `refetchInterval` (both `useListValidationRuns` and `useListProfileRuns`) over the manual `setInterval` effect. Verify at runtime: start a run, watch it settle to SUCCESS/FAILED without a manual refresh.

### D3. Overview/collection-triggered spinners (item 13)

**Symptom:** a run started from the all-tables overview actions bar (or by running a collection) doesn't show "Running" spinners on the top buttons when you then open that table's detail page.

**Root cause hypothesis (confirm at build):** the table detail view derives in-flight state from runs it launched itself (local mutation state) rather than from a query over current run status for that table. A run launched elsewhere isn't reflected.

**Fix:** derive the table's in-flight state from a query keyed on the table's current runs (the same validation-runs data, filtered to this table/binding), so any RUNNING run — regardless of where it was launched — lights up the spinners. Enable polling while such a run is in-flight (reuse the D2 `refetchInterval` pattern). Ensure the query is invalidated when a run is launched from the overview/collection path so the detail page picks it up on open.

---

## Workstream E — Small UI (items 8, 10)

### E1. Dimensions dropdown tooltip (item 8)

`ui/components/RegistryRuleFormDialog.tsx` (dimensions dropdown ~lines 2940–2965): wrap each dimension `SelectItem` with the existing `Tooltip`/`TooltipTrigger`/`TooltipContent` (from `components/ui/tooltip.tsx`). Content = `value_descriptions[dimensionValue]` from the `LabelDefinition` for the reserved dimension key (`value_descriptions` already exists on `LabelDefinition` in `api-custom.ts`). Only render a tooltip when a description exists. Match the existing tooltip usage pattern in `RulesPicker.tsx`.

### E2. "Slots" → "Column Type(s)" (item 10)

i18n string-value change only (no key renames), across all four locales (`en.json` source, then `pt-BR.json`, `it.json`, `es.json`). Keys under `rulesRegistry.*`:
- `colSlots`: "Slots" → "Column Type(s)"
- Review the related slot keys for label consistency (`slotsLabel`, `slotsPanelTitle`, etc.) and update the *user-facing label* strings to the "Column Type(s)" phrasing where they currently say "slots"/"Column slots", keeping hint prose readable. Translate each into pt-BR/it/es (don't leave English behind).
- Do **not** rename i18n keys or code identifiers (`slots` column id, `slot.family`) — label text only.

---

## Sequencing

1. **B** (object permissions) — keystone; migration + EXECUTE + materialized rows + dialog. Most files, highest risk.
2. **A** (roles) — depends on B's boundary semantics being clear; matrix + run-route gates + UI gating + role-in-dropdown.
3. **C** (profiler draft) — independent of A/B but medium risk; needs runtime verification.
4. **D** (runs status) — independent; frontend polling.
5. **E** (small UI) — trivial; last.

## Delivery

- Subagent-driven development: one implementer + one reviewer per task; controller does not implement delegated tasks.
- Cover changes with tests: backend pytest (permissions matrix, boundary, migration, run-gate), bun tests (permission-utils auto-select, dialog logic). Extend `test_entitlements_hard_boundary.py`.
- Runtime verification (not just unit tests) for B (permission delete + EXECUTE gate), C (profiler draft populate/save), D (live update + cross-launch spinners) — these are the classes of bug that pass unit tests but fail in the running app.
- Deploy to fe-sandbox (`make app-deploy PROFILE=fe-sandbox-dq-demo-2 TARGET=dev`) at the end for user verification.
- **Do not push commits** until the user says go. Deploying for test is fine.

## Known tradeoffs surfaced to user

- Dropping `runner` with no migration silently demotes runner-only groups to viewer until re-mapped to author.
- EXECUTE in the day-one default preserves current run behavior but means every existing object (post-backfill) is runnable by any author until an admin narrows it — matching today, now narrowable.
