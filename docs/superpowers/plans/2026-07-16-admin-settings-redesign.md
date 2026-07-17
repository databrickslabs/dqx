# Admin Settings Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the DQX Studio admin settings page (`config.tsx`) — restructure tabs, standardise copy to sentence case, unify save behaviour (auto-save, no Save buttons/dividers), and rework each tab's cards per the detailed spec below.

**Architecture:** Nearly the entire settings UI lives in one file: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx` (~3000 lines). Each card is a private local function component. Cross-file reusable pieces: `components/RoleManagement.tsx`, `components/permissions/PrincipalPicker.tsx`, `components/rules/test/RuleTestPanel.tsx` (`SampleSelector`), shadcn `components/ui/*`. All copy is i18n via `t("config.xxx")` / `t("roleManagement.xxx")`. dqlake reference lives at `/Users/oliver.gordon/Documents/Code/Other/databricks-dqwatch`.

**Tech Stack:** React 19 + TypeScript (strict), TanStack Router/Query, shadcn/ui, Tailwind CSS 4, Lucide icons, react-i18next, orval-generated API hooks. Backend: FastAPI + Pydantic + Databricks SDK.

## Global Constraints

These bind EVERY task. Copied verbatim from the spec + repo constraints:

1. **i18n parity is a CI gate.** All four locale files (`ui/lib/i18n/locales/{en,es,it,pt-BR}.json`) MUST have identical key SETS and identical `{{placeholder}}` sets per key (enforced by `app/tests/test_i18n_locale_parity.py`, run via `make app-test`). Therefore: **adding a key → add it to all 4 files**; **removing a key → remove it from all 4**; **changing English display text → edit `en.json` only** (leave the other three as-is; stale translations are parity-valid). For a NEW key, copy the English value into es/it/pt-BR (do not machine-translate). Never leave a key in only some files.
2. **All titles in sentence case, consistently.** e.g. "Display timezone" (not "Display Timezone"), "Run review statuses", "Global results tab", "Rules Registry" (proper noun keeps its casing), "Data retention", "Draft runs", "Danger zone". Do a pass on EVERY setting title and option label in your task's scope.
3. **No "Save changes" buttons and no horizontal dividers between/within cards.** Saving must work like the existing auto-save cards: mutate on change (`onCheckedChange`/`onValueChange`/`onBlur`/debounced `onChange`) → `queryClient.invalidateQueries(...)` → `toast.success/error`. Remove every `border-t` divider row and every Save/Reset button row. Reference the existing auto-save idiom in `TimezoneSettings`, `RulesRegistrySettingsCard`, `ApprovalsModeCard`, `PermissionsSettingsCard`.
4. **Consistent card spacing.** Inter-card gap stays `space-y-6` (on `TabsContent`). Within every card, standardise `CardContent` to `className="space-y-4"` (fix the outliers currently on `space-y-5`/`space-y-3`). Toggle rows: `flex items-start gap-3` (label + muted `<p className="text-xs text-muted-foreground">` stack) OR the existing `flex items-center justify-between rounded-md border p-3` — pick ONE idiom per card and keep spacing uniform.
5. **dqlake ports are LITERAL code copies.** When porting a component/pattern from dqlake, copy its structure/classNames verbatim and re-point the data hooks to DQX's — do NOT invent a "cleaner" equivalent. See `docs/superpowers/notes/dqlake-reference.md`.
6. **Copy is plain and confident.** No gimmicky/over-enthusiastic wording. Match the exact target strings given in each task.
7. **Verification per task (frontend):** `cd app && make app-check` (tsc + basedpyright) must pass, and `make app-test K=i18n` (locale parity) must pass. There is no meaningful unit-test surface for pure restyles; correctness is verified by tsc + parity + a browser smoke check where noted.
8. **CardTitle idiom:** `<CardTitle className="flex items-center gap-2">` + a 20px (`h-5 w-5`) lucide icon, matching existing cards.

Reference notes (read before starting your task):
- `docs/superpowers/notes/dqx-settings-map.md` — current DQX settings structure, exact line anchors, save patterns, reuse targets.
- `docs/superpowers/notes/dqlake-reference.md` — dqlake components to port (Compute pickers, AI Gateway link, layout idiom, version pinning).

---

### Task 1: Tab bar & shell restructure

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/routes/_sidebar/config.tsx` (tab type ~2831, tabs array ~2855-2866, `entries` array ~2868-2889, tab bar render ~2954-2993, icon imports near top)
- Modify: `ui/lib/i18n/locales/{en,es,it,pt-BR}.json` (only if tab labels change — they don't, keep existing `config.tab*` keys)

**Interfaces:**
- Produces: the final tab order and per-tab card membership that all later tasks assume. Later tasks edit card internals only; this task owns which tab each card renders under and the tab order.

**Changes:**

- [ ] **Step 1: Reorder tabs.** The tabs array order must become: **General**, then the current middle tabs in **alphabetical order** (AI, Compute, Entitlements, Governance, Tags), then **Danger zone** last. Final `SettingsTabId` order: `general, ai, compute, entitlements, governance, tags, danger`. Reorder both the `tabs` array (2855-2866) and keep the type union consistent.

- [ ] **Step 2: General tab icon.** Change the General tab's icon from `Globe` to `SlidersHorizontal` (import from `lucide-react`; drop the `Globe` import if now unused). AI tab keeps `Sparkles`.

- [ ] **Step 3: AI gradient on the AI tab trigger.** When rendering the AI tab's `TabsTrigger`, apply the AI gradient styling used elsewhere: import `AI_TEXT_GRADIENT` and `AI_ICON_COLOR` from `@/lib/ai-style` (already imported in this file at ~127) and apply `AI_ICON_COLOR` to the AI trigger's `Sparkles` icon and wrap/label the AI trigger's text with `AI_TEXT_GRADIENT` (`bg-clip-text text-transparent bg-gradient-to-r ...`). Follow the same conditional pattern the danger tab uses (2972) — a per-tab `cn(...)` branch keyed on `tab.id === "ai"`. The gradient shows on the AI tab always (not only when active), mirroring the AI settings card.

- [ ] **Step 4: Reassign card→tab membership in the `entries` array.** Per spec:
  - **General** must contain ONLY: Display timezone (`TimezoneSettings`) and Global results tab (`GlobalResultsSettingsCard`).
  - **Run review statuses** (`RunReviewStatusesSettings`) moves from General to **Governance** (`tab: "governance"`). (Its restyle happens in Task 4; here just move its entry.)
  - Confirm AI/Compute/Entitlements/Governance/Tags/Danger memberships otherwise unchanged. `DeployDemoCard` stays on `danger`.

- [ ] **Step 5: Verify.** `cd app && make app-check && make app-test K=i18n`. Then `make app-start-dev`, load `/config`, confirm tab order is General → AI → Compute → Entitlements → Governance → Tags → Danger zone, the AI tab shows gradient text + fuchsia icon, General shows the sliders icon, and Run review statuses now appears under Governance. Screenshot the tab bar.

- [ ] **Step 6: Commit.** `git add -A && git commit -m "feat(config): restructure settings tab bar (order, icons, AI gradient)"`

---

### Task 2: General tab — Timezone & Global results

**Files:**
- Modify: `config.tsx` — `TimezoneSettings` (~187), `GlobalResultsSettingsCard` (~2271)
- Modify: `ui/lib/i18n/locales/en.json` (values); ensure other 3 locales keep parity

**Changes:**

- [ ] **Step 1: Timezone title → sentence case.** `config.timezoneTitle`: `'Display Timezone'` → `'Display timezone'` (edit `en.json` only). Ensure `CardContent` uses `space-y-4`.

- [ ] **Step 2: Global results — remove the description paragraph.** Remove the rendered `config.globalResultsDescription` text ("An app-wide view of data quality across every monitored table. Off by default, since per-table, per-space, and per-rule results are already available."). Delete the `<p>` that renders `config.globalResultsDescription` from the component. Then **remove the now-unused `config.globalResultsDescription` key from all 4 locale files** (parity).

- [ ] **Step 3: Global results — reword the hint.** `config.globalResultsHint`: `'Adds a Results item to the sidebar and a short explainer to the Home score card.'` → `'Adds a results link to the sidebar'` (edit `en.json` only).

- [ ] **Step 4: Title sentence case.** `config.globalResultsTitle`: `'Global Results tab'` → `'Global results tab'` (en.json).

- [ ] **Step 5: Verify.** `make app-check && make app-test K=i18n`. Browser: `/config` → General shows only Display timezone + Global results tab; Global results has no long description, hint reads "Adds a results link to the sidebar".

- [ ] **Step 6: Commit.** `git commit -am "feat(config): General tab — timezone + global results copy"`

---

### Task 3: AI tab

**Files:**
- Modify: `config.tsx` — `AiSettingsCard` (~1813), `ServingEndpointSelect` (~1767)
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Rename card title.** `config.aiSettingsTitle`: `'AI settings'` → `'AI'` (en.json).

- [ ] **Step 2: Remove the card description.** Remove the rendered `config.aiSettingsDescription` ("Choose the serving endpoint behind AI-assisted authoring and the rule suggester.") `<p>` from `AiSettingsCard`, then remove `config.aiSettingsDescription` from all 4 locales.

- [ ] **Step 3: Add "Manage in AI Gateway" link to the endpoint option description.** Port dqlake's pattern (see notes §2). Render, as part of the model-serving-endpoint option's description area, an anchor:
  - text: exactly `Manage in AI Gateway` (add a new key `config.aiSettingsManageLink = 'Manage in AI Gateway'` to all 4 locales).
  - href: `${workspaceHost}/ml/endpoints/${encodeURIComponent(endpoint)}` when an endpoint is selected, else `${workspaceHost}/ml/endpoints`.
  - `target="_blank" rel="noopener noreferrer"`, className `text-xs text-primary hover:underline inline-flex items-center gap-1`, followed by an `ExternalLink` icon (`h-3 w-3`).
  - **Find the DQX workspace-host source**: grep for `workspace_host` / `workspaceUrl` / `host` in the current-user/me hooks (`useCurrentUserRole`, `@/lib/api`, `@/hooks/use-suspense-queries`). If DQX exposes a workspace host on the current-user/me payload, use it; if not present, fall back to a relative `/ml/endpoints/...` link (still functional in-workspace). Only render the link when a host is resolvable.

- [ ] **Step 4: Remove the Save button — auto-save the endpoint.** Convert `AiSettingsCard` to auto-save (Global Constraint 3): the endpoint `Select`'s `onValueChange` should call the existing save mutation directly and invalidate + toast; remove the `config.aiSettingsSaveButton` button row and any `isDirty`/draft state. Keep the enable/disable `Switch` auto-saving too if it isn't already. Then remove `config.aiSettingsSaveButton` from all 4 locales (if no longer referenced).

- [ ] **Step 5: Verify.** `make app-check && make app-test K=i18n`. Browser: AI tab titled "AI", no description paragraph, endpoint picker shows a "Manage in AI Gateway ↗" link, changing the endpoint saves immediately (toast), no Save button.

- [ ] **Step 6: Commit.** `git commit -am "feat(config): AI tab — rename, AI Gateway link, auto-save"`

---

### Task 4: Run review statuses (now under Governance) — restyle to match Rule Labels

**Files:**
- Modify: `config.tsx` — `RunReviewStatusesSettings` (~1329), referencing `AllowedValuesEditor` (~612-847) / `DefinitionEditorCard` (~849) for the target style
- Modify: `en.json` (+ parity)

**Context:** The user is separately reworking the Tags → Rule Labels "allowed values" mini-section (Task 8). This task must **match that section's final style** — align with the layout Task 8 produces (one-line Value + full-width Description). Read Task 8 before implementing so both end up consistent.

**Changes:**

- [ ] **Step 1: Reword the card description.** Replace the current description with exactly: `Statuses used when reviewing results of each run via the Results tab`. (Find the current description string in the component / its `config.*` or inline key; set en.json to the new text.)

- [ ] **Step 2: Remove the "renaming doesn't change past runs" note and its mini-box.** Delete the note text "Renaming a value doesn't change past runs — they keep the label they were tagged with." AND remove the bordered mini-box/container it lives in. Remove the corresponding i18n key(s) from all 4 locales.

- [ ] **Step 3: Remove the per-status "Default for new runs" / "Make default" toggle**, EXCEPT keep it for the pre-existing **"Pending review"** status: that one shows the default indicator toggled ON and **disabled** (non-editable). All other statuses lose the default toggle entirely.

- [ ] **Step 4: Remove "(optional)" text** wherever it appears in this card.

- [ ] **Step 5: Restyle to match Rule Labels allowed-values.** Rework the status list rows to mirror the `AllowedValuesEditor` row style (color dot + value + full-width description, one line), per Task 8's final layout. Remove any Save/Reset button row and dividers — auto-save on change (Global Constraint 3).

- [ ] **Step 6: Sentence-case the title.** `config.reviewStatusesTitle` "Run review statuses" is already sentence case — confirm.

- [ ] **Step 7: Verify.** `make app-check && make app-test K=i18n`. Browser: Governance tab now shows Run review statuses; it visually matches the Rule Labels allowed-values style; only "Pending review" has a locked default; no mini-box, no "(optional)", no Save button.

- [ ] **Step 8: Commit.** `git commit -am "feat(config): restyle run review statuses to match rule labels; move to Governance"`

---

### Task 5: Governance — Rules Registry card

**Files:**
- Modify: `config.tsx` — `RulesRegistrySettingsCard` (~1954)
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Rename card title.** `config.rulesRegistrySettingsTitle`: `'Rules Registry governance'` → `'Rules Registry'` (keep the proper-noun capitalisation "Rules Registry").

- [ ] **Step 2: Move "Auto-approve upgrades" OUT of this card.** The `auto_upgrade_without_approval` toggle moves to the Approvals workflow card (Task 6). Remove it from `RulesRegistrySettingsCard` here. (Task 6 adds it there. Coordinate: this card should end with only `default_auto_upgrade` and `tag_auto_apply`.)

- [ ] **Step 3: Rename + re-describe "Follow latest version by default".**
  - Label `config.defaultAutoUpgradeLabel`: `'Follow latest version by default'` → `'Upgrade rule mappings by default'`.
  - Description `config.defaultAutoUpgradeHint`: make it **state-dependent** on the toggle value:
    - When enabled: `When applying a new rule to a table, default to always running the "latest" version of that rule. Can be overridden on a per-rule basis.`
    - When disabled: `When applying a new rule to a table, default to always explicitly pinning the most recent version of that rule. Can be overridden on a per-rule basis.`
  - Implement by adding a second key: keep `config.defaultAutoUpgradeHint` for the enabled text and add `config.defaultAutoUpgradeHintOff` for the disabled text (add to all 4 locales); render whichever matches the current toggle state.

- [ ] **Step 4: Re-describe "Auto-apply rules by tag".** `config.tagAutoApplyHint` → `When on, automatically apply rules matching Unity Catalog tags of the column in question. When off, offer these rules as suggestions instead` (en.json only).

- [ ] **Step 5: Spacing/title pass.** `CardContent` → `space-y-4`; ensure auto-save (already is).

- [ ] **Step 6: Verify.** `make app-check && make app-test K=i18n`. Browser: card titled "Rules Registry", has "Upgrade rule mappings by default" (description flips with the toggle) and "Auto-apply rules by tag" (new description); no auto-approve toggle here.

- [ ] **Step 7: Commit.** `git commit -am "feat(config): Rules Registry card — rename, reword, relocate auto-approve"`

---

### Task 6: Governance — Approvals workflow card (+ receives auto-approve-upgrades)

**Files:**
- Modify: `config.tsx` — `ApprovalsModeCard` (~2056), consuming the `auto_upgrade_without_approval` setting moved from Task 5
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Remove the "enabled" mode hint.** `config.approvalsMode_enabled_hint`: `'Authors submit for review; an approver or admin approves.'` — remove it from the rendered UI and delete the key from all 4 locales. (Keep the other two mode hints.)

- [ ] **Step 2: Add the "Bypass approvals for automatic rule upgrades" toggle into this card.** This is the `auto_upgrade_without_approval` setting relocated from Rules Registry (Task 5). Render it as a toggle within `ApprovalsModeCard`, below the mode select.
  - Label: rename `config.autoUpgradeWithoutApprovalLabel` `'Auto-approve upgrades on already-following rules'` → `'Bypass approvals for automatic rule upgrades'`.
  - Description `config.autoUpgradeWithoutApprovalHint` → `Automatically approve changes to tables when rule versions are upgraded` (en.json).
  - Reuse the existing `default_auto_upgrade`/`auto_upgrade_without_approval` save hook (the Rules Registry settings mutation) — the setting key does not change, only where it renders.

- [ ] **Step 3: Gate the toggle on approvals mode.** When Approvals mode is **off** (`disabled`), the "Bypass approvals for automatic rule upgrades" toggle must be **disabled AND set to off**. Implement: if approvals mode value is `"disabled"`, render the switch `disabled` and `checked={false}`, and when approvals mode is changed to `disabled`, persist the bypass setting to `false`.

- [ ] **Step 4: Title sentence case.** `config.approvalsModeTitle` "Approvals workflow" — confirm sentence case.

- [ ] **Step 5: Verify.** `make app-check && make app-test K=i18n`. Browser: Approvals workflow card has no "Authors submit..." hint; contains "Bypass approvals for automatic rule upgrades"; setting approvals mode to "No approval needed" disables + turns off the bypass toggle.

- [ ] **Step 6: Commit.** `git commit -am "feat(config): Approvals workflow — move+rename bypass toggle, gate on mode"`

---

### Task 7: Governance — Draft runs card (rename from "Require a draft run before submit")

**Files:**
- Modify: `config.tsx` — `RequireDraftRunSettingsCard` (~2135)
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Rename card title.** `config.requireDraftRunTitle`: `'Require a draft run before submit'` → `'Draft runs'`.

- [ ] **Step 2: Remove the card description.** Remove the rendered `config.requireDraftRunDescription` ("Require a draft run before a monitored table, table space, or per-table rule can be submitted for review, so checks are tested first. Registry rules and cross-table SQL checks aren't gated.") and delete the key from all 4 locales.

- [ ] **Step 3: Reword the option description.** `config.requireDraftRunHint` → `Require a test/draft run before a rule, monitored table, or table space can be submitted for review, so checks are tested first` (en.json).

- [ ] **Step 4: Verify.** `make app-check && make app-test K=i18n`. Browser: card titled "Draft runs", no long description, option hint reworded.

- [ ] **Step 5: Commit.** `git commit -am "feat(config): rename Require-draft-run card to Draft runs; reword"`

---

### Task 8: Governance — Data retention card

**Files:**
- Modify: `config.tsx` — `RetentionSettings` (~945-1149)
- Modify: `en.json` (+ parity)

**Context:** Backend stores retention in **days**. Current defaults: quarantine = 30 days, global = 90 days. The UI must present a small number box + a unit picker (days/months/years). Mapping: 30 days = "1 month", 90 days = "3 months" (1 month = 30 days, 1 year = 365 days). Store days on save (convert from the chosen unit). Keep the backend 7-day floor behaviour — just don't surface the copy about it.

**Changes:**

- [ ] **Step 1: Reword the card description.** Replace the long description ("How long runs and quarantined rows are kept before the daily cleanup removes them...") with exactly: `How long runs and quarantined rows are kept`.

- [ ] **Step 2: Reorder — Invalid results first.** Move the "Quarantine retention" field ABOVE the global field, and **rename it "Invalid results"**.

- [ ] **Step 3: Rename the global field.** "Global retention (days)" → `All other data`.

- [ ] **Step 4: Reword the global sub-hint.** "Applies to run history, profiling results, and metrics." → `Includes run profiling results, history, and metrics`.

- [ ] **Step 5: Remove the "Default N days (not yet customised)" hints** for BOTH fields (the "Default 90 days (not yet customised)" and "Default 30 days ..." strings). Remove the associated i18n key(s) from all 4 locales.

- [ ] **Step 6: Compact number input + unit picker.** For each field: render a **narrow** number `Input` (wide enough for ~3 digits only — e.g. `className="w-16"` / `w-20`), **left-aligned** to the card, immediately followed by a unit `Select` with options **days / months / years**. Add i18n keys `config.retentionUnitDays='days'`, `config.retentionUnitMonths='months'`, `config.retentionUnitYears='years'` (all 4 locales). Conversion helper: days→{value,unit} picks the largest exact unit (365→1 year, 30→1 month, else days); {value,unit}→days multiplies (years×365, months×30, days×1).
  - **Defaults:** Invalid results defaults to **1 month** (30 days); All other data defaults to **3 months** (90 days).

- [ ] **Step 7: Remove reset/restore-defaults buttons AND the Save button — auto-save.** Delete the Save changes + Reset/Restore-defaults button row and its `border-t` divider. Auto-save on change: persist on the number `onBlur`/`onChange` (debounced) and on unit `onValueChange`, converting to days, then invalidate + toast. Remove now-unused i18n keys (`config.reset`, restore-defaults strings, etc.) from all 4 locales **only if no other card still references them** — grep first (`config.reset` is shared with Labels; do not remove shared keys).

- [ ] **Step 8: Verify.** `make app-check && make app-test K=i18n`. Browser: Data retention shows "Invalid results" (top, default 1 month) then "All other data" (default 3 months); each is a small left-aligned number + days/months/years picker; no Save/Reset buttons; changing a value saves immediately.

- [ ] **Step 9: Commit.** `git commit -am "feat(config): Data retention — reorder, unit picker, auto-save"`

---

### Task 9: Tags tab — split Built-in / Custom, restyle Rule Labels

**Files:**
- Modify: `config.tsx` — `LabelDefinitionsSettings` (~347), `DefinitionEditorCard` (~849-930), `AllowedValuesEditor` (~612-847), `ValueColorEditor` (~518)
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Split into two cards.** Replace the single "Rule Labels" card with two cards: **"Built in tags"** and **"Custom tags"**. Built-in definitions (`is_builtin` / reserved keys `RESERVED_WEIGHT_KEY`, `RESERVED_SEVERITY_KEY`) render in "Built in tags"; the rest in "Custom tags". Add i18n keys `config.builtinTagsTitle='Built in tags'` and `config.customTagsTitle='Custom tags'` (all 4 locales).

- [ ] **Step 2: Remove the "Create new label" button from the Built-in card.** Only the Custom tags card gets an add button. Rename it "Create new label" → `Add new key` (`config.addLabelDefinition`: `'Create new label'` → `'Add new key'`).

- [ ] **Step 3: Full-width description + inline bin.** In `DefinitionEditorCard`, make the definition "description" input **full width**, and move the delete (bin) icon to be **inline with / after** the description input (same row), not in a separate header/corner.

- [ ] **Step 4: Severity default description.** For the severity definition (`RESERVED_SEVERITY_KEY`), set its default description to `Default rule severity of a rule, overridable on a per-table basis.` (surface via the existing description field / placeholder as appropriate).

- [ ] **Step 5: Allowed values — one line Value + Description.** In `AllowedValuesEditor`, when a value row is expanded, put Value and Description on **one line**: keep Value where it is (its current width), and make Description take the **rest of the full width**. (Not stacked.)

- [ ] **Step 6: Move "Allow custom values" to the bottom.** Relocate the "Allow custom values" checkbox (`config.allowCustomValues`) to sit **just after the "Add value" button** at the bottom of the allowed-values section (currently it's near the top by the "Allowed values" label).

- [ ] **Step 7: Font/weight/size consistency pass.** Review all fields/inputs in these cards so fonts, weights, and text sizes match the rest of the app (labels `text-sm`, hints `text-xs text-muted-foreground`, inputs default shadcn sizing). Fix any oversized/mismatched text.

- [ ] **Step 8: Remove Save changes / Reset buttons and the horizontal bar — auto-save.** Convert `LabelDefinitionsSettings` to auto-save (persist definitions on change/blur). Remove the `config.saveChanges`/`config.reset` button row and its `border-t` divider from these cards. (Do NOT delete `config.saveChanges`/`config.reset` keys if referenced elsewhere — grep first.)

- [ ] **Step 9: Verify.** `make app-check && make app-test K=i18n`. Browser: Tags tab has two cards (Built in tags / Custom tags); built-in has no add button; Custom has "Add new key"; description is full width with an inline bin; expanded allowed-value shows Value + full-width Description on one line; "Allow custom values" is after "Add value"; no Save/Reset buttons.

- [ ] **Step 10: Commit.** `git commit -am "feat(config): Tags — split built-in/custom, restyle labels, auto-save"`

---

### Task 10: Entitlements — backend endpoint for workspace admins & app owners

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/routes/v1/roles.py` (add endpoint) OR a new route module
- Modify: `backend/models.py` (add response model)
- Modify: `backend/services/role_service.py` (or a small helper) for the SCIM/app-permissions lookup
- Test: `app/tests/test_roles_route.py` (or the nearest existing roles/role-service test file — grep `tests/` for `role`)

**Interfaces:**
- Produces: `GET /api/v1/roles/privileged-principals` (operation_id `listPrivilegedPrincipals`) → `list[PrivilegedPrincipalOut]` where `PrivilegedPrincipalOut = { principal: str (display name/email), kind: Literal["workspace_admin","app_owner"] }`. The frontend (Task 11) renders these as disabled rows.

**Changes:**

- [ ] **Step 1: Write the failing test.** In the roles route/service test file, add `test_list_privileged_principals_returns_workspace_admins_and_app_owners`: with a `create_autospec(WorkspaceClient)` whose `groups.list`/`.get` returns an "admins" group with members A,B, and whose app-permissions lookup returns principal C with `CAN_MANAGE`, assert the endpoint returns `[{principal:A,kind:workspace_admin},{principal:B,kind:workspace_admin},{principal:C,kind:app_owner}]` (order not asserted; use a set).

- [ ] **Step 2: Run it to confirm it fails.** `cd app && .venv/bin/pytest tests/test_roles_route.py -k privileged -v` → FAIL (endpoint missing).

- [ ] **Step 3: Implement.** Add `PrivilegedPrincipalOut` to `models.py`. Add the endpoint (admin-gated like the other roles endpoints). Resolve:
  - **Workspace admins:** the members of the SCIM `admins` group (via `sp_ws.groups.list(filter='displayName eq "admins"', attributes="id,members")` then resolve member display names). Handle absence gracefully (return []).
  - **App owners (CAN_MANAGE on the app):** look up the app's permissions via the SDK (`sp_ws.apps.get` / the app permissions API — grep `apps` usage in backend; the app name/id is discoverable from config/metadata). Extract principals with `CAN_MANAGE`. If the SDK path is unavailable, degrade to `[]` and log — do not hard-fail the endpoint.
  - Follow the existing error handling idiom in `list_workspace_groups` (try/except → `HTTPException(500)`), and the log-injection guidance in AGENTS.md (don't interpolate raw principal names into logs without sanitising).

- [ ] **Step 4: Run the test to confirm it passes.** `.venv/bin/pytest tests/test_roles_route.py -k privileged -v` → PASS.

- [ ] **Step 5: Regenerate the API client.** `cd .. && make app-regen-api` (dumps OpenAPI + orval). Confirm `useListPrivilegedPrincipals` appears in `ui/lib/api.ts`.

- [ ] **Step 6: Commit.** `git commit -am "feat(backend): endpoint listing workspace admins & app owners for entitlements"`

---

### Task 11: Entitlements — frontend rework

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RoleManagement.tsx` (whole component; `AddRoleMappingForm` ~308, `RoleMappingRow` ~261, `GroupCombobox` ~102)
- Reuse: `components/permissions/PrincipalPicker.tsx` (exported `PrincipalPicker`, `PickedPrincipal`)
- Modify: `config.tsx` — `PermissionsSettingsCard` (~2202)
- Modify: `ui/lib/i18n/locales/en.json` (`roleManagement.*`, `config.permissions*`) + parity

**Interfaces:**
- Consumes: `useListPrivilegedPrincipals` from Task 10; `PrincipalPicker` (`onChange(picked: PickedPrincipal | null)`, users+groups).

**Changes (RoleManagement / Entitlements):**

- [ ] **Step 1: Reword the top explanatory text, consistent with other cards.** `roleManagement.description`: `'Map Databricks workspace groups to application roles. Users inherit the highest-priority role from their group memberships.'` → `Map Databricks workspace groups to application roles. If applied to multiple entitlements, users inherit the highest level of permission.` Render it in the same muted `<p className="text-xs text-muted-foreground">` idiom as other cards' descriptions (make the internal layout — especially the top explanatory text — match the other settings cards).

- [ ] **Step 2: Replace the caching paragraph.** Remove `roleManagement.delayBody` ("Each user's resolved role is cached client-side for 60 seconds...") and replace it with: `Alternatively, hard refresh the page with ⌘+R or Ctrl+R (context depending on OS!!!)`. Set `roleManagement.delayBody` (en.json) to that text — keep it OS-neutral by showing both shortcuts. (Keep the key; just change the value.)

- [ ] **Step 3: Reword the delay title.** `roleManagement.delayTitle`: `'Role changes take up to ~1 minute to reach an active session.'` → `Role changes take ~1 minute to take effect.` (en.json).

- [ ] **Step 4: Rename "Role" → "Entitlement".** `roleManagement.role`: `'Role'` → `'Entitlement'`; `roleManagement.selectRole`: keep as "Select entitlement..." — update value to `'Select entitlement...'`.

- [ ] **Step 5: Replace the Databricks Group dropdown with the DQ-Steward picker.** Swap `GroupCombobox` for `PrincipalPicker` (the same UI used to select DQ Stewards). Rename its label `roleManagement.databricksGroup`: `'Databricks Group'` → `'User/Group'`. Make the picker **less wide** (constrain its width, e.g. `w-56`/`max-w-xs`). The picker returns a `PickedPrincipal` (user or group); map it into the create-mapping payload (the backend `createRoleMapping` currently keys on `group_name` — pass the principal's group/user identifier; if the mapping API is group-only, confirm it also accepts users, else note the limitation and still support groups. Grep `useCreateRoleMapping` payload shape first).

- [ ] **Step 6: Widen the Entitlement dropdown; show only the entitlement name.** Make the entitlement (formerly Role) `Select` a bit **wider**. When an entitlement is selected, the trigger must show ONLY the entitlement name, **left-aligned** — not the description, not centered. (The dropdown items may still show descriptions, but the selected/trigger display is name-only, left-aligned.) You may remake/reuse a simpler `Select` for this.

- [ ] **Step 7: Add-on-select (no Add button for the pending row).** Selecting a User/Group + Entitlement should take effect immediately (create the mapping on selection) — remove the requirement to press "Add" for the in-progress selection.

- [ ] **Step 8: Repurpose the "Add" button.** Move the button to **below the existing entitlement rows** and relabel it `Add new entitlement` (update `roleManagement.add` or add `roleManagement.addEntitlement='Add new entitlement'`). Clicking it reveals a fresh User/Group + Entitlement selector row (which itself auto-adds on selection per Step 7).

- [ ] **Step 9: Surface privileged principals as disabled rows.** Using `useListPrivilegedPrincipals` (Task 10), render workspace admins and app owners as **disabled/non-editable** rows in the entitlements list. Each such row shows the principal and, just after the Entitlement dropdown position, a muted suffix: `(workspace admin)` for `kind==="workspace_admin"` and `(app owner)` for `kind==="app_owner"`. These rows have no delete affordance and their controls are hardcoded/disabled. Add i18n keys `roleManagement.suffixWorkspaceAdmin='(workspace admin)'`, `roleManagement.suffixAppOwner='(app owner)'` (all 4 locales).

**Changes (Permissions card):**

- [ ] **Step 10: Rename "Permission inheritance" → "Permissions".** `config.permissionsDefaultInheritTitle`: `'Permission inheritance'` → `'Permissions'`. Remove the explanatory text `config.permissionsDefaultInheritHelp` (the "Sets the default for new grants..." paragraph) from the UI and delete that key from all 4 locales.

- [ ] **Step 11: Rename + re-describe the toggle.** `config.permissionsDefaultInheritLabel`: `'Inherit new grants by default'` → `'Cascade permissions by default'`. Set `config.permissionsDefaultInheritHint` → `When granting permission to a table space or monitored table, also by default grant SELECT permissions to underlying monitored tables and rules` (en.json).

- [ ] **Step 12: Verify.** `make app-check && make app-test K=i18n`. Browser: Entitlements card matches other cards' layout; "User/Group" uses the steward picker (narrower); "Entitlement" dropdown is wider and shows name-only left-aligned; selecting both auto-adds; "Add new entitlement" sits below rows; workspace admins/app owners appear as disabled rows with the right suffix; Permissions card renamed with "Cascade permissions by default".

- [ ] **Step 13: Commit.** `git commit -am "feat(config): Entitlements rework + Permissions card rename"`

---

### Task 12: Compute tab — dqlake port + Draft runs

**Files:**
- Modify: `config.tsx` — `ComputeSettingsCard` (~2332), `WarehouseSelect` (~1652), `ClusterSelect` (~1707), jobs-compute kind Select, `DraftRunSampleLimitSettings` (~1150)
- Reuse/extract: `components/rules/test/RuleTestPanel.tsx` — `SampleSelector` (~353-402) needs exporting for reuse
- Reference: dqlake `ComputePickerCard.tsx`, `EvalWarehouseCard.tsx`, `AiToggleCard.tsx` (see notes §1, §3)
- Modify: `en.json` (+ parity)

**Context:** The Compute picker "fails to load most of the time (needs a hard page refresh)". The card uses non-suspense hooks (`useGetComputeSettings`, `useListComputeWarehouses`, `useListComputeClusters`) with a `hydrated` guard (`config.tsx:2355-2364`). Investigate the load race with superpowers:systematic-debugging before restyling.

**Changes:**

- [ ] **Step 1: Diagnose & fix the load failure.** Reproduce (load `/config` → Compute; observe it often shows blank/error until hard refresh). Likely causes: the `hydrated` one-shot effect not re-running after a late `settings` fetch, or a Suspense/query-cache ordering issue vs. the shared `SettingSection` boundary. Fix so the picker reliably loads on first navigation without a hard refresh (e.g. correct the hydration effect dependency, or move to suspense hooks consistent with the other cards). Confirm by navigating away and back to Compute repeatedly.

- [ ] **Step 2: Restyle the Compute picker like the Approvals mode option.** The current picker is inconsistently styled — restyle it to match `ApprovalsModeCard`'s shadcn `Select` idiom (label + muted hint + `Select`). Keep the SP warehouse-access warning + grant button.

- [ ] **Step 3: Correct the options to mirror dqlake.** Per notes §1, the option set: **Jobs compute** = {Serverless, All-purpose cluster (pick one)}; **SQL warehouse** = serverless & classic warehouses grouped. Make DQX's option lists mirror dqlake's semantics (serverless vs classic warehouse grouping; serverless vs existing-cluster jobs compute). Reconcile against DQX's `ComputeSettingsIn`/`JobsComputeModel` (already has `serverless`/`existing_cluster` + `sql_warehouse_id`). Fix any incorrect/stale option labels. (If the backend model lacks a needed field, note it — but prefer frontend-only reconciliation.)

- [ ] **Step 4: Remove the Compute Save button — auto-save.** Convert `ComputeSettingsCard` off the Save-button pattern to auto-save on change (Global Constraint 3), matching the other pickers.

- [ ] **Step 5: Draft runs — rename + reuse SampleSelector.** Rename `DraftRunSampleLimitSettings` card title "Draft run sampling" (`config.draftSampleTitle`) → `Draft runs`. Replace the bespoke single number input with the **`SampleSelector`** control from `RuleTestPanel.tsx` (the "Random sample" / "1000" / "records" control from Rules Registry → Test → Table Test). Export `SampleSelector` (and its `SampleKind` type + props) from `RuleTestPanel.tsx`, import it here, and **re-plumb** it to the draft-run sample setting: map the existing `draft_sample_limit` (rows; 0 = whole table) to/from `SampleSelector`'s `{sampleKind, sampleValue}` (records ↔ number, full ↔ 0). Restyle consistently. Auto-save on change.

- [ ] **Step 6: Verify.** `make app-check && make app-test K=i18n`. Browser: Compute tab loads reliably on first navigation (no hard refresh); the picker is styled like Approvals mode with dqlake-correct options; "Draft runs" uses the Random-sample control; both auto-save.

- [ ] **Step 7: Commit.** `git commit -am "feat(config): Compute — fix load, restyle picker, reuse sample selector"`

---

### Task 13: Danger zone

**Files:**
- Modify: `config.tsx` — `DangerZoneCard` (~2574)
- Modify: `en.json` (+ parity)

**Changes:**

- [ ] **Step 1: Shorten the warning.** Replace the rendered warning body + preserved-note with just `This cannot be undone.`
  - Set `config.resetDbWarningBody` (en.json) → `This cannot be undone.`
  - Remove the rendered `config.resetDbPreservedNote` ("Your admin access is preserved...") from the UI and delete that key from all 4 locales.
  - (Keep `config.resetDbWarningHeading` behaviour as-is unless it duplicates; if the heading + body now read oddly together, keep only the single "This cannot be undone." line — use judgement, plain and confident.)

- [ ] **Step 2: Rename the button.** `config.resetDbButton`: `'Reset database…'` → `'Reset'`.

- [ ] **Step 3: Verify.** `make app-check && make app-test K=i18n`. Browser: Danger zone shows "This cannot be undone." and a "Reset" button; the type-to-confirm dialog still works.

- [ ] **Step 4: Commit.** `git commit -am "feat(config): Danger zone — shorten copy, rename button"`

---

## Cross-task notes

- **Task ordering:** Task 1 (shell) first — it owns tab order + card→tab membership. Task 10 (backend) must precede Task 11 (needs the generated hook). Task 8 (Tags allowed-values restyle) informs Task 4 (Run review statuses) — implement Task 8's allowed-values layout decisions and reference them in Task 4; if executing in numeric order, Task 4's implementer should read Task 9's allowed-values target. Consider running Task 9 before Task 4 to establish the shared style. Otherwise tasks are largely independent (all edit config.tsx, so run sequentially, never parallel implementers).
- **i18n key removals:** before deleting any `config.*` key, grep the whole `ui/` tree for other references — several keys (`config.reset`, `config.saveChanges`, `config.adminOnly`) are shared across cards.
- **Final whole-branch review** after Task 13, then squash-merge to `dqx-dqlake-integration`.

## Self-Review (completed)

- **Spec coverage:** tab bar/order/icons/AI-gradient (T1); sentence case + spacing + no-save-buttons/dividers (Global Constraints, applied per task); General timezone+global-results (T2); AI rename/desc/gateway-link (T3); run-review-statuses move+restyle (T4/T1); Rules Registry rename/move/reword (T5); Approvals bypass+gate (T6); Draft runs rename (T7); Data retention reorder/units/defaults/auto-save (T8); Tags split/restyle/rename/auto-save (T9); Entitlements backend (T10) + frontend rework + Permissions rename (T11); Compute load-fix/restyle/options/draft-runs (T12); Danger zone copy/button (T13). All spec bullets mapped.
- **Placeholders:** none — exact current→target strings given from the live `en.json`; structural anchors cited by line.
- **Design decisions made (per user delegation):** General icon = `SlidersHorizontal`; AI-tab gradient always-on; "Manage in AI Gateway" uses that literal text with dqlake's `/ml/endpoints/<endpoint>` href; retention unit mapping 30d=1mo, 90d=3mo, 365d=1yr; retention defaults 1 month / 3 months; privileged-principals endpoint shape `{principal, kind}`.
