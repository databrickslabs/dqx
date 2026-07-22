# Threshold Feedback Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Address 11 pieces of user feedback on the pass-threshold feature: relocate/consolidate the admin setting, add a feature-disable toggle, always-emit the effective threshold, forbid blanks, fix by-column apply + pill alignment + mixed-state, move the rule-form field, and consolidate exports into a single modal.

**Architecture:** Backend adds `pass_threshold_enabled` + folds `default_pass_threshold` into the existing **Rules Registry settings** model (one VIEWER+-readable endpoint → fixes an auth gap and gives the UI one hook for both values). A thin `use-pass-threshold-enabled` hook gates all threshold UI; when disabled, `dq_results.py` passes `resolve_threshold=None` so no breaches compute. The materializer resolves the effective threshold per column-group and emits it unconditionally (when enabled). Exports consolidate into one shadcn `ExportDialog`.

**Tech Stack:** FastAPI + Pydantic 2, React 19 + TanStack Query + shadcn/ui, orval-generated `api.ts`, react-i18next (en/pt-BR/it/es), pytest + bun test.

## Global Constraints

- **Threshold value is an integer percent [0,100]. Blanks are NOT allowed** anywhere in apply-time UI once the feature is enabled — the pill always shows a concrete number (per-column override → per-rule override → registry default → admin default).
- **Feature disable:** a new admin boolean `pass_threshold_enabled` (default **true**). When false: hide all threshold UI (pills in by-rule + by-column, the rule-form threshold field, results breach indicators) AND compute no breaches server-side (`resolve_threshold=None`). When the flag can't be read yet, default to **enabled** (fail-open to current behavior).
- **Single source of truth for both threshold admin values** = the existing Rules Registry settings endpoint (`getRulesRegistrySettings`/`saveRulesRegistrySettings`, `RulesRegistrySettingsOut`/`In`). The standalone `getDefaultPassThreshold` endpoint is ADMIN-only; do NOT rely on it for all-role consumers. Keep it working (settings card + tests may still use it) but the pills/rule-form/results read the default from the rules-registry settings hook.
- **Resolution precedence (unchanged):** per-column → per-rule → registry-rule default → admin default. `resolve_pass_threshold` (`backend/registry_models.py`) is the backend source of truth; frontend mirrors it (nullish `??`, 0 is a real value — never truthy checks).
- **i18n:** every new/changed user-facing string in all four locales (`ui/lib/i18n/locales/{en,pt-BR,it,es}.json`); en is source + fallback. Pluralize with `_one`/`_other`.
- **After backend model/route change:** `make app-regen-api` (never hand-edit `ui/lib/api.ts`). Always `git checkout app/uv.lock uv.lock` before staging. If `.superpowers/sdd/.gitignore` shows modified, `git checkout` it — never stage `.superpowers/`.
- **No lint suppressions** (project forbids `# noqa`/`# type: ignore`/`eslint-disable`). Ruff clean on changed backend files.
- Gates per task: `cd app && node_modules/.bin/tsc -b --incremental` (clean), `bun test src/databricks_labs_dqx_app/ui` (green), relevant `uv run --group test pytest <files> -q`, and `uv run --group test pytest tests/test_i18n_locale_parity.py -q` when locales change.
- Backend paths under `app/src/databricks_labs_dqx_app/backend/`, frontend under `app/src/databricks_labs_dqx_app/ui/`, tests under `app/tests/`.

---

## File Structure

**Backend**
- `backend/services/app_settings_service.py` — add `pass_threshold_enabled` get/save; extend rules-registry settings getter/saver to include `default_pass_threshold` + `pass_threshold_enabled` (T1).
- `backend/routes/v1/config.py` — extend `RulesRegistrySettingsOut`/`In` with the two fields (T1).
- `backend/services/materializer.py` — resolve + always-emit effective threshold per group, gated on enabled (T2).
- `backend/routes/v1/dq_results.py` — pass `resolve_threshold=None` when disabled (T2).

**Frontend**
- `ui/hooks/use-pass-threshold-enabled.ts` (new), `ui/hooks/use-default-pass-threshold.ts` (new or reuse) — thin readers off rules-registry settings (T3).
- `ui/routes/_sidebar/settings.tsx` — fold default-threshold control + enable toggle into `RulesRegistrySettingsCard`; remove standalone `default-pass-threshold` entry (T3).
- `ui/components/apply-rules/ThresholdPill.tsx` — no-blanks (T4).
- `ui/components/apply-rules/RuleConfigCard.tsx` + `RulesByColumn.tsx` + `monitored-tables.$bindingId.tsx` — mixed-state by-rule pill, right-align by-column pills, gate on enabled, fix apply-in-column bug (T5, T6).
- `ui/components/apply-rules/AddRulesDialog.tsx` + `shared.tsx` — map initialColumn into a slot, don't force lens (T6).
- `ui/components/RegistryRuleFormDialog.tsx` — move field to Implementation Advanced, show real default in monospace, gate on enabled (T7).
- `ui/components/ExportDialog.tsx` (new) + 5 call sites + results breach gating (T8, T9).

---

## Task 1: Backend — fold both threshold settings into Rules Registry settings + add enable flag

**Files:**
- Modify: `backend/services/app_settings_service.py`
- Modify: `backend/routes/v1/config.py`
- Test: `app/tests/test_app_settings_service.py`, the rules-registry-settings route test (`grep -rl getRulesRegistrySettings app/tests` → likely `tests/test_rules_registry_settings_route.py` or `test_config_routes`)

**Interfaces:**
- Produces: `AppSettingsService.get_pass_threshold_enabled() -> bool` (default **True**), `save_pass_threshold_enabled(enabled, *, user_email) -> bool`. `RulesRegistrySettingsOut` gains `default_pass_threshold: int` and `pass_threshold_enabled: bool`; `RulesRegistrySettingsIn` gains both as optional (only update when present). Existing `getDefaultPassThreshold`/`saveDefaultPassThreshold` stay as-is.

- [ ] **Step 1: Failing service tests** — in `test_app_settings_service.py`: `get_pass_threshold_enabled()` defaults True; save/get roundtrip false→false, true→true. Mirror `get_require_draft_run_before_submit` (default-False bool) but default **True** — copy the `get_permissions_default_inherit` pattern (`raw is None → True`) at app_settings_service.py:322-336.

- [ ] **Step 2: Run → fail** — `cd app && uv run --group test pytest tests/test_app_settings_service.py -k pass_threshold_enabled -q`.

- [ ] **Step 3: Implement service** — add near the default_pass_threshold block (~825):
```python
_PASS_THRESHOLD_ENABLED_KEY = "pass_threshold_enabled"

def get_pass_threshold_enabled(self) -> bool:
    """Master switch for the pass-threshold feature (default ON)."""
    raw = self.get_setting(self._PASS_THRESHOLD_ENABLED_KEY)
    return raw is None or raw.strip().lower() == "true"

def save_pass_threshold_enabled(self, enabled: bool, *, user_email: str | None = None) -> bool:
    self.save_setting(self._PASS_THRESHOLD_ENABLED_KEY, "true" if enabled else "false", user_email=user_email)
    return enabled
```

- [ ] **Step 4: Extend the rules-registry settings service methods** — find `get_rules_registry_settings`/`save_rules_registry_settings` (app_settings_service.py ~366-376, ~967-975). Add `default_pass_threshold` (via `get_default_pass_threshold()`) and `pass_threshold_enabled` (via `get_pass_threshold_enabled()`) to the returned dict/dataclass; in save, when the incoming payload carries those keys, delegate to `save_default_pass_threshold` / `save_pass_threshold_enabled`. (Reuse the existing single-value setters so both endpoints stay consistent.)

- [ ] **Step 5: Failing route test** — extend the rules-registry-settings route test: GET now returns `default_pass_threshold` (70 default) + `pass_threshold_enabled` (true default); PUT with `{pass_threshold_enabled: false}` persists; PUT with `{default_pass_threshold: 85}` persists and is readable via BOTH this endpoint and `getDefaultPassThreshold`. Assert GET is VIEWER+ (not ADMIN-only) — mirror the existing rules-registry GET role check.

- [ ] **Step 6: Run → fail**, then implement route models in `config.py`: extend `RulesRegistrySettingsOut` (add `default_pass_threshold: int`, `pass_threshold_enabled: bool`) and `RulesRegistrySettingsIn` (add both optional). Wire the handler to read/write via the service methods from Steps 3-4.

- [ ] **Step 7: Run → pass** — `pytest tests/test_app_settings_service.py tests/<rules_registry_route_test>.py tests/test_default_pass_threshold.py -q`.

- [ ] **Step 8: Regen + typecheck** — `make app-regen-api && cd app && node_modules/.bin/tsc -b --incremental`. Confirm `RulesRegistrySettingsOut` in `api.ts` carries both new fields.

- [ ] **Step 9: Commit** — `git checkout app/uv.lock uv.lock`; stage backend + api.ts + tests; `feat(threshold): add pass_threshold_enabled + fold default into rules-registry settings`.

---

## Task 2: Backend — always-emit effective threshold (materializer) + server-side disable

**Files:**
- Modify: `backend/services/materializer.py` (`render_check` ~287/392-395; `_iter_rendered_checks` loop ~634-656)
- Modify: `backend/routes/v1/dq_results.py` (`_build_threshold_resolver` ~389-419 + the resolver call sites ~810/918/1074/1372)
- Test: `app/tests/test_materializer.py`, `app/tests/test_dq_results_service.py` (or route test)

**Interfaces:**
- Consumes: `resolve_pass_threshold`, `get_rule_pass_threshold`, `get_applied_column_pass_thresholds` (registry_models), `AppSettingsService.get_default_pass_threshold` + `get_pass_threshold_enabled`. `render_check` already receives `version`, `app_settings`, `pass_threshold`, `per_application_tags`, `group`.

- [ ] **Step 1: Failing materializer test** — assert: with the feature enabled and NO per-rule/registry/column override, the rendered check's `user_metadata["pass_threshold"]` equals the admin default (e.g. "70") — NOT absent. With a per-column override on the group's mapped column, it equals that. With the feature DISABLED, `pass_threshold` is absent from user_metadata. (Mirror existing render_check tests in test_materializer.py.)

- [ ] **Step 2: Run → fail** — `pytest tests/test_materializer.py -k threshold -q`.

- [ ] **Step 3: Implement** — in `render_check`, replace the `if pass_threshold is not None:` block (392-395) with: if `app_settings.get_pass_threshold_enabled()` is false, emit nothing; else compute
```python
column_override = None
cols = _mapped_columns(group, definition.slots)  # helper already used at ~372
col_map = get_applied_column_pass_thresholds(per_application_tags)
overrides = [col_map[c] for c in cols if c in col_map]
column_override = max(overrides) if overrides else None  # strictest, matches T4 breach semantics
effective = resolve_pass_threshold(
    column_override=column_override,
    rule_override=pass_threshold,
    registry_default=get_rule_pass_threshold(version.user_metadata) if version else None,
    admin_default=app_settings.get_default_pass_threshold(),
)
user_metadata = {**user_metadata, "pass_threshold": str(effective)}
```
(Confirm `_mapped_columns` signature; if it returns normalized names, match against `col_map` keys the same way `get_applied_column_pass_thresholds` stores them.)

- [ ] **Step 4: Server-side disable** — in `dq_results.py`, where `_build_threshold_resolver` is called / `resolve_threshold=resolver` is passed (810/918/1074/1372), guard: `resolver = _build_threshold_resolver(...) if app_settings.get_pass_threshold_enabled() else None`. The results service already treats `resolve_threshold=None` as "no breach evaluation" (dq_results_service.py:617-628).

- [ ] **Step 5: Failing results test** — assert breach fields are all `False/None` when the feature is disabled (resolver None) even with failing checks. Extend `test_dq_results_service.py` TestBreach or the route test.

- [ ] **Step 6: Run → pass** — `pytest tests/test_materializer.py tests/test_dq_results_service.py -q`; `uv run ruff check` the two changed files.

- [ ] **Step 7: Commit** — `feat(threshold): materializer always emits effective threshold; disable flag skips breach eval`.

---

## Task 3: Frontend — settings relocation + enable toggle + reader hooks

**Files:**
- Create: `ui/hooks/use-pass-threshold-enabled.ts`, `ui/hooks/use-default-pass-threshold.ts`
- Modify: `ui/routes/_sidebar/settings.tsx` (fold into `RulesRegistrySettingsCard` ~2078-2158; remove `default-pass-threshold` entry ~3023 + the `DefaultPassThresholdSettings` component ~1293-1382)
- Modify: `ui/lib/i18n/locales/*.json`

**Interfaces:**
- Consumes `useGetRulesRegistrySettings` (now carrying both fields). Produces `usePassThresholdEnabled(): boolean` (default true, `staleTime: Infinity`) and `useDefaultPassThreshold(): number` (default 70) — both mirror `use-global-results-enabled.ts`.

- [ ] **Step 1: Reader hooks** — create both hooks mirroring `ui/hooks/use-global-results-enabled.ts` (`useGetRulesRegistrySettings({ query: { select: (d) => d.data, staleTime: Infinity }})`), returning `data?.pass_threshold_enabled ?? true` and `data?.default_pass_threshold ?? 70`.

- [ ] **Step 2: i18n (4 locales, under `config`)** — `rulesRegistryPassThresholdEnabledLabel` ("Pass-threshold quality gates"), `...EnabledHint` ("When on, checks warn when fewer than a set % of rows pass. Turn off to hide all threshold controls."), `rulesRegistryDefaultPassThresholdLabel` ("Default pass threshold"), `...Hint` ("Checks warn when fewer than this % of rows pass. Overridable per rule and per column."). Keep the old `config.defaultPassThreshold*` keys only if still referenced; otherwise remove.

- [ ] **Step 3: Fold controls into `RulesRegistrySettingsCard`** — add two mini-rows (mirror the existing Switch rows at settings.tsx:2118-2150): a `<Switch>` bound to `settings.pass_threshold_enabled` → `save({pass_threshold_enabled})`, and a number `<Input min0 max100 step1>` (+ `%`) bound to `settings.default_pass_threshold` → `save({default_pass_threshold})` (clamp [0,100], save on blur), disabled when the toggle is off. Use the card's existing `useSaveRulesRegistrySettings`.

- [ ] **Step 4: Remove the standalone card** — delete the `default-pass-threshold` entry (line ~3023) and the `DefaultPassThresholdSettings` component (~1293-1382) and its now-unused imports. (Leave the generated `useGetDefaultPassThreshold` hook + backend endpoint intact — just no longer rendered here.)

- [ ] **Step 5: Gates** — tsc clean, `bun test`, i18n parity.

- [ ] **Step 6: Commit** — `feat(threshold): relocate default + enable toggle into Rules Registry settings card`.

---

## Task 4: ThresholdPill — forbid blanks, drop "blank = default" hint

**Files:** `ui/components/apply-rules/ThresholdPill.tsx`; `ui/lib/i18n/locales/*.json`

- [ ] **Step 1** — In `commitDraft` (~63-73): remove the `if (trimmed === "") { onChange(null); return; }` branch; an empty/NaN input becomes a no-op (retain current value). Keep the clamp `onChange(Math.max(0, Math.min(100, n)))`.
- [ ] **Step 2** — Remove the "Use default" button (~137-147) entirely (it was the reset-to-null control). Draft seeding (~81) and onBlur reset (~125): change to `String(value ?? effectiveDefault)` so the field is never blank.
- [ ] **Step 3** — Since `value` is effectively always non-null in the apply flow now, the pill shows the concrete number; keep the `*` overridden marker logic only where still meaningful (per-rule pill uses it to show an explicit override vs default — retain). i18n: edit `monitoredTables.thresholdPopoverHint` and `columnThresholdPopoverHint` (all 4 locales) to drop the "Blank = use default (…)." sentence; remove `thresholdResetToDefault` + `columnThresholdResetToDefault` keys (all 4) if no longer referenced.
- [ ] **Step 4** — Gates: tsc, `bun test`, parity. **Commit** — `fix(threshold): forbid blank thresholds; drop blank-equals-default hint`.

---

## Task 5: By-rule pill "Mixed" state + gate on enabled

**Files:** `ui/components/apply-rules/RuleConfigCard.tsx`; `ui/routes/_sidebar/monitored-tables.$bindingId.tsx`; `ThresholdPill.tsx` (add optional `mixed` prop); `ui/lib/i18n/locales/*.json`

- [ ] **Step 1** — `ThresholdPill`: add optional `mixed?: boolean` prop; when true, render label as `⚠ {t("monitoredTables.thresholdPillMixed")}` (+ the `*` marker) instead of the number, and the popover still edits the rule-level `value`. i18n `thresholdPillMixed` = "Mixed" (4 locales).
- [ ] **Step 2** — In `RuleConfigCard` (~718): compute `const columnOverrides = Object.values(rule.column_pass_thresholds ?? {}); const mixed = columnOverrides.length > 0 && new Set(columnOverrides).size >= 1 && columnOverrides.some(v => v !== (rule.pass_threshold ?? resolvedDefaultThreshold));` — i.e. mixed when any column override differs from the effective rule value. Pass `mixed` to the pill. (Keep it simple: mixed = there exists ≥1 per-column override that differs from the rule-level effective value.)
- [ ] **Step 3** — Gate the by-rule pill render on `usePassThresholdEnabled()` (from ApplyRulesTab; thread down a `thresholdEnabled` prop to RuleConfigCard, hide the pill when false).
- [ ] **Step 4** — Gates + **Commit** — `feat(threshold): by-rule pill shows Mixed when per-column overrides differ; gate on enable flag`.

---

## Task 6: By-column — right-align pills, fix apply-in-column bug, gate on enabled

**Files:** `ui/components/apply-rules/RulesByColumn.tsx`; `ui/components/apply-rules/AddRulesDialog.tsx`; `ui/routes/_sidebar/monitored-tables.$bindingId.tsx`; `ui/components/apply-rules/shared.tsx`

- [ ] **Step 1 (right-align)** — In the rule-entry row (`RulesByColumn.tsx` ~214-260): move `flex-1` off the name span (line ~227) OR add `ml-auto` to the ThresholdPill wrapper (~245) so the pill group right-aligns/expands rightward. Ensure all rows align consistently (the pill column lines up). Verify with the screenshot layout intent: pill sits at the right edge before the slot name.
- [ ] **Step 2 (gate)** — Hide the per-column ThresholdPill when `!thresholdEnabled` (thread the flag into RulesByColumn).
- [ ] **Step 3 (the jump bug — mapping)** — In `AddRulesDialog.handleAdd` (~111-126): when `initialColumn` is set, write it into the new row's `column_mapping`. For a slotted rule, pick the slot to bind: prefer a slot whose family matches `initialColumn.family`, else the first slot; build `columnMapping = [{ [slotName]: initialColumn.name }]` instead of `[]`. For non-slotted rules keep `[{}]`. (Use `rule.definition.slots` already read at ~114.)
- [ ] **Step 4 (the jump bug — lens)** — `stageNewRows` (host ~2243-2249) hardcodes `setLens("by-rule")`. Make it conditional: thread the origin — if the add came from a column CTA (`addColumnContext` non-null), keep `lens = "by-column"` and set `setOpenColumnName(addColumnContext.name)` so the column card is expanded; else keep the `by-rule` switch. Ensure the newly-staged row (now column-mapped from Step 3) appears under that column in `useRulesByColumn`.
- [ ] **Step 5** — Test: add a unit test if `handleAdd`'s mapping logic is extractable (e.g. a `pickSlotForColumn(slots, family)` helper in shared.tsx with tests). At minimum, tsc + `bun test` green.
- [ ] **Step 6 (i18n)** — none new expected beyond Task 4/5; keep parity.
- [ ] **Step 7** — **Commit** — `fix(threshold): apply-in-column stays in by-column and maps the column; right-align per-column pills`.

---

## Task 7: Rule form — move field to Implementation Advanced, show real default in monospace, gate

**Files:** `ui/components/RegistryRuleFormDialog.tsx`; `ui/lib/i18n/locales/*.json`

- [ ] **Step 1 (move)** — Remove the `AdvancedDisclosure` holding the passThreshold field from `aboutTabContent` (~2912-2944), leaving About with no advanced box. Add a single shared `AdvancedDisclosure` (label `rulesRegistry.advancedSectionLabel`) at the BOTTOM of `implementationTabContent` (before its closing `</div>` ~3715), containing the passThreshold field. (Don't duplicate into the 3 per-mode disclosures — one shared block at the tab bottom, always present regardless of mode.)
- [ ] **Step 2 (real default, monospace)** — Import `useDefaultPassThreshold` (Task 3 hook) in the form; when `passThreshold === null`, the input's placeholder shows the actual default number and the field/placeholder uses `font-mono`. Simplest: `placeholder={String(defaultThreshold)}` + `className="... font-mono"` on the Input. Remove the `rulesRegistry.defaultPassThresholdPlaceholder` ("Workspace default") usage (or repoint it). Keep `value={passThreshold ?? ""}`.
- [ ] **Step 3 (gate)** — Hide the entire threshold AdvancedDisclosure (or just the field) when `!usePassThresholdEnabled()`.
- [ ] **Step 4** — Confirm the 6 round-trip touch points (snapshotFromRule 1598, PRISTINE 1644, hydration 1860, currentSnapshot 2091, buildUserMetadata 2361, applyParsedToForm 2419) are untouched by the JSX move (they reference component-level state, so moving markup is safe — just re-verify tsc + a manual round-trip reasoning in the report).
- [ ] **Step 5** — Gates + **Commit** — `feat(threshold): move rule default-threshold field to Implementation Advanced; show workspace default in monospace`.

---

## Task 8: Single Export modal (ExportDialog) replacing all 5 sites

**Files:** Create `ui/components/ExportDialog.tsx`; modify `registry-rules.index.tsx`, `registry-rules.$ruleId.tsx`, `monitored-tables.index.tsx`, `table-spaces.index.tsx`, `monitored-tables.$bindingId.tsx`, `components/data-products/ProductHeader.tsx`; `ui/lib/i18n/locales/*.json`. Remove `ExportYamlMenu.tsx` usages (keep or delete the file).

**Interfaces:** `ExportDialog` props `{ open, onOpenChange, fetchDqx: ExportFetcher, fetchOdcs?: ExportFetcher, title? }`. Renders a shadcn `Dialog` with DQX / ODCS choices (hide ODCS when `fetchOdcs` absent — rules are DQX-only); on choose → `run(fetcher)` (download + toast, reuse `downloadExportFile`/`extractApiError`), then close. A trigger is NOT built in — callers put an "Export…" menu item / button that sets `open`.

- [ ] **Step 1** — Build `ExportDialog.tsx` (shadcn `Dialog`/`DialogContent`/`DialogHeader`/`DialogTitle`; two big option buttons DQX YAML / ODCS). i18n (4 locales): `exportYaml.modalTitle` ("Export"), `exportYaml.modalDescription` ("Choose a format."), reuse existing `exportYaml.dqxOption`/`odcsOption`.
- [ ] **Step 2 (rules overview + detail)** — `registry-rules.index.tsx` selection bar: replace `ExportYamlMenu` (~767) with an "Export…" button that opens `ExportDialog` (DQX only, `fetchDqx={() => exportRegistryRules({ rule_id: [...selectedIds] })}`). `registry-rules.$ruleId.tsx`: remove the standalone `ExportYamlMenu` (~309) and add an "Export…" item INTO the existing ⋮ menu (~359-395) that opens the dialog (`exportRegistryRule(ruleId)`, DQX only).
- [ ] **Step 3 (tables/spaces bulk bars)** — `monitored-tables.index.tsx` (~430) and `table-spaces.index.tsx` (~405): replace the dual-format `ExportYamlMenu` with an "Export…" button opening `ExportDialog` with both `fetchDqx`/`fetchOdcs` (the existing `exportMonitoredTables`/`exportDataProducts` calls).
- [ ] **Step 4 (detail ⋮ menus)** — `monitored-tables.$bindingId.tsx` (~674-689) and `ProductHeader.tsx` (~589-610): replace the two DQX/ODCS `DropdownMenuItem`s with ONE "Export…" item that opens `ExportDialog` (using the existing `handleExport`-style fetchers → refactor `handleExport(fmt)` into `fetchDqx`/`fetchOdcs` thunks the dialog calls).
- [ ] **Step 5** — Gates: tsc, `bun test`, parity. **Commit** — `feat(export): single Export… modal (DQX/ODCS) across rules, tables, and spaces`.

---

## Task 9: Gate results breach indicators on the enable flag

**Files:** `ui/components/results/MultiTableResults.tsx` (+ wherever `BreachIcon`/breach markers render: `DimensionBreakdown.tsx`, `RunPicker.tsx`, `ScoreTrendChart.tsx`)

- [ ] **Step 1** — Read `usePassThresholdEnabled()` at the results container (`MultiTableResults.tsx`) and thread a `thresholdEnabled` flag (or simply short-circuit): when disabled, do not render `BreachIcon` in drill-down rows / run picker, and pass no breach markers to the trend chart. (Server already returns no breaches when disabled per Task 2, so this is belt-and-suspenders + avoids any stale-cache flash.)
- [ ] **Step 2** — Gates: tsc, `bun test`. **Commit** — `feat(threshold): hide results breach indicators when feature disabled`.

---

## Task 10: Redeploy to fe-sandbox-dq-demo-2

Not a code task — the controller runs this after the whole-branch review passes. See `docs/superpowers/REDEPLOY-RUNBOOK-2026-07-19.md` and `reference-dqx-app-deploy-workspace` memory. `make app-deploy PROFILE=fe-sandbox-dq-demo-2 TARGET=dev` (bundle deploy may need `--auto-approve`; app restarts on deploy). Verify app RUNNING + HTTP 302 + logs show startup complete.

---

## Notes for the executor
- **Fail-open:** every UI gate defaults to ENABLED when the settings hook hasn't resolved, so the feature never disappears due to a loading state.
- **Precedence parity:** the materializer (T2), the pill placeholders (T4/T5/T6), and `resolve_pass_threshold` must all agree — column(max) → rule → registry → admin, nullish not truthy.
- **Don't break round-trips:** T7 moves JSX only; the 6 rule-form state touch points stay.
- **Minor findings** from per-task reviews: record in the ledger; the final whole-branch review triages.
