# App Feedback v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Large DQX Studio UX refactor — consolidate rule-type authoring behind the function dropdown, move filtering into the rule definition, rework the three overview pages' actions + add quick-action bars, delete the Review&Approve screen (moving "view changes" into the overview Actions columns), rename the three pages, and add an app resource tag.

**Architecture:** Mostly frontend (React/TS) over the single consolidated rule editor `RegistryRuleFormDialog.tsx` and the three overview pages/tables; plus backend changes for friendly-name `label` on check functions, a `filter` field on `RuleDefinition` (materialized into DQX's native `DQRule.filter`), and column-arity substitution in the materializer. Bundle change for the app tag.

**Design spec:** `docs/superpowers/specs/2026-07-16-app-feedback-v3-design.md` — READ IT FIRST. Line numbers here + in the spec are exploration-pass anchors; **re-locate before editing** (earlier tasks shift lines).

## Global Constraints
- **App-layer only** — no changes under `src/databricks/labs/dqx/` (core lib). DQX core already supports `DQRule.filter` natively; we only consume it.
- **Commits only — do NOT deploy.**
- **i18n all 4 locales** (`ui/lib/i18n/locales/{en,es,it,pt-BR}.json`, en = source of truth) for every new/changed user-facing string; translate values, don't leave English in non-English files.
- **No lint/type suppression** (`@ts-ignore`/`# type: ignore`/`eslint-disable`/`# noqa`). Fix the code.
- **GPG-signed commits**, each message ending with a trailer line exactly: `Co-authored-by: Isaac`.
- **Never stage** `app/uv.lock` or `app/src/databricks_labs_dqx_app/__dist__/`. After `make app-regen-api`/`uv sync`, run `git checkout app/uv.lock` before committing.
- **Type-check gate (env-specific):** `cd app && node_modules/.bin/tsc -b --incremental` (exit 0). NOT `make app-check` (broken alias here), NOT `bun x tsc` (wrong TS version). App JS deps are installed.
- **Backend tests:** `cd app && uv run --group test pytest tests/<file> -v`. Known PRE-EXISTING failures at base (do NOT attribute to your work): 3× `test_i18n_locale_parity` (upstream untranslated bulkApprove/Deprecate/Revoke keys), 1× `test_monitored_table_service::TestGet` IndexError, 1× `test_lint_policy` BLE001 (core src/), 5× `test_tag_auto_suppressions_schema`. Your new keys must be in all 4 locales so you don't ADD to the i18n-parity failures.
- **eslint not installed** in this env — tsc + the no-suppression rule are the gates.
- **After backend model/route changes:** `make app-regen-api` (regenerates `ui/lib/api.ts`), then `git checkout app/uv.lock`, and commit the regenerated `api.ts`.
- Most changes are visual/interaction with no unit-test seam — the gate is tsc + a documented manual-reasoning check. Write TDD tests only where a real seam exists (backend services, pure helpers). Do not fabricate brittle DOM tests.

---

## Task 1: App resource tag (C5)

**Files:** Modify `app/databricks.yml` (`resources.apps.dqx-studio`, ~line 141).

- [ ] **Step 1: Add the tag.** Under `resources.apps.dqx-studio` (after the `permissions:` block, ~line 183), add a `tags:` map matching the job's form (`app/databricks.yml:361`):

```yaml
      tags:
        app: "dqx-studio"
```

- [ ] **Step 2: Validate the bundle accepts it.** Run: `cd app && databricks bundle validate -p fe-sandbox-dq-demo -t dev`. Expected: `Validation OK!`. If the app resource REJECTS `tags` (schema doesn't support it), STOP and report DONE_WITH_CONCERNS with the exact validation error (app-resource tag support has historically lagged jobs/warehouses — the controller will decide the fallback).

- [ ] **Step 3: Confirm no unrelated files dirtied; commit.**

```bash
git checkout app/uv.lock 2>/dev/null || true
git add app/databricks.yml
git commit -m "$(printf 'App Feedback v3: add app resource tag app=dqx-studio\n\nCo-authored-by: Isaac')"
```

---

## Task 2: Page renames (C1)

**Files:** Modify `ui/lib/i18n/locales/{en,es,it,pt-BR}.json` (values only); verify `ui/routes/_sidebar/route.tsx` uses the keys (no change needed there).

- [ ] **Step 1: Rename the three primary labels in all 4 locales.** Change VALUES (keep keys):
  - `sidebar.rulesRegistry`: "Rules Registry" → "Rules" (en; translate: es "Reglas", it "Regole", pt-BR "Regras").
  - `sidebar.monitoredTables`: "Monitored Tables" → "Tables" (es "Tablas", it "Tabelle", pt-BR "Tabelas").
  - `sidebar.dataProducts`: "Table Spaces" → "Spaces" (es "Espacios", it "Spazi", pt-BR "Espaços").
  - `rulesRegistry.title`: "Rules Registry" → "Rules".
  - `monitoredTables.title` AND `monitoredTables.breadcrumb`: "Monitored Tables" → "Tables".
  - `dataProducts.title`: "Table Spaces" → "Spaces".

- [ ] **Step 2: Sweep embedded prose.** In the `dataProducts.*` and `monitoredTables.*` blocks, update user-facing strings that embed the old names for consistency: e.g. `dataProducts.newProduct` "New Table Space"→"New Space", `createTitle` "Create table space"→"Create space", `searchPlaceholder` "Search table spaces…"→"Search spaces…", empty-state strings; `monitoredTables` subtitle/empty-state "monitored tables"→"tables". Keep it natural per locale. Do NOT touch route paths or internal ids (`dataProducts`, `/table-spaces`).

- [ ] **Step 3: Type-check + confirm no key added/removed (values only).** Run: `cd app && node_modules/.bin/tsc -b --incremental` (exit 0). Grep to confirm the 4 locale files still have identical key sets.

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: rename Rules/Tables/Spaces page labels\n\nCo-authored-by: Isaac')"
```

---

## Task 3: Backend friendly names for check functions (A1)

**Files:** Modify `backend/models.py` (`CheckFunctionDef` ~2269), `backend/routes/v1/check_functions.py` (add `_FRIENDLY_LABELS` + set `label` in `_introspect_check_functions` ~363-408). Test: `app/tests/` (check-functions test — locate or create). Regenerate `ui/lib/api.ts`.

**Interfaces:** Produces `CheckFunctionDef.label: str` on the API; consumed by the FE function dropdown (Task 8) and rule-logic display (Task 9).

- [ ] **Step 1: Write the failing test.** In the check-functions test, assert the endpoint/introspection returns `label` for a sample: `is_aggr_equal` → "Is Aggregate Equal", a plain one like `is_not_null` → "Is Not Null", and an acronym one (e.g. `is_valid_ipv4_address` or whichever exists) upper-cases IP. Run → FAIL.

- [ ] **Step 2: Add `label` to the model.** In `models.py`, add `label: str` to `CheckFunctionDef` (with a docstring). 

- [ ] **Step 3: Compute labels.** In `check_functions.py`, add a module-level `_FRIENDLY_LABELS: dict[str, str]` override map (curated: the `is_aggr_*` family → "Is Aggregate …", and any acronym cases) and a helper `_friendly_label(name: str) -> str` that returns the override if present else title-cases `name.replace("_", " ")` with an acronym-fixup pass (SQL/IP/JSON/PII upper-cased). Set `label=_friendly_label(fn_name)` when building each `CheckFunctionDef` in `_introspect_check_functions`.

- [ ] **Step 4: Run test → PASS.** `cd app && uv run --group test pytest tests/<check_functions_test> -v`.

- [ ] **Step 5: Regen client + checkout uv.lock.** `make app-regen-api` then `git checkout app/uv.lock`. Confirm `label` appears on the generated check-function type in `ui/lib/api.ts`. Run `cd app && node_modules/.bin/tsc -b --incremental` (exit 0).

- [ ] **Step 6: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/backend/models.py app/src/databricks_labs_dqx_app/backend/routes/v1/check_functions.py app/tests app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -m "$(printf 'App Feedback v3: friendly labels for check functions\n\nCo-authored-by: Isaac')"
```

---

## Task 4: Single-select cascading table picker (A2)

**Files:** Add a single-select mode to `ui/components/monitored-tables/MultiSelectPopover.tsx` (or a new `SingleTableScopePicker`), reuse the cascading state from `TableScopePicker.tsx`. Modify `ui/components/rules/test/TableTestSource.tsx:66` and `ui/components/rules/lowcode/JoinBlock.tsx:74` to use it.

**Interfaces:** Produces a single-select cascading picker with `value: string /*fqn*/`, `onChange(fqn: string)`. Consumed by Table Test + join blocks. Join `target_table` semantics unchanged (single fqn string).

- [ ] **Step 1: Read the target components.** `MultiSelectPopover.tsx` (multi-select popover), `TableScopePicker.tsx` (cascading catalog→schema→table hook + fields), `SearchableSelect.tsx` (existing single-select popover), and the two current call sites (`TableTestSource.tsx`, `JoinBlock.tsx`) + `CatalogBrowser.tsx` to preserve behavior. Ask questions if the fqn shape differs between sites.

- [ ] **Step 2: Build the single-select picker.** Add `single?: boolean` to `MultiSelectPopover` (when set: selecting a row replaces the selection and closes; no checkboxes/select-all; value is the single selected string), OR create `SingleTableScopePicker` composing the cascading catalog→schema→table logic from `TableScopePicker` with single-select popovers. It must render three compact dropdowns (Catalog / Schema / Table) matching the new-monitored-table look, not `CatalogBrowser`'s wide `grid-cols-3` plain selects. Emit a single `table_fqn` string via `onChange`. Reuse existing `monitoredTables.wizard.*` i18n keys where possible; add new keys (4 locales) only if needed.

- [ ] **Step 3: Wire into Table Test.** In `TableTestSource.tsx`, replace `<CatalogBrowser value={fqn} onChange={setFqn} />` (~line 66) with the new single-select picker (same value/onChange contract). Keep everything else in the Table Test flow identical.

- [ ] **Step 4: Wire into low-code joins.** In `JoinBlock.tsx`, replace `<CatalogBrowser value={join.target_table} onChange={...} />` (~line 74) with the new picker, preserving the `target_table` update semantics.

- [ ] **Step 5: Type-check.** `cd app && node_modules/.bin/tsc -b --incremental` (exit 0).

- [ ] **Step 6: Manual reasoning check.** Confirm via code reading: both sites now render the compact monitored-table-style dropdowns, single-select only, correct fqn out. Note in commit body.

- [ ] **Step 7: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/monitored-tables app/src/databricks_labs_dqx_app/ui/components/rules/test/TableTestSource.tsx app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/JoinBlock.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: single-select table picker in Table Test + joins\n\nCo-authored-by: Isaac')"
```

---

## Task 5: Backend — rule-level filter field on RuleDefinition (B4 backend)

**Files:** Modify `backend/registry_models.py` (`RuleDefinition` ~95-119, add `filter`), `backend/services/registry_service.py` (validate on create/update), `backend/rule_fingerprint.py`/`registry_fingerprint.py` (include filter), `backend/services/materializer.py` (`render_check` reads `definition.filter`, substitutes slots, sets `check_dict["filter"]`; stop reading `applied.row_filter`). Test: registry-service + materializer tests.

**Interfaces:** Produces `RuleDefinition.filter: str | None`, materialized into `check_dict["filter"]` with `{{slot}}` substitution. Consumed by the editor (Task 10) and the run path.

- [ ] **Step 1: Write failing tests.** (a) registry_service: creating/updating a rule with an unsafe `filter` (e.g. contains `;` / DDL) raises the validation error; a safe filter persists on the definition and freezes into the version snapshot. (b) materializer: `render_check` for a rule whose `definition.filter` is `{{col_a}} > 0` with a mapping `{col_a: amount}` produces `check_dict["filter"] == "amount > 0"` (slot-substituted); and a rule with no filter produces no `filter` key. Run → FAIL.

- [ ] **Step 2: Add the model field.** In `RuleDefinition`, add `filter: str | None = None` (mirror `error_message` docstring/placement). It auto-carries into `RuleVersion.definition`.

- [ ] **Step 3: Validate on create/update.** In `RegistryService.create_rule`/`update_draft`, validate `definition.filter` (reuse the `is_sql_query_safe(f"SELECT * FROM _t WHERE ({filter})")` + max-len logic from `apply_rules_service.py:78-98` — extract a shared helper or inline). Raise the existing SQL-safety error type on failure.

- [ ] **Step 4: Fingerprint it.** Ensure `compute_definition_fingerprint` canonicalization includes `filter` so filter-only edits register as changes / modified-since-publish.

- [ ] **Step 5: Materialize it.** In `materializer.render_check`: after building the check dict, if `definition.filter` is set, run it through `_substitute_text(filter, group, definition.slots)` and set `check_dict["filter"]`. REMOVE the `row_filter` param usage (`:361-367`, `:583`) — stop reading `applied.row_filter` (per user decision "rule filter only"). Leave the `dq_applied_rules.row_filter` column in place (no migration).

- [ ] **Step 6: Run tests → PASS.** `cd app && uv run --group test pytest tests/test_registry_service.py tests/test_materializer.py tests/test_registry_fingerprint.py -v` (adjust to actual test filenames).

- [ ] **Step 7: Regen client (definition shape changed) + checkout uv.lock.** `make app-regen-api`; `git checkout app/uv.lock`; `tsc -b` exit 0.

- [ ] **Step 8: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/backend app/tests app/src/databricks_labs_dqx_app/ui/lib/api.ts
git commit -m "$(printf 'App Feedback v3: rule-level filter on RuleDefinition + materialize into DQRule.filter\n\nCo-authored-by: Isaac')"
```

---

## Task 6: Backend — native column arity semantics in materializer (B3 backend)

**Files:** Modify `backend/services/materializer.py` (`_substitute_arguments` / native column binding). Test: materializer tests.

**Interfaces:** Consumes `RuleDefinition.filter` (Task 5) to identify filter-only columns. Produces correct function-column binding: multi-col funcs get all declared columns except filter-only ones; single-col funcs get only the first.

- [ ] **Step 1: Write failing tests.** (a) A multi-column native func (e.g. `is_unique`) with 3 declared slots, none in the filter → function args include all 3. (b) Same, but one slot referenced in `definition.filter` → function args include only the other 2 (the filter-referenced one is filter-only). (c) A single-column native func with 2 declared slots → function arg = first slot only; second slot used only if in filter. Run → FAIL.

- [ ] **Step 2: Implement arity-aware binding.** In the materializer native path, determine the function's column-parameter arity (single vs list) from the check-function signature/registry. For list-arity: bind all declared slot columns EXCEPT those whose `{{slot}}` appears in `definition.filter`. For single-arity: bind only the first declared slot's column. Filter-only columns are used only in the filter substitution (Task 5), never in function args.

- [ ] **Step 3: Run tests → PASS.**

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/backend/services/materializer.py app/tests
git commit -m "$(printf 'App Feedback v3: native column arity binding (multi vs single, filter-only cols)\n\nCo-authored-by: Isaac')"
```

---

## Task 7: Remove per-binding filter UI (B4 frontend cleanup)

**Files:** Modify `ui/components/apply-rules/RuleConfigCard.tsx` (remove `row_filter` input ~800-818, prop ~509-511/555), `ui/routes/_sidebar/monitored-tables.$bindingId.tsx` (`handleRowFilterChange` ~2224 + wiring ~2420), `ui/components/apply-rules/shared.tsx` (`row_filter` at ~203/226). Remove `monitoredTables.ruleFilter*` i18n keys (4 locales).

- [ ] **Step 1: Remove the UI + state + payload.** Delete the `row_filter` input and its prop from `RuleConfigCard`; delete `handleRowFilterChange` and its wiring in `monitored-tables.$bindingId.tsx`; remove `row_filter` from `buildDesiredApplications` and `desiredApplicationsKey` in `shared.tsx`. Keep the pass-threshold override (only remove filter). If the backend `DesiredAppliedRuleIn`/`ApplyRuleIn` still accept `row_filter`, that's fine (harmless); just stop sending it.

- [ ] **Step 2: Remove i18n keys.** Delete `monitoredTables.ruleFilterLabel/ruleFilterPlaceholder/ruleFilterHint` from all 4 locales (keep the parity — remove from all four).

- [ ] **Step 3: Type-check.** `cd app && node_modules/.bin/tsc -b --incremental` (exit 0). Watch for unused-import/var errors from the removal.

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/apply-rules 'app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.$bindingId.tsx' app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: remove per-binding row filter UI (filter now on the rule)\n\nCo-authored-by: Isaac')"
```

---

## Task 8: Rule editor — low-code "decision point" drives everything (B1, REVISED)

**Read spec §B1 (revised) in full before starting — this task changed from the original "Core Checks section" design to a low-code-first "decision point" design.**

**Files:** Modify `ui/components/RegistryRuleFormDialog.tsx` (remove `ModeSegmentedSwitch` ~848-890 + render ~2267; gate the low-code chrome on the empty state; wire the decision-point selection → mode/body via `performModeSwitch`/`requestModeChange` ~2171-2246). Modify `ui/components/rules/lowcode/LowcodeRow.tsx` + `OperatorDropdown.tsx`/`ValueCell.tsx` (the decision-point cell + its dropdown). i18n: 4 locales.

**Interfaces:** Consumes `CheckFunctionDef.label` (Task 3). The editor always opens in low-code chrome; the decision-point cell is the single entry point.

- [ ] **Step 1: Read the editor + low-code row.** `performModeSwitch` (~2171-2221), `requestModeChange` (~2223-2246), `MODE_SEGMENTS` (~840), the three body render blocks (lowcode ~2286, native ~2338, sql ~2507), and how `sql_expression`/`sql_query` route. Read `components/rules/lowcode/LowcodeRow.tsx`, `OperatorDropdown.tsx`, `ValueCell.tsx`, `LowcodeBuilder.tsx` (the row stack + Add-condition/aggregated/Advanced buttons + the polarity "then the row" control). Ask questions if the decision-point wiring is unclear.

- [ ] **Step 2: Remove the segmented switch + gate the empty state.** Delete `ModeSegmentedSwitch` (~848-890), its render (~2267), and the `ruleTypeHeader` section. On a fresh/empty rule, render the low-code chrome but: hide the "Add condition" / "Add aggregated condition" / "Advanced" buttons; disable the polarity ("then the row passes/fails") control with tooltip `t("rulesRegistry.polarityGatedTooltip")` = "You must apply your condition logic first". Render the decision-point cell (the operator/"is null" cell in the first low-code row) **empty with an inverted/high-contrast fill** to invite a click (placeholder `t("rulesRegistry.decisionPointPlaceholder")`).

- [ ] **Step 3: Build the decision-point dropdown.** Clicking the decision-point cell opens a dropdown reusing the DQX-native check-picker presentation (like `FunctionCombobox`), ordered:
  1. **Condition Builder** — desc "Build custom, complex conditions in low-code SQL", right-arrow (→) affordance, **subtle highlight fill** to stand out. Select → `requestModeChange("lowcode")`: reveal Add-condition/aggregated/Advanced buttons, enable polarity, and this row becomes a normal low-code condition row.
  2. **Single Table SQL** (`sql_expression`) — right-arrow. Select → `mode:"sql"` `{predicate}` → SQL editor.
  3. **Cross-Table SQL** (`sql_query`) — right-arrow. Select → `mode:"sql"` `{sql_query}` → SQL editor.
  4. The remaining DQX-native functions (grouped as today, friendly `label`s), each → `mode:"dqx_native"`. **FILTER** these by the data type(s)/families of the currently-declared columns AND arity (column count): only show native functions whose column-parameter families + arity are compatible with the declared columns. Use the slot families on declared columns + the function signature's column-param families/arity (from `deriveSlotsAndParameters`). If no columns are declared yet, show all (unfiltered).
  Reuse `ModeSwitchDialog` confirmation only when a later change would lose content.

- [ ] **Step 4: Fill the corresponding body.** Once an option is chosen, render the matching existing body block (low-code builder / SQL editor / native args). Changing the decision-point selection later re-drives mode/body through the existing machinery. The decision-point cell reflects the current selection (Condition Builder / Single Table SQL / Cross-Table SQL / the native function's friendly label).

- [ ] **Step 5: i18n.** Add to all 4 locales: `rulesRegistry.decisionPointPlaceholder`, `rulesRegistry.polarityGatedTooltip`, `rulesRegistry.coreConditionBuilder`, `rulesRegistry.coreConditionBuilderDesc`, `rulesRegistry.coreSingleTableSql`, `rulesRegistry.coreCrossTableSql`. Retire `modeDqxNative`/`modeLowcode`/`modeSql`/`ruleTypeHeader` (remove from all 4, keep parity).

- [ ] **Step 6: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm: no segmented switch; empty editor shows gated low-code chrome + inverted empty decision cell; clicking it opens the ordered dropdown (Condition Builder highlighted, then the two SQL options, then type/arity-filtered native checks); selecting each fills the right body + ungates the chrome. Note in commit body.

- [ ] **Step 7: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/components/rules/lowcode app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: low-code decision-point drives rule type (no mode picker)\n\nCo-authored-by: Isaac')"
```

---

## Task 9: Friendly names in rule-logic display (B1 display)

**Files:** Modify `ui/components/apply-rules/RuleConfigCard.tsx` (rule-logic body render), `ui/components/RegistryRuleBadges.tsx`, and any view-mode function-name render. Consumes the check-functions list (for `label`).

- [ ] **Step 1: Map function name → label at display.** Where saved rule logic shows the raw function name (RuleConfigCard's logic display from earlier bug-bash showed `fn()`; badges), look up the function's `label` from the `useListCheckFunctions()` data and render the friendly label. For the Core Checks pseudo-types (sql_expression/sql_query/lowcode) show "Single Table SQL"/"Cross Table SQL"/"Custom Condition". Fallback to the raw name if no label found.

- [ ] **Step 2: Type-check + manual reasoning.** `tsc -b` exit 0. Note in commit body.

- [ ] **Step 3: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/apply-rules/RuleConfigCard.tsx app/src/databricks_labs_dqx_app/ui/components/RegistryRuleBadges.tsx
git commit -m "$(printf 'App Feedback v3: show friendly check-function names in rule logic\n\nCo-authored-by: Isaac')"
```

---

## Task 10: Rule editor — filter field in Advanced sections (B4 frontend)

**Files:** Modify `ui/components/RegistryRuleFormDialog.tsx` (add `filter` state mirroring `errorMessage`; add `AdvancedDisclosure` to native + sql branches; add filter input to all three Advanced sections; include `filter` in `buildDefinition` branches). i18n: 4 locales. Consumes `RuleDefinition.filter` (Task 5).

- [ ] **Step 1: Add filter state + round-trip.** Add `const [filter, setFilter] = useState("")` mirroring `errorMessage` (`:1164`): seed from `def.filter` on rehydrate (`:1234`), include in the form snapshot (`:1426`), and emit `filter: filter.trim() || undefined` in all three `buildDefinition` branches (`:1541/:1559/:1570`).

- [ ] **Step 2: Add Advanced sections to native + sql.** Low-code already has `AdvancedDisclosure` (`:2305`). Add an `AdvancedDisclosure` to the dqx_native branch (`:2338+`) and the sql branch (`:2507+`). Put a filter text input (with `{{slot}}` column-reference affordance, reusing the declared-columns list exposed to lowcode/groupby) inside all three Advanced sections. Label via new i18n keys.

- [ ] **Step 3: i18n.** Add `rulesRegistry.filterLabel`, `.filterPlaceholder`, `.filterHint` to all 4 locales.

- [ ] **Step 4: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm the filter field appears in Advanced for all three editor bodies and round-trips into the definition. Note in commit body.

- [ ] **Step 5: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: rule filter field in Advanced (native/sql/lowcode)\n\nCo-authored-by: Isaac')"
```

---

## Task 11: Rule editor — column persistence across type swaps + native arity UX (B2 + B3 frontend)

**Files:** Modify `ui/components/RegistryRuleFormDialog.tsx` (mode-switch carry-over of `nativeSlots`↔`sqlSlots` + `slotTags`; single-column "add column" popover). i18n: 4 locales.

- [ ] **Step 1: Carry columns across swaps.** In `performModeSwitch`/`requestModeChange`, ensure declared columns + their `slotTags` persist: when switching into dqx_native, MERGE carried slots (names/families/tags) with the function's signature arity rather than blindly re-seeding from signature (`:2382`); when leaving native, copy `nativeSlots`→`sqlSlots`. `slotTags` is already shared — verify it survives. Net: the "Columns used" panel + tag chips are unchanged by a type swap.

- [ ] **Step 2: Single-column "add column" popover.** In the SlotsPanel (or wherever "add column" is rendered), when the current native function is single-column arity, clicking "add column" opens an explanatory popover/dialog: "Only the top column applies to this rule; the rest are only used in advanced filter conditions." (New i18n key.) Still allow adding the extra column (it's valid as a filter-only column) — the popover just explains. Multi-column funcs: no popover.

- [ ] **Step 3: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm: swapping types preserves columns+tags; single-col add-column shows the explanation. Note in commit body.

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: persist columns/tags across type swaps + single-column add-column explainer\n\nCo-authored-by: Isaac')"
```

---

## Task 12: Rule editor — unused-column save blocking (B5)

**Files:** Modify `ui/components/RegistryRuleFormDialog.tsx` (add `unusedSlots` derivation; feed save-enable logic ~1518-1544; error message). i18n: 4 locales.

- [ ] **Step 1: Derive unused slots.** Add a memo `unusedSlots` = declared slots whose `{{slotName}}` is not referenced anywhere in the effective body: native function args (per arity — single-col: first slot is used by being the arg; multi-col: all in args unless filter-only), SQL `predicate`/`sql_query` text, low-code AST (rows/joins/group-by), OR the filter (Task 10). Reference-detection: string search for `{{slotName}}` across the relevant serialized body parts + filter, plus the native-arg binding.

- [ ] **Step 2: Block save.** If `unusedSlots.length > 0`, push an error label into BOTH `missingDraftFieldLabels` and `missingSubmitFieldLabels` (so all save/submit buttons disable), with a message naming the unused column(s), e.g. `t("rulesRegistry.unusedColumnsError", { columns })`. Ensure the existing "why is save disabled" surface shows it.

- [ ] **Step 3: i18n.** Add `rulesRegistry.unusedColumnsError` (with `{{columns}}` interpolation) to all 4 locales.

- [ ] **Step 4: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm: declaring a column and not using it disables save + shows the named-column error; using it re-enables. Note in commit body.

- [ ] **Step 5: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: block save when declared columns are unused\n\nCo-authored-by: Isaac')"
```

---

## Task 13: Delete Review & Approve screen + references (C2 part 1)

**Files:** Delete `ui/routes/_sidebar/rules.drafts.tsx`. Modify `ui/routes/_sidebar/route.tsx` (remove nav block ~134-147 + `<hr>`), `ui/routes/_sidebar/profiler.tsx` (remove "view in drafts" link ~2128). KEEP `ui/components/drafts/ChangeDiffDialog.tsx` (reused in Task 14) and `useApprovalsMode`/`ApprovalsModeCard`.

- [ ] **Step 1: Delete the route + nav + profiler link.** Remove `rules.drafts.tsx`. Remove the `sidebar.reviewAndApprove` nav item + surrounding `<hr>` in `route.tsx`. Remove the profiler `<Link to="/rules/drafts">` at `profiler.tsx:2128`. Do NOT delete `ChangeDiffDialog.tsx`, `ApprovalQueueCard.tsx` may become unused (remove if fully orphaned + no other importer — verify by grep). Do NOT remove the approvals-mode setting.

- [ ] **Step 2: Handle the route tree + any dangling imports.** Let `routeTree.gen.ts` regenerate (restart dev / `make app-build` regenerates; for the gate just ensure `tsc -b` passes — if routeTree references the deleted route, regen it via the tanstack plugin or hand-fix per ui/CLAUDE.md guidance). Remove now-unused i18n keys `sidebar.reviewAndApprove`, `rulesDrafts.*` (all 4 locales) IF nothing else references them (the diff dialog uses `rulesDrafts.diff.*` — keep those keys, they're consumed by ChangeDiffDialog wrappers reused in Task 14; verify before deleting any `rulesDrafts.*`).

- [ ] **Step 3: Confirm approvals-mode ambiguity.** The user said "get rid of Review & Approve screen and all references to it in admin settings." The `ApprovalsModeCard` in config governs the approve/reject flow the overview pages now host, so KEEP it. If, on reading, "references in admin settings" clearly means the approvals-mode card itself, STOP and report DONE_WITH_CONCERNS asking the controller — do not remove the approvals-mode setting without confirmation.

- [ ] **Step 4: Type-check + manual reasoning.** `tsc -b` exit 0 (regenerate routeTree if needed). Confirm the screen + nav + profiler link are gone, ChangeDiffDialog + approvals-mode remain. Note in commit body.

- [ ] **Step 5: Commit.**

```bash
git add -A app/src/databricks_labs_dqx_app/ui
git commit -m "$(printf 'App Feedback v3: remove Review & Approve screen + nav/profiler references\n\nCo-authored-by: Isaac')"
```

---

## Task 14: "View changes" in the three overview Actions columns (C2 part 2)

**Files:** Modify `ui/routes/_sidebar/registry-rules.index.tsx`, `ui/routes/_sidebar/monitored-tables.index.tsx`, `ui/routes/_sidebar/table-spaces.index.tsx` (add a purple view-changes action + `diffTarget` state, reusing `RegistryRuleDiffDialog`/`MonitoredTableDiffDialog`/`TableSpaceDiffDialog`). Possibly bump `ACTIONS_COL_WIDTH` in `data-table/sticky-actions.ts`.

- [ ] **Step 1: Add view-changes to Rules actions.** In `registry-rules.index.tsx` `renderActionsCell`, add a `GitCompare` icon button, PURPLE, shown when `display_status === "modified" || status === "pending_approval"`, positioned right of approve/reject and left of delete. Wire a `diffTarget` state + render `RegistryRuleDiffDialog` (from `components/drafts/ChangeDiffDialog.tsx`). Purple must be contrast-safe in dark mode — use a Tailwind purple that meets contrast on the dark table row (e.g. `text-purple-400` in dark / `text-purple-600` in light via a theme-aware class); verify both themes.

- [ ] **Step 2: Add view-changes to Monitored Tables actions.** In `monitored-tables.index.tsx` `renderActions`, add the same purple view-changes button shown when `status === "pending_approval"` (only signal available for MT), wired to `MonitoredTableDiffDialog` + a `diffTarget` state.

- [ ] **Step 3: Add view-changes to Table Spaces actions.** In `table-spaces.index.tsx` `renderActions`, add it shown when `display_status === "modified" || "pending_approval"`, wired to `TableSpaceDiffDialog`.

- [ ] **Step 4: Width budget.** If any row now shows >4 action icons, bump `ACTIONS_COL_WIDTH` (`data-table/sticky-actions.ts:35`) and its comment accordingly.

- [ ] **Step 5: Type-check + manual reasoning (both themes).** `tsc -b` exit 0. Confirm the purple icon is visible/contrasty in dark AND light, positioned correctly, shown only when changes exist, and opens the right diff dialog per page. Note in commit body.

- [ ] **Step 6: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/registry-rules.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/table-spaces.index.tsx app/src/databricks_labs_dqx_app/ui/components/data-table/sticky-actions.ts
git commit -m "$(printf 'App Feedback v3: view-changes action (purple) in overview Actions columns\n\nCo-authored-by: Isaac')"
```

---

## Task 15: Actions column cleanup — remove download, conditional delete (C3)

**Files:** Modify the three overview pages (`registry-rules.index.tsx`, `monitored-tables.index.tsx`, `table-spaces.index.tsx`).

- [ ] **Step 1: Remove per-row download.** Delete the per-row `<ExportYamlMenu iconOnly>` from Rules (`~535-539`), Monitored Tables (`~422-427`), and Table Spaces (`~342-347`). KEEP the page-header export menus. Remove now-unused imports.

- [ ] **Step 2: Hide delete when there's something to approve.** On Monitored Tables (`~476-491`) and Table Spaces (`~396-411`), add `&& !hasPendingChanges(row)` to the existing `canCreateRules` guard on the delete button, where `hasPendingChanges` = the same signal used for view-changes (MT: `status==="pending_approval"`; Spaces: `display_status modified||pending_approval`).

- [ ] **Step 3: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm no per-row download on the three pages; delete hidden when the row has changes to approve on MT + Spaces. Note in commit body.

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/routes/_sidebar/registry-rules.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/table-spaces.index.tsx
git commit -m "$(printf 'App Feedback v3: remove per-row download; hide delete when changes pending\n\nCo-authored-by: Isaac')"
```

---

## Task 16: Quick-actions (bulk) bars on Monitored Tables + Table Spaces + consistency pass (C4)

**Files:** Modify `ui/components/monitored-tables/MonitoredTablesTable.tsx` + `ui/components/data-products/DataProductsTable.tsx` (add selection support mirroring `RulesTable`), and `monitored-tables.index.tsx` + `table-spaces.index.tsx` (add bulkToolbar). Review `registry-rules.index.tsx` bulk bar for consistency. i18n: 4 locales.

**Interfaces:** Consumes `RulesTableSelection` shape from `RulesTable.tsx:447-453` as the model.

- [ ] **Step 1: Add selection to the two tables.** Add a `selection?` prop + leading checkbox column to `MonitoredTablesTable` and `DataProductsTable`, mirroring `RulesTable` (`:447-453`, header checkbox `:563-572`, per-row `:626-641`).

- [ ] **Step 2: Add bulk toolbars.** In `monitored-tables.index.tsx` and `table-spaces.index.tsx`, add a `bulkToolbar` modeled on `registry-rules.index.tsx:675-729` with actions each shown only when ≥1 selected row qualifies: **Run** (runnable), **Approve** + **Reject** (pending), **Delete**, + **Clear selection**. Reuse the existing confirm-dialog patterns. Wire selection state (`selectedIds`, `toggleSelect`, `toggleSelectAll`, `tableSelection`) as `registry-rules.index.tsx` does.

- [ ] **Step 3: Consistency pass across all three bulk bars.** Align button order, icons (lucide), labels, disabled/visible logic, and confirm-dialog copy so Rules/Tables/Spaces bulk bars read as one system. Rules keeps its domain actions (Approve/Deprecate/Revoke) but harmonize styling/order with the new Run/Approve/Reject/Delete bars.

- [ ] **Step 3b: Checkbox reveal-on-hover (user refinement, all 3 overview tables).** In `RulesTable`, `MonitoredTablesTable`, and `DataProductsTable`: the header "select all" checkbox stays ALWAYS visible. Per-row checkboxes: when NO rows are selected, each row's checkbox is hidden and only appears on ROW HOVER (e.g. `opacity-0 group-hover:opacity-100`, with the `<tr>`/row carrying `group`); once ≥1 row is selected, ALL row checkboxes become persistently visible (so further ticking is easy). Implement as a shared behavior — the tables already share the selection pattern from `RulesTable`; drive the "any selected" state from the selection prop (`selectedIds.size > 0`). Keep the checkbox focus-visible/keyboard-accessible even when visually hidden (don't `display:none` — use opacity so it stays reachable, or reveal on focus-within too).

- [ ] **Step 4: i18n.** Add bulk-action keys for MT + Spaces (bulkRun/bulkApprove/bulkReject/bulkDelete/clearSelection/selectedCount) to all 4 locales — reuse existing keys where identical.

- [ ] **Step 5: Type-check + manual reasoning.** `tsc -b` exit 0. Confirm both pages have working selection + bulk bars, actions gated correctly, and the three bars are visually consistent. Note in commit body.

- [ ] **Step 6: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/monitored-tables/MonitoredTablesTable.tsx app/src/databricks_labs_dqx_app/ui/components/data-products/DataProductsTable.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/monitored-tables.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/table-spaces.index.tsx app/src/databricks_labs_dqx_app/ui/routes/_sidebar/registry-rules.index.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: quick-action bulk bars on Tables + Spaces; consistency pass\n\nCo-authored-by: Isaac')"
```

---

## Task 17: Compare-versions modal — verify/restore red/green diff highlighting (user 2026-07-16)

**Files:** Investigate `ui/components/drafts/ChangeDiffDialog.tsx` (already has `diffLines` LCS + `bg-destructive/10` removed / `bg-emerald-500/10` added split-diff rendering ~124-190) and the three wrapper dialogs (`RegistryRuleDiffDialog`/`MonitoredTableDiffDialog`/`TableSpaceDiffDialog`).

**Symptom (user):** the compare-versions modal shows NO red/green git-style highlighting of changes. The code DOES contain split-diff highlighting — so this is a DEBUG task (why isn't it showing?), not a build-from-scratch task. Use systematic debugging.

- [ ] **Step 1: Reproduce + root-cause.** Determine why highlighting doesn't appear. Likely causes: (a) the wrapper passes `previous=null` (e.g. `TableSpaceDiffDialog` has no prior snapshot; `RegistryRuleDiffDialog` `hasPrior = version>0 && published exists`) so it renders the plain single-pane (no diff) branch instead of the split diff; (b) the two YAML strings are identical after normalization so every line is "equal"; (c) a rendering/theme bug making the bg tints invisible. Read the dialog + wrappers and identify which branch actually renders for the reported case (most likely the "no previous version → plain pane" path is being hit when the user expects a diff).

- [ ] **Step 2: Fix the identified cause.** If it's the missing-prior-version case: ensure that when a prior version DOES exist it's fetched and passed as `previous` so the split diff renders; if a case legitimately has no prior (brand-new draft), that's correct behavior (nothing to diff) — but confirm the reported scenario has a prior. If it's a contrast/theme bug, strengthen the added/removed background tints so they're clearly visible in both light and dark. If the highlighting is genuinely fine and the issue is only the no-prior case, document that and make the empty/plain state clearer ("No previous version to compare").

- [ ] **Step 3: Type-check + manual reasoning (both themes).** `cd app && node_modules/.bin/tsc -b --incremental` exit 0. Confirm a rule/table/space WITH a prior version shows red removed / green added lines in the modal, visibly, in dark + light.

- [ ] **Step 4: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/drafts/ChangeDiffDialog.tsx
git commit -m "$(printf 'App Feedback v3: fix red/green highlighting in compare-versions modal\n\nCo-authored-by: Isaac')"
```

---

## Task 18: Animated cycling placeholder on the empty decision-point button (user 2026-07-16)

**Files:** `ui/components/RegistryRuleFormDialog.tsx` (the empty-state decision-point button from Task 8 — the inverted-fill cell that opens the type/function picker). i18n: 4 locales.

**Context:** Task 8 built an inverted-fill decision-point button shown in the empty state (before a check is chosen), currently with a static placeholder (`rulesRegistry.decisionPointPlaceholder`). The user wants it to feel alive: instead of a static placeholder, animate/cycle through a curated shortlist of really common check options (a mix of DQX-native checks AND low-code sub-functions), so the user sees example possibilities rotating.

- [ ] **Step 1: Curate the shortlist (steward persona).** Pick ~6–10 genuinely common, recognisable options a data steward would reach for, from the AVAILABLE options (real check-function friendly labels via the check-functions list + the low-code "Condition Builder" and "SQL" pseudo-options). Adopt a data-steward lens: e.g. "Is Not Null", "Is Unique", "Is In Range", "Matches Regex", "Is In List / Allowed Values", "Custom Condition", "SQL"… — but derive the exact set from the real available functions (don't hardcode names that may not exist; map from the loaded check-functions labels + the Core options). Keep it to the truly common ones, not the long tail.

- [ ] **Step 2: Animate the placeholder.** When the decision-point button is EMPTY (no selection yet), cycle the placeholder text through the shortlist (e.g. a gentle fade/slide swap every ~2s, using the existing Motion library already in the app, or a lightweight CSS transition). Respect `prefers-reduced-motion` — fall back to the static placeholder (or no animation) when reduced motion is requested. The animation is purely presentational: clicking the button still opens the real picker; the cycling text is illustrative ("e.g. Is Not Null…"), so prefix or style it so users understand it's an example, not the current value. Stop cycling once a selection is made (the button then shows the chosen label).

- [ ] **Step 3: i18n.** Keep the shortlist entries sourced from existing friendly labels where possible (no new per-option keys). If a framing string is needed (e.g. "e.g. {{example}}"), add `rulesRegistry.decisionPointExample` to all 4 locales. Don't hardcode English example names — use the localized/friendly labels already available.

- [ ] **Step 4: Type-check + manual reasoning.** `cd app && node_modules/.bin/tsc -b --incremental` exit 0. Confirm: empty decision-point button cycles through common examples; reduced-motion shows static; picking an option stops the cycle and shows the real selection; no layout jump as text changes (reserve width / use a stable container). Note in commit body.

- [ ] **Step 5: Commit.**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
git commit -m "$(printf 'App Feedback v3: animated cycling placeholder on empty decision-point button\n\nCo-authored-by: Isaac')"
```

---

## Final verification (after all tasks)
- [ ] **Full type-check:** `cd app && node_modules/.bin/tsc -b --incremental` clean.
- [ ] **Backend tests:** `cd app && uv run --group test pytest tests/ -q` — only the documented pre-existing failures remain; no NEW failures. Confirm any new keys didn't add to the i18n-parity failures.
- [ ] **i18n parity:** every new key present + translated in all 4 locales.
- [ ] **No forbidden staging:** no `app/uv.lock`/`__dist__` staged; `target.dev.yml` stays untracked; each task one focused signed commit.
- [ ] **Report readiness — do NOT deploy.** Summarize commits + manual-check results + any DONE_WITH_CONCERNS flags for the user.

## Spec coverage map
| Spec | Task |
|---|---|
| A1 friendly labels | 3 |
| A2 single-select picker | 4 |
| B1 dropdown drives editor | 8 (+9 display) |
| B2 column persistence | 11 |
| B3 native arity (backend/frontend) | 6 / 11 |
| B4 rule filter (backend/frontend) | 5 / 7 / 10 |
| B5 unused-column block | 12 |
| C1 renames | 2 |
| C2 delete review screen + view-changes | 13 / 14 |
| C3 actions cleanup | 15 |
| C4 quick-action bars | 16 |
| C5 app tag | 1 |
