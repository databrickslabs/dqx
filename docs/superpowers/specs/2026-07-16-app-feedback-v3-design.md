# App Feedback v3 — DQX Studio — Design / Spec

**Branch:** `dqx/app-feedback-v3` (off `dqx-dqlake-integration` @ `a68087b8`)
**Constraint:** App-layer only (no core `src/databricks/labs/dqx/` changes unless explicitly noted — none are needed). Commits only, **NO deploy**. i18n in all 4 locales (`en`/`es`/`it`/`pt-BR`, en = source of truth). No lint/type suppression. GPG-signed commits ending `Co-authored-by: Isaac`. Never stage `app/uv.lock` or `__dist__`. After backend model/route changes run `make app-regen-api` then `git checkout app/uv.lock`.

Type-check gate (env-specific): `cd app && node_modules/.bin/tsc -b --incremental` (NOT `make app-check` — its `bun run tsc` alias is broken here; NOT `bun x tsc` — wrong TS version). Backend tests: `cd app && uv run --group test pytest tests/...`. eslint is not installed in this env.

Paths are under `app/src/databricks_labs_dqx_app/` unless noted. Line numbers are exploration-pass anchors — **re-locate before editing**.

---

## Workstream A — Foundations

### A1. Backend friendly names for check functions
`backend/routes/v1/check_functions.py`. Add a `label: str` field to `CheckFunctionDef` (`backend/models.py:2269`). Compute in `_introspect_check_functions` (`:363-408`): default = title-case of the function name with underscores→spaces (`is_aggr_equal`→"Is Aggr Equal"), overridden by a curated `_FRIENDLY_LABELS` dict for names needing help — acronyms upper-cased (`SQL`, `IP`, `JSON`, `PII`), and specific rewrites (`is_aggr_equal`→"Is Aggregate Equal", `is_aggr_not_equal`→"Is Aggregate Not Equal", `is_aggr_greater_than`→"Is Aggregate Greater Than", `is_aggr_less_than`→"Is Aggregate Less Than", etc.). Keep the raw `name` too (used as the value/key). Regen orval so `label` is on the FE type. This `label` is the single source of truth used in the function dropdown **and** when rendering saved rule logic.

### A2. Single-select cascading table picker
Build a **single-select** cascading catalog→schema→table picker matching the new-monitored-table look (`components/monitored-tables/TableScopePicker.tsx` / `MultiSelectPopover.tsx`), NOT the old `CatalogBrowser` (3 plain `<Select>`s). Approach: add a `single?: boolean` mode to `MultiSelectPopover` (radio/replace-selection behavior, closes on pick) OR extract a `SingleTableScopePicker` built on the same `Popover`+cmdk primitives / `SearchableSelect`. It must expose `value: string (fqn)` / `onChange(fqn)`. Reuse it in:
- **Table Test "pick table"** — `components/rules/test/TableTestSource.tsx:66` (replace `<CatalogBrowser>`).
- **Low-code join blocks** — `components/rules/lowcode/JoinBlock.tsx:74` (replace `<CatalogBrowser value={join.target_table} …>`).
Both currently use `CatalogBrowser` (single-select but wrong element + spaced too far apart). Do NOT change `CatalogBrowser`'s other callers (profiler multiSelect, reference-table field) unless trivially beneficial. Keep join `target_table` semantics identical (single fqn).

---

## Workstream B — Rule authoring refactor (all in `components/RegistryRuleFormDialog.tsx` + lowcode/ + libs)

### B1. Remove the mode picker; the low-code "decision point" cell drives everything (REVISED per user 2026-07-16)
The rule editor no longer shows a rule-type segmented switch. Instead it **always opens in the low-code editor's chrome**, and the single entry point is the low-code condition row's **decision-point cell** (currently the operator/"is null" cell). Selecting an option there sets the underlying `mode` + editor body behind the scenes.

- Delete `ModeSegmentedSwitch` (`RegistryRuleFormDialog.tsx:848-890`), its render site (`:2267`), and the `rulesRegistry.ruleTypeHeader` section.
- **Initial (empty) state — low-code chrome, gated:** On a fresh rule the editor shows the low-code builder but with authoring gated until a check is chosen:
  - Hide the **"Add condition"**, **"Add aggregated condition"**, and **"Advanced"** buttons.
  - Disable the **"then the row passes/fails"** control (polarity) with a tooltip: *"You must apply your condition logic first"* (new i18n key).
  - The **decision-point cell** (where the low-code row currently shows the operator, e.g. "is null") renders **empty** with an **inverted / high-contrast fill** so it clearly invites a click (it's the call-to-action). (Reuse/extend `OperatorDropdown`/`ValueCell` area in `components/rules/lowcode/LowcodeRow.tsx`.)
- **The decision-point dropdown** (opens on clicking that cell) reuses the current DQX-native check-type picker UI (`FunctionCombobox` presentation), with this ordered content:
  1. **Condition Builder** — description *"Build custom, complex conditions in low-code SQL"* — with a right-pointing arrow (→) affordance; give this option a **subtle fill/highlight** so it stands out. Selecting it → `mode:"lowcode"`: reveals the low-code builder (Add condition / aggregated / Advanced buttons appear, polarity enabled) and this first row becomes a normal low-code condition row.
  2. **SQL** (single option — user refinement 2026-07-16, collapses the former "Single Table SQL" + "Cross-Table SQL" into one) — right-arrow affordance. Selecting → `mode:"sql"` → the SQL predicate editor WITH an optional **"joins" card** (reuse the low-code Advanced card: `AdvancedDisclosure` + `JoinsBuilder`), empty by default.
  - **SQL body type is derived DYNAMICALLY from JOIN PRESENCE, not a menu choice or hidden flag:** reuse the low-code compiler's existing rule (`lowcodeCompile.ts` `compileLowcodeBody`): *no joins declared → `{predicate}` (`sql_expression` / single-table path); a join declared → `{sql_query, merge_columns}` (`sql_query` cross-table path)*. One "SQL" menu entry, one editor; the empty-vs-populated joins card decides single- vs cross-table automatically. `buildDefinition`'s sql branch emits `sql_query` iff joins are present (via the existing `compileJoinsToSql`/`compileLowcodeBody` logic), else `predicate`. WYSIWYG: a join in the box = a cross-table check. On rehydrate, `body.sql_query` present → restore the query (joins may not be fully round-trippable from raw SQL — restore the query text at minimum); `body.predicate` → single-table.
  4. Then the **rest of the existing DQX-native checks** (grouped as today, friendly `label`s from A1), each → `mode:"dqx_native"` with that function. **Filtered** by: the data type(s) of the column(s) added to the rule logic AND the number of columns (arity) — i.e. only show native functions compatible with the currently-declared columns' families and count. (Use the slot families already on the declared columns + the function's column-parameter families/arity from the signature to filter.)
- Once an option is chosen, the editor fills out the corresponding native / low-code / SQL UI below (reuse the existing three body render blocks). Switching the decision-point selection later drives the underlying `mode`/body via the existing `performModeSwitch`/`requestModeChange` machinery (`:2171-2246`), reusing `ModeSwitchDialog` confirmation only where content would be lost.
- The three legacy category surfacings change: `sql_expression` is no longer under "Custom SQL" (it's "Single Table SQL" at the top); `sql_query` is reachable only via "Cross-Table SQL"; the native checks keep their categories but appear below the three top options and are data-type/arity filtered.
- Rule-logic **display** (view mode / `RuleConfigCard.tsx`, `RegistryRuleBadges.tsx`) shows the friendly `label` (or "Condition Builder"/"Single Table SQL"/"Cross-Table SQL" for the low-code/sql pseudo-types) instead of the raw function name.
- The arity/type filtering interacts with B3 (native single- vs multi-column). Note: because the decision point lives on the low-code row, the "columns added to the rule logic" that drive the filter are the declared slots/columns present when the dropdown opens.
- i18n: new keys — `rulesRegistry.decisionPointPlaceholder` (empty-cell hint), `rulesRegistry.polarityGatedTooltip` ("You must apply your condition logic first"), `rulesRegistry.coreConditionBuilder` + `.coreConditionBuilderDesc`, `rulesRegistry.coreSingleTableSql`, `rulesRegistry.coreCrossTableSql` — all 4 locales. Retire `modeDqxNative`/`modeLowcode`/`modeSql`/`ruleTypeHeader`.

### B2. Persist columns-used + tag mappings across type swaps
- `slotTags` already persists (shared state). The gap: `nativeSlots` (`:1159`) is separate from `sqlSlots` (`:1133`), and switching *into* dqx_native re-seeds from the function signature (`:2382`), discarding carried columns; switching native→sql/lowcode doesn't copy `nativeSlots`→`sqlSlots`.
- Fix: on any type swap, carry the existing declared columns (slot names/families + their `slotTags`) across. When switching into a native function, MERGE carried columns with the function's signature-derived arity rather than blind re-seed: keep existing column names/tags, map them onto the function's column parameter(s) per B3 rules. When leaving native, copy `nativeSlots`→`sqlSlots`. Net: the "Columns used" panel content + tag chips survive every swap.

### B3. Native multi- vs single-column semantics (user clarification)
- **Multi-column native functions** (accept a list, e.g. `is_unique`): by DEFAULT ALL declared columns are applied to the function. A declared column is exempt from "applied to the function" ONLY if it is explicitly referenced in the **filter** condition (then it's a filter-only column). So the function's column argument = all declared slots minus filter-only ones.
- **Single-column native functions** (arity 1): only the TOP/first declared column is applied to the function; any additional columns exist solely for the advanced filter. When the user clicks "add column" on a single-input function, open an explanatory popover/dialog: "Only the top column applies to this rule; the rest are used only in advanced filter conditions." (New i18n key, 4 locales.)
- The materializer's native substitution (`materializer.py:_substitute_arguments`) must reflect this: bind the function's column arg(s) from the applied columns per the arity rule; filter-only columns are used only in the filter substitution (B4), not the function args.
- Determine arity from the function signature (`deriveSlotsAndParameters` in `lib/registry-rule-conversion.ts`).

### B4. Rule-level filter (reimplemented from scratch; drop per-binding filter)
- **Backend model:** add `filter: str | None` to `RuleDefinition` (`registry_models.py:95-119`), mirroring `error_message`. Auto-freezes into `RuleVersion.definition`.
- **Validation:** validate at rule create/update in `RegistryService` (reuse the `is_sql_query_safe(f"SELECT * FROM _t WHERE ({filter})")` + max-len logic currently in `apply_rules_service.py:78-98`); raise a rule-validation error on unsafe filter.
- **Fingerprint:** include `filter` in `compute_definition_fingerprint` / `registry_fingerprint.py` canonicalization so filter-only edits register as changes (and modified-since-publish detection works).
- **Materialize:** in `materializer.render_check`, read `definition.filter`, run it through `_substitute_text(filter, group, definition.slots)` for `{{slot}}` support, set `check_dict["filter"]`. This is the native `DQRule.filter`.
- **Remove per-binding filter:** delete the `row_filter` UI in `RuleConfigCard.tsx:800-818` (+ prop `:509-511,:555`), `handleRowFilterChange` (`monitored-tables.$bindingId.tsx:2224`, wiring `:2420`), and `row_filter` in `shared.tsx:203,:226`. Drop the binding-filter data path (per user decision "rule filter only"): stop reading `applied.row_filter` in `materializer.render_check` (`:361-367,:583`) and stop threading it through `apply_rules_service` / `saveAppliedRules` / `applyRule`. Leave the `dq_applied_rules.row_filter` COLUMN in place (harmless orphaned column — no destructive migration); just stop writing/reading it. Remove `monitoredTables.ruleFilter*` i18n keys.
- **Editor UI:** add a `filter` state var in `RegistryRuleFormDialog.tsx` (mirror `errorMessage` at `:1164/:1234/:1426/:1541/:1559/:1570`), rendered as a text input inside an `AdvancedDisclosure`. Add an `AdvancedDisclosure` to the **native** and **SQL** mode branches (only lowcode has one today at `:2305`); put the filter field in all three Advanced sections. The filter field offers `{{slot}}` column references (reuse the declared-columns list already exposed to lowcode/groupby editors).

### B5. Unused-column (save-blocking) validation
- A declared slot counts as **used** if its `{{slot}}` placeholder is referenced anywhere in the rule body: native function args (per B3 arity), SQL `predicate`, `sql_query`, low-code AST rows/joins/group-by, OR the filter (B4). For single-column native funcs, the top column is "used" by being the function arg; extra columns must appear in the filter to count as used.
- If any declared slot is unreferenced, disable ALL save/submit buttons at the top of the editor and show an error naming the unused column(s). (New i18n key, 4 locales.) Implement as a derived `unusedSlots` memo feeding the existing save-enable logic (`:1518-1544`).

---

## Workstream C — Overview pages, Review&Approve removal, renames, app tag

### C1. Renames (labels only; route paths + internal ids unchanged)
- "Rules Registry"→**"Rules"**, "Monitored Tables"→**"Tables"**, "Table Spaces"→**"Spaces"**. Update i18n VALUES (not keys) in all 4 locales:
  - Sidebar: `sidebar.rulesRegistry`, `sidebar.monitoredTables`, `sidebar.dataProducts`.
  - Titles/breadcrumbs: `rulesRegistry.title`, `monitoredTables.title` + `monitoredTables.breadcrumb`, `dataProducts.title`.
  - Review the many `dataProducts.*` / `monitoredTables.*` strings that embed "Table Space"/"Monitored Table" prose (new/create/search/empty-state) and update to "Space"/"Table" for consistency where user-facing.
- Do NOT rename route paths (`/registry-rules`, `/monitored-tables`, `/table-spaces`) or internal identifiers (`dataProducts`).

### C2. Delete the Review & Approve screen + admin refs; move "view changes" into Actions columns
- **Delete** `routes/_sidebar/rules.drafts.tsx` (the `/rules/drafts` route), remove the sidebar nav block (`route.tsx:134-147` + its `<hr>`), and the profiler "view in drafts" link (`profiler.tsx:2128`). Let `routeTree.gen.ts` regenerate. Keep `useApprovalsMode` and the `ApprovalsModeCard` in config — approvals MODE still governs submit/approve behavior; only the standalone review SCREEN is removed. (Confirm: the user said "get rid of Review&Approve screen and references in admin settings" — remove the review-screen references, but the approvals-mode setting itself stays since it drives the approve/reject flow the overview pages now host. If the user meant remove the approvals-mode setting entirely, that's a bigger change — FLAG at implementation if ambiguous.)
- **Move "view changes"** into the Actions column of all three overview pages, reusing the exported `RegistryRuleDiffDialog` / `MonitoredTableDiffDialog` / `TableSpaceDiffDialog` from `components/drafts/ChangeDiffDialog.tsx` (keep that file). A `GitCompare`-style icon button, **purple**, positioned right of approve/reject and left of delete, **contrast-safe in dark mode** (use a purple token that meets contrast on the dark table row — verify both themes). Shown ONLY when the row has changes to approve:
  - Rules: `display_status === "modified"` OR `status === "pending_approval"`.
  - Spaces: `display_status === "modified"` OR `"pending_approval"`.
  - Monitored Tables: `status === "pending_approval"` (only signal available — per user decision, no new backend field).
- Each page manages a `diffTarget` state (as `rules.drafts.tsx` did) and renders the matching dialog.

### C3. Actions column cleanup
- **Remove per-row download** (`ExportYamlMenu` iconOnly) from Rules (`registry-rules.index.tsx:535-539`) and Monitored Tables (`monitored-tables.index.tsx:422-427`). (Keep the page-header export menus.) [Table Spaces per-row download `table-spaces.index.tsx:342-347` — the user listed Rules + MT explicitly and "same changes" for Spaces; remove the Spaces per-row download too for consistency.]
- **Hide delete when there's something to approve**: on Monitored Tables (`:476-491`) and Table Spaces (`:396-411`), gate the delete button with `&& !hasPendingChanges(row)` (pending_approval / modified) in addition to the existing `canCreateRules`. (Apply the same to Rules for consistency if it reads well — but the user specified MT + Spaces; keep Rules delete as-is unless it conflicts.)
- Mind `ACTIONS_COL_WIDTH = 152` (`data-table/sticky-actions.ts:35`) budgets 4 icons. Removing download and adding view-changes keeps within budget in most states; if any row state exceeds 4 icons, bump the constant.

### C4. Quick-actions (bulk) bar on Monitored Tables + Table Spaces
- Add selection support to `MonitoredTablesTable` and `DataProductsTable` mirroring `RulesTable`'s `RulesTableSelection` (`RulesTable.tsx:447-453` + checkbox rendering `:563-572,:626-641`).
- Add a `bulkToolbar` to `monitored-tables.index.tsx` and `table-spaces.index.tsx` modeled on `registry-rules.index.tsx:675-729`. Actions (each shown only when ≥1 selected row qualifies): **Run** (selected runnable), **Approve** + **Reject** (selected pending), **Delete** (selected), + **Clear selection**.
- **Consistency pass across all three bulk bars**: align button labels, icons, ordering, confirm-dialog patterns, and disabled/visible logic so Rules/Tables/Spaces bulk bars read as one system. (Rules bar currently has Approve/Deprecate/Revoke/Clear — keep its domain-specific actions but harmonize styling/order.)

### C5. App resource tag
- `app/databricks.yml` `resources.apps.dqx-studio` (`:141`): add a `tags:` map (map form, as the job at `:361` uses):
  ```yaml
  tags:
    app: "dqx-studio"
  ```
- Verify against the current Databricks Apps bundle schema (`databricks bundle validate`); if the app resource rejects `tags`, flag it (app-resource tag support has historically lagged jobs/warehouses).

---

## Out of scope / explicitly not doing
- No route-path renames or internal-identifier renames (labels only).
- No destructive migration for `dq_applied_rules.row_filter` (leave the orphaned column).
- No new backend `display_status` for MonitoredTableOut (MT uses `pending_approval` only).
- No changes to `CatalogBrowser`'s profiler/reference-table callers.
- Keep the approvals-mode setting; only the review SCREEN is removed.

## Testing & verification
- Backend: `make app-test` for touched services (registry_service filter validation, materializer filter+column substitution, check_functions labels). TDD where a seam exists.
- Frontend: `tsc -b` clean; no suppressions. Manual (documented, not deployed): each overview page (actions, bulk bar, view-changes, renames), the rule editor (function dropdown → editor swap, filter in Advanced, column persistence across swaps, unused-column save-block, single-column add-column popover), Table Test + join pickers.
- i18n: every new/changed key present + translated in all 4 locales.
- After backend changes: `make app-regen-api` then `git checkout app/uv.lock`; commit regenerated `api.ts`.
