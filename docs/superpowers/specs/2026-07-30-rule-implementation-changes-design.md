# Rule Implementation-Tab Changes — Design

**Date:** 2026-07-30
**Branch:** `dqx-studio/rule-implementation-changes`
**Scope:** Frontend-only. DQX Studio rule authoring UI — the Implementation tab of
`RegistryRuleFormDialog.tsx`, plus the "Build with AI" banner and i18n locales.
No backend changes.

## Context

All rule authoring runs through `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`
(~5000 lines). The Implementation tab renders, in order: a "Columns used"
(`SlotsPanel`) section, a "Condition" section (header carries the authoring-surface
pill and the granularity control), an "Advanced" disclosure, and a "THEN THE ROW"
polarity switch. Three authoring modes share this layout: `dqx_native` (built-in
checks), `lowcode` (visual builder), and `sql`.

The recent `aa321527` / `c7dda321` commits reworked this area. Several of those
changes regressed the UX; this spec restores or adjusts specific pieces. Where a
change "restores" earlier behaviour, the earlier source is named by commit so the
implementer can read it directly.

## Changes

### 1. Remove the derived "APPLIES TO" granularity tag; keep the SQL switch

The Condition header (`RegistryRuleFormDialog.tsx` ~3985–4011) renders granularity
two ways under an "APPLIES TO" label (`granularityLabel`):

- when `granularityFrozenReason !== undefined` → a read-only `GranularityTag`
  (native checks, visual builder, plain-predicate SQL). **This is purely
  informational. Remove it** — both the `GranularityTag` branch and its
  surrounding label/wrapper for the frozen case.
- when `granularityFrozenReason === undefined` → the real two-way
  `GranularitySwitch`. **This is a genuine choice** (only for a full SQL *query*,
  where Row-level vs Table-level is the whole difference between merge-columns and
  a single aggregated verdict). **Keep it**, along with its "APPLIES TO" label in
  that case.

Net effect: the "APPLIES TO" row disappears for native / visual-builder /
plain-predicate SQL, and remains as a live switch only for full SQL queries.

### 2. Rule-type picker: fold SQL into the Custom condition group, rename, re-describe

In `ConditionSelector`'s root view (`RegistryRuleFormDialog.tsx` ~742–830):

- The **SQL entry** currently sits in its own bottom group (`sqlGroup` heading,
  `sqlConditionShortcut` label "Write SQL", `sqlConditionShortcutDesc`). Move this
  entry **into the Custom condition `CommandGroup` at the top**, rendered as a
  second `CommandItem` directly beneath the "Custom condition" item. Remove the
  now-empty bottom SQL `CommandGroup`.
- Its selection behaviour is unchanged: `onSelect({ type: "sql" })` + close.
- **Rename** the label to **"SQL"** and description to
  **"Write custom single-table or cross-table SQL"**.
  - i18n: repurpose `sqlConditionShortcut` → `"SQL"` and `sqlConditionShortcutDesc`
    → `"Write custom single-table or cross-table SQL"` (keys stay; values change),
    OR add `sqlCustomLabel` / `sqlCustomDesc`. Prefer editing the existing two keys
    to avoid orphans — verify no other consumer depends on the old strings
    (`grep sqlConditionShortcut`).
- **Search matching:** the SQL entry must still appear when the query matches SQL
  terms. `sqlEntryMatchesQuery` already covers this; keep it. Since the SQL item now
  lives inside the Custom-condition group, its visibility is
  `customConditionMatchesQuery || sqlEntryMatchesQuery` at the group level, but the
  SQL `CommandItem` itself gates on `sqlEntryMatchesQuery` and the Custom-condition
  item gates on `customConditionMatchesQuery`, so a search for "sql" shows only the
  SQL item under the group heading (and vice-versa).

- **Custom condition group description:** currently `customConditionDesc` =
  "Build your own rule when none of the built-in checks fit". Restore the
  even-earlier wording from before `aa321527`:
  **"Build custom, complex conditions in low-code SQL"** (the value of
  `coreConditionBuilderDesc` in `aa321527^`). Update `customConditionDesc` to this
  string.

### 3. Authoring-surface pill (Visual builder / SQL) — inverse color

`CUSTOM_SURFACE_TAB_CLASS` (`RegistryRuleFormDialog.tsx` ~393–402) styles both tabs
with indigo active/hover states. Replace the indigo hue with an **inverse
(hue-less) treatment** driven by theme tokens:

- Active tab: **`bg-foreground` fill with `text-background`** — i.e. dark-on-light in
  light mode, light-on-dark in dark mode. Keep the rounded-full pill + a subtle
  ring (`ring-1 ring-border` or `ring-foreground/20`).
- Inactive tab: `text-muted-foreground`; hover → `hover:bg-foreground/10
  hover:text-foreground`.
- Remove every `indigo-*` class from this constant. No `dark:`-specific hue needed
  since the tokens invert automatically, but keep any `dark:` override required to
  beat the shared `TabsTrigger` default active background (verify in dark mode).

### 4. Remove hovertext from "Visual builder" when SQL is selected

The `CursorTooltip` wrapping the "Visual builder" `TabsTrigger`
(`RegistryRuleFormDialog.tsx` ~3965–3976) passes
`text={mode === "sql" ? customSurfaceVisualHint : undefined}`. Change so the
tooltip is **never** shown on the Visual builder trigger (pass `undefined`
unconditionally, or drop the `CursorTooltip` wrapper entirely). The
`customSurfaceVisualHint` key can remain defined but unused, or be removed if no
other consumer references it (`grep` first).

### 5. Remove the standalone "← Change rule type" button; restore the header chip

- **Remove** the inline `<button>` (`RegistryRuleFormDialog.tsx` ~4018–4027) with
  the `ArrowLeft` icon and `changeRuleTypeHeader` label that sits below the
  Condition header.
- **Restore** the chip-variant `ConditionSelector` on the Condition header — the
  affordance present in `aa321527` (~3948–3960). It renders when `conditionChosen`,
  shows `currentTypeLabel`, uses `variant="chip"`, `initialView="root"`,
  `onSelect={requestModeChange}`, and reopens the rule-type picker. This restores
  the ability to change type from **SQL mode** back to a built-in check.
- The chip sits in the header's `action` area, next to (before) the authoring-surface
  pill and the granularity switch. Label it with the existing `ruleTypeLabel`
  ("Rule type") uppercase caption, matching the `aa321527` render.
- The `chip` variant of `ConditionSelector` still exists (see the `variant?: "field"
  | "chip"` prop and its render at ~664/698), so no new component work — just wire
  it back into the header.

### 6. "IF" alignment and font size

Across all three modes, the "IF" framing word must (a) left-align with "THEN THE
ROW" (currently inset) and (b) match its (larger) font size.

- "THEN THE ROW" renders through `FramingWord` (~999): `text-[11px] font-semibold
  uppercase tracking-[0.08em] text-muted-foreground`.
- "IF" currently renders as inline spans at `text-[10px] ... tracking-wider` with a
  leading indent (`pl-2` inside an `h-8` cell) in three places:
  - the anchor row (~4041–4046): `<div className="... pl-2 ...">` + `text-[10px]` IF span
  - the SQL IF row (~4151–4154): `<div className="flex h-8 items-center pl-2">` + `text-[10px]`
  - `LowcodeRow.tsx` (~125): `text-[10px] ...` IF span
- **Fix:** render every "IF" through `FramingWord` (preferred — one source of truth)
  or, if the surrounding grid/cell layout makes that awkward, match its classes
  exactly (`text-[11px]`, `tracking-[0.08em]`). **Remove the leading `pl-2`/inset**
  so IF's left edge aligns with THEN THE ROW's left edge. Verify alignment in all
  three modes (native, low-code, SQL) and in `LowcodeRow`.

### 7. "Columns used" panel — consistent (thinner) height

Selecting a basic native check makes the "Columns used" panel *thin*, while a fresh
page / other rule types render it *thicker*, so everything from Condition down
shifts vertically when the rule type changes. The user prefers the **thinner**
height consistently.

- Investigate the height delta in `SlotsPanel` (~1490+) and its header `action`
  footprint (the add-button reservation, ~1494–1560) and slot-row rendering
  (~1569+). The difference is likely the add-button variant (single-column-fn
  Popover vs plain Button vs gated) and/or per-slot chrome (`SlotTagRegion`,
  disclosure chevron) rendered in some modes but not others.
- **Fix:** normalize the panel's baseline height so it does not change when the mode
  changes — match the thinner (basic-check) state. Content-driven growth (more slots)
  is fine; only the *baseline* thickening is removed. Confirm by switching rule
  types on a fresh rule and observing no vertical shift of the Condition section.

### 8. "Merge results back on" — remove help text, autofill from Columns used

In the SQL row-level branch (`RegistryRuleFormDialog.tsx` ~4190–4210):

- **Remove** the help paragraph rendering `granularityMergeColumnsHelp` ("Key
  columns present on the monitored table…"). Remove the key from all four locales.
- **Autofill:** when the "Merge results back on" field would render and
  `sqlMergeColumns` is empty, default it to the declared "Columns used" (the
  `sqlSlots` names, i.e. the `sqlMergeColumnOptions` values joined by ", ").
  - Implement as an effect: when `sqlGranularityIsChoice && sqlGranularity ===
    "row"` becomes true (or the field first renders) and `sqlMergeColumns` is empty
    and `sqlSlots` is non-empty, set `sqlMergeColumns` to the slot names. Do **not**
    clobber a value the user has typed or one restored from a loaded rule
    (`loadedSqlQueryRef` / the reopen path at ~2201 already sets it). Guard so
    autofill only fills a genuinely-empty field and does not fight the user's edits.

### 9. SQL Advanced — remove Joins, move "Merge results back on" into Advanced

For SQL mode only (`RegistryRuleFormDialog.tsx` ~4425–4469 Advanced block, and the
merge field at ~4190–4210):

- **Remove** the `JoinsBuilder` from the SQL-mode Advanced disclosure
  (~4434–4445). Cross-table joins in SQL are hand-written in the query text.
- **Move** the conditionally-shown "Merge results back on" field (the row-level
  branch at ~4190–4204, currently rendered directly under the SQL editor) **into**
  the SQL-mode Advanced disclosure. It should still only render when
  `sqlGranularityIsChoice && sqlGranularity === "row"`. The Table-level aggregate
  warning (~4205–4209) stays where it is (under the editor), or moves with the
  field — implementer's judgment; prefer keeping the warning under the editor since
  it's an inline validity signal, and moving only the merge-columns picker.
- **Cleanup:** with Joins gone from SQL mode, `sqlJoins` state, `setSqlJoins`,
  `sqlJoinsAst`, `sqlJoinsConflict`, and any `buildSqlBody` join wiring specific to
  SQL mode become dead for SQL. Remove the SQL-specific join code paths carefully —
  **the visual builder's `JoinsBuilder` (lowcode Advanced, ~4242) and its
  `lowcodeAst.joins` are unaffected and must stay.** Verify the SQL save path
  (`buildSqlBody`, ~2668+) still compiles predicate/query + merge_columns correctly
  without the join inputs, and that reopening a stored `sql_query` rule (which never
  round-tripped joins anyway) is unchanged.

### 10. "Build with AI" banner — random example prompt

The Build-with-AI banner (`RegistryRuleFormDialog.tsx` ~4609–4665) shows a single
static shine-text placeholder (`aiBuildPlaceholder` = "e.g. Order amount must be
positive and less than $1,000,000").

- Add **~20 hardcoded example prompts** as i18n keys (`aiBuildExample1` …
  `aiBuildExample20`) in all four locales, translated per-locale.
- On mount (once per dialog open), pick one at **random** and use it as the banner's
  placeholder shine-text (the `aiDescription === ""` span at ~4640–4647 and the
  `aria-label`). Use a `useMemo(() => pick random, [])` or a state seeded once so it
  doesn't reshuffle on every render/keystroke.
- Keep `aiBuildPlaceholder` as a safe fallback, or drop it once all references move
  to the random pick. The examples should be realistic DQ requirements phrased in
  plain language (e.g. "Email addresses must be valid and unique", "Order dates
  can't be in the future", "Every customer_id must exist in the customers table",
  "Revenue must be non-negative", …).

## i18n

Every user-facing string change touches `en.json` (source of truth) and must be
mirrored in `pt-BR.json`, `it.json`, `es.json` with translated values. Summary of
key changes:

- `customConditionDesc` → "Build custom, complex conditions in low-code SQL"
- `sqlConditionShortcut` → "SQL"
- `sqlConditionShortcutDesc` → "Write custom single-table or cross-table SQL"
- Remove `granularityMergeColumnsHelp` (all locales) — verify no other consumer.
- `customSurfaceVisualHint` — now unused; remove if no other consumer, else leave.
- `sqlGroup` — now unused if the bottom SQL group is deleted; remove if orphaned.
- Add `aiBuildExample1` … `aiBuildExample20` (all locales).

Run a final check that `en` key set == other locales' key sets (no key present in
one but missing in another).

## Testing & verification

- `make app-check` (tsc + basedpyright) must pass — no type errors, no unused
  symbols left behind (strict `noUnusedLocals`).
- Manual/visual verification of each item via the dev server or the deploy:
  1. APPLIES TO tag gone for native/lowcode/predicate-SQL; switch present for SQL query.
  2. Picker shows Custom condition + SQL together at top; SQL label + desc correct;
     group desc restored.
  3. Pill switch is inverse-colored (dark-on-light / light-on-dark), no blue.
  4. No tooltip on Visual builder in SQL mode.
  5. No standalone back button; header chip switches type from SQL mode.
  6. IF aligns with THEN THE ROW and is the same (larger) size in all 3 modes.
  7. No vertical shift of Condition section when switching rule types.
  8. Merge help text gone; field autofills from Columns used.
  9. SQL Advanced has no Joins; Merge results back on lives in Advanced.
  10. Build-with-AI placeholder varies across dialog opens.
- Existing unit tests under `components/rules/**/*.test.ts` must still pass
  (`ModeSwitchDialog.test.ts`, etc.). Add/adjust tests only where behaviour (not
  pure styling) changed — notably the SQL-mode join removal and merge-columns
  autofill, if they have testable seams.

## Out of scope

- No backend / API changes.
- No changes to visual-builder (lowcode) Joins.
- No changes to the DQX library rule engine.
