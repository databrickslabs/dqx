# Rule Implementation-Tab Changes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply a set of UX fixes to the DQX Studio rule authoring Implementation tab (`RegistryRuleFormDialog.tsx`), plus a random Build-with-AI example prompt.

**Architecture:** All work is frontend-only in the DQX Studio React app under `app/`. The bulk is edits to one large component, `RegistryRuleFormDialog.tsx`, with supporting edits to `LowcodeRow.tsx` and the four i18n locale files. Two pure helpers are extracted to enable real unit tests (AI-prompt picker, merge-columns autofill). Everything else is verified via `make app-check` (tsc + basedpyright + bun UI tests) and described visual checks.

**Tech Stack:** React 19, TypeScript (strict), Tailwind CSS 4, shadcn/ui, react-i18next, bun test runner.

## Global Constraints

- Work happens in `app/` — use `make app-check` to type-check (never raw `tsc`/`bun` unless iterating). Reference: `app/CLAUDE.md`, `app/src/databricks_labs_dqx_app/ui/CLAUDE.md`.
- **Every user-facing string uses `t()`** — never hard-code display text. Get `t` from `useTranslation()`.
- **Add/change every i18n key in all four locales:** `en.json` (source of truth), `pt-BR.json`, `it.json`, `es.json`. A key in `en` but missing elsewhere is a silent bug. Translate the value per locale — don't leave English in a non-English file.
- **Never delete a locale key that still has a consumer.** `grep` the key across the UI before removing it. `en` key set must equal every other locale's key set at the end.
- TypeScript is strict with `noUnusedLocals`/`noUnusedParameters` — remove dead symbols you orphan, or `make app-check` fails.
- Do not touch the backend, the DQX library, or the visual-builder (lowcode) `JoinsBuilder`.
- Import alias `@/` → `app/src/databricks_labs_dqx_app/ui/`.
- All paths below are relative to repo root `/Users/oliver.gordon/Documents/Code/Other/dqx`.
- The primary file: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (~5000 lines). Line numbers are approximate — grep for the quoted anchor strings.

---

### Task 1: Rule-type picker — fold SQL into Custom condition group, rename, restore group description

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (root view of `ConditionSelector`, ~742–830)
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json`, `pt-BR.json`, `it.json`, `es.json`

**Interfaces:**
- Consumes: existing `customConditionMatchesQuery`, `sqlEntryMatchesQuery` memos (~586–608); `onSelect({ type: "lowcode" })` and `onSelect({ type: "sql" })`.
- Produces: no new exported symbols. i18n values changed: `customConditionDesc`, `sqlConditionShortcut`, `sqlConditionShortcutDesc`.

- [ ] **Step 1: Update i18n values in all four locales**

In `en.json` (`rulesRegistry` block, ~1493–1496):
```json
"customConditionGroup": "Custom condition",
"customConditionDesc": "Build custom, complex conditions in low-code SQL",
"sqlConditionShortcut": "SQL",
"sqlConditionShortcutDesc": "Write custom single-table or cross-table SQL",
```
Apply the equivalent translated values to `pt-BR.json`, `it.json`, `es.json` (translate the English meaning; keep the same keys). Example pt-BR:
```json
"customConditionDesc": "Crie condições personalizadas e complexas em SQL low-code",
"sqlConditionShortcut": "SQL",
"sqlConditionShortcutDesc": "Escreva SQL personalizado de tabela única ou entre tabelas",
```
(Use correct it/es translations similarly.)

- [ ] **Step 2: Move the SQL CommandItem into the Custom condition group**

In `RegistryRuleFormDialog.tsx`, the root view (`{view === "root" && (`, ~742). Currently:
- a `CommandGroup heading={customConditionGroup}` holds ONE item (`__custom_condition__`), gated by `customConditionMatchesQuery` (~749–771)
- the native check groups render (`grouped.map`, ~775–803)
- a separate bottom `CommandGroup heading={sqlGroup}` holds the `__sql_condition__` item, gated by `sqlEntryMatchesQuery` (~810–829)

Restructure so the SQL item lives **inside** the Custom condition group, right after the custom-condition item, and **delete** the bottom SQL group. The group renders if either item matches:

```tsx
{(customConditionMatchesQuery || sqlEntryMatchesQuery) && (
  <CommandGroup
    heading={t("rulesRegistry.customConditionGroup")}
    className={COMMAND_GROUP_HEADING_CLASS}
  >
    {customConditionMatchesQuery && (
      <CommandItem
        value="__custom_condition__"
        onSelect={() => {
          setQuery("");
          onSelect({ type: "lowcode" });
          setOpen(false);
        }}
        className="items-start gap-2 text-xs"
      >
        <span className="min-w-0 flex-1">
          <span className="font-medium">{t("rulesRegistry.customConditionGroup")}</span>
          <span className="block text-[10px] text-muted-foreground">
            {t("rulesRegistry.customConditionDesc")}
          </span>
        </span>
      </CommandItem>
    )}
    {sqlEntryMatchesQuery && (
      <CommandItem
        value="__sql_condition__"
        onSelect={() => {
          setQuery("");
          onSelect({ type: "sql" });
          setOpen(false);
        }}
        className="items-start gap-2 text-xs"
      >
        <span className="min-w-0 flex-1">
          <span className="font-medium">{t("rulesRegistry.sqlConditionShortcut")}</span>
          <span className="block text-[10px] text-muted-foreground">
            {t("rulesRegistry.sqlConditionShortcutDesc")}
          </span>
        </span>
      </CommandItem>
    )}
  </CommandGroup>
)}
```
Keep `<CommandEmpty>` and the `grouped.map(...)` native check groups where they are (after this group). Delete the old standalone bottom SQL `CommandGroup` block entirely.

- [ ] **Step 3: Remove the now-orphaned `sqlGroup` key if unused**

Run: `grep -rn "sqlGroup" app/src/databricks_labs_dqx_app/ui`
If the only hits were the deleted render + the locale definitions, remove `"sqlGroup"` from all four locales. If anything else consumes it, leave it.

- [ ] **Step 4: Type-check**

Run: `make app-check`
Expected: PASS (no TS errors, no unused-symbol errors).

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): fold SQL into Custom condition group, restore group copy"
```

---

### Task 2: Condition header — remove derived APPLIES TO tag, remove change-rule-type button, restore header chip

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (Condition `SectionHeader` action, ~3949–4027)

**Interfaces:**
- Consumes: `conditionChosen`, `mode`, `readOnly`, `granularityDescribesSomething`, `granularityFrozenReason`, `effectiveGranularity`, `setSqlGranularity`, `requestModeChange`, `currentTypeLabel`, `anchorOperatorFamily`, `checkFunctions`, `currentSlots`. The `GranularitySwitch` component and the `ConditionSelector` `variant="chip"` path (already exists, ~664/698).
- Produces: no new symbols. Removes usage of `GranularityTag` in the header (keep the component/import if still used elsewhere — grep first) and removes the standalone change-type `<button>`.

- [ ] **Step 1: Restore the header rule-type chip (before the surface pill)**

In the Condition `SectionHeader action={...}` (~3949), inside the `conditionChosen ? (<div className="flex items-center gap-3">...`), add the rule-type chip as the FIRST child (before the `mode !== "dqx_native"` surface-pill block), matching the `aa321527` render:

```tsx
{conditionChosen && !readOnly && (
  <div className="flex items-center gap-2">
    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
      {t("rulesRegistry.ruleTypeLabel")}
    </span>
    <div className="max-w-[20rem]">
      <ConditionSelector
        checkFunctions={checkFunctions}
        currentSlots={currentSlots}
        operatorFamily={anchorOperatorFamily}
        onSelect={requestModeChange}
        disabled={readOnly}
        currentLabel={currentTypeLabel}
        initialView="root"
        variant="chip"
      />
    </div>
  </div>
)}
```

- [ ] **Step 2: Remove the derived granularity tag, keep the switch**

In the granularity block (~3985–4011), the ternary currently renders `GranularitySwitch` when `granularityFrozenReason === undefined` else `GranularityTag`. Remove the tag branch so the whole "APPLIES TO" cluster only renders when granularity is a genuine choice:

```tsx
{granularityDescribesSomething && granularityFrozenReason === undefined && (
  <div className="flex items-center gap-2">
    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
      {t("rulesRegistry.granularityLabel")}
    </span>
    <GranularitySwitch
      value={effectiveGranularity}
      onChange={readOnly ? undefined : setSqlGranularity}
      disabled={readOnly}
      disabledReason={
        effectiveGranularity === "row"
          ? t("rulesRegistry.granularityRowTooltip")
          : t("rulesRegistry.granularityDatasetTooltip")
      }
    />
  </div>
)}
```
(The `GranularityTag` is no longer rendered here. It may still be used in the native check list at ~799 — do NOT remove that; grep `GranularityTag` and keep the import if any consumer remains.)

- [ ] **Step 3: Remove the standalone "← Change rule type" button**

Delete the block at ~4018–4027:
```tsx
{conditionChosen && mode !== "dqx_native" && !readOnly && (
  <button ... > <ArrowLeft .../> {t("rulesRegistry.changeRuleTypeHeader")} </button>
)}
```
Then run `grep -rn "ArrowLeft" app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` — `ArrowLeft` is still used by the operators drill-in back button (~853) so keep the import. If grep shows it's now unused, remove the import.

- [ ] **Step 4: Type-check**

Run: `make app-check`
Expected: PASS. If it flags `GranularityTag` or `ArrowLeft` as unused, remove only the genuinely-orphaned import.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx
git commit -m "feat(app): drop APPLIES TO tag + back button, restore rule-type chip"
```

---

### Task 3: Authoring-surface pill — inverse color; remove Visual builder tooltip in SQL mode

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (`CUSTOM_SURFACE_TAB_CLASS` ~393–402; the pill render ~3958–3982)

**Interfaces:**
- Consumes: `CUSTOM_SURFACE_TAB_CLASS`, `CursorTooltip`, `Tabs`/`TabsList`/`TabsTrigger`.
- Produces: no new symbols.

- [ ] **Step 1: Replace indigo styling with an inverse (token-based) treatment**

Replace `CUSTOM_SURFACE_TAB_CLASS` (~393–402) with:
```tsx
const CUSTOM_SURFACE_TAB_CLASS = [
  "gap-1.5 rounded-full px-3 text-xs",
  // Inverse active state: dark-on-light in light mode, light-on-dark in dark mode.
  // Uses foreground/background tokens (no hue) so it inverts with the theme.
  "data-[state=active]:bg-foreground data-[state=active]:text-background",
  "dark:data-[state=active]:bg-foreground dark:data-[state=active]:text-background",
  "data-[state=active]:ring-1 data-[state=active]:ring-foreground/20",
  "dark:data-[state=active]:border-transparent",
  "data-[state=inactive]:text-muted-foreground",
  "data-[state=inactive]:hover:bg-foreground/10 data-[state=inactive]:hover:text-foreground",
  "dark:data-[state=inactive]:hover:text-foreground",
];
```
Update the doc comment above it to describe the inverse (not indigo) treatment.

- [ ] **Step 2: Remove the Visual builder tooltip in SQL mode**

At the "Visual builder" trigger (~3965–3976), the `CursorTooltip` passes `text={mode === "sql" ? t("rulesRegistry.customSurfaceVisualHint") : undefined}`. Remove the tooltip by dropping the `CursorTooltip` wrapper and rendering the `TabsTrigger` directly:
```tsx
<TabsTrigger
  value="lowcode"
  disabled={readOnly}
  className={cn(CUSTOM_SURFACE_TAB_CLASS)}
>
  <SlidersHorizontal className="h-3.5 w-3.5" />
  {t("rulesRegistry.customSurfaceVisual")}
</TabsTrigger>
```

- [ ] **Step 3: Remove the now-orphaned `customSurfaceVisualHint` key if unused**

Run: `grep -rn "customSurfaceVisualHint" app/src/databricks_labs_dqx_app/ui`
If no consumer remains, remove the key from all four locales. If `CursorTooltip` is now unused in the file, remove its import (grep first — it's used elsewhere in the file, e.g. ~4053, so likely stays).

- [ ] **Step 4: Type-check**

Run: `make app-check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): inverse-color authoring-surface pill, drop visual-builder tooltip"
```

---

### Task 4: "IF" alignment and font size — match "THEN THE ROW"

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (anchor row ~4040–4047; SQL IF row ~4150–4155)
- Modify: `app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/LowcodeRow.tsx` (~124–126)

**Interfaces:**
- Consumes: `FramingWord` component (~999), `t("rulesRegistry.ifCondition")`, `t("rulesRegistry.thenTheRow")`.
- Produces: no new symbols. `FramingWord` may need to be reused in `LowcodeRow.tsx` — if importing it there is awkward (it's defined inside `RegistryRuleFormDialog.tsx` and not exported), match its classes inline instead.

- [ ] **Step 1: Anchor row IF (RegistryRuleFormDialog ~4040–4047)**

The IF cell is `<div className="flex items-center h-8 pl-2 justify-self-start">` wrapping a `<span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">`. Remove the `pl-2` (un-indent) and bump the span to match `FramingWord`:
```tsx
<div className="flex items-center h-8 justify-self-start">
  <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
    {t("rulesRegistry.ifCondition")}
  </span>
</div>
```

- [ ] **Step 2: SQL IF row (RegistryRuleFormDialog ~4150–4155)**

Currently `<div className="flex h-8 items-center pl-2">` + `text-[10px] ... tracking-wider`. Remove `pl-2` and match sizing:
```tsx
<div className="flex h-8 items-center">
  <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
    {t("rulesRegistry.ifCondition")}
  </span>
</div>
```

- [ ] **Step 3: LowcodeRow IF (LowcodeRow.tsx ~124–126)**

Find the IF span (`text-[10px] font-semibold uppercase tracking-wider text-muted-foreground` + `{t("rulesRegistry.ifCondition")}`). Change to `text-[11px] ... tracking-[0.08em]` and remove any leading `pl-*`/inset on its wrapping cell so it left-aligns with the row's other framing. Verify the surrounding grid cell doesn't reintroduce an indent.

- [ ] **Step 4: Type-check and visually confirm alignment**

Run: `make app-check`
Expected: PASS.
Visual check (during Task 10 deploy or dev server): in native, low-code, and SQL modes, "IF" left-aligns with "THEN THE ROW" and is the same (11px) size.

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/LowcodeRow.tsx
git commit -m "fix(app): align IF with THEN THE ROW and match font size"
```

---

### Task 5: "Columns used" panel — consistent (thinner) baseline height

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (`SlotsPanel`, ~1490–1650)

**Interfaces:**
- Consumes: `SlotsPanel` props `isSingleColumnFn`, `disabled`, `addDisabledReason`, `lockFamily`, `slotTags`, and the `SectionHeader action` add-button variants (~1494–1560).
- Produces: no new symbols.

- [ ] **Step 1: Identify the height delta**

Run the dev server or inspect visually: switching from a basic native check (thin) to a fresh/low-code/SQL rule (thick) shifts the Condition section down. The delta comes from the panel's header `action` footprint and/or per-slot chrome differing by mode. Read `SlotsPanel` (~1490–1650) and determine which element's height varies:
- header add-button: all three branches (gated / single-column Popover / plain) already use `h-7` — confirm they're equal height.
- slot rows: `SlotTagRegion`, the disclosure chevron, and the family Select (`h-7`) render conditionally by `disabled`/`lockFamily`. A native basic check uses `lockFamily` (Badge, shorter) while low-code/SQL use the `h-7` Select (taller). This row-height difference per mode is the likely cause.

- [ ] **Step 2: Normalize slot-row height**

Make the slot row's control column a fixed height regardless of `lockFamily`, so the Badge (native) and the Select (low-code/SQL) occupy the same vertical space. In the row's control cell (~1611–1636), wrap both the Badge branch and the Select branch so their container is `h-7 flex items-center` (or give the Badge branch matching vertical padding), eliminating the per-mode delta. Concretely, ensure the `disabled || lockFamily` Badge branch container is `className="flex items-center gap-1.5 h-7"` to match the Select's `h-7`.

- [ ] **Step 3: Verify no baseline shift**

Run: `make app-check`
Expected: PASS.
Visual check: on a fresh rule, cycle rule type native ⇄ low-code ⇄ SQL; the "Columns used" panel keeps a constant baseline height and the Condition section below does not jump. Content growth (adding slots) may still grow the panel — that's expected.

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx
git commit -m "fix(app): keep Columns used panel height consistent across rule types"
```

---

### Task 6: Merge-columns autofill helper + wiring; remove help text

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.ts`
- Create: `app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.test.ts`
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (merge field render ~4190–4210; add an effect)
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json` (remove `granularityMergeColumnsHelp`)

**Interfaces:**
- Consumes: `sqlSlots` (array of `{ name: string; family: ... }`), `sqlMergeColumns` (string), `setSqlMergeColumns`.
- Produces: `computeMergeColumnsAutofill(current: string, slotNames: string[]): string | null` — returns the string to set when autofill should apply, or `null` to leave the field untouched.

- [ ] **Step 1: Write the failing test**

`app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.test.ts`:
```ts
import { describe, expect, test } from "bun:test";
import { computeMergeColumnsAutofill } from "./mergeColumnsAutofill";

describe("computeMergeColumnsAutofill", () => {
  test("fills from slot names when field is empty", () => {
    expect(computeMergeColumnsAutofill("", ["id", "region"])).toBe("id, region");
  });
  test("leaves a non-empty field untouched", () => {
    expect(computeMergeColumnsAutofill("id", ["id", "region"])).toBeNull();
  });
  test("treats whitespace-only as empty", () => {
    expect(computeMergeColumnsAutofill("   ", ["id"])).toBe("id");
  });
  test("returns null when there are no slots", () => {
    expect(computeMergeColumnsAutofill("", [])).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.test.ts`
Expected: FAIL — module/function not found.

- [ ] **Step 3: Implement the helper**

`app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.ts`:
```ts
/**
 * Decide whether to autofill the "Merge results back on" field. Returns the
 * comma-joined slot names when the field is empty (or whitespace-only) and there
 * is at least one slot; otherwise null (leave the author's / loaded value alone).
 */
export function computeMergeColumnsAutofill(current: string, slotNames: string[]): string | null {
  if (current.trim() !== "") return null;
  if (slotNames.length === 0) return null;
  return slotNames.join(", ");
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.test.ts`
Expected: PASS.

- [ ] **Step 5: Wire the autofill effect in RegistryRuleFormDialog**

Import the helper. Add an effect near the other SQL state effects (after `sqlMergeColumnOptions` is defined, ~3550). The effect fires when the row-level merge field becomes relevant:
```tsx
// Autofill "Merge results back on" from the declared Columns used the first
// time a row-level SQL query needs it, without clobbering a typed/loaded value.
useEffect(() => {
  if (!(sqlGranularityIsChoice && sqlGranularity === "row")) return;
  const next = computeMergeColumnsAutofill(
    sqlMergeColumns,
    sqlSlots.map((s) => s.name),
  );
  if (next !== null) setSqlMergeColumns(next);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally not
  // depending on sqlMergeColumns: this fills only on the empty→relevant edge.
}, [sqlGranularityIsChoice, sqlGranularity, sqlSlots]);
```
NOTE: if the repo's eslint config forbids the disable comment (see AGENTS.md "never disable linting"), instead structure the guard without needing it — e.g. depend on `sqlMergeColumns` too; the helper already returns `null` when the field is non-empty, so re-running on every merge-columns change is harmless (it will never overwrite a non-empty value). Prefer that: include `sqlMergeColumns` and `setSqlMergeColumns` in deps and drop the disable comment.

- [ ] **Step 6: Remove the merge-columns help text**

In the row-level branch (~4200–4204), delete the `<p>` rendering `t("rulesRegistry.granularityMergeColumnsHelp")`. Then run `grep -rn "granularityMergeColumnsHelp" app/src/databricks_labs_dqx_app/ui` — if orphaned, remove the key from all four locales.

- [ ] **Step 7: Type-check and full UI test run**

Run: `make app-check`
Expected: PASS (includes the new bun test).

- [ ] **Step 8: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.ts app/src/databricks_labs_dqx_app/ui/lib/mergeColumnsAutofill.test.ts app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): autofill merge columns from Columns used, drop help text"
```

---

### Task 7: SQL Advanced — remove Joins, move "Merge results back on" into Advanced

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (SQL merge field ~4190–4210; SQL Advanced ~4425–4469; SQL state/wiring)

**Interfaces:**
- Consumes: the merge field JSX (now including the autofill from Task 6), `AdvancedDisclosure`, `sqlGranularityIsChoice`, `sqlGranularity`, `sqlMergeColumns`/`setSqlMergeColumns`, `sqlMergeColumnOptions`. The SQL `JoinsBuilder`, `sqlJoins`/`setSqlJoins`, `sqlJoinsAst`, `sqlJoinsConflict`.
- Produces: no new symbols. Removes SQL-mode join wiring.

- [ ] **Step 1: Move the "Merge results back on" field into SQL Advanced**

In the SQL Condition branch (~4148–4212), the row-level merge field currently renders under the editor (~4190–4204). Cut the `GroupByField` merge-columns block (the `sqlGranularity === "row"` branch that renders `<GroupByField ... label={granularityMergeColumnsLabel} ...>`). Leave the Table-level aggregate warning (`granularityAggregateWarning`, ~4205–4209) where it is under the editor.

In the SQL Advanced disclosure (~4434), render the merge field there instead, gated the same way:
```tsx
<AdvancedDisclosure label={t("rulesRegistry.advancedSectionLabel")}>
  {sqlGranularityIsChoice && sqlGranularity === "row" && (
    <div className="space-y-1.5">
      <GroupByField
        value={sqlMergeColumns}
        onChange={setSqlMergeColumns}
        declaredColumns={sqlMergeColumnOptions}
        disabled={readOnly}
        label={t("rulesRegistry.granularityMergeColumnsLabel")}
        placeholder={t("rulesRegistry.granularityMergeColumnsPlaceholder")}
      />
    </div>
  )}
  {/* Row filter (kept) */}
  ...existing PredicateEditor filter block...
  {thresholdField}
</AdvancedDisclosure>
```
(The autofill effect from Task 6 keeps working regardless of where the field renders — it's state-driven, not render-driven.)

- [ ] **Step 2: Remove the JoinsBuilder from SQL Advanced**

Delete the `<JoinsBuilder ast={sqlJoinsAst} onChange={(next) => setSqlJoins(next.joins)} ... />` block (~4440–4445) from the SQL Advanced disclosure. Keep the row-filter `PredicateEditor` and `thresholdField`.

- [ ] **Step 3: Remove now-dead SQL join wiring**

`grep -rn "sqlJoins\|sqlJoinsAst\|sqlJoinsConflict\|setSqlJoins" app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` and remove the SQL-mode join code that is now unreachable/dead:
- `sqlJoinsAst` memo (~3556) — remove if only the deleted JoinsBuilder used it.
- `sqlJoinsConflict` (~2469) and its use in the error gate (~2487) — with no structured SQL joins, this can no longer trigger; remove the variable and its `?? sqlJoinsConflict` in the error chain.
- `sqlJoins` state (~1968) + `setSqlJoins` calls: the save path (`buildSqlBody`, ~2668–2682) and test-draft path (~2392, ~4494) pass `sqlJoins`. Since SQL joins are gone, pass an empty array to `buildSqlBody` (or drop the join parameter if `buildSqlBody`'s signature allows) so a hand-written `SELECT … JOIN …` still compiles as a `sql_query`. **Do not change `buildSqlBody`'s handling of the query text itself** — only stop feeding it structured joins. Verify by reading `buildSqlBody` and the `sqlJoins`-consuming effects (~2560–2605).
- Keep `sqlJoins`/`setSqlJoins` ONLY if removing them cascades into shared compile helpers used by low-code; in that case leave the state as a constant `[]` and remove only the UI + conflict. Prefer full removal if clean.

**Caution:** the lowcode `JoinsBuilder` (~4242, `ast={lowcodeAst}`) and `lowcodeAst.joins` are separate and must remain fully functional.

- [ ] **Step 4: Type-check**

Run: `make app-check`
Expected: PASS. Fix any unused-symbol errors from the join removal.

- [ ] **Step 5: Verify save/reopen still work for SQL rules**

Read the SQL save path and confirm: a plain predicate saves as `sql_expression`; a full `SELECT` query saves as `sql_query` with `merge_columns` (row-level) or aggregate (table-level); reopening a stored `sql_query` rule restores predicate + merge columns (the reopen path ~2181–2219 already sets `sqlJoins([])`, so it's consistent). No runtime path should reference a removed symbol.

- [ ] **Step 6: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx
git commit -m "feat(app): SQL mode — drop structured Joins, move merge columns into Advanced"
```

---

### Task 8: Build-with-AI — random example prompt

**Files:**
- Create: `app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.ts`
- Create: `app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.test.ts`
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (AI banner ~4609–4665; add the picked example)
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json` (add `aiBuildExample1`..`aiBuildExample20`)

**Interfaces:**
- Consumes: `t()`, the banner placeholder render (`aiDescription === ""` span ~4640, and `aria-label` ~4637).
- Produces: `pickAiExampleKey(count: number, rand?: () => number): string` — returns an `aiBuildExampleN` key (1-based) chosen via `rand` (default `Math.random`).

- [ ] **Step 1: Write the failing test**

`app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.test.ts`:
```ts
import { describe, expect, test } from "bun:test";
import { pickAiExampleKey } from "./aiExamplePrompt";

describe("pickAiExampleKey", () => {
  test("returns a 1-based key within range", () => {
    expect(pickAiExampleKey(20, () => 0)).toBe("aiBuildExample1");
    expect(pickAiExampleKey(20, () => 0.999)).toBe("aiBuildExample20");
  });
  test("maps mid-range value correctly", () => {
    expect(pickAiExampleKey(20, () => 0.5)).toBe("aiBuildExample11");
  });
  test("never returns index 0 or > count", () => {
    for (let r = 0; r < 1; r += 0.017) {
      const key = pickAiExampleKey(20, () => r);
      const n = Number(key.replace("aiBuildExample", ""));
      expect(n).toBeGreaterThanOrEqual(1);
      expect(n).toBeLessThanOrEqual(20);
    }
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the picker**

`app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.ts`:
```ts
/** Total number of aiBuildExample* keys defined in the locales. */
export const AI_EXAMPLE_COUNT = 20;

/**
 * Pick a 1-based `aiBuildExampleN` i18n key at random. `rand` is injectable for
 * tests; defaults to Math.random. Clamps so the result is always in [1, count].
 */
export function pickAiExampleKey(count: number, rand: () => number = Math.random): string {
  const n = Math.min(count, Math.max(1, Math.floor(rand() * count) + 1));
  return `aiBuildExample${n}`;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.test.ts`
Expected: PASS.

- [ ] **Step 5: Add 20 example prompts to all four locales**

Add `aiBuildExample1` … `aiBuildExample20` to the `rulesRegistry` block of each locale (translated per locale). English values (prefix each with "e.g. " to match the existing placeholder tone):
```json
"aiBuildExample1": "e.g. Order amount must be positive and less than $1,000,000",
"aiBuildExample2": "e.g. Email addresses must be valid and unique",
"aiBuildExample3": "e.g. Order dates can't be in the future",
"aiBuildExample4": "e.g. Every customer_id must exist in the customers table",
"aiBuildExample5": "e.g. Revenue must be non-negative",
"aiBuildExample6": "e.g. Country code must be a valid ISO 3166 value",
"aiBuildExample7": "e.g. Phone numbers must match the E.164 format",
"aiBuildExample8": "e.g. Status must be one of active, pending, or closed",
"aiBuildExample9": "e.g. No duplicate rows across id and event_time",
"aiBuildExample10": "e.g. Ship date must be on or after the order date",
"aiBuildExample11": "e.g. Discount percentage must be between 0 and 100",
"aiBuildExample12": "e.g. Product SKU must not be null or empty",
"aiBuildExample13": "e.g. Total must equal the sum of line-item amounts",
"aiBuildExample14": "e.g. Latitude must be between -90 and 90",
"aiBuildExample15": "e.g. Currency must be a 3-letter ISO 4217 code",
"aiBuildExample16": "e.g. Created timestamp must not be null",
"aiBuildExample17": "e.g. Account balance must reconcile with the ledger table",
"aiBuildExample18": "e.g. Postal code must match the country's format",
"aiBuildExample19": "e.g. Quantity must be a whole number greater than zero",
"aiBuildExample20": "e.g. Every foreign key in orders must resolve in products"
```
Translate all 20 values for `pt-BR.json`, `it.json`, `es.json`.

- [ ] **Step 6: Wire the random pick into the banner**

Import `pickAiExampleKey` and `AI_EXAMPLE_COUNT`. Near the top of the component body, memoize one key per dialog open:
```tsx
const aiExampleKey = useMemo(() => pickAiExampleKey(AI_EXAMPLE_COUNT), []);
```
In the banner (~4637 and ~4645), replace `t("rulesRegistry.aiBuildPlaceholder")` with `t(\`rulesRegistry.${aiExampleKey}\`)` for both the `aria-label` and the shine-text span. Keep `aiBuildPlaceholder` defined (harmless fallback) unless grep shows it fully orphaned, in which case remove it from all locales.

- [ ] **Step 7: Type-check and full UI test run**

Run: `make app-check`
Expected: PASS (includes both new bun tests).

- [ ] **Step 8: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.ts app/src/databricks_labs_dqx_app/ui/lib/aiExamplePrompt.test.ts app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): random Build-with-AI example prompt from a set of 20"
```

---

### Task 9: Final verification — locale parity + full check

**Files:** none (verification only)

- [ ] **Step 1: Locale key parity**

Run a parity check that every key in `en.json`'s `rulesRegistry` (and any block touched) exists in the other three locales and vice-versa:
```bash
cd app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
for f in pt-BR it es; do
  echo "== en vs $f =="
  diff <(python3 -c "import json,sys;print('\n'.join(sorted(json.load(open('en.json')).get('rulesRegistry',{}).keys())))") \
       <(python3 -c "import json,sys;print('\n'.join(sorted(json.load(open('$f.json')).get('rulesRegistry',{}).keys())))")
done
```
Expected: no diff output for any locale. Fix any missing/extra keys.

- [ ] **Step 2: Full app check**

Run: `make app-check`
Expected: PASS (tsc, basedpyright, all bun UI tests).

- [ ] **Step 3: Commit any parity fixes**

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "chore(app): sync i18n locale keys for rule implementation changes"
```
(Skip if nothing changed.)

---

## Self-Review

**Spec coverage:**
- Spec #1 (remove APPLIES TO tag, keep SQL switch) → Task 2 ✓
- Spec #2 (SQL into Custom condition, rename, restore desc) → Task 1 ✓
- Spec #3 (inverse pill color) → Task 3 ✓
- Spec #4 (remove Visual builder tooltip in SQL) → Task 3 ✓
- Spec #5 (remove back button, restore header chip) → Task 2 ✓
- Spec #6 (IF alignment + size) → Task 4 ✓
- Spec #7 (Columns used height) → Task 5 ✓
- Spec #8 (merge help removal + autofill) → Task 6 ✓
- Spec #9 (SQL Advanced: drop Joins, move merge into Advanced) → Task 7 ✓
- Spec #10 (random AI prompt) → Task 8 ✓
- i18n parity → Task 9 ✓

**Placeholder scan:** No TBD/TODO. Every code step shows the code. Grep-then-remove guards are explicit for each orphan-able key.

**Type consistency:** `computeMergeColumnsAutofill(current, slotNames)` used identically in Task 6 test + wiring. `pickAiExampleKey(count, rand?)` + `AI_EXAMPLE_COUNT` consistent across Task 8 test + wiring. `CUSTOM_SURFACE_TAB_CLASS` shape unchanged (string array). Header chip uses the existing `ConditionSelector` `variant="chip"` contract.

**Ordering note:** Task 6 (autofill/help) precedes Task 7 (which relocates the merge field into Advanced) — the autofill effect is state-driven so it survives the relocation. Task 1 changes locale values that Task 3/6/8 also touch; each task commits its own locale edits, and Task 9 does the final parity sweep.
