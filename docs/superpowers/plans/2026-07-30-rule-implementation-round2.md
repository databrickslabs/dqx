# Rule Implementation-Tab Round-2 Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct and extend the round-1 rule Implementation-tab work per user feedback: remove the erroneous corner RULE TYPE chip, restore the SQL-mode back-arrow, delete the SqlImportNotice banner, equalize Columns-used row height, and drop extra columns when switching to a built-in check.

**Architecture:** Frontend-only edits to `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` and the four i18n locale files. One pure helper (`mergeCarriedSlotsIntoSignature`) changes with a real bun unit test.

**Tech Stack:** React 19, TypeScript (strict), Tailwind CSS 4, react-i18next, bun test runner.

## Global Constraints

- Work in `app/`. Type-check with `make app-check` (from repo root) — runs tsc + basedpyright + bun UI tests. tsc must PASS with `noUnusedLocals`.
- The 7 basedpyright errors in `backend/services/apply_rules_service.py` and `monitored_table_service.py` are PRE-EXISTING and unrelated (this branch touches no Python) — not a gate failure for these tasks. Confirm your diff is TS/JSON-only.
- Every user-facing string uses `t()`. Never hard-code display text.
- Add/change every i18n key in ALL FOUR locales (`en.json` source of truth, `pt-BR.json`, `it.json`, `es.json`) with translated values. `en` key set must equal every other locale's key set. Never remove a key with a remaining consumer — grep first.
- Do NOT touch: low-code Joins, the granularity switch, the authoring-surface (visual builder/SQL) pill, or the Build-with-AI prompt (all round-1, must stay working).
- Import alias `@/` → `app/src/databricks_labs_dqx_app/ui/`.
- All paths relative to repo root `/Users/oliver.gordon/Documents/Code/Other/dqx`. Line numbers are approximate — grep the quoted anchor strings.
- Primary file: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`.

---

### Task 1: Remove the corner "RULE TYPE" chip; restore the SQL-mode back-arrow

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`

**Interfaces:**
- Consumes: `conditionChosen`, `readOnly`, `mode`, `setDecisionPointChosen`, `t`, `ArrowLeft` (already imported), `SqlAiAssistMenu`, the existing SQL Condition branch.
- Produces: no new symbols.

- [ ] **Step 1: Delete the corner RULE TYPE chip block**

In the Condition `SectionHeader action={...}` (~3925–3945), delete the entire chip block:
```tsx
{conditionChosen && !readOnly && (
  <div className="flex items-center gap-2">
    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
      {t("rulesRegistry.ruleTypeLabel")}
    </span>
    <div className="max-w-[20rem]">
      <ConditionSelector … variant="chip" … />
    </div>
  </div>
)}
```
Leave the authoring-surface pill block (`{mode !== "dqx_native" && (…)}`) and the granularity block that follow it in the same `action` untouched.

- [ ] **Step 2: Confirm `currentTypeLabel` is still used (keep it)**

Run: `grep -n "currentTypeLabel" app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`
Expected: still referenced by the native anchor row (`currentLabel={decisionPointChosen ? currentTypeLabel : undefined}`, ~4049). Keep the `currentTypeLabel` definition (~3491). Do NOT remove it. If tsc later flags `ruleTypeLabel` i18n key as unused, that's fine — i18n keys aren't tracked by tsc; leave `ruleTypeLabel` in the locales (it may be reused).

- [ ] **Step 3: Add the SQL-mode back-arrow next to "IF"**

In the SQL Condition branch (`decisionPointChosen && mode === "sql"`, the IF row that renders the "IF" span + `SqlAiAssistMenu`, ~4148–4155), replace the IF-cell div so a back-arrow precedes the "IF" span:
```tsx
<div className="flex h-8 items-center gap-2">
  {!readOnly && (
    <button
      type="button"
      onClick={() => setDecisionPointChosen(false)}
      aria-label={t("rulesRegistry.changeRuleTypeHeader")}
      title={t("rulesRegistry.changeRuleTypeHeader")}
      className="text-muted-foreground hover:text-foreground -ml-1 p-0.5 rounded"
    >
      <ArrowLeft className="h-3.5 w-3.5" />
    </button>
  )}
  <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
    {t("rulesRegistry.ifCondition")}
  </span>
</div>
```
Keep the round-1 IF font (`text-[11px] tracking-[0.08em]`). Do NOT add this arrow to low-code or native mode.

- [ ] **Step 4: Type-check**

Run: `make app-check`
Expected: tsc PASS; diff is TS-only. (7 pre-existing basedpyright errors ignored.)

- [ ] **Step 5: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx
git commit -m "fix(app): remove corner RULE TYPE chip, restore SQL-mode change-type back-arrow"
```

---

### Task 2: Remove the SqlImportNotice banner

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json`

**Interfaces:**
- Consumes: nothing new.
- Produces: removes `SqlImportNotice`, `sqlImportNotice` state, `setSqlImportNotice`.

- [ ] **Step 1: Delete the render site**

Delete (~4062–4064):
```tsx
{decisionPointChosen && mode === "lowcode" && sqlImportNotice && (
  <SqlImportNotice notice={sqlImportNotice} onDismiss={() => setSqlImportNotice(null)} />
)}
```

- [ ] **Step 2: Delete the state and all setter calls**

- Delete the state (~1996): `const [sqlImportNotice, setSqlImportNotice] = useState<{ mapped: number; unmapped: string[] } | null>(null);`
- Grep every `setSqlImportNotice`: `grep -n "setSqlImportNotice" app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx`. Delete each call:
  - ~3636 `if (!(mode === "sql" && next === "lowcode")) setSqlImportNotice(null);` — delete this line (it only cleared the notice).
  - ~3690 `setSqlImportNotice(null);` — delete.
  - ~3703 `setSqlImportNotice({ mapped: imported.rows.length, unmapped: imported.unmapped });` — delete. The surrounding `else` branch still performs the import (`setLowcodeAst({ rows, joins: [] })` etc.); only the announcement is removed. If `imported.unmapped` / `imported.rows` become unused after this, verify the rest of the branch still uses `imported.rows` (it does — `const rows = imported.rows.length > 0 ? … `), so no orphan.

- [ ] **Step 3: Delete the component definition**

Delete the `function SqlImportNotice({ … }) { … }` definition (~236–275). Note any icon/import it alone used; if tsc `noUnusedLocals` flags an import afterward, remove it — but grep shared icons (e.g. `AlertCircle`, `X`) first; they are used elsewhere and must stay.

- [ ] **Step 4: Remove orphaned i18n keys**

For each of `sqlImportBestEffort`, `sqlImportPartial`, `sqlImportOverwriteHint`:
`grep -rn "<key>" app/src/databricks_labs_dqx_app/ui`
If no consumer remains, remove it from all four locales. Keep any that still has a consumer.

- [ ] **Step 5: Type-check**

Run: `make app-check`
Expected: tsc PASS. Fix any unused-symbol errors introduced by the removal.

- [ ] **Step 6: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): remove SqlImportNotice best-effort banner"
```

---

### Task 3: Equalize "Columns used" row height across modes

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (`SlotsPanel`, ~1490–1650)

**Interfaces:**
- Consumes: `SlotsPanel` slot-row render.
- Produces: no new symbols.

- [ ] **Step 1: Diagnose the remaining height delta**

Read the slot-row render (~1566–1650). Round 1 set both the family `Badge` container and the `Select` trigger to `h-6`, but rows still differ across modes. Determine the remaining source. Likely candidates, confirm by reading:
- the left cell `flex flex-wrap items-center gap-2 min-w-0` (~1581) — the `ChevronDown` disclosure chevron renders only when `!disabled`, and `SlotTagRegion` height may vary.
- the row grid `grid grid-cols-[1fr_auto_auto] items-center gap-3 px-3 py-2` (~1580) has no min-height, so its height is content-driven and varies with which of the above render.

- [ ] **Step 2: Apply a shared minimum row height**

Give the row grid container a fixed minimum height so every mode's single-line row is identical. Change (~1580):
```tsx
<div className="grid grid-cols-[1fr_auto_auto] items-center gap-3 px-3 py-2 min-h-[3.25rem]">
```
Adjust the exact `min-h-[…]` value to match the tallest normal single-line row (the mode that currently renders tallest — measure by reading which elements it includes: chevron + code + tag region + h-6 control). `items-center` keeps all cells vertically centered. This removes the baseline delta; a row whose tags wrap to a second line may still grow (acceptable).

- [ ] **Step 3: Type-check + reason about equality**

Run: `make app-check`
Expected: tsc PASS.
In the report, state which element varied and confirm the `min-h` now dominates the single-line height in every mode (native Badge row, low-code/SQL Select row, disabled/read-only row) so there is no per-mode shift.

- [ ] **Step 4: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx
git commit -m "fix(app): give Columns used rows a consistent baseline height"
```

---

### Task 4: Drop extra columns on switch to a built-in check; update warning copy

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (`mergeCarriedSlotsIntoSignature`, ~1260–1280)
- Create: `app/src/databricks_labs_dqx_app/ui/components/mergeCarriedSlots.test.ts` (or colocated test — see Step 1)
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json` (modeSwitch body copy)

**Interfaces:**
- Consumes: `mergeCarriedSlotsIntoSignature(signature: RuleSlot[], carried: RuleSlot[]): RuleSlot[]` (~1260); its sole caller at ~3760 (`setNativeSlots(mergeCarriedSlotsIntoSignature(sigSlots, sqlSlots))`).
- Produces: `mergeCarriedSlotsIntoSignature` no longer appends beyond signature arity. It must be exported for the unit test (currently module-private).

- [ ] **Step 1: Make the helper testable + write the failing test**

`mergeCarriedSlotsIntoSignature` is a module-level function in `RegistryRuleFormDialog.tsx`. To unit-test it without importing the whole dialog, the cleanest move is to **extract it to a small lib module**: create `app/src/databricks_labs_dqx_app/ui/lib/slotCarry.ts` exporting `mergeCarriedSlotsIntoSignature` (and move its dependency types/imports — it uses `RuleSlot`), then import it back into `RegistryRuleFormDialog.tsx`. If extraction pulls in too many dialog-local dependencies, instead just `export` the function in place and import it from the test with a relative path. Prefer extraction to `lib/slotCarry.ts` if `RuleSlot` is importable there.

Write `app/src/databricks_labs_dqx_app/ui/lib/slotCarry.test.ts`:
```ts
import { describe, expect, test } from "bun:test";
import { mergeCarriedSlotsIntoSignature } from "./slotCarry";
import type { RuleSlot } from "@/…"; // import RuleSlot from wherever it's defined

const slot = (name: string, family = "any"): RuleSlot =>
  ({ name, family, position: 0 } as RuleSlot);

describe("mergeCarriedSlotsIntoSignature", () => {
  test("maps carried names onto signature positions", () => {
    const sig = [slot("column_1"), slot("column_2")];
    const carried = [slot("id"), slot("region")];
    const out = mergeCarriedSlotsIntoSignature(sig, carried);
    expect(out.map((s) => s.name)).toEqual(["id", "region"]);
  });
  test("does NOT append carried slots beyond signature arity", () => {
    const sig = [slot("column_1")]; // check takes 1 column
    const carried = [slot("id"), slot("extra_a"), slot("extra_b")];
    const out = mergeCarriedSlotsIntoSignature(sig, carried);
    expect(out).toHaveLength(1);
    expect(out[0].name).toBe("id");
  });
  test("returns signature unchanged when carried is empty", () => {
    const sig = [slot("column_1")];
    expect(mergeCarriedSlotsIntoSignature(sig, [])).toEqual(sig);
  });
});
```
Adjust the `RuleSlot` import path and the minimal shape to satisfy the type (fill required fields). Grep the `RuleSlot` type definition first to get its required fields.

- [ ] **Step 2: Run the test — verify it fails**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/slotCarry.test.ts`
Expected: the "does NOT append" test FAILS (current code appends), or module-not-found if you haven't created `slotCarry.ts` yet. Capture output for RED evidence.

- [ ] **Step 3: Change the helper to not append beyond arity**

Remove the append loop (~1274–1278):
```tsx
// Append any extra carried slots that exceed the signature arity. These
// become filter-only extras from the user's perspective (consistent with B3).
for (let i = signature.length; i < carried.length; i++) {
  merged.push({ ...carried[i], position: i, arg_key: undefined });
}
```
So the function maps carried slots onto the signature's own positions only and returns `merged` (length == signature length). Update the docstring: remove the "Append any extra carried slots… filter-only extras" paragraph and state that extras beyond the check's arity are intentionally dropped when switching to a built-in check.

- [ ] **Step 4: Run the test — verify it passes**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/slotCarry.test.ts`
Expected: PASS (all three). Capture GREEN output.

- [ ] **Step 5: Update the mode-switch warning copy (all four locales)**

In `en.json` (`rulesRegistry.modeSwitch.body`):
```json
"LOWCODE_TO_NATIVE": "The conditions you built will be removed, along with any columns the check doesn't use.",
"SQL_TO_NATIVE": "The SQL you wrote will be removed, along with any columns the check doesn't use.",
```
(Leave `NATIVE_TO_LOWCODE` / `NATIVE_TO_SQL` unchanged.) Translate the two updated values for pt-BR/it/es. Example pt-BR:
```json
"LOWCODE_TO_NATIVE": "As condições que você criou serão removidas, junto com as colunas que a verificação não usa.",
"SQL_TO_NATIVE": "O SQL que você escreveu será removido, junto com as colunas que a verificação não usa.",
```
(Use correct it/es translations similarly.)

- [ ] **Step 6: Full check**

Run: `make app-check`
Expected: tsc PASS + new slotCarry test PASS + all other bun tests PASS. Diff is TS/JSON-only.

- [ ] **Step 7: Commit**

```bash
git add app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx app/src/databricks_labs_dqx_app/ui/lib/slotCarry.ts app/src/databricks_labs_dqx_app/ui/lib/slotCarry.test.ts app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "feat(app): drop columns a built-in check doesn't use on switch, update warning"
```

---

### Task 5: Final verification — locale parity + full check

**Files:** none (verification only)

- [ ] **Step 1: Locale key parity (recursive, all blocks)**

```bash
cd app/src/databricks_labs_dqx_app/ui/lib/i18n/locales
for f in pt-BR it es; do
  echo "== en vs $f =="
  diff <(python3 -c "import json
def keys(d,p=''):
 o=[]
 for k,v in d.items():
  kp=p+'.'+k if p else k
  o.append(kp)
  if isinstance(v,dict): o+=keys(v,kp)
 return o
print('\n'.join(sorted(keys(json.load(open('en.json'))))))") \
       <(python3 -c "import json
def keys(d,p=''):
 o=[]
 for k,v in d.items():
  kp=p+'.'+k if p else k
  o.append(kp)
  if isinstance(v,dict): o+=keys(v,kp)
 return o
print('\n'.join(sorted(keys(json.load(open('$f.json'))))))")
done
```
Expected: no diff output. Fix any drift.

- [ ] **Step 2: Full app check**

Run: `make app-check`
Expected: tsc PASS, all bun UI tests PASS. (7 pre-existing basedpyright errors unrelated.)

- [ ] **Step 3: Commit any parity fixes** (skip if none)

```bash
git add app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/*.json
git commit -m "chore(app): sync i18n locale keys for round-2 fixes"
```

---

## Self-Review

**Spec coverage:**
- Spec #1 (remove corner chip) → Task 1 ✓
- Spec #2 (restore SQL back-arrow) → Task 1 ✓
- Spec #3 (remove SqlImportNotice) → Task 2 ✓
- Spec #4 (Columns-used height) → Task 3 ✓
- Spec #5 (drop extra columns + warning) → Task 4 ✓
- i18n parity → Task 5 ✓

**Placeholder scan:** No TBD/TODO. Code shown for every code step; grep-then-remove guards explicit for orphan-able keys/imports.

**Type consistency:** `mergeCarriedSlotsIntoSignature(signature, carried)` signature unchanged (only its body changes); test imports it from the same module it's exported from. The SQL back-arrow uses existing `setDecisionPointChosen`, `ArrowLeft`, `changeRuleTypeHeader`.

**Ordering note:** Task 1 removes the chip AND restores the back-arrow together (both are the single "how do I change rule type" concern — a reviewer would reject one without the other). Tasks 2–4 are independent. Task 5 sweeps parity last.
