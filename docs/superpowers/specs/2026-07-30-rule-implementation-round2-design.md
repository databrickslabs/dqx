# Rule Implementation-Tab Changes — Round 2 (feedback fixes) — Design

**Date:** 2026-07-30
**Branch:** `dqx-studio/rule-implementation-changes`
**Scope:** Frontend-only. Corrects/extends the round-1 work in
`RegistryRuleFormDialog.tsx` (plus its i18n locales) based on user feedback.
No backend changes.

## Context

Round 1 shipped a set of Implementation-tab changes. User feedback flagged five
issues, some of which correct round-1 mistakes:

1. Round 1 removed the standalone "← Change rule type" button but **wrongly added a
   corner "RULE TYPE" dropdown chip** on the Condition header. The user never asked
   for that chip. Remove it.
2. In an earlier version (`a58b5a65`), **SQL mode had a small `←` back-arrow next to
   "IF"** (above the editor) that reopened the rule-type picker. It disappeared.
   Restore it — SQL mode has no inline picker row (its body is just the editor), so
   this is the only in-place way to change type from SQL.
3. Round 1 (and prior) shows a `SqlImportNotice` banner ("Read back from your SQL,
   best effort — N condition(s) mapped… / Your SQL is kept as-is until you edit a
   condition here…"). The user wants it gone entirely.
4. The "Columns used" panel rows still render at **slightly different heights across
   rule types** — round-1 Task 5 was incomplete. Make every row/mode consistent.
5. Switching from SQL/Custom-condition to a DQX built-in check **keeps extra
   "Columns used" slots** (columns beyond the check's arity). Drop those extras on
   that transition, and update the mode-switch warning copy to say the columns will
   be removed.

Picker reachability per mode (verified): **native** has the inline `IF [col]
[picker]` row; **low-code**'s first builder row renders a `ConditionSelector` (opens
to operators view, whose back button reaches "Change rule type"); **SQL** has no
picker element at all. So the change-type gap is SQL-only.

## Changes

### 1. Remove the corner "RULE TYPE" chip

`RegistryRuleFormDialog.tsx` ~3927–3945: the Condition `SectionHeader` `action`
renders a `{conditionChosen && !readOnly && (<div>… RULE TYPE … <ConditionSelector
variant="chip" …/></div>)}` block. **Delete this entire block.** The authoring-surface
pill (visual builder / SQL) and the granularity switch that follow it in the same
`action` stay.

`currentTypeLabel` (~3491) may become unused after this deletion AND the round-1
anchor row still uses it (~4049 `currentLabel={decisionPointChosen ? currentTypeLabel
: undefined}`). Grep before removing — it is still referenced by the native anchor
row, so keep it.

### 2. Restore the SQL-mode `←` back-arrow next to "IF"

In the SQL Condition branch (`RegistryRuleFormDialog.tsx` ~4148 region, the
`decisionPointChosen && mode === "sql"` block, the IF row that currently renders just
the "IF" `FramingWord`-style span + `SqlAiAssistMenu`), add a small back-arrow button
immediately before the "IF" span, mirroring `a58b5a65`:

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

Notes:
- Keep the round-1 IF font styling (`text-[11px] tracking-[0.08em]`) — do not
  regress to `text-[10px] tracking-wider`.
- `setDecisionPointChosen(false)` reopens the anchor `IF [col] [picker]` row (the
  `!decisionPointChosen` branch at ~4004), which is the full rule-type picker. This
  is the same mechanism `a58b5a65` used.
- `ArrowLeft` is already imported and used elsewhere; no import change needed.
- Scope: **SQL mode only.** Do NOT add this to low-code (its first row's
  ConditionSelector already reaches change-type) or native (its anchor row is the
  picker).

### 3. Remove the `SqlImportNotice` banner

- Delete the `SqlImportNotice` component definition (~236–275).
- Delete its render site (~4062–4064, `{decisionPointChosen && mode === "lowcode" &&
  sqlImportNotice && (<SqlImportNotice … />)}`).
- Delete the `sqlImportNotice` state (`const [sqlImportNotice, setSqlImportNotice] =
  useState…`, ~1996) and every `setSqlImportNotice(...)` call (grep — ~3636, ~3690,
  ~3703). Removing the calls must not break the surrounding logic: at ~3703 the call
  is the only statement reporting the SQL→low-code import result; dropping it is fine
  (the import still happens; it just isn't announced). At ~3636 and ~3690 the calls
  clear the notice; drop them.
- Remove now-orphaned i18n keys after grep: `sqlImportBestEffort`, `sqlImportPartial`,
  `sqlImportOverwriteHint` (all four locales). Grep each first; remove only if no
  remaining consumer. `en` key set must equal every other locale's key set.
- If removing `SqlImportNotice` orphans any import it alone used (e.g. an icon), let
  tsc's `noUnusedLocals` flag it and remove it — but grep shared icons first.

### 4. "Columns used" panel — consistent row height across all modes

Round-1 Task 5 shrank the type `Select` to `h-6` to match the native `Badge`, but
rows still differ by mode. Re-diagnose the real remaining source of the delta in
`SlotsPanel` (~1490–1650). Candidate causes to check:
- The `SlotTagRegion` renders in the row's left cell for all modes but may occupy
  different height when a slot has vs. lacks tags, or when `disabled`.
- The left cell uses `flex flex-wrap` (~1581) — wrapping tags can add a line.
- The type control cell differs: `disabled || lockFamily` → `Badge` (round-1 gave it
  `h-6`); else → `h-6` `Select`. Confirm both are truly equal after round 1.
- The `ChevronDown` disclosure affordance renders only when `!disabled` (~1584).

The fix: give the slot row a **single fixed minimum row height** that every mode
shares, so the panel baseline never shifts when the rule type changes. Concretely,
set a consistent `min-h` on the row's grid container (`grid grid-cols-[1fr_auto_auto]
items-center gap-3 px-3 py-2`, ~1580) — e.g. `min-h-[3.25rem]` (or whatever matches
the tallest normal single-line row) — and ensure the left, type, and remove cells all
vertically center within it. Verify by cycling native ⇄ low-code ⇄ SQL on a fresh
rule: no vertical shift, all rows equal height. Content growth (tags wrapping to a
second line) may still grow a specific row — that is acceptable; the requirement is
that the *baseline* single-line row height is identical across modes.

Report the precise element whose height varied and how the fix equalizes it.

### 5. Drop extra "Columns used" when switching to a built-in check; update warning

`mergeCarriedSlotsIntoSignature(signature, carried)` (~1260–1280) currently
**appends** carried slots beyond the check's signature arity (the `for (let i =
signature.length; i < carried.length; i++)` loop, ~1274–1278) as "filter-only
extras". When switching SQL/Custom → a native check, the user wants those extras
**dropped**, so the native check shows only the columns its signature defines
(populated from the carried names where positions overlap).

- Change the behaviour so switching to a native check does **not** append extra
  carried slots. Options:
  - Simplest: remove the append loop from `mergeCarriedSlotsIntoSignature` so it only
    maps carried slots onto the signature's own positions (names/families carried
    where compatible) and never grows past the signature arity.
  - Verify no other caller depends on the append behaviour — `grep
    mergeCarriedSlotsIntoSignature`; it is used only at ~3760 for the native switch,
    so removing the append is safe. Update the function's docstring (the lines
    describing "Append any extra carried slots… filter-only extras") to match.
- The transition already runs through the `ModeSwitchDialog` guard (SQL_TO_NATIVE /
  LOWCODE_TO_NATIVE). Update the warning copy so it tells the author their extra
  columns will be removed. Current strings (`i18n rulesRegistry.modeSwitch.body`):
  - `SQL_TO_NATIVE`: "The SQL you wrote will be removed."
  - `LOWCODE_TO_NATIVE`: "The conditions you built will be removed."
  Update both (all four locales) to also mention that columns not used by the chosen
  check will be removed, e.g.:
  - `SQL_TO_NATIVE`: "The SQL you wrote will be removed, along with any columns the
    check doesn't use."
  - `LOWCODE_TO_NATIVE`: "The conditions you built will be removed, along with any
    columns the check doesn't use."
  Keep the wording tight (the user dislikes verbose copy). Translate for pt-BR/it/es.

## i18n

- Remove (if orphaned, grep first): `sqlImportBestEffort`, `sqlImportPartial`,
  `sqlImportOverwriteHint` — all four locales.
- Update `modeSwitch.body.SQL_TO_NATIVE` and `modeSwitch.body.LOWCODE_TO_NATIVE` —
  all four locales, translated.
- No new keys expected (reuse `changeRuleTypeHeader` for the SQL back-arrow
  aria/title).
- Final parity check: `en` key set == pt-BR == it == es (recursive, all blocks).

## Testing & verification

- `make app-check` (tsc + basedpyright + bun UI tests) — tsc must PASS with
  `noUnusedLocals`; the 7 pre-existing basedpyright errors in backend Python files are
  unrelated (this branch never touches them). All existing bun UI tests pass.
- If a pure helper changes testably (`mergeCarriedSlotsIntoSignature` — it is a
  module-level pure function), add/adjust a bun unit test asserting it no longer
  appends beyond signature arity. Check whether it is exported/testable; if it is
  not currently exported, exporting it for test is acceptable (small, justified).
- Manual/visual verification at deploy:
  1. No corner "RULE TYPE" chip on the Condition header.
  2. SQL mode shows a `←` next to IF that reopens the rule-type picker.
  3. No "Read back from your SQL…" banner anywhere.
  4. "Columns used" rows are equal height; no shift when changing rule type.
  5. Switching SQL/Custom → a built-in check drops columns the check doesn't use;
     the confirm dialog says so.
- Locale parity holds.

## Out of scope

- No backend/API/DQX-library changes.
- No changes to low-code Joins, the granularity switch, the authoring-surface pill,
  or the Build-with-AI prompt (all round-1, unaffected).
