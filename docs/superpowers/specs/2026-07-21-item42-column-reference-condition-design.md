# Item 42 — Reference a column in a condition value box

**Date:** 2026-07-21
**Status:** Approved for implementation
**Surface:** DQX Studio rule builder (React/TS frontend; no backend change)

## Problem

In the rule builder, a condition like `IF {{column_1}} equals <VALUE>` only lets the
user type a **literal** into the value box. There is no way to compare a column to
**another column** (e.g. `amount >= credit_limit`, `start_date before end_date`). Item
42 adds that ability.

## Goal / behaviour

In any condition **value box that has a typed input**, typing `{` opens an autocomplete
of columns **filtered to the compatible family**; picking one inserts a `{{column}}`
token, rendered as a resolved chip with an ✕ to clear back to a literal. The whole box
is **either** a literal **or** exactly one `{{column}}` — never a mixed expression
(no `{{col}} + 30`, no free-typed SQL).

## Scope

**In scope — low-code Condition Builder** (`ValueCell.tsx`), for every shape with a
typed input:
- `single` — all comparison / equality / temporal-ordering operators
  (`>=`, `<=`, `>`, `<`, `=`, `!=`, `equals`, `not equals`, `before`, `after`,
  `on or before`, `on or after`, `matches regex`… — i.e. anything `rowSql` renders with
  `quote(value)` on the RHS).
- `double` — both bounds of `between` / `length between`.
- `chip` — each entry of `in` / `not in` (per-entry: a chip is a literal OR a column).

**In scope — Build-with-AI**, via the AI service's existing **SQL representation** only
(see "AI path" below). No new AI plumbing beyond slot-derivation from the predicate.

**Out of scope — left entirely untouched:**
- **Native Basic Checks single-function path** (`mode === "dqx_native"`): scalar
  argument boxes (`value`, `limit`, `min_limit`, `max_limit`, …) stay **literal-only**.
  Verification confirmed col-vs-col there needs UI + slot-declaration + serialization +
  validation plumbing. Per decision, this feature is not added to native checks.
- Pure-`Select` value shapes with no text input: `type-picker` (`is a valid <type>`) and
  the valueless operators (`is null`, `is empty`, `is positive`, `is today`, …) — nothing
  to type `{` into, and no operand to compare a column against.
- `interval` shape (`is in last N days`) — the operand is a duration, not a column.

## Why this runs (verified against code)

- **Materializer substitution is position-agnostic.** `materializer.py` `_substitute_text`
  replaces *every* `{{slot}}` token anywhere in the compiled predicate — LHS and RHS
  identically, exactly like join keys. A RHS `{{credit_limit}}` is substituted the same
  as the LHS `{{amount}}`.
- **Low-code with no joins/group-by compiles to `sql_expression`** (`compileLowcodeBody`),
  so `amount >= credit_limit` becomes a `sql_expression` predicate.
- **SQL-safety gate passes.** `render_check` runs `is_sql_query_safe(...)` on the
  substituted predicate before emit; `amount >= credit_limit` is valid, safe SQL.
- **No backend / materializer / DQX-engine change is required** — the entire change is
  frontend (UI affordance + AST value shape + compiler RHS emission), plus the AI
  slot-derivation gap noted below.

## Architecture / components

### 1. AST — discriminated value shape (`ui/lib/lowcodeAst.ts`)
Today `RowAst.value` / `AggregatedRowAst.value` is `unknown` (a bare scalar / array /
`{number,unit}` / type string). Add a **column-reference marker** as an additive shape,
e.g. `{ $col: string }` (the referenced column's name, a declared slot / `LowcodeColumnRef.name`).
- Literals remain **bare scalars** → full backward compatibility; legacy ASTs untouched.
- For `double`, each bound may independently be a scalar or a `{ $col }`.
- For `chip`, the array may hold a mix of literal strings and `{ $col }` entries.
- Provide a small type guard (`isColumnRef(v)`).

### 2. Compiler — emit `ref()` for column-ref values (`ui/lib/lowcodeCompile.ts`)
`rowSql` currently sends the RHS through `quote(value)`. Change: when a value (or a
bound / chip entry) is a `{ $col }`, emit `ref(col)` (→ `{{col}}` for a plain declared
column, or raw `table.col` for a qualified join column) instead of `quote()`. Applies to:
- `single` comparison/equality/temporal ops (the `quote(value)` sites).
- `double` bounds (`BETWEEN ref(lo) AND ref(hi)` mixable with literals).
- `chip` (`IN (...)` — each element `ref()` or `quote()`).
- Ensure the referenced column is registered as a **declared slot** so it round-trips and
  substitutes (the low-code column picker already sources from `declaredColumns`, which
  are persisted as `sqlSlots`).

### 3. UI — the `{` autocomplete affordance (`ui/components/rules/lowcode/ValueCell.tsx` + `LowcodeRow.tsx`)
- Pass `declaredColumns: LowcodeColumnRef[]` (already available in `LowcodeRow`) into
  `ValueCell`.
- In each **text `Input`** (single, double bounds, chip entry): detect a leading/typed
  `{` and open a compatible-column autocomplete (a lightweight popover/command list
  reusing the existing column-picker data + shadcn `Command`), **filtered by family**
  (`valueCellShape`'s family / the row's family). Selecting inserts a column-ref value
  (sets AST `value` to `{ $col }`); the box then renders a **resolved chip** (mono, the
  `{{col}}` label, with an ✕ to revert to an empty literal input).
- Family compatibility: numeric box → numeric columns; text → text; temporal → temporal;
  `ANY` → all. Incompatible columns are **not shown** (strict filter — safer, matches the
  approved mockup).
- Read-only / diff rendering: a column-ref value renders as the mono `{{col}}` chip
  (never an editable input). The frozen-check-JSON diff view already renders the compiled
  `sql_expression` predicate, which now contains `{{col}}` on the RHS — legible as-is.

### 4. AI path — slot-derivation for SQL proposals (`backend/services/ai_rules_service.py`)
The AI SQL pass already emits `{{slot}}`-templated `sql_query`/`sql_expression`
predicates and instructs the model to reference columns as `{{slot}}`. **Gap:** the SQL
branch of `_validate_and_repair_proposal` leaves `slots=[]` (only the native branch
populates slots). Fix: derive slots from the predicate's `{{...}}` tokens for the SQL
pass (the low-code pass already does this — reuse that token-extraction). This lets an
AI-generated `{{amount}} >= {{credit_limit}}` declare its RHS slot and run. No prompt or
output-schema change; this is the one small AI-side change and it is additive.

## Data flow (low-code, end to end)

1. User picks `{{credit_limit}}` in the value box → `RowAst.value = { $col: "credit_limit" }`.
2. `compileAstToSql` → `rowSql` emits `{{amount}} >= {{credit_limit}}`; `credit_limit`
   registered as a declared slot → `sql_expression` predicate saved on the rule.
3. On apply/run, `materializer._substitute_text` replaces both `{{amount}}` and
   `{{credit_limit}}` with the mapped real columns → `amount >= credit_limit`.
4. `is_sql_query_safe` passes → `sql_expression` check runs in the DQX engine.

## Security

- RHS columns come **only from the picker** (declared slots) — never free-typed into SQL /
  `F.expr()`. A typed token that does not resolve to a known compatible column stays a
  literal.
- The compiled predicate stays inside the existing `is_sql_query_safe` envelope
  (unchanged gate). No new SQL-injection surface.

## Edge cases / risks

- **Family/type mismatch:** picker shows only same-family columns; user cannot build a
  temporal-vs-numeric comparison.
- **Round-trip on load/edit:** the discriminated `{ $col }` value must survive
  `parseDqxCheckJson`'s low-code branch (it carries `lowcode_ast` forward). Add
  serialization/round-trip coverage.
- **Existing rules never break:** literals stay bare scalars; the column marker is strictly
  additive.
- **`double`/`chip` mixing:** a bound/entry may be literal while its sibling is a column.
- **Cross-table:** a qualified `table.col` (join column) emits raw via `ref()` and runs
  when a join is declared — comes for free, no extra work.
- **i18n:** new UI strings (autocomplete header "Columns", "clear column" aria-label,
  literal/column affordance labels) added to all four locales (`en`, `pt-BR`, `it`, `es`),
  `en` as source of truth.

## Testing

- **Unit (bun) — `lowcodeCompile`:** RHS column-ref emits `ref()` not `quote()` for each
  operator family; `double` with one/both column bounds; `chip` with mixed entries;
  literal path unchanged (regression). Extend `lowcodeCompile.test.ts`.
- **Unit — AST round-trip:** `{ $col }` value serializes and re-parses; legacy literal
  ASTs unaffected.
- **Unit — AI slot-derivation:** a SQL proposal with `{{col}}` on the RHS derives the slot
  (backend pytest around `ai_rules_service` SQL-branch slot population).
- **Manual/visual QA:** the `{` autocomplete → chip interaction in the Condition Builder;
  family filtering; clear-to-literal; read-only/diff rendering; a saved col-vs-col rule
  actually runs (dry-run) and reports.

## Gates

`bun tsc -b` clean; `bun test` (frontend) green; backend `pytest` for the AI slot change;
i18n key parity across all four locales. Commit + deploy to fe-sandbox. **Do not
squash-merge or push** (per instruction).
