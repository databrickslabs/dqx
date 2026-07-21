# Item 42 — Column Reference in Condition Value Boxes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user reference another column (instead of a literal) in the low-code Condition Builder value boxes — type `{` to pick a compatible-family column, inserting a `{{column}}` token — and let Build-with-AI express the same, all running through the existing `sql_expression` path.

**Architecture:** Frontend-only for the manual builder (AST value shape + compiler emission + `ValueCell` autocomplete UI), plus one small additive backend fix so the AI SQL pass declares its predicate's slots. No materializer / DQX-engine / backend-route change — a RHS `{{col}}` is substituted position-agnostically by the existing materializer and passes `is_sql_query_safe`.

**Tech Stack:** React 19 + TypeScript, shadcn/ui (`Command`, `Popover`), TanStack; `bun tsc -b` + `bun test` (frontend); Python/FastAPI + pytest (the one AI change); react-i18next (4 locales).

## Global Constraints

- The whole value box is EITHER a literal OR exactly one `{{column}}` reference — never a mixed expression. No free-typed SQL reaches the value.
- Column references come ONLY from the picker (a declared column); a typed token that does not resolve to a known compatible column stays a literal.
- Family filter is STRICT: the autocomplete shows only same-family columns (numeric box → numeric; text → text; temporal → temporal; `ANY` → all). Incompatible columns are not shown.
- Backend / materializer / DQX-engine: NO changes except Task 5 (AI SQL-pass slot derivation, additive).
- Native `dqx_native` Basic Checks scalar arg boxes are OUT OF SCOPE — do not modify that authoring surface. Pure-`Select` shapes (`type-picker`, valueless operators) and the `interval` shape have no typed input and get no affordance.
- Every new user-facing string uses `t()` and is added to ALL FOUR locales (`en`, `pt-BR`, `it`, `es`); `en` is the source of truth.
- Literal path must remain byte-identical (regression): existing rules with scalar values compile exactly as before.
- Gates before commit: `bun tsc -b` clean; `bun test` green; backend `pytest` for Task 5. `git checkout app/uv.lock uv.lock` before staging. Commit messages end with `Co-authored-by: Isaac <isaac@example.com>`. Do NOT squash-merge or push.
- Worktree root: `/Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4`. All paths below are relative to it. Frontend commands run from `app/`.

---

## File Structure

- `app/src/databricks_labs_dqx_app/ui/lib/lowcodeAst.ts` — add the `ColumnRefValue` type + `isColumnRef` guard (the AST value shape).
- `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts` — `rowSql` / `compileRow` emit `ref()` for a column-ref value across `single` / `double` / `chip`; `between`/`in` bounds may mix literal + column.
- `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts` — new compiler cases (Task 2 & 3).
- `app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/ValueCell.tsx` — the `{` autocomplete + resolved-chip render; accept `declaredColumns` + `family`.
- `app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/LowcodeRow.tsx` — pass `declaredColumns` + row `family` into `ValueCell`.
- `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/{en,pt-BR,it,es}.json` — new UI strings.
- `app/src/databricks_labs_dqx_app/backend/services/ai_rules_service.py` — derive slots on the `sql` branch of `_validate_and_repair_proposal`.
- `app/tests/test_ai_rules_service.py` (or the existing AI-service test module) — Task 5 test.

---

## Task 1: AST column-reference value shape

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeAst.ts`

**Interfaces:**
- Produces: `type ColumnRefValue = { $col: string }`; `function isColumnRef(v: unknown): v is ColumnRefValue`. Consumed by `lowcodeCompile.ts` (Tasks 2-3) and `ValueCell.tsx` (Task 4).

- [ ] **Step 1: Add the type + guard**

Append to `lowcodeAst.ts` (after `AnyRow`, before `JoinKeyAst`):

```ts
/**
 * A condition value that references another COLUMN instead of holding a
 * literal (item 42). `$col` is the referenced column's name — a declared slot
 * (plain name → `{{name}}` at compile time) or a joined-table column
 * (`table.col`, emitted raw). A value box holds EITHER a literal (bare scalar /
 * array / object, as before) OR one of these — never a mix. Additive: literal
 * values stay bare scalars, so existing ASTs are unchanged.
 */
export interface ColumnRefValue {
  $col: string;
}

/** True when a condition value (or a `between` bound / `in` entry) is a column
 *  reference rather than a literal. */
export function isColumnRef(v: unknown): v is ColumnRefValue {
  return (
    typeof v === "object" &&
    v !== null &&
    typeof (v as Record<string, unknown>).$col === "string" &&
    (v as ColumnRefValue).$col.length > 0
  );
}
```

- [ ] **Step 2: Type-check**

Run: `cd app && bun tsc -b`
Expected: no errors (new exports, nothing consumes them yet).

- [ ] **Step 3: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/ui/lib/lowcodeAst.ts
git commit -m "feat(item42): add ColumnRefValue AST shape + isColumnRef guard

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 2: Compiler — emit `ref()` for a column-ref value (single-value operators)

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts`
- Test: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`

**Interfaces:**
- Consumes: `isColumnRef` from `lowcodeAst.ts` (Task 1); existing `ref()` (module-local).
- Produces: a `valueSql(value)` helper — `ref(value.$col)` when `isColumnRef(value)`, else `quote(value)`. Used by all single-value operator branches in `rowSql`, and by Task 3 for `between`/`in`.

- [ ] **Step 1: Write the failing tests**

Add to `lowcodeCompile.test.ts` inside `describe("operator SQL", ...)`:

```ts
import { isColumnRef } from "./lowcodeAst"; // (import already covers lowcodeAst types)

test("item42: comparison RHS column-ref emits {{col}} not a quoted literal", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: { $col: "credit_limit" } })])),
  ).toBe("{{amount}} >= {{credit_limit}}");
});

test("item42: equals with a column-ref RHS compiles to = {{col}}", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "a", operator: "equals", value: { $col: "b" } })])),
  ).toBe("{{a}} = {{b}}");
});

test("item42: temporal 'before' with a column-ref RHS compiles to < {{col}}", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "start_date", operator: "before", value: { $col: "end_date" } })])),
  ).toBe("{{start_date}} < {{end_date}}");
});

test("item42: a qualified (join) column RHS emits raw table.col", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: { $col: "orders.total" } })])),
  ).toBe("{{amount}} >= orders.total");
});

test("item42: literal RHS is unchanged (regression)", () => {
  expect(compileAstToSql(ast([row({ column_ref: "amount", operator: ">=", value: 100 })]))).toBe(
    "{{amount}} >= 100",
  );
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`
Expected: the 4 new `item42` non-literal cases FAIL (RHS renders as `'[object Object]'` via `quote`); the regression case passes.

- [ ] **Step 3: Add the `valueSql` helper + use it in single-value branches**

In `lowcodeCompile.ts`, add after `quote()` (and import the guard at top: `import { isColumnRef, type AnyRow, type JoinAst, type LowcodeAstV2 } from "./lowcodeAst";` — merge with the existing `lowcodeAst` import):

```ts
// A comparison RHS is EITHER a column reference (item 42 — emit ref(), so a
// plain name becomes {{name}} and a joined-table col emits raw) OR a literal
// (quote() as before). This is the only place the literal-vs-column decision
// is made for scalar operands.
function valueSql(value: unknown): string {
  return isColumnRef(value) ? ref(value.$col) : quote(value);
}
```

Then in `rowSql`, replace `quote(value)` with `valueSql(value)` for the SCALAR-operand operators ONLY (leave `contains`/`starts with`/`ends with`/`is a multiple of`/regex/length/`is a valid` etc. as `quote()` — those are literal-only patterns, and column refs are not offered for them by the UI). Specifically change these lines:

```ts
  if (["=", "!=", "<", "<=", ">", ">="].includes(op)) return `${left} ${op} ${valueSql(value)}`;
  if (op === "equals") return `${left} = ${valueSql(value)}`;
  if (op === "not equals") return `${left} != ${valueSql(value)}`;
  ...
  if (op === "before") return `${left} < ${valueSql(value)}`;
  if (op === "after") return `${left} > ${valueSql(value)}`;
  if (op === "on or before") return `${left} <= ${valueSql(value)}`;
  if (op === "on or after") return `${left} >= ${valueSql(value)}`;
```

(Do NOT change `matches regex`, `has length`, `is longer than`, `is shorter than`, `is a multiple of`, or any LIKE/pattern branch — those remain `quote()`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`
Expected: PASS (all new + existing).

- [ ] **Step 5: Type-check**

Run: `cd app && bun tsc -b`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts
git commit -m "feat(item42): compile single-value comparison RHS column refs to {{col}}

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 3: Compiler — column refs in `between` bounds and `in` entries

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts`
- Test: `app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`

**Interfaces:**
- Consumes: `valueSql` (Task 2).
- Produces: `between` / `length between` bounds and `in` / `not in` entries each independently literal-or-column.

- [ ] **Step 1: Write the failing tests**

Add to `lowcodeCompile.test.ts`:

```ts
test("item42: between with a column upper bound mixes literal + {{col}}", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "x", operator: "between", value: [0, { $col: "cap" }] })])),
  ).toBe("{{x}} BETWEEN 0 AND {{cap}}");
});

test("item42: 'in' with a column entry emits {{col}} alongside literals", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "code", operator: "in", value: ["A", { $col: "fallback_code" }] })])),
  ).toBe("{{code}} IN ('A', {{fallback_code}})");
});

test("item42: length between literal bounds unchanged (regression)", () => {
  expect(
    compileAstToSql(ast([row({ column_ref: "name", operator: "length between", value: [1, 10] })])),
  ).toBe("length({{name}}) BETWEEN 1 AND 10");
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`
Expected: the two new non-literal cases FAIL; regression passes.

- [ ] **Step 3: Use `valueSql` in the `between` / `length between` / `in` / `not in` branches**

In `rowSql`, change:

```ts
  if (op === "between") {
    const [lo, hi] = Array.isArray(value) ? (value as unknown[]) : [null, null];
    return `${left} BETWEEN ${valueSql(lo)} AND ${valueSql(hi)}`;
  }
  if (op === "in") return `${left} IN (${((value as unknown[]) ?? []).map(valueSql).join(", ")})`;
  if (op === "not in") return `${left} NOT IN (${((value as unknown[]) ?? []).map(valueSql).join(", ")})`;
```

And in the `length between` branch:

```ts
  if (op === "length between") {
    const [lo, hi] = Array.isArray(value) ? (value as unknown[]) : [null, null];
    return `length(${left}) BETWEEN ${valueSql(lo)} AND ${valueSql(hi)}`;
  }
```

(`quoteList` is now only unused if nothing else references it — leave it if still referenced elsewhere; if `bun tsc -b` reports it unused, delete the `quoteList` function.)

- [ ] **Step 4: Run tests + type-check**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts && bun tsc -b`
Expected: PASS; no type errors.

- [ ] **Step 5: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.ts app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts
git commit -m "feat(item42): allow column refs in between bounds and in/not-in entries

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 4: ValueCell `{` autocomplete + resolved-chip UI

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/ValueCell.tsx`
- Modify: `app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/LowcodeRow.tsx`
- Modify: `app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/{en,pt-BR,it,es}.json`

**Interfaces:**
- Consumes: `LowcodeColumnRef` (`lowcodeCompile.ts`), `isColumnRef` / `ColumnRefValue` (`lowcodeAst.ts`), `Family` (`lowcodeOperators.ts`), the row `family` computed in `LowcodeRow`.
- Produces: `ValueCell` accepts new props `declaredColumns: LowcodeColumnRef[]` and `family: Family`.

- [ ] **Step 1: Add i18n keys (all four locales)**

In `en.json`, under the `rulesRegistry` object (near the other `lowcode*` keys), add:

```json
"lowcodeValueColumnsHeader": "Columns",
"lowcodeValueClearColumn": "Use a literal value instead",
"lowcodeValueNoColumns": "No compatible columns",
```

Add the SAME three keys with translated values:
- `pt-BR.json`: `"Colunas"`, `"Usar um valor literal"`, `"Nenhuma coluna compatível"`
- `it.json`: `"Colonne"`, `"Usa un valore letterale"`, `"Nessuna colonna compatibile"`
- `es.json`: `"Columnas"`, `"Usar un valor literal"`, `"Sin columnas compatibles"`

- [ ] **Step 2: Add a shared column-autocomplete input to `ValueCell.tsx`**

At the top of `ValueCell.tsx`, extend imports:

```tsx
import { Popover, PopoverContent, PopoverAnchor } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandItem, CommandList } from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";
import { isColumnRef, type ColumnRefValue } from "@/lib/lowcodeAst";
import type { Family } from "@/lib/lowcodeOperators";
```

Extend `Props`:

```tsx
type Props = {
  operator: string;
  family: Family;
  value: unknown;
  onChange: (next: unknown) => void;
  /** Declared columns available to reference (item 42). */
  declaredColumns?: LowcodeColumnRef[];
};
```

Add a self-contained sub-component that renders EITHER a resolved column chip OR a text input that opens a family-filtered column picker when the user types `{`. Place it above the `ValueCell` function:

```tsx
/**
 * A single value input that can hold a literal OR one column reference (item
 * 42). Typing `{` opens a family-filtered column autocomplete; picking a column
 * commits a ColumnRefValue and the box renders as a chip (✕ clears back to a
 * literal). The whole box is literal-XOR-column — never a mixed expression.
 */
function ColumnAwareInput({
  value,
  onChange,
  family,
  declaredColumns,
  type = "text",
  placeholder,
}: {
  value: unknown;
  onChange: (next: unknown) => void;
  family: Family;
  declaredColumns: LowcodeColumnRef[];
  type?: "text" | "number" | "date";
  placeholder?: string;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);

  // Only same-family columns are offered (ANY family shows all). Qualified
  // join columns (table.col) carry ANY and are always shown.
  const candidates = declaredColumns.filter(
    (c) => family === "ANY" || c.family === "ANY" || c.family === family,
  );

  if (isColumnRef(value)) {
    return (
      <Badge
        variant="secondary"
        className="h-8 gap-1 rounded-md px-2 font-mono text-xs font-normal"
      >
        <span className="truncate">{`{{${(value as ColumnRefValue).$col}}}`}</span>
        <button
          type="button"
          aria-label={t("rulesRegistry.lowcodeValueClearColumn")}
          className="opacity-60 hover:opacity-100"
          onClick={() => onChange("")}
        >
          <X className="h-3 w-3" />
        </button>
      </Badge>
    );
  }

  const text = value == null ? "" : String(value);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverAnchor asChild>
        <Input
          type={type}
          value={text}
          placeholder={placeholder}
          className={VALUE_INPUT_CLASS}
          onChange={(e) => {
            const v = e.target.value;
            // Typing "{" (as the FIRST char) enters column-pick mode: open the
            // autocomplete and keep the box a literal until a column is chosen.
            if (v === "{") {
              setOpen(true);
              return;
            }
            onChange(type === "number" ? Number(v) : v);
          }}
          onKeyDown={(e) => {
            if (e.key === "{") {
              e.preventDefault();
              setOpen(true);
            }
          }}
        />
      </PopoverAnchor>
      <PopoverContent className="w-56 p-0" align="start">
        <Command>
          <CommandList>
            <CommandEmpty>{t("rulesRegistry.lowcodeValueNoColumns")}</CommandEmpty>
            <CommandGroup heading={t("rulesRegistry.lowcodeValueColumnsHeader")}>
              {candidates.map((c) => (
                <CommandItem
                  key={c.name}
                  value={c.name}
                  className="font-mono text-xs"
                  onSelect={() => {
                    onChange({ $col: c.name });
                    setOpen(false);
                  }}
                >
                  {c.name.includes(".") ? c.name : `{{${c.name}}}`}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
```

- [ ] **Step 3: Use `ColumnAwareInput` in the `single`, `double`, and `chip` shapes**

Replace the `single` branch's `<Input …/>` with:

```tsx
  if (shape.kind === "single") {
    return (
      <ColumnAwareInput
        value={value}
        onChange={onChange}
        family={family}
        declaredColumns={declaredColumns ?? []}
        type={shape.type}
      />
    );
  }
```

Replace each `<Input …/>` in the `double` branch with a `ColumnAwareInput` (per bound), keeping the `lo`/`hi` update logic:

```tsx
  if (shape.kind === "double") {
    const [lo, hi] = Array.isArray(value) ? value : [null, null];
    const update = (i: 0 | 1, v: unknown) => {
      const next = Array.isArray(value) ? [...value] : [null, null];
      next[i] = v;
      onChange(next);
    };
    return (
      <div className="flex w-full min-w-0 items-center gap-1">
        <ColumnAwareInput
          value={lo}
          onChange={(v) => update(0, v)}
          family={family}
          declaredColumns={declaredColumns ?? []}
          placeholder={t("rulesRegistry.lowcodeMinPlaceholder")}
        />
        <span className="text-xs text-muted-foreground">…</span>
        <ColumnAwareInput
          value={hi}
          onChange={(v) => update(1, v)}
          family={family}
          declaredColumns={declaredColumns ?? []}
          placeholder={t("rulesRegistry.lowcodeMaxPlaceholder")}
        />
      </div>
    );
  }
```

For `chip` (the `ChipInput` sub-component): a chip list is comma-buffer text today. Keep the free-text buffer for literals, but when the user types `{` as the sole content of the buffer, offer the column picker and append a `{ $col }` entry to the array. Implement by giving `ChipInput` the same `family` + `declaredColumns` props and, on a committed `{{col}}`-style token OR a picker selection, storing a `ColumnRefValue` element in the array (the compiler in Task 3 already renders array elements via `valueSql`). Minimal approach: render existing column-ref entries as chips above the text input and add a small "＋ column" affordance that opens the same `Command` popup; literal entries stay comma-typed. (If chip complexity is high, the reviewer may split this into its own follow-up — but the array/`valueSql` plumbing from Task 3 already supports it.)

- [ ] **Step 4: Pass `declaredColumns` + `family` from `LowcodeRow`**

In `LowcodeRow.tsx`, the row `family` is already computed (`const family: Family = …`). Find the `<ValueCell …/>` render and add the two props:

```tsx
<ValueCell
  operator={row.operator}
  family={family}
  value={row.value}
  onChange={setValue}
  declaredColumns={declaredColumns}
/>
```

- [ ] **Step 5: Type-check + run frontend tests**

Run: `cd app && bun tsc -b && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts`
Expected: no type errors; tests green.

- [ ] **Step 6: i18n parity check**

Run: `cd app && node -e "const en=require('./src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json');for(const l of ['pt-BR','it','es']){const o=require('./src/databricks_labs_dqx_app/ui/lib/i18n/locales/'+l+'.json');for(const k of ['lowcodeValueColumnsHeader','lowcodeValueClearColumn','lowcodeValueNoColumns']){if(!o.rulesRegistry||!o.rulesRegistry[k]){console.error(l,'MISSING',k);process.exit(1)}}}console.log('i18n parity OK')"`
Expected: `i18n parity OK`.

- [ ] **Step 7: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/ValueCell.tsx app/src/databricks_labs_dqx_app/ui/components/rules/lowcode/LowcodeRow.tsx app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/en.json app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/pt-BR.json app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/it.json app/src/databricks_labs_dqx_app/ui/lib/i18n/locales/es.json
git commit -m "feat(item42): '{' column autocomplete + chip in Condition Builder value boxes

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 5: AI SQL-pass slot derivation (so AI col-vs-col runs)

**Files:**
- Modify: `app/src/databricks_labs_dqx_app/backend/services/ai_rules_service.py` (the `sql` branch of `_validate_and_repair_proposal`, ~line 713)
- Test: `app/tests/test_ai_rules_service.py` (add a case; if that module does not exist, create it following the sibling `app/tests/test_*.py` conventions)

**Interfaces:**
- Consumes: existing `extract_slot_tokens` (already used by `_derive_lowcode_slots`) and the `sql_query` string on the proposal `definition`.
- Produces: the returned proposal dict's `slots` is populated for `mode == "sql"` (one slot per distinct `{{token}}` in the `sql_query`), matching the low-code pass's behaviour.

- [ ] **Step 1: Write the failing test**

Add to the AI-service test module:

```python
def test_sql_proposal_derives_slots_from_predicate_tokens():
    """A col-vs-col SQL proposal declares a slot per {{token}} so the RHS column
    is substituted and runs (item 42)."""
    svc = _make_ai_rules_service()  # reuse the module's existing service factory/fixture
    proposal = {
        "name": "amount within limit",
        "description": "amount must not exceed credit_limit",
        "mode": "sql",
        "dimension": "validity",
        "severity": "error",
        "polarity": "pass",
        "definition": {"sql_query": "{{amount}} <= {{credit_limit}}"},
        "columns": [
            {"name": "amount", "family": "numeric"},
            {"name": "credit_limit", "family": "numeric"},
        ],
    }
    result = svc._validate_and_repair_proposal(proposal)
    assert result is not None
    names = [s["name"] for s in result["slots"]]
    assert names == ["amount", "credit_limit"]
    assert {s["family"] for s in result["slots"]} == {"numeric"}
```

(Match the module's existing construction pattern for `AiRulesService` — reuse its fixture/helper rather than inventing one. If the SQL branch requires `sql_query` to be a full `SELECT`, use the shape the existing SQL-pass tests use and keep the two `{{token}}` refs.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4 && UV_DEFAULT_INDEX=https://pypi-proxy.cloud.databricks.com/simple/ UV_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple/ make app-test K="derives_slots_from_predicate"`
Expected: FAIL — `result["slots"]` is `[]` (the SQL branch never populates slots).

- [ ] **Step 3: Populate slots on the SQL branch**

In `_validate_and_repair_proposal`, the `elif mode == "sql":` block currently only validates safety. After the `is_sql_query_safe` check passes, derive slots from the query's tokens. Reuse the same token extraction `_derive_lowcode_slots` uses. Change the `sql` branch to:

```python
        elif mode == "sql":
            sql_query = definition.get("sql_query")
            if not isinstance(sql_query, str) or not sql_query.strip():
                return None
            if not is_sql_query_safe(sql_query):
                logger.warning("AI-generated sql rule dropped: unsafe SQL query")
                return None
            # Declare a slot per {{token}} in the predicate so the materializer
            # substitutes every column ref — including a RHS col-vs-col ref
            # (item 42) — exactly like the low-code pass does. Family hints come
            # from the model's `columns`, else "any".
            slots = self._derive_sql_slots(sql_query, proposal.get("columns"))
```

Add a small helper next to `_derive_lowcode_slots` (reusing `extract_slot_tokens` + the `_VALID_SLOT_FAMILIES` family-hint logic — factor the shared family-hint parse if it is already a helper; otherwise inline it identically):

```python
    @staticmethod
    def _derive_sql_slots(sql_query: str, ai_columns: object) -> list[dict[str, Any]]:
        """One slot per distinct {{token}} in a raw sql_query proposal (item 42),
        so a RHS column reference is substituted and runs. Mirrors
        _derive_lowcode_slots' token/family handling."""
        family_hint: dict[str, str] = {}
        if isinstance(ai_columns, list):
            for col in ai_columns:
                if not isinstance(col, dict):
                    continue
                name = col.get("name")
                if not isinstance(name, str) or not name.strip():
                    continue
                raw_family = col.get("family")
                family = raw_family.lower() if isinstance(raw_family, str) else ""
                family_hint[name.strip()] = family if family in _VALID_SLOT_FAMILIES else "any"
        tokens = extract_slot_tokens(sql_query)
        return [
            {"name": name, "family": family_hint.get(name, "any"), "position": i, "cardinality": "one", "arg_key": None}
            for i, name in enumerate(tokens)
        ]
```

(Confirm `extract_slot_tokens`'s signature from its use in `_derive_lowcode_slots` — it accepts one or more strings and returns distinct tokens in first-appearance order. Call it with just `sql_query` here.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4 && UV_DEFAULT_INDEX=https://pypi-proxy.cloud.databricks.com/simple/ UV_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple/ make app-test K="derives_slots_from_predicate"`
Expected: PASS.

- [ ] **Step 5: basedpyright + lint on the changed file**

Run: `cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4 && UV_DEFAULT_INDEX=https://pypi-proxy.cloud.databricks.com/simple/ UV_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple/ bash -c 'cd app && uv run --frozen ruff check src/databricks_labs_dqx_app/backend/services/ai_rules_service.py'`
Expected: no new errors (pre-existing repo-wide basedpyright debt is not introduced by this change).

- [ ] **Step 6: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/backend/services/ai_rules_service.py app/tests/test_ai_rules_service.py
git commit -m "feat(item42): derive slots on AI sql-pass so col-vs-col predicates run

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 6: Round-trip + read-only/diff verification (no new runtime code)

**Files:**
- Test/inspect: `app/src/databricks_labs_dqx_app/ui/components/RegistryRuleFormDialog.tsx` (parse/serialize of `lowcode_ast`), `app/src/databricks_labs_dqx_app/ui/lib/registry-rule-conversion.ts`.

**Interfaces:** none produced; this task confirms the additive `{ $col }` value survives save→reload and renders read-only.

- [ ] **Step 1: Confirm the AST is persisted/rehydrated verbatim**

Read `RegistryRuleFormDialog.tsx` around where `lowcodeAst` is read from `body.lowcode_ast` and written on save. Confirm the AST is stored as-is (JSON) — a `{ $col }` value is carried through with no scalar coercion. Note the file:line in the commit message. (If a scalar coercion / normalization step is found that would drop the object shape, STOP and surface it — that would need a small fix here.)

- [ ] **Step 2: Add a round-trip compiler assertion**

Add to `lowcodeCompile.test.ts`:

```ts
test("item42: a column-ref value survives JSON round-trip and still compiles", () => {
  const original = ast([row({ column_ref: "amount", operator: ">=", value: { $col: "credit_limit" } })]);
  const rehydrated = JSON.parse(JSON.stringify(original)) as typeof original;
  expect(compileAstToSql(rehydrated)).toBe("{{amount}} >= {{credit_limit}}");
});
```

- [ ] **Step 3: Run tests + type-check**

Run: `cd app && bun test src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts && bun tsc -b`
Expected: PASS; no type errors.

- [ ] **Step 4: Commit**

```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4
git checkout app/uv.lock uv.lock 2>/dev/null
git add app/src/databricks_labs_dqx_app/ui/lib/lowcodeCompile.test.ts
git commit -m "test(item42): assert column-ref value survives round-trip + compiles

Co-authored-by: Isaac <isaac@example.com>"
```

---

## Task 7: Full gates + build + deploy to fe-sandbox

**Files:** none (verification + deploy).

- [ ] **Step 1: Full frontend gates**

Run: `cd app && bun tsc -b && bun test`
Expected: no type errors; all tests green.

- [ ] **Step 2: Full backend test for the AI change**

Run: `cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4 && UV_DEFAULT_INDEX=https://pypi-proxy.cloud.databricks.com/simple/ UV_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple/ make app-test K="ai_rules or derives_slots"`
Expected: green.

- [ ] **Step 2b: i18n parity (all keys, all locales)**

Run the parity check from Task 4 Step 6.
Expected: `i18n parity OK`.

- [ ] **Step 3: Build + deploy (.cloud proxy fallback for the flaky dev proxy)**

Run:
```bash
cd /Users/oliver.gordon/Documents/Code/Other/dqx/.claude/worktrees/bug-bash-v4/app
export UV_DEFAULT_INDEX=https://pypi-proxy.cloud.databricks.com/simple/
export UV_INDEX_URL=https://pypi-proxy.cloud.databricks.com/simple/
databricks bundle deploy -p fe-sandbox-dq-demo-2 -t dev --auto-approve
```
Expected: `Deployment complete!`

- [ ] **Step 4: Restart the app**

Run: `databricks bundle run dqx-studio -p fe-sandbox-dq-demo-2 -t dev`
Expected: `App started successfully`.

- [ ] **Step 5: DO NOT squash-merge or push.** Report deployed status to the user, including the sandbox URL, and note the manual visual-QA items (see below).

---

## Manual visual QA (report to user; not automated)

- Condition Builder: in a numeric operand box, type `{` → only numeric columns appear; pick one → green `{{col}}` chip; ✕ clears back to a literal input.
- Text/temporal boxes show only their family's columns.
- `between`: set one bound to a literal and the other to a column.
- Save a `col_a >= col_b` rule, reload the editor → the column chip is restored (round-trip).
- Dry-run the saved rule → it executes and reports (RHS `{{col}}` substituted).
- Build-with-AI: ask for a col-vs-col rule (e.g. "flag rows where amount exceeds credit_limit") → the generated rule saves with both slots and runs.
- Native Basic Checks: confirm scalar arg boxes are UNCHANGED (no `{` affordance).

## Self-Review notes

- Spec coverage: AST shape (T1), compiler single (T2) + between/in (T3), UI affordance across single/double/chip (T4), AI SQL slots (T5), round-trip/read-only (T6), gates+deploy (T7). All spec sections covered.
- Out-of-scope guardrails restated in Global Constraints (native untouched; type-picker/valueless/interval get no affordance).
- Types consistent: `ColumnRefValue`/`isColumnRef` (T1) used verbatim in T2/T4; `valueSql` (T2) reused in T3; `_derive_sql_slots` mirrors `_derive_lowcode_slots` (T5).
- Risk flagged inline: T6 Step 1 stops if a scalar-coercion step would drop the `{ $col }` shape on save.
