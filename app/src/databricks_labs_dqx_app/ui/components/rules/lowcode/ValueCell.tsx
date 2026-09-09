import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Popover, PopoverContent, PopoverAnchor, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Plus, X } from "lucide-react";
import { valueCellShape, operatorAllowsColumnRef, VALIDITY_TYPES, type Family } from "@/lib/lowcodeOperators";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";
import { isColumnRef, type ColumnRefValue } from "@/lib/lowcodeAst";

type Props = {
  operator: string;
  family: Family;
  value: unknown;
  onChange: (next: unknown) => void;
  /** Declared columns available to reference (item 42). */
  declaredColumns?: LowcodeColumnRef[];
};

const UNIT_OPTIONS = ["minutes", "hours", "days", "weeks", "months", "years"];

// Shared value-input styling so every input in this cell matches the
// operator/column dropdowns beside it: h-8, monospace, xs. The base `Input`
// component ships `md:text-sm`, which tailwind-merge KEEPS (it's a responsive
// variant, a different modifier scope from the plain `text-xs` we add) — so on
// desktop the inputs would render a size larger than the Select triggers
// (whose plain `text-sm` IS fully overridden by `text-xs`). Pinning
// `md:text-xs` closes that gap. `w-full min-w-0` lets the input fill — and grow
// with — its grid track without overflowing it.
const VALUE_INPUT_CLASS = "h-8 w-full min-w-0 font-mono text-xs md:text-xs";

/**
 * A single value input that can hold a literal OR one column reference (item
 * 42). Typing `{` opens a family-filtered column autocomplete; picking a column
 * commits a ColumnRefValue and the box renders as a chip (✕ clears back to a
 * literal). The whole box is literal-XOR-column — never a mixed expression.
 *
 * When `allowColumnRef` is false the column-picker is entirely disabled: no `{`
 * trigger, no Popover, no column autocomplete. The box behaves as a plain
 * literal input identical to pre-item-42 behaviour. This must be passed for
 * operators whose compiler path (`rowSql`) uses `quote()`/`likeLiteral()` rather
 * than `valueSql()` — those operators cannot honour a column reference and would
 * silently emit `[object Object]` into the generated SQL.
 */
function ColumnAwareInput({
  value,
  onChange,
  family,
  declaredColumns,
  type = "text",
  placeholder,
  allowColumnRef = true,
}: {
  value: unknown;
  onChange: (next: unknown) => void;
  family: Family;
  declaredColumns: LowcodeColumnRef[];
  type?: "text" | "number" | "date";
  placeholder?: string;
  allowColumnRef?: boolean;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);

  // Only same-family columns are offered (ANY family shows all). Qualified
  // join columns (table.col) carry ANY and are always shown.
  const candidates = declaredColumns.filter(
    (c) => family === "ANY" || c.family === "ANY" || c.family === family,
  );

  if (allowColumnRef && isColumnRef(value)) {
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

  // When allowColumnRef is false (or value was a stale column ref from a
  // prior operator), treat the value as a plain literal string.
  const text = isColumnRef(value) ? "" : value == null ? "" : String(value);

  if (!allowColumnRef) {
    // Plain literal input — no Popover, no `{` interception. Byte-identical to
    // the pre-item-42 input for operators the compiler treats as literal-only.
    return (
      <Input
        type={type}
        value={text}
        placeholder={placeholder}
        className={VALUE_INPUT_CLASS}
        onChange={(e) => {
          const v = e.target.value;
          onChange(type === "number" ? Number(v) : v);
        }}
      />
    );
  }

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
          <CommandInput placeholder={t("rulesRegistry.lowcodeGroupBySearch")} />
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

// Ported 1:1 from dqlake's ValueCell — the operator+family determine the
// value-input shape (single/double/chip/interval/type-picker/none).
export function ValueCell({ operator, family, value, onChange, declaredColumns }: Props) {
  const { t } = useTranslation();
  const shape = valueCellShape(operator, family);
  const colRefAllowed = operatorAllowsColumnRef(operator);

  if (shape.kind === "none") {
    // Operators like "is null" / "is not null" take no value — render an empty
    // cell (no placeholder dash) so the row reads cleanly.
    return <div aria-hidden />;
  }

  if (shape.kind === "single") {
    return (
      <ColumnAwareInput
        value={value}
        onChange={onChange}
        family={family}
        declaredColumns={declaredColumns ?? []}
        type={shape.type}
        allowColumnRef={colRefAllowed}
      />
    );
  }

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
          allowColumnRef={colRefAllowed}
        />
        <span className="text-xs text-muted-foreground">…</span>
        <ColumnAwareInput
          value={hi}
          onChange={(v) => update(1, v)}
          family={family}
          declaredColumns={declaredColumns ?? []}
          placeholder={t("rulesRegistry.lowcodeMaxPlaceholder")}
          allowColumnRef={colRefAllowed}
        />
      </div>
    );
  }

  if (shape.kind === "chip") {
    return (
      <ChipInput
        value={value}
        onChange={onChange}
        family={family}
        declaredColumns={declaredColumns ?? []}
        allowColumnRef={colRefAllowed}
      />
    );
  }

  if (shape.kind === "type-picker") {
    const current = typeof value === "string" ? value : "";
    return (
      <Select value={current} onValueChange={(v) => onChange(v)}>
        <SelectTrigger className="h-8 w-full font-mono text-xs">
          <SelectValue placeholder={t("rulesRegistry.lowcodeTypePlaceholder")} />
        </SelectTrigger>
        <SelectContent>
          {VALIDITY_TYPES.map((ty) => (
            <SelectItem key={ty.value} value={ty.value}>
              {ty.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  }

  if (shape.kind === "interval") {
    const obj = (typeof value === "object" && value !== null ? value : {}) as { number?: number; unit?: string };
    return (
      <div className="flex w-full min-w-0 items-center gap-1">
        <Input
          type="number"
          value={obj.number ?? ""}
          onChange={(e) => onChange({ number: Number(e.target.value), unit: obj.unit ?? "days" })}
          className="h-8 w-20 min-w-0 font-mono text-xs md:text-xs"
        />
        <Select value={obj.unit ?? "days"} onValueChange={(v) => onChange({ number: obj.number ?? 0, unit: v })}>
          <SelectTrigger className="h-8 w-24 font-mono text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {UNIT_OPTIONS.map((u) => (
              <SelectItem key={u} value={u}>
                {u}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    );
  }

  return null;
}

function ChipInput({
  value,
  onChange,
  family,
  declaredColumns,
  allowColumnRef = true,
}: {
  value: unknown;
  onChange: (v: unknown) => void;
  family: Family;
  declaredColumns: LowcodeColumnRef[];
  allowColumnRef?: boolean;
}) {
  const { t } = useTranslation();
  const [pickerOpen, setPickerOpen] = useState(false);

  // An `in` / `not in` list holds a mix of literals and column references
  // (item 42). Column refs render as removable chips; literals stay in the
  // free-text comma buffer exactly as before. Splitting the array this way
  // keeps ChipInput's literal comma-entry behaviour untouched.
  const entries = Array.isArray(value) ? (value as unknown[]) : [];
  const columnRefs = entries.filter(isColumnRef) as ColumnRefValue[];
  const literals = entries.filter((v) => !isColumnRef(v));

  // Local buffer lets the user type commas / trailing whitespace freely; we
  // only commit the parsed array on blur, so the visible text reflects
  // exactly what they typed until they tab/click away. It tracks the LITERAL
  // entries only — column refs are rendered as chips, not text.
  const initial = literals.join(", ");
  const [text, setText] = useState<string>(initial);
  const [focused, setFocused] = useState(false);

  useEffect(() => {
    if (focused) return;
    // Recompute the literal buffer from `value` inline so the effect's deps
    // stay exhaustive on the committed `value` and focus — no derived array in
    // the closure — without refiring on every keystroke.
    const next = (Array.isArray(value) ? (value as unknown[]) : [])
      .filter((v) => !isColumnRef(v))
      .join(", ");
    setText((prev) => (prev === next ? prev : next));
  }, [value, focused]);

  // Same strict family filter as ColumnAwareInput: only same-family columns
  // (ANY shows all; ANY-family columns are always shown).
  const candidates = declaredColumns.filter(
    (c) => family === "ANY" || c.family === "ANY" || c.family === family,
  );

  const commit = (s: string) => {
    const parts = s
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    // Preserve existing column-ref entries; literals reflect the text buffer.
    onChange([...parts, ...columnRefs]);
  };

  const addColumn = (name: string) => {
    // Skip if this column ref is already present — prevents duplicate {$col}
    // chips and a shared React key warning.
    if (columnRefs.some((c) => c.$col === name)) {
      setPickerOpen(false);
      return;
    }
    // Rebuild the literals from the current buffer so an uncommitted edit is
    // not lost, then append the new column reference.
    const parts = text
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    onChange([...parts, ...columnRefs, { $col: name }]);
    setPickerOpen(false);
  };

  const removeColumn = (name: string) => {
    const parts = text
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    onChange([...parts, ...columnRefs.filter((c) => c.$col !== name)]);
  };

  return (
    <div className="flex w-full min-w-0 flex-wrap items-center gap-1">
      {allowColumnRef && columnRefs.map((c) => (
        <Badge
          key={c.$col}
          variant="secondary"
          className="h-8 gap-1 rounded-md px-2 font-mono text-xs font-normal"
        >
          <span className="truncate">{`{{${c.$col}}}`}</span>
          <button
            type="button"
            aria-label={t("rulesRegistry.lowcodeValueClearColumn")}
            className="opacity-60 hover:opacity-100"
            onClick={() => removeColumn(c.$col)}
          >
            <X className="h-3 w-3" />
          </button>
        </Badge>
      ))}
      <Input
        value={text}
        onChange={(e) => setText(e.target.value)}
        onFocus={() => setFocused(true)}
        onBlur={() => {
          setFocused(false);
          commit(text);
        }}
        placeholder={t("rulesRegistry.lowcodeChipPlaceholder")}
        className={VALUE_INPUT_CLASS}
      />
      {allowColumnRef && <Popover open={pickerOpen} onOpenChange={setPickerOpen}>
        <PopoverTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-8 shrink-0 gap-1 px-2 text-xs text-muted-foreground"
          >
            <Plus className="h-3 w-3" />
            {t("rulesRegistry.lowcodeValueColumnsHeader")}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-56 p-0" align="start">
          <Command>
            <CommandInput placeholder={t("rulesRegistry.lowcodeGroupBySearch")} />
            <CommandList>
              <CommandEmpty>{t("rulesRegistry.lowcodeValueNoColumns")}</CommandEmpty>
              <CommandGroup heading={t("rulesRegistry.lowcodeValueColumnsHeader")}>
                {candidates.map((c) => (
                  <CommandItem
                    key={c.name}
                    value={c.name}
                    className="font-mono text-xs"
                    onSelect={() => addColumn(c.name)}
                  >
                    {c.name.includes(".") ? c.name : `{{${c.name}}}`}
                  </CommandItem>
                ))}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>}
    </div>
  );
}
