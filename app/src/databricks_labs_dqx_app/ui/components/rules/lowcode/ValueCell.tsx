import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { valueCellShape, VALIDITY_TYPES, type Family } from "@/lib/lowcodeOperators";

type Props = {
  operator: string;
  family: Family;
  value: unknown;
  onChange: (next: unknown) => void;
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

// Ported 1:1 from dqlake's ValueCell — the operator+family determine the
// value-input shape (single/double/chip/interval/type-picker/none).
export function ValueCell({ operator, family, value, onChange }: Props) {
  const { t } = useTranslation();
  const shape = valueCellShape(operator, family);

  if (shape.kind === "none") {
    // Operators like "is null" / "is not null" take no value — render an empty
    // cell (no placeholder dash) so the row reads cleanly.
    return <div aria-hidden />;
  }

  if (shape.kind === "single") {
    return (
      <Input
        type={shape.type}
        value={value == null ? "" : String(value)}
        onChange={(e) => onChange(shape.type === "number" ? Number(e.target.value) : e.target.value)}
        className={VALUE_INPUT_CLASS}
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
        <Input
          value={lo == null ? "" : String(lo)}
          onChange={(e) => update(0, e.target.value)}
          className={VALUE_INPUT_CLASS}
          placeholder={t("rulesRegistry.lowcodeMinPlaceholder")}
        />
        <span className="text-xs text-muted-foreground">…</span>
        <Input
          value={hi == null ? "" : String(hi)}
          onChange={(e) => update(1, e.target.value)}
          className={VALUE_INPUT_CLASS}
          placeholder={t("rulesRegistry.lowcodeMaxPlaceholder")}
        />
      </div>
    );
  }

  if (shape.kind === "chip") {
    return <ChipInput value={value} onChange={onChange} />;
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

function ChipInput({ value, onChange }: { value: unknown; onChange: (v: unknown) => void }) {
  const { t } = useTranslation();
  // Local buffer lets the user type commas / trailing whitespace freely; we
  // only commit the parsed array on blur, so the visible text reflects
  // exactly what they typed until they tab/click away.
  const initial = Array.isArray(value) ? (value as unknown[]).join(", ") : "";
  const [text, setText] = useState<string>(initial);
  const [focused, setFocused] = useState(false);

  useEffect(() => {
    if (focused) return;
    const next = Array.isArray(value) ? (value as unknown[]).join(", ") : "";
    // Functional update so the effect reads no outer `text` — deps stay
    // exhaustive on the values that should re-sync the buffer (the incoming
    // committed `value` and focus), without refiring on every keystroke.
    setText((prev) => (prev === next ? prev : next));
  }, [value, focused]);

  const commit = (s: string) => {
    const parts = s
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    onChange(parts);
  };

  return (
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
  );
}
