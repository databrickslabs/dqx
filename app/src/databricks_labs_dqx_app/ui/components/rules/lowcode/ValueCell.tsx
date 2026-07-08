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

// Ported 1:1 from dqlake's ValueCell — the operator+family determine the
// value-input shape (single/double/chip/interval/type-picker/none).
export function ValueCell({ operator, family, value, onChange }: Props) {
  const { t } = useTranslation();
  const shape = valueCellShape(operator, family);

  if (shape.kind === "none") {
    return <div className="text-xs text-muted-foreground italic">—</div>;
  }

  if (shape.kind === "single") {
    return (
      <Input
        type={shape.type}
        value={value == null ? "" : String(value)}
        onChange={(e) => onChange(shape.type === "number" ? Number(e.target.value) : e.target.value)}
        className="h-8 font-mono text-xs"
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
      <div className="flex items-center gap-1">
        <Input
          value={lo == null ? "" : String(lo)}
          onChange={(e) => update(0, e.target.value)}
          className="h-8 font-mono text-xs"
          placeholder={t("rulesRegistry.lowcodeMinPlaceholder")}
        />
        <span className="text-xs text-muted-foreground">…</span>
        <Input
          value={hi == null ? "" : String(hi)}
          onChange={(e) => update(1, e.target.value)}
          className="h-8 font-mono text-xs"
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
        <SelectTrigger className="h-8 font-mono text-xs">
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
      <div className="flex items-center gap-1">
        <Input
          type="number"
          value={obj.number ?? ""}
          onChange={(e) => onChange({ number: Number(e.target.value), unit: obj.unit ?? "days" })}
          className="h-8 w-20 font-mono text-xs"
        />
        <Select value={obj.unit ?? "days"} onValueChange={(v) => onChange({ number: obj.number ?? 0, unit: v })}>
          <SelectTrigger className="h-8 w-24 text-xs">
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
    if (next !== text) setText(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
      className="h-8 font-mono text-xs"
    />
  );
}
