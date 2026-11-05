// TypedCell — a single editable test-data cell (P22-E, ported from dqlake's
// `test/TypedCell.tsx`). The control rendered is chosen by the slot `family`
// so typed columns get the right picker (number / date / boolean) while
// untyped columns (text / array / any / unknown) stay free text so arbitrary
// or mixed values are still allowed. Every variant keeps the borderless
// grid-cell look and the empty-string -> null convention.
//
// DQX families are lowercase (numeric/temporal/boolean/text/array/any) — the
// only deliberate change from dqlake's uppercase families.

import { useTranslation } from "react-i18next";

export type CellFamily = "numeric" | "temporal" | "boolean" | "text" | "any" | string;

export function TypedCell({
  family,
  value,
  onChange,
  onEnter,
  disabled,
  placeholder,
  className,
  noMinWidth,
  style,
}: {
  family: CellFamily;
  value: string | null;
  onChange: (v: string | null) => void;
  /** Called when Enter is pressed in the cell — used to add + focus a new row. */
  onEnter?: () => void;
  disabled?: boolean;
  placeholder?: string;
  className?: string;
  /** Skip the value-length minWidth so the cell can fill a parent column. */
  noMinWidth?: boolean;
  /** Explicit sizing for the number / text inputs; replaces the value-length minWidth. */
  style?: React.CSSProperties;
}) {
  const { t } = useTranslation();
  const fam = (family || "any").toLowerCase();
  const set = (v: string) => onChange(v === "" ? null : v);
  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      onEnter?.();
    }
  };

  const base =
    "h-8 w-full bg-transparent px-2 font-mono text-xs outline-none focus:bg-accent/40 placeholder:text-muted-foreground/40";
  const widthStyle: React.CSSProperties | undefined =
    style ?? (noMinWidth ? undefined : { minWidth: `${(value ?? "").length + 2}ch` });

  if (fam === "numeric") {
    return (
      <input
        type="number"
        inputMode="decimal"
        className={className ?? base}
        style={widthStyle}
        placeholder={placeholder ?? t("ruleTest.cellNumberPlaceholder")}
        value={value ?? ""}
        disabled={disabled}
        onChange={(e) => set(e.target.value)}
        onKeyDown={onKeyDown}
      />
    );
  }

  if (fam === "temporal") {
    return (
      <input
        type="date"
        className={className ?? base}
        value={value ?? ""}
        disabled={disabled}
        onChange={(e) => set(e.target.value)}
        onKeyDown={onKeyDown}
        aria-label={t("ruleTest.cellDateLabel")}
      />
    );
  }

  if (fam === "boolean") {
    return (
      <select
        className={className ?? "appearance-none bg-transparent border-0 outline-none h-8 w-full px-2 text-xs focus:bg-accent/40"}
        value={value ?? ""}
        disabled={disabled}
        onChange={(e) => set(e.target.value)}
        onKeyDown={onKeyDown}
        aria-label={t("ruleTest.cellBooleanLabel")}
      >
        <option value="">{placeholder ?? "—"}</option>
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }

  // text / array / any / unknown -> free text.
  return (
    <input
      type="text"
      className={className ?? base}
      style={widthStyle}
      placeholder={placeholder ?? t("ruleTest.cellTextPlaceholder")}
      value={value ?? ""}
      disabled={disabled}
      onChange={(e) => set(e.target.value)}
      onKeyDown={onKeyDown}
    />
  );
}
