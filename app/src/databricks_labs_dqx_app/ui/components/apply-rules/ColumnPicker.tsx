// ColumnPicker — family-filtered column selector used when mapping a
// registry rule's slots to real table columns. A slot's `family` ("numeric",
// "text", "temporal", "boolean", or "any") narrows the candidate list so
// users aren't offered columns that can't satisfy the check.

import { useTranslation } from "react-i18next";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { ColumnOut, RuleSlot } from "@/lib/api";

export type ColumnFamily = "numeric" | "text" | "temporal" | "boolean" | "any";

export function familyForType(typeName: string): ColumnFamily {
  const t = typeName.toUpperCase();
  if (/^(INT|BIGINT|SMALLINT|TINYINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|LONG|SHORT|BYTE)/.test(t)) return "numeric";
  if (/^(STRING|VARCHAR|CHAR)/.test(t)) return "text";
  if (/^(DATE|TIMESTAMP)/.test(t)) return "temporal";
  if (t === "BOOLEAN") return "boolean";
  return "any";
}

/**
 * @param excludeColumns Column names to omit from the candidate list — used
 * by the "+ Apply to another column" flow to hide columns the rule is
 * already mapped to, mirroring dqlake's `usedSetForNew` exclusion.
 */
export function columnsForSlot(
  columns: ColumnOut[],
  slot: Pick<RuleSlot, "family">,
  excludeColumns?: string[],
): ColumnOut[] {
  const exclude = excludeColumns?.length ? new Set(excludeColumns) : null;
  return columns.filter(
    (c) => (slot.family === "any" || familyForType(c.type_name) === slot.family) && !exclude?.has(c.name),
  );
}

interface SingleColumnPickerProps {
  slot: RuleSlot;
  columns: ColumnOut[];
  value: string | undefined;
  onChange: (column: string) => void;
  /** See `columnsForSlot`'s `excludeColumns`. */
  excludeColumns?: string[];
}

/** Single-column picker for a `cardinality: "one"` slot. */
export function SingleColumnPicker({ slot, columns, value, onChange, excludeColumns }: SingleColumnPickerProps) {
  const { t } = useTranslation();
  const matches = columnsForSlot(columns, slot, excludeColumns);
  return (
    <Select value={value ?? ""} onValueChange={onChange}>
      <SelectTrigger className="h-8 text-xs">
        <SelectValue placeholder={t("monitoredTables.selectColumnPlaceholder")} />
      </SelectTrigger>
      <SelectContent>
        {matches.length === 0 ? (
          <div className="p-2 text-xs text-muted-foreground">{t("monitoredTables.noMatchingColumns")}</div>
        ) : (
          matches.map((col) => (
            <SelectItem key={col.name} value={col.name} className="text-xs font-mono">
              {col.name}
            </SelectItem>
          ))
        )}
      </SelectContent>
    </Select>
  );
}

interface MultiColumnPickerProps {
  slot: RuleSlot;
  columns: ColumnOut[];
  value: string[];
  onChange: (columns: string[]) => void;
  /** See `columnsForSlot`'s `excludeColumns`. */
  excludeColumns?: string[];
}

/** Multi-column checklist picker for a `cardinality: "many"` slot. */
export function MultiColumnPicker({ slot, columns, value, onChange, excludeColumns }: MultiColumnPickerProps) {
  const { t } = useTranslation();
  const matches = columnsForSlot(columns, slot, excludeColumns);
  if (matches.length === 0) {
    return <p className="text-xs text-muted-foreground">{t("monitoredTables.noMatchingColumns")}</p>;
  }
  return (
    <div className="grid grid-cols-2 gap-1.5 max-h-40 overflow-y-auto border rounded-md p-2">
      {matches.map((col) => {
        const checked = value.includes(col.name);
        return (
          <label key={col.name} className="flex items-center gap-1.5 text-xs">
            <Checkbox
              checked={checked}
              onCheckedChange={(v) => {
                onChange(v ? [...value, col.name] : value.filter((c) => c !== col.name));
              }}
            />
            <span className="font-mono truncate">{col.name}</span>
          </label>
        );
      })}
    </div>
  );
}
