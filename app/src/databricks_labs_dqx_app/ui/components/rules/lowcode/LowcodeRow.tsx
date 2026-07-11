import { useTranslation } from "react-i18next";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import { OperatorDropdown } from "./OperatorDropdown";
import { ValueCell } from "./ValueCell";
import { AggregatedFieldArea } from "./AggregatedFieldArea";
import { aggregateOutputFamily, type Family } from "@/lib/lowcodeOperators";
import type { AnyRow, Combinator } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  row: AnyRow;
  isFirst: boolean;
  declaredColumns: LowcodeColumnRef[];
  onChange: (next: AnyRow) => void;
  onDelete: () => void;
  readOnly?: boolean;
};

function familyOf(name: string, declared: LowcodeColumnRef[]): Family {
  const d = declared.find((c) => c.name === name);
  return d?.family ?? "ANY";
}

// Ported 1:1 from dqlake's LowcodeRow — one condition row: IF anchor / AND-OR
// pill, column (or aggregate) picker, operator dropdown, value cell, delete.
export function LowcodeRow({ row, isFirst, declaredColumns, onChange, onDelete, readOnly }: Props) {
  const { t } = useTranslation();
  const family: Family =
    row.kind === "row"
      ? familyOf(row.column_ref, declaredColumns)
      : aggregateOutputFamily(row.aggregate, familyOf(row.column_ref, declaredColumns));

  const setCombinator = (c: Combinator) => onChange({ ...row, combinator: c } as AnyRow);
  const setOperator = (op: string) => onChange({ ...row, operator: op } as AnyRow);
  const setValue = (v: unknown) => onChange({ ...row, value: v } as AnyRow);

  return (
    <div
      className={cn(
        // Left-packed row (item 3). `max-w-2xl` caps the grid so on wide
        // layouts (e.g. the routed detail page) the controls stop stretching
        // edge-to-edge ("fully justified"); the block is left-aligned so the
        // slack sits on the right. Each control fills its `fr` track (the
        // triggers carry `w-full`) so the columns read as a flush, aligned
        // table — matching dqlake's LowcodeRow, whose base SelectTrigger is
        // `w-full` (DQX's is `w-fit`, which is what left the earlier build
        // looking ragged with controls pinned to the left of each track).
        "grid max-w-2xl gap-2 items-center py-1",
        readOnly
          ? "grid-cols-[80px_minmax(0,1.6fr)_minmax(0,1fr)_minmax(0,1.4fr)]"
          : "grid-cols-[80px_minmax(0,1.6fr)_minmax(0,1fr)_minmax(0,1.4fr)_28px]",
      )}
    >
      {isFirst ? (
        // The "IF" framing word sits inline with this row, vertically
        // centered against the condition controls next to it — matching
        // dqlake's LowcodeRow (item 6). RegistryRuleFormDialog no longer
        // renders a duplicate "IF" label above the builder for Low-Code
        // mode (that framing word stays above the condition editor for the
        // Native / SQL modes only, where there's no per-row grid to anchor
        // to).
        <div className="flex items-center h-8 pl-2 justify-self-start">
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            {t("rulesRegistry.ifCondition")}
          </span>
        </div>
      ) : (
        <div className="flex items-center h-8 pr-2 justify-self-end">
          <div className="relative inline-grid grid-cols-2 p-0.5 rounded-full bg-muted">
            <span
              aria-hidden
              className="absolute top-0.5 bottom-0.5 left-0.5 w-[calc(50%-2px)] rounded-full bg-primary transition-transform duration-200 ease-out"
              style={{ transform: `translateX(${(row.combinator ?? "AND") === "OR" ? "100%" : "0%"})` }}
            />
            {(["AND", "OR"] as const).map((c) => {
              const on = (row.combinator ?? "AND") === c;
              return (
                <button
                  key={c}
                  type="button"
                  onClick={() => setCombinator(c)}
                  className={cn(
                    "relative z-10 rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors duration-200 ease-out",
                    on ? "text-primary-foreground" : "text-muted-foreground hover:text-foreground",
                  )}
                >
                  {c}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {row.kind === "row" ? (
        <Select value={row.column_ref || ""} onValueChange={(v) => onChange({ ...row, column_ref: v } as AnyRow)}>
          {/* `w-full` so the picker fills its grid track — matching dqlake's
              LowcodeRow (whose base SelectTrigger is `w-full`), so the column
              pickers line up flush down the stack instead of floating
              content-width at the left of each track (item 3). */}
          <SelectTrigger className="h-8 w-full font-mono text-xs">
            <SelectValue placeholder={t("rulesRegistry.lowcodeColumnPlaceholder")} />
          </SelectTrigger>
          <SelectContent>
            {declaredColumns.map((c) => (
              <SelectItem key={c.name} value={c.name} className="font-mono text-xs">
                {c.name.includes(".") ? c.name : `{{${c.name}}}`}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ) : (
        <AggregatedFieldArea
          aggregate={row.aggregate}
          column_ref={row.column_ref}
          aggregate_param={row.aggregate_param}
          declaredColumns={declaredColumns}
          onChange={({ aggregate, column_ref, aggregate_param }) =>
            onChange({ ...row, aggregate, column_ref, aggregate_param } as AnyRow)
          }
        />
      )}

      <OperatorDropdown value={row.operator} family={family} onChange={setOperator} />
      <ValueCell operator={row.operator} family={family} value={row.value} onChange={setValue} />

      {!readOnly && (
        <Button type="button" variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={onDelete}>
          <X className="h-4 w-4" />
        </Button>
      )}
    </div>
  );
}
