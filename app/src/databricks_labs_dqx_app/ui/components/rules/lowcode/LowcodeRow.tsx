import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";
import { OperatorDropdown } from "./OperatorDropdown";
import { ValueCell } from "./ValueCell";
import { AggregatedFieldArea } from "./AggregatedFieldArea";
import { aggregateOutputFamily, operatorValidForFamily, type Family } from "@/lib/lowcodeOperators";
import type { AnyRow, Combinator } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  row: AnyRow;
  isFirst: boolean;
  declaredColumns: LowcodeColumnRef[];
  onChange: (next: AnyRow) => void;
  onDelete: () => void;
  readOnly?: boolean;
  /** When provided (registry editor), render the operator cell via this callback
   * instead of the standard OperatorDropdown — used to swap in the merged
   * condition selector. Receives this row's family + operator getter/setter and
   * whether it's the first row (row 0 hosts escalation/change-type; rows 2+ and
   * aggregated rows are operators-only). Applies to both row and aggregated kinds. */
  renderOperator?: (ctx: {
    family: Family;
    value: string;
    onChange: (op: string) => void;
    isFirst: boolean;
  }) => ReactNode;
  /** Whether this row can be deleted. `false` hides the X — used to keep at
   * least one condition on the rule (a rule with zero conditions is invalid). */
  canDelete?: boolean;
  /** Condition-Builder-only compact layout (NOT the page-open anchor row, NOT
   * row filters): the operator box sizes to its CONTENT (so `<=` is narrow and
   * the box shrinks from the right, left edge fixed) and the value box is
   * capped so the row isn't forced to full width. When false (row filters), the
   * operator keeps its fixed 18rem width and the value fills the remainder. */
  compact?: boolean;
};

function familyOf(name: string, declared: LowcodeColumnRef[]): Family {
  const d = declared.find((c) => c.name === name);
  return d?.family ?? "ANY";
}

// Ported 1:1 from dqlake's LowcodeRow — one condition row: IF anchor / AND-OR
// pill, column (or aggregate) picker, operator dropdown, value cell, delete.
export function LowcodeRow({ row, isFirst, declaredColumns, onChange, onDelete, readOnly, renderOperator, canDelete = true, compact = false }: Props) {
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
        // Left-packed row (item 3). Each control fills its track (the triggers
        // carry `w-full`) so the columns read as a flush, aligned table —
        // matching dqlake's LowcodeRow, whose base SelectTrigger is `w-full`.
        "grid gap-2 items-center py-1",
        // The field column is a FIXED 11rem for a normal row (one column
        // picker), but an AGGREGATED row packs TWO controls into that cell
        // (aggregate-function picker + column picker), so 11rem cramps both
        // ("cou…" / "{{col"). Widen the field track to 22rem for aggregated
        // rows so each of its two controls gets ~11rem — matching a normal
        // row's single control — while normal rows are UNCHANGED at 11rem.
        // The operator + value tracks differ by mode:
        //   - compact (Condition Builder only): the whole row is capped at
        //     max-w-2xl (the original bounded width) so it never sprawls across
        //     a wide dialog. Operator sizes to CONTENT (minmax(6rem,max-content))
        //     so `<=` is narrow and shrinks from the RIGHT with a fixed left
        //     edge; value fills the REMAINDER within the cap (minmax(0,1fr)) —
        //     bounded, so the extra input box is never oversized.
        //   - default (row filters): operator fixed 18rem, value fills the
        //     remainder — unchanged.
        // NOTE: Tailwind only generates arbitrary grid-cols values that appear
        // as STATIC literal strings in source (no template interpolation), so
        // every variant is spelled out in full below. `agg` widens the field
        // track (11rem -> 22rem) for aggregated rows only.
        // Aggregated rows: wider cap (max-w-4xl) and the field track GROWS to
        // fill — minmax(22rem,1fr) — so the aggregate-function + column pickers
        // (split evenly by AggregatedFieldArea's internal grid-cols-2) expand
        // leftward into the available width instead of sitting at a fixed size.
        // The value there is usually a small number, so it's bounded to
        // ~14rem rather than eating the remainder. Normal rows unchanged.
        (() => {
          const agg = row.kind === "aggregated";
          if (compact) {
            if (agg) {
              return readOnly
                ? "max-w-4xl grid-cols-[80px_minmax(22rem,1fr)_minmax(6rem,max-content)_minmax(8rem,14rem)]"
                : "max-w-4xl grid-cols-[80px_minmax(22rem,1fr)_minmax(6rem,max-content)_minmax(8rem,14rem)_28px]";
            }
            return readOnly
              ? "max-w-2xl grid-cols-[80px_11rem_minmax(6rem,max-content)_minmax(0,1fr)]"
              : "max-w-2xl grid-cols-[80px_11rem_minmax(6rem,max-content)_minmax(0,1fr)_28px]";
          }
          if (agg) {
            return readOnly
              ? "grid-cols-[80px_minmax(22rem,1fr)_18rem_minmax(8rem,14rem)]"
              : "grid-cols-[80px_minmax(22rem,1fr)_18rem_minmax(8rem,14rem)_28px]";
          }
          return readOnly
            ? "grid-cols-[80px_11rem_18rem_minmax(0,1fr)]"
            : "grid-cols-[80px_11rem_18rem_minmax(0,1fr)_28px]";
        })(),
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
                  // Clicking anywhere on the pill flips to the other value —
                  // both segments toggle as one control, matching the
                  // PASS/FAIL polarity switcher.
                  onClick={() => setCombinator((row.combinator ?? "AND") === "AND" ? "OR" : "AND")}
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
            {declaredColumns
              // Once an operator/function is chosen, only offer columns whose
              // family supports it — so you can't pick a column the operator
              // can't run on (e.g. swap a datetime column for a boolean one
              // under a `>` comparison). Before an operator is chosen, every
              // column is offered. The currently-selected column stays visible
              // regardless, so its Select value never orphans.
              .filter(
                (c) =>
                  !row.operator ||
                  c.name === row.column_ref ||
                  operatorValidForFamily(row.operator, c.family),
              )
              .map((c) => (
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

      {renderOperator
        ? renderOperator({ family, value: row.operator, onChange: setOperator, isFirst })
        : <OperatorDropdown value={row.operator} family={family} onChange={setOperator} />}
      <ValueCell operator={row.operator} family={family} value={row.value} onChange={setValue} declaredColumns={declaredColumns} />

      {!readOnly &&
        (canDelete ? (
          <Button type="button" variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={onDelete}>
            <X className="h-4 w-4" />
          </Button>
        ) : (
          // Keep the grid column so the row stays aligned, but no X — a rule
          // must keep at least one condition.
          <span aria-hidden className="h-8 w-8" />
        ))}
    </div>
  );
}
