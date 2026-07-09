import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Plus } from "lucide-react";
import { LowcodeRow } from "./LowcodeRow";
import type { AnyRow, LowcodeAstV2 } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  ast: LowcodeAstV2;
  onChange: (next: LowcodeAstV2) => void;
  declaredColumns: LowcodeColumnRef[];
  readOnly?: boolean;
};

function defaultColumnRef(declared: LowcodeColumnRef[]): string {
  return declared[0]?.name ?? "";
}

// Ported 1:1 from dqlake's LowcodeBuilder — the row stack with "Add
// condition" / "Add aggregated condition" actions.
export function LowcodeBuilder({ ast, onChange, declaredColumns, readOnly }: Props) {
  const { t } = useTranslation();

  const addRow = () => {
    if (readOnly) return;
    const next: AnyRow = {
      kind: "row",
      combinator: ast.rows.length === 0 ? null : "AND",
      column_ref: defaultColumnRef(declaredColumns),
      operator: "is null",
      value: null,
    };
    onChange({ ...ast, rows: [...ast.rows, next] });
  };

  const addAggregated = () => {
    if (readOnly) return;
    const next: AnyRow = {
      kind: "aggregated",
      combinator: ast.rows.length === 0 ? null : "AND",
      aggregate: "count",
      column_ref: defaultColumnRef(declaredColumns),
      operator: "=",
      value: 0,
    };
    onChange({ ...ast, rows: [...ast.rows, next] });
  };

  const updateRow = (i: number, row: AnyRow) => {
    if (readOnly) return;
    const next = ast.rows.slice();
    next[i] = row;
    onChange({ ...ast, rows: next });
  };

  const deleteRow = (i: number) => {
    if (readOnly) return;
    const next = ast.rows.slice();
    next.splice(i, 1);
    if (next.length > 0) next[0] = { ...next[0], combinator: null };
    onChange({ ...ast, rows: next });
  };

  return (
    <fieldset
      disabled={readOnly}
      className={readOnly ? "space-y-3 opacity-70 [&_button]:pointer-events-none" : "space-y-3"}
    >
      {ast.rows.length === 0 && (
        <p className="text-xs text-muted-foreground italic">
          {readOnly ? t("rulesRegistry.lowcodeNoConditions") : t("rulesRegistry.lowcodeAddFirstCondition")}
        </p>
      )}
      {ast.rows.map((row, i) => (
        <LowcodeRow
          key={i}
          row={row}
          isFirst={i === 0}
          declaredColumns={declaredColumns}
          onChange={(r) => updateRow(i, r)}
          onDelete={() => deleteRow(i)}
          readOnly={readOnly}
        />
      ))}
      {!readOnly && (
        // Buttons sit directly under the row stack; the divider below them
        // separates this row-editing block from whatever follows (Advanced /
        // "Then the row") instead of sitting between the rows and the
        // buttons (item 8).
        <div className="pt-2 pb-3 border-b space-y-2">
          <div className="flex gap-2">
            {/* text-xs to match the adjacent row dropdowns' size (item 23d) —
                `size="sm"` only trims height/padding, Button's base class
                still sets text-sm. */}
            <Button type="button" variant="outline" size="sm" className="text-xs" onClick={addRow}>
              <Plus className="mr-1.5 h-3.5 w-3.5" /> {t("rulesRegistry.lowcodeAddCondition")}
            </Button>
            <Button type="button" variant="outline" size="sm" className="text-xs" onClick={addAggregated}>
              <Plus className="mr-1.5 h-3.5 w-3.5" /> {t("rulesRegistry.lowcodeAddAggregatedCondition")}
            </Button>
          </div>
        </div>
      )}
    </fieldset>
  );
}
