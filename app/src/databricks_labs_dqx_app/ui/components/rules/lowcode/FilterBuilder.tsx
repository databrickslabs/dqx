import { useTranslation } from "react-i18next";
import { Button } from "@/components/ui/button";
import { Plus } from "lucide-react";
import { LowcodeRow } from "./LowcodeRow";
import type { AnyRow, LowcodeAstV2 } from "@/lib/lowcodeAst";
import type { LowcodeColumnRef } from "@/lib/lowcodeCompile";

type Props = {
  /** The filter's condition-row AST (rows only — a filter is a plain WHERE
   * predicate, no joins/group-by/aggregates). */
  ast: LowcodeAstV2;
  onChange: (next: LowcodeAstV2) => void;
  declaredColumns: LowcodeColumnRef[];
  readOnly?: boolean;
};

function defaultColumnRef(declared: LowcodeColumnRef[]): string {
  return declared[0]?.name ?? "";
}

/**
 * A low-code condition builder for a rule's ROW FILTER (the pre-condition SQL
 * WHERE that scopes which rows the check evaluates). Reuses {@link LowcodeRow}
 * — the same column / operator / value controls used for the main condition —
 * so filters are built visually rather than typed as raw SQL. The compiled
 * predicate materializes into DQRule.filter; the AST is persisted
 * (`body.filter_ast`) so it re-opens in the builder.
 *
 * Unlike the main condition builder, a filter is OPTIONAL: it starts empty (no
 * seeded row), every row is deletable, and there are no aggregated conditions.
 */
export function FilterBuilder({ ast, onChange, declaredColumns, readOnly }: Props) {
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
    <div className="space-y-2">
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
        <Button type="button" variant="outline" size="sm" className="text-xs" onClick={addRow}>
          <Plus className="mr-1.5 h-3.5 w-3.5" /> {t("rulesRegistry.filterAddCondition")}
        </Button>
      )}
    </div>
  );
}
