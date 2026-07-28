// ManualGrid — editable manual-entry grid for the Rule Test tab (P22-E),
// ported from dqlake's `test/ManualGrid.tsx`. Styled as a tidy compact
// spreadsheet; after a run it tints rows in place (green pass / red fail) via
// `verdicts`. Enter in a cell adds a new row and focuses its first cell.

import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";
import { Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { TypedCell, type CellFamily } from "./TypedCell";

export interface ManualColumn {
  name: string; // display label, already wrapped in {{ }} for slot-backed grids
  family: CellFamily;
}

const EDITABLE_FAMILIES: CellFamily[] = ["any", "numeric", "text", "temporal", "boolean"];

export function ManualGrid({
  columns,
  rows,
  verdicts,
  onCellChange,
  onAddRow,
  onDeleteRow,
  onRenameColumn,
  onRetypeColumn,
  onAddColumn,
  onDeleteColumn,
}: {
  columns: ManualColumn[];
  rows: (string | null)[][];
  verdicts?: (boolean | null | undefined)[]; // per-row pass/fail; undefined before a run
  onCellChange: (ri: number, ci: number, v: string | null) => void;
  onAddRow: () => void;
  onDeleteRow: (ri: number) => void;
  /** Reference-table grids only: their columns are the author's to define, since
   *  they mirror a real table's columns rather than the rule's slots. Passing
   *  these turns the header into an editor; omitting them keeps it a static
   *  header (the input grid, whose columns ARE the slots). */
  onRenameColumn?: (ci: number, name: string) => void;
  onRetypeColumn?: (ci: number, family: CellFamily) => void;
  onAddColumn?: () => void;
  onDeleteColumn?: (ci: number) => void;
}) {
  const { t } = useTranslation();
  const editableColumns = !!onAddColumn;
  const tbodyRef = useRef<HTMLTableSectionElement>(null);
  const pendingFocus = useRef(false);
  // After Enter adds a row, focus the first editable cell of the new (last) row.
  useEffect(() => {
    if (!pendingFocus.current) return;
    pendingFocus.current = false;
    const lastRow = tbodyRef.current?.querySelector("tr:last-child");
    lastRow?.querySelector<HTMLElement>("input, select")?.focus();
  }, [rows.length]);
  const handleEnter = () => {
    pendingFocus.current = true;
    onAddRow();
  };

  return (
    <div className="inline-block max-w-full overflow-x-auto rounded-md border">
      <table className="border-collapse text-xs">
        <thead>
          <tr className="bg-muted/50">
            {columns.map((c, ci) =>
              editableColumns ? (
                <th key={ci} className="border-b px-1.5 py-1 text-left min-w-[14ch] font-normal align-top">
                  <div className="flex items-center gap-1">
                    <input
                      value={c.name}
                      onChange={(e) => onRenameColumn?.(ci, e.target.value)}
                      placeholder={t("ruleTest.columnNamePlaceholder")}
                      aria-label={t("ruleTest.columnName")}
                      className="h-6 w-[10ch] min-w-0 rounded border bg-background px-1 font-mono text-xs font-semibold"
                    />
                    <select
                      value={c.family}
                      onChange={(e) => onRetypeColumn?.(ci, e.target.value as CellFamily)}
                      aria-label={t("ruleTest.columnType")}
                      className="h-6 rounded border bg-background px-0.5 text-[10px] text-muted-foreground"
                    >
                      {EDITABLE_FAMILIES.map((f) => (
                        <option key={f} value={f}>
                          {f}
                        </option>
                      ))}
                    </select>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-5 w-5 text-muted-foreground/60 hover:text-destructive"
                      aria-label={t("ruleTest.deleteColumn")}
                      onClick={() => onDeleteColumn?.(ci)}
                    >
                      <X className="h-3 w-3" />
                    </Button>
                  </div>
                </th>
              ) : (
                <th
                  key={c.name}
                  className="border-b px-2 py-1.5 text-left whitespace-nowrap min-w-[12ch] font-semibold"
                >
                  {c.name}
                </th>
              ),
            )}
            {editableColumns ? (
              <th className="border-b px-1 py-1 align-top">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 gap-1 px-1.5 text-[11px] font-normal text-muted-foreground"
                  onClick={onAddColumn}
                >
                  <Plus className="h-3 w-3" /> {t("ruleTest.addColumn")}
                </Button>
              </th>
            ) : (
              <th className="border-b w-8 bg-transparent" aria-hidden />
            )}
          </tr>
        </thead>
        <tbody ref={tbodyRef}>
          {rows.map((row, ri) => {
            const passed = verdicts?.[ri];
            return (
              <tr
                key={ri}
                className={cn(
                  "group border-b last:border-b-0",
                  passed === true && "bg-green-500/10",
                  passed === false && "bg-red-500/10",
                )}
              >
                {columns.map((c, ci) => (
                  // Keyed by index, not name: an author-editable header can hold
                  // a duplicate or empty name mid-typing.
                  <td key={ci} className="border-r border-border/50 last:border-r-0 p-0 align-middle min-w-[12ch]">
                    <TypedCell
                      family={c.family}
                      value={row[ci] ?? null}
                      onChange={(v) => onCellChange(ri, ci, v)}
                      onEnter={handleEnter}
                    />
                  </td>
                ))}
                <td className="w-8 px-0.5 text-center align-middle">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 text-muted-foreground/50 opacity-0 group-hover:opacity-100 hover:text-destructive"
                    aria-label={t("ruleTest.deleteRow")}
                    onClick={() => onDeleteRow(ri)}
                  >
                    <X className="h-3.5 w-3.5" />
                  </Button>
                </td>
              </tr>
            );
          })}
        </tbody>
        <tfoot>
          <tr>
            <td colSpan={columns.length + 1} className="border-t p-0">
              <button
                type="button"
                onClick={onAddRow}
                className="flex w-full items-center justify-center gap-1 py-1.5 text-xs text-muted-foreground hover:bg-accent/40"
              >
                <Plus className="h-3.5 w-3.5" /> {t("ruleTest.addRow")}
              </button>
            </td>
          </tr>
        </tfoot>
      </table>
    </div>
  );
}
