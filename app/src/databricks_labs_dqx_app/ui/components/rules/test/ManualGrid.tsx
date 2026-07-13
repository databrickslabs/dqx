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
  name: string; // display label, already wrapped in {{ }}
  family: CellFamily;
}

export function ManualGrid({
  columns,
  rows,
  verdicts,
  onCellChange,
  onAddRow,
  onDeleteRow,
}: {
  columns: ManualColumn[];
  rows: (string | null)[][];
  verdicts?: (boolean | null | undefined)[]; // per-row pass/fail; undefined before a run
  onCellChange: (ri: number, ci: number, v: string | null) => void;
  onAddRow: () => void;
  onDeleteRow: (ri: number) => void;
}) {
  const { t } = useTranslation();
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
            {columns.map((c) => (
              <th
                key={c.name}
                className="border-b px-2 py-1.5 text-left whitespace-nowrap min-w-[12ch] font-semibold"
              >
                {c.name}
              </th>
            ))}
            <th className="border-b w-8 bg-transparent" aria-hidden />
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
                  <td key={c.name} className="border-r border-border/50 last:border-r-0 p-0 align-middle min-w-[12ch]">
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
