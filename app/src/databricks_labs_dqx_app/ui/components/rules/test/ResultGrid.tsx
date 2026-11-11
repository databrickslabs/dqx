// ResultGrid — read-only tinted results table for the Rule Test tab's Table
// mode (P22-E), ported from dqlake's `test/DataGrid.tsx` (read-only variant).
// Each returned row is tinted green (pass) / red (fail) via `verdicts`.

import { useState } from "react";
import { ColumnVisibilityPicker } from "@/components/common/ColumnVisibilityPicker";
import { cn } from "@/lib/utils";

export interface ResultColumn {
  name: string;
  mapped: boolean; // referenced/mapped by the rule -> bold header
}

export function ResultGrid({
  columns,
  rows,
  verdicts,
}: {
  columns: ResultColumn[];
  rows: (string | null)[][];
  verdicts?: (boolean | null)[]; // per-row pass/fail
}) {
  // Columns hidden with the picker; rows keep every value, indexed as in `columns`.
  const [hidden, setHidden] = useState<Set<string>>(() => new Set());
  const shown = columns
    .map((c, i) => ({ ...c, index: i }))
    .filter((c) => !hidden.has(c.name));
  return (
    <div className="space-y-2">
      <div className="flex justify-end">
        <ColumnVisibilityPicker
          columns={columns.map((c) => c.name)}
          hiddenColumns={hidden}
          onHiddenColumnsChange={setHidden}
        />
      </div>
      {/* Scrolls both ways inside a bounded box; the header stays in view. */}
      <div className="max-h-[70vh] overflow-auto rounded-md border">
        <table className="min-w-full text-sm" style={{ width: "max-content" }}>
          <thead className="sticky top-0 z-10 bg-muted">
            <tr>
              {shown.map((c) => (
                <th
                  key={c.name}
                  className={cn(
                    "px-2 py-1 text-left whitespace-nowrap min-w-[7ch]",
                    c.mapped
                      ? "font-bold"
                      : "font-medium text-muted-foreground",
                  )}
                >
                  {c.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => {
              const passed = verdicts?.[ri];
              return (
                <tr
                  key={ri}
                  className={cn(
                    "border-t",
                    passed === true && "bg-green-500/12",
                    passed === false && "bg-red-500/10",
                  )}
                >
                  {shown.map((c) => (
                    <td key={c.name} className="p-1 align-top min-w-[7ch]">
                      <span className="px-1 font-mono text-xs whitespace-nowrap">
                        {row[c.index] ?? ""}
                      </span>
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
