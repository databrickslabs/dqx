// ResultGrid — read-only tinted results table for the Rule Test tab's Table
// mode (P22-E), ported from dqlake's `test/DataGrid.tsx` (read-only variant).
// Each returned row is tinted green (pass) / red (fail) via `verdicts`.

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
  return (
    <div className="overflow-auto rounded-md border">
      <table className="text-sm" style={{ width: "max-content" }}>
        <thead className="bg-muted/50">
          <tr>
            {columns.map((c) => (
              <th
                key={c.name}
                className={cn(
                  "px-2 py-1 text-left whitespace-nowrap min-w-[7ch]",
                  c.mapped ? "font-bold" : "font-medium text-muted-foreground",
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
                className={cn("border-t", passed === true && "bg-green-500/12", passed === false && "bg-red-500/10")}
              >
                {columns.map((c, ci) => (
                  <td key={c.name} className="p-1 align-top min-w-[7ch]">
                    <span className="px-1 font-mono text-xs whitespace-nowrap">{row[ci] ?? ""}</span>
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
