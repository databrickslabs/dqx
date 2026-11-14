// ResultGrid — read-only tinted results table for the Rule Test tab's Table
// mode (P22-E), ported from dqlake's `test/DataGrid.tsx` (read-only variant).
// Each returned row is tinted green (pass) / red (fail) via `verdicts`.

import { useMemo, useState } from "react";
import { PointerSensor, useSensor, useSensors, type DragEndEvent } from "@dnd-kit/core";
import { EditColumnsDropdown } from "@/components/data-table/EditColumnsDropdown";
import { cn } from "@/lib/utils";
import {
  canToggleColumn,
  columnsSignature,
  indexColumns,
  moveColumn,
  toggleColumn,
  visibleColumns,
  type IndexedColumn,
  type ResultColumn,
} from "./result-columns";

export type { ResultColumn };

type Verdicts = (boolean | null)[]; // per-row pass/fail

interface ResultGridProps {
  columns: ResultColumn[];
  rows: (string | null)[][];
  verdicts?: Verdicts;
  /** Height cap of the scrolling box; pass a shorter one where the grid sits in a dialog. */
  maxHeightClassName?: string;
}

/** The results table with the app's Edit View menu to hide and reorder its columns. */
export function ResultGrid(props: ResultGridProps) {
  // Hidden columns and their order belong to one result. Remounting on a new
  // column list drops them, so a re-run against another table or sample never
  // starts with columns silently hidden.
  return <ResultGridView key={columnsSignature(props.columns)} {...props} />;
}

function ResultGridView({ columns, rows, verdicts, maxHeightClassName }: ResultGridProps) {
  const indexed = useMemo(() => indexColumns(columns), [columns]);
  const [order, setOrder] = useState<string[]>(() => indexed.map((c) => c.id));
  const [hidden, setHidden] = useState<Set<string>>(() => new Set());
  const shown = useMemo(() => visibleColumns(indexed, order, hidden), [indexed, order, hidden]);
  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));
  const onDragEnd = ({ active, over }: DragEndEvent) => {
    if (over) setOrder((prev) => moveColumn(prev, String(active.id), String(over.id)));
  };
  return (
    <div className="space-y-2">
      <div className="flex justify-end">
        <EditColumnsDropdown
          order={order}
          labelOf={(id) => indexed[Number(id)]?.name ?? id}
          toggleableOf={(id) => canToggleColumn(id, order, hidden)}
          isChecked={(id) => !hidden.has(id)}
          onToggle={(id) => setHidden((prev) => toggleColumn(id, order, prev))}
          onDragEnd={onDragEnd}
          sensors={sensors}
        />
      </div>
      <ResultTable columns={shown} rows={rows} verdicts={verdicts} maxHeightClassName={maxHeightClassName} />
    </div>
  );
}

/** The table itself: renders `columns` in the order given, reading each cell at the column's own index. */
export function ResultTable({
  columns,
  rows,
  verdicts,
  maxHeightClassName = "max-h-[70vh]",
}: {
  columns: IndexedColumn[];
  rows: (string | null)[][];
  verdicts?: Verdicts;
  maxHeightClassName?: string;
}) {
  return (
    // Scrolls both ways inside a bounded box; the header stays in view.
    <div className={cn("overflow-auto rounded-md border", maxHeightClassName)}>
      <table className="min-w-full text-sm" style={{ width: "max-content" }}>
        <thead className="sticky top-0 z-10 bg-muted">
          <tr>
            {columns.map((c) => (
              <th
                key={c.id}
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
                className={cn(
                  "border-t",
                  passed === true && "bg-green-500/12",
                  passed === false && "bg-red-500/10",
                )}
              >
                {columns.map((c) => (
                  <td key={c.id} className="p-1 align-top min-w-[7ch]">
                    <span className="px-1 font-mono text-xs whitespace-nowrap">{row[c.index] ?? ""}</span>
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
