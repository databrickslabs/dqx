import type { ReactNode } from "react";

/**
 * The row above an overview table: filter pills on the left, wrapping onto
 * further rows as needed, and the Edit Columns control pinned to the right of
 * the first row, so a long filter bar never pushes it out of line.
 */
export function FilterToolbar({ filters, editColumns }: { filters?: ReactNode; editColumns: ReactNode }) {
  return (
    <div className="flex items-start gap-2">
      <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">{filters}</div>
      {editColumns}
    </div>
  );
}
