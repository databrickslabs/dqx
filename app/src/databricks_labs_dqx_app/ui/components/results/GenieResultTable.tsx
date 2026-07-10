import { useTranslation } from "react-i18next";

// Ported from dqlake's `components/results/GenieResultTable.tsx` (the spec —
// keep the JSON-expansion + capping semantics aligned with it). Sanctioned
// deviations: the footer string via t(). The `failing_record` JSON-object
// expansion is KEPT even though this app's aggregates-only space never
// returns row-level data — the code path is generic (any all-JSON-object
// column expands), harmless, and faithful; see expandJsonColumns.
// The pure helper is exported for the bun tests.

const MAX_ROWS = 20;

/** Parse a cell as a JSON object ({...}), else null. Arrays/scalars don't count. */
function asJsonObject(cell: string | null): Record<string, unknown> | null {
  if (cell == null) return null;
  const s = cell.trim();
  if (s.length < 2 || s[0] !== "{") return null;
  try {
    const v = JSON.parse(s) as unknown;
    return v && typeof v === "object" && !Array.isArray(v)
      ? (v as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

/** A column whose non-null cells are ALL JSON objects (e.g. `failing_record`,
 *  the to_json'd raw record Genie returns) is worth expanding into real
 *  columns. */
function isJsonObjectColumn(rows: (string | null)[][], ci: number): boolean {
  let seen = 0;
  for (const r of rows) {
    const cell = r[ci];
    if (cell == null || cell === "") continue;
    seen++;
    if (asJsonObject(cell) === null) return false;
  }
  return seen > 0;
}

function toCell(value: unknown): string | null {
  if (value == null) return null;
  return typeof value === "string" ? value : String(value);
}

/**
 * Expand any JSON-object column into one column per key, in first-seen order,
 * keeping sibling columns in place. This turns a single JSON-record cell (the
 * whole raw record, to_json'd because the source row is stored as a MAP) into
 * the dataset's actual columns — a real wide row, not a JSON blob.
 * Returns the input unchanged when there's nothing to expand.
 */
export function expandJsonColumns(
  columns: string[],
  rows: (string | null)[][],
): { columns: string[]; rows: (string | null)[][] } {
  const jsonCols = columns.map((_, ci) => isJsonObjectColumn(rows, ci));
  if (!jsonCols.some(Boolean)) return { columns, rows };

  // Per JSON column, collect the union of keys across all rows (first-seen).
  const keysByCol: Record<number, string[]> = {};
  for (let ci = 0; ci < columns.length; ci++) {
    if (!jsonCols[ci]) continue;
    const order: string[] = [];
    const seen = new Set<string>();
    for (const r of rows) {
      const obj = asJsonObject(r[ci]);
      if (!obj) continue;
      for (const k of Object.keys(obj)) {
        if (!seen.has(k)) {
          seen.add(k);
          order.push(k);
        }
      }
    }
    keysByCol[ci] = order;
  }

  const outColumns: string[] = [];
  for (let ci = 0; ci < columns.length; ci++) {
    if (jsonCols[ci]) outColumns.push(...keysByCol[ci]);
    else outColumns.push(columns[ci]);
  }

  const outRows = rows.map((r) => {
    const out: (string | null)[] = [];
    for (let ci = 0; ci < columns.length; ci++) {
      if (jsonCols[ci]) {
        const obj = asJsonObject(r[ci]);
        for (const k of keysByCol[ci]) out.push(obj ? toCell(obj[k]) : null);
      } else {
        out.push(r[ci]);
      }
    }
    return out;
  });

  return { columns: outColumns, rows: outRows };
}

/** Compact rendering of a Genie query result (result_columns / result_rows).
 *  Caps visible rows and scrolls horizontally for wide results. A column of
 *  JSON objects is expanded into real columns first (see expandJsonColumns). */
export function GenieResultTable({
  columns,
  rows,
}: {
  columns: string[];
  rows: (string | null)[][];
}) {
  const { t } = useTranslation();
  if (columns.length === 0 || rows.length === 0) return null;
  const { columns: cols, rows: expanded } = expandJsonColumns(columns, rows);
  const visible = expanded.slice(0, MAX_ROWS);
  const hidden = expanded.length - visible.length;

  return (
    <div className="mt-2 overflow-hidden rounded-md border">
      <div className="max-h-64 overflow-auto">
        <table className="w-full border-collapse text-left text-xs">
          <thead className="sticky top-0 bg-muted/80 backdrop-blur">
            <tr>
              {cols.map((c) => (
                <th
                  key={c}
                  className="whitespace-nowrap px-2 py-1.5 font-medium text-muted-foreground"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visible.map((row, ri) => (
              <tr key={ri} className="border-t">
                {cols.map((_, ci) => (
                  <td
                    key={ci}
                    className="whitespace-nowrap px-2 py-1 font-light tabular-nums"
                  >
                    {row[ci] ?? "—"}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hidden > 0 && (
        <p className="border-t bg-muted/40 px-2 py-1 text-[11px] text-muted-foreground">
          {t("genie.tableShowingRows", {
            visible: visible.length,
            total: expanded.length,
          })}
        </p>
      )}
    </div>
  );
}
