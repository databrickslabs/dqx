import { useRef, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ChevronDown, ChevronUp, FileEdit, Loader2, ShieldCheck } from "lucide-react";
import { cn } from "@/lib/utils";
import { useColumnLayout, type ColumnLayoutDef } from "@/components/data-table/column-layout";
import { EditColumnsDropdown } from "@/components/data-table/EditColumnsDropdown";
import { formatDateShort } from "@/lib/format-utils";
import type { MonitoredTableSummaryOut } from "@/lib/api";

/** Column keys that carry a comparable value and can drive client sort. */
export type MonitoredTablesSortKey =
  | "catalog"
  | "schema"
  | "table"
  | "description"
  | "checksCount"
  | "rulesCount"
  | "dqScore"
  | "lastRun"
  | "owner"
  | "steward"
  | "status";

interface ColumnDef {
  labelKey: string;
  toggleable: boolean;
  defaultVisible: boolean;
  defaultWidth: number;
  sortable: boolean;
  resizable?: boolean;
  headClassName?: string;
  renderHeader(label: string): ReactNode;
  renderCell(r: MonitoredTableSummaryOut): ReactNode;
}

/**
 * Renders text with a tooltip that only appears when the text is actually
 * clipped. Shared pattern with RulesTable's TruncatedCell — ported from
 * dqlake.
 */
function TruncatedCell({
  text,
  className,
  tooltipText,
}: {
  text: string;
  className?: string;
  tooltipText?: string;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [overflow, setOverflow] = useState(false);

  const checkOverflow = () => {
    const el = ref.current;
    if (!el) return;
    setOverflow(el.scrollWidth > el.clientWidth);
  };

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span ref={ref} className={cn("block truncate", className)} onPointerEnter={checkOverflow}>
          {text}
        </span>
      </TooltipTrigger>
      {overflow && (
        <TooltipContent side="top" className="max-w-md break-words">
          {tooltipText ?? text}
        </TooltipContent>
      )}
    </Tooltip>
  );
}

/** Splits a `catalog.schema.table` FQN into its three parts, tolerating
 *  malformed values (missing parts render as empty strings). */
function splitFqn(fqn: string): { catalog: string; schema: string; table: string } {
  const parts = fqn.split(".");
  return { catalog: parts[0] ?? "", schema: parts[1] ?? "", table: parts[2] ?? fqn };
}

function StatusBadge({ status }: { status: string }) {
  const { t } = useTranslation();
  if (status === "published") {
    return (
      <Badge variant="outline" className="gap-1 text-[10px] border-emerald-500 text-emerald-600">
        <ShieldCheck className="h-2.5 w-2.5" />
        {t("monitoredTables.statusPublished")}
      </Badge>
    );
  }
  return (
    <Badge variant="secondary" className="gap-1 text-[10px]">
      <FileEdit className="h-2.5 w-2.5" />
      {t("monitoredTables.statusDraft")}
    </Badge>
  );
}

const COLUMNS: Record<MonitoredTablesSortKey, ColumnDef> = {
  catalog: {
    labelKey: "monitoredTables.colCatalog",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={splitFqn(r.table.table_fqn).catalog} />,
  },
  schema: {
    labelKey: "monitoredTables.colSchema",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 140,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={splitFqn(r.table.table_fqn).schema} />,
  },
  table: {
    labelKey: "monitoredTables.colTableName",
    toggleable: false,
    defaultVisible: true,
    defaultWidth: 200,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={splitFqn(r.table.table_fqn).table} className="font-medium text-sm" />,
  },
  description: {
    // DQX doesn't capture a description for a monitored table today (the
    // API surfaces no free-text field on MonitoredTableOut) — the column
    // is kept, hidden by default, for parity with dqlake's layout and to
    // make room for the field once the backend adds it.
    labelKey: "monitoredTables.colDescription",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 240,
    sortable: false,
    renderHeader: (label) => label,
    renderCell: () => <span className="text-muted-foreground">—</span>,
  },
  checksCount: {
    // DQX doesn't distinguish "checks" (materialized column-level checks)
    // from "rules" (applied registry rules) in the summary API the way
    // dqlake's binding does — kept hidden by default until that split
    // exists server-side.
    labelKey: "monitoredTables.colChecksCount",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 90,
    sortable: false,
    renderHeader: (label) => label,
    renderCell: () => <span className="text-muted-foreground">—</span>,
  },
  rulesCount: {
    labelKey: "monitoredTables.colRulesCount",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 90,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <span className="tabular-nums">{r.applied_rule_count ?? 0}</span>,
  },
  dqScore: {
    // No quality-score aggregate is exposed on MonitoredTableSummaryOut
    // yet; column is present (hidden by default) for layout parity with
    // dqlake and to avoid another localStorage-key migration once it lands.
    labelKey: "monitoredTables.colDqScore",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 140,
    sortable: false,
    renderHeader: (label) => label,
    renderCell: () => <span className="text-muted-foreground">—</span>,
  },
  lastRun: {
    labelKey: "monitoredTables.colLastRun",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 130,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => {
      const ts = r.table.last_profiled_at;
      if (!ts) return <span className="text-muted-foreground">{"—"}</span>;
      return <span title={ts}>{formatDateShort(ts)}</span>;
    },
  },
  owner: {
    labelKey: "monitoredTables.colOwner",
    toggleable: true,
    defaultVisible: false,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.table.created_by || "—"} className="text-muted-foreground" />,
  },
  steward: {
    labelKey: "monitoredTables.colSteward",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 180,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <TruncatedCell text={r.table.steward || "—"} className="text-muted-foreground" />,
  },
  status: {
    labelKey: "monitoredTables.colStatus",
    toggleable: true,
    defaultVisible: true,
    defaultWidth: 110,
    sortable: true,
    renderHeader: (label) => label,
    renderCell: (r) => <StatusBadge status={r.table.status} />,
  },
};

const DEFAULT_ORDER: MonitoredTablesSortKey[] = [
  "catalog",
  "schema",
  "table",
  "description",
  "checksCount",
  "rulesCount",
  "dqScore",
  "lastRun",
  "owner",
  "steward",
  "status",
];

/** Returns the sortable value for a given column + row, shared between this
 *  component's click-to-sort handling and any caller that pre-sorts rows. */
export function getMonitoredTablesSortValue(
  key: MonitoredTablesSortKey,
  r: MonitoredTableSummaryOut,
): string | number {
  const fqn = splitFqn(r.table.table_fqn);
  switch (key) {
    case "catalog":
      return fqn.catalog.toLowerCase();
    case "schema":
      return fqn.schema.toLowerCase();
    case "table":
      return fqn.table.toLowerCase();
    case "description":
      return "";
    case "checksCount":
      return -1;
    case "rulesCount":
      return r.applied_rule_count ?? 0;
    case "dqScore":
      return -1;
    case "lastRun":
      return r.table.last_profiled_at ?? "";
    case "owner":
      return (r.table.created_by ?? "").toLowerCase();
    case "steward":
      return (r.table.steward ?? "").toLowerCase();
    case "status":
      return r.table.status;
  }
}

const LS_KEY_LAYOUT = "dqx.monitoredTables.layout";

export interface MonitoredTablesTableProps {
  /** Rows to render — already filtered, sorted, and paginated by the caller. */
  rows: MonitoredTableSummaryOut[];
  sortKey: MonitoredTablesSortKey | null;
  sortDir: "asc" | "desc";
  onHeaderClick: (key: MonitoredTablesSortKey) => void;
  onRowClick: (row: MonitoredTableSummaryOut) => void;
  renderActions?: (row: MonitoredTableSummaryOut) => ReactNode;
  pendingBindingId?: string | null;
  /** Rendered to the left of the "Edit Columns" trigger — the filter row. */
  toolbarExtra?: ReactNode;
  emptyState?: ReactNode;
}

/**
 * The Monitored Tables list table: selectable + drag-reorderable columns
 * (persisted to localStorage), stable widths across sort clicks. Ported
 * from dqlake's `BindingsTable` and adapted to DQX's monitored-table
 * fields — see the `checksCount`/`dqScore`/`description` column comments
 * above for fields dqlake has that DQX's API doesn't expose yet.
 */
export function MonitoredTablesTable({
  rows,
  sortKey,
  sortDir,
  onHeaderClick,
  onRowClick,
  renderActions,
  pendingBindingId,
  toolbarExtra,
  emptyState,
}: MonitoredTablesTableProps) {
  const { t } = useTranslation();

  const {
    colOrder,
    colWidths,
    visibleKeys,
    toggleColumn,
    handleDragEnd,
    sensors,
    onResizeStart,
  } = useColumnLayout<MonitoredTablesSortKey>({
    storageKey: LS_KEY_LAYOUT,
    defaultOrder: DEFAULT_ORDER,
    columns: COLUMNS as Record<MonitoredTablesSortKey, ColumnLayoutDef>,
  });

  const hasActions = !!renderActions;
  const allKeys = hasActions ? [...visibleKeys, "__actions__" as MonitoredTablesSortKey] : visibleKeys;

  function handleHeaderClick(key: MonitoredTablesSortKey) {
    if (!COLUMNS[key].sortable) return;
    onHeaderClick(key);
  }

  const totalWidth =
    visibleKeys.reduce((acc, k) => acc + (colWidths[k] ?? COLUMNS[k].defaultWidth), 0) + (hasActions ? 96 : 0);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        {toolbarExtra}
        <EditColumnsDropdown
          order={colOrder}
          labelOf={(key) => t(COLUMNS[key].labelKey)}
          toggleableOf={(key) => COLUMNS[key].toggleable}
          isChecked={(key) => visibleKeys.includes(key)}
          onToggle={toggleColumn}
          onDragEnd={handleDragEnd}
          sensors={sensors}
        />
      </div>

      <div className="overflow-x-auto">
        <Table className="table-fixed" style={{ width: totalWidth, minWidth: totalWidth }}>
          <colgroup>
            {visibleKeys.map((k) => (
              <col key={k} style={{ width: colWidths[k] ?? COLUMNS[k].defaultWidth }} />
            ))}
            {hasActions && <col style={{ width: 96 }} />}
          </colgroup>
          <TableHeader>
            <TableRow className="bg-muted/50 hover:bg-muted/50">
              {visibleKeys.map((k) => {
                const def = COLUMNS[k];
                const width = colWidths[k] ?? def.defaultWidth;
                const isSorted = sortKey === k;
                const isResizable = def.resizable !== false;
                const label = t(def.labelKey);
                return (
                  <TableHead
                    key={k}
                    className={cn(
                      "relative text-xs font-medium",
                      def.headClassName,
                      def.sortable && "cursor-pointer select-none",
                    )}
                    style={{ width, minWidth: width, maxWidth: width }}
                    onClick={def.sortable ? () => handleHeaderClick(k) : undefined}
                    aria-sort={isSorted ? (sortDir === "asc" ? "ascending" : "descending") : undefined}
                  >
                    <span className="inline-flex items-center gap-1">
                      {def.renderHeader(label)}
                      {isSorted &&
                        (sortDir === "asc" ? (
                          <ChevronUp className="h-3 w-3" aria-hidden />
                        ) : (
                          <ChevronDown className="h-3 w-3" aria-hidden />
                        ))}
                    </span>
                    {isResizable && (
                      <span
                        role="separator"
                        aria-orientation="vertical"
                        className="absolute right-0 top-0 h-full w-1 cursor-col-resize select-none hover:bg-border"
                        onMouseDown={(e) => onResizeStart(k, e)}
                        onClick={(e) => e.stopPropagation()}
                      />
                    )}
                  </TableHead>
                );
              })}
              {hasActions && (
                <TableHead className="text-right text-xs font-medium" style={{ width: 96 }}>
                  {t("monitoredTables.colActions")}
                </TableHead>
              )}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((r) => {
              const bindingId = r.table.binding_id;
              const busy = pendingBindingId === bindingId;
              return (
                <TableRow key={bindingId} className="cursor-pointer" onClick={() => onRowClick(r)}>
                  {visibleKeys.map((k) => {
                    const width = colWidths[k] ?? COLUMNS[k].defaultWidth;
                    return (
                      <TableCell
                        key={k}
                        style={{ width, minWidth: width, maxWidth: width }}
                        className="overflow-hidden"
                      >
                        {COLUMNS[k].renderCell(r)}
                      </TableCell>
                    );
                  })}
                  {hasActions && (
                    <TableCell
                      style={{ width: 96 }}
                      className="text-right"
                      onClick={(e) => e.stopPropagation()}
                    >
                      {busy ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground inline-block" />
                      ) : (
                        renderActions?.(r)
                      )}
                    </TableCell>
                  )}
                </TableRow>
              );
            })}
            {rows.length === 0 && emptyState && (
              <TableRow>
                <TableCell colSpan={allKeys.length} className="text-center py-16">
                  {emptyState}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
